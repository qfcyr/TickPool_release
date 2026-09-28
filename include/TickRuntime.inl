#pragma once
// TickRuntime.inl —— TickRuntime 模板实现
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <chrono>

// ========== 辅助函数 ==========
inline void atomicDecAndNotify(std::atomic<size_t>& atom,
    std::memory_order order = std::memory_order_acq_rel) noexcept {
    if (atom.fetch_sub(1, order) == 1) {
        atom.notify_one();
    }
}

template<typename ResultType>
static void enqueueParallelResult(void* queuePtr, ResultType&& result) {
    if constexpr (!std::is_void_v<ResultType>) {
        auto q = static_cast<moodycamel::ConcurrentQueue<ResultType>*>(queuePtr);
        q->enqueue(std::forward<ResultType>(result));
    }
}

template<typename ResultType>
static void dequeueParallelResults(void* queuePtr, std::vector<ResultType>& output) {
    if constexpr (!std::is_void_v<ResultType>) {
        auto q = static_cast<moodycamel::ConcurrentQueue<ResultType>*>(queuePtr);
        output.clear();
        // 直接批量出队，由 vector 自身控制扩容（首次后容量稳定，不再分配）
        q->try_dequeue_bulk(std::back_inserter(output), std::numeric_limits<size_t>::max());
    }
}

template<typename BufferType>
static void withParallelBufferImpl(void* bufferPtr, auto&& func) {
    auto q = static_cast<moodycamel::ConcurrentQueue<BufferType>*>(bufferPtr);
    func(*q);
}

template<typename BufferType>
static void withParallelBuffersImpl(void* bufferPtr, std::vector<BufferType>& output) {
    auto q = static_cast<moodycamel::ConcurrentQueue<BufferType>*>(bufferPtr);
    output.clear();
    q->try_dequeue_bulk(std::back_inserter(output), std::numeric_limits<size_t>::max());
}

// ========== 异常处理 ==========
template<typename Func, typename Handler>
static void safeExecute(Func&& func, Handler&& handler) {
    try { func(); }
    catch (const std::exception& e) {
        if constexpr (std::is_invocable_v<Handler, const std::exception&>) { handler(e); }
        else std::cerr << "Unhandled exception: " << e.what() << std::endl;
    }
    catch (...) {
        if constexpr (std::is_invocable_v<Handler, const std::exception&>) { handler(std::runtime_error("Unknown exception")); }
        else std::cerr << "Unknown exception" << std::endl;
    }
}

// action 异常的**统一上报点**。语义与 construct 的异常上报一致：先写 stderr（带任务名与阶段），
// 再调用 onTaskException（若注册）。**不**走 onConstructException 策略 —— 那条策略是 construct 专用的；
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::reportTaskException(
    Context& ctx, const std::exception& e, const char* phase) {
    const TaskColdData& cold = state_.coldTable_[ctx.taskId < state_.coldTable_.size() ? ctx.taskId : 0];
    std::cerr << "Task \"" << (cold.debugName.empty() ? "unnamed" : cold.debugName.c_str())
              << "\" threw in " << phase << ": " << e.what() << std::endl;
    if (state_.onTaskExceptionCb_)
        state_.onTaskExceptionCb_(ctx, e, state_.onTaskExceptionData_);
}

// ========== 线程队列索引 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
size_t TickRuntime<Duration, OptionsT, ThreadPoolType>::getThreadQueueIndex() const {
    return state_.roundRobin_.fetch_add(1, std::memory_order_relaxed) % pool_.threadCount();
}

// ========== Arena 分配器 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
typename TickRuntime<Duration, OptionsT, ThreadPoolType>::Context*
TickRuntime<Duration, OptionsT, ThreadPoolType>::acquireContext() {
    void* mem = state_.arena_.allocate(sizeof(Context), alignof(Context));
    auto* ctx = new (mem) Context();
    ctx->pool = owner_;
#ifndef NDEBUG
    ctx->debugMagic = 0xCAFEBABE;
#endif
    return ctx;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::releaseContext(Context* ctx) {
#ifndef NDEBUG
    ctx->debugMagic = 0xDEADDEAD;
#endif
    // arena 在 Tick 结束时重置，无需单独释放
}

// ========== 并行提交：轮插入（ScheduledSubmission 已构造好） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::scheduleTick(
    TaskID ownerTaskId, uint32_t actionId, uint64_t seq, Payload payload, const SubmitOptions& opts) {

    TICKPOOL_PROF_INC(commands);
    size_t execTick = state_.tickCount_.load(std::memory_order_acquire) + opts.delay;
    size_t submitTick = state_.tickCount_.load(std::memory_order_acquire);

    if constexpr (options.enableShortDelayQueue) {
        if (opts.delay <= options.shortDelayThreshold) [[likely]] {
            size_t slot = execTick & (options.shortDelayRingSize - 1);
            state_.shortDelaySlots_[slot].enqueue(
                HierarchicalWheel::Task{ execTick, submitTick, ownerTaskId, actionId, seq, std::move(payload) });
            return;
        }
    }
    state_.delayedWheel_.addTask(state_.tickCount_.load(std::memory_order_acquire), opts.delay,
        HierarchicalWheel::Task{ execTick, submitTick, ownerTaskId, actionId, seq, std::move(payload) });
}

// ========== 确定性 seq 分配 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
uint64_t TickRuntime<Duration, OptionsT, ThreadPoolType>::allocSeq() noexcept {
    if constexpr (options.enableDeterministicOrdering)
        return state_.seqCounter_.fetch_add(1, std::memory_order_relaxed);
    else
        return 0;
}

// ========== ：提交（任务）级去重（键 = 显式 hash，强度 = TaskDesc::merge） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
bool TickRuntime<Duration, OptionsT, ThreadPoolType>::submitDedupCheck(
    TaskID ownerTaskId, uint32_t actionId, uint64_t execTick, uint64_t hash, const ::Payload* probe) {
    // 只在提交显式给出 hash 时被调用 → 未给 hash 的默认路径不会走到这里（连这把锁都不碰）。
    const TaskColdData& cold = state_.coldTable_[ownerTaskId];
    const MergePolicy mp = cold.desc.merge;
    const Equal equal = cold.desc.equal;

    // Disabled：该任务**不参与**提交级去重 —— 写了 hash 也不去重（任务级总开关）
    if (mp == MergePolicy::Disabled) return true;

    const typename State::SubmitDedupKey key{
        static_cast<uint32_t>(ownerTaskId), actionId, execTick, hash };
    std::lock_guard<std::mutex> lock(state_.submitDedupMutex_);
    auto it = state_.submitDedup_.find(key);

    if (it == state_.submitDedup_.end()) {
        // 首次见到该键：占位。只有 Conservative 需要留下 payload 副本供后续值确认。
        auto& slot = state_.submitDedup_[key];
        if (mp == MergePolicy::Conservative && equal != nullptr && probe != nullptr && *probe)
            slot.push_back(probe->clone());
        return true;
    }

    // 键已存在
    if (mp == MergePolicy::Aggressive) return false;   // 只看键：命中即折叠（不调用 equal）

    // Conservative：必须再用 equal 确认"值完全一样"才折叠；缺 equal 或无 payload 则不折叠
    // （宁可不去重，也不冒误合风险）。
    if (equal == nullptr || probe == nullptr || !*probe) return true;
    for (const auto& kept : it->second)
        if (equal(kept.get(), probe->get())) return false;
    it->second.push_back(probe->clone());              // 键同值不同 → 保留，并纳入后续比较
    return true;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::clearSubmitDedup() noexcept {
    // 每 tick 一次（收集到期命令之前）：一次无争用加锁 + 清表；与「每提交」无关，故不进热路径。
    std::lock_guard<std::mutex> lock(state_.submitDedupMutex_);
    state_.submitDedup_.clear();
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::noteAsyncMergeOverride(TaskID id, bool enable) noexcept {
    const uint8_t want = enable ? State::kAsyncMergeForceOn : State::kAsyncMergeForceOff;
    std::lock_guard<std::mutex> lock(state_.asyncMergeOverrideMutex_);
    auto it = state_.asyncMergeOverride_.find(static_cast<uint32_t>(id));
    if (it == state_.asyncMergeOverride_.end()) {
        state_.asyncMergeOverride_.emplace(static_cast<uint32_t>(id), want);
        state_.asyncMergeOverrideCount_.fetch_add(1, std::memory_order_relaxed);
    }
    else if (it->second != want) {
        // 同一次 tick 内设置冲突：退出（更严格）优先
        it->second = State::kAsyncMergeForceOff;
    }
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
std::optional<bool> TickRuntime<Duration, OptionsT, ThreadPoolType>::asyncMergeOverrideOf(TaskID id) const noexcept {
    // 默认（从未显式设置过）走这一次 relaxed 读取就返回 —— 不取锁、不查表。
    if (state_.asyncMergeOverrideCount_.load(std::memory_order_relaxed) == 0) return std::nullopt;
    std::lock_guard<std::mutex> lock(state_.asyncMergeOverrideMutex_);
    auto it = state_.asyncMergeOverride_.find(static_cast<uint32_t>(id));
    if (it == state_.asyncMergeOverride_.end()) return std::nullopt;
    return it->second == State::kAsyncMergeForceOn;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::clearAsyncMergeOverride(TaskID id) noexcept {
    std::lock_guard<std::mutex> lock(state_.asyncMergeOverrideMutex_);
    if (state_.asyncMergeOverride_.erase(static_cast<uint32_t>(id)) > 0)
        state_.asyncMergeOverrideCount_.fetch_sub(1, std::memory_order_relaxed);
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::clearAsyncMergeOverrides() noexcept {
    std::lock_guard<std::mutex> lock(state_.asyncMergeOverrideMutex_);
    state_.asyncMergeOverride_.clear();
    state_.asyncMergeOverrideCount_.store(0, std::memory_order_relaxed);
}

// ========== Async 模式提交 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename TaskFunc>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::submitAsync(
    TaskID id, TaskFunc&& task, const SubmitOptions& opts) {

    // 提交级去重（匿名异步也覆盖；无 payload → 强度只能到 Aggressive，
    // Conservative 因无从做值确认而**不去重**）。目标 tick 取当前 tick —— 本线程立即执行。
    if (opts.hash) {
        const uint64_t nowTick = state_.tickCount_.load(std::memory_order_acquire);
        if (!submitDedupCheck(id, kAnonymousSubmitActionId, nowTick,
                              static_cast<uint64_t>(*opts.hash), nullptr)) {
            state_.dedupedSubmissions_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
    // 异步合并的提交期覆盖（仅显式设置时触碰）
    if (opts.enableAsyncMerge) noteAsyncMergeOverride(id, *opts.enableAsyncMerge);

    TaskTickHot& tick = state_.tickHotTable_[id];
    TaskAsyncHot& async = state_.asyncHotTable_[id];
    void* typedQueuePtr = tick.asyncQueueRaw;

    // 只在 Assert 错误分支按需读取。

    // 根据背压策略选择入队函数
    const uint64_t seq = allocSeq();
    auto enqueueResult = [&](auto&& result) {
        if constexpr (std::is_void_v<std::remove_reference_t<decltype(result)>>) {
            switch (options.asyncBackpressure) {
            case AsyncBackpressurePolicy::DropOldest:
                if (async.asyncEnqueueVoidForce) async.asyncEnqueueVoidForce(typedQueuePtr, seq);
                break;
            case AsyncBackpressurePolicy::Block:
                // 真阻塞（队列满时让出 CPU 重试，不静默丢弃）
                while (!async.asyncEnqueueVoidBlocking) std::this_thread::yield();
                async.asyncEnqueueVoidBlocking(typedQueuePtr, seq);
                break;
            case AsyncBackpressurePolicy::Assert: {
                bool ok = async.asyncEnqueueVoid != nullptr;
                if (!ok) {
                    const TaskColdData& coldErr = state_.coldTable_[id];
                    std::cerr << "[TickPool] Async queue full on task \"" << (coldErr.debugName.empty() ? "unnamed" : coldErr.debugName.c_str()) << "\"" << std::endl;
                    assert(false && "Async queue capacity exceeded");
                }
                async.asyncEnqueueVoid(typedQueuePtr, seq);
                break;
            }
            default: // DropNewest 或其他：失败即丢弃
                if (async.asyncEnqueueVoid) async.asyncEnqueueVoid(typedQueuePtr, seq);
                break;
            }
        }
        else {
            switch (options.asyncBackpressure) {
            case AsyncBackpressurePolicy::DropOldest:
                if (async.asyncEnqueueForce)
                    async.asyncEnqueueForce(typedQueuePtr, const_cast<void*>(static_cast<const void*>(&result)), seq);
                break;
            case AsyncBackpressurePolicy::Block:
                // 真阻塞（队列满时让出 CPU 重试，不静默丢弃）
                while (!async.asyncEnqueueBlocking) std::this_thread::yield();
                async.asyncEnqueueBlocking(typedQueuePtr, const_cast<void*>(static_cast<const void*>(&result)), seq);
                break;
            case AsyncBackpressurePolicy::Assert: {
                bool ok = async.asyncEnqueue != nullptr;
                if (!ok) {
                    const TaskColdData& coldErr = state_.coldTable_[id];
                    std::cerr << "[TickPool] Async queue full on task \"" << (coldErr.debugName.empty() ? "unnamed" : coldErr.debugName.c_str()) << "\"" << std::endl;
                    assert(false && "Async queue capacity exceeded");
                }
                async.asyncEnqueue(typedQueuePtr, const_cast<void*>(static_cast<const void*>(&result)), seq);
                break;
            }
            default: // DropNewest 及其他
                if (async.asyncEnqueue)
                    async.asyncEnqueue(typedQueuePtr, const_cast<void*>(static_cast<const void*>(&result)), seq);
                break;
            }
        }
    };

    using ResultType = decltype(task());
    if constexpr (std::is_void_v<ResultType>) {
        task();
        enqueueResult(nullptr);
    }
    else {
        auto result = task();
        enqueueResult(result);
    }
}

// ========== withParallelBuffer / withParallelBuffers ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename T, typename Func>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::withParallelBuffer(Func&& func) {
    static_assert(!std::is_void_v<T>);
    Context* ctx = tls_currentCtx_;
    if (!ctx) throw std::runtime_error("withParallelBuffer must be called inside a parallel task");

    // 直接通过 hot/cold 表获取队列和类型信息
    TaskTickHot& hot = state_.tickHotTable_[tls_currentId_];
#if TICKPOOL_ENABLE_TYPE_CHECK
    TaskColdData& cold = state_.coldTable_[tls_currentId_];
    if (cold.bufferType != typeid(T)) throw std::runtime_error("Type mismatch in withParallelBuffer");
#endif
    withParallelBufferImpl<T>(hot.bufferQueueRaw, std::forward<Func>(func));
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename T, typename Func>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::withParallelBuffers(Func&& func) {
    static_assert(!std::is_void_v<T>);
    Context* ctx = tls_currentCtx_;
    if (!ctx) throw std::runtime_error("withParallelBuffers must be called inside a task's onDestruct");

    TaskTickHot& hot = state_.tickHotTable_[tls_currentId_];
#if TICKPOOL_ENABLE_TYPE_CHECK
    TaskColdData& cold = state_.coldTable_[tls_currentId_];
    if (cold.bufferType != typeid(T)) throw std::runtime_error("Type mismatch");
#endif

    static thread_local std::vector<T> results;
    results.clear();
    withParallelBuffersImpl<T>(hot.bufferQueueRaw, results);
    func(results);
}

// ========== withParallelResults ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename T, typename Func>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::withParallelResults(Func&& func) {
    static_assert(!std::is_void_v<T>);
    Context* ctx = tls_currentCtx_;
    if (!ctx) throw std::runtime_error("withParallelResults must be called inside a task's onDestruct");

    TaskTickHot& hot = state_.tickHotTable_[tls_currentId_];
#if TICKPOOL_ENABLE_TYPE_CHECK
    TaskColdData& cold = state_.coldTable_[tls_currentId_];
    if (cold.parallelResultType != typeid(T)) throw std::runtime_error("Type mismatch");
#endif

    static thread_local std::vector<T> results;
    results.clear();
    if constexpr (options.enableDeterministicOrdering) {
        // 按提交 seq 排序（确定性入队序），再交给用户
        using Item = std::pair<uint64_t, T>;
        auto* q = static_cast<moodycamel::ConcurrentQueue<Item>*>(hot.parallelQueueRaw);
        static thread_local std::vector<Item> items;
        items.clear();
        q->try_dequeue_bulk(std::back_inserter(items), std::numeric_limits<size_t>::max());
        // 近有序短路 —— 结果按提交 seq 近似有序，免 O(n log n) 全排（乱序才排序）
        {
            TICKPOOL_PROFILE_SCOPE(TICKPOOL_PROF_ACC(sortNs));
            auto bySeq = [](const Item& a, const Item& b) { return a.first < b.first; };
            if (!std::is_sorted(items.begin(), items.end(), bySeq))
                std::sort(items.begin(), items.end(), bySeq);
        }
        results.reserve(items.size());
        for (auto& it : items) results.push_back(std::move(it.second));
    }
    else {
        dequeueParallelResults<T>(hot.parallelQueueRaw, results);
    }

    // 注意：并行结果**不做任何合并/去重** —— 原样（确定性模式下按 seq 排序后）交付。

    func(results);
}

// ========== withAsyncResults（id 由 TickPool 解析后传入） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename T, typename Func>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::withAsyncResults(TaskID id, Func&& func) {
    static_assert(!std::is_void_v<T>, "withAsyncResults called with void type, use std::monostate instead");
    TaskTickHot& tick = state_.tickHotTable_[id];
    TaskColdData& cold = state_.coldTable_[id];
#if TICKPOOL_ENABLE_TYPE_CHECK
    if (cold.asyncResultType != typeid(T)) throw std::runtime_error("Type mismatch in withAsyncResults");
#endif

    auto& policy = cold.desc.asyncMerge;
    policy.adjustForTimeScale(state_.timeScale_.load(std::memory_order_acquire));

    // 提交期覆盖（tri-state）优先于定义期策略。nullopt = 无意见 → 沿用定义期策略，

    const std::optional<bool> mergeOverride = asyncMergeOverrideOf(id);
    const bool allowStateful  = mergeOverride.value_or(policy.enableStatefulResults);
    const bool allowHashMerge = mergeOverride.value_or(policy.enableHashMerge);

    // 队列条目为 {seq, result}（确定性）或 result
    using Item = std::conditional_t<options.enableDeterministicOrdering, std::pair<uint64_t, T>, T>;
    auto* typedQueue = static_cast<TypedAsyncQueue<Item, options.asyncQueueCapacity>*>(tick.asyncQueueRaw);
    if (!typedQueue) throw std::runtime_error("asyncQueue is null");

    static thread_local std::vector<Item> rawItems;
    static thread_local size_t maxFetched = 0;
    rawItems.clear();
    size_t fetched = typedQueue->dequeueBulkTyped(rawItems, policy.maxResultsPerTick);
    if (fetched > maxFetched) { maxFetched = fetched; rawItems.reserve(maxFetched); }

    // 提取结果；确定性模式按 seq 排序
    static thread_local std::vector<T> rawBatch;
    rawBatch.clear();
    rawBatch.reserve(rawItems.size());
    if constexpr (options.enableDeterministicOrdering) {
        // 近有序短路（async 结果同样近似按 seq 有序）
        {
            TICKPOOL_PROFILE_SCOPE(TICKPOOL_PROF_ACC(sortNs));
            auto bySeq = [](const Item& a, const Item& b) { return a.first < b.first; };
            if (!std::is_sorted(rawItems.begin(), rawItems.end(), bySeq))
                std::sort(rawItems.begin(), rawItems.end(), bySeq);
        }
        for (auto& it : rawItems) rawBatch.push_back(std::move(it.second));
    }
    else {
        for (auto& it : rawItems) rawBatch.push_back(std::move(it));
    }
    fetched = rawBatch.size();

    if (fetched == 0) {
        func(rawBatch);
        return;
    }

#if TICKPOOL_ENABLE_STATS
    size_t beforeSize = fetched;
#endif

    // 状态化去重
    if (allowStateful && cold.desc.hasher) {
        static thread_local ankerl::unordered_dense::map<size_t, std::vector<T>> latest;
        latest.clear();
        for (auto& item : rawBatch) {
            size_t h = cold.desc.hasher(&item);
            auto it = latest.find(h);
            if (it == latest.end()) latest[h] = { std::move(item) };
            else {
                bool duplicate = false;
                for (const auto& existing : it->second)
                    if (cold.desc.equal && cold.desc.equal(&existing, &item)) { duplicate = true; break; }
                if (!duplicate) it->second.push_back(std::move(item));
            }
        }
        rawBatch.clear();
        for (auto& [h, vec] : latest)
            for (auto& val : vec) rawBatch.push_back(std::move(val));
#if TICKPOOL_ENABLE_STATS
        beforeSize = rawBatch.size();
#endif
    }

    // 基于哈希的去重
    if (allowHashMerge && cold.desc.hasher && cold.desc.equal &&
        rawBatch.size() > policy.hashMergeThreshold) {
        static thread_local ankerl::unordered_dense::map<size_t, T> uniq;
        static thread_local std::vector<T> collisions;
        uniq.clear();
        collisions.clear();
        for (auto& item : rawBatch) {
            size_t h = cold.desc.hasher(&item);
            auto it = uniq.find(h);
            if (it == uniq.end()) uniq[h] = std::move(item);
            else if (!cold.desc.equal(&it->second, &item)) collisions.push_back(std::move(item));
        }
        rawBatch.clear();
        rawBatch.reserve(uniq.size() + collisions.size());
        for (auto& [h, val] : uniq) rawBatch.push_back(std::move(val));
        for (auto& val : collisions) rawBatch.push_back(std::move(val));
#if TICKPOOL_ENABLE_STATS
        size_t afterHash = rawBatch.size();
        cold.mergedResults.fetch_add(beforeSize - afterHash, std::memory_order_relaxed);
#endif
    }

    func(rawBatch);

    // 覆盖是任务级粘滞 —— 该任务的异步结果已全部被读走，则回落定义期策略。
    if (mergeOverride.has_value() && typedQueue->size_approx() == 0)
        clearAsyncMergeOverride(id);

#if TICKPOOL_ENABLE_STATS
    size_t curSize = typedQueue->size_approx();
    size_t max = cold.maxQueueSize.load(std::memory_order_relaxed);
    while (curSize > max && !cold.maxQueueSize.compare_exchange_weak(max, curSize, std::memory_order_relaxed, std::memory_order_relaxed)) {}
#endif
}

// ========== 延迟任务收集 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::processDelayedWheel(size_t currentTick) {
    // 提交侧去重的窗口边界 —— 本 tick 收集到期命令之前作废上一窗口的去重表。
    clearSubmitDedup();
    // 复用跨 tick 的分组 buffer —— 仅任务数变化时重分配；旧内容清空。
    auto& tasksByOwner = tasksByOwnerBuf_;
    if (tasksByOwner.size() != state_.tickHotTable_.size()) {
        // 首次或任务数变化：整表重建（新槽为空，无需清）
        tasksByOwner.clear();
        tasksByOwner.resize(state_.tickHotTable_.size());
        activeOwners_.clear();
    }
    else {
        // 只清上一 tick 活跃槽（免全表 O(任务数) 扫描）
        for (TaskID o : activeOwners_) tasksByOwner[o].clear();
        activeOwners_.clear();
    }
    // 记脏 + 入槽（同槽后续 push 不再重复记）
    auto pushToOwner = [&](TaskID owner, HierarchicalWheel::Task&& task) {
        auto& v = tasksByOwner[owner];
        if (v.empty()) activeOwners_.push_back(owner);
        v.push_back(std::move(task));
    };
    // 短延迟队列
    if constexpr (options.enableShortDelayQueue) {
        size_t slotIdx = currentTick & (options.shortDelayRingSize - 1);
        auto& slot = state_.shortDelaySlots_[slotIdx];
        auto& batch = wheelBatchBuf_;   // 跨 tick 复用
        batch.clear();
        // 到期任务**全量**取出（不做任何到期节流）。

        // 留着一个「看起来能调、实际无效」的开关比删掉更危险。
        while (slot.try_dequeue_bulk(std::back_inserter(batch), 64)) {
            for (auto& task : batch) {
                if (task.executeTick == currentTick) [[likely]] {
                    pushToOwner(task.ownerTaskId, std::move(task));
                }
                else {
                    state_.delayedWheel_.addTaskAtTarget(currentTick, task.executeTick, std::move(task));
                }
            }
            batch.clear();
        }
    }
    // 长延迟时间轮
    auto& wheelTasks = wheelTasksBuf_;   // 出参复用
    wheelTasks.clear();                  // 必须先清空：swap 会把旧内容交给槽位
    state_.delayedWheel_.tick(currentTick, wheelTasks);
    for (auto& task : wheelTasks) {
        pushToOwner(task.ownerTaskId, std::move(task));
    }
}

// ========== ：执行体三件套（worker 路径与小波次内联路径共用） ==========

// 构造执行体：TLS 建立/恢复 + 异常策略（Abort / LogAndContinue / Callback）全集于一处。
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::runConstructBody(TaskID id, Context* ctx) noexcept {
    Context* oldCtx = tls_currentCtx_;
    TaskID oldId = tls_currentId_;
    tls_currentCtx_ = ctx;
    tls_currentId_ = id;
    ctx->taskId = id;
    TaskColdData& cold = state_.coldTable_[id];
    try {
        if (cold.onConstruct) cold.onConstruct();
    }
    catch (const std::exception& e) {
        TaskColdData& c = state_.coldTable_[id];
        std::cerr << "Task \"" << (c.debugName.empty() ? ("#" + std::to_string(id)).c_str() : c.debugName.c_str())
            << "\" threw in onConstruct: " << e.what() << std::endl;
        if constexpr (options.onConstructException == DefaultTickPoolOptions::ConstructExceptionPolicy::LogAndContinue) {
            if (state_.onTaskExceptionCb_) state_.onTaskExceptionCb_(*ctx, e, state_.onTaskExceptionData_);
        }
        else if constexpr (options.onConstructException == DefaultTickPoolOptions::ConstructExceptionPolicy::Callback) {
            if (state_.onTaskExceptionCb_) state_.onTaskExceptionCb_(*ctx, e, state_.onTaskExceptionData_);
        }
        else {
            if (state_.onTaskExceptionCb_) state_.onTaskExceptionCb_(*ctx, e, state_.onTaskExceptionData_);
            std::abort();
        }
    }
    catch (...) {
        TaskColdData& c = state_.coldTable_[id];
        std::cerr << "Task \"" << (c.debugName.empty() ? ("#" + std::to_string(id)).c_str() : c.debugName.c_str())
            << "\" threw unknown exception in onConstruct" << std::endl;
        std::runtime_error unknown("Unknown exception");
        if constexpr (options.onConstructException == DefaultTickPoolOptions::ConstructExceptionPolicy::LogAndContinue) {
            if (state_.onTaskExceptionCb_) state_.onTaskExceptionCb_(*ctx, unknown, state_.onTaskExceptionData_);
        }
        else if constexpr (options.onConstructException == DefaultTickPoolOptions::ConstructExceptionPolicy::Callback) {
            if (state_.onTaskExceptionCb_) state_.onTaskExceptionCb_(*ctx, unknown, state_.onTaskExceptionData_);
        }
        else {
            if (state_.onTaskExceptionCb_) state_.onTaskExceptionCb_(*ctx, unknown, state_.onTaskExceptionData_);
            std::abort();
        }
    }
    tls_currentCtx_ = oldCtx;
    tls_currentId_ = oldId;
}

// 子任务执行体：经 ActionRegistry 按 actionId 解析行为。
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::runSubTaskBody(HierarchicalWheel::Task& t) noexcept {
    TICKPOOL_PROF_INC(subtasks);
    Context ctx;
    ctx.pool = owner_;
    ctx.tick = state_.tickCount_.load(std::memory_order_acquire);
    ctx.taskId = t.ownerTaskId;
    ctx.seq = t.seq;
    ctx.parallelResultQueue = state_.tickHotTable_[t.ownerTaskId].parallelQueueRaw;
    // TLS：action work 内经 pool.withParallelBuffer / withParallelResults 访问自身队列
    Context* oldCtx = tls_currentCtx_;
    TaskID oldId = tls_currentId_;
    tls_currentCtx_ = &ctx;
    tls_currentId_ = t.ownerTaskId;
    bool ok = true;
    try {
        if (registry_) registry_->execute(t.actionId, ctx, t.payload.get());
    }
    catch (const std::exception& e) {
        ok = false;
        reportTaskException(ctx, e, "parallel subtask");   // M24：上报，不再静默/终止
    }
    catch (...) {
        ok = false;
        const std::runtime_error unknown("Unknown exception");
        reportTaskException(ctx, unknown, "parallel subtask");
    }
    tls_currentCtx_ = oldCtx;
    tls_currentId_ = oldId;
#if TICKPOOL_ENABLE_STATS
    if (ok) {   // 失败的子任务没有产出结果，不计入结果计数（等待时长同理无意义）
        TaskColdData& cold = state_.coldTable_[t.ownerTaskId];
        size_t curTick = state_.tickCount_.load(std::memory_order_acquire);
        cold.totalWaitTicks.fetch_add(curTick - t.submitTick, std::memory_order_relaxed);
        cold.totalResultCount.fetch_add(1, std::memory_order_relaxed);
    }
#endif
}

// 析构执行体：起恢复 TLS（防 run 线程残留上下文放行外部并行提交）。
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::runDestructBody(TaskID id, Context* ctx) noexcept {
    Context* oldCtx = tls_currentCtx_;
    TaskID oldId = tls_currentId_;
    tls_currentCtx_ = ctx;
    tls_currentId_ = id;
    ctx->taskId = id;
    TaskColdData& cold = state_.coldTable_[id];
    safeExecute([&] { if (cold.onDestruct) cold.onDestruct(); },
        [&](const std::exception& ex) { if (state_.onTaskExceptionCb_) state_.onTaskExceptionCb_(*ctx, ex, state_.onTaskExceptionData_); });
    tls_currentCtx_ = oldCtx;
    tls_currentId_ = oldId;
}

// ========== 单 Tick 执行 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickRuntime<Duration, OptionsT, ThreadPoolType>::executeTick() {
    // tick 执行标记（同线程死锁防护）+ quiescent 门（shared 锁；世界导出侧持 unique 锁等待）。
    struct TickMarker {
        bool& f;
        explicit TickMarker(bool& flag) noexcept : f(flag) { f = true; }
        ~TickMarker() { f = false; }
    };
    TickMarker marker(tls_inTick_);
#ifndef NDEBUG
    struct ExecGuard {
        std::atomic<bool>& flag;
        explicit ExecGuard(std::atomic<bool>& f) noexcept : flag(f) {
            assert(!flag.exchange(true) && "TickPool: concurrent executeTick — run() must be on a single dedicated thread");
        }
        ~ExecGuard() { flag.store(false, std::memory_order_relaxed); }
    } execGuard(execActive_);
#endif
    std::shared_lock tickGateLock(state_.tickGate_);
    TICKPOOL_PROFILE_SCOPE(TICKPOOL_PROF_ACC(tickTotalNs));   // 覆盖整个 tick body

    size_t current = state_.tickCount_.load(std::memory_order_acquire);
    {
        TICKPOOL_PROFILE_SCOPE(TICKPOOL_PROF_ACC(wheelNs));
        processDelayedWheel(current);
    }
    auto& tasksByOwner = tasksByOwnerBuf_;   // 跨 tick 复用的按-owner 分组 buffer

    // tick 边界回调（合成上下文：pool + tick，taskId 无效）
    Context boundaryCtx;
    boundaryCtx.pool = owner_;
    boundaryCtx.tick = current;
    if (state_.onTickBeginCb_) state_.onTickBeginCb_(boundaryCtx, state_.onTickBeginData_);

    if (state_.timeScale_.load(std::memory_order_acquire) == 0.0) {
        std::unique_lock lock(state_.timeMutex_);
        state_.timeCV_.wait(lock, [this] {
            return state_.timeScale_.load(std::memory_order_acquire) > 0.0 ||
                !state_.running_.load(std::memory_order_acquire);
        });
        if (!state_.running_.load(std::memory_order_acquire)) return;
    }

    // 固定步长节拍：startWallTime_ 是「上一 tick 的结束时刻」（每 tick 末尾都会重置），

    // → 第 k 个 tick 实际 sleep (k+1)×周期，tick 周期线性增长，累计耗时 = dur × N(N+1)/2。

    // 20ms×20 ticks 应为 400ms 实为 4353ms。该 bug 曾使 stress_test 的 3.05 t/s 基线成立。
    auto wallDuration = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        tickDuration_ * state_.timeScale_.load(std::memory_order_acquire));
    auto targetWallTime = state_.startWallTime_ + wallDuration;

    state_.arena_.reset();
    // 跨 tick 复用
    auto& waveContexts = waveContextsBuf_;

    const TaskID startSentinel = startSentinel_;   // loadTaskRegistry 时已缓存（免每 tick 按名查）
    for (auto& w : plan_->waves()) {
        if (w.count == 0 || (w.count == 1 && plan_->waveTasks()[w.start] == startSentinel))
            continue;

        TICKPOOL_PROF_INC(waves);
        waveContexts.reserve(w.count);
        const TaskID* base = plan_->waveTasks().data() + w.start;
        bool hasBeforeParallel = false;
        for (uint32_t i = 0; i < w.count; ++i) {
            TaskID id = base[i];
            TaskTickHot& tick = state_.tickHotTable_[id];
            TaskColdData& cold = state_.coldTable_[id];
            if (cold.desc.construct == ConstructPolicy::BeforeParallel)
                hasBeforeParallel = true;
            Context* ctx = acquireContext();
            ctx->tick = current;
            ctx->parallelResultQueue = tick.parallelQueueRaw;
            ctx->parallelBufferQueue = tick.bufferQueueRaw;
            waveContexts.push_back(ctx);
        }

        // ---------- 本波次到期子任务计数（内联判定与派发都要用，故先算） ----------
        size_t waveSubCount = 0;
        for (uint32_t i = 0; i < w.count; ++i)
            waveSubCount += tasksByOwner[base[i]].size();
        // 真实任务数：扣除两个哨兵（它们无任何 construct/destruct 行为，纯拓扑占位）
        size_t realTasks = w.count;
        for (uint32_t i = 0; i < w.count; ++i) {
            const TaskID id = base[i];
            if (id == startSentinel_ || id == endSentinel_) --realTasks;
        }

        // ---------- ：小波次内联执行 ----------

        // 不入队、不 wakeWorkers、不碰任何等待计数器。
        if constexpr (options.inlineWaveMaxTasks > 0) {
            if (realTasks + waveSubCount <= options.inlineWaveMaxTasks) {
                TICKPOOL_PROF_INC(inlinedWaves);
                TICKPOOL_PROFILE_SCOPE(TICKPOOL_PROF_ACC(inlineNs));
                for (uint32_t i = 0; i < w.count; ++i)
                    runConstructBody(base[i], waveContexts[i]);
                for (uint32_t i = 0; i < w.count; ++i)
                    for (auto& task : tasksByOwner[base[i]])
                        runSubTaskBody(task);
                for (uint32_t i = 0; i < w.count; ++i)
                    runDestructBody(base[i], waveContexts[i]);
                waveContexts.clear();
                continue;
            }
        }

        // ---------- 构造阶段（worker 派发路径） ----------
        std::atomic<size_t> constructPending{ w.count };
        for (uint32_t i = 0; i < w.count; ++i) {
            TaskID id = base[i];
            Context* ctx = waveContexts[i];
            // 不捕获 TaskKey（std::string）—— 构造体见 runConstructBody（与内联路径共用，
            auto constructWrapper = [this, id, ctx, &constructPending]() mutable {
                runConstructBody(id, ctx);
                atomicDecAndNotify(constructPending, std::memory_order_release);
            };
            pool_.enqueue(getThreadQueueIndex(), std::move(constructWrapper));
        }

        // ---------- 本波次到期子任务 ----------
        std::atomic<size_t> subTasksPending{ waveSubCount };
        auto enqueueSubTasks = [&]() {
            for (uint32_t i = 0; i < w.count; ++i) {
                for (auto& task : tasksByOwner[base[i]]) {
                    pool_.enqueue(getThreadQueueIndex(),
                        [this, t = &task, &subTasksPending]() {
                            runSubTaskBody(*t);
                            atomicDecAndNotify(subTasksPending, std::memory_order_release);
                        });
                }
            }
        };

        auto waitLoop = [&](std::atomic<size_t>& pending, std::atomic<uint64_t>* acc, const char*) {
            // 无在途任务 → 直接返回，**不唤醒**（Sequential 析构等情形下 destructPending 常为 0，

            if (pending.load(std::memory_order_relaxed) == 0) return;
            TICKPOOL_PROFILE_SCOPE(acc);
            // 本阶段任务已全部入队 → 唤醒空闲 worker（必须 notify_all，理由见 wakeWorkers 注释）。
            pool_.wakeWorkers();
            size_t prev = pending.load(std::memory_order_acquire);
            while (prev > 0) {
                pending.wait(prev, std::memory_order_acquire);
                prev = pending.load(std::memory_order_acquire);
                if (prev > 0) {
                    if (pool_.pendingTasks() > 0)
                        pool_.executeUpTo(getThreadQueueIndex(), 16);   // 批量帮助执行
                    else
                        std::this_thread::yield();
                    prev = pending.load(std::memory_order_acquire);
                }
            }
        };

        if (hasBeforeParallel) {
            waitLoop(constructPending, TICKPOOL_PROF_ACC(constructNs), "construct");
            if (subTasksPending.load(std::memory_order_relaxed) > 0) {
                enqueueSubTasks();
                waitLoop(subTasksPending, TICKPOOL_PROF_ACC(subtaskNs), "subtasks");
            }
        }
        else {
            if (subTasksPending.load(std::memory_order_relaxed) > 0)
                enqueueSubTasks();
            pool_.wakeWorkers();   // OverlapParallel：构造+子任务已入队，唤醒一次（须 notify_all）
            // OverlapParallel 下 run 线程同时在等构造与子任务，整段记到 constructNs
            TICKPOOL_PROFILE_SCOPE(TICKPOOL_PROF_ACC(constructNs));
            size_t prevC = constructPending.load(std::memory_order_acquire);
            size_t prevS = subTasksPending.load(std::memory_order_acquire);
            while (prevC > 0 || prevS > 0) {
                if (prevC > 0) constructPending.wait(prevC, std::memory_order_acquire);
                if (prevS > 0) subTasksPending.wait(prevS, std::memory_order_acquire);
                prevC = constructPending.load(std::memory_order_acquire);
                prevS = subTasksPending.load(std::memory_order_acquire);
                if (prevC > 0 || prevS > 0) {
                    if (pool_.pendingTasks() > 0)
                        pool_.executeUpTo(getThreadQueueIndex(), 16);   // 批量帮助执行
                    else
                        std::this_thread::yield();
                    prevC = constructPending.load(std::memory_order_acquire);
                    prevS = subTasksPending.load(std::memory_order_acquire);
                }
            }
        }

        // ---------- 析构阶段 ----------
        {
            TICKPOOL_PROFILE_SCOPE(TICKPOOL_PROF_ACC(destructNs));
            for (uint32_t i = 0; i < w.count; ++i) {
                TaskID id = base[i];
                if (state_.coldTable_[id].desc.destruct == DestructPolicy::Sequential)
                    runDestructBody(id, waveContexts[i]);
            }
        }

        std::atomic<size_t> destructPending{ 0 };
        for (uint32_t i = 0; i < w.count; ++i) {
            TaskID id = base[i];
            if (state_.coldTable_[id].desc.destruct == DestructPolicy::OverlapParallel) {
                destructPending.fetch_add(1, std::memory_order_relaxed);
                // 析构体见 runDestructBody（与内联路径共用；闭包只剩 POD 捕获）
                auto destructWrapper = [this, id, ctx = waveContexts[i], &destructPending]() {
                    runDestructBody(id, ctx);
                    atomicDecAndNotify(destructPending, std::memory_order_release);
                };
                pool_.enqueue(getThreadQueueIndex(), std::move(destructWrapper));
            }
        }
        waitLoop(destructPending, TICKPOOL_PROF_ACC(destructNs), "destruct");

        waveContexts.clear();
    }

    // 超预算（本 tick 执行耗时 > 一个周期）时等待函数立即返回，且此处把锚点重置为 now
    // → 不累积欠账、不做追赶（既定语义）。

    {
        TICKPOOL_PROFILE_SCOPE(TICKPOOL_PROF_ACC(sleepNs));
        if (state_.timeScale_.load(std::memory_order_acquire) > 0.0)
            tickpool_detail::sleepUntilSteady(targetWallTime);
    }
    state_.tickCount_.fetch_add(1, std::memory_order_release);
    state_.startWallTime_ = std::chrono::steady_clock::now();
    boundaryCtx.tick = state_.tickCount_.load(std::memory_order_acquire);
    if (state_.onTickEndCb_) state_.onTickEndCb_(boundaryCtx, state_.onTickEndData_);
    TICKPOOL_PROF_INC(ticks);
}
