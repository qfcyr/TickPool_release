#pragma once
// TickPool.inl —— TickPool 模板实现
#include "TickPool.h"
#include <iostream>
#include <cstdio>
#include <stdexcept>
#include <shared_mutex>
#include <random>
#include <type_traits>
#include <variant>
#include <algorithm>
#include <set>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

// ========== 构造/析构 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
TickPool<Duration, OptionsT, ThreadPoolType>::TickPool(Duration tickDuration)
    : threadPool_(std::make_unique<ThreadPoolType>()),
    runtime_(*threadPool_, tickDuration),
    tickDuration_(tickDuration) {
    runtime_.attachOwner(this);
    runtime_.state().startWallTime_ = std::chrono::steady_clock::now();
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
TickPool<Duration, OptionsT, ThreadPoolType>::TickPool(Duration tickDuration, std::unique_ptr<ThreadPoolType> pool)
    : threadPool_(std::move(pool)),
    runtime_(*threadPool_, tickDuration),
    tickDuration_(tickDuration) {
    runtime_.attachOwner(this);
    runtime_.state().startWallTime_ = std::chrono::steady_clock::now();
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
TickPool<Duration, OptionsT, ThreadPoolType>::~TickPool() { stop(); }

// ========== 任务注册（V2：Builder Commit 调用；行为与配置分离） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename ParallelResultType, typename AsyncResultType, typename BufferType>
void TickPool<Duration, OptionsT, ThreadPoolType>::registerTask(
    const TaskKey& key, std::vector<ActionDecl<Duration, OptionsT, ThreadPoolType>> actions,
    Fn construct, Fn destruct, TaskDesc desc) {
    // 身份注册（去重检查 + 分配 TaskID）归独立 TaskRegistry；重名抛 invalid_argument
    TaskID id = taskRegistry_.registerKey(key);

    runtime_.state().tickHotTable_.emplace_back();
    runtime_.state().asyncHotTable_.emplace_back();
    runtime_.state().coldTable_.emplace_back(std::move(desc));
    runtime_.state().coldTable_.back().onConstruct = std::move(construct);
    runtime_.state().coldTable_.back().onDestruct = std::move(destruct);

    TaskTickHot& tick = runtime_.state().tickHotTable_[id];
    TaskAsyncHot& async = runtime_.state().asyncHotTable_[id];
    TaskColdData& cold = runtime_.state().coldTable_[id];

    // ---------- 异步队列 ----------
    using SafeAsyncType = NonVoid_t<AsyncResultType>;
    using AsyncItem = std::conditional_t<options.enableDeterministicOrdering,
        std::pair<uint64_t, SafeAsyncType>, SafeAsyncType>;
    auto typedAsync = std::make_unique<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>>();
    void* asyncRaw = typedAsync.release();
    tick.asyncQueueRaw = runtime_.state().bufferRegistry_.add(
        std::unique_ptr<void, void(*)(void*)>(asyncRaw,
            &TypedQueueDeleter<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>>));

    // 异步回调
    if constexpr (std::is_void_v<AsyncResultType>) {
        async.asyncEnqueue = nullptr;
        async.asyncEnqueueVoid = [](void* typedQueuePtr, uint64_t seq) noexcept {
            auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
            if constexpr (options.enableDeterministicOrdering) q->enqueue({ seq, std::monostate{} });
            else q->enqueue(std::monostate{});
        };
        async.asyncEnqueueForce = nullptr;
        async.asyncEnqueueVoidForce = [](void* typedQueuePtr, uint64_t seq) noexcept {
            auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
            if constexpr (options.enableDeterministicOrdering) q->enqueue_drop_oldest({ seq, std::monostate{} });
            else q->enqueue_drop_oldest(std::monostate{});
        };
        // Block 背压阻塞入队（void 结果）
        async.asyncEnqueueBlocking = nullptr;
        async.asyncEnqueueVoidBlocking = [](void* typedQueuePtr, uint64_t seq) noexcept {
            auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
            if constexpr (options.enableDeterministicOrdering) q->enqueueBlocking(AsyncItem{ seq, std::monostate{} });
            else q->enqueueBlocking(std::monostate{});
        };
    }
    else {
        async.asyncEnqueue = [](void* typedQueuePtr, void* result, uint64_t seq) noexcept {
            auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
            if constexpr (options.enableDeterministicOrdering) q->enqueue({ seq, std::move(*static_cast<AsyncResultType*>(result)) });
            else q->enqueue(std::move(*static_cast<AsyncResultType*>(result)));
        };
        async.asyncEnqueueVoid = nullptr;
        async.asyncEnqueueForce = [](void* typedQueuePtr, void* result, uint64_t seq) noexcept {
            auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
            if constexpr (options.enableDeterministicOrdering) q->enqueue_drop_oldest({ seq, std::move(*static_cast<AsyncResultType*>(result)) });
            else q->enqueue_drop_oldest(std::move(*static_cast<AsyncResultType*>(result)));
        };
        async.asyncEnqueueVoidForce = nullptr;
        // Block 背压阻塞入队（非 void 结果）
        async.asyncEnqueueVoidBlocking = nullptr;
        async.asyncEnqueueBlocking = [](void* typedQueuePtr, void* result, uint64_t seq) noexcept {
            auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
            if constexpr (options.enableDeterministicOrdering) q->enqueueBlocking(AsyncItem{ seq, std::move(*static_cast<AsyncResultType*>(result)) });
            else q->enqueueBlocking(std::move(*static_cast<AsyncResultType*>(result)));
        };
    }
    async.asyncClear = [](void* typedQueuePtr) noexcept {
        auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
        q->clear();
    };
    // asyncSize / dropOldestAsync（pendingAsyncResults 与统计查询用）
    async.asyncSize = [](void* typedQueuePtr) noexcept -> size_t {
        auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
        return q->size_approx();
    };
    async.dropOldestAsync = [](void* typedQueuePtr) noexcept -> bool {
        auto* q = static_cast<TypedAsyncQueue<AsyncItem, options.asyncQueueCapacity>*>(typedQueuePtr);
        return q->dropOne();
    };

    // ---------- 并行结果队列 ----------
    using ParallelItem = std::conditional_t<options.enableDeterministicOrdering,
        std::pair<uint64_t, ParallelResultType>, ParallelResultType>;
    if constexpr (std::is_void_v<ParallelResultType>) {
        tick.parallelQueueRaw = nullptr;
    }
    else {
        auto pq = std::make_unique<moodycamel::ConcurrentQueue<ParallelItem>>();
        void* raw = pq.release();
        tick.parallelQueueRaw = runtime_.state().bufferRegistry_.add(
            std::unique_ptr<void, void(*)(void*)>(raw,
                &TypedQueueDeleter<moodycamel::ConcurrentQueue<ParallelItem>>));
    }

    // ---------- 缓冲队列 ----------
    if constexpr (std::is_void_v<BufferType>) {
        tick.bufferQueueRaw = nullptr;
    }
    else {
        auto bq = std::make_unique<moodycamel::ConcurrentQueue<BufferType>>();
        void* raw = bq.release();
        tick.bufferQueueRaw = runtime_.state().bufferRegistry_.add(
            std::unique_ptr<void, void(*)(void*)>(raw,
                &TypedQueueDeleter<moodycamel::ConcurrentQueue<BufferType>>));
    }

#if TICKPOOL_ENABLE_TYPE_CHECK
    cold.parallelQueueType = typeid(ParallelResultType);
    cold.bufferQueueType = typeid(BufferType);
    cold.parallelResultType = typeid(ParallelResultType);
    cold.asyncResultType = typeid(AsyncResultType);
    cold.bufferType = typeid(BufferType);
#endif

    // ---------- Action 注册（全名 = taskKey.actionName；经独立 ActionRegistry） ----------
    taskActionIndex_.emplace_back();   // per-task 短名索引槽（与队列表同序）
    for (auto& a : actions) {
        // 注意：registerAction 会移动 decl（a.name 随之清空），短名须先复制且求值顺序分离
        const std::string shortName = a.name;
        const uint32_t actionId = actionRegistry_.registerAction(key + "." + a.name, std::move(a));
        taskActionIndex_.back().emplace(shortName, actionId);
    }

    // id→key 名字由 TaskRegistry 维护（registerKey 已注册）；值拷贝防指针悬垂
    cold.debugName = taskRegistry_.idToKeyAt(id);

    topologyValid_ = false;
}

// ========== 提交：匿名异步（无 .action()；work(callable)） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename Callable>
void TickPool<Duration, OptionsT, ThreadPoolType>::submitAnonymousAsync(
    TaskID id, Callable&& callable, const SubmitOptions& opts) {
    runtime_.submitAsync(id, std::forward<Callable>(callable), opts);
}

// ========== 提交：带 action 的数据提交（并行/异步按注册模式分派） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename Payload>
void TickPool<Duration, OptionsT, ThreadPoolType>::submitWithAction(
    TaskID id, const TaskKey& key, const std::string& actionName, Payload&& payload, const SubmitOptions& opts) {

    // 热路径经 per-task action 短名索引单次哈希直取 actionId
    uint32_t actionId = 0;
    bool found = false;
    if (id < taskActionIndex_.size()) {
        auto it = taskActionIndex_[id].find(actionName);
        if (it != taskActionIndex_[id].end()) {
            actionId = it->second;
            found = true;
        }
    }
    std::string fullName;   // 仅错误消息用（延迟构造）
    if (!found) {
        fullName = key + "." + actionName;
        if (!actionRegistry_.hasAction(fullName))
            throw std::invalid_argument("submit: action \"" + fullName + "\" not registered (parallel actions must be registered at define-time)");
        actionId = actionRegistry_.actionId(fullName);
    }

    // payload 类型校验
    using PayloadT = std::conditional_t<std::is_void_v<Payload>, std::monostate, Payload>;
    if (actionRegistry_.payloadType(actionId) != typeid(PayloadT)) {
        // 错误路径才构造名字（无论是否命中索引；命中时 fullName 未建需显式拼）
        throw std::invalid_argument("submit: payload type mismatch for action \"" + key + "." + actionName + "\"");
    }

    if (actionRegistry_.isAsync(actionId)) {
        // 异步：立即执行 + 入异步队列（不进轮/快照）
        auto& state = runtime_.state();
        // 提交级去重（异步 action 同样覆盖；有 payload → Conservative 可做值确认）。
        ::Payload p(std::forward<Payload>(payload));
        if (opts.hash &&
            !runtime_.submitDedupCheck(id, actionId, state.tickCount_.load(std::memory_order_acquire),
                static_cast<uint64_t>(*opts.hash), &p)) {
            state.dedupedSubmissions_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        // 异步合并的提交期覆盖（仅显式设置时触碰）
        if (opts.enableAsyncMerge) runtime_.noteAsyncMergeOverride(id, *opts.enableAsyncMerge);
        void* asyncQueue = state.tickHotTable_[id].asyncQueueRaw;
        Context ctx;
        ctx.pool = this;
        ctx.tick = state.tickCount_.load(std::memory_order_acquire);
        ctx.taskId = id;
        ctx.seq = allocSeq();

        // 语义与并行子任务一致：报告 + 不产出结果 + 返回；不中断调用方、不杀进程。
        try {
            actionRegistry_.executeAsync(actionId, ctx, static_cast<const void*>(p.get()), asyncQueue);
        }
        catch (const std::exception& e) {
            runtime_.reportTaskException(ctx, e, "async action");
        }
        catch (...) {
            const std::runtime_error unknown("Unknown exception");
            runtime_.reportTaskException(ctx, unknown, "async action");
        }
        return;
    }

    // 并行：ScheduledSubmission 进轮（须位于 construct 上下文）
    using Runtime = TickRuntime<Duration, OptionsT, ThreadPoolType>;
    auto* curCtx = Runtime::currentCtx();
    if (!curCtx) [[unlikely]] throw std::runtime_error("submit(Tick) must be called inside a task's onConstruct");

    // 目标 tick = 当前 tick，而本 tick 的短延迟环槽位在 tick 开头 processDelayedWheel 时已经被
    if (opts.delay == 0) [[unlikely]]
        throw std::invalid_argument("submit: delay must be >= 1 — a parallel submission targeting the "
            "current tick can never run (its short-delay ring slot was already drained at the start of "
            "this tick), so it used to be dropped silently");
    // 提交级去重（强度 = 该任务 TaskDesc::merge，键 = 显式 hash；未给 hash 则全程零开销）。
    ::Payload p(std::forward<Payload>(payload));
    if (opts.hash) {
        const uint64_t submitTickNow = runtime_.state().tickCount_.load(std::memory_order_acquire);
        if (!runtime_.submitDedupCheck(id, actionId, submitTickNow + opts.delay,
                static_cast<uint64_t>(*opts.hash), &p)) {
            runtime_.state().dedupedSubmissions_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
    uint64_t seq;
#if TICKPOOL_ENABLE_NETWORK
    if constexpr (options.enableCommandLog) {
        // 命令日志记录（seq 在日志锁内分配 → 日志顺序 == 全局提交序，
        uint64_t submitTick = runtime_.state().tickCount_.load(std::memory_order_acquire);
        uint64_t execTick = submitTick + opts.delay;
        seq = appendCommandLog(submitTick, execTick, id, actionId, p);
    }
    else
#endif
    {
        seq = allocSeq();
    }
    runtime_.scheduleTick(id, actionId, seq, std::move(p), opts);
}

// ========== withParallelBuffer / withParallelBuffers（委托 TickRuntime） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename T, typename Func>
void TickPool<Duration, OptionsT, ThreadPoolType>::withParallelBuffer(const TaskKey& /*key*/, Func&& func) {
    static_assert(!std::is_void_v<T>);
    runtime_.template withParallelBuffer<T>(std::forward<Func>(func));
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename T, typename Func>
void TickPool<Duration, OptionsT, ThreadPoolType>::withParallelBuffers(const TaskKey& /*key*/, Func&& func) {
    static_assert(!std::is_void_v<T>);
    runtime_.template withParallelBuffers<T>(std::forward<Func>(func));
}

// ========== withParallelResults（委托 TickRuntime） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename T, typename Func>
void TickPool<Duration, OptionsT, ThreadPoolType>::withParallelResults(const TaskKey& /*key*/, Func&& func) {
    static_assert(!std::is_void_v<T>);
    runtime_.template withParallelResults<T>(std::forward<Func>(func));
}

// ========== withAsyncResults（key 解析留在此层，委托 TickRuntime） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename T, typename Func>
void TickPool<Duration, OptionsT, ThreadPoolType>::withAsyncResults(const TaskKey& key, Func&& func) {
    static_assert(!std::is_void_v<T>, "withAsyncResults called with void type, use std::monostate instead");
    TaskID id = getTaskID(key);
    runtime_.template withAsyncResults<T>(id, std::forward<Func>(func));
}

// ========== 统计信息 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
AsyncQueueStats TickPool<Duration, OptionsT, ThreadPoolType>::getAsyncQueueStats(const TaskKey& key) const {
    const auto& state = runtime_.state();
    TaskID id = getTaskID(key);
    const TaskTickHot& tick = state.tickHotTable_[id];
    const TaskAsyncHot& async = state.asyncHotTable_[id];
    const TaskColdData& cold = state.coldTable_[id];
    AsyncQueueStats stats;
    stats.pendingResults = async.asyncSize ? async.asyncSize(tick.asyncQueueRaw) : 0;
#if TICKPOOL_ENABLE_STATS
    stats.mergedResults = cold.mergedResults.load(std::memory_order_relaxed);
    stats.droppedResults = cold.droppedResults.load(std::memory_order_relaxed);
    stats.maxQueueSize = cold.maxQueueSize.load(std::memory_order_relaxed);
    size_t totalWait = cold.totalWaitTicks.load(std::memory_order_relaxed);
    size_t totalCount = cold.totalResultCount.load(std::memory_order_relaxed);
    stats.averageWaitTicks = totalCount ? static_cast<double>(totalWait) / totalCount : 0.0;
#else
    stats.mergedResults = stats.droppedResults = stats.maxQueueSize = 0; stats.averageWaitTicks = 0.0;
#endif
    return stats;
}

// ========== 钩子 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename UserData>
void TickPool<Duration, OptionsT, ThreadPoolType>::onTickBegin(TickCb cb, UserData* userData) {
    runtime_.state().onTickBeginCb_ = cb; runtime_.state().onTickBeginData_ = reinterpret_cast<void*>(userData);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::onTickBegin(TickCb cb) {
    runtime_.state().onTickBeginCb_ = cb; runtime_.state().onTickBeginData_ = nullptr;
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename UserData>
void TickPool<Duration, OptionsT, ThreadPoolType>::onTickEnd(TickCb cb, UserData* userData) {
    runtime_.state().onTickEndCb_ = cb; runtime_.state().onTickEndData_ = reinterpret_cast<void*>(userData);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::onTickEnd(TickCb cb) {
    runtime_.state().onTickEndCb_ = cb; runtime_.state().onTickEndData_ = nullptr;
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename UserData>
void TickPool<Duration, OptionsT, ThreadPoolType>::onTimeScaleChange(TimeScaleCb cb, UserData* userData) {
    runtime_.state().onTimeScaleChangeCb_ = cb; runtime_.state().onTimeScaleChangeData_ = reinterpret_cast<void*>(userData);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::onTimeScaleChange(TimeScaleCb cb) {
    runtime_.state().onTimeScaleChangeCb_ = cb; runtime_.state().onTimeScaleChangeData_ = nullptr;
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename UserData>
void TickPool<Duration, OptionsT, ThreadPoolType>::onTaskException(ExceptionCb cb, UserData* userData) {
    runtime_.state().onTaskExceptionCb_ = cb; runtime_.state().onTaskExceptionData_ = reinterpret_cast<void*>(userData);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::onTaskException(ExceptionCb cb) {
    runtime_.state().onTaskExceptionCb_ = cb; runtime_.state().onTaskExceptionData_ = nullptr;
}

// ========== 运行控制 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::run(size_t tickCount) {
#ifndef NDEBUG
    debugRunThreadId_ = std::this_thread::get_id();   // 记录 run 专用线程（导入安全点检测用）
#endif
    runtime_.state().running_.store(true, std::memory_order_release);
    ensureCompiled();
    runtime_.loadPlan(plan_);
    runtime_.loadTaskRegistry(&taskRegistry_);   // 运行期按名解析（哨兵/错误消息）走 TaskRegistry
    runtime_.loadRegistry(&actionRegistry_);
    size_t executed = 0;
    while (runtime_.state().running_.load(std::memory_order_acquire) && (tickCount == 0 || executed < tickCount)) {
        runtime_.executeTick();
        ++executed;
        // 回滚环采样（tick 边界；everyTicks 命中）
        if (rollbackMaxFrames_ > 0) {
            size_t cur = runtime_.state().tickCount_.load(std::memory_order_acquire);
            if (cur >= nextRollbackTick_) {
                captureRollbackFrame();
                nextRollbackTick_ = cur + rollbackEveryTicks_;
            }
        }
    }
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::stop() { runtime_.state().running_.store(false, std::memory_order_release); runtime_.state().timeCV_.notify_all(); }

// ========== 时间控制 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::setTimeScale(double factor) {
    double old = runtime_.state().timeScale_.exchange(factor, std::memory_order_acq_rel);
    if (runtime_.state().onTimeScaleChangeCb_) {
        Context ctx;
        ctx.pool = this;
        ctx.tick = runtime_.state().tickCount_.load(std::memory_order_acquire);
        runtime_.state().onTimeScaleChangeCb_(ctx, old, factor, runtime_.state().onTimeScaleChangeData_);
    }
    if (old == 0.0 && factor > 0.0) runtime_.state().timeCV_.notify_one();
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
double TickPool<Duration, OptionsT, ThreadPoolType>::timeScale() const noexcept { return runtime_.state().timeScale_.load(std::memory_order_acquire); }
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::pause() { setTimeScale(0.0); }
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::resume() { setTimeScale(1.0); }
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
bool TickPool<Duration, OptionsT, ThreadPoolType>::isPaused() const noexcept { return runtime_.state().timeScale_.load(std::memory_order_acquire) == 0.0; }

// ========== 时间信息 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
LogicTime TickPool<Duration, OptionsT, ThreadPoolType>::currentLogicTime() const noexcept 
{ return { runtime_.state().tickCount_.load(std::memory_order_acquire), runtime_.state().tickCount_.load(std::memory_order_acquire) * tickDuration_ }; }

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
WallTime TickPool<Duration, OptionsT, ThreadPoolType>::currentWallTime() const 
{ return { std::chrono::steady_clock::now(), runtime_.state().timeScale_.load(std::memory_order_acquire) }; }

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
size_t TickPool<Duration, OptionsT, ThreadPoolType>::tickCount() const noexcept 
{ return runtime_.state().tickCount_.load(std::memory_order_acquire); }

// ===== ：分段计时 Profile（关闭时返回全 0 / 空操作） =====
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
TickProfileSnapshot TickPool<Duration, OptionsT, ThreadPoolType>::getProfile() const noexcept {
#if TICKPOOL_ENABLE_PROFILE
    return runtime_.state().prof_.snapshot();
#else
    return TickProfileSnapshot{};
#endif
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::resetProfile() noexcept {
#if TICKPOOL_ENABLE_PROFILE
    runtime_.state().prof_.reset();
#endif
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
size_t TickPool<Duration, OptionsT, ThreadPoolType>::pendingAsyncResults(const TaskKey& key) const {
    const auto& state = runtime_.state();
    TaskID id = getTaskID(key);
    const TaskTickHot& tick = state.tickHotTable_[id];
    const TaskAsyncHot& async = state.asyncHotTable_[id];
    return async.asyncSize ? async.asyncSize(tick.asyncQueueRaw) : 0;
}

// ========== 序列化 ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
std::string TickPool<Duration, OptionsT, ThreadPoolType>::toMermaid(bool groupByWave) {
    ensureCompiled();   // 惰性编译：允许在 run() 之前直接调用

    const auto& state = runtime_.state();
    const auto& reg = taskRegistry_;
    const auto& plan = plan_;
    const int n = static_cast<int>(reg.taskCount());

    // 波次归属（waveTasks 里同样含哨兵，先一并记录，输出时再过滤）
    std::vector<int> waveOf(static_cast<size_t>(n), -1);
    for (size_t wi = 0; wi < plan.waves().size(); ++wi) {
        const auto& w = plan.waves()[wi];
        for (uint32_t i = 0; i < w.count; ++i)
            waveOf[plan.waveTasks()[w.start + i]] = static_cast<int>(wi);
    }

    // 引擎注入的哨兵任务不是用户图的一部分（否则每条边都会绕到它们身上）
    auto isSentinel = [&](int id) {
        const TaskKey& k = reg.idToKeyAt(static_cast<TaskID>(id));
        return k == "TickConstructionTask" || k == "TickDestructionTask";
    };

    // 边方向（易错，务必与语义一致）：两个列表都是**从对方角度**描述的 ——
    std::set<std::pair<int, int>> edges;
    int unresolved = 0;
    for (int id = 0; id < n; ++id) {
        if (isSentinel(id)) continue;
        const auto& deps = state.coldTable_[static_cast<size_t>(id)].desc.deps;
        for (const auto& nm : deps.after) {          // X 在我之后
            const TaskID d = reg.taskIdOrInvalid(nm);
            if (d == TaskRegistry::kInvalidTaskID) { ++unresolved; continue; }
            edges.emplace(id, static_cast<int>(d));
        }
        for (const auto& nm : deps.before) {         // X 在我之前
            const TaskID d = reg.taskIdOrInvalid(nm);
            if (d == TaskRegistry::kInvalidTaskID) { ++unresolved; continue; }
            edges.emplace(static_cast<int>(d), id);
        }
    }

    size_t userTasks = 0;
    for (int id = 0; id < n; ++id) if (!isSentinel(id)) ++userTasks;

    std::string out;
    out += "%% TickPool task DAG — ";
    out += std::to_string(userTasks) + " tasks, " + std::to_string(plan.waves().size()) + " waves";
    out += "（哨兵任务已剔除；边来自 deps.after/before）\n";
    out += "graph TD\n";

    if (groupByWave) {
        for (size_t wi = 0; wi < plan.waves().size(); ++wi) {
            const auto& w = plan.waves()[wi];
            bool anyUser = false;
            for (uint32_t i = 0; i < w.count; ++i)
                if (!isSentinel(static_cast<int>(plan.waveTasks()[w.start + i]))) { anyUser = true; break; }
            if (!anyUser) continue;
            out += "  subgraph W" + std::to_string(wi) + "[\"wave " + std::to_string(wi) + "\"]\n";
            for (uint32_t i = 0; i < w.count; ++i) {
                const int id = static_cast<int>(plan.waveTasks()[w.start + i]);
                if (isSentinel(id)) continue;
                out += "    T" + std::to_string(id) + "[\"" + reg.idToKeyAt(static_cast<TaskID>(id)) + "\"]\n";
            }
            out += "  end\n";
        }
        // 防御：万一某任务未被任何波次覆盖（正常不该发生），仍保证它出现在图里
        for (int id = 0; id < n; ++id) {
            if (isSentinel(id) || waveOf[static_cast<size_t>(id)] >= 0) continue;
            out += "  T" + std::to_string(id) + "[\"" + reg.idToKeyAt(static_cast<TaskID>(id)) + "\"]\n";
        }
    }
    else {
        for (int id = 0; id < n; ++id) {
            if (isSentinel(id)) continue;
            out += "  T" + std::to_string(id) + "[\"" + reg.idToKeyAt(static_cast<TaskID>(id)) + "\"]\n";
        }
    }

    for (const auto& e : edges)
        out += "  T" + std::to_string(e.first) + " --> T" + std::to_string(e.second) + "\n";

    // 自检：波次由**同一个 DAG** 推导，所以每条边必须满足 wave(from) < wave(to)。

    int orderViolations = 0;
    for (const auto& e : edges) {
        const int wf = waveOf[static_cast<size_t>(e.first)];
        const int wt = waveOf[static_cast<size_t>(e.second)];
        if (wf >= 0 && wt >= 0 && wf >= wt) ++orderViolations;
    }
    if (orderViolations)
        out += "  %% 警告：" + std::to_string(orderViolations) +
            " 条边的方向与波次顺序矛盾（after/before 的语义解释可能有误）\n";
    if (unresolved)
        out += "  %% 注意：" + std::to_string(unresolved) +
            " 个依赖名未能解析到已注册任务（已跳过该边）\n";
    return out;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
std::string TickPool<Duration, OptionsT, ThreadPoolType>::toString() const {
#if defined(TICKPOOL_ENABLE_JSON)
    return encodeSnapshotJson(buildSnapshotData(true));
#else
    return {};
#endif
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::fromString(const std::string& data) {
#if defined(TICKPOOL_ENABLE_JSON)
    requireSafeImportPoint("fromString");           // M22：tick 内调用抛错
    if (data.empty()) return;
    SnapshotData d;
    decodeSnapshotJson(data, d);
    applySnapshotData(d, true);
#else
    (void)data;
#endif
}

// ========== 内部实现：编译（委托 GraphCompiler；惰性，首次 run 前触发） ==========
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::ensureCompiled() {
    if (topologyValid_) return;
    // 哨兵任务注入（定义期；GraphCompiler 不负责修改定义表；注册经 TaskRegistry）
    TaskKey startKey = "TickConstructionTask", endKey = "TickDestructionTask";
    if (!taskRegistry_.hasTask(startKey))
        defineTask(startKey).options(TaskDesc{});
    if (!taskRegistry_.hasTask(endKey))
        defineTask(endKey).options(TaskDesc{});
    // 编译：TaskRegistry（定义身份）+ cold 表 → ExecutionPlan（纯拓扑 taskOrder/waves/waveTasks）
    compiler_.compile(taskRegistry_, runtime_.state().coldTable_, plan_);
    runtime_.state().bufferRegistry_.reserve(runtime_.state().tickHotTable_.size() * 3);
    topologyValid_ = true;
}

// ======================================================================

// ---------- 场景钩子 / 迁移 / 世界哈希 设置 ----------
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename UserData>
void TickPool<Duration, OptionsT, ThreadPoolType>::setWorldCodec(
    Bytes (*save)(Context&, void*), void (*load)(Context&, const Bytes&, void*), UserData* userData) {
    setScenarioHooks(SnapshotScenario::FileSave, save, load, userData);
    setScenarioHooks(SnapshotScenario::Rollback, save, load, userData);
    setScenarioHooks(SnapshotScenario::Network, save, load, userData);
    setScenarioHooks(SnapshotScenario::Custom, save, load, userData);
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::setSchemaVersion(uint64_t v) {
    runtime_.state().schemaVersion_ = v;
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
uint64_t TickPool<Duration, OptionsT, ThreadPoolType>::schemaVersion() const noexcept {
    return runtime_.state().schemaVersion_;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename UserData>
void TickPool<Duration, OptionsT, ThreadPoolType>::onUnknownAction(UnknownActionCb cb, UserData* userData) {
    runtime_.state().onUnknownActionCb_ = cb; runtime_.state().onUnknownActionData_ = reinterpret_cast<void*>(userData);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::onUnknownAction(UnknownActionCb cb) {
    runtime_.state().onUnknownActionCb_ = cb; runtime_.state().onUnknownActionData_ = nullptr;
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename UserData>
void TickPool<Duration, OptionsT, ThreadPoolType>::onSchemaMigrate(SchemaMigrateCb cb, UserData* userData) {
    runtime_.state().onSchemaMigrateCb_ = cb; runtime_.state().onSchemaMigrateData_ = reinterpret_cast<void*>(userData);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::onSchemaMigrate(SchemaMigrateCb cb) {
    runtime_.state().onSchemaMigrateCb_ = cb; runtime_.state().onSchemaMigrateData_ = nullptr;
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
template<typename UserData>
void TickPool<Duration, OptionsT, ThreadPoolType>::setWorldHash(WorldHashCb cb, UserData* userData) {
    runtime_.state().worldHashCb_ = cb; runtime_.state().worldHashData_ = reinterpret_cast<void*>(userData);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::setWorldHash(WorldHashCb cb) {
    runtime_.state().worldHashCb_ = cb; runtime_.state().worldHashData_ = nullptr;
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
uint64_t TickPool<Duration, OptionsT, ThreadPoolType>::worldHash() const {
    const auto& s = runtime_.state();
    if (!s.worldHashCb_) return 0;
    Context ctx;
    ctx.pool = const_cast<TickPool*>(this);
    ctx.tick = s.tickCount_.load(std::memory_order_acquire);
    return s.worldHashCb_(ctx, s.worldHashData_);
}

// ---------- 数据收集 ----------
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::collectPendingSubmissions(SnapshotData& out) const {
    // 短延迟环：drain + 原序回填（tick 边界单消费者，安全；语义上非变异 → const_cast）
    if constexpr (options.enableShortDelayQueue) {
        std::vector<HierarchicalWheel::Task> drained;
        for (size_t slot = 0; slot < options.shortDelayRingSize; ++slot) {
            auto& q = const_cast<moodycamel::ConcurrentQueue<HierarchicalWheel::Task>&>(
                runtime_.state().shortDelaySlots_[slot]);
            HierarchicalWheel::Task t;
            while (q.try_dequeue(t)) {
                SnapshotSubmission s;
                s.task = taskRegistry_.idToKeyAt(t.ownerTaskId);   // 名字由 TaskID 解析（等价于原 t.key）
                s.action = actionNameOf(t.actionId);
                s.executeTick = t.executeTick;
                s.submitTick = t.submitTick;
                s.payload = actionRegistry_.writePayload(t.actionId, t.payload.get());
                out.submissions.push_back(std::move(s));
                drained.push_back(std::move(t));
            }
            for (auto& dt : drained) q.enqueue(std::move(dt));
            drained.clear();
        }
    }
    // 层级时间轮：只读遍历
    runtime_.state().delayedWheel_.visitAll([&](const HierarchicalWheel::Task& t) {
        SnapshotSubmission s;
        s.task = taskRegistry_.idToKeyAt(t.ownerTaskId);   // 名字由 TaskID 解析（等价于原 t.key）
        s.action = actionNameOf(t.actionId);
        s.executeTick = t.executeTick;
        s.submitTick = t.submitTick;
        s.payload = actionRegistry_.writePayload(t.actionId, t.payload.get());
        out.submissions.push_back(std::move(s));
    });
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
SnapshotData TickPool<Duration, OptionsT, ThreadPoolType>::buildSnapshotData(bool withUserSections) const {
    const auto& state = runtime_.state();
    SnapshotData d;
    d.schemaVersion = state.schemaVersion_;
    d.tickCount = state.tickCount_.load(std::memory_order_acquire);
    d.timeScale = state.timeScale_.load(std::memory_order_acquire);
    // JSON 调试兼容：tasks（旧 toString 格式）
    for (TaskID id = 0; id < state.tickHotTable_.size(); ++id) {
        SnapshotTaskInfo info;
        info.key = taskRegistry_.idToKeyAt(id);
        info.pendingAsync = state.asyncHotTable_[id].asyncSize
            ? state.asyncHotTable_[id].asyncSize(state.tickHotTable_[id].asyncQueueRaw) : 0;
        d.tasks.push_back(std::move(info));
    }
    collectPendingSubmissions(d);
    if (withUserSections) {
        auto pushScenario = [&](SnapshotScenario sc, const typename RuntimeState<Duration, OptionsT, ThreadPoolType>::ScenarioHooks& h) {
            if (!h.save) return;
            Context ctx;
            ctx.pool = const_cast<TickPool*>(this);
            ctx.tick = d.tickCount;
            SnapshotUserSection u;
            u.scenario = static_cast<uint8_t>(sc);
            u.data = h.save(ctx, h.userData);
            d.userSections.push_back(std::move(u));
        };
        pushScenario(SnapshotScenario::FileSave, state.fileSaveHooks_);
        pushScenario(SnapshotScenario::Rollback, state.rollbackHooks_);
        pushScenario(SnapshotScenario::Network, state.networkHooks_);
        pushScenario(SnapshotScenario::Custom, state.customHooks_);
    }
    return d;
}

// ---------- 清场 ----------
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::clearPendingSubmissions() {
    runtime_.state().delayedWheel_.clear();
    if constexpr (options.enableShortDelayQueue) {
        for (auto& q : runtime_.state().shortDelaySlots_) {
            HierarchicalWheel::Task t;
            while (q.try_dequeue(t)) {}
        }
    }
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::clearAsyncQueues() {
    auto& state = runtime_.state();
    for (TaskID id = 0; id < state.tickHotTable_.size(); ++id) {
        auto& async = state.asyncHotTable_[id];
        if (async.asyncClear) async.asyncClear(state.tickHotTable_[id].asyncQueueRaw);
    }
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::abortAndReset() {
    auto& state = runtime_.state();
    state.running_.store(false, std::memory_order_release);
    state.timeCV_.notify_all();
    threadPool_->waitIdle();          // 等待在途 worker 任务结束
    clearPendingSubmissions();
    clearAsyncQueues();               // 决策 4：恢复后异步清空
    runtime_.clearSubmitDedup();          // 去重表是 tick 内瞬态状态，清场一并作废
    runtime_.clearAsyncMergeOverrides();  // 异步合并覆盖回落定义期策略
#if TICKPOOL_ENABLE_NETWORK
    clearCommandLog();
#endif
    state.roundRobin_.store(0, std::memory_order_relaxed);
    state.seqCounter_.store(0, std::memory_order_relaxed);
    state.arena_.reset();
    state.startWallTime_ = std::chrono::steady_clock::now();
}

// ---------- 恢复 ----------
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::applySnapshotData(const SnapshotData& data, bool withUserSections) {
    auto& state = runtime_.state();

    // 1. 中止当前执行
    abortAndReset();

    // 2. schemaVersion 迁移
    if (state.schemaVersion_ != data.schemaVersion && state.onSchemaMigrateCb_) {
        Context ctx;
        ctx.pool = this;
        ctx.tick = data.tickCount;
        if (!state.onSchemaMigrateCb_(ctx, data.schemaVersion, state.schemaVersion_, state.onSchemaMigrateData_))
            throw std::runtime_error("loadSnapshot: schema migration rejected (" +
                std::to_string(data.schemaVersion) + " -> " + std::to_string(state.schemaVersion_) + ")");
    }

    // 3. tickCount 恢复
    state.tickCount_ = data.tickCount;

    // 4. 恢复 pending submissions（异步永不入快照——决策 4）
    Context migCtx;
    migCtx.pool = this;
    migCtx.tick = data.tickCount;
    for (const auto& s : data.submissions) {
        TaskKey key = s.task;
        TaskID id = 0;
        try {
            id = taskRegistry_.taskId(key);
        }
        catch (const std::out_of_range&) {
            if (!state.onUnknownActionCb_ || !state.onUnknownActionCb_(migCtx, s.task.c_str(), s.action.c_str(), data.schemaVersion, state.onUnknownActionData_))
                throw std::runtime_error("loadSnapshot: unknown task \"" + s.task + "\" (action \"" + s.action + "\")");
            continue;
        }
        std::string full = s.task + "." + s.action;
        if (!actionRegistry_.hasAction(full)) {
            if (!state.onUnknownActionCb_ || !state.onUnknownActionCb_(migCtx, s.task.c_str(), s.action.c_str(), data.schemaVersion, state.onUnknownActionData_))
                throw std::runtime_error("loadSnapshot: unknown action \"" + full + "\"");
            continue;
        }
        uint32_t actionId = actionRegistry_.actionId(full);
        // payload 反序列化（codec：byte-blit / 内置 / Write-Read / 报错）
        ::Payload p = actionRegistry_.makePayloadFrom(actionId, s.payload);
        // submitTick 重置为当前 tickCount；executeTick ≥ 当前（== 表示下一个 tick 到期，合法）
        if (s.executeTick < data.tickCount)
            throw std::runtime_error("loadSnapshot: submission executeTick in the past (task \"" + s.task + "\")");
        SubmitOptions opts;
        opts.delay = static_cast<size_t>(s.executeTick - data.tickCount);
        runtime_.scheduleTick(id, actionId, allocSeq(), std::move(p), opts);
    }

    // 5. 用户段恢复（只写已注册场景 → 按 scenario id 分派；未注册场景跳过）
    if (withUserSections) {
        for (const auto& u : data.userSections) {
            Context ctx;
            ctx.pool = this;
            ctx.tick = data.tickCount;
            const auto* hooks = scenarioHooksOf(static_cast<SnapshotScenario>(u.scenario));
            if (hooks && hooks->load) hooks->load(ctx, u.data, hooks->userData);
        }
    }
}

// ---------- 公共保存/加载 ----------
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
Bytes TickPool<Duration, OptionsT, ThreadPoolType>::savePoolState() const {
    return encodeSnapshot(buildSnapshotData(false));
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::loadPoolState(const Bytes& bytes) {
    requireSafeImportPoint("loadPoolState");          // M22：tick 内抛错 + Debug 断言非 run 线程
    SnapshotData d;
    decodeSnapshot(bytes, d, options.snapshotCompat);
    applySnapshotData(d, false);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
Bytes TickPool<Duration, OptionsT, ThreadPoolType>::exportSnapshot() const {
    return encodeSnapshot(buildSnapshotData(true));
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::importSnapshot(const Bytes& bytes) {
    requireSafeImportPoint("importSnapshot");        // M22
    SnapshotData d;
    decodeSnapshot(bytes, d, options.snapshotCompat);
    applySnapshotData(d, true);
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
bool TickPool<Duration, OptionsT, ThreadPoolType>::saveSnapshotToFile(const std::string& path, bool includeRollbackInfo) const {
#if TICKPOOL_SNAPSHOT_EXTERNAL_FILE
    (void)path; (void)includeRollbackInfo;
    throw std::runtime_error("saveSnapshotToFile disabled under TICKPOOL_SNAPSHOT_EXTERNAL_FILE — "
        "take the bytes via exportSnapshot() and persist them yourself");
#else
    SnapshotData d = buildSnapshotData(true);
    // 随档保存回滚信息（默认保存），避免存档出问题后无法回滚成废档
    if (includeRollbackInfo && rollbackMaxFrames_ > 0) {
        for (const auto& f : rollbackFrames_) d.rollbackFrames.push_back(f.data);
    }
    Bytes bytes = encodeSnapshot(d);
    FILE* f = nullptr;
    if (fopen_s(&f, path.c_str(), "wb") != 0 || !f) return false;
    bool ok = std::fwrite(bytes.data(), 1, bytes.size(), f) == bytes.size();
    std::fclose(f);
    return ok;
#endif
}
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
bool TickPool<Duration, OptionsT, ThreadPoolType>::loadSnapshotFromFile(const std::string& path) {
#if TICKPOOL_SNAPSHOT_EXTERNAL_FILE
    (void)path;
    throw std::runtime_error("loadSnapshotFromFile disabled under TICKPOOL_SNAPSHOT_EXTERNAL_FILE — "
        "read the bytes yourself and call importSnapshot() / loadPoolState()");
#else
    // bool 唯一语义 = "文件不存在/无法打开"（常见预期）；
    requireSafeImportPoint("loadSnapshotFromFile");  // M22
    FILE* f = nullptr;
    if (fopen_s(&f, path.c_str(), "rb") != 0 || !f) return false;
    std::fseek(f, 0, SEEK_END);
    long size = std::ftell(f);
    std::fseek(f, 0, SEEK_SET);
    Bytes bytes(static_cast<size_t>(size));
    bool readOk = std::fread(bytes.data(), 1, bytes.size(), f) == bytes.size();
    std::fclose(f);
    if (!readOk) throw std::runtime_error("loadSnapshotFromFile: failed to read file \"" + path + "\"");
    SnapshotData d;
    decodeSnapshot(bytes, d, options.snapshotCompat);
    applySnapshotData(d, true);
    rebuildRollbackRing(d.rollbackFrames);   // 随档恢复回滚环
    return true;
#endif
}

// ======================================================================

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
SnapshotData TickPool<Duration, OptionsT, ThreadPoolType>::buildRollbackFrameData() const {
    // 帧 = 池状态 + 仅 Rollback 场景段
    SnapshotData d = buildSnapshotData(false);
    const auto& hooks = runtime_.state().rollbackHooks_;
    if (hooks.save) {
        Context ctx;
        ctx.pool = const_cast<TickPool*>(this);
        ctx.tick = d.tickCount;
        SnapshotUserSection u;
        u.scenario = static_cast<uint8_t>(SnapshotScenario::Rollback);
        u.data = hooks.save(ctx, hooks.userData);
        d.userSections.push_back(std::move(u));
    }
    return d;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::captureRollbackFrame() {
    SnapshotData d = buildRollbackFrameData();
    RollbackFrame f;
    f.tick = d.tickCount;
    f.data = encodeSnapshot(d);
    rollbackFrames_.push_back(std::move(f));
    if (rollbackFrames_.size() > rollbackMaxFrames_)
        rollbackFrames_.pop_front();
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::setRollbackBuffer(size_t capacity, size_t everyTicks) {
#if TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK
    // 用户自管回滚：框架不采样也不维护回滚环。
    (void)capacity; (void)everyTicks;
    throw std::runtime_error("setRollbackBuffer disabled under TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK — "
        "the framework does not maintain a rollback ring in this mode; keep your own snapshots");
#else
    if (capacity == 0) {   // 禁用
        rollbackMaxFrames_ = 0;
        rollbackFrames_.clear();
        return;
    }
    // Rollback 场景钩子（world load）必须已注册，否则回滚无法恢复世界
    if (!runtime_.state().rollbackHooks_.load)
        throw std::runtime_error("setRollbackBuffer: Rollback scenario hook (world load) not registered — "
            "call setSnapshotCallbacks({ .rollback = { .save=..., .load=..., .userData=... } }) first");
    rollbackEveryTicks_ = std::max<size_t>(1, everyTicks);
    // 容量 = 帧数；策略由 options.rollbackCapacityMode 决定（User 指定 / Auto 按回滚窗口）
    if constexpr (options.rollbackCapacityMode == DefaultTickPoolOptions::RollbackCapacityMode::User)
        rollbackMaxFrames_ = capacity;
    else
        rollbackMaxFrames_ = std::max<size_t>(1, options.rollbackAutoWindowTicks / rollbackEveryTicks_);
    rollbackFrames_.clear();
    nextRollbackTick_ = 0;
#endif   // TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
bool TickPool<Duration, OptionsT, ThreadPoolType>::rollbackTo(size_t ticksAgo) {
    // 非法调用上下文（tick 内）必须**早于下面任何状态改动**被拦下 —— 否则会先把后段帧 erase，
    // 再在 importSnapshot 里抛错，留下被改了一半的回滚环。
    requireSafeImportPoint("rollbackTo");
    if (rollbackMaxFrames_ == 0 || rollbackFrames_.empty()) return false;
    const uint64_t cur = runtime_.state().tickCount_.load(std::memory_order_acquire);
    const uint64_t target = (cur > ticksAgo) ? (cur - ticksAgo) : 0;
    // tick ≤ target 的最近帧；越界钳位到最旧帧
    auto it = rollbackFrames_.begin();
    while (it != rollbackFrames_.end() && it->tick <= target) ++it;
    if (it != rollbackFrames_.begin()) --it;
    // 丢弃目标帧之后（更晚）的帧，环从目标点重建
    rollbackFrames_.erase(std::next(it), rollbackFrames_.end());
    const RollbackFrame chosen = *it;
    importSnapshot(chosen.data);   // 复用恢复流程（立即中止 + 重建）
    return true;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
bool TickPool<Duration, OptionsT, ThreadPoolType>::rollbackLast() {
    return rollbackTo(0);
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::rebuildRollbackRing(const std::vector<Bytes>& frames) {
#if TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK
    // 用户自管回滚：**必须**在此拒绝随档回滚帧，否则 loadSnapshotFromFile 会把环重新启用
    // （下面那行 rollbackMaxFrames_ = frames.size() 正是启用点），从而悄悄违背该模式。
    (void)frames;
#else
    rollbackFrames_.clear();
    if (frames.empty()) return;
    rollbackMaxFrames_ = frames.size();
    for (const auto& fb : frames) {
        SnapshotData fd;
        decodeSnapshot(fb, fd, options.snapshotCompat);
        rollbackFrames_.push_back({ fd.tickCount, fb });
    }
    // 从相邻帧 tick 差推断原采样间隔（保持恢复后的采样节奏）
    rollbackEveryTicks_ = 1;
    if (rollbackFrames_.size() >= 2 && rollbackFrames_[1].tick > rollbackFrames_[0].tick)
        rollbackEveryTicks_ = static_cast<size_t>(rollbackFrames_[1].tick - rollbackFrames_[0].tick);
    nextRollbackTick_ = 0;
#endif   // TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK
}

// ======================================================================
#if TICKPOOL_ENABLE_NETWORK

// ---------- 命令日志 ----------
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
uint64_t TickPool<Duration, OptionsT, ThreadPoolType>::appendCommandLog(
    uint64_t submitTick, uint64_t executeTick, TaskID taskId, uint32_t actionId, const Payload& payload) {
    auto& state = runtime_.state();
    // payload.clone() 提到锁外 —— 它与顺序无关（是独立的深拷贝），而这是默认并行提交

    // 提交者时日志使每命令多付 ~2.2µs。）
    ::Payload logCopy = payload.clone();
    uint64_t seq;
    {
        std::lock_guard lock(state.commandLogMutex_);
        if constexpr (options.enableDeterministicOrdering)
            seq = state.seqCounter_.fetch_add(1, std::memory_order_relaxed);
        else
            seq = 0;
        state.commandLog_.emplace_back(seq, submitTick, executeTick, taskId, actionId, std::move(logCopy));
    }
    return seq;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::clearCommandLog() {
    auto& state = runtime_.state();
    std::lock_guard lock(state.commandLogMutex_);
    state.commandLog_.clear();
    state.commandLogCursor_ = 0;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
Bytes TickPool<Duration, OptionsT, ThreadPoolType>::exportCommands() const {
    if constexpr (!options.enableCommandLog) return {};
    auto& state = runtime_.state();
    // 阶段 1（锁内）：把 [cursor, 尾) 移出到 staging，然后**只回收尺寸、保留容量**。

    // 可能触发页回收），且让日志从零容量重新增长，两件事都发生在所有 construct worker
    size_t firstIdx = 0;
    std::vector<typename RuntimeState<Duration, OptionsT, ThreadPoolType>::CommandEntry> staging;
    {
        std::lock_guard lock(state.commandLogMutex_);
        firstIdx = state.commandLogCursor_;
        if (firstIdx >= state.commandLog_.size()) return {};
        const size_t pending = state.commandLog_.size() - firstIdx;
        staging.reserve(pending);
        for (size_t i = firstIdx; i < state.commandLog_.size(); ++i)
            staging.push_back(std::move(state.commandLog_[i]));
        state.commandLog_.resize(0);      // 容量保留（关键）；已导出前缀一并丢弃
        state.commandLogCursor_ = 0;
    }
    // 阶段 2（锁外，重活）：规范排序 + 名称解析 + payload 网络编码（成本由调用线程承担，不阻塞主循环）
    // 规范序（跨 peer 字节一致）：(executeTick, taskId, seq) 稳定排序 —— executeTick 确定；
    std::stable_sort(staging.begin(), staging.end(),
        [](const typename RuntimeState<Duration, OptionsT, ThreadPoolType>::CommandEntry& a,
           const typename RuntimeState<Duration, OptionsT, ThreadPoolType>::CommandEntry& b) {
            if (a.executeTick != b.executeTick) return a.executeTick < b.executeTick;
            if (a.taskId != b.taskId) return a.taskId < b.taskId;
            return a.seq < b.seq;
        });
    std::vector<NetCommand> cmds;
    cmds.reserve(staging.size());
    for (const auto& e : staging) {
        NetCommand c;
        c.submitTick = e.submitTick;
        c.executeTick = e.executeTick;
        c.task = taskRegistry_.idToKeyAt(e.taskId);
        c.action = actionRegistry_.actionName(e.actionId);
        c.payload = actionRegistry_.writePayloadNet(e.actionId, e.payload.get());
        cmds.push_back(std::move(c));
    }
    return encodeCommands(cmds);
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::importCommands(const Bytes& bytes) {
    std::vector<NetCommand> cmds = decodeCommands(bytes);
    if (cmds.empty()) return;
    auto& state = runtime_.state();
    const uint64_t cur = state.tickCount_.load(std::memory_order_acquire);
    Context migCtx;
    migCtx.pool = this;
    migCtx.tick = cur;
    for (const auto& e : cmds) {
        TaskKey key = e.task;
        TaskID id = 0;
        try {
            id = taskRegistry_.taskId(key);
        }
        catch (const std::out_of_range&) {
            if (!state.onUnknownActionCb_ || !state.onUnknownActionCb_(migCtx, e.task.c_str(), e.action.c_str(), state.schemaVersion_, state.onUnknownActionData_))
                throw std::runtime_error("importCommands: unknown task \"" + e.task + "\" (action \"" + e.action + "\")");
            continue;
        }
        std::string full = e.task + "." + e.action;
        if (!actionRegistry_.hasAction(full)) {
            if (!state.onUnknownActionCb_ || !state.onUnknownActionCb_(migCtx, e.task.c_str(), e.action.c_str(), state.schemaVersion_, state.onUnknownActionData_))
                throw std::runtime_error("importCommands: unknown action \"" + full + "\"");
            continue;
        }
        uint32_t actionId = actionRegistry_.actionId(full);
        ::Payload p = actionRegistry_.makePayloadFromNet(actionId, e.payload);
        // 安全点语义：导入须在 tick 边界前；executeTick < 当前 → 过期命令，报错（不静默丢弃）
        if (e.executeTick < cur)
            throw std::runtime_error("importCommands: command executeTick in the past (task \"" + e.task +
                "\" executeTick " + std::to_string(e.executeTick) + " < current " + std::to_string(cur) + ")");
        // b：**tick 内**导入一条目标为"当前 tick"的命令 → 同样永远不可能执行：本 tick 的短延迟环
        if (e.executeTick == cur && TickRuntime<Duration, OptionsT, ThreadPoolType>::inTick())
            throw std::runtime_error("importCommands: command targets the currently executing tick "
                "(executeTick " + std::to_string(e.executeTick) + ") while called from inside a tick callback — "
                "it could never run (this tick's short-delay ring slot is already drained); inject it at a tick "
                "boundary, or use executeTick > current");
        SubmitOptions opts;
        opts.delay = static_cast<size_t>(e.executeTick - cur);
        runtime_.scheduleTick(id, actionId, allocSeq(), std::move(p), opts);
    }
}

// ---------- 世界传输（external：仅池；managed：池 + network 场景钩子段） ----------
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::collectPendingSubmissionsNet(NetWorldData& out) const {
    auto pushTask = [&](const HierarchicalWheel::Task& t) {
        NetCommand s;
        s.task = taskRegistry_.idToKeyAt(t.ownerTaskId);   // 名字由 TaskID 解析（等价于原 t.key）
        s.action = actionNameOf(t.actionId);
        s.executeTick = t.executeTick;
        s.submitTick = t.submitTick;
        s.payload = actionRegistry_.writePayloadNet(t.actionId, t.payload.get());
        out.submissions.push_back(std::move(s));
    };
    // 短延迟环：drain + 原序回填（quiescent 下安全：调用方持有 tickGate_ unique 锁）
    if constexpr (options.enableShortDelayQueue) {
        std::vector<HierarchicalWheel::Task> drained;
        for (size_t slot = 0; slot < options.shortDelayRingSize; ++slot) {
            auto& q = const_cast<moodycamel::ConcurrentQueue<HierarchicalWheel::Task>&>(
                runtime_.state().shortDelaySlots_[slot]);
            HierarchicalWheel::Task t;
            while (q.try_dequeue(t)) {
                pushTask(t);
                drained.push_back(std::move(t));
            }
            for (auto& dt : drained) q.enqueue(std::move(dt));
            drained.clear();
        }
    }
    // 层级时间轮：只读遍历
    runtime_.state().delayedWheel_.visitAll(pushTask);
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
NetWorldData TickPool<Duration, OptionsT, ThreadPoolType>::buildNetWorldData(bool withNetworkUserSection) const {
    const auto& state = runtime_.state();
    NetWorldData d;
    d.schemaVersion = state.schemaVersion_;
    d.tickCount = state.tickCount_.load(std::memory_order_acquire);
    collectPendingSubmissionsNet(d);
    if (withNetworkUserSection) {
        const auto& h = state.networkHooks_;
        if (h.save) {
            Context ctx;
            ctx.pool = const_cast<TickPool*>(this);
            ctx.tick = d.tickCount;
            NetUserSection u;
            u.scenario = static_cast<uint8_t>(SnapshotScenario::Network);
            u.data = h.save(ctx, h.userData);
            d.userSections.push_back(std::move(u));
        }
    }
    return d;
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
Bytes TickPool<Duration, OptionsT, ThreadPoolType>::exportPoolData() const {
    if (TickRuntime<Duration, OptionsT, ThreadPoolType>::inTick())
        throw std::runtime_error("exportPoolData: cannot be called from within executeTick (deadlock guard); call from another thread");
    std::unique_lock gate(runtime_.state().tickGate_);   // quiescent：等待在途 tick，阻止新 tick
    return encodeWorldPool(buildNetWorldData(false));
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
Bytes TickPool<Duration, OptionsT, ThreadPoolType>::exportWorldState() const {
#if TICKPOOL_SNAPSHOT_EXTERNAL_NETWORK
    // 用户自管世界传输：框架只给纯池数据，世界字节由用户自定
    throw std::runtime_error("exportWorldState disabled under TICKPOOL_SNAPSHOT_EXTERNAL_NETWORK — "
        "use exportPoolData() for the pool bytes and serialize your world yourself");
#else
    if (TickRuntime<Duration, OptionsT, ThreadPoolType>::inTick())
        throw std::runtime_error("exportWorldState: cannot be called from within executeTick (deadlock guard); call from another thread");
    std::unique_lock gate(runtime_.state().tickGate_);   // quiescent：内容 = 最近完成的 tick 边界
    return encodeWorldPool(buildNetWorldData(true));
#endif
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::applyNetWorldData(const NetWorldData& data, bool withUserSections) {
    auto& state = runtime_.state();

    // 1. 立即中止当前执行并清场（同 importSnapshot 语义；含命令日志清空）
    abortAndReset();

    // 2. schemaVersion 迁移（单级：from = 快照版本，to = 当前）
    if (state.schemaVersion_ != data.schemaVersion && state.onSchemaMigrateCb_) {
        Context ctx;
        ctx.pool = this;
        ctx.tick = data.tickCount;
        if (!state.onSchemaMigrateCb_(ctx, data.schemaVersion, state.schemaVersion_, state.onSchemaMigrateData_))
            throw std::runtime_error("importWorld: schema migration rejected (" +
                std::to_string(data.schemaVersion) + " -> " + std::to_string(state.schemaVersion_) + ")");
    }

    // 3. tickCount 恢复
    state.tickCount_ = data.tickCount;

    // 4. 恢复 pending submissions（网络协议 payload；executeTick ≥ 当前）
    Context migCtx;
    migCtx.pool = this;
    migCtx.tick = data.tickCount;
    for (const auto& s : data.submissions) {
        TaskKey key = s.task;
        TaskID id = 0;
        try {
            id = taskRegistry_.taskId(key);
        }
        catch (const std::out_of_range&) {
            if (!state.onUnknownActionCb_ || !state.onUnknownActionCb_(migCtx, s.task.c_str(), s.action.c_str(), data.schemaVersion, state.onUnknownActionData_))
                throw std::runtime_error("importWorld: unknown task \"" + s.task + "\" (action \"" + s.action + "\")");
            continue;
        }
        std::string full = s.task + "." + s.action;
        if (!actionRegistry_.hasAction(full)) {
            if (!state.onUnknownActionCb_ || !state.onUnknownActionCb_(migCtx, s.task.c_str(), s.action.c_str(), data.schemaVersion, state.onUnknownActionData_))
                throw std::runtime_error("importWorld: unknown action \"" + full + "\"");
            continue;
        }
        uint32_t actionId = actionRegistry_.actionId(full);
        ::Payload p = actionRegistry_.makePayloadFromNet(actionId, s.payload);
        if (s.executeTick < data.tickCount)
            throw std::runtime_error("importWorld: submission executeTick in the past (task \"" + s.task + "\")");
        SubmitOptions opts;
        opts.delay = static_cast<size_t>(s.executeTick - data.tickCount);
        runtime_.scheduleTick(id, actionId, allocSeq(), std::move(p), opts);
    }

    // 5. 用户段恢复（按 scenario id 分派；未注册场景跳过）
    if (withUserSections) {
        for (const auto& u : data.userSections) {
            Context ctx;
            ctx.pool = this;
            ctx.tick = data.tickCount;
            const auto* hooks = scenarioHooksOf(static_cast<SnapshotScenario>(u.scenario));
            if (hooks && hooks->load) hooks->load(ctx, u.data, hooks->userData);
        }
    }
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::importPoolData(const Bytes& bytes) {
    requireSafeImportPoint("importPoolData");        // M22
    NetWorldData d = decodeWorldPool(bytes);
    applyNetWorldData(d, false);
}

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
void TickPool<Duration, OptionsT, ThreadPoolType>::importWorldState(const Bytes& bytes) {
#if TICKPOOL_SNAPSHOT_EXTERNAL_NETWORK
    (void)bytes;
    throw std::runtime_error("importWorldState disabled under TICKPOOL_SNAPSHOT_EXTERNAL_NETWORK — "
        "use importPoolData() for the pool bytes and restore your world yourself");
#else
    requireSafeImportPoint("importWorldState");     // M22
    NetWorldData d = decodeWorldPool(bytes);
    applyNetWorldData(d, true);
#endif
}
#endif   // TICKPOOL_ENABLE_NETWORK
