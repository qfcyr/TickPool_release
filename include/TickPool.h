#pragma once
#include "FnWrapper.h"
#include "TickPayload.h"
#include "TickPoolAssert.h"
// ========== 快照格式编解码 ==========
#include "SnapshotCodec.h"
#include "ThreadPool.h"
#include <chrono>
#include <atomic>
#include <functional>
#include <variant>
#include <unordered_map>
#include <deque>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <vector>
#include <optional>
#include <any>
#include <memory>
#include <queue>
#include <concepts>
#include <type_traits>
#include <typeindex>
#include <concurrentqueue/moodycamel/concurrentqueue.h>
#include <ankerl/unordered_dense.h>

// ========== 编译期开关 ==========
#ifndef TICKPOOL_ENABLE_STATS
#  ifdef NDEBUG
#    define TICKPOOL_ENABLE_STATS 0
#  else
#    define TICKPOOL_ENABLE_STATS 1
#  endif
#endif

#ifndef TICKPOOL_ENABLE_TYPE_CHECK
#  ifdef NDEBUG
#    define TICKPOOL_ENABLE_TYPE_CHECK 0
#  else
#    define TICKPOOL_ENABLE_TYPE_CHECK 1
#  endif
#endif

// 小波次内联阈值默认值（见 DefaultTickPoolOptions::inlineWaveMaxTasks）。

// （TickConstructionTask/TickDestructionTask）由引擎注入且无行为，判定时已扣除。
#ifndef TICKPOOL_INLINE_WAVE_MAX_TASKS
#  define TICKPOOL_INLINE_WAVE_MAX_TASKS 1
#endif

// ========== ：分段计时 Profile（默认关闭） ==========

// 分段计时快照（纯数据）。除 fnHeapAllocs 为进程级累计外，其余均为该池自启动以来的累计值。
struct TickProfileSnapshot {
    uint64_t ticks = 0;            // 已统计的 tick 数
    uint64_t wheelNs = 0;          // processDelayedWheel（时间轮 + 短延迟环收集）
    uint64_t constructNs = 0;      // 构造阶段等待（含 worker 上构造的执行时间）
    uint64_t subtaskNs = 0;        // 并行子任务等待
    uint64_t destructNs = 0;       // 析构阶段（run 线程直跑 + OverlapParallel 等待）
    uint64_t inlineNs = 0;         // 内联波次（construct+子任务+destruct 全在 run 线程）
    uint64_t sleepNs = 0;          // tick 末尾的墙钟对齐等待
    uint64_t sortNs = 0;           // 结果按 seq 排序（含 is_sorted 短路）
    uint64_t tickTotalNs = 0;      // executeTick 全body
    uint64_t commands = 0;         // 并行提交命令数
    uint64_t subtasks = 0;         // 实际执行的并行子任务数
    uint64_t waves = 0;            // 处理的波次数
    uint64_t inlinedWaves = 0;     // 其中被内联执行的波次数
    uint64_t fnHeapAllocs = 0;     // 进程级 Fn 堆分配次数（证明 SBO 是否生效）
};

#if TICKPOOL_ENABLE_PROFILE
namespace tickpool_detail {
    // 注：g_fnHeapAllocs 定义在 FnWrapper.h（它在本头文件之前被包含）
    struct ProfileScope {
        std::atomic<uint64_t>* acc;
        std::chrono::steady_clock::time_point t0;
        explicit ProfileScope(std::atomic<uint64_t>* a) noexcept : acc(a) {
            if (acc) t0 = std::chrono::steady_clock::now();
        }
        ProfileScope(const ProfileScope&) = delete;
        ProfileScope& operator=(const ProfileScope&) = delete;
        ~ProfileScope() {
            if (!acc) return;
            acc->fetch_add(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - t0).count()),
                std::memory_order_relaxed);
        }
    };
}
#  define TICKPOOL_PROF_ACC(field)  (&state_.prof_.field)
#  define TICKPOOL_PROF_INC(field)  (state_.prof_.field.fetch_add(1, std::memory_order_relaxed))
#  define TICKPOOL_PROF_ADD(field, v) (state_.prof_.field.fetch_add((v), std::memory_order_relaxed))
#  define TICKPOOL_PROFILE_SCOPE(pacc) ::tickpool_detail::ProfileScope _tickpoolProf_##__LINE__(pacc)
#else
#  define TICKPOOL_PROF_ACC(field)  (nullptr)
#  define TICKPOOL_PROF_INC(field)  ((void)0)
#  define TICKPOOL_PROF_ADD(field, v) ((void)0)
#  define TICKPOOL_PROFILE_SCOPE(pacc) ((void)0)
#endif

// ========== Concepts ==========
template<typename T>
struct is_duration : std::false_type {};
template<typename Rep, typename Period>
struct is_duration<std::chrono::duration<Rep, Period>> : std::true_type {};
template<typename T>
inline constexpr bool is_duration_v = is_duration<T>::value;

template<typename D>
concept TickDuration = requires {
    typename D::rep;
    typename D::period;
        requires is_duration_v<D>;
        requires is_valid_duration<D>();
};

template<typename T>
concept ThreadPool = requires(T & pool, size_t idx, std::function<void()> task) {
    { pool.enqueue(idx, std::move(task)) } -> std::same_as<void>;
    { pool.tryExecuteOne(idx) } -> std::same_as<bool>;
    { pool.threadCount() } -> std::same_as<size_t>;
};

// ========== 辅助类型映射 ==========
template<typename T> struct NonVoid { using type = T; };
template<> struct NonVoid<void> { using type = std::monostate; };
template<typename T> using NonVoid_t = typename NonVoid<T>::type;

// ---------- 多生产者单消费者固定容量环形队列（Vyukov 风格，要求容量为2的幂） ----------
template<typename T, size_t Capacity>
class MPSCQueue {
    static_assert((Capacity& (Capacity - 1)) == 0, "Capacity must be a power of two");
    static constexpr size_t kMask = Capacity - 1;

    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    alignas(64) Cell buffer_[Capacity];
    alignas(64) std::atomic<size_t> enqueuePos_{ 0 };
    alignas(64) std::atomic<size_t> dequeuePos_{ 0 };

public:
    MPSCQueue() noexcept {
        for (size_t i = 0; i < Capacity; ++i)
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        enqueuePos_.store(0, std::memory_order_relaxed);
        dequeuePos_.store(0, std::memory_order_relaxed);
    }

    // 多生产者安全的入队，返回成功与否（队列满则失败）
    bool try_enqueue(T&& value) noexcept {
        size_t pos = enqueuePos_.load(std::memory_order_relaxed);
        for (;;) {
            Cell* cell = &buffer_[pos & kMask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            if (seq == pos) {
                // 成功抢占位置
                if (enqueuePos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                    break;
            }
            else if (seq < pos) {
                // 队列已满
                return false;
            }
            else {
                pos = enqueuePos_.load(std::memory_order_relaxed);
            }
        }
        // 写入数据并发布
        buffer_[pos & kMask].data = std::move(value);
        buffer_[pos & kMask].sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    // 单消费者安全的出队
    bool try_dequeue(T& result) noexcept {
        size_t pos = dequeuePos_.load(std::memory_order_relaxed);
        Cell* cell = &buffer_[pos & kMask];
        size_t seq = cell->sequence.load(std::memory_order_acquire);
        if (seq != pos + 1)
            return false;   // 空
        result = std::move(cell->data);
        cell->sequence.store(pos + Capacity, std::memory_order_release);
        dequeuePos_.store(pos + 1, std::memory_order_release);
        return true;
    }

    // 批量出队，返回实际取出数量
    template<typename OutIt>
    size_t try_dequeue_bulk(OutIt dst, size_t limit) noexcept {
        size_t count = 0;
        T tmp;
        while (count < limit && try_dequeue(tmp)) {
            *dst++ = std::move(tmp);
            ++count;
        }
        return count;
    }

    // 在 MPSCQueue 类中增加：
    bool enqueue_drop_oldest(T&& value) noexcept {
        while (true) {
            size_t pos = enqueuePos_.load(std::memory_order_relaxed);
            Cell* cell = &buffer_[pos & kMask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            if (seq == pos) {
                if (enqueuePos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    cell->data = std::move(value);
                    cell->sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
            }
            else if (seq < pos) {
                // 队列满，尝试丢弃队头（最旧元素）
                size_t r = dequeuePos_.load(std::memory_order_acquire);
                // 如果队头未被消费者取走，则尝试推进读指针（丢弃）
                if (r != pos) {
                    size_t newR = r + 1;
                    // 不能越过生产者已经占用的位置
                    if (newR <= pos) {
                        if (dequeuePos_.compare_exchange_weak(r, newR, std::memory_order_acq_rel)) {
                            // 丢弃 = 消费 —— 须按 try_dequeue 的释放协议把被丢 cell
                            buffer_[r & kMask].sequence.store(r + Capacity, std::memory_order_release);
                            continue;   // 成功丢弃一个，继续循环尝试入队
                        }
                    }
                }
                // 如果消费者刚好取走了，则重新尝试写入
            }
            // 其他情况自旋重试
            std::this_thread::yield();
        }
    }

    // 当前大致大小（多生产者版本，近似值）
    size_t size_approx() const noexcept {
        size_t w = enqueuePos_.load(std::memory_order_acquire);
        size_t r = dequeuePos_.load(std::memory_order_acquire);
        return w - r;
    }
};

struct Dependencies { std::vector<std::string> before; std::vector<std::string> after; };
enum class ConstructPolicy { BeforeParallel, OverlapParallel };
enum class DestructPolicy { Sequential, OverlapParallel };
enum class SubmitMode { Tick, Async };
enum class AsyncBackpressurePolicy { DropOldest, DropNewest, Block, Assert };
enum class MergePolicy { Disabled, Aggressive, Conservative };
// MergePolicy —— **提交（任务）级去重的强度**（作用于该任务带 payload 的提交）。
using Hasher = size_t(*)(const void*);
using Equal = bool(*)(const void*, const void*);

struct AsyncMergePolicy {
    size_t maxResultsPerTick = 64; bool enableHashMerge = true; size_t hashMergeThreshold = 1024; bool enableStatefulResults = false;
    void adjustForTimeScale(double timeScale) noexcept {
        if (timeScale < 0.1) { maxResultsPerTick = 32; enableHashMerge = true; hashMergeThreshold = 256; }
        else if (timeScale > 2.0) { maxResultsPerTick = 128; enableHashMerge = timeScale > 5.0; }
    }
};

// ========== TaskDesc 纯配置体归 Builder，此处仅配置） ==========
struct TaskDesc {
    Dependencies deps;
    ConstructPolicy construct = ConstructPolicy::BeforeParallel;
    DestructPolicy destruct = DestructPolicy::Sequential;
    MergePolicy merge = MergePolicy::Aggressive;   // 提交级去重强度（见 MergePolicy 注释；键 = SubmitOptions::hash）
    AsyncMergePolicy asyncMerge;
    Hasher hasher = nullptr;
    Equal equal = nullptr;
};

struct SubmitOptions {
    // 注意：同步/异步不由这里决定 —— 提交模式取自 action 定义期的注册信息
    // （ActionRegistry::isAsync()），匿名提交则走 submitAnonymousAsync。
    size_t delay = 1;

    // 显式提交哈希 = **提交（任务）级去重的键**。同一个 tick 窗口内，
    std::optional<size_t> hash = std::nullopt;

    // 异步结果合并的**提交期覆盖**（tri-state，默认"无意见"以免改变既有行为）：

    //   true    = 强制参与合并/去重（仍需定义期提供 hasher/equal，否则无从比较）
    std::optional<bool> enableAsyncMerge = std::nullopt;
};

struct LogicTime {
    size_t tickCount; std::chrono::nanoseconds time;
    LogicTime operator+(const LogicTime& o) const { return { tickCount + o.tickCount, time + o.time }; }
    LogicTime operator-(const LogicTime& o) const { return { tickCount - o.tickCount, time - o.time }; }
    bool operator<(const LogicTime& o) const { return tickCount < o.tickCount; }
    bool operator==(const LogicTime& o) const { return tickCount == o.tickCount; }
};
struct WallTime {
    std::chrono::steady_clock::time_point time; double timeScale;
    auto elapsedSince(const WallTime& o) const { return std::chrono::duration_cast<std::chrono::nanoseconds>(time - o.time); }
};
struct AsyncQueueStats { size_t pendingResults, mergedResults, droppedResults; double averageWaitTicks; size_t maxQueueSize; };

template<typename T, size_t Capacity = 4096>
class TypedAsyncQueue {
public:
    void enqueue(NonVoid_t<T>&& item) {
        queue_.try_enqueue(std::move(item));
    }
    size_t dequeueBulkTyped(std::vector<NonVoid_t<T>>& output, size_t limit) {
        return queue_.try_dequeue_bulk(std::back_inserter(output), limit);
    }
    bool dropOne() {
        NonVoid_t<T> dummy;
        return queue_.try_dequeue(dummy);
    }
    void enqueue_drop_oldest(NonVoid_t<T>&& item) {
        queue_.enqueue_drop_oldest(std::move(item));
    }
    // Block 背压 —— 阻塞入队（队列满时让出 CPU 重试直到成功；try_enqueue 失败不消费参数）
    void enqueueBlocking(NonVoid_t<T>&& item) {
        while (!queue_.try_enqueue(std::move(item)))
            std::this_thread::yield();
    }
    size_t size_approx() const { return queue_.size_approx(); }
    void clear() {   // 恢复时清空
        NonVoid_t<T> dummy;
        while (queue_.try_dequeue(dummy)) {}
    }
private:
    MPSCQueue<NonVoid_t<T>, Capacity> queue_;
};

using TaskKey = std::string;

struct DefaultTickPoolOptions {
    // ---------- 时间轮 / 短延迟环 ----------
    static constexpr size_t wheelBaseSizePower = 8, wheelMaxLevels = 4, wheelBaseSize = 1ull << wheelBaseSizePower;
    static constexpr bool enableShortDelayQueue = true;
    static constexpr size_t shortDelayThreshold = 16, shortDelayRingSizePower = 6, shortDelayRingSize = 1ull << shortDelayRingSizePower;
    static constexpr size_t asyncQueueCapacity = 4096;
    static constexpr AsyncBackpressurePolicy asyncBackpressure = AsyncBackpressurePolicy::DropOldest;
    // 确定性顺序（提交入队序 seq + 结果按 seq 合并排序）。默认开；关闭则回到到达序（非确定性，仅性能敏感场景）
    static constexpr bool enableDeterministicOrdering = true;
    // 快照格式版本兼容策略（Any/Backward/Forward/Strict；决策 9，默认 Backward 向下兼容）
    static constexpr SnapshotCompatPolicy snapshotCompat = SnapshotCompatPolicy::Backward;
    // 回滚环容量策略：User = setRollbackBuffer(capacity) 指定帧数；Auto = tickpool 按回滚窗口自动决定
    enum class RollbackCapacityMode { User, Auto };
    static constexpr RollbackCapacityMode rollbackCapacityMode = RollbackCapacityMode::User;
    static constexpr size_t rollbackAutoWindowTicks = 120;   // Auto 模式：保留最近 N tick 的回滚帧
    enum class ConstructExceptionPolicy { Abort, LogAndContinue, Callback };
    static constexpr ConstructExceptionPolicy onConstructException = ConstructExceptionPolicy::Abort;
    // 命令日志开关（编译期；lockstep 输入记录）。开=提交时 O(1) append + exportCommands 增量导出；
#if TICKPOOL_ENABLE_NETWORK
    static constexpr bool enableCommandLog = true;
#else
    static constexpr bool enableCommandLog = false;
#endif

    // 不超过该值时，由 run 线程**就地串行执行**，不入队、不唤醒 worker。

    // 单任务波次与「只有哨兵的空池波次」是典型命中场景。

    // 可按池通过派生 OptionsT 覆盖（同 enableCommandLog 的模式）。
    static constexpr uint32_t inlineWaveMaxTasks = TICKPOOL_INLINE_WAVE_MAX_TASKS;
};

static_assert((DefaultTickPoolOptions::shortDelayRingSize& (DefaultTickPoolOptions::shortDelayRingSize - 1)) == 0,
    "shortDelayRingSize must be a power of two");

// ========== 静态执行描述 ==========
#include "ExecutionPlan.h"

// ========== 定义侧注册表 ==========
#include "TaskRegistry.h"

template<typename T>
void TypedQueueDeleter(void* ptr) noexcept {
    delete static_cast<T*>(ptr);
}

// ========== 分层时间轮 ==========
class HierarchicalWheel {
public:
    HierarchicalWheel(size_t baseSize, size_t maxLevels)
        : baseSize_(baseSize),
        maxLevels_(maxLevels),
        slotCount_(baseSize* maxLevels)
    {
        periods_.resize(maxLevels_);
        periods_[0] = 1;
        for (size_t i = 1; i < maxLevels_; ++i)
            periods_[i] = periods_[i - 1] * baseSize_;

        slots_.resize(slotCount_);
        mutexes_.reserve(slotCount_);
        for (size_t i = 0; i < slotCount_; ++i) {
            mutexes_.push_back(std::make_unique<std::mutex>());
        }
    }

    struct Task {
        size_t executeTick;
        size_t submitTick;
        uint32_t ownerTaskId;
        // 可序列化提交记录（替换原 Fn）：行为经 ActionRegistry 按 actionId 解析
        uint32_t actionId;
        uint64_t seq;                 // 确定性：提交入队序（enableDeterministicOrdering 关闭时为 0）
        Payload payload;              // 类型擦除提交参数（SBO+堆）
        // 不再携带 TaskKey（std::string）。执行路径只用 ownerTaskId/actionId/payload；

        Task() : executeTick(0), submitTick(0), ownerTaskId(0), actionId(0), seq(0) {}
        Task(size_t et, size_t st, uint32_t oid, uint32_t aid, uint64_t s, Payload&& p)
            : executeTick(et), submitTick(st), ownerTaskId(oid), actionId(aid), seq(s), payload(std::move(p)) {}

        Task(const Task&) = delete;
        Task& operator=(const Task&) = delete;
        Task(Task&&) noexcept = default;
        Task& operator=(Task&&) noexcept = default;
    };

    void addTask(size_t currentTick, size_t delayTicks, Task&& task) {
        addTaskAtTarget(currentTick, currentTick + delayTicks, std::move(task));
    }

    void addTaskAtTarget(size_t currentTick, size_t targetTick, Task&& task) {
        if (targetTick < currentTick) return;
        size_t diff = targetTick - currentTick;
        size_t level = 0;
        while (level + 1 < maxLevels_ && diff >= periods_[level + 1])
            ++level;
        size_t slot = (targetTick / periods_[level]) % baseSize_;
        size_t idx = level * baseSize_ + slot;
        std::lock_guard lock(*mutexes_[idx]);
        slots_[idx].push_back(std::move(task));
    }

    // 只读遍历全部槽（快照收集；逐槽加锁拷贝，不跨槽持锁）
    template<typename Visitor>
    void visitAll(Visitor&& visitor) const {
        for (size_t idx = 0; idx < slotCount_; ++idx) {
            std::lock_guard lock(*mutexes_[idx]);
            for (const auto& t : slots_[idx]) visitor(t);
        }
    }

    // 清空全部槽（恢复重建前）
    void clear() {
        for (size_t idx = 0; idx < slotCount_; ++idx) {
            std::lock_guard lock(*mutexes_[idx]);
            slots_[idx].clear();
        }
    }

    // 出参版本：避免每 tick 按值返回新 vector；且因 toExecute 现由调用方持有并跨 tick
    void tick(size_t currentTick, std::vector<Task>& toExecute) {
        // 第 0 层当前槽
        size_t slot0 = currentTick % baseSize_;
        size_t idx0 = slot0;
        {
            std::lock_guard lock(*mutexes_[idx0]);
            toExecute.swap(slots_[idx0]);
        }
        // 高层向下 rehash
        for (size_t level = 1; level < maxLevels_; ++level) {
            if (currentTick % periods_[level] == 0) {
                size_t slot = (currentTick / periods_[level]) % baseSize_;
                size_t idx = level * baseSize_ + slot;
                std::lock_guard lock(*mutexes_[idx]);
                auto& bucket = slots_[idx];
                for (auto& task : bucket)
                    addTaskAtTarget(currentTick, task.executeTick, std::move(task));
                bucket.clear();
            }
        }
        // 容错：若取出的任务 executeTick 不匹配，重新插入
        for (auto it = toExecute.begin(); it != toExecute.end(); ) {
            if (it->executeTick != currentTick) {
                addTaskAtTarget(currentTick, it->executeTick, std::move(*it));
                it = toExecute.erase(it);
            }
            else {
                ++it;
            }
        }
    }

private:
    size_t baseSize_;
    size_t maxLevels_;
    size_t slotCount_;
    std::vector<size_t> periods_;
    std::vector<std::vector<Task>> slots_;                     // 扁平为一维 vector
    std::vector<std::unique_ptr<std::mutex>> mutexes_;         // 扁平为一维
};

// ========== 统一的队列注册表 ==========
class BufferRegistry {
public:
    using Deleter = void(*)(void*);

    void* add(std::unique_ptr<void, Deleter> q) {
        void* raw = q.get();
        queues_.push_back(std::move(q));
        return raw;
    }

    void reserve(size_t n) { queues_.reserve(n); }

private:
    std::vector<std::unique_ptr<void, Deleter>> queues_;
};

// ========== TickPool ==========
struct TaskTickHot {
    void* parallelQueueRaw = nullptr;
    void* bufferQueueRaw = nullptr;
    void* asyncQueueRaw = nullptr;
};

// 异步路径热数据
struct TaskAsyncHot {
    void (*asyncEnqueue)(void* typedQueue, void* result, uint64_t seq) noexcept = nullptr;
    void (*asyncEnqueueVoid)(void* typedQueue, uint64_t seq) noexcept = nullptr;
    void (*asyncEnqueueForce)(void* typedQueue, void* result, uint64_t seq) noexcept = nullptr;
    void (*asyncEnqueueVoidForce)(void* typedQueue, uint64_t seq) noexcept = nullptr;
    // Block 背压的阻塞入队（队列满时让出 CPU 重试，不静默丢弃）
    void (*asyncEnqueueBlocking)(void* typedQueue, void* result, uint64_t seq) noexcept = nullptr;
    void (*asyncEnqueueVoidBlocking)(void* typedQueue, uint64_t seq) noexcept = nullptr;
    bool (*dropOldestAsync)(void* typedQueuePtr) = nullptr;
    size_t(*asyncSize)(void* typedQueuePtr) = nullptr;
    void (*asyncClear)(void* typedQueuePtr) = nullptr;   // 恢复时清空异步队列
};

// 任务冷数据
struct TaskColdData {
    TaskDesc desc;
    Fn onConstruct;   // 行为（Builder 注册；运行期调用）
    Fn onDestruct;

#if TICKPOOL_ENABLE_TYPE_CHECK
    std::type_index parallelQueueType;
    std::type_index bufferQueueType;
    std::type_index parallelResultType;
    std::type_index asyncResultType;
    std::type_index bufferType;
#endif

    // vector 扩容、SSO 短串被 move 后指针悬垂，异常打印 debugName 即读悬垂；改独立 string）
    std::string debugName;

#if TICKPOOL_ENABLE_STATS
    std::atomic<size_t> mergedResults{ 0 };
    std::atomic<size_t> droppedResults{ 0 };
    std::atomic<size_t> maxQueueSize{ 0 };
    std::atomic<size_t> totalWaitTicks{ 0 };
    std::atomic<size_t> totalResultCount{ 0 };
#endif

    TaskColdData()
#if TICKPOOL_ENABLE_TYPE_CHECK
        : parallelQueueType(typeid(void)),
        bufferQueueType(typeid(void)),
        parallelResultType(typeid(void)),
        asyncResultType(typeid(void)),
        bufferType(typeid(void))
#endif
    {}

    explicit TaskColdData(TaskDesc d)
        : desc(std::move(d)),
        onConstruct(Fn{}),
        onDestruct(Fn{})
#if TICKPOOL_ENABLE_TYPE_CHECK
        , parallelQueueType(typeid(void)),
        bufferQueueType(typeid(void)),
        parallelResultType(typeid(void)),
        asyncResultType(typeid(void)),
        bufferType(typeid(void))
#endif
    {}

    TaskColdData(const TaskColdData&) = delete;
    TaskColdData& operator=(const TaskColdData&) = delete;

    TaskColdData(TaskColdData&& other) noexcept
        : desc(std::move(other.desc)),
        onConstruct(std::move(other.onConstruct)),
        onDestruct(std::move(other.onDestruct)),
#if TICKPOOL_ENABLE_TYPE_CHECK
        parallelQueueType(other.parallelQueueType),
        bufferQueueType(other.bufferQueueType),
        parallelResultType(other.parallelResultType),
        asyncResultType(other.asyncResultType),
        bufferType(other.bufferType),
#endif
        debugName(std::move(other.debugName))
#if TICKPOOL_ENABLE_STATS
        , mergedResults(other.mergedResults.load(std::memory_order_relaxed))
        , droppedResults(other.droppedResults.load(std::memory_order_relaxed))
        , maxQueueSize(other.maxQueueSize.load(std::memory_order_relaxed))
        , totalWaitTicks(other.totalWaitTicks.load(std::memory_order_relaxed))
        , totalResultCount(other.totalResultCount.load(std::memory_order_relaxed))
#endif
    {}

    TaskColdData& operator=(TaskColdData&& other) noexcept {
        if (this != &other) {
            desc = std::move(other.desc);
            onConstruct = std::move(other.onConstruct);
            onDestruct = std::move(other.onDestruct);
#if TICKPOOL_ENABLE_TYPE_CHECK
            parallelQueueType = other.parallelQueueType;
            bufferQueueType = other.bufferQueueType;
            parallelResultType = other.parallelResultType;
            asyncResultType = other.asyncResultType;
            bufferType = other.bufferType;
#endif
            debugName = std::move(other.debugName);
#if TICKPOOL_ENABLE_STATS
            mergedResults.store(other.mergedResults.load(std::memory_order_relaxed), std::memory_order_relaxed);
            droppedResults.store(other.droppedResults.load(std::memory_order_relaxed), std::memory_order_relaxed);
            maxQueueSize.store(other.maxQueueSize.load(std::memory_order_relaxed), std::memory_order_relaxed);
            totalWaitTicks.store(other.totalWaitTicks.load(std::memory_order_relaxed), std::memory_order_relaxed);
            totalResultCount.store(other.totalResultCount.load(std::memory_order_relaxed), std::memory_order_relaxed);
#endif
        }
        return *this;
    }
};

// TickPool 前向声明（TaskContext 需持有类型安全的池指针；默认参数只允许出现在类定义处）
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
class TickPool;

// 通用执行上下文
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
struct TaskContext {
    using Pool = TickPool<Duration, OptionsT, ThreadPoolType>;
    using TaskID = uint32_t;

    Pool* pool = nullptr;              // 当前池（回调内经 ctx.pool() 调框架方法）
    size_t tick = 0;                   // 当前 tick（tick 边界回调亦有效）
    TaskID taskId = UINT32_MAX;        // 当前任务；tick 边界回调为无效值
    uint64_t seq = 0;                  // 本次提交的全局序号（确定性合并排序用）

    void* parallelResultQueue = nullptr;
    void* parallelBufferQueue = nullptr;
#ifndef NDEBUG
    uint32_t debugMagic = 0xDEADBEEF;
#endif

    Pool& owner() const noexcept { return *pool; }
    size_t currentTick() const noexcept { return tick; }
    TaskID currentTask() const noexcept { return taskId; }
};

// ========== 快照场景与用户钩子（方向约定：保存=返回 Bytes，加载=接收 Bytes，userData 末参） ==========
enum class SnapshotScenario : uint8_t { FileSave = 0, Rollback = 1, Network = 2, Custom = 3 };

template<typename Context>
struct SnapshotScenarioHooks {
    Bytes (*save)(Context&, void* userData) = nullptr;          // 保存侧：用户产出字节
    void  (*load)(Context&, const Bytes&, void* userData) = nullptr;  // 加载侧：tickpool 传入字节，用户恢复
};

// 链式注册器：.fileSave<UserData>(hooks, userData) —— userData 类型编译期检查
template<typename Context>
class SnapshotCallbackBuilder {
    using Pool = Context::Pool;
    Pool* pool_ = nullptr;
    explicit SnapshotCallbackBuilder(Pool* p) noexcept : pool_(p) {}
    friend Pool;
public:
    template<typename UserData>
    SnapshotCallbackBuilder& fileSave(SnapshotScenarioHooks<Context> hooks, UserData* userData) {
        pool_->setScenarioHooks(SnapshotScenario::FileSave, hooks.save, hooks.load, userData);
        return *this;
    }
    template<typename UserData>
    SnapshotCallbackBuilder& rollback(SnapshotScenarioHooks<Context> hooks, UserData* userData) {
        pool_->setScenarioHooks(SnapshotScenario::Rollback, hooks.save, hooks.load, userData);
        return *this;
    }
    template<typename UserData>
    SnapshotCallbackBuilder& network(SnapshotScenarioHooks<Context> hooks, UserData* userData) {
        pool_->setScenarioHooks(SnapshotScenario::Network, hooks.save, hooks.load, userData);
        return *this;
    }
    template<typename UserData>
    SnapshotCallbackBuilder& custom(SnapshotScenarioHooks<Context> hooks, UserData* userData) {
        pool_->setScenarioHooks(SnapshotScenario::Custom, hooks.save, hooks.load, userData);
        return *this;
    }
};

// Tick 级单调（bump）分配器。

// 但 `executeTick` 会在**同一条波次内**为每个任务 acquireContext 并保存 Context*，

// 单波次 128 个任务（+2 哨兵）即触发，进程以 0xC0000374 (STATUS_HEAP_CORRUPTION) 崩溃。
struct MonotonicArena {
    static constexpr size_t kBlockBytes = 64 * 1024;   // 每块 64KB（约 1170 个 Context）
    static constexpr size_t kInlineAlign = 64;         // 常见最大对齐；超出则额外留 align 字节

    struct Block {
        std::unique_ptr<uint8_t[]> mem;
        size_t size = 0;
    };

    std::vector<Block> blocks_;   // 只增；vector 扩容只移动 Block 结构，不动其堆内存
    size_t cur_ = 0;              // 当前块下标
    size_t offset_ = 0;           // 当前块内已用字节（相对块基址）
#ifndef NDEBUG
    std::thread::id ownerThread_; // 记录第一次分配的线程ID
    bool threadChecked = false;
#endif

    void reset() noexcept {
        cur_ = 0;
        offset_ = 0;
#ifndef NDEBUG
        threadChecked = false;    // 重置后允许下一个Tick重新分配
#endif
    }

    void* allocate(size_t size, size_t align = alignof(std::max_align_t)) noexcept {
#ifndef NDEBUG
        // 仅在 Debug 下检查：同一 Arena 不能被多个线程并发使用
        if (!threadChecked) {
            ownerThread_ = std::this_thread::get_id();
            threadChecked = true;
        }
        else {
            assert(ownerThread_ == std::this_thread::get_id() &&
                "Arena accessed from multiple threads!");
        }
#endif
        for (;;) {
            if (cur_ >= blocks_.size()) {
                const size_t slack = align > kInlineAlign ? align : kInlineAlign;
                size_t want = kBlockBytes;
                if (size + slack > want) want = size + slack;
                Block nb;
                nb.size = want + slack;
                nb.mem = std::make_unique<uint8_t[]>(nb.size);
                blocks_.push_back(std::move(nb));
                offset_ = 0;
            }
            Block& b = blocks_[cur_];
            // 块基址只由 operator new 保证自然对齐；按实际地址把偏移顶到 align。
            const size_t misalign = reinterpret_cast<uintptr_t>(b.mem.get()) % align;
            size_t off = offset_;
            if (misalign != 0) off += align - misalign;
            if (off + size <= b.size) {
                void* ptr = b.mem.get() + off;
                offset_ = off + size;
                return ptr;
            }
            ++cur_;       // 本块放不下：换下一块（旧块保留，已发出的指针继续有效）
            offset_ = 0;
        }
    }
};

// ========== 运行期状态 ==========
#include "TickRuntimeState.h"

// ========== 编译期 ==========
#include "GraphCompiler.h"

// ========== Action 声明与执行机制 ==========

template<typename F>
inline constexpr bool always_false_v = false;

#include "PayloadCodec.h"     // 定义 Bytes 与 payload codec（ActionDecl 依赖）
#if TICKPOOL_ENABLE_NETWORK
#include "NetworkCodec.h"     // M6：网络协议字节编解码（命令日志 + 世界传输；依赖 PayloadCodec/SnapshotCodec）
#endif

template<typename Callable>
struct ActionWorkBox {
    Callable fn;
    explicit ActionWorkBox(Callable&& f) : fn(std::move(f)) {}
};

// 分派调用：返回行为结果（void 或 Result）
template<typename Fn, typename Ctx, typename Payload>
static auto actionInvoke(Fn& fn, Ctx& ctx, const Payload& p) {
    if constexpr (requires { std::invoke(fn, *ctx.pool, p); }) return std::invoke(fn, *ctx.pool, p);
    else if constexpr (requires { std::invoke(fn, p); }) return std::invoke(fn, p);
    else if constexpr (requires { std::invoke(fn, *ctx.pool); }) return std::invoke(fn, *ctx.pool);
    else if constexpr (requires { std::invoke(fn); }) return std::invoke(fn);
    else static_assert(always_false_v<Fn>,
        "action work callable must accept (pool, payload) / (payload) / (pool) / ()");
}

// 定义期暂存的行为声明（Builder 收集，Commit 时注册进 ActionRegistry）
template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
struct ActionDecl {
    using Context = TaskContext<Duration, OptionsT, ThreadPoolType>;
    std::string name;                       // 短名（注册时拼全名 taskKey.name）
    bool isAsync = false;
    void* box = nullptr;                    // ActionWorkBox<Callable>
    void (*destroyBox)(void*) noexcept = nullptr;
    // 并行：延迟执行，结果入 ctx.parallelResultQueue（类型 = 任务并行结果类型）
    void (*execute)(void* box, Context& ctx, const void* payload) = nullptr;
    // 异步：立即执行，结果入 asyncQueue（类型 = 任务异步结果类型）
    void (*executeAsync)(void* box, Context& ctx, const void* payload, void* asyncQueue) = nullptr;
    std::type_index payloadType = typeid(void);   // PayloadT（void→monostate）
    // payload codec
    Bytes (*writePayload)(const void* payload) = nullptr;
    void (*readPayload)(const Bytes&, void* out) = nullptr;
    // 从序列化字节恢复 payload（构造 PayloadT + 读入 + 包装）
    ::Payload (*makePayloadFromBytes)(const Bytes&) = nullptr;
    // 网络协议 payload codec（可移植：内置/标量/Write-Read；禁止 byte-blit）
    Bytes (*writePayloadNet)(const void* payload) = nullptr;
    void (*readPayloadNet)(const Bytes&, void* out) = nullptr;
    ::Payload (*makePayloadFromNetBytes)(const Bytes&) = nullptr;

    ActionDecl() = default;
    ActionDecl(const ActionDecl&) = delete;
    ActionDecl& operator=(const ActionDecl&) = delete;
    ActionDecl(ActionDecl&& o) noexcept { *this = std::move(o); }
    ActionDecl& operator=(ActionDecl&& o) noexcept {
        if (this != &o) {
            if (box && destroyBox) destroyBox(box);
            name = std::move(o.name);
            isAsync = o.isAsync;
            box = o.box; destroyBox = o.destroyBox;
            execute = o.execute; executeAsync = o.executeAsync;
            payloadType = o.payloadType;
            writePayload = o.writePayload;
            readPayload = o.readPayload;
            makePayloadFromBytes = o.makePayloadFromBytes;
            writePayloadNet = o.writePayloadNet;
            readPayloadNet = o.readPayloadNet;
            makePayloadFromNetBytes = o.makePayloadFromNetBytes;
            o.box = nullptr; o.destroyBox = nullptr;
            o.execute = nullptr; o.executeAsync = nullptr;
            o.writePayload = nullptr; o.readPayload = nullptr; o.makePayloadFromBytes = nullptr;
            o.writePayloadNet = nullptr; o.readPayloadNet = nullptr; o.makePayloadFromNetBytes = nullptr;
        }
        return *this;
    }
    ~ActionDecl() { if (box && destroyBox) destroyBox(box); }
};

// ========== 行为注册表 ==========
#include "ActionRegistry.h"

// ========== 运行期执行器 ==========
// TickRuntime 执行已编译的 ExecutionPlan，持有 RuntimeState 与 TLS。
#include "TickRuntime.h"

template<TickDuration Duration,
    typename OptionsT = DefaultTickPoolOptions,
    ThreadPool ThreadPoolType = WorkStealingThreadPool >
class TickPool {

    // 派生静态成员覆盖不生效 → 全部编译期配置实际无法按池定制；现按类型参数 + 值成员，
    // 派生结构体的 static constexpr 覆盖（如 enableCommandLog=false）可正常生效。
    static_assert(std::is_base_of_v<DefaultTickPoolOptions, OptionsT>,
        "OptionsT must derive from DefaultTickPoolOptions");
public:
    explicit TickPool(Duration tickDuration = Duration(1));
    explicit TickPool(Duration tickDuration, std::unique_ptr<ThreadPoolType> pool);
    ~TickPool();

    // 编译期配置值（派生静态成员覆盖生效）
    static constexpr OptionsT options{};

#if !TICKPOOL_ENABLE_NETWORK
    // 命令日志是网络子系统的输入流 —— 网络被编译掉时它无处导出，故禁止显式开启。
    static_assert(!options.enableCommandLog,
        "enableCommandLog=true requires TICKPOOL_ENABLE_NETWORK=1: the command log is the network "
        "subsystem's input stream and has no exporter when networking is compiled out");
#endif

    using Context = TaskContext<Duration, OptionsT, ThreadPoolType>;
    using TaskID = uint32_t;

    // ---------- 回调 ----------
    using TickCb      = void    (*)(Context&, void* userData);
    using TimeScaleCb = void    (*)(Context&, double oldValue, double newValue, void* userData);
    using ExceptionCb = void    (*)(Context&, const std::exception&, void* userData);
    template<typename UserData>
    void onTickBegin(TickCb cb, UserData* userData);
    void onTickBegin(TickCb cb);   // 无用户数据
    template<typename UserData>
    void onTickEnd(TickCb cb, UserData* userData);
    void onTickEnd(TickCb cb);
    template<typename UserData>
    void onTimeScaleChange(TimeScaleCb cb, UserData* userData);
    void onTimeScaleChange(TimeScaleCb cb);
    template<typename UserData>
    void onTaskException(ExceptionCb cb, UserData* userData);
    void onTaskException(ExceptionCb cb);

    // ---------- ：快照 / 迁移 / 世界哈希 ----------
    using UnknownActionCb = bool    (*)(Context&, const char* task, const char* action, uint64_t schemaVer, void* userData);
    using SchemaMigrateCb = bool    (*)(Context&, uint64_t from, uint64_t to, void* userData);
    using WorldHashCb     = uint64_t(*)(Context&, void* userData);
    SnapshotCallbackBuilder<Context> setSnapshotCallbacks() { return SnapshotCallbackBuilder<Context>(this); }
    template<typename UserData>
    void setWorldCodec(Bytes (*save)(Context&, void*), void (*load)(Context&, const Bytes&, void*), UserData* userData);  // 便捷糖：为所有场景注册同一实现
    void setSchemaVersion(uint64_t v);
    uint64_t schemaVersion() const noexcept;
    template<typename UserData>
    void onUnknownAction(UnknownActionCb cb, UserData* userData);
    void onUnknownAction(UnknownActionCb cb);
    template<typename UserData>
    void onSchemaMigrate(SchemaMigrateCb cb, UserData* userData);
    void onSchemaMigrate(SchemaMigrateCb cb);
    template<typename UserData>
    void setWorldHash(WorldHashCb cb, UserData* userData);
    void setWorldHash(WorldHashCb cb);
    uint64_t worldHash() const;    // 调世界哈希钩子

    // 池状态（只含 tickpool；方向约定：保存=返回 Bytes，加载=接收 Bytes）
    Bytes savePoolState() const;
    void  loadPoolState(const Bytes& bytes);
    // 完整快照（池状态 + 用户段）
    Bytes exportSnapshot() const;
    void  importSnapshot(const Bytes& bytes);
    // Managed 文件 I/O（TICKPOOL_SNAPSHOT_EXTERNAL 下不可用）
    bool  saveSnapshotToFile(const std::string& path, bool includeRollbackInfo = true) const;
    bool  loadSnapshotFromFile(const std::string& path);

    // ---------- ：回滚（内存快照环） ----------
    void setRollbackBuffer(size_t capacity, size_t everyTicks = 1);
    bool rollbackTo(size_t ticksAgo);
    bool rollbackLast();

    // ---------- ：网络同步（命令日志 / 世界传输） ----------
#if TICKPOOL_ENABLE_NETWORK
    // 命令日志（lockstep 输入；开关=编译期 options.enableCommandLog）：
    Bytes exportCommands() const;
    void  importCommands(const Bytes& bytes);
    // 世界传输：external（仅池数据）/ managed（池数据 + network 场景钩子段，quiescent 语义）：
    Bytes exportPoolData() const;
    void  importPoolData(const Bytes& bytes);
    Bytes exportWorldState() const;
    void  importWorldState(const Bytes& bytes);
#endif

    // ---------- DefinitionBuilder（Commit = .options()；所有方法 && 链式） ----------
    template<typename P_, typename A_, typename B_>
    class DefinitionBuilder {
        using ActionDeclT = ActionDecl<Duration, OptionsT, ThreadPoolType>;
        TickPool* pool_ = nullptr;
        TaskKey key_;
        std::vector<ActionDeclT> actions_;
        Fn construct_;
        Fn destruct_;
        template<typename, typename, typename> friend class DefinitionBuilder;
        DefinitionBuilder(TickPool* p, TaskKey k,
            std::vector<ActionDeclT> acts, Fn c, Fn d)
            : pool_(p), key_(std::move(k)), actions_(std::move(acts)), construct_(std::move(c)), destruct_(std::move(d)) {}
    public:
        DefinitionBuilder(TickPool* p, TaskKey k) : pool_(p), key_(std::move(k)) {}
        DefinitionBuilder(DefinitionBuilder&&) = default;
        DefinitionBuilder& operator=(DefinitionBuilder&&) = default;
        DefinitionBuilder(const DefinitionBuilder&) = delete;
        DefinitionBuilder& operator=(const DefinitionBuilder&) = delete;

        template<typename T>
        DefinitionBuilder<T, A_, B_> parallelResult() && {
            static_assert(!std::is_void_v<T>, "parallelResult<T> requires non-void T");
            return { pool_, std::move(key_), std::move(actions_), std::move(construct_), std::move(destruct_) };
        }
        template<typename T>
        DefinitionBuilder<P_, T, B_> asyncResult() && {
            static_assert(!std::is_void_v<T>, "asyncResult<T> requires non-void T");
            return { pool_, std::move(key_), std::move(actions_), std::move(construct_), std::move(destruct_) };
        }
        template<typename T>
        DefinitionBuilder<P_, A_, T> buffer() && {
            static_assert(!std::is_void_v<T>, "buffer<T> requires non-void T");
            return { pool_, std::move(key_), std::move(actions_), std::move(construct_), std::move(destruct_) };
        }

        // 并行 action：结果类型 = 任务并行结果类型（可为 void，此时结果丢弃）
        template<typename Params = void, SubmitMode Mode = SubmitMode::Tick, typename Callable>
        DefinitionBuilder&& action(std::string name, Callable&& callable) && {
            if constexpr (Mode == SubmitMode::Tick) {
                actions_.push_back(pool_->actionRegistry_.template makeParallel<Callable, Params, P_>(std::move(name), std::forward<Callable>(callable)));
            }
            else {
                actions_.push_back(pool_->actionRegistry_.template makeAsync<Callable, Params, A_>(std::move(name), std::forward<Callable>(callable)));
            }
            return std::move(*this);
        }
        template<typename Callable>
        DefinitionBuilder&& construct(Callable&& c) && {
            construct_ = Fn(std::forward<Callable>(c));
            return std::move(*this);
        }
        template<typename Callable>
        DefinitionBuilder&& destruct(Callable&& c) && {
            destruct_ = Fn(std::forward<Callable>(c));
            return std::move(*this);
        }
        void options(TaskDesc desc) {   // Commit 点（强制最后一次调用）
            pool_->template registerTask<P_, A_, B_>(key_, std::move(actions_), std::move(construct_), std::move(destruct_), std::move(desc));
        }
    };

    DefinitionBuilder<void, void, void> defineTask(const TaskKey& key) {
        return DefinitionBuilder<void, void, void>(this, key);
    }

    // ---------- SubmitBuilder（有 .action()：work 只接受数据；无 .action()：work(callable) 匿名异步） ----------
    class SubmitWithAction {
        TickPool* pool_ = nullptr;
        TaskID taskId_ = 0;
        TaskKey key_;
        std::string actionName_;
        SubmitOptions opts_;

        // 于是 `submit(k).options({...}).action("A").work(p)` 里的 options **被静默丢弃**
        SubmitWithAction(TickPool* p, TaskID id, TaskKey k, std::string name, SubmitOptions opts)
            : pool_(p), taskId_(id), key_(std::move(k)), actionName_(std::move(name)), opts_(std::move(opts)) {}
        friend class TickPool;
    public:
        SubmitWithAction(SubmitWithAction&&) = default;
        SubmitWithAction(const SubmitWithAction&) = delete;
        SubmitWithAction& operator=(const SubmitWithAction&) = delete;
        SubmitWithAction& options(SubmitOptions o) { opts_ = std::move(o); return *this; }
        // 数据提交 = Commit（并行/异步 action 按注册模式分派）
        template<typename Payload>
        void work(Payload&& payload) {
            pool_->submitWithAction(taskId_, key_, actionName_, std::forward<Payload>(payload), opts_);
        }
        void work() {   // 空 payload 提交 = Commit
            pool_->submitWithAction(taskId_, key_, actionName_, std::monostate{}, opts_);
        }
    };

    class SubmitBuilderBase {
        TickPool* pool_ = nullptr;
        TaskID taskId_ = 0;
        TaskKey key_;
        SubmitOptions opts_;
        SubmitBuilderBase(TickPool* p, TaskID id, TaskKey k) : pool_(p), taskId_(id), key_(std::move(k)) {}
        friend class TickPool;
    public:
        SubmitBuilderBase(SubmitBuilderBase&&) = default;
        SubmitBuilderBase(const SubmitBuilderBase&) = delete;
        SubmitBuilderBase& operator=(const SubmitBuilderBase&) = delete;
        SubmitBuilderBase& options(SubmitOptions o) { opts_ = std::move(o); return *this; }
        // 无 .action()：匿名异步提交（不进快照）= Commit
        template<typename Callable>
        void work(Callable&& callable) {
            pool_->submitAnonymousAsync(taskId_, std::forward<Callable>(callable), opts_);
        }
        // 指明 action：进入数据提交形态（该形态无 callable 重载 → 行为提交编译失败）
        SubmitWithAction action(std::string name) {
            return SubmitWithAction(pool_, taskId_, key_, std::move(name), opts_);
        }
    };

    SubmitBuilderBase submit(const TaskKey& key) {
        return SubmitBuilderBase(this, getTaskID(key), key);
    }

    template<typename T, typename Func>
    void withParallelBuffer(const TaskKey& key, Func&& func);
    template<typename T, typename Func>
    void withParallelBuffers(const TaskKey& key, Func&& func);
    template<typename T, typename Func>
    void withParallelResults(const TaskKey& key, Func&& func);
    template<typename T, typename Func>
    void withAsyncResults(const TaskKey& key, Func&& func);

    AsyncQueueStats getAsyncQueueStats(const TaskKey& key) const;

    void run(size_t tickCount = 0);
    void stop();
    void setTimeScale(double factor);
    double timeScale() const noexcept;
    void pause(); void resume();
    bool isPaused() const noexcept;

    LogicTime currentLogicTime() const noexcept;
    WallTime currentWallTime() const;
    size_t tickCount() const noexcept;
    size_t pendingAsyncResults(const TaskKey& key) const;

    // ===== ：提交侧去重（见 SubmitOptions::hash） =====
    uint64_t dedupedSubmissions() const noexcept { return runtime_.dedupedSubmissions(); }

    // ===== ：分段计时 Profile =====
    // 需以 `TICKPOOL_ENABLE_PROFILE=1` 编译才有数据；关闭时返回全 0。任意线程可读（累计快照）。
    TickProfileSnapshot getProfile() const noexcept;
    void resetProfile() noexcept;

    // ===== ：编译产物与定义注册表的正式只读视图 =====
    const ExecutionPlan& executionPlan() const noexcept { return plan_; }
    // 定义侧身份：TaskKey↔TaskID 与 id→key 名字（Definition 阶段起即有效，运行期按名解析同源）
    const TaskRegistry& taskRegistry() const noexcept { return taskRegistry_; }

    // ===== 任务依赖图 → Mermaid 源码（graph TD） =====
    // groupByWave=true 时按波次分 subgraph，把「同一波次 = 可并行层」直观画出来；false 则只出节点与边。
    std::string toMermaid(bool groupByWave = true);

    std::string toString() const;
    void fromString(const std::string& data);

private:
    using TaskID = uint32_t;

    template<typename C> friend class SnapshotCallbackBuilder;   // 链式注册器访问 setScenarioHooks

    std::unique_ptr<ThreadPoolType> threadPool_;

    GraphCompiler<options> compiler_;   // 编译期：定义 → ExecutionPlan
    TickRuntime<Duration, OptionsT, ThreadPoolType> runtime_;  // 运行期执行器（持有 RuntimeState）

    TaskRegistry taskRegistry_;     // 定义侧身份（TaskKey↔TaskID；注册即维护，run 前可解析）
    ExecutionPlan plan_;            // 静态执行描述
    bool topologyValid_ = false;

    Duration tickDuration_;

    // ---------- Action 注册表 ----------
    ActionRegistry<Duration, OptionsT, ThreadPoolType> actionRegistry_;

    // per-task action 短名 → ActionID 索引（registerTask 时与 ActionRegistry 同步填充）。
    std::vector<ankerl::unordered_dense::map<std::string, uint32_t, std::hash<std::string>>> taskActionIndex_;

    // ---------- ：回滚环（内存快照帧，按 tick 升序） ----------
    struct RollbackFrame { uint64_t tick = 0; Bytes data; };
    std::deque<RollbackFrame> rollbackFrames_;
    size_t rollbackMaxFrames_ = 0;       // 0 = 未启用
    size_t rollbackEveryTicks_ = 1;
    uint64_t nextRollbackTick_ = 0;

    // ---------- 内部注册 / 提交 ----------
    template<typename P, typename A, typename B>
    void registerTask(const TaskKey& key, std::vector<ActionDecl<Duration, OptionsT, ThreadPoolType>> actions,
        Fn construct, Fn destruct, TaskDesc desc);
    template<typename Payload>
    void submitWithAction(TaskID id, const TaskKey& key, const std::string& actionName, Payload&& payload, const SubmitOptions& opts);
    template<typename Callable>
    void submitAnonymousAsync(TaskID id, Callable&& callable, const SubmitOptions& opts);

    uint64_t allocSeq() noexcept {
        if constexpr (options.enableDeterministicOrdering)
            return runtime_.state().seqCounter_.fetch_add(1, std::memory_order_relaxed);
        else
            return 0;
    }

    void ensureCompiled();

    // ---------- ：快照实现 ----------
    SnapshotData buildSnapshotData(bool withUserSections) const;
    void collectPendingSubmissions(SnapshotData& out) const;
    void applySnapshotData(const SnapshotData& data, bool withUserSections);
    void abortAndReset();          // load 前置：停止运行 + 清场
    void clearPendingSubmissions();// 清空轮与短延迟环
    void clearAsyncQueues();       // 决策 4：恢复后异步清空
    const std::string& actionNameOf(uint32_t actionId) const noexcept { return actionRegistry_.actionName(actionId); }
    // 场景钩子注册（UserData 编译期检查；内部存 void*）
    template<typename UserData>
    void setScenarioHooks(SnapshotScenario sc, Bytes (*save)(Context&, void*), void (*load)(Context&, const Bytes&, void*), UserData* userData) {
        auto& s = runtime_.state();
        typename RuntimeState<Duration, OptionsT, ThreadPoolType>::ScenarioHooks h{
            save, load, reinterpret_cast<void*>(userData) };
        switch (sc) {
        case SnapshotScenario::FileSave: s.fileSaveHooks_ = h; break;
        case SnapshotScenario::Rollback: s.rollbackHooks_ = h; break;
        case SnapshotScenario::Network: s.networkHooks_ = h; break;
        default: s.customHooks_ = h; break;
        }
    }
    const typename RuntimeState<Duration, OptionsT, ThreadPoolType>::ScenarioHooks* scenarioHooksOf(SnapshotScenario sc) const noexcept {
        const auto& s = runtime_.state();
        switch (sc) {
        case SnapshotScenario::FileSave: return &s.fileSaveHooks_;
        case SnapshotScenario::Rollback: return &s.rollbackHooks_;
        case SnapshotScenario::Network: return &s.networkHooks_;
        default: return &s.customHooks_;
        }
    }
    // 回滚帧 = 池状态 + 仅 Rollback 场景段
    SnapshotData buildRollbackFrameData() const;
    void captureRollbackFrame();
    void rebuildRollbackRing(const std::vector<Bytes>& frames);   // 随档恢复（loadSnapshotFromFile）

    // ---------- ：网络同步实现（受 TICKPOOL_ENABLE_NETWORK 控制） ----------
#if TICKPOOL_ENABLE_NETWORK
    // 返回本命令分配的 seq（与日志记录在同一锁内完成 → 日志序 == 全局提交序）
    uint64_t appendCommandLog(uint64_t submitTick, uint64_t executeTick, TaskID taskId, uint32_t actionId, const ::Payload& payload);
    void clearCommandLog();                                   // 恢复/回滚时清空（与 abortAndReset 联动）
    NetWorldData buildNetWorldData(bool withNetworkUserSection) const;  // 调用方须持 tickGate_ unique 锁
    void collectPendingSubmissionsNet(NetWorldData& out) const;
    void applyNetWorldData(const NetWorldData& data, bool withUserSections);
#endif

    // 导入/加载/回滚的**安全点**检查。

    //      调用根本没被发现），Release 下完全静默 —— 与文档"会抛错"的承诺不符。
#ifndef NDEBUG
    std::thread::id debugRunThreadId_ = {};
#endif
    void requireSafeImportPoint(const char* what) const {
        if (TickRuntime<Duration, OptionsT, ThreadPoolType>::inTick())
            throw std::runtime_error(std::string(what) + ": cannot be called from inside a tick callback "
                "(executeTick) — it resets pool state mid-tick (dangling Context*/Task*) and can deadlock on "
                "waitIdle(); call it after stop()");
#ifndef NDEBUG
        const auto& s = runtime_.state();
        if (s.running_.load(std::memory_order_acquire) &&
            std::this_thread::get_id() != debugRunThreadId_) {
            assert(false && "TickPool: load/import/rollback called from a non-run thread while running — "
                "call stop() (and wait for the run thread) first");
        }
#endif
    }

    TaskID getTaskID(const TaskKey& key) const {
        return taskRegistry_.taskId(key);   // TaskRegistry（defineTask 注册即维护，run 前可解析）
    }
};

#include "TickPool.inl"
