#pragma once
// TickRuntimeState.h —— 运行期状态

#include <chrono>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <array>
#include <concurrentqueue/moodycamel/concurrentqueue.h>

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
struct RuntimeState {
    using Context = TaskContext<Duration, OptionsT, ThreadPoolType>;

    // 编译期配置
    static constexpr OptionsT options{};

    // ---------- 回调槽 ----------
    using TickCb      = void    (*)(Context&, void* userData);
    using TimeScaleCb = void    (*)(Context&, double oldValue, double newValue, void* userData);
    using ExceptionCb = void    (*)(Context&, const std::exception&, void* userData);
    TickCb      onTickBeginCb_       = nullptr;  void* onTickBeginData_       = nullptr;
    TickCb      onTickEndCb_         = nullptr;  void* onTickEndData_         = nullptr;
    TimeScaleCb onTimeScaleChangeCb_ = nullptr;  void* onTimeScaleChangeData_ = nullptr;
    ExceptionCb onTaskExceptionCb_   = nullptr;  void* onTaskExceptionData_   = nullptr;

    // ---------- ：快照钩子（场景钩子 / 迁移 / 世界哈希） ----------
    using SnapshotSaveCb  = Bytes (*)(Context&, void* userData);
    using SnapshotLoadCb  = void  (*)(Context&, const Bytes&, void* userData);
    using UnknownActionCb = bool  (*)(Context&, const char* task, const char* action, uint64_t schemaVer, void* userData);
    using SchemaMigrateCb = bool  (*)(Context&, uint64_t from, uint64_t to, void* userData);
    using WorldHashCb     = uint64_t (*)(Context&, void* userData);
    struct ScenarioHooks {
        SnapshotSaveCb save = nullptr;
        SnapshotLoadCb load = nullptr;
        void* userData = nullptr;
    };
    ScenarioHooks fileSaveHooks_, rollbackHooks_, networkHooks_, customHooks_;
    UnknownActionCb onUnknownActionCb_ = nullptr;  void* onUnknownActionData_ = nullptr;
    SchemaMigrateCb onSchemaMigrateCb_ = nullptr;  void* onSchemaMigrateData_ = nullptr;
    WorldHashCb     worldHashCb_       = nullptr;  void* worldHashData_       = nullptr;
    uint64_t schemaVersion_ = 0;

    // ---------- 队列表与队列实例 ----------
    std::vector<TaskTickHot>  tickHotTable_;
    std::vector<TaskAsyncHot> asyncHotTable_;
    std::vector<TaskColdData> coldTable_;
    BufferRegistry            bufferRegistry_; // 统一持有所有队列

    // ---------- 运行进度与时间控制 ----------
    std::atomic<size_t> tickCount_{ 0 };
    std::chrono::steady_clock::time_point startWallTime_;
    std::atomic<double> timeScale_{ 1.0 };
    mutable std::mutex timeMutex_;
    std::condition_variable timeCV_;
    std::atomic<bool> running_{ false };

    // ---------- 调度辅助 ----------

    // 高频 RMW（roundRobin_ 每命令一次、seqCounter_ 每提交一次）→ 真·假共享。各占一条缓存行。
    alignas(64) mutable std::atomic<size_t> roundRobin_{ 0 };
    MonotonicArena arena_;
    alignas(64) std::atomic<uint64_t> seqCounter_{ 0 };   // 全局提交序号（确定性入队序）

    // ---------- 延迟提交（轮 + 短延迟环） ----------
    HierarchicalWheel delayedWheel_{ options.wheelBaseSize, options.wheelMaxLevels };
    std::array<moodycamel::ConcurrentQueue<HierarchicalWheel::Task>, options.shortDelayRingSize> shortDelaySlots_;

    // ---------- ：命令日志（lockstep 输入记录；append-only + drain 游标） ----------
#if TICKPOOL_ENABLE_NETWORK
    // 记录点 = 并行提交（仅并行，与"异步永不进快照"同构）；导出 = 增量 drain（自动推进游标）；
    struct CommandEntry {
        uint64_t seq = 0;             // 全局提交序号（规范序次级键）
        uint64_t submitTick = 0;
        uint64_t executeTick = 0;
        uint32_t taskId = 0;          // 定义序 TaskID（导出时经 TaskRegistry 解析为名字）
        uint32_t actionId = 0;
        ::Payload payload;            // 提交时 clone 的副本（move-only）

        CommandEntry() = default;
        CommandEntry(uint64_t s, uint64_t st, uint64_t et, uint32_t tid, uint32_t aid, ::Payload&& p)
            : seq(s), submitTick(st), executeTick(et), taskId(tid), actionId(aid), payload(std::move(p)) {}
        CommandEntry(const CommandEntry&) = delete;
        CommandEntry& operator=(const CommandEntry&) = delete;
        CommandEntry(CommandEntry&&) noexcept = default;
        CommandEntry& operator=(CommandEntry&&) noexcept = default;
    };
    mutable std::mutex commandLogMutex_;
    mutable std::vector<CommandEntry> commandLog_;     // 无界（决策：用户定期 drain 控制内存）
    mutable size_t commandLogCursor_ = 0;              // 已导出前缀（drain 游标）
#endif   // TICKPOOL_ENABLE_NETWORK

    // ---------- ：tick 执行互斥门（quiescent 世界导出） ----------
    mutable std::shared_mutex tickGate_;

    // ---------- ：提交（任务）级去重（键 = SubmitOptions::hash，强度 = TaskDesc::merge） ----------
    struct SubmitDedupKey {
        uint32_t ownerTaskId;
        uint32_t actionId;
        uint64_t execTick;
        uint64_t hash;
        bool operator==(const SubmitDedupKey& o) const noexcept {
            return ownerTaskId == o.ownerTaskId && actionId == o.actionId &&
                   execTick == o.execTick && hash == o.hash;
        }
    };
    struct SubmitDedupKeyHash {
        size_t operator()(const SubmitDedupKey& k) const noexcept {
            uint64_t h = k.hash;
            h ^= static_cast<uint64_t>(k.ownerTaskId) * 0x9E3779B97F4A7C15ull;
            h ^= static_cast<uint64_t>(k.actionId)     * 0xC2B2AE3D27D4EB4Full;
            h ^= k.execTick                            * 0x165667B19E3779F9ull;
            h ^= h >> 29;
            h *= 0xBF58476D1CE4E5B9ull;
            h ^= h >> 32;
            return static_cast<size_t>(h);
        }
    };
    mutable std::mutex submitDedupMutex_;
    // 键 → 该键下已接受提交的 payload 副本（只有 Conservative 会填：用于 equal 值确认；
    ankerl::unordered_dense::map<SubmitDedupKey, std::vector<::Payload>, SubmitDedupKeyHash> submitDedup_;
    std::atomic<uint64_t> dedupedSubmissions_{ 0 };   // 被提交级去重折叠丢弃的提交数（可观测；进程内累计）

    // ---------- ：异步结果合并的提交期覆盖（SubmitOptions::enableAsyncMerge） ----------
    // 每任务一个槽：NoOpinion（沿用定义期 TaskDesc.asyncMerge）/ ForceOn / ForceOff。

    // 异步提交可来自任意线程（并行提交才被限制在构造相），故用锁 + 原子计数保护：
    static constexpr uint8_t kAsyncMergeNoOpinion = 0;
    static constexpr uint8_t kAsyncMergeForceOn   = 1;
    static constexpr uint8_t kAsyncMergeForceOff  = 2;
    mutable std::mutex asyncMergeOverrideMutex_;
    ankerl::unordered_dense::map<uint32_t, uint8_t> asyncMergeOverride_;   // TaskID → 覆盖值
    std::atomic<uint32_t> asyncMergeOverrideCount_{ 0 };

    // ---------- ：分段计时 Profile 累加器（仅 TICKPOOL_ENABLE_PROFILE=1 时存在） ----------
#if TICKPOOL_ENABLE_PROFILE
    struct ProfileAccum {
        std::atomic<uint64_t> ticks{ 0 };
        std::atomic<uint64_t> wheelNs{ 0 };
        std::atomic<uint64_t> constructNs{ 0 };
        std::atomic<uint64_t> subtaskNs{ 0 };
        std::atomic<uint64_t> destructNs{ 0 };
        std::atomic<uint64_t> inlineNs{ 0 };
        std::atomic<uint64_t> sleepNs{ 0 };
        std::atomic<uint64_t> sortNs{ 0 };
        std::atomic<uint64_t> tickTotalNs{ 0 };
        std::atomic<uint64_t> commands{ 0 };
        std::atomic<uint64_t> subtasks{ 0 };
        std::atomic<uint64_t> waves{ 0 };
        std::atomic<uint64_t> inlinedWaves{ 0 };

        static void bump(std::atomic<uint64_t>& a, uint64_t v) noexcept {
            a.fetch_add(v, std::memory_order_relaxed);
        }
        static void inc(std::atomic<uint64_t>& a) noexcept {
            a.fetch_add(1, std::memory_order_relaxed);
        }

        TickProfileSnapshot snapshot() const noexcept {
            TickProfileSnapshot s;
            s.ticks         = ticks.load(std::memory_order_relaxed);
            s.wheelNs       = wheelNs.load(std::memory_order_relaxed);
            s.constructNs   = constructNs.load(std::memory_order_relaxed);
            s.subtaskNs     = subtaskNs.load(std::memory_order_relaxed);
            s.destructNs    = destructNs.load(std::memory_order_relaxed);
            s.inlineNs      = inlineNs.load(std::memory_order_relaxed);
            s.sleepNs       = sleepNs.load(std::memory_order_relaxed);
            s.sortNs        = sortNs.load(std::memory_order_relaxed);
            s.tickTotalNs   = tickTotalNs.load(std::memory_order_relaxed);
            s.commands      = commands.load(std::memory_order_relaxed);
            s.subtasks      = subtasks.load(std::memory_order_relaxed);
            s.waves         = waves.load(std::memory_order_relaxed);
            s.inlinedWaves  = inlinedWaves.load(std::memory_order_relaxed);
            s.fnHeapAllocs  = tickpool_detail::g_fnHeapAllocs.load(std::memory_order_relaxed);
            return s;
        }

        void reset() noexcept {
            ticks.store(0, std::memory_order_relaxed);
            wheelNs.store(0, std::memory_order_relaxed);
            constructNs.store(0, std::memory_order_relaxed);
            subtaskNs.store(0, std::memory_order_relaxed);
            destructNs.store(0, std::memory_order_relaxed);
            inlineNs.store(0, std::memory_order_relaxed);
            sleepNs.store(0, std::memory_order_relaxed);
            sortNs.store(0, std::memory_order_relaxed);
            tickTotalNs.store(0, std::memory_order_relaxed);
            commands.store(0, std::memory_order_relaxed);
            subtasks.store(0, std::memory_order_relaxed);
            waves.store(0, std::memory_order_relaxed);
            inlinedWaves.store(0, std::memory_order_relaxed);
        }
    };
    mutable ProfileAccum prof_;
#endif
};
