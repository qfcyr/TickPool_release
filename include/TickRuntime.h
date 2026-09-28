#pragma once
// TickRuntime.h —— 运行期执行器

#include <atomic>

template<TickDuration Duration, typename OptionsT, ThreadPool ThreadPoolType>
class TickRuntime {
public:
    using TaskID = uint32_t;
    static constexpr TaskID kInvalidTaskID = UINT32_MAX;
    using Context = TaskContext<Duration, OptionsT, ThreadPoolType>;
    using State = RuntimeState<Duration, OptionsT, ThreadPoolType>;

    // 编译期配置
    static constexpr OptionsT options{};

    explicit TickRuntime(ThreadPoolType& pool, Duration tickDuration)
        : pool_(pool), tickDuration_(tickDuration) {}

    // 绑定编译产物（run() 首次编译后调用；Plan 全生命周期有效）
    void loadPlan(const ExecutionPlan& plan) noexcept { plan_ = &plan; }

    // 绑定定义注册表（哨兵按名解析 / 错误消息 id→key 用；与 loadPlan 同时绑定）
    void loadTaskRegistry(const TaskRegistry* registry) noexcept {
        taskRegistry_ = registry;
        startSentinel_ = registry->taskId("TickConstructionTask");

        // ensureCompiled 以空 TaskDesc 注入，没有任何 construct/destruct 行为，纯属拓扑占位；
        endSentinel_ = registry->taskId("TickDestructionTask");
    }

    // 绑定行为注册表
    void loadRegistry(const ActionRegistry<Duration, OptionsT, ThreadPoolType>* registry) noexcept { registry_ = registry; }

    // 绑定协调器（TaskContext::pool 用；TickPool 构造时调用）
    void attachOwner(TickPool<Duration, OptionsT, ThreadPoolType>* owner) noexcept { owner_ = owner; }

    void executeTick();   // 原 TickPool::runOneTick

    // ---------- TLS（submit / withParallel* 的上下文校验） ----------
    static Context* currentCtx() noexcept { return tls_currentCtx_; }
    static TaskID currentTaskId() noexcept { return tls_currentId_; }
    static void setCurrentCtx(Context* c) noexcept { tls_currentCtx_ = c; }
    static void setCurrentId(TaskID id) noexcept { tls_currentId_ = id; }
    // 当前线程是否正在 executeTick 内（quiescent 导出的同线程死锁防护）
    static bool inTick() noexcept { return tls_inTick_; }

    // ---------- 运行期状态（TickPool 公共 API 委托访问） ----------
    State& state() noexcept { return state_; }
    const State& state() const noexcept { return state_; }

    // ---------- 提交与结果通道（TickPool 委托） ----------
    template<typename TaskFunc>
    void submitAsync(TaskID id, TaskFunc&& task, const SubmitOptions& opts);
    // 并行提交：把已构造好的 ScheduledSubmission 放入延迟轮/短延迟环
    void scheduleTick(TaskID ownerTaskId, uint32_t actionId, uint64_t seq, Payload payload, const SubmitOptions& opts);
    template<typename T, typename Func>
    void withParallelBuffer(Func&& func);
    template<typename T, typename Func>
    void withParallelBuffers(Func&& func);
    template<typename T, typename Func>
    void withParallelResults(Func&& func);
    template<typename T, typename Func>
    void withAsyncResults(TaskID id, Func&& func);

    // ---------- ：提交（任务）级去重与异步合并覆盖（TickPool 提交路径委托） ----------
    // 匿名异步提交（submit().work(callable)）没有 actionId，用该哨兵值占一个独立命名空间。
    static constexpr uint32_t kAnonymousSubmitActionId = 0xFFFFFFFFu;
    // **提交级去重的唯一判定点**（三条提交路径共用）。返回 true = 接受该提交；false = 命中重复，
    // 调用方应直接丢弃并计入 dedupedSubmissions()。
    bool submitDedupCheck(TaskID ownerTaskId, uint32_t actionId, uint64_t execTick, uint64_t hash,
                          const ::Payload* probe);
    // 清空去重表（每 tick 收集到期命令前调用）与异步合并覆盖（恢复/回滚清场时调用）。
    void clearSubmitDedup() noexcept;
    void clearAsyncMergeOverrides() noexcept;
    uint64_t dedupedSubmissions() const noexcept {
        return state_.dedupedSubmissions_.load(std::memory_order_relaxed);
    }
    // 提交期 tri-state 覆盖：显式设置写槽（退出优先），读取返回 nullopt 表示"无意见"。
    void noteAsyncMergeOverride(TaskID id, bool enable) noexcept;
    std::optional<bool> asyncMergeOverrideOf(TaskID id) const noexcept;
    void clearAsyncMergeOverride(TaskID id) noexcept;    // 该任务异步结果读空 → 粘滞结束

    // ---------- ：action 异常的统一下报点 ----------
    void reportTaskException(Context& ctx, const std::exception& e, const char* phase);

private:
    ThreadPoolType& pool_;
    TickPool<Duration, OptionsT, ThreadPoolType>* owner_ = nullptr;
    const ExecutionPlan* plan_ = nullptr;
    const TaskRegistry* taskRegistry_ = nullptr;   // 定义身份（哨兵/错误消息按名解析）
    const ActionRegistry<Duration, OptionsT, ThreadPoolType>* registry_ = nullptr;
    State state_;
    Duration tickDuration_;

    // 按-owner 分组的到期任务 buffer（跨 tick 复用，消除每 tick 的 N 空 vector 分配）；
    TaskID startSentinel_ = kInvalidTaskID;
    TaskID endSentinel_ = kInvalidTaskID;   // 见 loadTaskRegistry
    // 本 tick 接触过的 owner（首次 push 记脏）→ 下 tick 只清这些活跃槽，免全表扫描
    std::vector<TaskID> activeOwners_;
    std::vector<std::vector<HierarchicalWheel::Task>> tasksByOwnerBuf_;

    // 跨 tick 复用的一次性缓冲。

    // ≈10KB）、以及 HierarchicalWheel::tick() 的按值返回 vector。
    std::vector<Context*> waveContextsBuf_;
    std::vector<HierarchicalWheel::Task> wheelBatchBuf_;
    std::vector<HierarchicalWheel::Task> wheelTasksBuf_;

    // 并发 executeTick 占用标志（双 run 线程检测）
    std::atomic<bool> execActive_{ false };

    static inline thread_local Context* tls_currentCtx_ = nullptr;
    static inline thread_local TaskID tls_currentId_ = kInvalidTaskID;
    static inline thread_local bool tls_inTick_ = false;   // executeTick 执行标记（死锁防护）

    Context* acquireContext();
    void releaseContext(Context* ctx);
    void processDelayedWheel(size_t currentTick);   // 填充 tasksByOwnerBuf_（不复用 vector 返回值）
    size_t getThreadQueueIndex() const;
    uint64_t allocSeq() noexcept;

    // 执行体三件套 —— worker 入队路径与「小波次内联」路径**共用**，避免内联分支
    // 复制一遍 TLS 建立/恢复与异常策略（复制正是这类改动最容易出错的地方）。
    void runConstructBody(TaskID id, Context* ctx) noexcept;
    void runSubTaskBody(HierarchicalWheel::Task& t) noexcept;
    void runDestructBody(TaskID id, Context* ctx) noexcept;
};

#include "TickRuntime.inl"
