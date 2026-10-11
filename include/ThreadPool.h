#pragma once
#include "FnWrapper.h"
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <mutex>
#include <condition_variable>
#include <concurrentqueue/moodycamel/concurrentqueue.h>

// 高精度墙钟等待（实现见 ThreadPool.cpp —— 那里才引入 <windows.h>，避免污染公开头文件）。

// ~15.625ms —— 20ms 的 tick 实际睡 ~31.3ms（2 个量子）、10ms 实际睡 ~15.6ms（1 个量子）；

// 本函数优先使用 Win10 1803+ 的 `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION` 可等待定时器规避该粒度，

//
// 语义：目标时刻已过则立即返回 —— 调用方据此保持「超预算不追赶」的既定语义。
namespace tickpool_detail {
    void sleepUntilSteady(std::chrono::steady_clock::time_point tp) noexcept;
}

class WorkStealingThreadPool {
public:
    // asyncThreadRatio（0..1）：异步专用线程比例。
    //   0  = 不预留（默认）：异步与 tick 共享全部线程；
    //   >0 = 预留 A = clamp(round(N×ratio), 1, N-1) 条线程只服务异步泳道，其余做 tick 任务；
    //        N < 2 时无法预留 → 构造抛 std::invalid_argument（不静默退化）。
    // 角色**构造期定死**：不新增线程（总数 N 不变），运行期不改角色。
    // 单向帮忙：异步线程空闲时可偷 tick 队列；**tick 线程永不消费异步泳道**。
    explicit WorkStealingThreadPool(size_t numThreads = std::thread::hardware_concurrency(),
                                    double asyncThreadRatio = 0.0);
    ~WorkStealingThreadPool();

    size_t threadCount() const { return threadCount_; }            // 总线程数（tick + 异步）
    size_t tickThreadCount() const { return tickThreadCount_; }    // 服务 tick 任务的线程数（= 队列数）
    size_t asyncThreadCount() const { return asyncThreadCount_; }  // 异步专用线程数（0 = 未预留）

    // 模板化提交：接受任意可调用对象，内部用 Fn 存储，无 std::function 分配
    template<typename F>
    void enqueue(size_t queueIdx, F&& task) {
        size_t idx = queueIdx % queues_.size();
        inFlight_.fetch_add(1, std::memory_order_relaxed);
        queues_[idx].enqueue(Fn{ std::forward<F>(task) });
    }

    // 异步泳道：异步体（匿名 work / SubmitMode::Async action）唯一的执行去处。
    //   - 只有"会消费泳道的角色"取它：预留了异步线程时 = 异步角色；未预留（ratio=0）= 全体 worker。
    //     **tick 角色与 run 线程都不碰它**（帮助执行 executeUpTo 与偷取 stealTask 只看 tick 队列），
    //     否则 run 线程会在波次等待里把异步体捡回来跑，"异步不占用 tick 线程"立刻失效；
    //   - 入队计入 inFlight_（全量清场用）与 asyncInFlight_（waitAsyncIdle/pendingAsyncWork）；
    //   - 唤醒按批：仅当泳道此前没有待认领条目时 notify_all 一次（逐条唤醒会惊群）；
    //   - 配套"有货绝不 park"闸门，见 ThreadPool.cpp 的 worker 循环。
    template<typename F>
    void enqueueAsync(F&& task) {
        Fn fn{ std::forward<F>(task) };
        inFlight_.fetch_add(1, std::memory_order_relaxed);
        asyncInFlight_.fetch_add(1, std::memory_order_relaxed);
        const bool wasIdle = asyncPending_.fetch_add(1, std::memory_order_acq_rel) == 0;
        asyncQueue_.enqueue(std::move(fn));
        if (wasIdle) wakeWorkers();
    }

    bool tryExecuteOne(size_t thiefIdx);
    // 主线程 help-execute 批量 —— 本队列 bulk dequeue 连续执行，不足 limit 再 steal；
    size_t executeUpTo(size_t thiefIdx, size_t limit) {
        static thread_local std::vector<Fn> batch;
        size_t executed = 0;
        size_t idx = thiefIdx % queues_.size();
        while (executed < limit) {
            batch.clear();
            size_t got = queues_[idx].try_dequeue_bulk(std::back_inserter(batch), limit - executed);
            if (got == 0) break;
            for (auto& t : batch) runTask(t);
            executed += got;
        }
        while (executed < limit && stealTask(thiefIdx)) ++executed;
        return executed;
    }
    size_t pendingTasks() const;

    // 事件驱动唤醒：空闲 worker 用 std::atomic::wait 阻塞，入队方**每批**任务入队后调用一次。

    // sleep_for 向上取整到一个定时器量子（~1ms），于是每个波次都付出 ~0.88ms 纯等待
    void wakeWorkers() noexcept {
        wakeGen_.fetch_add(1, std::memory_order_release);
        wakeGen_.notify_all();
    }

    // 等待**全部**在途工作（tick 任务 + 异步体）完成（恢复/导入/回滚/析构前清场用）
    void waitIdle() {
        std::unique_lock lock(idleMutex_);
        idleCV_.wait(lock, [this] { return inFlight_.load(std::memory_order_acquire) == 0; });
    }

    // 只等**异步体**结束（结果在返回前已写入队列）—— 公共 waitAsyncIdle() 的底层
    void waitAsyncIdle() {
        std::unique_lock lock(idleMutex_);
        asyncIdleCV_.wait(lock, [this] { return asyncInFlight_.load(std::memory_order_acquire) == 0; });
    }

    // 在途异步体数（诊断/自检）
    size_t pendingAsyncWork() const noexcept { return asyncInFlight_.load(std::memory_order_acquire); }

private:
    size_t threadCount_ = 0;
    size_t tickThreadCount_ = 0;
    size_t asyncThreadCount_ = 0;
    std::vector<std::thread> workers_;
    std::vector<moodycamel::ConcurrentQueue<Fn>> queues_;   // tick 队列（每个 tick 线程一条）
    moodycamel::ConcurrentQueue<Fn> asyncQueue_;            // 异步泳道（不进偷取集合）
    std::atomic<size_t> asyncPending_{ 0 };                 // 已入泳道、未被认领的条目数（唤醒按批 + park 闸门）
    std::atomic<bool> stop_{ false };
    std::atomic<size_t> inFlight_{ 0 };        // 已入队未完成（tick + 异步，含正在执行）
    std::atomic<size_t> asyncInFlight_{ 0 };   // 已入队未完成的**异步体**（waitAsyncIdle 的判据）
    std::atomic<uint64_t> wakeGen_{ 0 };  // 事件驱动唤醒代数（见 wakeWorkers）
    std::mutex idleMutex_;
    std::condition_variable idleCV_;       // inFlight_ → 0
    std::condition_variable asyncIdleCV_;  // asyncInFlight_ → 0

    // worker 并发访问同一元素（thiefIdx 来自 roundRobin_ 轮转，非线程身份）→ 数据竞争。

    bool stealTask(size_t thiefIdx);

    // 执行一个任务并维护在途计数（tick worker / 异步 worker / 主线程帮助执行共用）。
    // 计数归零时**持锁**再 notify：谓词检查与递减之间存在"检查后通知前"的竞态（递减→通知都
    // 发生在等待方阻塞之前 → 丢失唤醒）；持锁闭合它，且只在归零那一次触发，不在热路径上。
    void runTask(Fn& task) noexcept {
        try {
            task();
        }
        catch (...) { /* 兜底：策略未覆盖的异常不让其杀 worker */ }
        if (inFlight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lock(idleMutex_);
            idleCV_.notify_all();
        }
    }

    // 异步体结束（在 runTask 之后调用：结果写入发生在任务体内部，故此刻"结果已入队"成立）
    void noteAsyncDone() noexcept {
        if (asyncInFlight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lock(idleMutex_);
            asyncIdleCV_.notify_all();
        }
    }
};
