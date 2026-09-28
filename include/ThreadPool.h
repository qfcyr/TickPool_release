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
    explicit WorkStealingThreadPool(size_t numThreads = std::thread::hardware_concurrency());
    ~WorkStealingThreadPool();

    size_t threadCount() const { return threadCount_; }

    // 模板化提交：接受任意可调用对象，内部用 Fn 存储，无 std::function 分配
    template<typename F>
    void enqueue(size_t queueIdx, F&& task) {
        size_t idx = queueIdx % queues_.size();
        inFlight_.fetch_add(1, std::memory_order_relaxed);
        queues_[idx].enqueue(Fn{ std::forward<F>(task) });
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

    // 等待所有已入队任务完成（快照保存/恢复前清场用）
    void waitIdle() {
        std::unique_lock lock(idleMutex_);
        idleCV_.wait(lock, [this] { return inFlight_.load(std::memory_order_acquire) == 0; });
    }

private:
    size_t threadCount_;
    std::vector<std::thread> workers_;
    std::vector<moodycamel::ConcurrentQueue<Fn>> queues_;   // 核心改动
    std::atomic<bool> stop_{ false };
    std::atomic<size_t> inFlight_{ 0 };   // 已入队未完成（含 worker 正在执行）
    std::atomic<uint64_t> wakeGen_{ 0 };  // 事件驱动唤醒代数（见 wakeWorkers）
    std::mutex idleMutex_;
    std::condition_variable idleCV_;

    // worker 并发访问同一元素（thiefIdx 来自 roundRobin_ 轮转，非线程身份）→ 数据竞争。

    bool stealTask(size_t thiefIdx);

    // 执行一个任务并维护在途计数（worker / 主线程帮助执行共用）
    void runTask(Fn& task) noexcept {
        try {
            task();
        }
        catch (...) { /* 兜底：策略未覆盖的异常不让其杀 worker */ }
        if (inFlight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            idleCV_.notify_all();
        }
    }
};
