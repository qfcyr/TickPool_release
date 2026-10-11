#include "ThreadPool.h"
#include <algorithm>
#include <cmath>
#include <stdexcept>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  define NOMINMAX
#  include <windows.h>
#endif

namespace {
    // ---------- 高精度墙钟等待（动机见 ThreadPool.h 的声明处） ----------
#ifdef _WIN32
    // Win10 1803+ 的标志；老 SDK 头可能没有定义，按文档值兜底。
#  ifndef CREATE_WAITABLE_TIMER_HIGH_RESOLUTION
#    define CREATE_WAITABLE_TIMER_HIGH_RESOLUTION 0x00000002
#  endif

    // 每线程复用一个可等待定时器（创建开销不可忽略）。构造失败则 h == nullptr → 调用方走回退。
    struct HighResTimer {
        HANDLE h = nullptr;
        HighResTimer() noexcept {
            h = CreateWaitableTimerExW(nullptr, nullptr,
                CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
            if (!h) {
                // 老系统：退化为普通可等待定时器（仍受粒度限制，但不比 sleep_until 差）
                h = CreateWaitableTimerExW(nullptr, nullptr, 0, TIMER_ALL_ACCESS);
            }
        }
        ~HighResTimer() { if (h) CloseHandle(h); }
        HighResTimer(const HighResTimer&) = delete;
        HighResTimer& operator=(const HighResTimer&) = delete;
    };
#endif
}

namespace tickpool_detail {
    void sleepUntilSteady(std::chrono::steady_clock::time_point tp) noexcept {
        const auto now = std::chrono::steady_clock::now();
        if (tp <= now) return;   // 已过期 → 立即返回（保持调用方「不追赶」语义）
#ifdef _WIN32
        static thread_local HighResTimer timer;
        if (timer.h) {
            const auto remainNs =
                std::chrono::duration_cast<std::chrono::nanoseconds>(tp - now).count();
            LARGE_INTEGER due;
            due.QuadPart = -static_cast<LONGLONG>(remainNs / 100);   // 负值 = 相对时间（100ns 单位）
            if (due.QuadPart == 0) due.QuadPart = -1;
            if (SetWaitableTimer(timer.h, &due, 0, nullptr, nullptr, FALSE)) {
                WaitForSingleObject(timer.h, INFINITE);
                return;
            }
        }
#endif
        std::this_thread::sleep_until(tp);   // 回退：与原行为一致
    }
}

namespace {
    // 每线程独立 PRNG（xorshift32）。

    inline uint32_t& stealRngState() noexcept {
        static thread_local uint32_t s = 0x9E3779B9u;
        return s;
    }
    inline size_t nextVictim(size_t n) noexcept {
        uint32_t& s = stealRngState();
        s ^= s << 13; s ^= s >> 17; s ^= s << 5;
        return static_cast<size_t>((static_cast<uint64_t>(s) * static_cast<uint64_t>(n)) >> 32);
    }
}

WorkStealingThreadPool::WorkStealingThreadPool(size_t numThreads, double asyncThreadRatio) {
    numThreads = std::max<size_t>(numThreads, 1);
    threadCount_ = numThreads;
    // 比例越界是用法错误：明确抛错，不静默当 0 或当 1
    if (asyncThreadRatio < 0.0 || asyncThreadRatio > 1.0) {
        throw std::invalid_argument(
            "WorkStealingThreadPool: asyncThreadRatio must be in [0, 1] "
            "(0 = no reservation / 1 = reserve all threads but one)");
    }
    // 异步专用线程数 = clamp(round(N × ratio), 1, N−1)：ratio>0 的语义就是"预留"，故至少 1 条；
    // 同时至少留 1 条给 tick 任务。ratio = 0 → 0（不预留，全体共享泳道）。
    if (asyncThreadRatio > 0.0) {
        if (numThreads < 2) {
            throw std::invalid_argument(
                "WorkStealingThreadPool: asyncThreadRatio > 0 requires at least 2 threads "
                "(one reserved for async, one must remain for tick work)");
        }
        const size_t reserved = static_cast<size_t>(
            std::llround(static_cast<double>(numThreads) * asyncThreadRatio));
        asyncThreadCount_ = std::clamp<size_t>(reserved, 1, numThreads - 1);
    }
    tickThreadCount_ = numThreads - asyncThreadCount_;
    queues_.resize(tickThreadCount_);

    for (size_t i = 0; i < numThreads; ++i) {
        const bool isAsyncRole = i >= tickThreadCount_;   // 后 asyncThreadCount_ 条是异步角色
        workers_.emplace_back([this, i, isAsyncRole] {
            // 空闲策略：完全事件驱动 —— 无工作即 parking 阻塞（零 CPU），由 wakeWorkers() 唤醒。
            // 谁可以消费异步泳道：异步角色永远可以；未预留（ratio=0）时全体共享泳道。
            const bool consumesLane = isAsyncRole || asyncThreadCount_ == 0;
            Fn scratch;   // 复用同一 Fn：moodycamel try_dequeue 是移动赋值，Fn::operator= 先 clear
            auto runAsyncOne = [&]() -> bool {
                if (!asyncQueue_.try_dequeue(scratch)) return false;
                asyncPending_.fetch_sub(1, std::memory_order_acq_rel);   // 已认领
                runTask(scratch);
                noteAsyncDone();   // 结果写入发生在任务体内部 → 此处"结果已入队"成立
                return true;
            };
            auto tryRunTick = [&]() -> bool {
                // 异步角色没有自己的 tick 队列（队列数 = tick 线程数），故只能从"偷取"进入
                if (!isAsyncRole && queues_[i].try_dequeue(scratch)) { runTask(scratch); return true; }
                for (int attempt = 0; attempt < 3; ++attempt) {
                    const size_t idx = nextVictim(queues_.size());
                    if (!isAsyncRole && idx == i) continue;
                    if (queues_[idx].try_dequeue(scratch)) { runTask(scratch); return true; }
                }
                return false;
            };
            // 异步角色：泳道优先，空闲时**单向帮忙** tick 队列；tick 角色：只做 tick（未预留时才碰泳道）
            auto tryRunOne = [&]() -> bool {
                if (isAsyncRole) return runAsyncOne() || tryRunTick();
                if (tryRunTick()) return true;
                return consumesLane && runAsyncOne();
            };
            while (!stop_) {
                if (tryRunOne()) continue;
                // C++20 std::atomic 只有 wait（无 wait_for），因此必须闭合 park/unpark 竞态：
                const uint64_t parkedGen = wakeGen_.load(std::memory_order_acquire);
                if (tryRunOne()) continue;
                // 闸门：**会消费泳道**的线程，在泳道仍有待认领条目时绝不 park。普通队列有"再唤醒者"
                // （run 线程在波次等待里反复 wakeWorkers），泳道没有 —— 一旦漏看一次入队就再没人叫醒
                // 这个 worker，条目永久滞留且 waitIdle() 挂死。有闸门后漏唤醒最多退化为让出 CPU 重试。
                if (consumesLane && asyncPending_.load(std::memory_order_acquire) != 0) {
                    std::this_thread::yield();
                    continue;
                }
                wakeGen_.wait(parkedGen, std::memory_order_acquire);
            }
        });
    }
}

WorkStealingThreadPool::~WorkStealingThreadPool() {
    stop_ = true;
    wakeWorkers();   // 让阻塞在 wakeGen_ 上的 worker 立即看到 stop_ 并退出
    for (auto& w : workers_) if (w.joinable()) w.join();
}

bool WorkStealingThreadPool::tryExecuteOne(size_t thiefIdx) {
    Fn task;
    if (queues_[thiefIdx].try_dequeue(task)) {
        runTask(task);
        return true;
    }
    return stealTask(thiefIdx);
}

bool WorkStealingThreadPool::stealTask(size_t thiefIdx) {
    Fn task;
    for (int attempt = 0; attempt < 3; ++attempt) {
        const size_t i = nextVictim(queues_.size());
        if (i == thiefIdx) continue;
        if (queues_[i].try_dequeue(task)) {
            runTask(task);
            return true;
        }
    }
    return false;
}

size_t WorkStealingThreadPool::pendingTasks() const {
    // 刻意不计异步泳道：本函数只被 run 线程用来判断"是否有可帮助执行的任务"，而它无法（也不应）
    // 从泳道取任务；计入只会让它空转一轮。
    size_t total = 0;
    for (const auto& q : queues_) total += q.size_approx();
    return total;
}
