#include "ThreadPool.h"
#include <algorithm>

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

WorkStealingThreadPool::WorkStealingThreadPool(size_t numThreads) {
    numThreads = std::max<size_t>(numThreads, 1);
    queues_.resize(numThreads);
    threadCount_ = numThreads;

    for (size_t i = 0; i < numThreads; ++i) {
        workers_.emplace_back([this, i] {
            // 空闲策略：完全事件驱动 —— 无工作即 parking 阻塞（零 CPU），由 wakeWorkers() 唤醒。

            // 推到 ~198ms/tick，而唤醒延迟并无改善）。
            Fn scratch;   // 复用同一 Fn：moodycamel try_dequeue 是移动赋值，Fn::operator= 先 clear
            auto tryRunOne = [&]() -> bool {
                if (queues_[i].try_dequeue(scratch)) { runTask(scratch); return true; }
                for (int attempt = 0; attempt < 3; ++attempt) {
                    const size_t idx = nextVictim(queues_.size());
                    if (idx == i) continue;
                    if (queues_[idx].try_dequeue(scratch)) { runTask(scratch); return true; }
                }
                return false;
            };
            while (!stop_) {
                if (tryRunOne()) continue;
                // C++20 std::atomic 只有 wait（无 wait_for），因此必须闭合 park/unpark 竞态：
                const uint64_t parkedGen = wakeGen_.load(std::memory_order_acquire);
                if (tryRunOne()) continue;
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
    size_t total = 0;
    for (const auto& q : queues_) total += q.size_approx();
    return total;
}
