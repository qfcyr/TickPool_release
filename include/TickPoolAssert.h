#pragma once
#include <chrono>
#include <ratio>
#include <type_traits>

// ======================================================================

// TICKPOOL_SNAPSHOT_EXTERNAL —— 快照持久化**总开关**。

//   三个分场景开关全部设为 1：用户自管持久化/传输，框架不再代劳。

#ifndef TICKPOOL_SNAPSHOT_EXTERNAL_FILE
#  ifdef TICKPOOL_SNAPSHOT_EXTERNAL
#    define TICKPOOL_SNAPSHOT_EXTERNAL_FILE 1
#  else
#    define TICKPOOL_SNAPSHOT_EXTERNAL_FILE 0
#  endif
#endif
#ifndef TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK
#  ifdef TICKPOOL_SNAPSHOT_EXTERNAL
#    define TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK 1
#  else
#    define TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK 0
#  endif
#endif
#ifndef TICKPOOL_SNAPSHOT_EXTERNAL_NETWORK
#  ifdef TICKPOOL_SNAPSHOT_EXTERNAL
#    define TICKPOOL_SNAPSHOT_EXTERNAL_NETWORK 1
#  else
#    define TICKPOOL_SNAPSHOT_EXTERNAL_NETWORK 0
#  endif
#endif

// TICKPOOL_ENABLE_NETWORK —— 网络同步通道（命令日志 + 世界传输 + 协议字节编解码）。
#ifndef TICKPOOL_ENABLE_NETWORK
#  define TICKPOOL_ENABLE_NETWORK 1
#endif

// ========== Duration 合法性 ==========
// 约束：**被支持的最大时间单位必须是 Duration(1) 的整数倍**（即 Duration 整除该单位）。
inline constexpr std::chrono::hours kMaxSupportedTimeUnit{ 24 };   // 1 天

template<typename Duration>
constexpr bool is_valid_duration() {
    constexpr auto unit_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(kMaxSupportedTimeUnit).count();
    constexpr auto dur_ns = std::chrono::nanoseconds(Duration(1)).count();
    static_assert(dur_ns != 0,
        "Duration is zero or finer than 1 nanosecond — TickPool requires at least nanosecond resolution");
    // 在 ns 计数上做整除判定，避免用 Duration::rep 相乘时溢出
    constexpr auto ticks_per_unit = unit_ns / dur_ns;
    return ticks_per_unit * dur_ns == unit_ns;
}

#define TICKPOOL_ASSERT_VALID_DURATION(Duration) \
    static_assert(is_valid_duration<Duration>(), \
        "the largest supported time unit (kMaxSupportedTimeUnit) must be an integer multiple of Duration")
