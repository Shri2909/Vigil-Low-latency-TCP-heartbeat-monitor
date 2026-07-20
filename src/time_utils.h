#pragma once

// CLOCK_BOOTTIME helper shared by Connection, FeedMonitor, FeedSimulator, and
// the benchmarks -- one place for the "why CLOCK_BOOTTIME, not CLOCK_REALTIME
// or CLOCK_MONOTONIC" note so it doesn't need repeating at every call site:
// the heartbeat protocol's RTT and timeout math (PROJECT_PLAN.md section 3)
// requires a clock that never jumps backward (NTP adjustments, manual clock
// changes), which CLOCK_REALTIME does not guarantee. CLOCK_MONOTONIC meets
// that bar but freezes across system suspend/resume; CLOCK_BOOTTIME is
// identical to CLOCK_MONOTONIC except it also keeps advancing through
// suspend, which is the correct semantics here -- a feed that's been
// unreachable across a host suspend should read as having missed every
// heartbeat during that gap, not as if no time passed at all. Linux-only
// (available since 3.17), consistent with this project's only supported
// platform.

#include <cstdint>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

inline int64_t now_monotonic_ns() {
    struct timespec ts;
    ::clock_gettime(CLOCK_BOOTTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + static_cast<int64_t>(ts.tv_nsec);
}

// Human-readable duration formatting, adaptively picking ns/us/ms/s. Shared
// by cli_display (RTT columns) and the benchmarks (bench/bench_common.h) --
// extracted here once the second use appeared, same DRY reasoning as
// epoll_utils.h's helpers.
inline std::string format_duration_ns(double ns) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (ns < 1000.0) {
        oss << ns << "ns";
    } else if (ns < 1'000'000.0) {
        oss << (ns / 1000.0) << "us";
    } else if (ns < 1'000'000'000.0) {
        oss << (ns / 1'000'000.0) << "ms";
    } else {
        oss << (ns / 1'000'000'000.0) << "s";
    }
    return oss.str();
}
