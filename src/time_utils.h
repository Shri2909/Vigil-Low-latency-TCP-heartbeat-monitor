#pragma once

// Time helpers: now_monotonic_ns() reads CLOCK_BOOTTIME (never jumps backward like
// CLOCK_REALTIME, and keeps counting across suspend unlike CLOCK_MONOTONIC), and
// format_duration_ns() renders durations for the CLI and benchmarks.

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

// Formats a duration with an adaptive ns/us/ms/s unit.
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
