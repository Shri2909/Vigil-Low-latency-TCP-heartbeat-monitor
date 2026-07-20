#pragma once

// Small helpers shared by every epoll reactor in this project (FeedSimulator,
// FeedMonitor). Extracted here once a second call site appeared -- see
// PROJECT_PLAN.md's "no premature abstraction" guidance; duplicating these
// across feed_simulator.cpp and feed_monitor.cpp verbatim would have been the
// premature choice, not sharing them.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include <sys/epoll.h>
#include <sys/timerfd.h>

// Builds an epoll interest mask via bitwise-or into a plain uint32_t rather
// than `cond ? EPOLLET : 0` ternaries -- the latter trips -Wextra's
// enumerated/non-enumerated mixed-type warning, since EPOLLIN/EPOLLET/EPOLLOUT
// are values of glibc's anonymous EPOLL_EVENTS enum, not int.
inline uint32_t epoll_interest_flags(bool readable, bool writable, bool edge_triggered) {
    uint32_t flags = 0;
    if (readable) flags |= static_cast<uint32_t>(EPOLLIN);
    if (writable) flags |= static_cast<uint32_t>(EPOLLOUT);
    if (edge_triggered) flags |= static_cast<uint32_t>(EPOLLET);
    return flags;
}

[[noreturn]] inline void throw_errno(const std::string& what) {
    throw std::runtime_error(what + ": " + std::strerror(errno));
}

// Arms timer_fd (already created via timerfd_create) as a repeating timer at
// `interval`. A zero interval would disarm timerfd entirely (its documented
// meaning for it_interval == 0 is "one-shot"), so this clamps to a sane
// minimum rather than silently going quiet on a misconfigured 0ms interval.
// Shared by FeedMonitor (its heartbeat/timeout tick) and FeedSimulator (its
// fault-injection deferred-reply tick, Phase 9) -- extracted once the second
// use appeared, same reasoning as epoll_interest_flags/throw_errno above.
inline void arm_periodic_timer(int timer_fd, std::chrono::milliseconds interval) {
    const auto safe_interval = std::max(interval, std::chrono::milliseconds(1));
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(safe_interval);
    const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(safe_interval - secs);

    itimerspec spec{};
    spec.it_value.tv_sec = secs.count();
    spec.it_value.tv_nsec = nanos.count();
    spec.it_interval = spec.it_value;
    ::timerfd_settime(timer_fd, 0, &spec, nullptr);
}
