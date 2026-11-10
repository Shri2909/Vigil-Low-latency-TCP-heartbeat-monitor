#pragma once

// Small helpers shared by the epoll reactors (FeedMonitor, FeedSimulator).

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include <sys/epoll.h>
#include <sys/timerfd.h>

// Builds an epoll interest mask as uint32_t (the EPOLL* enumerators are not int, so ternaries trip -Wextra).
inline uint32_t epoll_interest_flags(bool readable, bool writable, bool edge_triggered) {
    uint32_t flags = 0;
    if (readable) flags |= static_cast<uint32_t>(EPOLLIN);
    if (writable) flags |= static_cast<uint32_t>(EPOLLOUT);
    if (edge_triggered) flags |= static_cast<uint32_t>(EPOLLET);
    return flags;
}

[[noreturn]] inline void throw_errno(const std::string& what) {
    // Capture errno before the message allocation can clobber it.
    const int saved_errno = errno;
    throw std::runtime_error(what + ": " + std::strerror(saved_errno));
}

// Arms timer_fd as a repeating timer. Intervals under 1ms are clamped, since a zero it_interval means one-shot.
inline void arm_periodic_timer(int timer_fd, std::chrono::milliseconds interval) {
    const auto safe_interval = std::max(interval, std::chrono::milliseconds(1));
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(safe_interval);
    const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(safe_interval - secs);

    itimerspec spec{};
    spec.it_value.tv_sec = secs.count();
    spec.it_value.tv_nsec = nanos.count();
    spec.it_interval = spec.it_value;
    if (::timerfd_settime(timer_fd, 0, &spec, nullptr) < 0) {
        throw_errno("timerfd_settime");
    }
}
