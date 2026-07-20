#pragma once

// Per-connection statistics and the fixed-size ring buffer that backs
// percentile estimation. See PROJECT_PLAN.md section 7 and the audit finding
// #16 that added this module (the original spec asked for "real-time stats"
// and "latency analysis" without ever defining a data structure for either).
//
// Deliberately dependency-free (no connection.h / feed_monitor.h include):
// ConnectionState lives in connection.h, and combining state + stats into a
// display-ready snapshot is a FeedMonitor-level concern (see FeedSnapshot in
// feed_monitor.h) -- keeping that combination out of this header is what lets
// stats.h build before connection.h even exists.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

// Fixed-size, overwrites-oldest ring buffer. Used to keep a bounded window of
// recent RTT samples for percentile estimation without unbounded growth over
// a long-running connection.
template <typename T, size_t N>
class RingBuffer {
public:
    void push(T value) {
        data_[next_] = value;
        next_ = (next_ + 1) % N;
        if (count_ < N) {
            ++count_;
        }
    }

    // Returns a sorted copy of the samples currently held (oldest-overwritten
    // semantics mean this is whatever's left in the window, not full history).
    // O(count log count); only ever called from the low-frequency CLI refresh
    // path, never from the per-heartbeat hot path.
    std::vector<T> sorted_copy() const {
        std::vector<T> result(data_.begin(), data_.begin() + static_cast<long>(count_));
        std::sort(result.begin(), result.end());
        return result;
    }

    size_t size() const { return count_; }
    static constexpr size_t capacity() { return N; }

private:
    std::array<T, N> data_{};
    size_t count_ = 0;
    size_t next_ = 0;
};

struct ConnectionStats {
    uint64_t heartbeats_sent = 0;
    uint64_t heartbeats_acked = 0;
    uint64_t missed = 0;
    uint64_t reconnects = 0;

    int64_t last_rtt_ns = 0;
    int64_t min_rtt_ns = std::numeric_limits<int64_t>::max();
    int64_t max_rtt_ns = 0;
    double ewma_rtt_ns = 0.0;
    static constexpr double kEwmaAlpha = 0.2;

    RingBuffer<int64_t, 256> rtt_samples_ns;
    // CLOCK_MONOTONIC nanoseconds (see time_utils.h::now_monotonic_ns), not a
    // std::chrono::steady_clock::time_point: everything else in this project
    // already works in raw int64_t monotonic nanoseconds, and mixing that
    // with steady_clock -- which happens to share the same clock on
    // Linux/libstdc++ but isn't guaranteed to -- is a needless landmine.
    // 0 means "not yet connected."
    int64_t connected_since_ns = 0;

    void record_ping_sent() { ++heartbeats_sent; }

    void record_pong_received(int64_t rtt_ns) {
        ++heartbeats_acked;
        last_rtt_ns = rtt_ns;
        min_rtt_ns = std::min(min_rtt_ns, rtt_ns);
        max_rtt_ns = std::max(max_rtt_ns, rtt_ns);
        ewma_rtt_ns = (heartbeats_acked == 1)
                          ? static_cast<double>(rtt_ns)
                          : (kEwmaAlpha * static_cast<double>(rtt_ns) + (1.0 - kEwmaAlpha) * ewma_rtt_ns);
        rtt_samples_ns.push(rtt_ns);
    }

    void record_missed() { ++missed; }
    void record_reconnect() { ++reconnects; }
};

// Linear-interpolated percentile, p in [0, 100]. Precondition: sorted_samples
// is sorted ascending (as returned by RingBuffer::sorted_copy()) -- not
// re-validated here since every call site already has a sorted vector and
// this stays off the per-heartbeat hot path regardless.
double percentile(const std::vector<int64_t>& sorted_samples, double p);
