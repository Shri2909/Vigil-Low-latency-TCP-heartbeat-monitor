#pragma once

// Per-connection statistics and a fixed-size RTT ring buffer for percentiles.
// Dependency-free: combining state + stats is FeedMonitor's job (FeedSnapshot).

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

// Fixed-size ring buffer that overwrites the oldest sample.
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

    // Sorted copy of the current window. O(n log n): CLI refresh path only,
    // never per heartbeat.
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
    // now_monotonic_ns() timestamp (time_utils.h); 0 means "not yet connected".
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
// is sorted ascending (RingBuffer::sorted_copy()). Returns 0.0 for empty input
// or NaN p; p outside [0, 100] clamps to min/max.
double percentile(const std::vector<int64_t>& sorted_samples, double p);
