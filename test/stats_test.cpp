#include "stats.h"
#include "mini_test.h"

TEST_CASE("RingBuffer under capacity keeps all pushed values in order") {
    RingBuffer<int, 4> rb;
    rb.push(10);
    rb.push(20);
    rb.push(30);
    REQUIRE(rb.size() == 3);
    auto sorted = rb.sorted_copy();
    REQUIRE(sorted.size() == 3);
    CHECK(sorted[0] == 10);
    CHECK(sorted[1] == 20);
    CHECK(sorted[2] == 30);
}

TEST_CASE("RingBuffer beyond capacity overwrites the oldest entries") {
    RingBuffer<int, 3> rb;
    rb.push(1);
    rb.push(2);
    rb.push(3);
    rb.push(4);  // overwrites 1
    rb.push(5);  // overwrites 2
    REQUIRE(rb.size() == 3);
    auto sorted = rb.sorted_copy();
    // only {3, 4, 5} should remain
    CHECK(sorted[0] == 3);
    CHECK(sorted[1] == 4);
    CHECK(sorted[2] == 5);
}

TEST_CASE("RingBuffer capacity() reports the compile-time size") {
    using RingBuffer256 = RingBuffer<int64_t, 256>;
    RingBuffer256 rb;
    CHECK(RingBuffer256::capacity() == 256);
    CHECK(rb.size() == 0);
}

TEST_CASE("ConnectionStats tracks min/max/last RTT across samples") {
    ConnectionStats stats;
    stats.record_pong_received(100);
    stats.record_pong_received(50);
    stats.record_pong_received(200);
    CHECK(stats.last_rtt_ns == 200);
    CHECK(stats.min_rtt_ns == 50);
    CHECK(stats.max_rtt_ns == 200);
    CHECK(stats.heartbeats_acked == 3);
}

TEST_CASE("ConnectionStats EWMA seeds from the first sample, then blends") {
    ConnectionStats stats;
    stats.record_pong_received(1000);
    CHECK(stats.ewma_rtt_ns == 1000.0);

    stats.record_pong_received(2000);
    // alpha=0.2: 0.2*2000 + 0.8*1000 = 1200
    const double expected = 0.2 * 2000.0 + 0.8 * 1000.0;
    CHECK(stats.ewma_rtt_ns == expected);
}

TEST_CASE("ConnectionStats record_missed/record_reconnect/record_ping_sent counters") {
    ConnectionStats stats;
    stats.record_ping_sent();
    stats.record_ping_sent();
    stats.record_missed();
    stats.record_reconnect();
    CHECK(stats.heartbeats_sent == 2);
    CHECK(stats.missed == 1);
    CHECK(stats.reconnects == 1);
}

TEST_CASE("percentile on an empty vector returns 0") {
    std::vector<int64_t> empty;
    CHECK(percentile(empty, 50.0) == 0.0);
}

TEST_CASE("percentile at p=0 and p=100 returns min and max") {
    std::vector<int64_t> data = {10, 20, 30, 40, 50};
    CHECK(percentile(data, 0.0) == 10.0);
    CHECK(percentile(data, 100.0) == 50.0);
}

TEST_CASE("percentile at p=50 on an odd-sized dataset returns the median exactly") {
    std::vector<int64_t> data = {10, 20, 30, 40, 50};
    CHECK(percentile(data, 50.0) == 30.0);
}

TEST_CASE("percentile interpolates between ranks on an even-sized dataset") {
    std::vector<int64_t> data = {10, 20, 30, 40};
    // rank = 0.5 * 3 = 1.5 -> interpolate between index 1 (20) and index 2 (30)
    CHECK(percentile(data, 50.0) == 25.0);
}
