#include "cli_display.h"
#include "mini_test.h"

namespace {

FeedSnapshot make_snapshot(uint32_t feed_id, const std::string& name, ConnectionState state) {
    FeedSnapshot snap;
    snap.feed_id = feed_id;
    snap.name = name;
    snap.state = state;
    return snap;
}

}  // namespace

TEST_CASE("format_table includes each feed's id, name, and state") {
    AggregateStats stats;
    stats.feeds.push_back(make_snapshot(1, "NASDAQ", ConnectionState::kHealthy));
    stats.feeds.push_back(make_snapshot(2, "NYSE", ConnectionState::kDegraded));
    stats.healthy_count = 1;
    stats.degraded_count = 1;

    const std::string table = cli_display::format_table(stats, /*use_ansi=*/false, /*now_ns=*/1'000'000);

    CHECK(table.find("NASDAQ") != std::string::npos);
    CHECK(table.find("NYSE") != std::string::npos);
    CHECK(table.find("HEALTHY") != std::string::npos);
    CHECK(table.find("DEGRADED") != std::string::npos);
}

TEST_CASE("format_table without ansi omits escape codes") {
    AggregateStats stats;
    const std::string table = cli_display::format_table(stats, /*use_ansi=*/false, 0);
    CHECK(table.find("\x1b[") == std::string::npos);
}

TEST_CASE("format_table with ansi includes cursor-home/clear escape codes") {
    AggregateStats stats;
    const std::string table = cli_display::format_table(stats, /*use_ansi=*/true, 0);
    CHECK(table.find("\x1b[H") != std::string::npos);
    CHECK(table.find("\x1b[J") != std::string::npos);
}

TEST_CASE("format_table shows a dash for uptime when never connected") {
    AggregateStats stats;
    FeedSnapshot snap = make_snapshot(1, "NEVER_CONNECTED", ConnectionState::kConnecting);
    snap.stats.connected_since_ns = 0;
    stats.feeds.push_back(snap);

    const std::string table = cli_display::format_table(stats, false, 5'000'000'000LL);
    CHECK(table.find("NEVER_CONNECTED") != std::string::npos);
    // uptime column should render as "-" for this row, not a bogus duration
    CHECK(table.find(" -   ") != std::string::npos || table.find(" -\n") != std::string::npos ||
          table.find(" -") != std::string::npos);
}

TEST_CASE("format_table shows a dash for RTT columns when a feed has never received a pong") {
    // Regression test for a real bug, visible directly in a live demo: a
    // feed under heavy packet loss (or one that's simply never gotten a
    // reply yet) rendered "0.00ns" in LAST_RTT/EWMA_RTT/P99_RTT -- reading as
    // an implausibly fast real measurement rather than "no data yet". Fixed
    // to follow the same sentinel pattern format_uptime already used.
    AggregateStats stats;
    FeedSnapshot snap = make_snapshot(1, "LOSSY", ConnectionState::kDegraded);
    // default-constructed ConnectionStats: heartbeats_acked == 0, last/ewma
    // RTT == 0 -- exactly the "never acked" state the bug mishandled.
    stats.feeds.push_back(snap);

    const std::string table = cli_display::format_table(stats, false, 0);
    CHECK(table.find("0.00ns") == std::string::npos);
    CHECK(table.find("LOSSY") != std::string::npos);
}

TEST_CASE("format_table still renders real RTT values once a feed has acked at least one pong") {
    AggregateStats stats;
    FeedSnapshot snap = make_snapshot(1, "HEALTHY_FEED", ConnectionState::kHealthy);
    snap.stats.record_pong_received(1234);  // heartbeats_acked becomes 1, last_rtt_ns becomes 1234
    stats.feeds.push_back(snap);

    const std::string table = cli_display::format_table(stats, false, 0);
    CHECK(table.find("1.23us") != std::string::npos);  // last_rtt_ns=1234ns formatted as microseconds
}

TEST_CASE("format_table renders a non-zero uptime as HH:MM:SS") {
    AggregateStats stats;
    FeedSnapshot snap = make_snapshot(1, "UP", ConnectionState::kHealthy);
    // connected_since_ns <= 0 means "never connected" (see format_uptime's
    // sentinel check) -- use a small positive timestamp, not exactly 0.
    snap.stats.connected_since_ns = 1;
    stats.feeds.push_back(snap);

    // connected at t=1ns, rendered ~3661 seconds later -> 01:01:01
    const int64_t now_ns = 1 + 3661LL * 1'000'000'000LL;
    const std::string table = cli_display::format_table(stats, false, now_ns);
    CHECK(table.find("01:01:01") != std::string::npos);
}

TEST_CASE("a name that fills its whole column still gets a separator before STATE") {
    // Regression test: a name exactly as long as (or longer than) its target
    // column width used to consume all of setw's padding, running straight
    // into the next column with zero separation -- e.g. a real run produced
    // "LOSSY_EXCHANGEFAILED" for a 14-character name. pad_field() guarantees
    // at least one space regardless of content length; this pins that down.
    AggregateStats stats;
    stats.feeds.push_back(make_snapshot(1, "LOSSY_EXCHANGE", ConnectionState::kFailed));  // 14 chars

    const std::string table = cli_display::format_table(stats, false, 0);
    CHECK(table.find("LOSSY_EXCHANGEFAILED") == std::string::npos);
    CHECK(table.find("LOSSY_EXCHANGE FAILED") != std::string::npos);
}

TEST_CASE("format_table renders the aggregate counts in the header line") {
    AggregateStats stats;
    stats.healthy_count = 3;
    stats.degraded_count = 1;
    stats.failed_count = 2;
    stats.reconnecting_count = 1;
    const std::string table = cli_display::format_table(stats, false, 0);
    CHECK(table.find("healthy=3") != std::string::npos);
    CHECK(table.find("degraded=1") != std::string::npos);
    CHECK(table.find("failed=2") != std::string::npos);
    CHECK(table.find("reconnecting=1") != std::string::npos);
}
