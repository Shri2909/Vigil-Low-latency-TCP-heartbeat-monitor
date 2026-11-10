#include "cli_display.h"
#include "mini_test.h"

#include <sstream>

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

TEST_CASE("format_table with ansi homes the cursor, clears per line, and never clears the whole screen") {
    AggregateStats stats;
    stats.feeds.push_back(make_snapshot(1, "A", ConnectionState::kHealthy));
    const std::string table = cli_display::format_table(stats, /*use_ansi=*/true, 0);
    CHECK(table.rfind("\x1b[H", 0) == 0);                 // starts with cursor-home
    CHECK(table.find("\x1b[K\n") != std::string::npos);   // clear-to-EOL before each newline
    CHECK(table.find("\x1b[J") != std::string::npos);     // clear below the last row
    CHECK(table.find("\x1b[2J") == std::string::npos);    // no full-screen clear (flicker)
}

TEST_CASE("format_table shows a dash for uptime when never connected") {
    AggregateStats stats;
    FeedSnapshot snap = make_snapshot(1, "NEVER_CONNECTED", ConnectionState::kConnecting);
    snap.stats.connected_since_ns = 0;
    stats.feeds.push_back(snap);

    const std::string table = cli_display::format_table(stats, false, 5'000'000'000LL);
    CHECK(table.find("NEVER_CONNECTED") != std::string::npos);
    CHECK(table.find("00:00:") == std::string::npos);  // no bogus duration
}

TEST_CASE("format_table shows a dash for RTT columns when a feed has never received a pong") {
    AggregateStats stats;
    stats.feeds.push_back(make_snapshot(1, "LOSSY", ConnectionState::kDegraded));  // heartbeats_acked == 0

    const std::string table = cli_display::format_table(stats, false, 0);
    CHECK(table.find("0.00ns") == std::string::npos);
    CHECK(table.find("LOSSY") != std::string::npos);
}

TEST_CASE("format_table still renders real RTT values once a feed has acked at least one pong") {
    AggregateStats stats;
    FeedSnapshot snap = make_snapshot(1, "HEALTHY_FEED", ConnectionState::kHealthy);
    snap.stats.record_pong_received(1234);
    stats.feeds.push_back(snap);

    const std::string table = cli_display::format_table(stats, false, 0);
    CHECK(table.find("1.23us") != std::string::npos);
}

TEST_CASE("format_table renders a non-zero uptime as HH:MM:SS") {
    AggregateStats stats;
    FeedSnapshot snap = make_snapshot(1, "UP", ConnectionState::kHealthy);
    snap.stats.connected_since_ns = 1;  // <= 0 means "never connected"
    stats.feeds.push_back(snap);

    // 3661 s after connecting -> 01:01:01
    const int64_t now_ns = 1 + 3661LL * 1'000'000'000LL;
    const std::string table = cli_display::format_table(stats, false, now_ns);
    CHECK(table.find("01:01:01") != std::string::npos);
}

TEST_CASE("a name that fills its whole column still gets a separator before STATE") {
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

TEST_CASE("feed names are sanitized so control characters cannot inject escape sequences") {
    AggregateStats stats;
    stats.feeds.push_back(make_snapshot(1, "EVIL\x1b[2J\nNAME", ConnectionState::kHealthy));
    const std::string table = cli_display::format_table(stats, /*use_ansi=*/false, 0);
    CHECK(table.find('\x1b') == std::string::npos);
    CHECK(table.find("EVIL?[2J?NAME") != std::string::npos);
}

TEST_CASE("stale last/EWMA RTT is dashed out when a feed is not HEALTHY, but shown when it is") {
    AggregateStats stats;
    FeedSnapshot healthy = make_snapshot(1, "UPFEED", ConnectionState::kHealthy);
    healthy.stats.record_pong_received(5'000'000);  // 5.00ms
    FeedSnapshot failed = make_snapshot(2, "DOWNFEED", ConnectionState::kFailed);
    failed.stats.record_pong_received(7'000'000);   // 7.00ms, now stale

    stats.feeds.push_back(healthy);
    CHECK(cli_display::format_table(stats, false, 0).find("5.00ms") != std::string::npos);

    stats.feeds.clear();
    stats.feeds.push_back(failed);
    const std::string table = cli_display::format_table(stats, false, 0);
    // last and EWMA must be dashed; only the p99 window summary may show 7.00ms.
    const size_t first = table.find("7.00ms");
    CHECK(first != std::string::npos);
    CHECK(table.find("7.00ms", first + 1) == std::string::npos);
}

TEST_CASE("format_frame prints an alerts area below the table") {
    AggregateStats stats;
    stats.feeds.push_back(make_snapshot(1, "A", ConnectionState::kFailed));
    const std::string frame = cli_display::format_frame(stats, {"feed 1 FAILED", "bad\x1b[2Jalert"}, false, 0);
    const size_t table_pos = frame.find("FEED");
    const size_t alerts_pos = frame.find("Alerts:");
    CHECK(table_pos != std::string::npos);
    CHECK(alerts_pos != std::string::npos);
    CHECK(table_pos < alerts_pos);
    CHECK(frame.find("feed 1 FAILED") > alerts_pos);
    CHECK(frame.find('\x1b') == std::string::npos);
}

TEST_CASE("Renderer on a non-TTY emits no ANSI and does not dump a table every tick") {
    std::ostringstream out;
    cli_display::Renderer r(out, /*is_tty=*/false);
    AggregateStats stats;
    stats.feeds.push_back(make_snapshot(1, "A", ConnectionState::kHealthy));

    int64_t now = 1'000'000'000LL;
    CHECK(r.render(stats, now) == true);
    const size_t after_first = out.str().size();
    for (int i = 1; i <= 100; ++i) {      // 100 ticks of 50ms = 5s, nothing changed
        CHECK(r.render(stats, now + i * 50'000'000LL) == false);
    }
    CHECK(out.str().size() == after_first);
    CHECK(out.str().find('\x1b') == std::string::npos);

    stats.feeds[0].state = ConnectionState::kFailed;  // on-change print
    CHECK(r.render(stats, now + 5'100'000'000LL) == true);
    CHECK(out.str().find("FAILED") != std::string::npos);

    // periodic print once kPlainPeriodNs elapses with no change
    const int64_t later = now + 5'100'000'000LL + cli_display::Renderer::kPlainPeriodNs;
    CHECK(r.render(stats, later) == true);
}

TEST_CASE("Renderer on a non-TTY prints each alert once") {
    std::ostringstream out;
    cli_display::Renderer r(out, false);
    AggregateStats stats;
    r.render(stats, 1'000'000'000LL);
    r.add_alert("feed 3 down");
    CHECK(r.render(stats, 1'050'000'000LL) == true);
    CHECK(out.str().find("ALERT: feed 3 down") != std::string::npos);
    const size_t len = out.str().size();
    CHECK(r.render(stats, 1'100'000'000LL) == false);
    CHECK(out.str().size() == len);
}

TEST_CASE("Renderer on a TTY clears once, skips unchanged frames, rate-limits redraws, and keeps alerts") {
    std::ostringstream out;
    cli_display::Renderer r(out, /*is_tty=*/true);
    AggregateStats stats;
    stats.feeds.push_back(make_snapshot(1, "A", ConnectionState::kHealthy));
    const int64_t now = 10'000'000'000LL;
    stats.feeds[0].stats.connected_since_ns = now;  // uptime 00:00:00 at `now`
    CHECK(r.render(stats, now) == true);
    CHECK(out.str().rfind("\x1b[2J", 0) == 0);  // one-time initial clear
    CHECK(r.render(stats, now + 300'000'000LL) == false);  // identical frame -> skipped (same uptime second)

    // uptime ticks to 00:00:01 -> content changed, and >=250ms since the last draw -> redrawn
    const size_t len = out.str().size();
    CHECK(r.render(stats, now + 1'100'000'000LL) == true);
    CHECK(out.str().size() > len);
    const size_t len2 = out.str().size();
    r.add_alert("hello");
    CHECK(r.render(stats, now + 1'150'000'000LL) == false);  // changed but within 250ms of last draw
    CHECK(r.render(stats, now + 1'400'000'000LL) == true);
    CHECK(out.str().substr(len2).find("hello") != std::string::npos);
    CHECK(out.str().substr(len2).find("\x1b[2J") == std::string::npos);  // never a full clear again
}
