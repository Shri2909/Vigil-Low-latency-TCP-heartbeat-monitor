#include "cli_display.h"
#include "connection.h"
#include "time_utils.h"

#include <iomanip>
#include <iostream>
#include <sstream>

#include <unistd.h>

namespace {

// Left-aligns value within `width`, but always appends at least one
// separating space regardless of how long value is -- plain `std::setw`
// provides zero gap once content reaches or exceeds the target width, which
// silently runs adjacent columns together (e.g. a 14-character feed name
// butting straight up against "FAILED" with no space between them).
template <typename T>
std::string pad_field(const T& value, size_t width) {
    std::ostringstream oss;
    oss << value;
    std::string out = oss.str();
    if (out.size() < width) {
        out.append(width - out.size(), ' ');
    }
    out.push_back(' ');
    return out;
}

// "0.00ns" for a feed that has never received a single pong reads as a real
// (implausibly fast) measurement rather than "no data yet" -- mirrors
// format_uptime's sentinel pattern below (connected_since_ns <= 0 -> "-")
// applied to the three RTT columns, all of which default to 0 the same way
// connected_since_ns does before anything has actually happened.
std::string format_rtt_or_dash(double ns, uint64_t heartbeats_acked) {
    if (heartbeats_acked == 0) {
        return "-";
    }
    return format_duration_ns(ns);
}

std::string format_uptime(int64_t connected_since_ns, int64_t now_ns) {
    if (connected_since_ns <= 0) {
        return "-";
    }
    int64_t elapsed_s = (now_ns - connected_since_ns) / 1'000'000'000LL;
    if (elapsed_s < 0) {
        elapsed_s = 0;  // clock skew between snapshot capture and render -- clamp rather than show negative
    }
    const int64_t h = elapsed_s / 3600;
    const int64_t m = (elapsed_s % 3600) / 60;
    const int64_t s = elapsed_s % 60;
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(2) << h << ":" << std::setw(2) << m << ":" << std::setw(2) << s;
    return oss.str();
}

}  // namespace

std::string cli_display::format_table(const AggregateStats& stats, bool use_ansi, int64_t now_ns) {
    std::ostringstream out;
    if (use_ansi) {
        out << "\x1b[H\x1b[J";  // cursor home + clear from cursor to end of screen
    }

    out << "Low-Latency TCP Heartbeat Monitor -- " << stats.feeds.size() << " feeds  (healthy="
        << stats.healthy_count << " degraded=" << stats.degraded_count << " failed=" << stats.failed_count
        << " reconnecting=" << stats.reconnecting_count << ")\n\n";

    out << pad_field("FEED", 5) << pad_field("NAME", 13) << pad_field("STATE", 13) << pad_field("LAST_RTT", 11)
        << pad_field("EWMA_RTT", 11) << pad_field("P99_RTT", 11) << pad_field("MISSED", 7)
        << pad_field("RECONN", 7) << pad_field("UPTIME", 9) << "\n";

    for (const FeedSnapshot& f : stats.feeds) {
        const auto samples = f.stats.rtt_samples_ns.sorted_copy();
        const double p99 = percentile(samples, 99.0);

        const uint64_t acked = f.stats.heartbeats_acked;
        out << pad_field(f.feed_id, 5) << pad_field(f.name, 13) << pad_field(to_string(f.state), 13)
            << pad_field(format_rtt_or_dash(static_cast<double>(f.stats.last_rtt_ns), acked), 11)
            << pad_field(format_rtt_or_dash(f.stats.ewma_rtt_ns, acked), 11)
            << pad_field(format_rtt_or_dash(p99, acked), 11) << pad_field(f.stats.missed, 7)
            << pad_field(f.stats.reconnects, 7) << pad_field(format_uptime(f.stats.connected_since_ns, now_ns), 9)
            << "\n";
    }

    return out.str();
}

void cli_display::render(const AggregateStats& stats) {
    const bool use_ansi = ::isatty(STDOUT_FILENO) != 0;
    std::cout << format_table(stats, use_ansi, now_monotonic_ns()) << std::flush;
}
