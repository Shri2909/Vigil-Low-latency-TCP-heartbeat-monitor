#include "cli_display.h"
#include "connection.h"
#include "time_utils.h"

#include <iomanip>
#include <iostream>
#include <sstream>

#include <unistd.h>

namespace {

// Left-aligns within `width` and always appends one separating space, so an
// over-long value can't run into the next column (unlike plain std::setw).
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

// "-" instead of a misleading "0.00ns" before the first pong.
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
        elapsed_s = 0;  // snapshot may be newer than now_ns; never show negative
    }
    const int64_t h = elapsed_s / 3600;
    const int64_t m = (elapsed_s % 3600) / 60;
    const int64_t s = elapsed_s % 60;
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(2) << h << ":" << std::setw(2) << m << ":" << std::setw(2) << s;
    return oss.str();
}

// Replaces control characters (incl. ESC/CR/LF/DEL) with '?' so untrusted
// names/alert text can't inject escape sequences or break row structure.
std::string sanitize(const std::string& text) {
    std::string out = text;
    for (char& c : out) {
        const auto u = static_cast<unsigned char>(c);
        if (u < 0x20 || u == 0x7f) {
            c = '?';
        }
    }
    return out;
}

// Table body without cursor control. With ansi, every row ends with
// clear-to-EOL so a shorter row fully overwrites a longer old one.
std::string table_body(const AggregateStats& stats, bool ansi, int64_t now_ns) {
    const char* eol = ansi ? "\x1b[K\n" : "\n";
    std::ostringstream out;

    out << "Low-Latency TCP Heartbeat Monitor -- " << stats.feeds.size() << " feeds  (healthy="
        << stats.healthy_count << " degraded=" << stats.degraded_count << " failed=" << stats.failed_count
        << " reconnecting=" << stats.reconnecting_count << ")" << eol << eol;

    out << pad_field("FEED", 5) << pad_field("NAME", 13) << pad_field("STATE", 13) << pad_field("LAST_RTT", 11)
        << pad_field("EWMA_RTT", 11) << pad_field("P99_RTT", 11) << pad_field("MISSED", 7)
        << pad_field("RECONN", 7) << pad_field("UPTIME", 9) << eol;

    for (const FeedSnapshot& f : stats.feeds) {
        const auto samples = f.stats.rtt_samples_ns.sorted_copy();
        const double p99 = percentile(samples, 99.0);

        // last/EWMA go stale once a feed leaves HEALTHY, so show a dash; P99
        // is a window summary and stays. Display-only; stats are untouched.
        const bool live = f.state == ConnectionState::kHealthy;
        const uint64_t acked = live ? f.stats.heartbeats_acked : 0;
        const uint64_t window_acked = f.stats.heartbeats_acked;
        out << pad_field(f.feed_id, 5) << pad_field(sanitize(f.name), 13)
            << pad_field(to_string(f.state), 13)
            << pad_field(format_rtt_or_dash(static_cast<double>(f.stats.last_rtt_ns), acked), 11)
            << pad_field(format_rtt_or_dash(f.stats.ewma_rtt_ns, acked), 11)
            << pad_field(format_rtt_or_dash(p99, window_acked), 11) << pad_field(f.stats.missed, 7)
            << pad_field(f.stats.reconnects, 7) << pad_field(format_uptime(f.stats.connected_since_ns, now_ns), 9)
            << eol;
    }
    return out.str();
}

// Plain-mode redraw trigger: state and event counters only (RTT/uptime change
// every tick).
std::string state_signature(const AggregateStats& stats) {
    std::ostringstream sig;
    for (const FeedSnapshot& f : stats.feeds) {
        sig << f.feed_id << ':' << to_string(f.state) << ':' << f.stats.missed << ':' << f.stats.reconnects << ';';
    }
    return sig.str();
}

}  // namespace

std::string cli_display::format_frame(const AggregateStats& stats, const std::vector<std::string>& alerts,
                                      bool use_ansi, int64_t now_ns) {
    std::string out;
    if (use_ansi) {
        out += "\x1b[H";
    }
    out += table_body(stats, use_ansi, now_ns);
    if (!alerts.empty()) {
        out += use_ansi ? "\x1b[K\n" : "\n";
        out += use_ansi ? "Alerts:\x1b[K\n" : "Alerts:\n";
        for (const std::string& a : alerts) {
            out += "  " + sanitize(a) + (use_ansi ? "\x1b[K\n" : "\n");
        }
    }
    if (use_ansi) {
        out += "\x1b[J";  // clear leftovers from a taller previous frame
    }
    return out;
}

std::string cli_display::format_table(const AggregateStats& stats, bool use_ansi, int64_t now_ns) {
    return format_frame(stats, {}, use_ansi, now_ns);
}

void cli_display::Renderer::add_alert(const std::string& message) {
    std::lock_guard<std::mutex> lock(mu_);
    const std::string clean = sanitize(message);
    alerts_.push_back(clean);
    while (alerts_.size() > kMaxAlerts) {
        alerts_.pop_front();
    }
    pending_plain_alerts_.push_back(clean);
}

bool cli_display::Renderer::render(const AggregateStats& stats, int64_t now_ns) {
    std::lock_guard<std::mutex> lock(mu_);

    if (!is_tty_) {
        const std::string sig = state_signature(stats);
        const bool changed = !drawn_once_ || sig != last_signature_ || !pending_plain_alerts_.empty();
        const bool due = (now_ns - last_draw_ns_) >= kPlainPeriodNs;
        if (!changed && !due) {
            return false;
        }
        out_ << format_table(stats, /*use_ansi=*/false, now_ns);
        for (const std::string& a : pending_plain_alerts_) {
            out_ << "ALERT: " << a << "\n";
        }
        out_ << "\n" << std::flush;
        pending_plain_alerts_.clear();
        last_signature_ = sig;
        last_draw_ns_ = now_ns;
        drawn_once_ = true;
        return true;
    }

    const std::vector<std::string> alerts(alerts_.begin(), alerts_.end());
    // Skip identical frames, and cap redraws since RTT/uptime change every tick.
    const std::string frame = format_frame(stats, alerts, /*use_ansi=*/true, now_ns);
    if (drawn_once_ && frame == last_frame_) {
        return false;
    }
    if (drawn_once_ && (now_ns - last_draw_ns_) < kMinTtyRedrawNs) {
        return false;
    }
    if (!drawn_once_) {
        out_ << "\x1b[2J";  // one-time clear before the first frame
    }
    out_ << frame << std::flush;
    last_frame_ = frame;
    last_draw_ns_ = now_ns;
    drawn_once_ = true;
    return true;
}

namespace {

cli_display::Renderer& global_renderer() {
    static cli_display::Renderer renderer(std::cout, ::isatty(STDOUT_FILENO) != 0);
    return renderer;
}

}  // namespace

void cli_display::render(const AggregateStats& stats) {
    global_renderer().render(stats, now_monotonic_ns());
}

void cli_display::add_alert(const std::string& message) {
    global_renderer().add_alert(message);
}
