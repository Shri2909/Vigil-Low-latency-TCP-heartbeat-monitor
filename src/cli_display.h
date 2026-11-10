#pragma once

// Terminal table renderer for AggregateStats (in-place refresh, no curses).
// FeedMonitor does not depend on this header; it reports via callbacks.

#include "feed_monitor.h"

#include <cstdint>
#include <deque>
#include <iosfwd>
#include <mutex>
#include <string>
#include <vector>

namespace cli_display {

// Pure table formatting; now_ns drives the UPTIME column. With use_ansi: cursor
// home first, clear-to-EOL after every row, clear-to-EOS at the end (no
// full-screen clear, so no flicker).
std::string format_table(const AggregateStats& stats, bool use_ansi, int64_t now_ns);

// format_table() plus an alerts area below it (oldest first); omitted if empty.
std::string format_frame(const AggregateStats& stats, const std::vector<std::string>& alerts, bool use_ansi,
                         int64_t now_ns);

// Decides whether to draw, not just how.
//  * TTY: redraw in place only when the frame changed, at most once per
//    kMinTtyRedrawNs.
//  * Non-TTY: no ANSI; print a plain table when a feed's state/missed/reconnect
//    counters change or every kPlainPeriodNs, and each alert once as "ALERT: ...".
// Thread-safe: add_alert() may be called from any thread.
class Renderer {
public:
    static constexpr int64_t kMinTtyRedrawNs = 250'000'000LL;     // ~4 Hz cap
    static constexpr int64_t kPlainPeriodNs = 10'000'000'000LL;    // 10 s
    static constexpr size_t kMaxAlerts = 5;

    Renderer(std::ostream& out, bool is_tty) : out_(out), is_tty_(is_tty) {}

    void add_alert(const std::string& message);
    // Returns true if anything was written.
    bool render(const AggregateStats& stats, int64_t now_ns);

private:
    std::ostream& out_;
    const bool is_tty_;
    std::mutex mu_;
    std::deque<std::string> alerts_;
    std::vector<std::string> pending_plain_alerts_;
    std::string last_frame_;
    std::string last_signature_;
    int64_t last_draw_ns_ = 0;
    bool drawn_once_ = false;
};

// Process-wide Renderer bound to std::cout (is_tty = isatty(stdout)).
void render(const AggregateStats& stats);
void add_alert(const std::string& message);

}  // namespace cli_display
