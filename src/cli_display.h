#pragma once

// Renders an AggregateStats snapshot as an in-place-refreshing terminal
// table, similar to `top` -- no curses dependency (PROJECT_PLAN.md section
// 9). Deliberately depends on feed_monitor.h (for AggregateStats/FeedSnapshot)
// but FeedMonitor does NOT depend back on this header -- see the callback
// design in feed_monitor.h's set_tick_callback, which is what keeps the
// reactor itself free of any rendering concern.

#include "feed_monitor.h"

#include <cstdint>
#include <string>

namespace cli_display {

// Pure formatting function -- exposed separately from render() so it's
// testable without capturing stdout. now_ns drives the UPTIME column;
// use_ansi controls whether the in-place-redraw escape codes are emitted.
std::string format_table(const AggregateStats& stats, bool use_ansi, int64_t now_ns);

// Redraws the full table in place each call (cursor-home + clear-to-end when
// stdout is a real terminal; a plain sequential dump otherwise, so output
// redirected to a file or pipe stays readable instead of filling up with
// escape codes). Safe to call repeatedly on a timer.
void render(const AggregateStats& stats);

}  // namespace cli_display
