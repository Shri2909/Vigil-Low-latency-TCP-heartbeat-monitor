#pragma once

// Shared benchmark helpers: percentile summaries, repeated-trial spread, CLI
// parsing, CPU pinning, RSS.

#include "stats.h"
#include "time_utils.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

namespace bench {

struct PercentileReport {
    size_t count = 0;
    int64_t min = 0;
    double p50 = 0.0;
    double p90 = 0.0;
    double p99 = 0.0;
    double p999 = 0.0;
    double max = 0.0;
};

// Sorts a copy of the samples and reduces them to the percentiles above (via
// stats.h's percentile(), shared with the live CLI display).
PercentileReport summarize(std::vector<int64_t> samples);

void print_report(const std::string& label, const PercentileReport& report);

// min/median/max of each percentile across repetitions. Output format is parsed
// by scripts/generate_key_graphs.py.
void print_spread_report(const std::string& label, const std::vector<PercentileReport>& reports);

// Strict positive-integer parse: rejects empty, non-numeric, trailing garbage,
// negatives, zero and overflow. Leaves `out` untouched on rejection.
bool parse_positive_size(const char* text, size_t& out);

// Best-effort: pins the calling thread to `cpu_id` and requests SCHED_FIFO.
// Prints a note and carries on if the process lacks permission.
void try_pin_and_prioritize(int cpu_id = 0);

// Restores SCHED_OTHER and the affinity mask the process had before
// try_pin_and_prioritize(). Call first thing in every spawned thread that is
// not the measured one: threads inherit the creator's pin and SCHED_FIFO.
void reset_affinity_unpinned();

// Process RSS in bytes from /proc/self/status (0 if unavailable).
int64_t current_rss_bytes();

// Calls fn() `n` times and returns the results in call order.
template <typename Fn>
auto repeat_trials(Fn&& fn, size_t n = 5) -> std::vector<std::invoke_result_t<Fn>> {
    std::vector<std::invoke_result_t<Fn>> results;
    results.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        results.push_back(fn());
    }
    return results;
}

// min/median/max of one numeric metric across repeated-trial results.
struct Spread {
    double min = 0.0;
    double median = 0.0;
    double max = 0.0;
};

template <typename T, typename Extract>
Spread summarize_spread(const std::vector<T>& results, Extract&& extract) {
    std::vector<double> values;
    values.reserve(results.size());
    for (const auto& r : results) {
        values.push_back(static_cast<double>(extract(r)));
    }
    std::sort(values.begin(), values.end());
    Spread s;
    if (values.empty()) {
        return s;
    }
    s.min = values.front();
    s.max = values.back();
    const size_t mid = values.size() / 2;
    s.median = (values.size() % 2 == 1) ? values[mid] : (values[mid - 1] + values[mid]) / 2.0;
    return s;
}

}  // namespace bench
