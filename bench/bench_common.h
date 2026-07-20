#pragma once

// Shared benchmarking utilities: warmup-then-measure sample collection,
// percentile reporting (reusing stats.h's percentile(), not a duplicate
// implementation), and best-effort CPU pinning. See PROJECT_PLAN.md section
// 12 and the reference-material notes in section 15.

#include "stats.h"
#include "time_utils.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

namespace bench {

// Runs `warmup_iters` throwaway iterations of fn (letting caches, branch
// predictors, and the kernel's TCP fast paths settle) before timing
// `measured_iters` more, recording one elapsed-ns sample per call.
//
// Portability note: Rasovsky's finding (cited in PROJECT_PLAN.md section 15)
// that clock_gettime(CLOCK_MONOTONIC) overhead relative to TSC-based timing
// is highly environment-dependent applies here -- read every number this
// produces as "measured on this machine, this run," not as a portable
// constant. That's also why every sample is individually timed rather than
// dividing one bulk elapsed time by iteration count: a single average would
// hide exactly the tail latency this benchmark exists to characterize.
template <typename Fn>
std::vector<int64_t> measure(Fn&& fn, size_t warmup_iters, size_t measured_iters) {
    for (size_t i = 0; i < warmup_iters; ++i) {
        fn();
    }
    std::vector<int64_t> samples;
    samples.reserve(measured_iters);
    for (size_t i = 0; i < measured_iters; ++i) {
        const int64_t t0 = now_monotonic_ns();
        fn();
        const int64_t t1 = now_monotonic_ns();
        samples.push_back(t1 - t0);
    }
    return samples;
}

struct PercentileReport {
    size_t count = 0;
    int64_t min = 0;
    double p50 = 0.0;
    double p90 = 0.0;
    double p99 = 0.0;
    double p999 = 0.0;
    double max = 0.0;
};

// Sorts a copy of samples and reduces it to the percentiles above, via the
// same percentile() used by the live CLI display's p99 column (stats.h) --
// one interpolation implementation for the whole project, not a
// benchmark-local reimplementation.
PercentileReport summarize(std::vector<int64_t> samples);

void print_report(const std::string& label, const PercentileReport& report);

// Best-effort: pins the calling thread to CPU `cpu_id` and requests
// SCHED_FIFO real-time priority, to reduce scheduling-noise-induced tail
// latency in the measurements that follow. Silently does nothing if the
// process lacks permission (CAP_SYS_NICE) -- an expected, non-fatal outcome
// when not run as root, not a benchmark failure; the numbers are just
// noisier without it.
void try_pin_and_prioritize(int cpu_id = 0);

// Undoes CPU-affinity inheritance for the calling thread. On Linux, a
// std::thread spawned via pthread_create with default attributes inherits
// the creating thread's CPU affinity mask (PTHREAD_INHERIT_SCHED) -- so any
// thread spawned *after* try_pin_and_prioritize() pins main() to one CPU
// silently inherits that same single-CPU restriction, with nothing in the
// code or output disclosing it. That's fine (even desirable) for the one
// thread actually doing latency-sensitive measurement, but wrong for
// anything else the benchmark spawns: a simulator/peer thread forced onto
// the same core as the pinned measurement thread now contends with it for
// time slices, adding exactly the scheduling noise pinning was meant to
// remove: in --naive mode this could mean hundreds of worker threads all
// fighting over one core, partly measuring thread-contention-on-one-core
// rather than the architecture under test. Call this as the first thing
// inside any spawned thread's lambda that shouldn't stay pinned.
void reset_affinity_unpinned();

// Current process RSS in bytes, read from /proc/self/status. Returns 0 if
// unavailable (e.g. non-Linux -- though this project targets Linux only, so
// that's a defensive fallback, not an expected path).
int64_t current_rss_bytes();

// Runs fn() `n` times back to back, returning the raw per-call results in
// call order. This exists because neither benchmark repeated a configuration
// more than once before this audit pass -- every number was a single-sample
// point estimate with no reported variance (PROJECT_PLAN.md F10). Ordering
// is preserved (not just the summary) because latency_bench's F7 fix needs
// to interleave two configurations across repetitions to cancel out a
// cross-trial confound, which requires the caller to control what fn() does
// on each individual call, not just how many times it's called.
template <typename Fn>
auto repeat_trials(Fn&& fn, size_t n = 5) -> std::vector<std::invoke_result_t<Fn>> {
    std::vector<std::invoke_result_t<Fn>> results;
    results.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        results.push_back(fn());
    }
    return results;
}

// min/median/max of one numeric metric extracted from each of a set of
// repeated-trial results, e.g. summarize_spread(reports, [](auto& r){ return
// r.p99; }) -- the "spread across repetitions" F7 and F10 call for, instead
// of a single point estimate per configuration.
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
    s.median = values[values.size() / 2];
    return s;
}

}  // namespace bench
