#include "bench_common.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>

#include <sched.h>
#include <unistd.h>

namespace bench {

PercentileReport summarize(std::vector<int64_t> samples) {
    std::sort(samples.begin(), samples.end());
    PercentileReport r;
    r.count = samples.size();
    if (samples.empty()) {
        return r;
    }
    r.min = samples.front();
    r.max = static_cast<double>(samples.back());
    r.p50 = percentile(samples, 50.0);
    r.p90 = percentile(samples, 90.0);
    r.p99 = percentile(samples, 99.0);
    r.p999 = percentile(samples, 99.9);
    return r;
}

void print_report(const std::string& label, const PercentileReport& r) {
    std::cout << label << " (" << r.count << " samples)\n"
              << "  min=" << format_duration_ns(static_cast<double>(r.min)) << "  p50=" << format_duration_ns(r.p50)
              << "  p90=" << format_duration_ns(r.p90) << "  p99=" << format_duration_ns(r.p99)
              << "  p99.9=" << format_duration_ns(r.p999) << "  max=" << format_duration_ns(r.max) << "\n";
}

// min/median/max of each percentile across repetitions. Output format is
// parsed by scripts/generate_key_graphs.py.
void print_spread_report(const std::string& label, const std::vector<PercentileReport>& reports) {
    const auto line = [&](const char* name, auto extract) {
        const auto sp = summarize_spread(reports, extract);
        std::cout << "  " << name << ": min=" << format_duration_ns(sp.min) << " median=" << format_duration_ns(sp.median)
                  << " max=" << format_duration_ns(sp.max) << "\n";
    };
    std::cout << label << " across " << reports.size() << " repetitions (the 'max' of each row is the worst repetition):\n";
    line("p50  ", [](const PercentileReport& r) { return r.p50; });
    line("p90  ", [](const PercentileReport& r) { return r.p90; });
    line("p99  ", [](const PercentileReport& r) { return r.p99; });
    line("p99.9", [](const PercentileReport& r) { return r.p999; });
    line("max  ", [](const PercentileReport& r) { return r.max; });
}

namespace {

// Affinity mask before any pinning; captured on first use, which is
// try_pin_and_prioritize() in main() before any thread is spawned.
const cpu_set_t& original_affinity() {
    static const cpu_set_t mask = [] {
        cpu_set_t m;
        CPU_ZERO(&m);
        if (::sched_getaffinity(0, sizeof(m), &m) != 0) {
            // Query failed: fall back to all online CPUs.
            const long nprocs = ::sysconf(_SC_NPROCESSORS_ONLN);
            for (long i = 0; i < nprocs && i < CPU_SETSIZE; ++i) {
                CPU_SET(static_cast<int>(i), &m);
            }
        }
        return m;
    }();
    return mask;
}

}  // namespace

void try_pin_and_prioritize(int cpu_id) {
    const cpu_set_t& original = original_affinity();  // must capture before narrowing
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    if (!CPU_ISSET(cpu_id, &original) || ::sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
        std::cerr << "[bench] note: could not pin to cpu " << cpu_id << " (not fatal) -- numbers may be noisier\n";
    }

    sched_param param{};
    param.sched_priority = sched_get_priority_max(SCHED_FIFO);
    if (::sched_setscheduler(0, SCHED_FIFO, &param) != 0) {
        std::cerr << "[bench] note: sched_setscheduler(SCHED_FIFO) failed, likely missing CAP_SYS_NICE "
                     "(not fatal) -- numbers may be noisier\n";
    }
}

void reset_affinity_unpinned() {
    // Best-effort: on failure the thread just keeps its inherited settings.
    const sched_param param{};  // priority 0 is the only valid value for SCHED_OTHER
    ::sched_setscheduler(0, SCHED_OTHER, &param);
    const cpu_set_t& original = original_affinity();
    ::sched_setaffinity(0, sizeof(original), &original);
}

bool parse_positive_size(const char* text, size_t& out) {
    if (text == nullptr || *text == '\0') {
        return false;
    }
    // strtoull accepts leading whitespace and '-' (wrapping); require a digit first.
    if (*text < '0' || *text > '9') {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long v = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || v == 0 ||
        v > static_cast<unsigned long long>(std::numeric_limits<size_t>::max())) {
        return false;
    }
    out = static_cast<size_t>(v);
    return true;
}

int64_t current_rss_bytes() {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::istringstream iss(line.substr(6));
            int64_t kb = 0;
            iss >> kb;
            return kb * 1024;
        }
    }
    return 0;
}

}  // namespace bench
