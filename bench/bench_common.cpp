#include "bench_common.h"

#include <algorithm>
#include <fstream>
#include <iostream>
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

void try_pin_and_prioritize(int cpu_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    if (::sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
        std::cerr << "[bench] note: sched_setaffinity failed (not fatal) -- numbers may be noisier\n";
    }

    sched_param param{};
    param.sched_priority = sched_get_priority_max(SCHED_FIFO);
    if (::sched_setscheduler(0, SCHED_FIFO, &param) != 0) {
        std::cerr << "[bench] note: sched_setscheduler(SCHED_FIFO) failed, likely missing CAP_SYS_NICE "
                     "(not fatal) -- numbers may be noisier\n";
    }
}

void reset_affinity_unpinned() {
    const long nprocs = ::sysconf(_SC_NPROCESSORS_ONLN);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (long i = 0; i < nprocs; ++i) {
        CPU_SET(static_cast<int>(i), &cpuset);
    }
    // Best-effort, same as try_pin_and_prioritize -- a failure here just
    // means this thread stays on whatever mask it inherited, not a fatal
    // benchmark error.
    ::sched_setaffinity(0, sizeof(cpuset), &cpuset);
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
