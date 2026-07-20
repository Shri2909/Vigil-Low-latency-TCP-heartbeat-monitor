#include "stats.h"

double percentile(const std::vector<int64_t>& sorted_samples, double p) {
    if (sorted_samples.empty()) {
        return 0.0;
    }
    if (p <= 0.0) {
        return static_cast<double>(sorted_samples.front());
    }
    if (p >= 100.0) {
        return static_cast<double>(sorted_samples.back());
    }

    const double rank = (p / 100.0) * static_cast<double>(sorted_samples.size() - 1);
    const size_t lo = static_cast<size_t>(rank);
    const size_t hi = std::min(lo + 1, sorted_samples.size() - 1);
    const double frac = rank - static_cast<double>(lo);
    return static_cast<double>(sorted_samples[lo]) * (1.0 - frac) +
           static_cast<double>(sorted_samples[hi]) * frac;
}
