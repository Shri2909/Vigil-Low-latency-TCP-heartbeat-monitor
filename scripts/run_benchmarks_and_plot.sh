#!/usr/bin/env bash
# One-shot: CPU tuning (best-effort) -> clean rebuild -> run every benchmark,
# saving raw output into bench/results/ -> regenerate bench/charts/*.png from
# those saved files (scripts/generate_key_graphs.py parses them; it does not
# use hardcoded numbers, so this script is the only place the numbers
# actually come from).
#
# CPU tuning and setcap both need root and are skipped gracefully if
# unavailable -- the benchmarks themselves print a note ("SCHED_FIFO failed,
# not fatal") and still produce valid, just slightly noisier, numbers without
# them. Nothing here fails the whole run over a missing privilege.
#
# Usage:
#   bash scripts/run_benchmarks_and_plot.sh

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

RESULTS_DIR="bench/results"
mkdir -p "${RESULTS_DIR}" bench/charts

echo "=== CPU tuning (best-effort) ==="
if command -v cpupower >/dev/null 2>&1; then
    sudo cpupower frequency-set -g performance || echo "cpupower failed/declined -- continuing without it"
else
    echo "cpupower not installed -- skipping governor change"
fi

echo
echo "=== clean rebuild ==="
make clean
make all
make bench

echo
echo "=== best-effort: grant CAP_SYS_NICE so the benchmarks can use SCHED_FIFO ==="
if command -v setcap >/dev/null 2>&1; then
    sudo setcap cap_sys_nice+ep bin/latency_bench || echo "setcap on latency_bench failed/declined -- continuing without it"
    sudo setcap cap_sys_nice+ep bin/throughput_bench || echo "setcap on throughput_bench failed/declined -- continuing without it"
else
    echo "setcap not installed -- skipping"
fi

echo
echo "=== running latency_bench (20,000 round trips x 5 repetitions) ==="
./bin/latency_bench 20000 500 5 | tee "${RESULTS_DIR}/latency_bench.txt"

echo
echo "=== running throughput_bench: epoll connection-count ramp + epoll_max_events sweep ==="
./bin/throughput_bench 10,100,1000,5000 | tee "${RESULTS_DIR}/throughput_bench_epoll.txt"

echo
echo "=== running throughput_bench --naive-only at N=10,100,1000,5000 ==="
{
    ./bin/throughput_bench --naive-only 10
    ./bin/throughput_bench --naive-only 100
    ./bin/throughput_bench --naive-only 1000
    ./bin/throughput_bench --naive-only 5000
} | tee "${RESULTS_DIR}/throughput_bench_naive.txt"

echo
echo "=== generating charts from the saved results above ==="
python3 scripts/generate_key_graphs.py

echo
echo "done. Raw output: ${RESULTS_DIR}/   Charts: bench/charts/"
