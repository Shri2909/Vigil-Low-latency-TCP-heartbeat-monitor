#!/usr/bin/env bash
# Clean rebuild -> latency + throughput benchmarks -> regenerate charts.
# Output goes to a temp dir and replaces bench/results/*.txt and bench/charts/*.png
# only after every step succeeds, so a failed run leaves the committed files untouched.
#
# Usage: bash scripts/run_benchmarks_and_plot.sh        (about 8-10 minutes with defaults)
#
# Environment (all optional):
#   COUNTS=10,100,1000,5000   connection counts for the throughput runs
#   REPS=3                    repetitions per throughput configuration
#   LAT_MEASURED=20000  LAT_WARMUP=500  LAT_REPS=5    latency_bench arguments
#   SUITE_GAP_SEC=60          pause between the epoll and naive suites (TIME_WAIT drain)
#   LAT_CPUS=<mask>           taskset mask for the main latency run (default: none; the
#                             benchmark pins its client to CPU 0 itself)
#   LAT_ISOLATED_CPUS=<mask>  also run latency under `taskset -c <mask>` (e.g. one isolated
#                             core) and save latency_bench_isolated.txt
#   THROUGHPUT_CPUS=<mask>    taskset mask for BOTH the epoll and naive throughput runs; the
#                             same mask keeps the comparison fair (avoid isolcpus cores)
# Example (the committed results' machine):
#   LAT_ISOLATED_CPUS=4 THROUGHPUT_CPUS=6-9,11 REPS=5 bash scripts/run_benchmarks_and_plot.sh
#
# CPU governor and setcap changes are best-effort via `sudo -n` (never prompts); the
# governor is restored on exit. Warns if the CPU thermal-throttle counter rose during a
# run (discard such a run) and prints TCP listen-overflow counters when `nstat` exists.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

COUNTS="${COUNTS:-10,100,1000,5000}"
REPS="${REPS:-3}"
LAT_MEASURED="${LAT_MEASURED:-20000}"
LAT_WARMUP="${LAT_WARMUP:-500}"
LAT_REPS="${LAT_REPS:-5}"
LAT_CPUS="${LAT_CPUS:-}"
LAT_ISOLATED_CPUS="${LAT_ISOLATED_CPUS:-}"
THROUGHPUT_CPUS="${THROUGHPUT_CPUS:-}"
SUITE_GAP_SEC="${SUITE_GAP_SEC:-60}"

run_pinned() {  # run_pinned <mask-or-empty> <command...>
    local mask="$1"; shift
    if [[ -n "${mask}" ]]; then taskset -c "${mask}" "$@"; else "$@"; fi
}

# Thermal throttling invalidates a run: record the counter before, compare after.
THROTTLE_FILE="/sys/devices/system/cpu/cpu0/thermal_throttle/package_throttle_count"
throttle_count() { [[ -r "${THROTTLE_FILE}" ]] && cat "${THROTTLE_FILE}" || echo ""; }
check_throttle() {  # check_throttle <label> <count-before>
    local after; after="$(throttle_count)"
    if [[ -n "$2" && -n "${after}" && "${after}" != "$2" ]]; then
        echo "WARNING: package thermal-throttle count rose ($2 -> ${after}) during '$1' -- let the machine cool and re-run; this result is thermally limited." >&2
    fi
}
net_counters() {
    if command -v nstat >/dev/null 2>&1; then
        echo "--- tcp counters ($1) -- expect ListenOverflows/ListenDrops = 0 ---"
        nstat 2>/dev/null | grep -E 'ListenOverflows|ListenDrops|ReqQFull|TimeWaitOverflow|RetransSegs' || echo "(all zero)"
    fi
}

RESULTS_DIR="bench/results"
CHARTS_DIR="bench/charts"
mkdir -p "${RESULTS_DIR}" "${CHARTS_DIR}"

# Same filesystem as the destination, so the final mv is an atomic rename.
TMP_DIR="$(mktemp -d "bench/.run.XXXXXX")"
GOVERNOR_FILE="/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
ORIG_GOVERNOR=""
GOVERNOR_CHANGED=0

cleanup() {
    local rc=$?
    if [[ "${GOVERNOR_CHANGED}" -eq 1 && -n "${ORIG_GOVERNOR}" ]]; then
        echo "restoring CPU governor to '${ORIG_GOVERNOR}'"
        sudo -n cpupower frequency-set -g "${ORIG_GOVERNOR}" >/dev/null 2>&1 \
            || echo "warning: could not restore CPU governor to '${ORIG_GOVERNOR}' -- restore it manually" >&2
    fi
    rm -rf "${TMP_DIR}"
    if [[ ${rc} -ne 0 ]]; then
        echo "FAILED (exit ${rc}) -- committed results in ${RESULTS_DIR}/ and ${CHARTS_DIR}/ were left untouched." >&2
    fi
    exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

have_sudo() { command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; }

echo "=== CPU tuning (best-effort) ==="
if command -v cpupower >/dev/null 2>&1 && have_sudo; then
    [[ -r "${GOVERNOR_FILE}" ]] && ORIG_GOVERNOR="$(cat "${GOVERNOR_FILE}")"
    if [[ -n "${ORIG_GOVERNOR}" && "${ORIG_GOVERNOR}" != "performance" ]]; then
        if sudo -n cpupower frequency-set -g performance >/dev/null; then
            GOVERNOR_CHANGED=1
            echo "governor: ${ORIG_GOVERNOR} -> performance (restored on exit)"
        else
            echo "cpupower failed -- continuing without it"
        fi
    else
        echo "governor already '${ORIG_GOVERNOR:-unknown}' -- nothing to change"
    fi
else
    echo "skipping governor change (needs cpupower and passwordless sudo: 'sudo -n')"
fi

echo
echo "=== clean rebuild ==="
make clean
make all
make bench

echo
echo "=== best-effort: grant CAP_SYS_NICE to latency_bench (SCHED_FIFO) ==="
# Rebuilds drop file capabilities, so this runs after every build. Only
# latency_bench uses SCHED_FIFO.
if command -v setcap >/dev/null 2>&1 && have_sudo; then
    sudo -n setcap cap_sys_nice+ep bin/latency_bench || echo "setcap failed -- continuing without it"
else
    echo "skipping setcap (needs setcap and passwordless sudo: 'sudo -n'); latency numbers may be noisier"
fi

echo
echo "=== running latency_bench (${LAT_MEASURED} round trips x ${LAT_REPS} repetitions) ==="
T0="$(throttle_count)"
run_pinned "${LAT_CPUS}" ./bin/latency_bench "${LAT_MEASURED}" "${LAT_WARMUP}" "${LAT_REPS}" | tee "${TMP_DIR}/latency_bench.txt"
check_throttle "latency_bench" "${T0}"

if [[ -n "${LAT_ISOLATED_CPUS}" ]]; then
    echo
    echo "=== running latency_bench pinned to CPUs ${LAT_ISOLATED_CPUS} (isolated-core variant) ==="
    T0="$(throttle_count)"
    run_pinned "${LAT_ISOLATED_CPUS}" ./bin/latency_bench "${LAT_MEASURED}" "${LAT_WARMUP}" "${LAT_REPS}" | tee "${TMP_DIR}/latency_bench_isolated.txt"
    check_throttle "latency_bench (isolated)" "${T0}"
fi

echo
echo "=== running raw_socket_baseline (kernel-floor reference for the latency numbers) ==="
T0="$(throttle_count)"
run_pinned "${LAT_CPUS}" ./bin/raw_socket_baseline "${LAT_MEASURED}" "${LAT_WARMUP}" "${LAT_REPS}" | tee "${TMP_DIR}/raw_socket_baseline.txt"
check_throttle "raw_socket_baseline" "${T0}"

echo
echo "=== running throughput_bench: epoll connection-count ramp + epoll_max_events sweep ==="
nstat -n >/dev/null 2>&1 || true
T0="$(throttle_count)"
run_pinned "${THROUGHPUT_CPUS}" ./bin/throughput_bench "${COUNTS}" 1000 "${REPS}" | tee "${TMP_DIR}/throughput_bench_epoll.txt"
check_throttle "throughput_bench (epoll)" "${T0}"
net_counters "epoll suite"
echo "(${SUITE_GAP_SEC} s pause so TIME_WAIT sockets drain before the next suite)"
sleep "${SUITE_GAP_SEC}"

echo
echo "=== running throughput_bench --naive-only at N=${COUNTS} ==="
# One process per N so each naive RSS reading starts from a clean heap.
nstat -n >/dev/null 2>&1 || true
T0="$(throttle_count)"
{
    IFS=',' read -ra COUNT_ARR <<< "${COUNTS}"
    for n in "${COUNT_ARR[@]}"; do
        run_pinned "${THROUGHPUT_CPUS}" ./bin/throughput_bench --naive-only "${n}" 1000 "${REPS}"
    done
} | tee "${TMP_DIR}/throughput_bench_naive.txt"
check_throttle "throughput_bench (naive)" "${T0}"
net_counters "naive suite"

# Environment snapshot of this run, saved next to the results.
{
    echo "run date      : $(date -Is)"
    echo "git commit    : $(git rev-parse --short HEAD 2>/dev/null || echo unknown)$(git diff --quiet 2>/dev/null || echo ' (+uncommitted changes)')"
    echo "kernel        : $(uname -r)"
    echo "cmdline       : $(cat /proc/cmdline)"
    echo "isolated cpus : $(cat /sys/devices/system/cpu/isolated 2>/dev/null)"
    echo "governor cpu0 : $(cat "${GOVERNOR_FILE}" 2>/dev/null)"
    echo "AC online     : $(cat /sys/class/power_supply/ADP1/online 2>/dev/null || echo n/a)"
    echo "turbo disabled: $(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo n/a)"
    echo "latency mask  : ${LAT_CPUS:-<none: benchmark pins client to CPU 0>}   isolated variant: ${LAT_ISOLATED_CPUS:-<not run>}"
    echo "throughput mask (epoll and naive): ${THROUGHPUT_CPUS:-<none>}"
    echo "counts=${COUNTS} reps=${REPS} lat=${LAT_MEASURED}x${LAT_REPS} (warmup ${LAT_WARMUP})"
    echo "compiler      : $(g++ --version | head -1)"
} > "${TMP_DIR}/environment_snapshot.txt"

echo
echo "=== generating charts from the new results ==="
mkdir -p "${TMP_DIR}/charts"
python3 scripts/generate_key_graphs.py --results-dir "${TMP_DIR}" --out-dir "${TMP_DIR}/charts"

echo
echo "=== all steps succeeded -- replacing committed results and charts ==="
mv -f "${TMP_DIR}"/latency_bench.txt "${TMP_DIR}"/throughput_bench_epoll.txt "${TMP_DIR}"/throughput_bench_naive.txt \
      "${TMP_DIR}"/environment_snapshot.txt "${RESULTS_DIR}/"
mv -f "${TMP_DIR}/raw_socket_baseline.txt" "${RESULTS_DIR}/"
if [[ -f "${TMP_DIR}/latency_bench_isolated.txt" ]]; then
    mv -f "${TMP_DIR}/latency_bench_isolated.txt" "${RESULTS_DIR}/"
fi
mv -f "${TMP_DIR}"/charts/*.png "${CHARTS_DIR}/"

echo
echo "done. Raw output: ${RESULTS_DIR}/   Charts: ${CHARTS_DIR}/"
