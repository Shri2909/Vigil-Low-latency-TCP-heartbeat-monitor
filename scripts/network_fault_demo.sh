#!/usr/bin/env bash
# Kernel-level fault injection (tc/netem on loopback) against the real binaries:
# checks the heartbeat timeout still detects loss/delay and does not misfire on
# transient loss. Prints PASS/FAIL per phase from feed_monitor's plain-text table;
# exits non-zero if any assertion fails.
#
# Usage: sudo ./scripts/network_fault_demo.sh     (after `make all`)
#
# !! netem is attached to the loopback device, so it degrades ALL 127.0.0.1 / ::1
# !! traffic on this machine while the script runs. Do not run it on a shared or
# !! busy host. Needs root (CAP_NET_ADMIN); not run by `make test` or CI.
# The qdisc is removed on exit, including on error or Ctrl-C.

set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
    echo "error: this script modifies loopback's queueing discipline (tc qdisc) and needs root." >&2
    echo "  run: sudo $0" >&2
    exit 1
fi

if ! command -v tc >/dev/null 2>&1; then
    echo "error: 'tc' (iproute2) not found." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
BIN_DIR="${PROJECT_ROOT}/bin"

if [[ ! -x "${BIN_DIR}/feed_monitor" || ! -x "${BIN_DIR}/feed_simulator" ]]; then
    echo "error: ${BIN_DIR}/feed_monitor and/or feed_simulator not found -- run 'make all' first." >&2
    exit 1
fi

if tc qdisc show dev lo 2>/dev/null | grep -qE 'netem|tbf|htb|prio'; then
    echo "error: loopback already has a non-default qdisc; refusing to replace/delete it:" >&2
    tc qdisc show dev lo >&2
    exit 1
fi

SIM_PID=""
MONITOR_PID=""
QDISC_ACTIVE=0
CLEANED_UP=0
FAILURES=0
LOG="$(mktemp)"

cleanup() {
    local exit_code=$?
    # EXIT also fires after the INT/TERM handler calls exit; run once.
    if [[ "${CLEANED_UP}" -eq 1 ]]; then
        return
    fi
    CLEANED_UP=1
    echo
    echo "--- cleaning up ---"
    if [[ -n "${MONITOR_PID}" ]] && kill -0 "${MONITOR_PID}" 2>/dev/null; then
        kill -INT "${MONITOR_PID}" 2>/dev/null || true
        wait "${MONITOR_PID}" 2>/dev/null || true
    fi
    if [[ -n "${SIM_PID}" ]] && kill -0 "${SIM_PID}" 2>/dev/null; then
        kill -INT "${SIM_PID}" 2>/dev/null || true
        wait "${SIM_PID}" 2>/dev/null || true
    fi
    if [[ "${QDISC_ACTIVE}" -eq 1 ]]; then
        echo "removing netem qdisc from lo"
        tc qdisc del dev lo root 2>/dev/null || true
        QDISC_ACTIVE=0
    fi
    rm -f "${LOG}"
    exit "${exit_code}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Set the flag before the tc call so cleanup still tries the delete if tc fails
# or we are interrupted midway (deleting a missing qdisc is harmless).
qdisc_add() {
    QDISC_ACTIVE=1
    tc qdisc add dev lo root netem "$@"
}
qdisc_change() { tc qdisc change dev lo root netem "$@"; }
qdisc_del() {
    tc qdisc del dev lo root
    QDISC_ACTIVE=0
}

# The monitor's plain-text table goes to ${LOG}. last_state is the feed's most
# recent state over the whole log; saw_state looks only at output since phase_begin.
PHASE_START=0
phase_begin() { PHASE_START=$(stat -c %s "${LOG}"); }
phase_output() { tail -c +"$((PHASE_START + 1))" "${LOG}"; }
last_state() {
    grep -E '^1 +NETEM +' "${LOG}" | awk '{print $3}' | tail -n 1
}
saw_state() { phase_output | grep -E '^1 +NETEM +'"$1"'( |$)' >/dev/null; }

assert_pass() { echo "    PASS: $1"; }
assert_fail() { echo "    FAIL: $1"; FAILURES=$((FAILURES + 1)); }

PORT=19700
FEED_ID=1

echo "--- starting feed_simulator (no fault injection at the application layer --"
echo "    all faults below are injected by the kernel via netem) ---"
"${BIN_DIR}/feed_simulator" --port "${PORT}" --feed-id "${FEED_ID}" &
SIM_PID=$!
sleep 0.5

echo "--- starting feed_monitor (heartbeat_interval=500ms, heartbeat_timeout=1500ms) ---"
"${BIN_DIR}/feed_monitor" \
    --feed "${FEED_ID}:NETEM:127.0.0.1:${PORT}" \
    --heartbeat-interval-ms 500 \
    --heartbeat-timeout-ms 1500 \
    --max-missed 2 \
    --reconnect-base-delay-ms 500 \
    --reconnect-max-delay-ms 3000 \
    >"${LOG}" 2>&1 </dev/null &
MONITOR_PID=$!

sleep 3
echo
echo "=== Phase 1: baseline, no kernel-level fault -- feed should be HEALTHY ==="
phase_begin
sleep 3
if [[ "$(last_state)" == "HEALTHY" ]]; then assert_pass "feed HEALTHY at baseline"; else assert_fail "feed not HEALTHY at baseline (last state: '$(last_state)')"; fi

echo
echo "=== Phase 2: 40% packet loss on loopback (tc qdisc netem loss 40%) ==="
echo "    Expect: intermittent misses, but NOT a full failure (40% loss still lets"
echo "    plenty of heartbeats through within heartbeat_timeout=1500ms)."
phase_begin
qdisc_add loss 40%
sleep 6
if saw_state FAILED; then assert_fail "feed reached FAILED under 40% loss (should survive transient loss)"; else assert_pass "feed never FAILED under 40% loss"; fi

echo
echo "=== Phase 3: 100% packet loss (tc qdisc netem loss 100%) ==="
echo "    Expect: HEALTHY -> DEGRADED -> FAILED -> reconnect attempts (which will"
echo "    also be dropped, since loss applies to all loopback traffic here)."
phase_begin
qdisc_change loss 100%
sleep 6
if saw_state FAILED; then assert_pass "feed reached FAILED under 100% loss"; else assert_fail "feed never reached FAILED under 100% loss"; fi

echo
echo "=== Phase 4: fault removed -- expect reconnect to succeed, HEALTHY again ==="
phase_begin
qdisc_del
sleep 6
if [[ "$(last_state)" == "HEALTHY" ]]; then assert_pass "feed recovered to HEALTHY"; else assert_fail "feed did not recover (last state: '$(last_state)')"; fi

echo
echo "=== Phase 5: 300ms extra latency + 25% reordering (tc qdisc netem delay/reorder) ==="
echo "    Expect: still HEALTHY throughout -- 300ms is comfortably under the"
echo "    1500ms heartbeat_timeout, and reordering alone (not loss) shouldn't"
echo "    cost any heartbeats, just their arrival order."
phase_begin
qdisc_add delay 300ms reorder 25% 50%
sleep 6
# 600ms RTT is within the 1500ms timeout: no FAILED/DEGRADED.
if saw_state FAILED || saw_state DEGRADED; then assert_fail "feed left HEALTHY under 300ms delay"; else assert_pass "feed stayed HEALTHY under 300ms delay + reordering"; fi
qdisc_del

echo
if [[ "${FAILURES}" -eq 0 ]]; then
    echo "--- demonstration complete: all phase assertions PASSED ---"
else
    echo "--- demonstration complete: ${FAILURES} phase assertion(s) FAILED ---" >&2
    echo "    (monitor output follows)" >&2
    cat "${LOG}" >&2
    exit 1
fi
