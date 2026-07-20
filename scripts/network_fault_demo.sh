#!/usr/bin/env bash
# Real network-layer fault injection against the shipped binaries, using
# tc/netem to impose actual kernel-level packet loss/delay/reordering on
# loopback -- not the application-layer fault injection FeedSimulator's own
# FaultConfig provides (drop_probability, extra_latency), which is a
# perfectly good tool for the state-machine tests elsewhere in this project
# but never touches the real network stack. This is the complementary,
# lower-layer check: does the heartbeat-timeout mechanism still correctly
# detect trouble when the *kernel*, not the application, is the one dropping
# or delaying packets, and does it correctly *not* misfire under transient
# loss that a real WAN path routinely produces.
#
# Requires root (tc qdisc manipulation needs CAP_NET_ADMIN). Not run as part
# of `make test` or any CI -- CI/automated runs can't assume root, and
# mutating a real network interface's queueing discipline (even loopback)
# is exactly the kind of system-wide change that shouldn't happen silently
# as a side effect of an ordinary test run. Run manually:
#
#   sudo ./scripts/network_fault_demo.sh
#
# Tears the qdisc back down unconditionally on exit (trap covers normal
# completion, an error, or Ctrl-C) so a failed or interrupted run never
# leaves loopback permanently degraded.

set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
    echo "error: this script modifies loopback's queueing discipline (tc qdisc) and needs root." >&2
    echo "  run: sudo $0" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
BIN_DIR="${PROJECT_ROOT}/bin"

if [[ ! -x "${BIN_DIR}/feed_monitor" || ! -x "${BIN_DIR}/feed_simulator" ]]; then
    echo "error: ${BIN_DIR}/feed_monitor and/or feed_simulator not found -- run 'make all' first." >&2
    exit 1
fi

SIM_PID=""
MONITOR_PID=""
QDISC_ACTIVE=0

cleanup() {
    local exit_code=$?
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
    fi
    exit "${exit_code}"
}
trap cleanup EXIT INT TERM

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
    --reconnect-max-delay-ms 3000 &
MONITOR_PID=$!

sleep 3
echo
echo "=== Phase 1: baseline, no kernel-level fault -- feed should be HEALTHY above ==="
sleep 3

echo
echo "=== Phase 2: 40% packet loss on loopback (tc qdisc netem loss 40%) ==="
echo "    Expect: intermittent misses, but NOT a full failure (40% loss still lets"
echo "    plenty of heartbeats through within heartbeat_timeout=1500ms)."
tc qdisc add dev lo root netem loss 40%
QDISC_ACTIVE=1
sleep 6

echo
echo "=== Phase 3: 100% packet loss (tc qdisc netem loss 100%) ==="
echo "    Expect: HEALTHY -> DEGRADED -> FAILED -> reconnect attempts (which will"
echo "    also be dropped, since loss applies to all loopback traffic here)."
tc qdisc change dev lo root netem loss 100%
sleep 6

echo
echo "=== Phase 4: fault removed -- expect reconnect to succeed, HEALTHY again ==="
tc qdisc del dev lo root
QDISC_ACTIVE=0
sleep 6

echo
echo "=== Phase 5: 300ms extra latency + 25% reordering (tc qdisc netem delay/reorder) ==="
echo "    Expect: still HEALTHY throughout -- 300ms is comfortably under the"
echo "    1500ms heartbeat_timeout, and reordering alone (not loss) shouldn't"
echo "    cost any heartbeats, just their arrival order."
tc qdisc add dev lo root netem delay 300ms reorder 25% 50%
QDISC_ACTIVE=1
sleep 6
tc qdisc del dev lo root
QDISC_ACTIVE=0

echo
echo "--- demonstration complete -- see the feed_monitor output above for the actual"
echo "    observed state transitions through each phase ---"
