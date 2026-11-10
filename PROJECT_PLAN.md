# Vigil: Design Decisions

A short record of what the project set out to do, the decisions that shaped it and why, what was deliberately left out, and what auditing found. For the structure of the finished system see [ARCHITECTURE.md](ARCHITECTURE.md); for how it was validated see [METHODOLOGY.md](METHODOLOGY.md); for results see [README.md](README.md).

## Goals

Build a feed-liveness monitor in the shape a market-data gateway needs:

1. **Detect silent failure.** A half-open TCP connection looks healthy to the kernel, so detection must come from a heartbeat enforced above TCP, not from socket errors or TCP keepalive (too slow and OS-dependent).
2. **Stop trusting, then recover.** Degrade on the first missed heartbeat, fail after several, and reconnect automatically with exponential backoff and jitter.
3. **Scale to thousands of feeds cheaply**, and measure that claim honestly against a fair thread-per-connection baseline.
4. **Be verifiable.** Real sockets in every test, sanitizers, fuzzing, CI, and fault injection at both the application and kernel layers.
5. **No external dependencies.** C++20, Linux, a Makefile, and a small in-repo test harness.

## Scope

In: two binaries (`feed_monitor`, `feed_simulator`), a fixed-size wire protocol, per-feed state machine, backoff reconnect, live CLI table and stdin commands, config file and CLI flags, tests, benchmarks, fuzzing, CI.

Out, by decision: encryption or authentication (CRC32 detects corruption only; the wire `version` byte is the extension point), DNS and IPv6 (the reactor must never block in `getaddrinfo`), persistent history and alert routing (`stderr` logging; use `logrotate` or a supervisor), kernel bypass (no benefit on loopback), a curses TUI (scope creep and a dependency), Windows and macOS (Linux syscalls throughout).

## Key design decisions

| Decision | Rationale |
|---|---|
| Add a first-class `FeedSimulator` (its own binary) | A monitor with nothing to connect to cannot be run, tested or demoed. The simulator also hosts fault injection (drop, delay, kill). |
| Fixed 28-byte big-endian message with CRC32 | Framing is trivial (accumulate 28 bytes, decode); no length prefix, no delimiter scanning, no partial-message state machine. A `static_assert` pins the layout. |
| Bidirectional PING/PONG, monitor is the only initiator | One-way timestamps need synchronized clocks. The monitor timestamps with its own clock and the feed echoes it, so round-trip time needs no synchronization. Mirrors how real heartbeat exchanges behave. |
| Explicit `ConnectionState` machine (CONNECTING, HANDSHAKING, HEALTHY, DEGRADED, FAILED) | "Healthy", "half-open" and "reconnecting" are all behaviors the system claims; they need defined transitions that tests can hit at exact injected-time boundaries. |
| Heartbeat-timeout detection via `timerfd` is primary; socket errors are a fast path | Keepalive cannot be the signal. Errors and EOF fail a connection immediately, but silence is the case that matters. |
| One single-threaded `epoll` reactor owning sockets, timers, stdin and stats | No locks, no cross-thread hot path, one place to reason about ordering and failure. Operator input is just another fd in the same `epoll` set, so commands run on the reactor thread. |
| Edge-triggered `epoll` by default (level-triggered selectable) | Idiomatic for a low-latency design; the cost is the drain-to-`EAGAIN` discipline, applied in every read and accept loop. |
| Per-connection read and write accumulation buffers | Non-blocking sockets produce short writes and split messages routinely. |
| Every buffer capped (64 KB read, write and command line), failing the connection on overflow | Unbounded growth from a hostile or wedged peer is a failure mode; silently dropping bytes would desync the protocol, which is worse than reconnecting. |
| `PONG` is liveness evidence only if its round trip is non-negative and below the timeout | A late reply does not prove current health; a future-timestamped one is not physically meaningful. Both are remembered as seen but do not reset the liveness clock. |
| Backoff `min(base * 2^attempt, max)` with plus or minus 25% jitter; attempt counter resets after a stable period | Correlated failures must not retry in lockstep. A feed that has been healthy for a full timeout earns a fresh budget. |
| `CLOCK_BOOTTIME` instead of `CLOCK_MONOTONIC` | Advances across suspend, so a feed unreachable during a suspend reads as having missed its heartbeats. |
| Hand-rolled `key = value` config with `[feed]` sections, `getopt_long` CLI; bad input is reported and skipped, never fatal | No parser dependency; a typo in one line must not take down monitoring of every other feed. Precedence: CLI, then file, then defaults. |
| Header-only mini test harness (`test/mini_test.h`) | GoogleTest or Catch2 would need a package manager or vendored source, which conflicts with a Makefile-only build. |
| `RLIMIT_NOFILE` raised to the hard limit at startup | A 1,024 soft limit would silently cap "thousands of feeds". |
| `SIGINT`/`SIGTERM` handled with an atomic flag plus an `eventfd` write; `SIGPIPE` ignored and `MSG_NOSIGNAL` on every send | Only async-signal-safe work in handlers; the loop wakes and exits cleanly. |
| One `Connection` class for both roles | The monitor's initiator and the simulator's responder share framing, buffering and state code, so the simulator exercises the same code the monitor ships. |
| Benchmarks drive real classes over real loopback sockets, with a fair thread-per-connection baseline | A comparison against a strawman proves nothing; the baseline sleeps one full interval between pings, like the reactor's cadence. |

## Audit and fix highlights

The project went through repeated adversarial audit passes: build or extend, attack it for gaps, fix with a regression test that was proven able to fail, run the full sanitizer sweep, record the result (including "audited, confirmed correct"). Representative findings:

**Correctness**

- A stale `PONG` (round trip beyond the timeout) was accepted as proof of health; a future-timestamped one produced a negative RTT and was accepted too. Both are now rejected as evidence.
- A reordered `PONG` (possible under simulated latency) was misclassified as a duplicate.
- A final `PONG` arriving in the same instant as the peer's EOF was discarded because EOF returned before the buffer drained. The same pattern was fixed in all three places it appeared.
- `remove_slot_at` could re-insert a stale fd-to-slot entry for a moved failed slot, leaking map entries.
- `force_reconnect` did not reset the attempt counter, so a forced reconnect after exhaustion gave up again after one more failure.
- Shrinking `heartbeat_timeout` mid-flight could spuriously degrade a healthy feed; liveness baselines are now reset when it changes.
- Responder connections could occupy a slot forever if a client went silent or stalled in the handshake; the simulator now reaps them.
- The simulator's startup banner sat unflushed in a pipe-buffered stdout; found by the end-to-end test that runs the real binary.

**Hardening**

- Outbound queue, inbound accumulator (checked inside the recv loop, not only after it) and command lines are all capped.
- Numeric config and CLI fields are range-checked (a `feed_id` above 32 bits used to wrap silently); the simulator's `atoi`/`atof` (flagged by `clang-tidy`) were replaced with checked parsing so `--port abc` is an error rather than port 0.
- Display shows a dash instead of a fake `0.00ns` for a feed with no RTT samples yet.

**Benchmark methodology** (full table in [METHODOLOGY.md](METHODOLOGY.md))

- A fixed trial order confounded the `tcp_nodelay` comparison; CPU was measured process-wide so the echo peer was charged to the monitor; the trial cadence was 5x the shipped default; spawned threads inherited a single-CPU pin; the baseline woke 20 times a second; a per-tick stats copy inflated the reactor's cost. The original "5x cheaper" result disappeared once these were fixed, and a later "1.5x cheaper" did not survive machine tuning. The honest result is equal CPU and about 23% less memory.

**Verification outcome.** 178 tests; ASan, UBSan and TSan clean; `-Werror` on g++ and clang++; two libFuzzer harnesses with a valid-CRC seed corpus and a CRC-fixing mutator; line coverage measured per file; CI on every push; kernel-level fault injection via `tc`/`netem`. TSan's two findings were both in test code that called reactor methods from the wrong thread, which is what prompted routing every mutation through the command fd.

## Known gaps, stated plainly

- CRC32 has blind spots for some multi-bit error patterns and is not tamper-proof.
- The single reactor thread is a ceiling (one core) and a shared fate: one slow callback delays every feed.
- Benchmarks are loopback on one laptop; absolute values will differ elsewhere.
- Root-requiring and long-running checks (`network_fault_demo.sh`, benchmarks, full-length fuzzing) are manual by design.

## Future work

- **Multi-reactor scaling**: N independent single-threaded reactors, one per core, with feeds distributed round-robin, preserving the no-shared-state property per reactor.
- **Hostname resolution** through a worker thread or a non-blocking resolver, and IPv6.
- **Authentication or TLS**, using the protocol `version` byte to negotiate.
- **Adaptive heartbeat intervals** and per-feed timing overrides.
- **Alerting and metrics integration** (webhook or Prometheus-style export), and configurable log levels.
- **Real feed adapters** that speak an actual exchange's heartbeat dialect.
