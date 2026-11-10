# Vigil: Scope and History

What the project set out to do, what it deliberately leaves out, what auditing found, and what comes next. Decisions and their trade-offs live in one place: [ARCHITECTURE.md](ARCHITECTURE.md#key-design-decisions). Numbers and how they were measured: [METHODOLOGY.md](METHODOLOGY.md) and [README.md](README.md).

## Goals

1. **Detect silent failure.** A half-open TCP connection looks healthy to the kernel, so detection comes from an application-level heartbeat, not socket errors or TCP keepalive.
2. **Distrust, then recover.** Degrade on the first missed heartbeat, fail after three, reconnect with capped exponential backoff and jitter.
3. **Scale cheaply and measure it honestly** against a fair thread-per-connection baseline.
4. **Be verifiable.** Real sockets in every test, sanitizers, fuzzing, CI, and fault injection at the application and kernel layers.
5. **No external dependencies.** C++20, Linux, a Makefile and a small in-repo test harness.

## Non-goals

Encryption or authentication (CRC32 detects corruption only; the `version` byte is the extension point), DNS and IPv6, persistent history and alert routing, kernel bypass (no benefit on loopback), a curses UI, and non-Linux platforms.

## Audit and fix highlights

The work went in repeated passes: build or extend, attack it for gaps, fix with a regression test that was shown to fail first, run the sanitizer sweep. Representative findings, all fixed:

- A stale `PONG` (round trip beyond the timeout) and a future-timestamped one were accepted as proof of health; both are now rejected.
- A reordered `PONG` was misclassified as a duplicate; acknowledgement is now tracked by membership, not a high-water mark.
- A final `PONG` arriving with the peer's EOF was discarded because EOF returned before the buffer drained; fixed everywhere the pattern appeared.
- `remove_slot_at` could re-insert a stale fd entry for a moved `FAILED` slot, and `force_reconnect` did not reset the attempt counter.
- Shrinking `heartbeat_timeout` mid-flight could spuriously degrade a healthy feed; liveness baselines now reset on change.
- Responder connections could occupy a slot forever if a client went silent mid-handshake; the simulator now reaps them.
- Outbound queue, inbound accumulator and command lines gained 64 KB caps that fail closed, and numeric config and CLI fields are range-checked.
- The benchmark harness itself had bias (trial order, process-wide CPU accounting, cadence, thread pinning). The original "5x cheaper" result disappeared once fixed: [the story](METHODOLOGY.md#bugs-in-my-own-benchmark).

Outcome: 178 tests, clean under ASan, UBSan and TSan, `-Werror` on g++ and clang++, two libFuzzer harnesses, CI on every push.

## Known gaps

CRC32 is not tamper-proof; one reactor thread is a one-core ceiling with shared fate; results are loopback on one machine; root-requiring and long-running checks (kernel `netem` demo, benchmarks, full-length fuzzing) are manual by design. Details: [ARCHITECTURE.md](ARCHITECTURE.md#limits-and-non-goals).

## Future work

- **Multi-reactor scaling:** N independent single-threaded reactors, one per core, feeds distributed round-robin, keeping the no-shared-state property per reactor.
- **Hostname resolution** through a worker thread or non-blocking resolver, and IPv6.
- **Authentication or TLS**, negotiated via the protocol `version` byte.
- **Adaptive heartbeat intervals** and per-feed timing overrides.
- **Alerting and metrics export** (webhook or Prometheus-style).
- **Real feed adapters** speaking an actual exchange's heartbeat dialect.
