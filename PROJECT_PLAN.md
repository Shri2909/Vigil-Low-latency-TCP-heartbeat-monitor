# Vigil — Low-Latency TCP Heartbeat Monitor — Full Project Plan

Status: **Build complete.** All 11 phases (§10) delivered: `feed_monitor` and `feed_simulator` binaries, 94 tests (all clean under `-Wall -Wextra -Wpedantic`, ASan, UBSan, and ThreadSanitizer), `latency_bench`/`throughput_bench`, and a live fault-injection demonstration (`make demo`). See §16 for the closing stress-test/polish pass, §17 for the Phase-12 benchmark/codebase re-audit, and §18 for the Phase-14 functional-correctness validation (the fault-injection tests and live demo that prove the actual detect/degrade/fail/reconnect/recover motive, as distinct from the performance benchmarks).

Target platform: Linux only (epoll/timerfd/eventfd are Linux syscalls — confirmed available: kernel 7.0.0-27-generic, g++ 15.2.0, GNU Make 4.4.1).
Language/standard: C++20, no external dependencies (no package manager, no CMake — Makefile only, as specified).

---

## 1. Audit of the Original Spec

The original description is a good product brief but is missing the decisions a real implementation needs. Each gap below was found during audit and resolved; the resolution is what the rest of this document builds on.

| # | Gap found in original spec | Resolution |
|---|---|---|
| 1 | `Connection` is described as connecting to "market data feeds," but nothing in the file list produces a listener for it to connect *to*. Without a counterpart process, nothing can be run, tested, or demoed end-to-end. | Add a first-class **`FeedSimulator`** component (mock exchange server) with its own binary. This is the single biggest addition to the original file list. |
| 2 | "Compact binary heartbeat protocol" has no defined byte layout, field sizes, byte order, or framing strategy. TCP is a byte stream — without a framing rule, message boundaries are undefined. | Fixed-size 28-byte packed struct (§3). Fixed size ⇒ trivial framing (read until 28 bytes accumulated, no length-prefix needed). |
| 3 | "Includes fields for sequence number, timestamp, and feed identifier" implies a *one-way* heartbeat. One-way timestamps can only measure latency if both processes' clocks are synchronized (NTP-level precision), which is out of scope and unreliable to demo. | Make the protocol **bidirectional ping/pong**: the monitor sends `PING` stamped with its own `CLOCK_MONOTONIC` time; the simulator echoes that exact timestamp back in `PONG`. RTT = `now - echoed_ts`, computed entirely on one clock. No clock-sync dependency. |
| 4 | No connection/feed state machine, despite "healthy," "failure," "half-open," and "reconnect" all being mentioned as behaviors. | Explicit `ConnectionState` enum with a defined transition table (§4). |
| 5 | No reconnect policy (interval, backoff, jitter, max attempts). Naive immediate-retry reconnect loops are a classic way to DoS your own dependency. | Exponential backoff with jitter and configurable caps (§4, §6). |
| 6 | Non-blocking epoll I/O is mentioned, but nothing addresses partial reads/writes — which non-blocking sockets under `epoll` *will* produce regularly (short writes, message split across reads). | Per-connection read/write accumulation buffers, explicitly designed in §4. |
| 7 | Edge-triggered vs. level-triggered `epoll` isn't chosen. This isn't cosmetic: edge-triggered (`EPOLLET`) requires draining each fd to `EAGAIN` on every wakeup or you silently stall. | Edge-triggered chosen explicitly (most idiomatic for a "low-latency" showcase); drain-loop requirement documented at the call site in `Connection::on_readable`. |
| 8 | "Detect half-open connections" is stated as a goal with no mechanism. TCP keepalive is too slow (minutes, OS-dependent) to be the primary signal. | Heartbeat-timeout-driven detection via `timerfd` is the primary mechanism; `EPOLLRDHUP`/`EPOLLHUP`/socket errors are treated as an immediate fast-path failure, not the only path. |
| 9 | Config file format is unspecified. | Minimal hand-rolled `key=value` + repeated `[feed]` section format (§6) — no parser dependency. |
| 10 | Test framework is unspecified. Pulling in GoogleTest/Catch2 needs a package manager or vendored source, which conflicts with the "Makefile only" build implied by the rest of the project. | A ~150-line **header-only mini test harness** (`test/mini_test.h`) written as part of this project — zero external dependency, consistent with a from-scratch systems project. |
| 11 | Benchmark methodology is unspecified (no warmup, no percentile method, no CPU-pinning guidance) — "measure latency" alone isn't reproducible. | `bench/bench_common.h` defines the timer, warmup protocol, and percentile calculation used by both benchmarks (§8). |
| 12 | No signal handling / graceful shutdown design, despite this being a long-running monitoring daemon. | `SIGINT`/`SIGTERM` → self-pipe/`eventfd` wakeup integrated into the epoll loop (§4). |
| 13 | Spec claims monitoring "1000s of connections" but never addresses the default `ulimit -n` (often 1024), which would silently cap that claim. | Startup path attempts to raise `RLIMIT_NOFILE` and logs the effective limit (§4, `main.cpp`). |
| 14 | CLI section implies a rich interactive UI ("one-touch adjustment," manual disconnect commands) without specifying input handling. A full curses TUI is scope creep with its own dependency and complexity. | Lightweight **non-blocking stdin command reader**, registered as just another fd in the *same* epoll set — plain-text commands (`disconnect <id>`, `reconnect <id>`, `set-interval <ms>`, `quit`). Keeps the whole program single-threaded. |
| 15 | `config.cpp` is listed but no `config_test.cpp` is in the test list, despite config parsing being exactly the kind of error-prone code that needs unit tests. | Added `test/config_test.cpp`. |
| 16 | "Display real-time stats" and "analyze latency" are called out as deliverables, but no stats data structure exists anywhere in the file list. | Added `src/stats.h`/`.cpp`: per-connection stats (RTT min/max/EWMA, missed count, reconnects) + a fixed-size ring buffer for percentile estimation, plus an aggregate snapshot type for the CLI. |
| 17 | Concurrency model is unstated — is the display on a separate thread from the event loop? Mixing threads around connection state invites locking on the hot path, which directly fights the "low-latency" goal. | **Fully single-threaded.** One `epoll` reactor thread owns everything: sockets, timers, stdin, and stats. No locks, no cross-thread hot path. This is also the idiomatic choice for a low-latency systems showcase. |
| 18 | No decision on which process initiates heartbeats, which matters once you fix gap #3. | The monitor (client) is the sole heartbeat initiator (`PING`); the simulator (server) only replies (`PONG`) or, optionally, injects faults for testing. Mirrors how a FIX/market-data heartbeat exchange actually behaves. |

Everything below reflects these resolved decisions, not the ambiguous version in the original brief.

---

## 2. Final Architecture

Two binaries share the `src/` library code:

- **`feed_monitor`** — the actual deliverable: connects out to N feed endpoints, sends heartbeats, detects failures, shows live stats, accepts CLI commands.
- **`feed_simulator`** — a mock exchange: listens on one or more ports, accepts connections, replies to `PING` with `PONG`, and can inject faults (drop packets, add latency, kill connections) so failure-detection and reconnect logic are actually exercisable and testable. Without this, `feed_monitor` has nothing to talk to.

Both are single-threaded `epoll` reactors built from the same `Connection`/`Heartbeat` primitives, which is also why `Connection` is designed to be role-agnostic (it doesn't know if it's the monitor side or the simulator side).

```
                    ┌─────────────────────┐
   feed_monitor     │   FeedMonitor        │        feed_simulator
   (this project's  │   (epoll reactor)    │        (test double / demo peer)
    main deliverable)│                     │
                     │  Connection[fd=5] ──┼── TCP ──► FeedSimulator listener
                     │  Connection[fd=6] ──┼── TCP ──► (per-client Connection)
                     │  Connection[fd=7] ──┼── TCP ──► ...
                     │  timerfd (tick)      │
                     │  stdin (CLI cmds)    │
                     │  wake/signal fd       │
                     └─────────────────────┘
```

---

## 3. Wire Protocol (`src/heartbeat.h` / `.cpp`)

Fixed-size, packed, 28 bytes, chosen so a TCP stream can always be parsed as "accumulate until 28 bytes, then decode one message" — no length-prefix or delimiter logic needed.

```cpp
#pragma pack(push, 1)
struct HeartbeatMessage {
    uint16_t magic;         // kProtocolMagic = 0xFEED, guards against stream desync/garbage
    uint8_t  version;       // kProtocolVersion = 1
    uint8_t  type;          // MessageType
    uint32_t feed_id;       // network byte order
    uint64_t sequence;      // monotonically increasing, set by the PING sender
    int64_t  timestamp_ns;  // CLOCK_MONOTONIC at PING send; echoed unchanged in PONG
    uint32_t crc32;         // CRC32 (IEEE 802.3) over all preceding bytes
};
#pragma pack(pop)
static_assert(sizeof(HeartbeatMessage) == 28);

enum class MessageType : uint8_t {
    kConnectHello = 1,  // monitor -> simulator, first message after connect()
    kConnectAck   = 2,  // simulator -> monitor, handshake complete
    kPing         = 3,  // monitor -> simulator, periodic heartbeat
    kPong         = 4,  // simulator -> monitor, echoes ping's sequence+timestamp
    kDisconnect   = 5,  // either side, graceful close notice
};

constexpr uint16_t kProtocolMagic = 0xFEED;
constexpr uint8_t  kProtocolVersion = 1;

// Encodes msg into out (exactly sizeof(HeartbeatMessage) bytes), converting
// multi-byte fields to network byte order and computing crc32 over the rest.
void encode_heartbeat(const HeartbeatMessage& msg, uint8_t out[sizeof(HeartbeatMessage)]);

// Decodes sizeof(HeartbeatMessage) bytes from in. Returns false (and leaves *out
// unspecified) if magic, version, or crc32 don't validate -- caller must treat
// this as a protocol error, not just drop silently, since it likely means
// stream desync and the connection should be reset.
bool decode_heartbeat(const uint8_t in[sizeof(HeartbeatMessage)], HeartbeatMessage* out);

uint32_t crc32_ieee(const uint8_t* data, size_t len);
```

Notes:
- `magic` + `crc32` together catch both "wrong protocol/garbage peer" and "stream desync from a previous parsing bug" — both are real failure modes when hand-rolling a binary protocol, and both should be loud (log + reset connection), not silently ignored.
- Endianness: encode always writes network byte order (`htons`/`htonl`/manual 64-bit swap since there's no standard `htonll`); decode always converts back. This makes the protocol portable even though the project only targets Linux/x86 today.

---

## 4. `Connection` (`src/connection.h` / `.cpp`)

Represents one TCP peer — used identically by both `feed_monitor` (as a client connection to an exchange) and `feed_simulator` (as an accepted client). Role is controlled by a constructor flag (`Role::kInitiator` sends `PING`s and expects `PONG`s; `Role::kResponder` expects `PING`s and replies `PONG`).

### State machine

```
DISCONNECTED → CONNECTING → HANDSHAKING → HEALTHY ⇄ DEGRADED → FAILED → RECONNECTING → CONNECTING ...
```

| State | Meaning | Exit condition |
|---|---|---|
| `DISCONNECTED` | Initial / after give-up | `connect()` issued → `CONNECTING` |
| `CONNECTING` | non-blocking `connect()` in flight | `EPOLLOUT` + `SO_ERROR==0` → `HANDSHAKING`; error → `FAILED` |
| `HANDSHAKING` | `kConnectHello`/`kConnectAck` exchanged | ack received → `HEALTHY`; timeout → `FAILED` |
| `HEALTHY` | 0 consecutive missed heartbeats | 1..N-1 missed → `DEGRADED`; N missed or socket error → `FAILED` |
| `DEGRADED` | 1..N-1 consecutive missed heartbeats | `PONG` received → `HEALTHY`; N-th miss → `FAILED` |
| `FAILED` | dead, alert already emitted | monitor schedules reconnect → `RECONNECTING` |
| `RECONNECTING` | backoff timer running | timer fires → `CONNECTING` |

### Class shape

```cpp
enum class Role : uint8_t { kInitiator, kResponder };

class Connection {
public:
    Connection(int fd, Role role, uint32_t feed_id, std::string host, uint16_t port,
               const Config& config, Stats* stats_sink);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    int fd() const noexcept;
    ConnectionState state() const noexcept;
    uint32_t feed_id() const noexcept;

    // epoll event handlers -- called by FeedMonitor/FeedSimulator's dispatch loop
    void on_readable();               // drains socket to EAGAIN (required: EPOLLET), reassembles messages
    void on_writable();               // completes in-flight connect() or flushes queued writes
    void on_hangup_or_error(int err); // EPOLLHUP/EPOLLRDHUP/EPOLLERR -> immediate FAILED

    void send_ping(int64_t now_ns);         // Role::kInitiator only; queues a PING, advances sequence
    bool check_timeout(int64_t now_ns);     // called every tick; returns true if state changed to FAILED
    void reset_for_reconnect();             // clears buffers/sequence, preserves cumulative stats, ++reconnects

    const ConnectionStats& stats() const;

private:
    void handle_message(const HeartbeatMessage& msg, int64_t recv_time_ns);
    void queue_write(const uint8_t* data, size_t len);  // appends to write_buffer_, tries immediate send
    void flush_write_buffer();                          // called from on_writable and after queue_write

    int fd_;
    Role role_;
    uint32_t feed_id_;
    std::string host_;
    uint16_t port_;
    ConnectionState state_ = ConnectionState::DISCONNECTED;

    uint64_t next_sequence_ = 0;
    uint64_t last_seen_sequence_ = 0;
    int consecutive_missed_ = 0;
    int64_t last_pong_time_ns_ = 0;

    std::vector<uint8_t> read_accum_;    // partial-message accumulator; consumed in 28-byte chunks
    std::deque<uint8_t> write_pending_;  // bytes not yet accepted by send() (EAGAIN / short write)

    const Config& config_;
    ConnectionStats* stats_;
};
```

### Non-blocking I/O correctness (the part the original spec skipped)

- **Reads**: `on_readable` loops `recv()` into a stack buffer until `EAGAIN`/`EWOULDBLOCK` (mandatory under edge-triggered `epoll`, or a hung fd is missed forever), appending everything to `read_accum_`. After each `recv()`, drain `read_accum_` in 28-byte chunks via `decode_heartbeat`; leftover partial bytes (0–27) stay buffered for the next call.
- **Writes**: `queue_write` first attempts an immediate `send()`; whatever isn't accepted goes into `write_pending_` and `EPOLLOUT` is (re-)armed on the fd. `on_writable` retries `send()` on `write_pending_` until empty or `EAGAIN`, then can drop `EPOLLOUT` interest again (avoids busy-looping on a writable-but-idle fd).
- **Half-open detection**: primary signal is `check_timeout` — no `PONG` within `heartbeat_timeout` for `max_missed_heartbeats` consecutive intervals ⇒ `FAILED`, independent of whatever the socket layer reports (a half-open peer often never generates a socket-level event at all). `EPOLLRDHUP`/`EPOLLHUP`/`ECONNRESET` are a *faster* path to the same `FAILED` state when the OS does notice.

---

## 5. `FeedMonitor` (`src/feed_monitor.h` / `.cpp`)

Single-threaded `epoll` reactor owning all connections, one `timerfd` (drives both heartbeat sending and timeout checks off one periodic tick — avoids relying on `epoll_wait`'s timeout argument, which is unreliable as a periodic-work source once real I/O events start dominating), a stdin fd (CLI commands), and a self-pipe/`eventfd` used only to unblock `epoll_wait` from a signal handler for clean shutdown.

```cpp
class FeedMonitor {
public:
    explicit FeedMonitor(Config config);
    ~FeedMonitor();

    void add_feed(const FeedEndpoint& endpoint);   // starts async connect immediately
    void remove_feed(uint32_t feed_id);
    void run();     // blocks; the reactor loop
    void stop();    // signal-safe; writes to wake_fd_

    AggregateStats snapshot_stats() const;
    void set_heartbeat_interval(std::chrono::milliseconds interval);
    void set_heartbeat_timeout(std::chrono::milliseconds timeout);

private:
    void event_loop_iteration();
    void handle_epoll_event(const epoll_event& ev);
    void on_timer_tick();              // fires from timerfd: send due pings + check_timeout() on all
    void on_stdin_command();           // parses one line, dispatches to handlers below
    void initiate_connect(uint32_t feed_id);
    void schedule_reconnect(uint32_t feed_id);  // exponential backoff + jitter
    void emit_failure_alert(uint32_t feed_id, std::string_view reason);

    int epoll_fd_;
    int timer_fd_;
    int wake_fd_;      // eventfd; signal handler + stop() both write to it
    int stdin_fd_ = STDIN_FILENO;

    // Connections live in one contiguous vector, not unordered_map<int, unique_ptr<Connection>>
    // -- see the "node containers" note in §15. Two small index maps handle the cold-path lookups
    // (by fd on epoll events, by feed_id on CLI commands); the hot per-tick loop (send_ping +
    // check_timeout over every connection) walks connections_ directly for cache locality.
    std::vector<Connection> connections_;
    std::unordered_map<int, size_t> index_of_fd_;
    std::unordered_map<uint32_t, size_t> index_of_feed_id_;
    std::unordered_map<uint32_t, FeedEndpoint> feed_config_;
    std::unordered_map<uint32_t, int> reconnect_attempts_;

    Config config_;
    Stats stats_;
    bool running_ = false;   // mutated only on the reactor thread; shutdown wakeup goes through wake_fd_, not this flag
    std::vector<epoll_event> event_batch_;  // sized by config_.epoll_max_events
};
```

Note: `connections_` is a `std::vector`, so inserting/erasing a connection can move other elements and invalidate indices held elsewhere (unlike `std::map`, whose iterators are stable). `index_of_fd_`/`index_of_feed_id_` must be rebuilt (or the moved slot's entries patched) on every removal — see §15 for why this trade is still worth making here, and §11 for the test that pins the behavior (`remove_feed` mid-vector, verified against a swap-and-pop implementation, must not corrupt lookups for the connection that got moved into the erased slot).

Reconnect scheduling: `delay = min(base * 2^attempt, max_delay) * (1 ± jitter)`, timed via the same `timerfd` tick rather than a fresh timer per connection (keeps the design to one timer fd regardless of connection count — important for the "1000s of connections" scalability claim).

Startup responsibility (in `main.cpp`, not here): attempt `setrlimit(RLIMIT_NOFILE, ...)` to raise the fd limit and log the effective value before `FeedMonitor` starts adding feeds.

---

## 6. `FeedSimulator` (new: `src/feed_simulator.h` / `.cpp`, `src/main_simulator.cpp`)

Not in the original file list — added per audit finding #1. Structurally the mirror image of `FeedMonitor`: an `epoll` reactor around a listening socket, using `Connection` in `Role::kResponder` mode for each accepted client.

```cpp
struct FaultConfig {
    double drop_probability = 0.0;              // probability a PONG is silently not sent
    std::chrono::milliseconds extra_latency{0};  // artificial delay before replying
    bool jitter = false;                         // randomize extra_latency ±50%
};

class FeedSimulator {
public:
    FeedSimulator(uint16_t port, uint32_t feed_id, FaultConfig faults = {});
    void run();
    void stop();

    void set_fault_config(FaultConfig faults);   // runtime-adjustable, for scripted failure tests
    void kill_random_connection();               // simulate abrupt exchange-side drop (RST)

private:
    int listen_fd_;
    int epoll_fd_;
    int wake_fd_;
    void accept_new_connections();
    void handle_epoll_event(const epoll_event& ev);

    std::unordered_map<int, std::unique_ptr<Connection>> clients_;
    uint32_t feed_id_;
    FaultConfig faults_;
    std::atomic<bool> running_{false};
};
```

`main_simulator.cpp` CLI: `--port`, `--feed-id`, `--drop-rate`, `--extra-latency-ms`, `--jitter`. This is what makes `Connection`/`FeedMonitor` testable and demoable at all — deliberately built early (see phase plan, §10) rather than last.

---

## 7. Stats (new: `src/stats.h` / `.cpp`)

Referenced by the original spec ("display real-time stats," "analyze latency") but never given a data structure. Design keeps all mutation on the single reactor thread — the CLI reads a copied snapshot, so no locking is needed on the hot path at all.

```cpp
template <typename T, size_t N>
class RingBuffer {   // fixed-size, overwrites oldest; used for percentile estimation
public:
    void push(T v);
    std::vector<T> sorted_copy() const;   // for percentile calc; O(N log N), called only by CLI refresh
private:
    std::array<T, N> data_{};
    size_t count_ = 0, next_ = 0;
};

struct ConnectionStats {
    uint64_t heartbeats_sent = 0;
    uint64_t heartbeats_acked = 0;
    uint64_t missed = 0;
    uint64_t reconnects = 0;
    int64_t  last_rtt_ns = 0;
    int64_t  min_rtt_ns = INT64_MAX;
    int64_t  max_rtt_ns = 0;
    double   ewma_rtt_ns = 0.0;              // alpha configurable, default 0.2
    RingBuffer<int64_t, 256> rtt_samples_ns; // basis for p50/p95/p99 in the CLI
    std::chrono::steady_clock::time_point connected_since;
};

double percentile(const std::vector<int64_t>& sorted_samples, double p);  // p in [0,100]
```

`FeedSnapshot`/`AggregateStats` (feed_id + name + `ConnectionState` + `ConnectionStats` combined) turned out to belong in `feed_monitor.h`, not here: `ConnectionState` is declared in `connection.h`, and having `stats.h` depend on it (or vice versa) creates a needless coupling between two otherwise-independent leaf modules. Combining "which state is this feed in" with "what are its numbers" is inherently a `FeedMonitor`-level concern, so that's where the combining type lives — see §5 amendments and the build-order fix in §10/§15.

---

## 8. Configuration (`src/config.h` / `.cpp`)

```cpp
struct FeedEndpoint { uint32_t feed_id; std::string name; std::string host; uint16_t port; };

struct Config {
    std::chrono::milliseconds heartbeat_interval{1000};
    std::chrono::milliseconds heartbeat_timeout{3000};
    int max_missed_heartbeats = 3;

    std::chrono::milliseconds reconnect_base_delay{500};
    std::chrono::milliseconds reconnect_max_delay{30000};
    int max_reconnect_attempts = -1;     // -1 = unlimited

    int epoll_max_events = 256;
    std::chrono::milliseconds tick_interval{50};
    bool tcp_nodelay = true;             // exposed so latency_bench can A/B it (original spec explicitly asks for this comparison)
    bool edge_triggered = true;

    std::vector<FeedEndpoint> feeds;
    std::string log_level = "info";      // trace|debug|info|warn|error
};

Config parse_config_file(const std::string& path);           // key=value + repeated [feed] sections
Config apply_cli_overrides(Config base, int argc, char** argv); // getopt_long; CLI wins over file
void print_usage(const char* prog_name);
```

Config file format (hand-rolled parser, no dependency):

```ini
heartbeat_interval_ms=1000
heartbeat_timeout_ms=3000
max_missed_heartbeats=3
reconnect_base_delay_ms=500
reconnect_max_delay_ms=30000
tcp_nodelay=true

[feed]
id=1
name=NASDAQ
host=127.0.0.1
port=9001

[feed]
id=2
name=NYSE
host=127.0.0.1
port=9002
```

Malformed lines: warn + skip rather than abort the whole file (one bad feed entry shouldn't take down monitoring of the rest) — verified by `config_test.cpp`.

---

## 9. CLI (`src/main.cpp`, `src/main_simulator.cpp`, `src/cli_display.h/.cpp`)

- `main.cpp`: raise `RLIMIT_NOFILE`, load config (`parse_config_file` + `apply_cli_overrides`), install `SIGINT`/`SIGTERM` handlers that write to `FeedMonitor`'s wake fd, construct `FeedMonitor`, `add_feed` for each configured endpoint, call `run()`.
- `cli_display.h/.cpp`: renders `AggregateStats` as an in-place-refreshing table (ANSI cursor-home + overwrite, like `top`; no curses dependency) — feed name, state, last/EWMA/p99 RTT, missed count, reconnects, uptime. Refreshed on the same `timerfd` tick as heartbeat checks (no separate thread/timer).
- Stdin commands (read non-blocking, registered in the same `epoll` set — satisfies "manual disconnect/reconnect for testing" and "one-touch adjustment" without a TUI dependency, per audit finding #14):

| Command | Effect |
|---|---|
| `disconnect <feed_id>` | force-closes that connection, transitions to `FAILED` |
| `reconnect <feed_id>` | cancels backoff wait, retries immediately |
| `set-interval <ms>` | `FeedMonitor::set_heartbeat_interval` |
| `set-timeout <ms>` | `FeedMonitor::set_heartbeat_timeout` |
| `quit` | graceful shutdown |

---

## 10. File Manifest & Build Order

Final tree (additions over the original spec marked **NEW**):

```
TCP/
├── Makefile
├── config/
│   └── default.conf
├── src/
│   ├── heartbeat.h / heartbeat.cpp
│   ├── config.h / config.cpp
│   ├── stats.h / stats.cpp                 (NEW)
│   ├── connection.h / connection.cpp
│   ├── feed_monitor.h / feed_monitor.cpp
│   ├── feed_simulator.h / feed_simulator.cpp   (NEW)
│   ├── cli_display.h / cli_display.cpp     (NEW)
│   ├── log.h                               (NEW, header-only)
│   ├── main.cpp                            (feed_monitor entry point)
│   └── main_simulator.cpp                  (NEW, feed_simulator entry point)
├── test/
│   ├── mini_test.h                         (NEW, header-only harness)
│   ├── test_main.cpp                       (NEW, sole TU defining MINI_TEST_MAIN)
│   ├── heartbeat_test.cpp
│   ├── connection_test.cpp
│   ├── config_test.cpp                     (NEW)
│   └── feed_monitor_test.cpp
└── bench/
    ├── bench_common.h                      (NEW)
    ├── latency_bench.cpp
    └── throughput_bench.cpp
```

Build order, corrected twice during implementation for real circular-dependency problems in the original draft (both corrections are logged in §15's running build log):

1. Repo scaffold + `Makefile` skeleton + `test/mini_test.h`
2. `heartbeat.{h,cpp}` + `heartbeat_test.cpp`
3. `config.{h,cpp}` + `config_test.cpp`
4. `stats.{h,cpp}` + `stats_test.cpp` -- moved ahead of `Connection` (was step 6): it's a dependency-free leaf module (`RingBuffer`, `ConnectionStats`, `percentile()`, no project-internal includes), and `Connection` needs `ConnectionStats` to exist first.
5. `connection.{h,cpp}` + `connection_test.cpp` -- tested against a **minimal raw-socket TCP peer written directly in the test file**, not against `FeedSimulator` (the original draft had `FeedSimulator` built before `Connection` while also being built *from* `Connection::kResponder`, and had `connection_test.cpp` testing against a `FeedSimulator` instance that didn't exist yet -- both circular; see §15).
6. `feed_simulator.{h,cpp}` + `main_simulator.cpp` (minimal ping/pong echo, no faults yet) -- now straightforward, built on the already-existing `Connection`.
7. `feed_monitor.{h,cpp}` + `feed_monitor_test.cpp` (also where `FeedSnapshot`/`AggregateStats` now live, see §7's amendment)
8. `cli_display.{h,cpp}` + `main.cpp` (full CLI, stdin commands, live table)
9. Fault injection in `FeedSimulator` (drop/latency/jitter/kill) — unblocks realistic failure-detection demos
10. `bench/bench_common.h`, `latency_bench.cpp`, `throughput_bench.cpp`
11. Stress pass: 1000+ simulated connections, `-fsanitize=address,undefined` debug run, polish logging

---

## 11. Testing Plan

**`heartbeat_test.cpp`**: encode/decode round-trip preserves all fields · decode rejects short buffer · rejects wrong magic · rejects wrong version · rejects corrupted CRC (single bit flip) · `static_assert`-level struct size sanity is compiled, not just tested at runtime.

**`config_test.cpp`**: CLI overrides win over file values · multiple `[feed]` sections parsed correctly · malformed line is skipped with a warning, not a hard failure · missing optional keys fall back to defaults · invalid port/host rejected with a clear error.

**`connection_test.cpp`** (against an in-test loopback `FeedSimulator`): starts `CONNECTING` → reaches `HEALTHY` after handshake · `send_ping` increments sequence and produces correct on-wire bytes · valid `PONG` updates RTT stats and clears missed-count · N consecutive missed heartbeats → `FAILED` (driven via injected `now_ns`, not real sleeps) · message split across two `on_readable` calls reassembles correctly · partial/short `send()` is completed via `write_pending_` on the next writable event · `EPOLLHUP`/`ECONNRESET` → immediate `FAILED` · `reset_for_reconnect` clears per-attempt state but keeps cumulative stats and increments `reconnects`.

**`feed_monitor_test.cpp`** (integration, real loopback sockets, no mocking of the kernel): `add_feed` reaches `HEALTHY` within `heartbeat_timeout` · `remove_feed` cleans up the fd from `epoll` and both maps · killing the simulator-side connection drives the monitor to `FAILED` after the configured miss threshold, then into `RECONNECTING` · backoff timing roughly matches `base`/multiplier within tolerance · `snapshot_stats()` counts match reality with a mix of healthy/failed feeds · `stop()` from another thread causes `run()` to return promptly · 50+ concurrent feeds all reach `HEALTHY`, exercising the `epoll` event-batch path.

`mini_test.h` design: `TEST_CASE("name") { ... }` macro registers a lambda into a static vector at static-init time; `REQUIRE(x)`/`CHECK(x)` macros; a generated `main()` runs the registry and reports pass/fail counts with non-zero exit on failure — enough for this project's needs without adopting Catch2/GoogleTest.

---

## 12. Benchmarking Plan

**`bench_common.h`**: `Timer` wrapping `clock_gettime(CLOCK_MONOTONIC)`; explicit warmup-then-measure protocol (discard first N iterations); `percentile()` helper shared with `stats.h`; notes on `sched_setaffinity`/`SCHED_FIFO` for reducing scheduling noise (documented as optional, since it needs elevated privileges — benchmarks must still run meaningfully without it).

**`latency_bench.cpp`**: single connection, loopback `FeedMonitor` ↔ `FeedSimulator`, ~100k `PING`/`PONG` round trips after warmup, reports p50/p90/p99/p99.9/max RTT. Parameterized runs: `tcp_nodelay` on vs. off (the explicit comparison the original spec asks for), varying `heartbeat_interval`.

**`throughput_bench.cpp`**: ramps monitored connection count (10 / 100 / 500 / 1000 / 5000, bounded by `RLIMIT_NOFILE`), measures `epoll_wait` calls/sec, events processed/sec, CPU (`getrusage`) and RSS (`/proc/self/status`) under load; varies `epoll_max_events` batch size (16/64/256/1024) to show its effect on syscall overhead. A naive blocking-per-connection-thread baseline is included as an optional comparison mode, guarded by a CLI flag, to produce the `epoll` vs. blocking-I/O comparison the original spec calls for.

---

## 13. Makefile Plan

Targets: `all` (builds both `bin/feed_monitor` and `bin/feed_simulator`), `test` (builds and runs `bin/run_tests` from `test/*.cpp` + library objects), `bench-latency`, `bench-throughput`, `debug` (`-g -O0 -fsanitize=address,undefined -DDEBUG`), `clean`. `CXXFLAGS = -std=c++20 -O2 -Wall -Wextra -Wpedantic -pthread`, dependency files via `-MMD -MP`. No external libraries linked — `-pthread` is present only because `<atomic>`/eventfd-adjacent glibc bits expect it, not because the design spawns worker threads.

---

## 14. Future Enhancements (carried over, unchanged from original)

Integration with real market data feed APIs · distributed deployment across nodes · adaptive heartbeat intervals · ML-based failure prediction · visualization dashboard · alerting/incident-management integration.

- **Round-robin multi-reactor scaling**, if this ever needs to scale past what one core's single-threaded epoll loop can drive. Robert Leahy's "The Networking TS in Practice" talk (MayStreet Inc. — a real low-latency market-data infrastructure company; found merged into `Downloads/Deciphering_Cpp_Coroutines-merged.pdf` during the §17 audit pass) describes exactly this shape: run *N* independent single-threaded reactors, one per core, and distribute accepted connections round-robin across them — instead of adding locks to one shared reactor. This project's current single-threaded-reactor-per-process design (§2) is precisely the natural unit that pattern scales by replicating, with zero changes to `Connection`/`FeedMonitor`/`FeedSimulator` themselves; only a new top-level dispatcher would be needed. Not implemented — this project is intentionally single-instance — but recorded here as the validated next step if that ever changes, rather than reaching for a shared-reactor-plus-locks design instead.

---

## 14.5. FeedMonitor Refinements Made While Implementing (Phase 7)

Several concrete decisions were made beyond §5's sketch, once `Connection`, `Stats`, and `FeedSimulator` already existed and FeedMonitor had to actually fit together with them:

- **`Slot` replaces the plan's separate `connections_`/`feed_config_`/`reconnect_attempts_`.** Each feed's `Connection`, display name, and reconnect bookkeeping (`reconnect_attempts`, `reconnect_ready_at_ns`, `give_up`, `next_ping_due_ns`) are bundled into one `Slot` struct, stored in one `std::vector<Slot>`. This is the same "Principle #1" reasoning from §15 taken one step further: the plan's version still had the hot per-tick loop bouncing between `connections_` and separate hash maps for reconnect state; `Slot` keeps everything about one feed in one cache-friendly place.
- **Command syntax and stats rendering are pulled out of `FeedMonitor` via callbacks**, rather than `FeedMonitor` owning stdin parsing and knowing about `cli_display` directly (as §5/§9's original sketch implied). `set_tick_callback(TickCallback)` hands a fresh `AggregateStats` to whoever's listening once per tick; `set_command_input(fd, CommandLineCallback)` registers an arbitrary fd (stdin in production, a pipe in tests) into the same epoll set and hands complete lines to a callback. `FeedMonitor` itself never parses "disconnect 3" or renders a table — that's Phase 8's job (`main.cpp`), keeping the reactor's own dependencies to just `config.h`/`connection.h`/`stats.h`.
- **A synchronous `connect()` failure (e.g. `ECONNREFUSED` on loopback, which happens routinely, not just under packet loss) needs no special-casing.** A non-blocking `connect()` that fails immediately still leaves the fd open with `SO_ERROR` set; `Connection::on_writable`'s existing `getsockopt(SO_ERROR)` check (written for the *async* failure case) handles it identically. `create_and_connect_socket()` therefore only returns `std::nullopt` for failures that truly have no fd to hand off (`socket()` itself failing, or an unparseable IPv4 address) — realized partway through implementation, not planned upfront.
- **A failed connection's fd is removed from epoll immediately** (`on_connection_failed`), not left registered until the eventual reconnect's `rebind()` closes it. Under level-triggered epoll (`edge_triggered=false`), a HUP/ERR fd left registered would keep reporting ready on every `epoll_wait` call for the entire backoff delay — a live busy-loop risk the original plan didn't call out.
- **`epoll_utils.h`** (`epoll_interest_flags()`, `throw_errno()`) was extracted once `feed_monitor.cpp` needed the exact same two helpers `feed_simulator.cpp` already had — a second real call site, not a premature abstraction.
- **`FeedMonitor` is neither copyable nor movable**, stricter than the plan implied. Every `Connection` it owns stores a raw `const Config*` back into `FeedMonitor::config_` (see §15's `Connection` amendment) so that `set_heartbeat_interval()`/`set_heartbeat_timeout()` take effect for every connection instantly with no propagation step — which only stays sound if `config_`'s address never moves.

`add_feed()` also returns `bool` (false if `socket()`/`inet_pton` fail synchronously) rather than the plan's `void`, so callers can distinguish "this feed_id will never connect, fix the config" from the normal async-connect path.

---

## 14.6. Fault Injection Design (Phase 9)

`FeedSimulator`'s fault injection ended up needing one small, deliberately minimal extension to `Connection` rather than living entirely inside `FeedSimulator`:

- **`Connection::set_ping_interceptor(PingInterceptor)`** -- an optional `std::function<bool(uint64_t sequence, int64_t timestamp_ns)>` consulted right before the automatic PONG reply to a PING. Returning `false` suppresses that automatic reply; unset (the default), behavior is unchanged from before Phase 9. This is the *only* fault-aware code in `Connection` -- it has no notion of drop rates or latency itself, keeping it exactly as reusable by `FeedMonitor` as before. `Connection::send_pong()` was made public alongside it, since deferred replies (for `extra_latency`) need to be sent later than the triggering PING, from `FeedSimulator`, not synchronously from inside `handle_ping`.
- **Both fault types route through the one interceptor**, not two separate mechanisms: `drop_probability` returns `false` and does nothing further (permanently dropped); `extra_latency` also returns `false` but schedules a `PendingReply{fire_at_ns, fd, sequence, timestamp_ns}`, drained by a periodic timer (`FeedSimulator` gained a `timerfd_create`-based tick for this, reusing the new shared `arm_periodic_timer()` helper in `epoll_utils.h`).
- **A real fd-reuse bug was caught during design, not left for a flaky test to find**: `PendingReply` is keyed by `fd`, and fd numbers get reused by the kernel once closed. `remove_client()` now purges any pending replies for a fd *before* erasing the `Connection` (and thus closing the real fd), so a stale deferred reply can never misfire at an unrelated later client that happens to get the same fd number.
- **`kill_random_connection()` sets `SO_LINGER{on=1, linger=0}`** before closing, so the close sends a raw RST instead of a graceful FIN -- this specifically exercises the monitor's `ECONNRESET` path (`Connection::on_hangup_or_error`), which a normal disconnect never reaches (that path only sees clean EOF).
- **`FeedSimulator` gained `set_command_input()`, mirroring `FeedMonitor`'s** (`kill`, `set-drop-rate <p>`, `quit`), for the same reason: `kill_random_connection()`/`set_fault_config()` mutate reactor state and must run on the reactor thread, not be called directly from another thread the way `stop()` can.
- **A real bug surfaced while writing the tests for this, not in production code**: a test registered a *blocking* pipe fd with `set_command_input`, which froze the entire reactor thread (all connections, not just that fd) inside the second `read()` call once the pipe's initially-available bytes were drained. `set_command_input`'s non-blocking-fd precondition was already implicitly satisfied by `main.cpp`/`main_simulator.cpp` (both `fcntl()` stdin before registering it) but was never stated as a precondition anywhere -- now documented explicitly on both `FeedMonitor` and `FeedSimulator`'s declarations.
- **A live demo run against the real binaries** (90% drop rate via `--drop-rate 0.9`) surfaced a genuine display bug from Phase 8: a 14-character feed name exactly filled its `setw(14)` column, consuming all padding and running directly into the next column (`"LOSSY_EXCHANGEFAILED"`, no space). `std::setw` provides zero separation once content reaches its target width; fixed with a `pad_field()` helper that always appends at least one space regardless of content length, plus a regression test pinning the exact failure case.

---

## 14.7. Benchmarking Design (Phase 10)

- **`latency_bench.cpp` deliberately bypasses `FeedMonitor::run()`** and drives a `Connection` directly through a minimal, benchmark-local epoll loop instead. `FeedMonitor` paces pings off `config.heartbeat_interval` via its periodic tick, which is correct behavior for a monitoring daemon but would conflate the tick's granularity with the protocol's actual round-trip cost, and makes "send the next ping the instant the previous pong arrives" awkward. The benchmark still uses a real `FeedSimulator` on the other end (not a stub), so what's measured is genuine `Connection` encode/decode/socket-syscall cost, back-to-back. RTT samples are read directly from `Connection::stats()` after each pong rather than drained from the live-display ring buffer, since that buffer's 256-sample cap exists for bounded memory in a long-running process, not for benchmark data collection. Result on this machine: p50 ≈ 5.9µs, p99 ≈ 10µs, `TCP_NODELAY` on vs. off shows minimal difference at this message size and pattern (correctly explained by there rarely being other outstanding unacked data for Nagle's algorithm to actually delay in a strict ping-then-wait pattern).
- **`throughput_bench.cpp` needed a peer that can answer many distinct `feed_id`s on one port**, which `FeedSimulator` structurally can't do (one `feed_id` fixed per instance, matching one real exchange). Spinning up thousands of `FeedSimulator` instances to ramp connection count would have measured thread-scheduling overhead from the *benchmark's own scaffolding*, not `FeedMonitor`. `MultiFeedEchoPeer` (defined locally in the bench file, not `src/`) answers using `heartbeat.h`'s encode/decode directly rather than the full `Connection` class, because `Connection`'s `feed_id` must be fixed at construction — before the first byte, which carries the feed_id, has even been read. On this machine: setup time scales from ~20ms (10 connections) to ~105ms (1000), steady-state CPU scales roughly linearly with connection count (expected — each connection independently heartbeats at the same interval), RSS grows from 4MB to 13MB.
- **Scoped out**: the "naive blocking-per-connection-thread baseline" comparison that §12 marked optional. Implementing it properly means writing and maintaining a second, alternate monitor architecture whose only purpose is one comparison data point — real work, but out of proportion to what's left in this build. Noted here rather than silently dropped.
- **Two Makefile bugs caught while wiring up the bench targets**: `EPOLLIN | (cond ? EPOLLET : 0)`-style ternaries hit the same enum/int `-Wextra` warning already fixed once in `feed_simulator.cpp` (Phase 6) — missed here because it's a new file, not a regression; fixed the same way, via `epoll_utils.h`'s `epoll_interest_flags()`. Separately, GNU Make was silently deleting `latency_bench.o`/`throughput_bench.o` after each link (its standard "intermediate file" cleanup for anything reachable only through a chain of pattern rules) — correct output, but it defeated incremental rebuilds for those two files specifically. Fixed with `.SECONDARY: $(BENCH_OBJS)`.

---

## 15. Reference Material Applied

Before/during implementation, the reference material already present on this machine was surveyed for content directly applicable to *this* code (not general C++ knowledge, which is applied from standard practice without needing a citation). Two sources had concrete, load-bearing content; the rest didn't, and that's recorded too so it's clear they were checked, not skipped.

**Applied:**

- **`Preparation/Books/Researh Paper low latency 2.pdf`** — Bilokon & Gunduz, *"C++ design patterns for low-latency applications including high-frequency trading"* (arXiv:2309.04259). Concrete, benchmarked C++ techniques, mapped to files below.
- **`Desktop/CppCon Talks Week 3_compressed.pdf`** — a 1121-page multi-talk compilation. Two talks in it had directly relevant, concrete content:
  - F.G. Pikus, *"C++ Atomics"* (CppCon 2015/2017) — `memory_order`, CAS strong vs. weak, false sharing and cache-line padding (`alignas(std::hardware_destructive_interference_size)`).
  - An order-book/low-latency-data-structures talk — **"Principle #1: Most of the time, you don't want node containers"**: a `std::map`-based order book was replaced with two flat `std::vector`s + `std::lower_bound`, trading O(log N) amortized inserts for cache-friendly contiguous scans, plus `[[likely]]`/`[[unlikely]]`, IIFE-wrapped `__attribute__((noinline, cold))` error paths, and `perf stat -M Frontend_Bound,Backend_Bound,Bad_Speculation,Retiring` as a profiling methodology.

| Technique (source) | Applied to |
|---|---|
| Flat vector over node containers ("Principle #1") | `FeedMonitor::connections_` redesigned from `unordered_map<int, unique_ptr<Connection>>` to a contiguous `std::vector<Connection>` — see the amended class shape in §5. The per-tick scan over up to thousands of connections is the hottest loop in the program; this is exactly the access pattern the talk benchmarks. |
| `[[likely]]`/`[[unlikely]]`, slowpath removal, branch reduction (Pikus talk + Bilokon/Gunduz §3.2) | `Connection::on_readable`/`check_timeout`: the "got a valid PONG, still healthy" path is marked likely and kept branch-flat; CRC failure, protocol-magic mismatch, and timeout handling are pulled into separate `[[unlikely]]`, non-inline functions so they don't bloat the hot path's instruction-cache footprint. |
| False sharing / `alignas(std::hardware_destructive_interference_size)` (Pikus talk) | Explicitly **not needed** in the current design — everything is single-threaded (finding #17), so there's no cross-core cache-line contention to pad against. Documented here so it's a deliberate omission, not an oversight, if the design ever grows a second thread. |
| `constexpr`, inlining, avoiding signed/unsigned mixing, avoiding float/double mixing (Bilokon/Gunduz §3.1/§3.3) | `heartbeat.h`: protocol constants (`kProtocolMagic`, message size) are `constexpr`; sequence numbers and byte counts are consistently `uint64_t`/`size_t`, never mixed with signed loop counters; RTT math stays in `int64_t` nanoseconds everywhere except the EWMA field, which is `double` by necessity (documented trade-off in `stats.h`). |
| Google Benchmark methodology + TSC-vs-`clock_gettime` caveat (Bilokon/Gunduz §2.5, citing Rasovsky) | `bench/bench_common.h`: explicit warmup-then-measure protocol, percentile reporting instead of a single mean, and a comment warning that relative overhead of `clock_gettime(CLOCK_MONOTONIC)` vs. TSC-based timing is environment-dependent — benchmark numbers from this project should be read as "measured on this machine," not assumed portable. |
| Kernel bypass (Bilokon/Gunduz §3.5) | Not applied — needs specialized NICs/drivers (DPDK/Solarflare/Mellanox) that don't fit a portable epoll-based showcase. Kept as a **Future Enhancement** note (§14) rather than implemented. |

**Checked, not directly applicable:**

- **`Preparation/Books/Research paper low latency trading.pdf`** (Hasbrouck & Saar, *Journal of Financial Markets* 2013) — an academic market-microstructure study of how HFT affects spreads/depth/volatility. Domain motivation for *why* feed monitoring matters, no systems-engineering content to apply to a code file.
- **`Preparation/Books/Inside the black box...pdf`** and the rest of `Preparation/Books/` — HFT business/strategy context, not implementation guidance for this project.
- **`Preparation/Books/Scott_Meyers_Effective_Modern_C++.pdf`** — general modern-C++ idiom (move semantics, RAII, smart pointers, rule of zero) is applied throughout as standard practice; not re-derived from the book page by page since none of it is project-specific.
- The CppCon compilation has no talk on `epoll`/sockets/TCP specifically (checked via keyword search — zero hits on "epoll", "reactor", "TCP_NODELAY", "Nagle"), which is exactly the gap audit finding #1–#9 already covers from first principles.

## 16. Final Stress Pass and Polish (Phase 11)

- **`make debug` (the full clean + ASan/UBSan build of everything + test suite) ran end-to-end for the first time** — it couldn't before Phase 8 existed (`main.cpp` was missing). It caught one real bug immediately: `feed_simulator_test.cpp`'s "a single client completes the handshake and gets tracked" test flaked under the sanitizer's much slower instrumented execution. The cause was a genuine, *pre-existing* race, not a sanitizer artifact: the test closed a client fd and immediately let its scope end (triggering `BackgroundRun`'s destructor, which calls `stop()`), with no guarantee that the reactor thread's `epoll_wait` would observe the resulting FIN before it observed `stop()`'s `eventfd` write — two independent async events with no ordering relationship. Under a fast release build, the FIN usually (not always) won that race by chance; under ASan/UBSan's overhead, it stopped winning reliably. Fixed with the same bounded `sleep_for(100ms)` an adjacent, near-identical test already used correctly — the inconsistency was mine, applying the fix to one test and not the other. Audited every other `close()`-then-implicit-`stop()` pattern in the test suite afterward; the rest all use causally-sound synchronization (a blocking `accept()` that can only return after the reactor really acted, or observing an RST that is a direct side-effect of the server-side state change completing) rather than a timing assumption.
- **Full sanitizer sweep, clean**: 67/67 under `-Wall -Wextra -Wpedantic`, 67/67 under ASan+UBSan (including both binaries), 67/67 under ThreadSanitizer.
- **Stress-tested to the top of the original spec's range**: `throughput_bench` at 5000 simultaneous connections (the "monitor 1000s of connections" claim from the original brief) completes in the same run as 10/1000/2000, with CPU scaling roughly linearly with connection count (0.5% → 17% → 32% → 57%) and RSS growing modestly (4MB → 47–67MB) — no cliff, no pathological blowup.
- **20-second live stability run of the real binaries** (`feed_monitor` against a `feed_simulator --drop-rate 0.6`, short intervals to force frequent churn): 17 full disconnect/reconnect cycles, open fd count sampled every 4 seconds, held rock-steady at 7 throughout — no fd leak in the reconnect path under sustained real-world-like failure conditions.
- **A second real bug, found by the user driving the actual binary interactively rather than by any automated test**: piping commands into `feed_monitor`'s stdin (`printf "disconnect 1\nset-interval 200\nquit\n" | ./bin/feed_monitor ...`) silently did nothing — the process ignored every command and only exited via an external `timeout`. Root cause in both `FeedMonitor::on_command_fd_readable` and `FeedSimulator::on_command_fd_readable`: on `read()` returning 0 (EOF), the function `return`ed immediately, before ever reaching the line-parsing loop below it -- so any commands appended to `command_line_buffer_` earlier in that *same* call were silently discarded. This never showed up in testing because every test either used a long-lived pipe (write end held open past the point the command was consumed) or an interactive-TTY-style scenario, where EOF essentially never arrives bundled with the last real line in one `read()` call. Piped/redirected input -- ordinary, common usage -- hits this every time, because the writer (`printf`, a script, a heredoc) closes its end right after writing, so the final command and EOF routinely land in the same `read()`. Fixed in both places: the parsing loop now always runs before EOF-triggered cleanup, and a final line with no trailing newline is now treated as a complete command too (rather than silently retained forever), covering `echo -n quit | feed_monitor` as well. Two regression tests added (`feed_monitor_test.cpp`, `feed_simulator_test.cpp`), both reproducing the bug by writing a command and closing the pipe's write end immediately rather than at the end of the test — 69/69 tests now pass, clean under ASan/UBSan and ThreadSanitizer.

## 17. Ruthless Re-Audit: Benchmark Methodology + Codebase Findings (Phase 12)

After §16's clean sanitizer sweep, the user asked for a second pass that took nothing on faith: a critical read of the actual benchmark numbers produced by two `latency_bench` runs and one `throughput_bench` run, an adversarial full-codebase audit (three independent fresh-eyes agents, each covering a different layer with no visibility into the others' findings or this document's design rationale), and a check of previously-unread reference material for anything missed. This found **real, confirmed bugs** — not style nits — including one (F1 below) in code re-traced and personally called correct minutes earlier in the same session, which is the clearest evidence the pass was worth doing. Every finding was independently re-verified against exact line numbers before being acted on.

**Reference material closed out**: `Downloads/Deciphering_Cpp_Coroutines-merged.pdf` (265 pages) turned out to contain a second, distinct CppCon talk merged in — Robert Leahy (MayStreet Inc.), "The Networking TS in Practice," with a literal "Heartbeat Example" and the round-robin multi-reactor scaling pattern now recorded in §14. `t6_merged-merged.pdf` (218 pages, Meerkat's "Zero-Coordination Principle" and Rein's tail-latency-aware scheduling) is multi-node/multicore material whose transferable idea — avoid cross-core coordination — this project already satisfies by construction (single-threaded, finding #17 in §15); no code changes indicated. Bilokon & Gunduz §5–6 (Disruptor pattern, evaluation), read in full this pass rather than just grepped, confirms not building a Disruptor-style MPMC ring buffer was correct for this project's actual shape (no producer/consumer thread split to decouple).

### Correctness bugs found and fixed

- **`index_of_fd_` stale re-insertion (`FeedMonitor::remove_slot_at`)**: a `kFailed` slot keeps its old, already-closed fd number until its *next* successful `rebind()`. Swap-and-pop unconditionally repointed `index_of_fd_[moved_fd]` for any moved slot, silently re-inserting the exact entry `on_connection_failed` had deliberately erased — an unbounded leak in `index_of_fd_` if that slot later gave up retrying (reachable whenever `max_reconnect_attempts >= 0`). Fixed by only repointing when the moved slot's connection isn't `kFailed`. Regression test: fail one feed, leave it in backoff, remove a different (earlier-indexed) feed, assert the failed feed's old fd never reappears in the index.
- **EOF-in-the-same-read-loop-as-final-data (`Connection::on_readable`)**: the third and last occurrence of a pattern already fixed twice elsewhere in this codebase (§16's `on_command_fd_readable` fix). `recv()` returning 0 called `handle_peer_closed()` and returned immediately, skipping `drain_read_buffer()` — a final PONG arriving in the same instant as the peer closing was silently discarded. Fixed identically to the other two instances: track EOF as a flag, always drain first, act on EOF only after.
- **Reordered PONG misclassified as stale (`Connection::handle_pong`)**: the dedup check assumed non-decreasing sequence arrival, but `set_ping_interceptor` (built for `FeedSimulator`'s `extra_latency` fault injection) can deliberately defer an earlier PING's reply behind a later one's — so a legitimate reordered PONG could arrive after a newer one and get dropped as "stale," silently losing an RTT sample in exactly the fault-injection path this project built to be tested rigorously. Fixed by replacing the single "highest sequence seen" threshold with a bounded 32-entry ring of recently-acked sequences, checked for membership instead of ordering.
- **`feed_id` silent `uint32_t` wraparound (`config.cpp`)**: only the lower bound (`< 0`) was checked before the cast; an oversized value (typo, or a pasted unrelated number) truncated to an arbitrary different `feed_id` with no warning — inconsistent with the adjacent port check, which already validated both bounds. Folded into a broader fix (below) alongside every other unchecked numeric config field.
- **`force_reconnect()` not resetting `reconnect_attempts` (`FeedMonitor`)**: cleared `give_up` and armed an immediate retry, but left the attempt counter at whatever value had already tripped `give_up` — so a manually forced reconnect after exhaustion gave up again after just one more failure, defeating the operator's actual intent. Fixed by resetting `reconnect_attempts = 0` alongside `give_up = false`.
- **`0.00ns` read as a real RTT measurement for a feed with zero samples (`cli_display.cpp`)**: `format_duration_ns` was fed the RTT fields unconditionally, producing a literal `"0.00ns"` for any feed that had never received a pong — visible directly in a live demo run under heavy drop. `format_uptime` already had the right pattern for exactly this ("-" for no data yet); applied the same pattern to LAST_RTT/EWMA_RTT/P99_RTT, gated on `heartbeats_acked == 0`.
- **Unchecked numeric config fields (`config.cpp`)**: beyond the `feed_id` case above, every duration/count field (file and CLI paths) accepted negative, zero, or absurd values with no warning, contradicting `config.h`'s own doc comments. Replaced the scattered ad-hoc parsing with one shared `require_int_at_least()` helper used consistently everywhere, with `max_reconnect_attempts` keeping `-1` as its documented "unlimited" sentinel.
- **`Role::kResponder` connections never timing out (`Connection::check_timeout`, `FeedSimulator`)**: reverses a deliberate Phase-5/7 simplification, confirmed in scope with the user rather than assumed. A hung or silent test client occupied a `Connection` + fd on `FeedSimulator` indefinitely. Fixed by generalizing `check_timeout` — the existing "stuck in this state too long" check now applies to a responder stuck in `kHandshaking` unchanged; a new `last_liveness_ns_` (renamed from the initiator-only `last_pong_time_ns_`, now also updated by `handle_connect_hello`/`handle_ping`) drives a simpler single-strike check for a responder gone quiet in `kHealthy`, no `kDegraded` intermediate. `FeedSimulator::on_timer_tick` now scans and reaps failed clients each tick, reusing the timer already wired up for fault injection's deferred-reply queue.
- **Unbounded command-line input buffer (`command_line_buffer_`, both reactors)**: no length cap — a command source that never emits `\n` grows it forever. Low severity today (`command_fd_` is always a locally-supplied `STDIN_FILENO` in the shipped binaries), but latent if `set_command_input` is ever wired to something less trusted. Capped at 64KB in both `FeedMonitor` and `FeedSimulator`, warning and discarding on overflow.

Every fix above got a real regression test, verified with the same revert-then-restore discipline used throughout this project: implement the fix, write the test, confirm it passes, temporarily revert the fix from a backup, confirm the test now *fails*, restore the fix, confirm the suite is green again. Two test-design pitfalls surfaced and were corrected during this: an old `check_timeout`-for-responder test asserting now-incorrect behavior had to be replaced rather than just left passing by accident, and a length-cap test that appended the trailing real command directly onto the oversized payload in one buffer would occasionally have the command swallowed by the same discard as the garbage that preceded it, since `on_command_fd_readable` reads in fixed 1024-byte chunks that don't respect logical content boundaries — fixed by sending the real command as a second, later write.

### Benchmark methodology flaws found and fixed (`bench/`)

- **`latency_bench`'s `TCP_NODELAY` comparison confounded by fixed, unrandomized trial order**: exactly one trial per configuration, always `true` then `false`. This alone explains the "false beats true" pattern seen in two independent live runs — whichever trial ran second inherited a more CPU-frequency-warmed state than the first (this machine runs the `powersave` governor), a cross-trial confound the benchmark's existing in-trial warmup never addressed. Fixed by running both configurations `repetitions` times (default 5) via a new `bench::repeat_trials()` helper, alternating which configuration goes first each repetition to cancel the effect out, and reporting min/median/max spread per percentile instead of one point estimate.
- **`throughput_bench`'s CPU% conflated the monitor under test with its own echo-peer thread**: both ran as threads in the same process, but `getrusage(RUSAGE_SELF, ...)` is process-wide and can't separate them — every reported `steady_cpu%` overstated `FeedMonitor`'s real cost by the peer's own cost. Fixed by switching to `pthread_getcpuclockid()` + `clock_gettime(CLOCK_THREAD_CPUTIME_ID, ...)` scoped to `monitor_thread` specifically; observed CPU% dropped substantially once isolated, as expected.
- **`throughput_bench`'s trial cadence was 5x more aggressive than the shipped defaults** (200ms heartbeat interval vs. `config.h`'s 1000ms), with nothing in the output saying so — every reported CPU% was roughly 5x what a default-config deployment would actually show at the same connection count. Fixed by defaulting the trial config to the real shipped defaults (1000ms interval, 3000ms timeout, matching the 1:3 ratio), overridable via a CLI argument, with the actual cadence in effect printed alongside every result line.
- **Neither benchmark repeated a configuration more than once**: every prior number was a single-sample point estimate with no reported variance. `bench::repeat_trials()` (default n, min/median/max via a new `bench::summarize_spread()` helper) is now used by both `latency_bench` (interleaved `TCP_NODELAY` comparison above) and `throughput_bench` (each connection-count/`epoll_max_events` configuration re-run 3 times by default).

### Confirmed correct by this audit (recorded so it isn't re-litigated)

`remove_slot_at`'s swap-and-pop for `index_of_feed_id_`; the `stop_requested_` + eventfd cross-thread stop pattern; the exponential backoff's overflow-avoidance cap; `on_timer_tick`'s O(n), not quadratic, scan; both signal handlers' async-signal-safety; `ConnectionStats`' EWMA seeding and `RingBuffer`/`percentile()` interpolation.

---

## 17.1. Closing Two Benchmarking-Plan Gaps (Phase 13)

A follow-up audit question ("what did the original spec ask to be measured, and are we actually doing that") surfaced two items §12 had explicitly promised that were never actually delivered — one silently, one via a documented but unimplemented scope decision. Both are now closed.

- **`epoll_wait` calls/sec and events processed/sec** — §12 promised this instrumentation; `throughput_bench.cpp` never measured it. `FeedMonitor` gained two `std::atomic<uint64_t>` counters (`epoll_wait_count_`, `events_processed_count_`), incremented once per successful `epoll_wait()` call and once per event it returns, exposed via `debug_epoll_wait_count()`/`debug_events_processed_count()`. Plain monotonic counters need no coordination with the reactor thread to read safely from another thread (unlike `debug_fd_index_count()`, which needs the reactor stopped first) — a relaxed atomic load is sufficient. `throughput_bench` samples both before/after its existing 2-second steady-state window and reports the rate alongside CPU%/RSS. On this machine: ~20-22 `epoll_wait`/sec at N=10-1000 (dominated by the periodic tick firing every `tick_interval`, 50ms by default = 20/sec, plus real heartbeat traffic), climbing to ~30-38/sec at N=5000 as heartbeat traffic starts contributing more wakeups of its own; the `epoll_max_events` batch-size sweep at N=5000 shows the mechanism working as designed — `epoll_max_events=16` needs ~176 waits/sec to drain the same ~2520 events/sec that `epoll_max_events=1024` drains in ~23 waits/sec, the syscall-count-vs-batch-size trade-off the parameter exists to control.
- **`epoll` vs. naive blocking-thread-per-connection baseline** — the other explicit comparison §12 (and the original spec) asked for, scoped out at build time as "real work, out of proportion to what's left." Built now: `throughput_bench --naive` adds a section spawning one real blocking OS thread per connection (blocking `connect()`, blocking handshake, then a blocking send-PING/wait-PONG loop paced by `heartbeat_interval`, timing out via `SO_RCVTIMEO` so threads still notice a stop signal) — deliberately the simplest correct implementation of "watch N feeds without an event loop," not a tuned alternative architecture. Per-thread CPU accounting (same `pthread_getcpuclockid` approach as the epoll-side F8 fix) sums cost across all N worker threads rather than using process-wide `getrusage`, for the same reason: the shared echo-peer thread must not be counted as either architecture's cost.
  - **A real measurement artifact was caught while sanity-checking the first combined run**: `--naive`'s RSS readings (72MB-130MB) looked far too high for the connection counts involved. Isolating N=10 alone confirmed it — 4.66MB standalone vs. 72MB when run immediately after the epoll section's N=5000 trials in the same process. RSS is an absolute snapshot (unlike CPU%, a before/after delta within its own window and so immune to this), and glibc's allocator doesn't return freed memory to the OS after a large trial finishes — every naive-section RSS number was inflated by the epoll section's high-water mark, not reflecting the naive baseline's own footprint. Fixed by adding `--naive-only`, which skips the epoll section entirely so the naive baseline can be measured as the first and only thing in its own process.
  - **Results (this machine, `--naive-only`, clean per-N process for RSS; CPU%/setup from the combined run since those aren't cross-contaminated)**: at N=10 the naive approach is actually slightly *cheaper* than `epoll` in both CPU (0.065% vs. 0.11%) and RSS (4.4MB vs. 4.3MB) — no event loop or timer overhead to pay when there's almost nothing to multiplex. The crossover happens fast: by N=100 naive CPU (0.51%) already exceeds `epoll`'s (0.21%); by N=1000 it's roughly 4x (5.08% vs. 1.22%); by N=5000 naive CPU is ~23% of one core vs. `epoll`'s ~6.8% — over 3x higher — while RSS at N=5000 is 66MB (naive, thread stacks) vs. 72MB (`epoll`, comparable — the flat `Connection` vector doesn't clearly win on memory at this N, it's CPU where the difference is stark). This is exactly the story the original spec's comparison ask was fishing for: naive blocking-thread-per-connection is a perfectly reasonable choice at a handful of connections, and gets measurably, increasingly worse than an `epoll` reactor as connection count climbs into the hundreds and thousands — the regime the spec's own "1000s of connections" claim lives in.
  - **superseded by §19 (Phase 15)**: these specific absolute percentages were later found to rest on a methodology bug (every spawned bench thread, naive workers included, was silently pinned to a single CPU) and are re-measured there with the bug fixed. The *trend* described above (naive gets rapidly, increasingly worse than `epoll` as N climbs) is not just preserved but sharper post-fix — see §19 for the corrected numbers and an explicit note on this sandbox's run-to-run variance.

---

## 18. Functional Validation: Proving the Actual Project Motive (Phase 14)

`latency_bench`/`throughput_bench` prove the heartbeat mechanism is fast and the architecture scales -- they do not, by themselves, prove the thing the whole project exists to do: **detect silent feed failure, mark the connection unhealthy, stop trusting it, reconnect automatically, and resume monitoring.** A pointed follow-up question ("what was supposed to be calculated, and are we doing that, for the *correctness* claims, not just the performance ones") drove an audit of test coverage against fourteen specific behaviors the state machine claims to implement, followed by six fault-injection integration scenarios and a live demonstration.

### A real bug found while auditing test coverage, not while looking for one

While checking coverage for "stale/late PONG is not accepted as proof of current health," reading `handle_pong` revealed it only checked *"have I already counted this sequence,"* never *"did this reply arrive within a timeframe that still proves anything."* Verified empirically before treating it as a finding: a hand-built PONG for a PING sent long enough ago that its round trip already exceeded `heartbeat_timeout`, delivered while the connection sat in `kDegraded` after several real misses, was accepted and immediately flipped the connection back to `kHealthy` -- with a reported RTT of several seconds. Confirmed with the user before fixing (this changes real protocol semantics, the same bar F13 was held to): `handle_pong` now rejects any PONG whose implied RTT (`now_ns - msg.timestamp_ns`) already meets or exceeds `heartbeat_timeout` as evidence of current health -- still remembered as acked (so a legitimate later duplicate isn't reprocessed), but it no longer resets `last_liveness_ns_`/`consecutive_missed_` or resurrects a `kDegraded` connection. One stale straggler can no longer mask every real miss that happened after it was sent. Covered by both a `connection_test.cpp` unit test (hand-built stale PONG, revert-verified) and a `feed_monitor_test.cpp` integration test (the same scenario via `FeedSimulator`'s real `extra_latency` fault injection set above `heartbeat_timeout`).

### Coverage audit against the fourteen claimed behaviors

Auditing all 85 existing tests by name and body (not just by title) against every behavior the state machine claims found most of it already genuinely covered from the original 11-phase build and the Phase-12 audit: missing-PONG detection, the HEALTHY/DEGRADED/FAILED transition with exact injected-time boundaries, max-missed-heartbeat handling, socket-error detection (`on_hangup_or_error`, peer-close), duplicate-PONG rejection (`has_acked_sequence`), reordered-but-legitimate-PONG acceptance (F3), sequence reset and cumulative-stats survival across `rebind()`, and corrupted-message rejection at the wire-decode level were all already real, assertion-backed tests -- not just plausible-sounding names. Confirmed gaps, closed this pass: exponential backoff/jitter had zero direct coverage (the pure function was never called from a test, nor was real wall-clock spacing between retries ever observed); `emit_failure_alert`'s actual log line was never verified to fire; `DEGRADED -> HEALTHY` recovery (a real, already-correctly-implemented code path) had no test at all; and every one of the six fault-injection *integration* scenarios below -- proving the full `FeedMonitor` + `FeedSimulator` chain, not just `Connection` in isolation -- was missing.

### Six fault-injection integration tests added (`feed_monitor_test.cpp`)

1. **Dropped-PONG**: `FeedSimulator` with `drop_probability=1.0` drives a real `HEALTHY -> DEGRADED -> FAILED` cycle; a new `StderrCapture` RAII helper (redirects fd 2 to a temp file for the test's duration, since `emit_failure_alert` writes straight to `std::cerr` with no callback hook) confirms the real `"[feed_monitor] ALERT: feed 1 ... failed"` line actually fires; since `drop_probability` only affects PONG replies (not the handshake -- see `FeedSimulator::on_ping_intercept`), the subsequent reconnect genuinely succeeds and the feed returns to `HEALTHY`, proving "reconnect scheduled" concretely rather than inferring it.
2. **Reconnection + recovery state** (combined into one test, since they're naturally sequential): `kill_random_connection()`'s `SO_LINGER{1,0}` RST drives the socket-error path (`ECONNRESET`), distinct from the existing EOF-only reconnect test. Verifies the new socket is active, the handshake succeeds, heartbeats *resume* (the acked count is observed climbing past its pre-kill value, not just the state enum flipping once), `stats().reconnects` increments, and cumulative stats survive rather than resetting to zero.
3. **Temporary failure**: drop replies just long enough to reach `DEGRADED`, restore them before `max_missed_heartbeats` is reached, confirm recovery to `HEALTHY` with `stats().reconnects == 0` -- proof the monitor doesn't overreact to a transient blip by forcing an unnecessary full reconnect.
4. **Slow response**: `FeedSimulator`'s real `extra_latency` fault, set above `heartbeat_timeout` -- every reply genuinely arrives, nothing is dropped, but always too late to count, so the connection still correctly fails. The integration-level proof of the stale-PONG fix above.
5. **Backoff + jitter**: a new `debug_reconnect_delay_ns(attempt)` test accessor makes the private backoff computation directly testable -- one test asserts the 1x/2x/4x/.../capped doubling holds within the documented +/-25% jitter band across many samples per attempt, a second asserts the jitter is actually randomized (not a constant, which would reintroduce the exact thundering-herd failure mode it exists to prevent), and a third is a real wall-clock integration test: an offline peer accepts four genuine, separately-timed reconnect attempts, confirming actual growing gaps between them, not a tight loop.
6. **No false failure**: the complement to every fault-injection test above -- a healthy feed under `fast_test_config()`'s aggressive cadence with *no* fault injection, observed across many real heartbeat cycles, never transitions away from `HEALTHY`.

All new tests, including the stale-PONG fix, pass clean under `-Wall -Wextra -Wpedantic`, ASan+UBSan, and ThreadSanitizer (94/94 total). `StderrCapture`'s cross-thread stdout/log capture and the wall-clock backoff test's real timing were the two additions most likely to introduce a race or flake; both came back clean under TSan.

### Live demonstration (`demo/lifecycle_demo.cpp`, `make demo` -> `bin/lifecycle_demo`)

A standalone, permanent demonstration binary (not a one-off transcript) drives a real `FeedMonitor` against a real `FeedSimulator` -- the same production classes the shipped binaries use, over real loopback sockets and real timers -- and prints a line only when the feed's actual observed state changes, sourced from the tick callback, nothing pre-scripted. Fault injection is driven through the same command-pipe mechanism the test suite and a real operator's stdin both use (`set_fault_config` is reactor-thread-only by design; this is not called directly from `main()`'s thread). Uses `config.h`'s real shipped cadence (1000ms heartbeat interval, 3000ms miss deadline) rather than a sped-up demo cadence -- a sub-second "missed heartbeat" doesn't read as believable, so the full cycle takes about 8 seconds to watch, not under 1. Reproduced identically across repeated runs:

```
[HH:MM:SS] NASDAQ: HEALTHY
[HH:MM:SS] --- Simulator stops replying ---
[HH:MM:SS] NASDAQ: DEGRADED
[feed_monitor] ALERT: feed 1 ("NASDAQ" 127.0.0.1:PORT) failed
[HH:MM:SS] NASDAQ: FAILED
[HH:MM:SS] Reconnect attempt scheduled (exponential backoff + jitter)
[HH:MM:SS] --- Simulator becomes available again ---
[HH:MM:SS] NASDAQ: CONNECTING
[HH:MM:SS] NASDAQ: HEALTHY
```

(An incidental `[connection] feed 1 ... peer closed connection` line sometimes appears right around the `CONNECTING -> HEALTHY` transition -- that's `FeedSimulator`'s own `Connection` on the *old*, now-superseded socket logging its own genuine EOF once `rebind()` finally closes it; both processes are logging their own real view of independent events, not a bug.)

---

---

## 19. Full-Proof Gap Closure (Phase 15)

A second self-audit ("what's still unproven, not just what's untested") turned up 20 further items: 4 concrete bugs findable by inspection, and 16 real gaps in scenario coverage, verification methodology, and documentation. Two needed an explicit scope decision from the user before work started, both confirmed: no CI pipeline (no git repo exists; creating one is a separate decision, recorded as a deliberate gap below, not silently dropped), and real kernel-level network fault injection (`tc netem`) delivered as a script the user runs themselves, since this session has no passwordless `sudo`. Everything else was closed to the same standard as every prior pass: implement, write a regression test, revert-verify it (temporarily undo the fix, confirm the test fails, restore, confirm green again), then sweep with ASan/UBSan and ThreadSanitizer.

### Four concrete bugs, fixed and revert-verified

- **`Connection::write_pending_` had no cap.** A peer that stops reading (wedged, slow-loris) let queued outbound bytes grow forever, one heartbeat at a time, with no drain guarantee. Capped at 64KB (matching the existing `command_line_buffer_` precedent); on overflow the connection is failed via the existing `handle_protocol_violation` path rather than silently discarding queued bytes, which would desync the peer's own decoder — a strictly worse outcome than tearing down a connection that's already un-servable. `connection.h`/`connection.cpp` (`queue_write`).
- **`Connection::read_accum_` was only bounded *across* `on_readable()` calls, not within one.** The recv-until-`EAGAIN` loop keeps appending across many `recv()`s before `drain_read_buffer()` ever runs once at the end of the call — a peer sustaining a flood faster than the drain rate (plausible on loopback) could grow `read_accum_` unboundedly within a single invocation. Same 64KB-class cap, checked inside the recv loop itself, same fail-the-connection outcome as above. `connection.cpp` (`on_readable`).
- **A future-timestamped PONG produced a negative RTT and was silently accepted as proof of health.** Empirically confirmed (`last_rtt_ns = -999999999998` from a hand-built malicious/clock-skewed PONG). `handle_pong` now rejects `rtt_ns < 0` alongside the existing "too old" staleness check — not proof of current health, not physically meaningful either way.
- **SIGPIPE safety was per-call, not structural.** Every `send()` already passes `MSG_NOSIGNAL` correctly, so this was defense-in-depth, not a live bug. `::signal(SIGPIPE, SIG_IGN)` added once at the top of `main()` in `main.cpp`, `main_simulator.cpp`, and `demo/lifecycle_demo.cpp`.

### Scenario coverage gaps closed

- **20 feeds failing and recovering simultaneously.** One `FeedSimulator` instance with 20 feed_ids registered against its single `host:port` (`FeedMonitor::add_feed` only dedupes on `feed_id`, confirmed nothing stops this) — one `set-drop-rate 1` command fails all 20 deterministically, far cheaper and less flaky than 20 separate simulator threads would have been. `max_reconnect_attempts=0` forces exactly one ALERT per feed; a `StderrCapture` RAII helper confirms 20 distinct `"ALERT: feed"` lines, and `debug_fd_index_count()` stays consistent after stopping the reactor. Recovery drives all 20 back to `kHealthy` via explicit `force_reconnect` calls routed through a command pipe. A second, cheaper test stresses `slots_`/`index_of_fd_` bookkeeping via `force_disconnect()` across 20 slots synchronously, independent of real fault-injection timing.
  - Two genuine ThreadSanitizer-caught data races surfaced in this test's own first draft, not in production code: calling `force_reconnect()` directly from the test thread while the reactor thread was running (fixed by routing through the command pipe, the documented reactor-thread-only contract every other mutation already follows) and closing simulator command-pipe fds from the main thread while simulator background threads — which outlive the monitor's own run scope — were still alive (fixed with an explicit `sim_runs.clear()` before the close loop).
- **A real shipped-binary end-to-end test** (`test/e2e_binary_test.cpp`): `fork()`+`execve()` launches the actual compiled `bin/feed_monitor` and `bin/feed_simulator`, drives real stdin commands, captures real stdout/stderr, and verifies clean `SIGINT` shutdown (`WIFEXITED && WEXITSTATUS==0`). Caught a real production bug: the "listening on port" startup banner was never flushed before `simulator.run()` blocked for the rest of the process's life — invisible on a TTY (line-buffered), but a real hang for any piped/redirected consumer (a log collector, a supervisor, or this very test). Fixed with an explicit `std::flush`. A second test confirms a malformed `--port` value now fails fast with a clear error instead of silently binding to port 0 (see the clang-tidy finding below).
- **Real network-layer fault injection**, `scripts/network_fault_demo.sh`: uses `tc qdisc add dev lo netem loss/delay/reorder` against the real shipped binaries, with cleanup on any exit path via a trap. Verified by careful inspection only (correct `tc` syntax, unconditional cleanup) — this session has no `sudo`, so it has not been executed or observed running. Documented plainly, not glossed over.
- **Operator command during active failure**: a feed mid-backoff after a real `drop_probability=1` failure gets `force_reconnect` fired via the command pipe while still waiting out `reconnect_ready_at_ns`. No double-connect, no corrupted slot state, exactly one clean reconnect.
- **Live demo extended to 3 feeds**: NASDAQ, NYSE, LSE, only NASDAQ faulted. NYSE/LSE are asserted to never leave `HEALTHY` for the demo's duration, demonstrating fault isolation (one feed dying doesn't touch the others), not just the single-feed mechanism.
- **Connection-flood / handshake-storm**: 300 raw sockets burst-connect to one `FeedSimulator` and never send `CONNECT_HELLO`; all are confirmed eventually reaped (`client_count()` back to 0) without the reactor thread stalling or any fd leaking.
- **Mid-flight `heartbeat_timeout` shrink — a real bug found and fixed.** A connection partway through accumulating missed heartbeats, with `set_heartbeat_timeout()` called to a smaller value mid-flight, could spuriously degrade an already-healthy connection: the timeout math was being evaluated against a liveness baseline set under the *old*, larger timeout. Fixed by having `set_heartbeat_timeout()` reset every slot's connection liveness baseline when the value changes. Regression-tested and revert-verified — the first diagnostic script written for this actually gave a false negative (it computed elapsed time against an arbitrary large absolute timestamp instead of real elapsed-since-last-liveness-update), corrected before trusting the result.
- **`heartbeat_interval` smaller than `tick_interval`**: observed real ping-send rate over a wall-clock window and confirmed it's throttled to the tick rate rather than bursting to catch up — a real, already-correct behavior now locked in by a test rather than assumed.

### Verification infrastructure

- **Fuzzing** (`make fuzz`, `clang++ -fsanitize=fuzzer,address`, kept out of `make all`/`make test`): `fuzz/decode_heartbeat_fuzz.cpp` fuzzes `decode_heartbeat` directly (always exactly 28 bytes — `HeartbeatWireBuffer` is a fixed-size array, so there's no "wrong size" dimension at that layer) — ran 34,724,088 iterations clean in 30 seconds. `fuzz/drain_read_buffer_fuzz.cpp` fuzzes `Connection` with arbitrary-length byte streams over a real socketpair handshake, the layer that actually answers "is the wire protocol robust to arbitrary input." Its logic was verified correct via a ~3,000-iteration debug run, but long automated runs hang on this specific sandbox due to an apparent libFuzzer-internal thread/pipe interaction (confirmed via `/proc/<pid>/wchan` inspection — the process sits blocked in `anon_pipe_read` at near-zero CPU; `ptrace` itself is blocked in this sandbox, ruling out a debugger-based diagnosis). Documented honestly as a sandbox limitation, not a code defect, rather than continuing to debug an environment quirk indefinitely.
- **Measured code coverage** (`make coverage`, `gcov` — `lcov`/`genhtml` aren't installed on this machine): `heartbeat.cpp` and `stats.cpp` 100%; `cli_display.cpp` 99.25%; `feed_monitor.cpp` 88.29%; `connection.cpp` 85.56%; `feed_simulator.cpp` 85.84%; `config.cpp` 75.95%; `main.cpp` 46.05% and `main_simulator.cpp` 54.03% (both low because `gcov` only instruments code linked into `run_tests` — the real binaries these two files build are exercised through `e2e_binary_test.cpp` as separate `exec`'d processes, whose coverage can't be captured by an instrumented `run_tests` build; the CLI-parsing and startup logic in both files *is* exercised, just not counted here). No 0%-covered reachable branch was found that wasn't already structurally defensive/unreachable code.
- **Static analysis** (`clang-tidy`, `bugprone-*`/`performance-*`/`clang-analyzer-*`; `cppcheck` isn't installed, skipped rather than faked): one genuine bug found and fixed — `main_simulator.cpp` used raw `atoi`/`atof` for `--port`/`--feed-id`/`--drop-rate`/`--extra-latency-ms` (`cert-err34-c`), which silently return 0 on garbage input instead of signaling an error, so `--port abc` would silently bind to port 0 rather than telling the operator they mistyped a flag — inconsistent with `config.cpp`'s already-hardened `strtoll`/`strtod`-based validation. Fixed the same way, failing fast with a clear error; the new e2e test locks this in. Every other finding was triaged by hand and dismissed with a specific reason (false positive or accepted style), not silently ignored.
- **`bench/` and `demo/` audited with the same adversarial rigor `src/` already had** (a fresh-eyes agent pass, then independently reproduced by hand). One significant real finding: **CPU-affinity inheritance.** `bench::try_pin_and_prioritize(0)` pins `main()` to CPU 0 for low scheduling-noise latency measurement; on Linux, every `std::thread` spawned afterward silently inherits that same single-CPU mask by default (`PTHREAD_INHERIT_SCHED`), confirmed with a standalone diagnostic on this 10-core machine. This meant `throughput_bench --naive`'s worker threads — potentially thousands of them — were being squeezed onto one core rather than spread as a real deployment would, and the epoll-side peer/monitor threads were contending with the pinned measurement thread for the same core, both of which partly measure thread-contention-on-one-core rather than the architecture actually under test. Fixed with a new `bench::reset_affinity_unpinned()`, called as the first statement in every spawned bench thread (`latency_bench.cpp`'s `sim_thread`; `throughput_bench.cpp`'s `peer_thread` x2, `monitor_thread`, and the naive `workers` lambda — 5 sites total). Verified empirically (`/proc/<pid>/task/*/status`'s `Cpus_allowed_list` showed spawned threads moving from `0` to `0-11` after the fix, while `main()` correctly stays pinned to `0`), and clean under ASan/UBSan and ThreadSanitizer.
  - **This invalidates the exact CPU-percentage figures recorded in §17.1** (already annotated there pointing here). Re-measured post-fix: naive-vs-epoll steady-state CPU at N=1000 is now ~31-34% vs. ~3.7% (roughly 8-9x, up from the previously-reported ~4x); at N=5000, ~101.7% vs. ~16.1% (roughly 6.3x, up from ~3x). The trend §17.1 reported — naive gets rapidly and increasingly worse than `epoll` as connection count climbs — is not just preserved but *more* pronounced with the methodology bug fixed, and remains the correct headline conclusion. The absolute percentages themselves should be read with real caution beyond that trend: repeating the N=1000 naive measurement three times back-to-back gave 30.9%/31.9%/30.7%/33.6%, and constraining the whole process to one core with `taskset -c 0` (deliberately reproducing something close to the original bug's effect) gave ~38% — *higher* than the unpinned figure, not lower, and nowhere near the originally-reported 5.08%. This machine is a shared/virtualized sandbox with no `CAP_SYS_NICE` (`SCHED_FIFO` request fails every run, logged and non-fatal by design) and unknown neighbor load; absolute CPU-percentage benchmark figures on it were never a stable ground truth, a limitation this file's own benchmarking notes already flagged in principle (Rasovsky's timing-portability citation, §15) but which this specific 6x-magnitude swing makes concrete. Treat every benchmark number in this document as "measured on this machine, this run" — a relative trend across configurations on the *same* run is trustworthy; an absolute percentage compared across different points in this long working session is not.
- **Clang build verification** (`make CXX=clang++ test`, plus a second pass with `-fsanitize=address,undefined`): zero compiler warnings, zero errors, all 105 tests pass both plain and under ASan/UBSan.

### `CLOCK_MONOTONIC` → `CLOCK_BOOTTIME`

One-line change, single call site (`time_utils.h`'s `now_monotonic_ns()`, used by everything in the project). `CLOCK_BOOTTIME` is monotonic exactly like `CLOCK_MONOTONIC` but also advances across system suspend/resume, which is the correct semantics for a heartbeat-timeout system: a feed that's been unreachable across a host suspend should read as having missed every heartbeat during that gap, not as if no time passed. Linux-only (available since 3.17), no portability concern for a project that already only targets Linux. Verified by inspection plus the full existing timing-test suite continuing to pass unchanged (plain, ASan/UBSan, and ThreadSanitizer) — nothing depends on which specific monotonic clock is used, only that it never jumps backward and is self-consistent, both of which `CLOCK_BOOTTIME` preserves.

### Deliberate, documented gaps (not silently dropped)

- **CRC32's blind spots.** The wire protocol's checksum (CRC32-IEEE) detects all single-bit errors and the overwhelming majority of multi-bit errors, but has known blind spots for certain multi-bit error patterns (e.g., some burst-error patterns aligned to the polynomial's structure) — it is an error-detection code against accidental corruption, not a cryptographic integrity guarantee against a deliberate adversary tampering with wire bytes. Upgrading this is a protocol-breaking, wire-format-versioning decision (the wire format's `version` field is the existing extension point if this is ever revisited) out of proportion to a documentation gap, and adjacent to — arguably part of — the already out-of-scope "no crypto/auth on the wire" decision. No code change.
- **Log rotation.** All logging in this project goes to `stderr` with no in-process rotation or size bound; in a long-running deployment this is conventionally solved externally (`logrotate`, or a supervisor redirecting to a rotating sink) rather than in-process, consistent with how the original spec already scoped alerting/ops-integration out of this build. No code change.
- **CI pipeline.** No git repository exists for this project; creating one plus a CI configuration and a remote is a separate decision from this plan's scope, made explicit here per the user's decision rather than silently omitted. No code change.

### Verification summary

Every Tier-1 fix has a dedicated regression test, revert-verified (temporarily undone via a backup + a small surgical script, confirmed the test then fails, restored, confirmed green again). Every new integration test ran under a full sanitizer sweep (`make debug` for ASan+UBSan, a separate ThreadSanitizer build) in addition to a plain `make test`, not just once. Two genuine TSan-caught races were found and fixed in this session's own new test code (documented above), plus the pre-existing production bugs listed above — every one of which followed the same discipline as every prior fix in this project. Final state: 105/105 tests passing under plain, ASan+UBSan, ThreadSanitizer, and `clang++` builds; `make clean && make all && make test && make bench && make demo && make coverage` all run clean; `make fuzz` run manually for a fixed duration outside the automated chain, as designed.

---

## Next Step

None — the build is complete. Future work is whatever's listed in §14 (Future Enhancements) plus the deliberate, documented gaps in §19, none of which is required for the project as specified.
