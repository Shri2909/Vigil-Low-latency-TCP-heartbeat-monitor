# All Commands: Vigil

Every command used to build, test, verify and benchmark the project, grouped by purpose, plus the full CLI reference for both binaries. Run everything from the repository root.

- [Build and test](#build-and-test)
- [Sanitizers and warnings](#sanitizers-and-warnings)
- [Second compiler verification](#second-compiler-verification)
- [Coverage, fuzzing, static analysis](#coverage-fuzzing-static-analysis)
- [Demo and manual simulation](#demo-and-manual-simulation)
- [CLI reference](#cli-reference)
- [Benchmarks](#benchmarks)
- [CPU tuning for benchmarks](#cpu-tuning-for-benchmarks)
- [Kernel-level fault injection](#kernel-level-fault-injection)
- [Full pipeline and cleanup](#full-pipeline-and-cleanup)

## Build and test

```bash
make all                 # bin/feed_monitor and bin/feed_simulator
make test                # builds bin/run_tests (plus the two binaries it exec()s) and runs it: 178 tests
./bin/run_tests          # run the already-built suite directly
make bench               # bin/latency_bench and bin/throughput_bench
make demo                # bin/lifecycle_demo
make fuzz                # bin/decode_heartbeat_fuzz and bin/drain_read_buffer_fuzz (clang++ only)
make clean               # removes build/, bin/, build-werror/, bin-werror/, fuzz-work/, fuzz-artifacts/
```

Requirements: Linux, `g++` or `clang++` with C++20, GNU `make`. `make -j4` parallelizes the build.

Build knobs, overridable on the command line (for example `make OPT=-O3 bench`):

| Variable | Meaning | Default |
|---|---|---|
| `CXX` | Compiler | `g++` |
| `OPT` | Optimisation flags | `-O2` |
| `SANITIZE` | Comma list for `-fsanitize=`, applied at compile and link time | empty |
| `WERROR=1` | Treat warnings as errors | off |
| `HARDEN=1` | `-D_GLIBCXX_ASSERTIONS` (bounds-checked containers); off so benchmarks measure the unchecked build | off |
| `DEFS` | Extra preprocessor definitions | empty |
| `CXXFLAGS`, `LDFLAGS` | Extra flags appended last | empty |
| `BUILD_DIR`, `BIN_DIR` | Output directories | `build`, `bin` |
| `FUZZ_CXX` | Compiler for the fuzz harnesses | `clang++` |
| `FUZZ_TIME` | Seconds per harness for `make fuzz-smoke` | `20` |

Changing flags does not recompile existing objects: run `make clean` (or use a separate `BUILD_DIR`/`BIN_DIR`) when switching.

## Sanitizers and warnings

```bash
# AddressSanitizer + UndefinedBehaviorSanitizer, -O0, libstdc++ assertions (does make clean first)
make debug

# ThreadSanitizer (variable-driven, not a named target; clean first when switching flags)
make clean
make SANITIZE=thread OPT="-O1 -g" test
TSAN_OPTIONS="log_path=tsan_report" make SANITIZE=thread OPT="-O1 -g" test   # optional: reports to files

# Warnings as errors (-Wall -Wextra -Wpedantic -Werror) + libstdc++ assertions,
# built into build-werror/ and bin-werror/ so the normal build is untouched
make werror
make CXX=clang++ werror
```

On some CI-style kernels with high ASLR entropy, ASan/TSan binaries crash at startup; the CI workflow works around it with `sudo sysctl vm.mmap_rnd_bits=28`.

## Second compiler verification

```bash
make clean
make CXX=clang++ test                    # 0 warnings, 0 errors expected

# optional: clang++ with ASan + UBSan as well
make clean
make CXX=clang++ OPT="-g -O0" DEFS=-DDEBUG SANITIZE=address,undefined test
```

## Coverage, fuzzing, static analysis

```bash
make coverage            # clean rebuild with --coverage, runs the tests, prints per-file gcov line coverage
```

Fuzzing needs `clang++` (libFuzzer) and is not part of `make test`:

```bash
make fuzz                                # build both harnesses (ASan + UBSan, UBSan findings abort)

# time-boxed run of both harnesses against the committed seed corpus (fuzz/corpus/), with the
# dictionary, -close_fd_mask=3 and a hard timeout. New inputs go to fuzz-work/, crashes and
# hangs to fuzz-artifacts/; the committed corpus is never modified.
make fuzz-smoke FUZZ_TIME=30

# by hand (the first corpus directory is the writable one)
mkdir -p fuzz-work/decode fuzz-artifacts
./bin/decode_heartbeat_fuzz -max_total_time=30 -timeout=10 -close_fd_mask=3 \
    -dict=fuzz/heartbeat.dict -artifact_prefix=fuzz-artifacts/ \
    fuzz-work/decode fuzz/corpus/decode_heartbeat_fuzz

python3 fuzz/gen_corpus.py               # regenerate the seed corpus (valid-CRC frames, both roles)
```

Static analysis uses the checks configured in `.clang-tidy` (`bugprone-*`, `performance-*`, `clang-analyzer-*`):

```bash
clang-tidy src/*.cpp -- -std=c++20 -Isrc
```

## Demo and manual simulation

```bash
make demo
./bin/lifecycle_demo     # scripted 3-feed run, about 8 s; exit 0 = recovered and NYSE/LSE stayed HEALTHY
                         # (1 = a neighbour feed was perturbed, 2 = a transition missed its 30 s deadline)
```

Two-terminal live simulation:

```bash
# Terminal A: simulated exchange feed
./bin/feed_simulator --port 9000 --feed-id 1

# Terminal B: the monitor
./bin/feed_monitor --feed 1:DEMO:127.0.0.1:9000 \
    --heartbeat-interval-ms 1000 --heartbeat-timeout-ms 3000 --max-missed 3

# or from a config file (see config/feeds.example.conf)
./bin/feed_monitor --config config/feeds.example.conf
```

Several feeds at once need one simulator per feed (a simulator answers for one feed id):

```bash
./bin/feed_simulator --port 9001 --feed-id 1 &
./bin/feed_simulator --port 9002 --feed-id 2 &
./bin/feed_monitor --feed 1:NYSE:127.0.0.1:9001 --feed 2:NASDAQ:127.0.0.1:9002
```

Fault injection without typing: `./bin/feed_simulator --port 9000 --drop-rate 0.3` drops 30% of replies; `--extra-latency-ms 4000` delays every reply past the default 3 s timeout.

## CLI reference

### `feed_monitor`

Precedence: command-line flags override the `--config` file, which overrides built-in defaults. The config file is loaded first wherever `--config` appears, and `--feed` flags add to the feeds it defines. A malformed or out-of-range value, an unknown flag, or a bad config line is reported on stderr and ignored (the default or previous value is kept); only an unreadable `--config` path is skipped whole. Cross-field rules are checked afterwards: `heartbeat_interval < heartbeat_timeout`, `reconnect_base_delay <= reconnect_max_delay`, `tick_interval <= heartbeat_interval`; a violating pair reverts to defaults with a warning.

| Flag | Meaning | Range | Default |
|---|---|---|---|
| `--config <path>` | Load a config file | | none |
| `--feed <id>:<name>:<host>:<port>` | Add a feed (repeatable). Host must be an IPv4 literal; the name cannot contain `:` | id 0 to 4294967295 and unique, port 1 to 65535 | none |
| `--heartbeat-interval-ms <n>` | PING cadence per feed | 1 to 86,400,000 | 1000 |
| `--heartbeat-timeout-ms <n>` | One missed heartbeat is this long without a valid PONG | 1 to 86,400,000 | 3000 |
| `--max-missed <n>` | Consecutive misses before `FAILED` | at least 1 | 3 |
| `--reconnect-base-delay-ms <n>` | First backoff delay | 1 to 86,400,000 | 500 |
| `--reconnect-max-delay-ms <n>` | Backoff cap | 1 to 86,400,000 | 30000 |
| `--max-reconnect-attempts <n>` | Attempts before giving up; `-1` is unlimited | at least -1 | -1 |
| `--epoll-max-events <n>` | `epoll_wait` batch size | 1 to 1,000,000 | 256 |
| `--tick-interval-ms <n>` | Timer tick: heartbeat sends and timeout checks | 1 to 86,400,000 | 50 |
| `--tcp-nodelay <bool>` | `TCP_NODELAY` on sockets | `true`/`false`/`1`/`0`/`yes`/`no` | true |
| `--edge-triggered <bool>` | `EPOLLET` instead of level-triggered | same | true |
| `-h`, `--help` | Usage | | |

Exit codes: 0 on clean shutdown or `--help`; 1 if no feed is configured or a fatal runtime error occurs. Stop with `quit`, `exit`, Ctrl-C or SIGTERM.

Stdin commands (one per line): `disconnect <feed_id>` (force a failure; the feed then reconnects through normal backoff), `reconnect <feed_id>` (retry a `FAILED` feed immediately and reset its attempt budget), `set-interval <ms>`, `set-timeout <ms>` (both must be above 0), `quit` / `exit`.

Config file keys are the flag names with underscores (`heartbeat_interval_ms`, `max_missed_heartbeats`, `reconnect_base_delay_ms`, ..., `tcp_nodelay`, `edge_triggered`) plus repeated `[feed]` sections with `id`, `name`, `host`, `port`. `#` or `;` at the start of a line, or ` #` after a value, starts a comment. See [`config/feeds.example.conf`](config/feeds.example.conf).

### `feed_simulator`

Invalid arguments exit with status 2 (unlike the monitor, the simulator is strict).

| Flag | Meaning | Range | Default |
|---|---|---|---|
| `-p`, `--port <n>` | TCP port to listen on (required) | 0 to 65535; 0 is OS-assigned | none |
| `--bind <ipv4>` | Listen address; `0.0.0.0` exposes it on all interfaces | IPv4 literal | 127.0.0.1 |
| `--max-clients <n>` | Simultaneous clients; extras are closed | 1 to 1,000,000 | 1024 |
| `-f`, `--feed-id <n>` | Feed id it answers for | 0 to 4294967295 | 1 |
| `-n`, `--tcp-nodelay <bool>` | `TCP_NODELAY` | bool | true |
| `-e`, `--edge-triggered <bool>` | `EPOLLET` | bool | true |
| `--heartbeat-timeout-ms <n>` | Reap a client that sends no PING for this long | 1 to 86,400,000 | 3000 |
| `--tick-interval-ms <n>` | Timeout and deferred-reply check cadence | 1 to 86,400,000 | 50 |
| `--drop-rate <p>` | Probability a PONG is silently dropped | 0 to 1 | 0 |
| `--extra-latency-ms <n>` | Artificial delay before each reply | 0 to 3,600,000 | 0 |
| `--jitter <bool>` | Randomize extra latency by 0.5x to 1.5x | bool | false |
| `-h`, `--help` | Usage | | |

Exit codes: 0 clean shutdown or `--help`, 1 runtime failure (for example `bind()` fails), 2 invalid command line.

Stdin commands: `set-drop-rate <0..1>` (change the drop probability live), `kill` (reset a random client connection), `quit` / `exit`.

## Benchmarks

```bash
make bench

# heartbeat round-trip latency; args: <measured_round_trips> <warmup> <repetitions>
./bin/latency_bench 20000 500 5

# epoll reactor connection-count ramp; the epoll_max_events batch-size sweep at the largest N runs in the same invocation
# args: <comma-separated counts> [heartbeat_interval_ms [repetitions]]
./bin/throughput_bench 10,100,1000,5000 1000 5

# naive thread-per-connection baseline: one count per process, so RSS is not inflated by an earlier trial's heap high-water mark
./bin/throughput_bench --naive-only 10 1000 5
./bin/throughput_bench --naive-only 5000 1000 5
```

### One shot: run everything, save results, regenerate charts

```bash
bash scripts/run_benchmarks_and_plot.sh

# the placement behind the committed results (12th-gen i5-1235U, CPUs 4 and 5 isolated at boot)
LAT_ISOLATED_CPUS=4 THROUGHPUT_CPUS=6-9,11 REPS=5 bash scripts/run_benchmarks_and_plot.sh
```

In order: best-effort governor change (needs `cpupower` and passwordless `sudo`; restored on exit), `make clean && make all && make bench`, best-effort `setcap cap_sys_nice+ep bin/latency_bench`, `latency_bench`, optionally the isolated-core latency run, the epoll ramp, then the naive runs, then `scripts/generate_key_graphs.py`. Raw output goes to a temporary directory and replaces `bench/results/*.txt` and `bench/charts/*.png` only if every step succeeded. It also writes `bench/results/environment_snapshot.txt` and warns if the thermal-throttle counter rose during a run. Run time is roughly 10 minutes.

| Variable | Meaning | Default |
|---|---|---|
| `LAT_CPUS` | `taskset` mask for the main latency run | none (the benchmark pins its client to CPU 0) |
| `LAT_ISOLATED_CPUS` | If set, also run latency under this mask, saved as `latency_bench_isolated.txt` | unset |
| `THROUGHPUT_CPUS` | `taskset` mask for both the epoll and naive throughput runs; use non-isolated cores | none |
| `COUNTS` | Connection counts | `10,100,1000,5000` |
| `REPS` | Throughput repetitions | 3 |
| `LAT_MEASURED`, `LAT_WARMUP`, `LAT_REPS` | Latency round trips, warmup, repetitions | 20000, 500, 5 |
| `SUITE_GAP_SEC` | Pause between the epoll and naive suites so TIME_WAIT sockets drain | 60 |

### Manual run, as used for the committed results

```bash
mkdir -p bench/results
./bin/latency_bench 20000 500 5 | tee bench/results/latency_bench.txt                          # client pinned to CPU 0 (P-core)
taskset -c 4 ./bin/latency_bench 20000 500 5 | tee bench/results/latency_bench_isolated.txt    # isolated E-core

# same CPU mask for epoll and naive so the comparison is fair; non-isolated cores only
nstat -n >/dev/null
taskset -c 6-9,11 ./bin/throughput_bench 10,100,1000,5000 1000 5 | tee bench/results/throughput_bench_epoll.txt
nstat | grep -E 'ListenOverflows|ListenDrops|ReqQFull|TimeWaitOverflow|RetransSegs'             # expect ~0
sleep 60
for n in 10 100 1000 5000; do
    taskset -c 6-9,11 ./bin/throughput_bench --naive-only "$n" 1000 5
done | tee bench/results/throughput_bench_naive.txt

pip install -r scripts/requirements.txt      # matplotlib, numpy (once)
python3 scripts/generate_key_graphs.py       # parses bench/results/*.txt and writes bench/charts/*.png
```

## CPU tuning for benchmarks

Optional but it changes the numbers a lot: laptop results vary with frequency scaling, idle states, other processes and thermal throttling. The committed setup is recorded in [`bench/results/ENVIRONMENT.txt`](bench/results/ENVIRONMENT.txt). Needs root; reverted by a reboot.

```bash
# governor: performance on every CPU
for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance | sudo tee "$g" >/dev/null; done

# optional, isolated-core latency run only: boot with  isolcpus=4,5 nohz_full=4,5 rcu_nocbs=4,5
# and disable deep idle states on those CPUs (revert by writing 0; raises idle heat)
for c in 4 5; do for s in 1 2 3 4; do echo 1 | sudo tee /sys/devices/system/cpu/cpu$c/cpuidle/state$s/disable >/dev/null; done; done

# loopback backlog headroom for the 5,000-connection runs
sudo sysctl -w net.core.netdev_max_backlog=16384

# SCHED_FIFO for the latency benchmark only; lost on every rebuild, so re-run after make
sudo setcap cap_sys_nice+ep bin/latency_bench

# validity check: this counter must not rise during a run (discard the run if it does)
cat /sys/devices/system/cpu/cpu0/thermal_throttle/package_throttle_count
```

Also: plug in AC power, close other applications, stop background services, and keep IRQs off the isolated CPUs. Run throughput benchmarks under `taskset -c <balanced, non-isolated cores>` for both epoll and naive; isolated cores get no load balancing. `lscpu -e` shows which CPUs are P-cores and E-cores.

## Kernel-level fault injection

Real `tc`/`netem` loss, delay and reordering on loopback against the shipped binaries. Needs root (`CAP_NET_ADMIN`) and degrades all loopback traffic while it runs, so use an otherwise idle machine. Deliberately not part of `make test` or CI. The qdisc is removed on exit, error or Ctrl-C; the script refuses to run if loopback already has a non-default qdisc.

```bash
make all
sudo ./scripts/network_fault_demo.sh
# 5 phases: baseline, 40% loss, 100% loss, recovery, 300 ms delay + 25% reorder; exits non-zero if an assertion fails
```

## Full pipeline and cleanup

```bash
make clean && make all && make test                     # plain build + full suite (178 tests)
make debug                                              # ASan + UBSan
make clean && make SANITIZE=thread OPT="-O1 -g" test    # TSan
make werror && make CXX=clang++ werror                  # -Werror on both compilers (separate build dirs)
make bench                                              # latency_bench + throughput_bench
bash scripts/run_benchmarks_and_plot.sh                 # run, save and chart all benchmarks
make demo && ./bin/lifecycle_demo                       # 3-feed live isolation proof
make coverage                                           # gcov per-file line %
make fuzz-smoke FUZZ_TIME=60                            # libFuzzer, seeded corpus (needs clang++)
clang-tidy src/*.cpp -- -std=c++20 -Isrc                # static analysis
sudo ./scripts/network_fault_demo.sh                    # real tc/netem packet loss

make clean                                              # removes build/, bin/ and the werror/fuzz scratch dirs
rm -f *.gcov                                            # stray gcov output, if `make coverage` was interrupted
```
