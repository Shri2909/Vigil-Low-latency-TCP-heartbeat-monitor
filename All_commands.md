# All Commands — Vigil (TCP Heartbeat Monitor)

Every command used across this project's build, test, verification, and benchmarking pipeline, in the order they're normally run. Grouped by purpose. Run all commands from the project root (`/home/abc/Desktop/TCP`) unless noted otherwise.

---

## 1. CPU tuning (best-effort, needs root — safe to skip)

Reduces benchmark noise from CPU frequency scaling and lets latency-sensitive threads request `SCHED_FIFO`. Both steps are optional; benchmarks print a non-fatal note and still produce valid (just slightly noisier) numbers if skipped.

```bash
# Set the CPU governor to "performance" (disables frequency scaling down for power saving)
sudo cpupower frequency-set -g performance

# Grant the benchmark binaries permission to request real-time (SCHED_FIFO) scheduling,
# without needing to run the whole benchmark as root
sudo setcap cap_sys_nice+ep bin/latency_bench
sudo setcap cap_sys_nice+ep bin/throughput_bench
```

---

## 2. Clean build

```bash
make clean               # removes build/ and bin/
make all                 # builds bin/feed_monitor and bin/feed_simulator
```

---

## 3. Unit + integration test suite

```bash
make test                # builds bin/run_tests + the two binaries it exec()s, then runs it
# equivalent manual form:
./bin/run_tests
```

---

## 4. Sanitizer builds

```bash
# AddressSanitizer + UndefinedBehaviorSanitizer
make debug                # = make clean && make (ASan+UBSan flags) all test

# ThreadSanitizer (separate manual invocation — not a named Makefile target)
make clean
make CXXFLAGS="-std=c++20 -Wall -Wextra -Wpedantic -O1 -g -fsanitize=thread" test
# optionally, to get a log file per-thread-issue instead of stderr:
TSAN_OPTIONS="log_path=tsan_report" make CXXFLAGS="-std=c++20 -Wall -Wextra -Wpedantic -O1 -g -fsanitize=thread" test
```

---

## 5. Second compiler (clang++) verification

```bash
make clean
make CXX=clang++ test                                                     # 0 warnings, 0 errors expected

# optional: clang++ with ASan+UBSan too
make clean
make CXX=clang++ CXXFLAGS="-std=c++20 -Wall -Wextra -Wpedantic -g -O0 -fsanitize=address,undefined -DDEBUG" \
     LDFLAGS="-pthread -fsanitize=address,undefined" test
```

---

## 6. Benchmarks (this project's four benchmarks)

### 6.0 Build the benchmark binaries

```bash
make bench                # builds bin/latency_bench and bin/throughput_bench
```

### 6.1 Heartbeat round-trip latency

```bash
# args: <measured_round_trips> <warmup_round_trips> <repetitions>
./bin/latency_bench 20000 500 5
```

### 6.2 Connection-count scaling (epoll reactor) + epoll_max_events batch-size sweep

```bash
# comma-separated connection counts to ramp through; the epoll_max_events
# batch-size sweep at the largest N runs automatically as part of the same invocation
./bin/throughput_bench 10,100,1000,5000
```

### 6.3 Naive thread-per-connection baseline

Run one connection count at a time, each as the *only* thing in its own process — this avoids RSS cross-contamination from a prior trial's high-water mark still being held by the allocator.

```bash
./bin/throughput_bench --naive-only 10
./bin/throughput_bench --naive-only 100
./bin/throughput_bench --naive-only 1000
./bin/throughput_bench --naive-only 5000
```

### 6.4 One-shot: everything above, saved to files, charts regenerated

```bash
bash scripts/run_benchmarks_and_plot.sh
```

This single script performs, in order: best-effort CPU tuning → `make clean && make all && make bench` → best-effort `setcap` → `latency_bench` (saved to `bench/results/latency_bench.txt`) → `throughput_bench` epoll ramp (saved to `bench/results/throughput_bench_epoll.txt`) → all four `--naive-only` runs (saved to `bench/results/throughput_bench_naive.txt`) → `python3 scripts/generate_key_graphs.py` to regenerate `bench/charts/*.png` directly from those saved `.txt` files.

To save raw output manually instead of using the script (`tee` each command):

```bash
mkdir -p bench/results
./bin/latency_bench 20000 500 5 | tee bench/results/latency_bench.txt
./bin/throughput_bench 10,100,1000,5000 | tee bench/results/throughput_bench_epoll.txt
{
    ./bin/throughput_bench --naive-only 10
    ./bin/throughput_bench --naive-only 100
    ./bin/throughput_bench --naive-only 1000
    ./bin/throughput_bench --naive-only 5000
} | tee bench/results/throughput_bench_naive.txt

python3 scripts/generate_key_graphs.py
```

---

## 7. Live demo

```bash
make demo                 # builds bin/lifecycle_demo
./bin/lifecycle_demo      # scripted 3-feed live isolation proof (NASDAQ faulted, NYSE/LSE stay HEALTHY)
```

Manual two-terminal live simulation:

```bash
# Terminal A — simulated exchange feed
./bin/feed_simulator --port 9000 --feed-id 1

# Terminal B — the monitor watching it
./bin/feed_monitor --feed 1:DEMO:127.0.0.1:9000 \
    --heartbeat-interval-ms 1000 --heartbeat-timeout-ms 3000 --max-missed 3
```

Then, typed into Terminal A's stdin while both are running:

```
set-drop-rate 1      # silently drop every PONG -> watch Terminal B go HEALTHY -> DEGRADED -> FAILED
set-drop-rate 0      # stop dropping -> watch it reconnect and recover
kill                 # forcibly RST the connection -> watch the socket-error reconnect path
quit                 # clean shutdown
```

---

## 8. Coverage

```bash
make coverage              # clean rebuild with --coverage, runs tests, prints per-file gcov line coverage
```

---

## 9. Fuzzing (clang++ only, manual/long-running, not part of `make test`)

```bash
make fuzz                                    # builds bin/decode_heartbeat_fuzz and bin/drain_read_buffer_fuzz

./bin/decode_heartbeat_fuzz -max_total_time=30       # fixed-duration run
./bin/drain_read_buffer_fuzz -max_total_time=30

# open-ended run (Ctrl-C to stop)
./bin/decode_heartbeat_fuzz
./bin/drain_read_buffer_fuzz
```

---

## 10. Static analysis

```bash
clang-tidy src/*.cpp -- -std=c++20 -Isrc
# (cppcheck was not installed on the reference machine and was skipped, not faked)
```

---

## 11. Real kernel-level network fault injection (needs root, CAP_NET_ADMIN)

Uses real `tc`/`netem` packet loss/delay/reordering on loopback against the actual shipped binaries. Deliberately not part of `make test` or any CI — mutating a network interface's queueing discipline shouldn't happen silently as a side effect of a normal test run. Tears the qdisc down unconditionally on exit/Ctrl-C.

```bash
sudo ./scripts/network_fault_demo.sh
# 5 phases: baseline -> 40% loss -> 100% loss -> recovery -> 300ms delay + 25% reorder
```

---

## 12. Full pipeline, back to back (manual form of the whole verification chain)

```bash
make clean && make all && make test           # plain build + full suite (105/105)
make debug                                     # ASan + UBSan
make clean && make CXXFLAGS="-std=c++20 -Wall -Wextra -Wpedantic -O1 -g -fsanitize=thread" test   # TSan
make clean && make CXX=clang++ test            # second compiler, 0 warnings
make bench                                     # build latency_bench + throughput_bench
bash scripts/run_benchmarks_and_plot.sh        # run + save + chart all 4 benchmarks
make demo                                      # 3-feed live isolation proof
make coverage                                  # gcov per-file line %
make fuzz                                      # libFuzzer, manual duration
clang-tidy src/*.cpp -- -std=c++20 -Isrc       # static analysis
sudo ./scripts/network_fault_demo.sh           # real tc/netem packet loss
```

---

## 13. Cleanup

```bash
make clean                # removes build/ and bin/
rm -f *.gcov               # stray gcov output in project root, if `make coverage` was interrupted
```
