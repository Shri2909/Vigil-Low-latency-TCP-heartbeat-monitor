# Low-Latency TCP Heartbeat Monitor -- build, test, and benchmark driver.
# See PROJECT_PLAN.md section 13 for the design rationale.

CXX      ?= g++
CXXSTD   := -std=c++20
WARN     := -Wall -Wextra -Wpedantic
OPT      := -O2
DEBUGOPT := -g -O0 -fsanitize=address,undefined -DDEBUG
DEPFLAGS := -MMD -MP

CXXFLAGS ?= $(CXXSTD) $(WARN) $(OPT) $(DEPFLAGS)
# -pthread is linked only because glibc's eventfd/timerfd-adjacent bits and
# <atomic> expect it on some toolchains -- the design itself is single-threaded
# (see PROJECT_PLAN.md finding #17), it does not spawn worker threads.
LDFLAGS  ?= -pthread

SRC_DIR   := src
TEST_DIR  := test
BENCH_DIR := bench
DEMO_DIR  := demo
FUZZ_DIR  := fuzz
BUILD_DIR := build
BIN_DIR   := bin

INCLUDES := -I$(SRC_DIR)

# --- library sources: everything in src/ except the two binary entry points ---
ALL_SRC_CPP := $(wildcard $(SRC_DIR)/*.cpp)
ENTRY_POINTS := $(SRC_DIR)/main.cpp $(SRC_DIR)/main_simulator.cpp
LIB_SRCS  := $(filter-out $(ENTRY_POINTS),$(ALL_SRC_CPP))
LIB_OBJS  := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/src/%.o,$(LIB_SRCS))

MONITOR_OBJ   := $(BUILD_DIR)/src/main.o
SIMULATOR_OBJ := $(BUILD_DIR)/src/main_simulator.o

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS := $(patsubst $(TEST_DIR)/%.cpp,$(BUILD_DIR)/test/%.o,$(TEST_SRCS))

# bench_common.cpp is a shared helper linked into every bench binary, not a
# binary of its own -- kept out of BENCH_BINS the same way LIB_SRCS excludes
# the two main*.cpp entry points.
BENCH_COMMON_SRC := $(BENCH_DIR)/bench_common.cpp
BENCH_COMMON_OBJ := $(BUILD_DIR)/bench/bench_common.o
BENCH_SRCS := $(filter-out $(BENCH_COMMON_SRC),$(wildcard $(BENCH_DIR)/*.cpp))
BENCH_OBJS := $(patsubst $(BENCH_DIR)/%.cpp,$(BUILD_DIR)/bench/%.o,$(BENCH_SRCS))
BENCH_BINS := $(patsubst $(BENCH_DIR)/%.cpp,$(BIN_DIR)/%,$(BENCH_SRCS))

.PHONY: all test bench demo fuzz debug clean print-vars
# Without this, GNU Make treats object files reachable only through a
# pattern-rule chain ($(BENCH_DIR)/%.cpp -> $(BUILD_DIR)/bench/%.o ->
# $(BIN_DIR)/%) as disposable intermediates and deletes them right after
# linking -- correct output, but it silently defeats incremental rebuilds for
# exactly those files (every `make bench` would recompile from scratch even
# with no source changes). LIB_OBJS/TEST_OBJS don't need this: they're also
# listed as explicit prerequisites of non-pattern targets elsewhere
# (bin/feed_monitor, bin/run_tests, ...), which already exempts them.
.SECONDARY: $(BENCH_OBJS)

all: $(BIN_DIR)/feed_monitor $(BIN_DIR)/feed_simulator

# --- binaries ---
$(BIN_DIR)/feed_monitor: $(LIB_OBJS) $(MONITOR_OBJ) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BIN_DIR)/feed_simulator: $(LIB_OBJS) $(SIMULATOR_OBJ) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BIN_DIR)/run_tests: $(LIB_OBJS) $(TEST_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ -o $@ $(LDFLAGS)

# each other bench/*.cpp is its own standalone binary, linked against the
# library objects plus the shared bench_common.o -- via an intermediate
# object file, same as every other binary here, so its .d dependency file
# lands in build/bench/ instead of stray in bin/.
$(BIN_DIR)/%: $(BUILD_DIR)/bench/%.o $(BENCH_COMMON_OBJ) $(LIB_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

# --- object files ---
$(BUILD_DIR)/src/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)/src
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/test/%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)/test
	$(CXX) $(CXXFLAGS) $(INCLUDES) -I$(TEST_DIR) -c $< -o $@

# e2e_binary_test.cpp fork()+exec()s the real bin/feed_monitor and
# bin/feed_simulator, so it needs their absolute path baked in at compile
# time rather than guessed relative to whatever cwd run_tests happens to be
# invoked from (an IDE run config, a sanitizer build run from a
# subdirectory, etc. all break a cwd-relative guess). An explicit rule for
# this one file overrides the generic pattern rule above -- same reasoning
# as lifecycle_demo's dedicated rule elsewhere in this file.
$(BUILD_DIR)/test/e2e_binary_test.o: $(TEST_DIR)/e2e_binary_test.cpp | $(BUILD_DIR)/test
	$(CXX) $(CXXFLAGS) $(INCLUDES) -I$(TEST_DIR) -DPROJECT_BIN_DIR="\"$(abspath $(BIN_DIR))\"" -c $< -o $@

$(BUILD_DIR)/bench/%.o: $(BENCH_DIR)/%.cpp | $(BUILD_DIR)/bench
	$(CXX) $(CXXFLAGS) $(INCLUDES) -I$(BENCH_DIR) -c $< -o $@

# lifecycle_demo is a single, explicitly-named binary (not a wildcard
# pattern like the bench binaries) specifically so it can't collide with the
# existing $(BIN_DIR)/% pattern rule above -- two pattern rules matching the
# same target with different prerequisite sets is exactly the kind of
# ambiguity GNU Make resolves unpredictably.
$(BIN_DIR)/lifecycle_demo: $(BUILD_DIR)/demo/lifecycle_demo.o $(LIB_OBJS) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/demo/%.o: $(DEMO_DIR)/%.cpp | $(BUILD_DIR)/demo
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# --- fuzzing harnesses (libFuzzer, clang++ only -- g++ has no
# -fsanitize=fuzzer). Explicit rules, not a wildcard pattern, for the same
# reason as lifecycle_demo: avoids colliding with the bench binaries'
# $(BIN_DIR)/% pattern rule. Compiles the library sources directly into each
# harness rather than reusing g++-built LIB_OBJS, since mixing object files
# from two different compilers/instrumentation into one link is asking for
# ABI or sanitizer-runtime trouble -- acceptable here because `make fuzz` is
# a rare, manual, one-off run, not part of the regular incremental build.
$(BIN_DIR)/decode_heartbeat_fuzz: $(FUZZ_DIR)/decode_heartbeat_fuzz.cpp $(LIB_SRCS) | $(BIN_DIR)
	clang++ -std=c++20 -Wall -Wextra -g -O1 -fsanitize=fuzzer,address $(INCLUDES) $(FUZZ_DIR)/decode_heartbeat_fuzz.cpp $(LIB_SRCS) -o $@ -pthread

$(BIN_DIR)/drain_read_buffer_fuzz: $(FUZZ_DIR)/drain_read_buffer_fuzz.cpp $(LIB_SRCS) | $(BIN_DIR)
	clang++ -std=c++20 -Wall -Wextra -g -O1 -fsanitize=fuzzer,address $(INCLUDES) $(FUZZ_DIR)/drain_read_buffer_fuzz.cpp $(LIB_SRCS) -o $@ -pthread

# --- directories ---
$(BUILD_DIR)/src $(BUILD_DIR)/test $(BUILD_DIR)/bench $(BUILD_DIR)/demo $(BIN_DIR):
	mkdir -p $@

# --- phony targets ---
# feed_monitor/feed_simulator are prerequisites here (not just of run_tests)
# because e2e_binary_test.cpp fork()+exec()s the real compiled binaries --
# they need to exist on disk before run_tests executes, even though
# run_tests itself doesn't link against them.
test: $(BIN_DIR)/run_tests $(BIN_DIR)/feed_monitor $(BIN_DIR)/feed_simulator
	./$(BIN_DIR)/run_tests

bench: $(BENCH_BINS)

demo: $(BIN_DIR)/lifecycle_demo

fuzz: $(BIN_DIR)/decode_heartbeat_fuzz $(BIN_DIR)/drain_read_buffer_fuzz

debug:
	$(MAKE) clean
	$(MAKE) CXXFLAGS="$(CXXSTD) $(WARN) $(DEBUGOPT) $(DEPFLAGS)" LDFLAGS="$(LDFLAGS) -fsanitize=address,undefined" all test

# Measured line coverage, not just a test count. lcov/genhtml aren't
# installed on this machine (confirmed) but gcov is -- no HTML report, just
# gcov's own per-file summary. Rebuilds everything with --coverage so
# main.cpp/main_simulator.cpp (exercised as real subprocesses by
# e2e_binary_test.cpp, not linked into run_tests itself) get real coverage
# data too, not just the library sources run_tests links directly.
coverage:
	$(MAKE) clean
	$(MAKE) CXXFLAGS="$(CXXSTD) $(WARN) -g -O0 --coverage $(DEPFLAGS)" LDFLAGS="--coverage -pthread" all test
	@echo ""
	@echo "=== per-file line coverage (gcov) ==="
	@for f in $(ALL_SRC_CPP); do \
		gcov -o $(BUILD_DIR)/src $$f 2>/dev/null | grep -A1 "File '$$f'" ; \
	done
	@rm -f *.gcov

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)

print-vars:
	@echo "LIB_SRCS   = $(LIB_SRCS)"
	@echo "TEST_SRCS  = $(TEST_SRCS)"
	@echo "BENCH_SRCS = $(BENCH_SRCS)"

-include $(LIB_OBJS:.o=.d) $(TEST_OBJS:.o=.d) $(MONITOR_OBJ:.o=.d) $(SIMULATOR_OBJ:.o=.d) $(BENCH_COMMON_OBJ:.o=.d) $(BENCH_OBJS:.o=.d) $(BUILD_DIR)/demo/lifecycle_demo.d
