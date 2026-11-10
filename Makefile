# Build, test, benchmark and fuzz driver. `make` builds both binaries.
#
# Knobs (override on the command line, e.g. `make OPT=-O3 bench`):
#   OPT       optimisation level                              (default -O2)
#   SANITIZE  comma list for -fsanitize=, used at compile and link time
#             (e.g. thread, address,undefined)
#   WERROR=1  warnings as errors (see `werror`)
#   HARDEN=1  -D_GLIBCXX_ASSERTIONS (bounds-checked containers; off by default
#             so benchmarks measure the unchecked build)
#   DEFS      extra preprocessor definitions                  (e.g. -DDEBUG)
#   CXXFLAGS  extra flags appended last; the core flags are not part of it, so
#             overriding it can't drop them
# Changing flags does not rebuild existing objects: `make clean` first.

CXX       ?= g++
CXXSTD    := -std=c++20
WARNFLAGS := -Wall -Wextra -Wpedantic
DEPFLAGS  := -MMD -MP

OPT      ?= -O2
SANITIZE ?=
WERROR   ?=
HARDEN   ?=
DEFS     ?=
CXXFLAGS ?=
LDFLAGS  ?=

ifneq ($(SANITIZE),)
SAN_FLAGS := -fsanitize=$(SANITIZE)
endif
ifneq ($(WERROR),)
WARNFLAGS += -Werror
endif
ifneq ($(HARDEN),)
HARDEN_FLAGS := -D_GLIBCXX_ASSERTIONS
endif

# Same flags for compiling and linking. -pthread: the tests use std::thread.
ALL_FLAGS  := $(CXXSTD) $(WARNFLAGS) $(OPT) $(HARDEN_FLAGS) $(DEFS) $(DEPFLAGS) $(SAN_FLAGS) $(CXXFLAGS)
LINK_FLAGS := $(LDFLAGS) -pthread

SRC_DIR   := src
TEST_DIR  := test
BENCH_DIR := bench
DEMO_DIR  := demo
FUZZ_DIR  := fuzz
BUILD_DIR := build
BIN_DIR   := bin

INCLUDES := -I$(SRC_DIR)

# --- sources ---
# Library = everything in src/ except the two entry points.
ALL_SRC_CPP  := $(wildcard $(SRC_DIR)/*.cpp)
ENTRY_POINTS := $(SRC_DIR)/main.cpp $(SRC_DIR)/main_simulator.cpp
LIB_SRCS     := $(filter-out $(ENTRY_POINTS),$(ALL_SRC_CPP))
LIB_OBJS     := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/src/%.o,$(LIB_SRCS))

MONITOR_OBJ   := $(BUILD_DIR)/src/main.o
SIMULATOR_OBJ := $(BUILD_DIR)/src/main_simulator.o

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS := $(patsubst $(TEST_DIR)/%.cpp,$(BUILD_DIR)/test/%.o,$(TEST_SRCS))

# Each bench/*.cpp is its own binary, except bench_common.cpp (linked into all).
BENCH_COMMON_SRC := $(BENCH_DIR)/bench_common.cpp
BENCH_COMMON_OBJ := $(BUILD_DIR)/bench/bench_common.o
BENCH_SRCS := $(filter-out $(BENCH_COMMON_SRC),$(wildcard $(BENCH_DIR)/*.cpp))
BENCH_OBJS := $(patsubst $(BENCH_DIR)/%.cpp,$(BUILD_DIR)/bench/%.o,$(BENCH_SRCS))
BENCH_BINS := $(patsubst $(BENCH_DIR)/%.cpp,$(BIN_DIR)/%,$(BENCH_SRCS))

DEMO_OBJ := $(BUILD_DIR)/demo/lifecycle_demo.o

.PHONY: all test bench demo fuzz fuzz-smoke debug werror coverage clean FORCE
# Keep bench objects: make would otherwise delete them as pattern-rule
# intermediates after linking, defeating incremental rebuilds.
.SECONDARY: $(BENCH_OBJS)

all: $(BIN_DIR)/feed_monitor $(BIN_DIR)/feed_simulator

# --- binaries ---
$(BIN_DIR)/feed_monitor: $(LIB_OBJS) $(MONITOR_OBJ) | $(BIN_DIR)
	$(CXX) $(ALL_FLAGS) $^ -o $@ $(LINK_FLAGS)

$(BIN_DIR)/feed_simulator: $(LIB_OBJS) $(SIMULATOR_OBJ) | $(BIN_DIR)
	$(CXX) $(ALL_FLAGS) $^ -o $@ $(LINK_FLAGS)

# The e2e test exec()s the real binaries, so they are order-only prerequisites:
# built first, but rebuilding them doesn't relink run_tests.
$(BIN_DIR)/run_tests: $(LIB_OBJS) $(TEST_OBJS) | $(BIN_DIR) $(BIN_DIR)/feed_monitor $(BIN_DIR)/feed_simulator
	$(CXX) $(ALL_FLAGS) $(LIB_OBJS) $(TEST_OBJS) -o $@ $(LINK_FLAGS)

$(BIN_DIR)/lifecycle_demo: $(DEMO_OBJ) $(LIB_OBJS) | $(BIN_DIR)
	$(CXX) $(ALL_FLAGS) $^ -o $@ $(LINK_FLAGS)

# Bench binaries (the explicit rules above take precedence over this pattern).
$(BIN_DIR)/%: $(BUILD_DIR)/bench/%.o $(BENCH_COMMON_OBJ) $(LIB_OBJS) | $(BIN_DIR)
	$(CXX) $(ALL_FLAGS) $^ -o $@ $(LINK_FLAGS)

# --- objects ---
# Object directories are order-only prerequisites so `make -j` never compiles
# before mkdir has run.
$(BUILD_DIR)/src/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)/src
	$(CXX) $(ALL_FLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/test/%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)/test
	$(CXX) $(ALL_FLAGS) $(INCLUDES) -I$(TEST_DIR) -c $< -o $@

$(BUILD_DIR)/bench/%.o: $(BENCH_DIR)/%.cpp | $(BUILD_DIR)/bench
	$(CXX) $(ALL_FLAGS) $(INCLUDES) -I$(BENCH_DIR) -c $< -o $@

$(BUILD_DIR)/demo/%.o: $(DEMO_DIR)/%.cpp | $(BUILD_DIR)/demo
	$(CXX) $(ALL_FLAGS) $(INCLUDES) -c $< -o $@

# The e2e test gets absolute bin/source paths baked in, so it works from any cwd.
# The stamp file changes with those paths, forcing a recompile only when needed.
$(BUILD_DIR)/test/.bin_dir.stamp: FORCE | $(BUILD_DIR)/test
	@echo '$(abspath $(BIN_DIR)) $(abspath .)' | cmp -s - $@ || echo '$(abspath $(BIN_DIR)) $(abspath .)' > $@

$(BUILD_DIR)/test/e2e_binary_test.o: $(TEST_DIR)/e2e_binary_test.cpp $(BUILD_DIR)/test/.bin_dir.stamp | $(BUILD_DIR)/test
	$(CXX) $(ALL_FLAGS) $(INCLUDES) -I$(TEST_DIR) -DPROJECT_BIN_DIR="\"$(abspath $(BIN_DIR))\"" -DPROJECT_SOURCE_DIR="\"$(abspath .)\"" -c $< -o $@

$(BUILD_DIR)/src $(BUILD_DIR)/test $(BUILD_DIR)/bench $(BUILD_DIR)/demo $(BIN_DIR):
	mkdir -p $@

# --- fuzzing (libFuzzer needs clang++) ---
# Library sources are compiled straight into each harness rather than reusing
# the g++-built LIB_OBJS, to avoid mixing compilers and sanitizer runtimes.
# UBSan findings abort so libFuzzer saves them as crash artifacts.
FUZZ_CXX    ?= clang++
FUZZ_FLAGS  := $(CXXSTD) -Wall -Wextra -g -O1 -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=undefined
FUZZ_BINS   := $(BIN_DIR)/decode_heartbeat_fuzz $(BIN_DIR)/drain_read_buffer_fuzz
ALL_HEADERS := $(wildcard $(SRC_DIR)/*.h) $(wildcard $(FUZZ_DIR)/*.h)

$(FUZZ_BINS): $(BIN_DIR)/%: $(FUZZ_DIR)/%.cpp $(LIB_SRCS) $(ALL_HEADERS) | $(BIN_DIR)
	$(FUZZ_CXX) $(FUZZ_FLAGS) $(INCLUDES) $< $(LIB_SRCS) -o $@ -pthread

# --- phony targets ---
test: $(BIN_DIR)/run_tests
	./$(BIN_DIR)/run_tests

bench: $(BENCH_BINS)

demo: $(BIN_DIR)/lifecycle_demo

fuzz: $(FUZZ_BINS)

# Time-boxed run of each harness on the committed corpus (usage: make fuzz-smoke
# FUZZ_TIME=60). New inputs go to $(FUZZ_WORK), never the committed corpus;
# crashes land in $(FUZZ_ARTIFACTS). `timeout` is a hard backstop.
FUZZ_TIME      ?= 20
FUZZ_WORK      := fuzz-work
FUZZ_ARTIFACTS := fuzz-artifacts
fuzz-smoke: fuzz
	@mkdir -p $(FUZZ_ARTIFACTS)
	@for h in $(notdir $(FUZZ_BINS)); do \
		mkdir -p $(FUZZ_WORK)/$$h; \
		echo "=== $$h ($(FUZZ_TIME)s) ==="; \
		timeout $$(( $(FUZZ_TIME) + 60 )) ./$(BIN_DIR)/$$h -max_total_time=$(FUZZ_TIME) -timeout=10 \
			-close_fd_mask=3 -dict=$(FUZZ_DIR)/heartbeat.dict -artifact_prefix=$(FUZZ_ARTIFACTS)/$$h- \
			$(FUZZ_WORK)/$$h $(FUZZ_DIR)/corpus/$$h || exit 1; \
	done

# ASan + UBSan at -O0 after a clean (sanitized objects must not mix with a normal build).
debug:
	$(MAKE) clean
	$(MAKE) OPT="-g -O0" DEFS="-DDEBUG" SANITIZE=address,undefined HARDEN=1 all test

# -Werror build + tests (with HARDEN) in separate dirs, leaving the normal build
# alone. `make CXX=clang++ werror` does the same with clang.
werror:
	$(MAKE) BUILD_DIR=build-werror BIN_DIR=bin-werror WERROR=1 HARDEN=1 all test

# Clean --coverage rebuild, run tests, print per-file gcov line coverage. The
# entry points are covered only through the e2e subprocess tests.
coverage:
	$(MAKE) clean
	$(MAKE) OPT="-g -O0" CXXFLAGS="--coverage" all test
	@echo ""
	@echo "=== per-file line coverage (gcov) ==="
	@for f in $(ALL_SRC_CPP); do \
		gcov -o $(BUILD_DIR)/src $$f 2>/dev/null | grep -A1 "File '$$f'" ; \
	done
	@rm -f *.gcov

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR) build-werror bin-werror fuzz-work fuzz-artifacts

-include $(LIB_OBJS:.o=.d) $(TEST_OBJS:.o=.d) $(MONITOR_OBJ:.o=.d) $(SIMULATOR_OBJ:.o=.d) $(BENCH_COMMON_OBJ:.o=.d) $(BENCH_OBJS:.o=.d) $(DEMO_OBJ:.o=.d)
