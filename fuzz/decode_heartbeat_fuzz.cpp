// libFuzzer harness for decode_heartbeat -- the wire-format parser's own
// unit tests (heartbeat_test.cpp) cover ~5 specific, hand-picked cases
// (wrong magic, wrong version, one corrupted bit, unknown type). This
// exists to find whatever those specific cases don't anticipate.
//
// decode_heartbeat takes a fixed-size HeartbeatWireBuffer (std::array<28>),
// not a (ptr, len) pair -- there's no "wrong size" dimension to fuzz at
// this layer, the size decision already happened in the real caller
// (Connection::drain_read_buffer, which only invokes this once >=28 bytes
// are buffered). So every fuzzer input is padded/truncated to exactly 28
// bytes here; fuzz/drain_read_buffer_fuzz.cpp is the companion harness that
// fuzzes arbitrary-length byte streams at the layer where that dimension
// actually exists.
//
// Build/run manually (not part of make all/test/CI):
//   make fuzz
//   ./bin/decode_heartbeat_fuzz -max_total_time=60

#include "heartbeat.h"

#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <cstring>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    HeartbeatWireBuffer wire{};
    const size_t n = std::min(size, wire.size());
    std::memcpy(wire.data(), data, n);
    // Any bytes past `size` (if the fuzzer gave us fewer than 28) stay
    // zero-initialized -- a legitimate, decodable-or-not input either way.

    HeartbeatMessage msg{};
    decode_heartbeat(wire, &msg);
    return 0;
}
