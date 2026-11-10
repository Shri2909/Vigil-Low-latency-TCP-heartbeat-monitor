// libFuzzer harness for decode_heartbeat. Inputs are padded/truncated to one
// 28-byte frame; arbitrary-length streams are drain_read_buffer_fuzz's job.
// Seeds in fuzz/corpus/decode_heartbeat_fuzz/ (fuzz/gen_corpus.py) and the
// custom mutator (fuzz_common.h) keep inputs past the CRC check.
//
// Invariants (abort on violation):
//   1. a successful decode re-encodes to exactly the input bytes;
//   2. a message with a known type survives encode -> decode unchanged, and
//      one with an unknown type is rejected.
// *out is unspecified on failure, so it is not asserted on.
//
// Run: make fuzz-smoke, or
//   ./bin/decode_heartbeat_fuzz -max_total_time=60 -dict=fuzz/heartbeat.dict \
//       fuzz-work/decode fuzz/corpus/decode_heartbeat_fuzz

#include "fuzz_common.h"
#include "heartbeat.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <random>

extern "C" size_t LLVMFuzzerCustomMutator(uint8_t* data, size_t size, size_t max_size,
                                           unsigned int seed) {
    if (max_size < kHeartbeatMessageSize) {
        return size;
    }
    if (size < kHeartbeatMessageSize) {  // pad to one full frame
        std::memset(data + size, 0, kHeartbeatMessageSize - size);
    }
    size = kHeartbeatMessageSize;
    size = LLVMFuzzerMutate(data, size, kHeartbeatMessageSize);
    if (size < kHeartbeatMessageSize) {
        std::memset(data + size, 0, kHeartbeatMessageSize - size);
        size = kHeartbeatMessageSize;
    }
    std::mt19937 rng(seed);
    fuzz::fix_frames(data, size, 0, rng);
    return size;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    HeartbeatWireBuffer wire{};
    const size_t n = std::min(size, wire.size());
    std::memcpy(wire.data(), data, n);
    // Bytes past `size` (when fewer than 28) stay zero.

    HeartbeatMessage msg{};
    if (decode_heartbeat(wire, &msg)) {
        HeartbeatWireBuffer again{};
        encode_heartbeat(msg, again);
        FUZZ_ASSERT(again == wire);
        FUZZ_ASSERT(msg.magic == kProtocolMagic);
        FUZZ_ASSERT(msg.version == kProtocolVersion);
        FUZZ_ASSERT(msg.type >= 1 && msg.type <= 5);
    }

    // Encode-side round trip: reinterpret the bytes as field values, force a
    // valid magic/version, and expect decode to accept iff the type is known.
    HeartbeatMessage built{};
    std::memcpy(&built, wire.data(), sizeof(built));
    built.magic = kProtocolMagic;
    built.version = kProtocolVersion;
    HeartbeatWireBuffer built_wire{};
    encode_heartbeat(built, built_wire);
    HeartbeatMessage decoded{};
    const bool ok = decode_heartbeat(built_wire, &decoded);
    const bool known_type = built.type >= 1 && built.type <= 5;
    FUZZ_ASSERT(ok == known_type);
    if (ok) {
        FUZZ_ASSERT(decoded.type == built.type);
        FUZZ_ASSERT(decoded.feed_id == built.feed_id);
        FUZZ_ASSERT(decoded.sequence == built.sequence);
        FUZZ_ASSERT(decoded.timestamp_ns == built.timestamp_ns);
    }
    return 0;
}
