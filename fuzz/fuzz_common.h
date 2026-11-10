#pragma once

// Shared helpers for the libFuzzer harnesses.
//
// Stock mutation leaves the trailing CRC32 stale, so mutated frames die at the
// CRC check. fix_frames() restores magic/version/type/CRC after each mutation,
// but leaves ~1/8 of each field wrong so the rejection paths stay covered.

#include "heartbeat.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>

extern "C" size_t LLVMFuzzerMutate(uint8_t* data, size_t size, size_t max_size);

namespace fuzz {

// Fixes up every complete frame in data[offset, size), stepping one frame at a time.
inline void fix_frames(uint8_t* data, size_t size, size_t offset, std::mt19937& rng) {
    constexpr size_t kCrcOffset = kHeartbeatMessageSize - sizeof(uint32_t);
    for (; offset + kHeartbeatMessageSize <= size; offset += kHeartbeatMessageSize) {
        uint8_t* frame = data + offset;
        const uint32_t r = rng();
        if ((r & 7u) != 0) {  // 7/8: valid magic + version
            frame[0] = static_cast<uint8_t>(kProtocolMagic >> 8);
            frame[1] = static_cast<uint8_t>(kProtocolMagic & 0xFF);
            frame[2] = kProtocolVersion;
        }
        if (((r >> 3) & 7u) != 0 && (frame[3] < 1 || frame[3] > 5)) {  // 7/8: known type
            frame[3] = static_cast<uint8_t>(1 + frame[3] % 5);
        }
        if (((r >> 6) & 7u) != 0) {  // 7/8: valid CRC
            const uint32_t crc = crc32_ieee(frame, kCrcOffset);
            frame[kCrcOffset + 0] = static_cast<uint8_t>(crc >> 24);
            frame[kCrcOffset + 1] = static_cast<uint8_t>(crc >> 16);
            frame[kCrcOffset + 2] = static_cast<uint8_t>(crc >> 8);
            frame[kCrcOffset + 3] = static_cast<uint8_t>(crc);
        }
    }
}

// Harness invariant check; aborts so libFuzzer saves a crash artifact. Unlike
// assert(), it survives NDEBUG.
#define FUZZ_ASSERT(cond)                                                          \
    do {                                                                           \
        if (!(cond)) {                                                             \
            std::fprintf(stderr, "FUZZ_ASSERT failed: %s (%s:%d)\n", #cond,        \
                         __FILE__, __LINE__);                                      \
            std::abort();                                                          \
        }                                                                          \
    } while (0)

}  // namespace fuzz
