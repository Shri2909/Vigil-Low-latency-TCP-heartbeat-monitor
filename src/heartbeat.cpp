#include "heartbeat.h"

#include <cstring>
#include <endian.h>  // htobe16/32/64, be16/32/64toh -- glibc, Linux-only (matches project scope)

namespace {

// CRC32 (IEEE 802.3, polynomial 0xEDB88320) lookup table, generated at compile
// time via constexpr rather than lazily built at runtime -- applying the
// "compile-time computation" technique from PROJECT_PLAN.md section 15
// (Bilokon & Gunduz section 3.1): the table exists before main() runs, at zero
// runtime cost and with no static-initialization-order concern.
constexpr std::array<uint32_t, 256> make_crc32_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int bit = 0; bit < 8; ++bit) {
            c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        table[i] = c;
    }
    return table;
}

constexpr std::array<uint32_t, 256> kCrc32Table = make_crc32_table();

bool is_known_message_type(uint8_t type) {
    switch (static_cast<MessageType>(type)) {
        case MessageType::kConnectHello:
        case MessageType::kConnectAck:
        case MessageType::kPing:
        case MessageType::kPong:
        case MessageType::kDisconnect:
            return true;
    }
    return false;
}

}  // namespace

uint32_t crc32_ieee(const uint8_t* data, size_t len) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = kCrc32Table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

void encode_heartbeat(const HeartbeatMessage& msg, HeartbeatWireBuffer& out) {
    size_t offset = 0;

    const uint16_t magic_be = htobe16(msg.magic);
    std::memcpy(out.data() + offset, &magic_be, sizeof(magic_be));
    offset += sizeof(magic_be);

    out[offset++] = msg.version;
    out[offset++] = msg.type;

    const uint32_t feed_id_be = htobe32(msg.feed_id);
    std::memcpy(out.data() + offset, &feed_id_be, sizeof(feed_id_be));
    offset += sizeof(feed_id_be);

    const uint64_t sequence_be = htobe64(msg.sequence);
    std::memcpy(out.data() + offset, &sequence_be, sizeof(sequence_be));
    offset += sizeof(sequence_be);

    // timestamp_ns is signed; reinterpret its bits as unsigned for the byte-order
    // swap, then write those same bits back out. The value itself is never
    // arithmetically touched, so this is lossless regardless of sign.
    uint64_t ts_bits;
    std::memcpy(&ts_bits, &msg.timestamp_ns, sizeof(ts_bits));
    const uint64_t ts_be = htobe64(ts_bits);
    std::memcpy(out.data() + offset, &ts_be, sizeof(ts_be));
    offset += sizeof(ts_be);

    const uint32_t crc = crc32_ieee(out.data(), offset);
    const uint32_t crc_be = htobe32(crc);
    std::memcpy(out.data() + offset, &crc_be, sizeof(crc_be));
}

bool decode_heartbeat(const HeartbeatWireBuffer& in, HeartbeatMessage* out) {
    uint16_t magic_be;
    std::memcpy(&magic_be, in.data(), sizeof(magic_be));
    const uint16_t magic = be16toh(magic_be);
    if (magic != kProtocolMagic) [[unlikely]] {
        return false;
    }

    const uint8_t version = in[2];
    if (version != kProtocolVersion) [[unlikely]] {
        return false;
    }

    const uint8_t type = in[3];
    if (!is_known_message_type(type)) [[unlikely]] {
        return false;
    }

    constexpr size_t kCrcOffset = kHeartbeatMessageSize - sizeof(uint32_t);
    uint32_t crc_be;
    std::memcpy(&crc_be, in.data() + kCrcOffset, sizeof(crc_be));
    const uint32_t expected_crc = be32toh(crc_be);
    const uint32_t actual_crc = crc32_ieee(in.data(), kCrcOffset);
    if (actual_crc != expected_crc) [[unlikely]] {
        return false;
    }

    uint32_t feed_id_be;
    std::memcpy(&feed_id_be, in.data() + 4, sizeof(feed_id_be));

    uint64_t sequence_be;
    std::memcpy(&sequence_be, in.data() + 8, sizeof(sequence_be));

    uint64_t ts_be;
    std::memcpy(&ts_be, in.data() + 16, sizeof(ts_be));
    const uint64_t ts_bits = be64toh(ts_be);

    out->magic = magic;
    out->version = version;
    out->type = type;
    out->feed_id = be32toh(feed_id_be);
    out->sequence = be64toh(sequence_be);
    std::memcpy(&out->timestamp_ns, &ts_bits, sizeof(out->timestamp_ns));
    out->crc32 = expected_crc;
    return true;
}
