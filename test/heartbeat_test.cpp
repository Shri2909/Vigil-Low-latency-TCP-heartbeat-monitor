#include "heartbeat.h"
#include "mini_test.h"

namespace {

HeartbeatMessage make_message() {
    HeartbeatMessage msg;
    msg.type = static_cast<uint8_t>(MessageType::kPing);
    msg.feed_id = 0x11223344;
    msg.sequence = 0x0102030405060708ULL;
    msg.timestamp_ns = 1234567890123LL;
    return msg;
}

}  // namespace

TEST_CASE("encode/decode round-trip preserves all fields") {
    const HeartbeatMessage original = make_message();
    HeartbeatWireBuffer wire{};
    encode_heartbeat(original, wire);

    HeartbeatMessage decoded{};
    REQUIRE(decode_heartbeat(wire, &decoded));
    CHECK(decoded.magic == original.magic);
    CHECK(decoded.version == original.version);
    CHECK(decoded.type == original.type);
    CHECK(decoded.feed_id == original.feed_id);
    CHECK(decoded.sequence == original.sequence);
    CHECK(decoded.timestamp_ns == original.timestamp_ns);
}

TEST_CASE("round-trip preserves a negative timestamp") {
    // timestamp_ns is signed; CLOCK_MONOTONIC never produces negative values in
    // practice, but the encode path bit-reinterprets rather than arithmetically
    // converts, so this must survive unchanged regardless of sign.
    HeartbeatMessage original = make_message();
    original.timestamp_ns = -42;
    HeartbeatWireBuffer wire{};
    encode_heartbeat(original, wire);

    HeartbeatMessage decoded{};
    REQUIRE(decode_heartbeat(wire, &decoded));
    CHECK(decoded.timestamp_ns == -42);
}

TEST_CASE("encode writes big-endian (network byte order) fields") {
    HeartbeatMessage msg = make_message();
    msg.feed_id = 0x11223344;
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);

    // magic: bytes [0,1]
    CHECK(wire[0] == 0xFE);
    CHECK(wire[1] == 0xED);
    // version, type: bytes [2,3]
    CHECK(wire[2] == kProtocolVersion);
    CHECK(wire[3] == static_cast<uint8_t>(MessageType::kPing));
    // feed_id: bytes [4..7], big-endian
    CHECK(wire[4] == 0x11);
    CHECK(wire[5] == 0x22);
    CHECK(wire[6] == 0x33);
    CHECK(wire[7] == 0x44);
}

TEST_CASE("decode rejects wrong magic") {
    HeartbeatMessage msg = make_message();
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);
    wire[0] ^= 0xFF;  // corrupt magic's high byte

    HeartbeatMessage decoded{};
    CHECK(!decode_heartbeat(wire, &decoded));
}

TEST_CASE("decode rejects wrong version") {
    HeartbeatMessage msg = make_message();
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);
    wire[2] = kProtocolVersion + 1;
    // crc was computed over the original bytes, so bumping the version alone
    // also fails the crc check -- this test exists to document that decode
    // rejects *before* trusting a mismatched version, not to isolate the two.

    HeartbeatMessage decoded{};
    CHECK(!decode_heartbeat(wire, &decoded));
}

TEST_CASE("decode rejects unknown message type") {
    HeartbeatMessage msg = make_message();
    msg.type = 0;  // 0 is not one of the five defined MessageType values
    HeartbeatWireBuffer wire{};
    // Encode manually re-running crc so the failure is isolated to the type
    // check, not a crc mismatch: encode_heartbeat always computes crc over
    // whatever bytes are present, so this exercises the real code path.
    encode_heartbeat(msg, wire);

    HeartbeatMessage decoded{};
    CHECK(!decode_heartbeat(wire, &decoded));
}

TEST_CASE("decode rejects a single corrupted bit anywhere in the payload") {
    HeartbeatMessage msg = make_message();
    HeartbeatWireBuffer wire{};
    encode_heartbeat(msg, wire);

    for (size_t byte_index = 0; byte_index < kHeartbeatMessageSize - sizeof(uint32_t); ++byte_index) {
        HeartbeatWireBuffer corrupted = wire;
        corrupted[byte_index] ^= 0x01;  // flip the low bit of one payload byte
        HeartbeatMessage decoded{};
        CHECK(!decode_heartbeat(corrupted, &decoded));
    }
}

TEST_CASE("HeartbeatMessage has no padding and is exactly the wire size") {
    // Guards against a future field reorder silently introducing alignment
    // padding, which would desync every reader of this protocol.
    REQUIRE(sizeof(HeartbeatMessage) == kHeartbeatMessageSize);
    REQUIRE(kHeartbeatMessageSize == 28);
}

TEST_CASE("crc32_ieee matches the standard test vector") {
    // "123456789" -> 0xCBF43926 is the canonical CRC-32/ISO-HDLC (IEEE 802.3)
    // check value, used to catch a wrong polynomial or reflection direction.
    const uint8_t data[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    CHECK(crc32_ieee(data, sizeof(data)) == 0xCBF43926u);
}
