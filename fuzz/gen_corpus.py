#!/usr/bin/env python3
"""Regenerates the committed libFuzzer seed corpora under fuzz/corpus/.

Frames are valid 28-byte heartbeats (CRC32/IEEE over the first 24 bytes), so
fuzzing starts past the CRC check. drain_read_buffer_fuzz seeds carry that
harness's 3-byte control header (layout in drain_read_buffer_fuzz.cpp).

Usage: python3 fuzz/gen_corpus.py   (writes fuzz/corpus/)
"""
import os
import struct
import zlib

HELLO, ACK, PING, PONG, DISCONNECT = 1, 2, 3, 4, 5
INITIATOR, RESPONDER = 0, 1
SKIP_HANDSHAKE, CLOSE_AT_END = 0x02, 0x04


def frame(msg_type, feed_id=1, seq=0, ts=0, magic=0xFEED, version=1, bad_crc=False):
    body = struct.pack(">HBBIQq", magic, version, msg_type, feed_id, seq, ts)
    crc = zlib.crc32(body) & 0xFFFFFFFF
    if bad_crc:
        crc ^= 1
    return body + struct.pack(">I", crc)


def header(role, flags=0, chunk=0, delta_ms=0):
    return bytes([role | flags | (chunk << 3), delta_ms & 0xFF, (delta_ms >> 8) & 0xFF])


def write(directory, name, data):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, name), "wb") as f:
        f.write(data)


def main():
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "corpus")

    # decode_heartbeat_fuzz: one frame per message type, field extremes, malformed frames.
    d = os.path.join(root, "decode_heartbeat_fuzz")
    for name, t in (("connect_hello", HELLO), ("connect_ack", ACK), ("ping", PING),
                    ("pong", PONG), ("disconnect", DISCONNECT)):
        write(d, name, frame(t, feed_id=1, seq=1, ts=123456789))
    write(d, "ping_extremes", frame(PING, feed_id=0xFFFFFFFF, seq=0xFFFFFFFFFFFFFFFF, ts=-1))
    write(d, "pong_min_ts", frame(PONG, seq=7, ts=-(2 ** 63)))
    write(d, "bad_crc", frame(PING, bad_crc=True))
    write(d, "unknown_type", frame(9))
    write(d, "bad_version", frame(PING, version=2))
    write(d, "bad_magic", frame(PING, magic=0xBEEF))

    # drain_read_buffer_fuzz
    d = os.path.join(root, "drain_read_buffer_fuzz")
    # Initiator (handshake done, harness has sent PINGs seq 0..3): valid, duplicate,
    # reordered, chunked and malformed PONGs, DISCONNECT, an unexpected PING.
    write(d, "init_pong", header(INITIATOR) + frame(PONG, seq=0, ts=0))
    write(d, "init_pong_dup", header(INITIATOR) + frame(PONG, seq=0, ts=0) * 2)
    write(d, "init_pong_reordered",
          header(INITIATOR) + frame(PONG, seq=2, ts=0) + frame(PONG, seq=1, ts=0) +
          frame(PONG, seq=0, ts=0))
    write(d, "init_pong_chunked_7", header(INITIATOR, chunk=7) + frame(PONG, seq=0, ts=0) +
          frame(PONG, seq=1, ts=0))
    write(d, "init_pong_never_sent", header(INITIATOR) + frame(PONG, seq=9, ts=0))  # harness sends seq 0..3
    write(d, "init_pong_wrong_feed", header(INITIATOR) + frame(PONG, feed_id=2, seq=0, ts=0))
    write(d, "init_pong_future_ts", header(INITIATOR) + frame(PONG, seq=0, ts=2 ** 40))
    write(d, "init_pong_late", header(INITIATOR, delta_ms=5000) + frame(PONG, seq=0, ts=0))
    write(d, "init_disconnect", header(INITIATOR) + frame(DISCONNECT))
    write(d, "init_unexpected_ping", header(INITIATOR) + frame(PING, seq=0, ts=0))
    write(d, "init_pong_then_eof", header(INITIATOR, flags=CLOSE_AT_END) + frame(PONG, seq=0, ts=0))
    write(d, "init_partial_frame", header(INITIATOR) + frame(PONG, seq=0, ts=0)[:27])
    write(d, "init_bad_crc", header(INITIATOR) + frame(PONG, bad_crc=True))
    # Initiator without the scripted handshake.
    write(d, "init_nohs_ack_pong", header(INITIATOR, flags=SKIP_HANDSHAKE) + frame(ACK) +
          frame(PONG, seq=0, ts=0))
    write(d, "init_nohs_wrong_feed", header(INITIATOR, flags=SKIP_HANDSHAKE) + frame(ACK, feed_id=2))
    # Responder: PINGs, DISCONNECT, unexpected PONG, handshake edge cases.
    write(d, "resp_ping", header(RESPONDER) + frame(PING, seq=0, ts=100))
    write(d, "resp_pings", header(RESPONDER) + b"".join(frame(PING, seq=i, ts=100 + i) for i in range(8)))
    write(d, "resp_pings_chunked_5", header(RESPONDER, chunk=5) +
          b"".join(frame(PING, seq=i, ts=100 + i) for i in range(3)))
    write(d, "resp_disconnect", header(RESPONDER) + frame(DISCONNECT))
    write(d, "resp_unexpected_pong", header(RESPONDER) + frame(PONG, seq=0, ts=0))
    write(d, "resp_duplicate_hello", header(RESPONDER) + frame(HELLO))
    write(d, "resp_ping_then_eof", header(RESPONDER, flags=CLOSE_AT_END) + frame(PING, seq=0, ts=100))
    write(d, "resp_nohs_hello_ping", header(RESPONDER, flags=SKIP_HANDSHAKE) + frame(HELLO) +
          frame(PING, seq=0, ts=100))
    write(d, "resp_nohs_wrong_feed", header(RESPONDER, flags=SKIP_HANDSHAKE) + frame(HELLO, feed_id=9))
    # Garbage, and a valid frame preceded by one stray byte.
    write(d, "garbage", header(INITIATOR) + bytes(range(64)))
    write(d, "misaligned_frame", header(INITIATOR) + b"\x00" + frame(PONG, seq=0, ts=0))
    # 100 PINGs (2.8 KB) so mutation and chunking work on a longer stream.
    write(d, "resp_ping_burst", header(RESPONDER) + b"".join(frame(PING, seq=i, ts=i) for i in range(100)))


if __name__ == "__main__":
    main()
