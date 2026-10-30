from __future__ import annotations

import struct

import pytest

from common.app_protocol import AppReassembler, decode_app, encode_app
from common.constants import AppType, Flags, PacketType
from common.crypto import decrypt, encrypt
from common.protocol import (
    PacketHeader,
    ProtocolError,
    decode_tcp_body,
    decrypt_udp_payload,
    encode_tcp_frame,
    encrypt_udp_payload,
    pack_packet,
    unpack_packet,
)


def test_packet_roundtrip() -> None:
    header = PacketHeader(
        type=int(PacketType.DATA),
        flags=int(Flags.FIRST),
        session_id=123,
        stream_id=456,
        seq=7,
        ack=3,
        window=64,
    )
    packet = pack_packet(header, b"payload")
    decoded_header, payload = unpack_packet(packet)
    assert decoded_header.session_id == 123
    assert decoded_header.stream_id == 456
    assert decoded_header.seq == 7
    assert payload == b"payload"


def test_udp_payload_encrypted_roundtrip() -> None:
    key = b"k" * 32
    header = PacketHeader(type=int(PacketType.HELLO), session_id=1, stream_id=2, seq=3)
    encrypted_header, ciphertext = encrypt_udp_payload(key, header, b"hello")
    packet = pack_packet(encrypted_header, ciphertext)
    decoded_header, decoded_payload = unpack_packet(packet)
    assert decoded_header.flags & Flags.ENCRYPTED
    assert decrypt_udp_payload(key, decoded_header, decoded_payload) == b"hello"


def test_packet_crc_rejects_corruption() -> None:
    packet = bytearray(pack_packet(PacketHeader(type=int(PacketType.DATA)), b"payload"))
    packet[-1] ^= 0xFF
    with pytest.raises(ProtocolError):
        unpack_packet(bytes(packet))


def test_tcp_frame_plaintext_roundtrip() -> None:
    obj = {"type": "PING", "ts": 1.5}
    frame = encode_tcp_frame(obj)
    length = struct.unpack("!I", frame[:4])[0]
    assert length == len(frame) - 4
    assert decode_tcp_body(frame[4:]) == obj


def test_tcp_frame_encrypted_roundtrip() -> None:
    key = b"k" * 32
    obj = {"type": "AUTH_OK", "token": "secret"}
    frame = encode_tcp_frame(obj, key)
    assert decode_tcp_body(frame[4:], key) == obj
    with pytest.raises(Exception):
        decode_tcp_body(frame[4:], b"x" * 32)


def test_app_reassembler() -> None:
    reassembler = AppReassembler()
    payload = encode_app(AppType.TEXT, {"text": "hello"})
    first = payload[:5]
    rest = payload[5:]
    assert reassembler.feed(1, first, Flags.FIRST) is None
    assert reassembler.feed(1, rest, Flags.LAST) == payload
    app_type, obj, raw = decode_app(payload)
    assert app_type is AppType.TEXT
    assert obj == {"text": "hello"}
    assert raw is None


def test_crypto_helpers_roundtrip() -> None:
    key = b"k" * 32
    ciphertext = encrypt(key, b"hello", b"aad")
    assert decrypt(key, ciphertext, b"aad") == b"hello"
    with pytest.raises(Exception):
        decrypt(key, ciphertext, b"wrong")
