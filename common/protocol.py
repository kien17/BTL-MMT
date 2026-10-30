from __future__ import annotations

import asyncio
import json
import os
import struct
import zlib
from dataclasses import dataclass, replace
from typing import Any

from common.constants import HEADER_FORMAT, HEADER_SIZE, MAX_TCP_FRAME, VERSION


class ProtocolError(Exception):
    pass


@dataclass
class PacketHeader:
    version: int = VERSION
    type: int = 0
    flags: int = 0
    header_len: int = HEADER_SIZE
    session_id: int = 0
    stream_id: int = 0
    seq: int = 0
    ack: int = 0
    sack: int = 0
    window: int = 0
    payload_len: int = 0
    crc: int = 0


def pack_header(header: PacketHeader) -> bytes:
    return struct.pack(
        HEADER_FORMAT,
        header.version,
        header.type,
        header.flags,
        header.header_len,
        header.session_id,
        header.stream_id,
        header.seq,
        header.ack,
        header.sack,
        header.window,
        header.payload_len,
        header.crc,
    )


def unpack_header(data: bytes) -> PacketHeader:
    if len(data) < HEADER_SIZE:
        raise ProtocolError("header too short")
    fields = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
    return PacketHeader(*fields)


def pack_packet(header: PacketHeader, payload: bytes) -> bytes:
    header = replace(header, header_len=HEADER_SIZE, payload_len=len(payload), crc=0)
    raw_header = pack_header(header)
    crc = zlib.crc32(raw_header + payload) & 0xFFFFFFFF
    return pack_header(replace(header, crc=crc)) + payload


def unpack_packet(data: bytes) -> tuple[PacketHeader, bytes]:
    header = unpack_header(data)
    if header.version != VERSION:
        raise ProtocolError(f"unsupported version {header.version}")
    if header.header_len != HEADER_SIZE:
        raise ProtocolError("invalid header length")
    payload = data[HEADER_SIZE:]
    if len(payload) != header.payload_len:
        raise ProtocolError("payload length mismatch")
    raw_header = pack_header(replace(header, crc=0))
    expected = zlib.crc32(raw_header + payload) & 0xFFFFFFFF
    if expected != header.crc:
        raise ProtocolError("crc mismatch")
    return header, payload


def encode_tcp_frame(obj: dict[str, Any], key: bytes | None = None) -> bytes:
    plaintext = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if key is None:
        payload = plaintext
        encrypted = 0
    else:
        from common.crypto import encrypt

        payload = encrypt(key, plaintext)
        encrypted = 1
    body = bytes([encrypted]) + payload
    return struct.pack("!I", len(body)) + body


def decode_tcp_body(body: bytes, key: bytes | None = None) -> dict[str, Any]:
    if not body:
        raise ProtocolError("empty tcp frame")
    encrypted = body[0]
    payload = body[1:]
    if encrypted:
        if key is None:
            raise ProtocolError("encrypted frame without key")
        from common.crypto import decrypt

        plaintext = decrypt(key, payload)
    else:
        plaintext = payload
    try:
        obj = json.loads(plaintext.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("invalid json frame") from exc
    if not isinstance(obj, dict):
        raise ProtocolError("frame must be an object")
    return obj


async def read_tcp_frame(reader: asyncio.StreamReader) -> bytes:
    prefix = await reader.readexactly(4)
    (length,) = struct.unpack("!I", prefix)
    if length <= 0 or length > MAX_TCP_FRAME:
        raise ProtocolError("invalid tcp frame length")
    return await reader.readexactly(length)


async def write_tcp_frame(
    writer: asyncio.StreamWriter,
    obj: dict[str, Any],
    key: bytes | None = None,
) -> None:
    writer.write(encode_tcp_frame(obj, key))
    await writer.drain()


def make_udp_aad(header: PacketHeader) -> bytes:
    return pack_header(replace(header, crc=0, payload_len=0))


def encrypt_udp_payload(key: bytes, header: PacketHeader, payload: bytes) -> tuple[PacketHeader, bytes]:
    from common.crypto import encrypt

    header = replace(header, flags=header.flags | 4, crc=0)
    aad = make_udp_aad(header)
    ciphertext = encrypt(key, payload, aad)
    header = replace(header, payload_len=len(ciphertext))
    return header, ciphertext


def decrypt_udp_payload(key: bytes, header: PacketHeader, payload: bytes) -> bytes:
    from common.crypto import decrypt

    aad = make_udp_aad(replace(header, crc=0))
    return decrypt(key, payload, aad)


def random_session_id() -> int:
    value = int.from_bytes(os.urandom(4), "big")
    return value or 1


def random_stream_id() -> int:
    value = int.from_bytes(os.urandom(4), "big")
    return value or 1
