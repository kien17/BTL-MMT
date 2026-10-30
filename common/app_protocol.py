from __future__ import annotations

import json
import struct
from typing import Any

from common.constants import AppType, Flags


class AppProtocolError(Exception):
    pass


def encode_app(
    app_type: AppType,
    obj: dict[str, Any] | None = None,
    raw: bytes | None = None,
) -> bytes:
    if app_type is AppType.FILE_CHUNK:
        if raw is None:
            raise AppProtocolError("file chunk requires raw data")
        return bytes([int(app_type)]) + raw
    if obj is None:
        raise AppProtocolError("application frame requires an object")
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return bytes([int(app_type)]) + payload


def decode_app(data: bytes) -> tuple[AppType, dict[str, Any] | None, bytes | None]:
    if not data:
        raise AppProtocolError("empty app frame")
    try:
        app_type = AppType(data[0])
    except ValueError as exc:
        raise AppProtocolError(f"unknown app type {data[0]}") from exc
    body = data[1:]
    if app_type is AppType.FILE_CHUNK:
        if len(body) < 4:
            raise AppProtocolError("file chunk too short")
        return app_type, None, body
    try:
        obj = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AppProtocolError("invalid app json") from exc
    if not isinstance(obj, dict):
        raise AppProtocolError("app payload must be an object")
    return app_type, obj, None


def encode_file_chunk(transfer_id: int, data: bytes) -> bytes:
    return struct.pack("!I", transfer_id) + data


def decode_file_chunk(raw: bytes) -> tuple[int, bytes]:
    if len(raw) < 4:
        raise AppProtocolError("file chunk too short")
    (transfer_id,) = struct.unpack("!I", raw[:4])
    return transfer_id, raw[4:]


class AppReassembler:
    def __init__(self) -> None:
        self._buffers: dict[int, bytearray] = {}

    def is_active(self, stream_id: int) -> bool:
        return stream_id in self._buffers

    def feed(self, stream_id: int, payload: bytes, flags: int) -> bytes | None:
        if flags & Flags.FIRST:
            self._buffers[stream_id] = bytearray()
        buffer = self._buffers.setdefault(stream_id, bytearray())
        buffer.extend(payload)
        if flags & Flags.LAST:
            complete = bytes(buffer)
            self._buffers.pop(stream_id, None)
            return complete
        return None

    def clear(self, stream_id: int) -> None:
        self._buffers.pop(stream_id, None)
