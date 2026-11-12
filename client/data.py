from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable

from common.app_protocol import encode_app
from common.config import Config
from common.congestion import make_controller
from common.constants import AppType, Flags, PacketType
from common.protocol import (
    PacketHeader,
    ProtocolError,
    decrypt_udp_payload,
    encrypt_udp_payload,
    pack_packet,
    random_stream_id,
    unpack_packet,
)
from common.reliability import ReliableEndpoint

DataCallback = Callable[[int, int, bytes], None]


class UDPClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, client: DataClient) -> None:
        self.client = client

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.client.transport = transport

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        self.client.on_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        self.client.last_error = str(exc)


class DataClient:
    def __init__(self, config: Config, on_data: DataCallback) -> None:
        self.config = config
        self.on_data = on_data
        self.transport: asyncio.DatagramTransport | None = None
        self.server_addr: tuple[str, int] | None = None
        self.local_port: int = 0
        self.session_id: int = 0
        self.key: bytes | None = None
        self.token: str = ""
        self.endpoint: ReliableEndpoint | None = None
        self.ready = asyncio.Event()
        self.last_error: str = ""
        self._tick_task: asyncio.Task | None = None

    async def start(self, server_host: str, server_udp_port: int) -> None:
        loop = asyncio.get_running_loop()
        self.server_addr = (server_host, server_udp_port)
        await loop.create_datagram_endpoint(
            lambda: UDPClientProtocol(self),
            local_addr=("0.0.0.0", 0),
        )
        if self.transport is None:
            raise RuntimeError("udp transport not created")
        self.local_port = int(self.transport.get_extra_info("sockname")[1])

    def set_session(self, session_id: int, key: bytes, token: str) -> None:
        self.session_id = session_id
        self.key = key
        self.token = token
        self.endpoint = ReliableEndpoint(
            session_id=session_id,
            send_packet=self.send_packet,
            on_data=self.on_data,
            chunk_size=self.config.protocol.chunk_size,
            window=self.config.protocol.window_packets,
            rto_min=self.config.protocol.rto_min,
            rto_max=self.config.protocol.rto_max,
            max_retries=self.config.protocol.max_retries,
            congestion=make_controller(
                self.config.protocol.congestion,
                10.0,
                float(self.config.protocol.window_packets * 4),
            ),
        )
        self.send_hello()
        self._tick_task = asyncio.create_task(self._tick_loop(), name="client-udp-tick")

    @property
    def send_window(self) -> int:
        if self.endpoint is not None:
            return self.endpoint.send_window
        return max(1, self.config.protocol.window_packets)

    async def wait_ready(self, timeout: float = 5.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not self.ready.is_set():
            self.send_hello()
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("udp handshake timeout")
            try:
                await asyncio.wait_for(self.ready.wait(), timeout=min(0.5, remaining))
            except TimeoutError:
                continue

    def send_hello(self) -> None:
        payload = json.dumps({"token": self.token}).encode("utf-8")
        header = PacketHeader(
            type=int(PacketType.HELLO),
            session_id=self.session_id,
            window=self.config.protocol.window_packets,
        )
        self.send_packet(header, payload)

    def send_packet(self, header: PacketHeader, payload: bytes) -> None:
        if self.transport is None or self.server_addr is None:
            return
        header.session_id = self.session_id
        if self.key is not None and header.type in (PacketType.DATA, PacketType.HELLO):
            header, payload = encrypt_udp_payload(self.key, header, payload)
        self.transport.sendto(pack_packet(header, payload), self.server_addr)

    def on_datagram(self, data: bytes, addr: tuple) -> None:
        try:
            header, payload = unpack_packet(data)
        except ProtocolError:
            return
        if header.type == PacketType.HELLO:
            if header.flags & Flags.ENCRYPTED and self.key is not None:
                try:
                    payload = decrypt_udp_payload(self.key, header, payload)
                    obj = json.loads(payload.decode("utf-8"))
                except Exception:
                    return
                if obj.get("ok"):
                    self.ready.set()
            return
        if header.type == PacketType.PING:
            self.send_packet(
                PacketHeader(type=int(PacketType.PONG), session_id=self.session_id),
                b"",
            )
            return
        if header.type == PacketType.PONG:
            return
        if header.flags & Flags.ENCRYPTED and self.key is not None:
            try:
                payload = decrypt_udp_payload(self.key, header, payload)
            except Exception:
                return
        if self.endpoint is not None:
            self.endpoint.handle_packet(header, payload)

    def send_app(
        self,
        app_type: AppType,
        obj: dict | None = None,
        raw: bytes | None = None,
        stream_id: int | None = None,
    ) -> int:
        if self.endpoint is None:
            raise RuntimeError("udp not ready")
        target_stream = stream_id or random_stream_id()
        self.endpoint.send_message(target_stream, encode_app(app_type, obj=obj, raw=raw))
        return target_stream

    def send_chunk(
        self,
        stream_id: int,
        app_type: AppType,
        obj: dict | None = None,
        raw: bytes | None = None,
        flags: int = 0,
    ) -> None:
        if self.endpoint is None:
            raise RuntimeError("udp not ready")
        self.endpoint.send_chunk(stream_id, encode_app(app_type, obj=obj, raw=raw), flags=flags)

    def close_stream(self, stream_id: int) -> None:
        if self.endpoint is not None:
            self.endpoint.close_stream(stream_id)

    def outstanding(self, stream_id: int) -> int:
        if self.endpoint is None:
            return 0
        return self.endpoint.outstanding(stream_id)

    def send_ping(self) -> None:
        self.send_packet(
            PacketHeader(type=int(PacketType.PING), session_id=self.session_id),
            struct_pack_timestamp(),
        )

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(0.05)
            if self.endpoint is not None:
                try:
                    self.endpoint.tick()
                except Exception as exc:
                    self.last_error = str(exc)

    async def close(self) -> None:
        if self._tick_task is not None:
            self._tick_task.cancel()
            await asyncio.gather(self._tick_task, return_exceptions=True)
            self._tick_task = None
        if self.transport is not None:
            self.transport.close()
            self.transport = None


def struct_pack_timestamp() -> bytes:
    import struct

    return struct.pack("!d", time.time())
