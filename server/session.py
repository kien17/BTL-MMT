from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from common.app_protocol import AppReassembler, encode_app
from common.constants import AppType, TCPType
from common.protocol import PacketHeader, random_session_id, random_stream_id, write_tcp_frame
from common.reliability import ReliableEndpoint


@dataclass
class Session:
    username: str
    session_id: int
    token: str
    key: bytes
    writer: asyncio.StreamWriter
    peer: tuple
    udp_addr: tuple | None = None
    endpoint: ReliableEndpoint | None = None
    last_seen: float = field(default_factory=time.monotonic)
    connected_at: float = field(default_factory=time.monotonic)
    reassembler: AppReassembler = field(default_factory=AppReassembler)
    pending_offline: dict[int, str] = field(default_factory=dict)
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def udp_ready(self) -> bool:
        return self.endpoint is not None and self.udp_addr is not None

    def touch(self) -> None:
        self.last_seen = time.monotonic()

    async def send_tcp(self, obj: dict) -> None:
        async with self.write_lock:
            await write_tcp_frame(self.writer, obj, self.key)

    def send_app(
        self,
        app_type: AppType,
        obj: dict | None = None,
        raw: bytes | None = None,
        stream_id: int | None = None,
    ) -> int | None:
        if self.endpoint is None:
            return None
        target_stream = stream_id or random_stream_id()
        payload = encode_app(app_type, obj=obj, raw=raw)
        self.endpoint.send_message(target_stream, payload)
        return target_stream

    def send_app_chunk(
        self,
        stream_id: int,
        app_type: AppType,
        obj: dict | None = None,
        raw: bytes | None = None,
        flags: int = 0,
    ) -> None:
        if self.endpoint is None:
            return
        payload = encode_app(app_type, obj=obj, raw=raw)
        self.endpoint.send_chunk(stream_id, payload, flags=flags)


class SessionManager:
    def __init__(self) -> None:
        self._by_name: dict[str, Session] = {}
        self._by_id: dict[int, Session] = {}

    def next_session_id(self) -> int:
        while True:
            session_id = random_session_id()
            if session_id not in self._by_id:
                return session_id

    def add(self, session: Session) -> None:
        self._by_name[session.username] = session
        self._by_id[session.session_id] = session

    def remove(self, session: Session) -> None:
        if self._by_name.get(session.username) is session:
            self._by_name.pop(session.username, None)
        if self._by_id.get(session.session_id) is session:
            self._by_id.pop(session.session_id, None)

    def get_by_name(self, username: str) -> Session | None:
        return self._by_name.get(username)

    def get_by_id(self, session_id: int) -> Session | None:
        return self._by_id.get(session_id)

    def all(self) -> list[Session]:
        return list(self._by_name.values())

    def online_usernames(self) -> list[str]:
        return sorted(self._by_name)

    def peers(self) -> list[dict]:
        now = time.monotonic()
        return [
            {
                "username": session.username,
                "online_seconds": int(now - session.connected_at),
                "udp_ready": session.udp_ready,
            }
            for session in sorted(self._by_name.values(), key=lambda item: item.username)
        ]

    def presence_payload(self) -> dict:
        return {"type": TCPType.PRESENCE, "peers": self.peers()}

    async def send_presence(self, session: Session) -> None:
        try:
            await session.send_tcp(self.presence_payload())
        except (ConnectionError, OSError):
            pass

    async def broadcast_presence(self) -> None:
        payload = self.presence_payload()
        tasks = []
        for session in self.all():
            tasks.append(self._send_presence_safe(session, payload))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_presence_safe(self, session: Session, payload: dict) -> None:
        try:
            await session.send_tcp(payload)
        except (ConnectionError, OSError):
            pass


def make_header(header_type: int, **kwargs) -> PacketHeader:
    return PacketHeader(type=header_type, **kwargs)
