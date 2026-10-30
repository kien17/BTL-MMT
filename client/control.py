from __future__ import annotations

import asyncio
import base64
import time
from typing import Any

from common.constants import TCPType
from common.crypto import derive_session_key, generate_keypair
from common.protocol import (
    decode_tcp_body,
    read_tcp_frame,
    write_tcp_frame,
)


class ControlError(Exception):
    pass


class AuthError(ControlError):
    pass


class ControlClient:
    def __init__(self) -> None:
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.key: bytes | None = None
        self.session_id: int = 0
        self.token: str = ""
        self.peers: list[dict] = []
        self.username: str = ""
        self.server_udp_port: int = 0

    @property
    def connected(self) -> bool:
        return self.writer is not None and not self.writer.is_closing()

    async def connect(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        udp_port: int,
    ) -> None:
        self.username = username
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=10,
        )
        private_key, public_key = generate_keypair()
        await write_tcp_frame(
            self.writer,
            {
                "type": TCPType.HELLO,
                "version": 1,
                "username": username,
                "udp_port": udp_port,
                "public_key": base64.b64encode(public_key).decode("ascii"),
                "ts": time.time(),
            },
        )
        body = await asyncio.wait_for(read_tcp_frame(self.reader), timeout=10)
        hello_ack = decode_tcp_body(body, None)
        if hello_ack.get("type") != TCPType.HELLO_ACK:
            raise ControlError("expected HELLO_ACK")
        server_public = base64.b64decode(str(hello_ack["server_public_key"]))
        salt = base64.b64decode(str(hello_ack["salt"]))
        self.session_id = int(hello_ack["session_id"])
        self.server_udp_port = int(hello_ack.get("udp_port", 0))
        self.key = derive_session_key(private_key, server_public, salt)

        await write_tcp_frame(
            self.writer,
            {"type": TCPType.AUTH_REQ, "username": username, "password": password},
            self.key,
        )
        body = await asyncio.wait_for(read_tcp_frame(self.reader), timeout=10)
        auth = decode_tcp_body(body, self.key)
        if auth.get("type") == TCPType.AUTH_ERR:
            raise AuthError(str(auth.get("reason", "authentication failed")))
        if auth.get("type") != TCPType.AUTH_OK:
            raise ControlError("expected AUTH_OK")
        self.token = str(auth["token"])
        self.peers = list(auth.get("peers", []))
        self.server_udp_port = int(auth.get("udp_port", self.server_udp_port))

    async def send(self, obj: dict[str, Any]) -> None:
        if self.writer is None:
            raise ControlError("not connected")
        await write_tcp_frame(self.writer, obj, self.key)

    async def recv(self) -> dict[str, Any]:
        if self.reader is None:
            raise ControlError("not connected")
        body = await read_tcp_frame(self.reader)
        return decode_tcp_body(body, self.key)

    async def ping(self) -> None:
        await self.send({"type": TCPType.PING, "ts": time.time()})

    async def close(self) -> None:
        if self.writer is None:
            return
        try:
            await self.send({"type": TCPType.BYE, "ts": time.time()})
        except (ConnectionError, OSError):
            pass
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass
        self.writer = None
        self.reader = None
