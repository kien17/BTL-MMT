from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from pathlib import Path

from client.control import ControlClient
from client.data import DataClient
from client.file_transfer import FileTransferManager
from common.app_protocol import (
    AppProtocolError,
    AppReassembler,
    decode_app,
    decode_file_chunk,
)
from common.config import Config
from common.constants import AppType, TCPType


class ClientCore:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.control = ControlClient()
        self.data: DataClient | None = None
        self.transfers = FileTransferManager(self)
        self.username = ""
        self.password = ""
        self.status = "disconnected"
        self.error = ""
        self.peers: list[dict] = []
        self.messages: deque[dict] = deque(maxlen=500)
        self._reassembler = AppReassembler()
        self._control_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._login_lock = asyncio.Lock()

    @property
    def online(self) -> bool:
        return self.status == "online"

    async def login(self, username: str, password: str) -> None:
        async with self._login_lock:
            if self.status in {"online", "connecting"}:
                return
            self.username = username
            self.password = password
            self.status = "connecting"
            self.error = ""
            data = DataClient(self.config, self._on_app_data)
            self.data = data
            try:
                await data.start(
                    self.config.client.server_host,
                    self.config.client.server_udp_port,
                )
                await self.control.connect(
                    self.config.client.server_host,
                    self.config.client.server_tcp_port,
                    username,
                    password,
                    data.local_port,
                )
                if self.control.server_udp_port:
                    data.server_addr = (
                        self.config.client.server_host,
                        self.control.server_udp_port,
                    )
                data.set_session(
                    self.control.session_id,
                    self.control.key or b"",
                    self.control.token,
                )
                await data.wait_ready(timeout=5.0)
            except Exception as exc:
                self.status = "error"
                self.error = str(exc)
                await data.close()
                self.data = None
                raise

            self.peers = list(self.control.peers)
            self.status = "online"
            self.add_message(
                {
                    "msg_id": f"system-{uuid.uuid4().hex}",
                    "from": "system",
                    "to": username,
                    "text": f"connected as {username}",
                    "ts": time.time(),
                    "kind": "system",
                }
            )
            self._control_task = asyncio.create_task(self._control_loop(), name="control-loop")
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(), name="heartbeat-loop"
            )

    async def _control_loop(self) -> None:
        try:
            while self.control.connected:
                message = await self.control.recv()
                await self._handle_control(message)
        except Exception as exc:
            if self.status == "online":
                self.error = str(exc)
        finally:
            if self.status == "online":
                self.status = "disconnected"
                self.add_message(
                    {
                        "msg_id": f"system-{uuid.uuid4().hex}",
                        "from": "system",
                        "to": self.username,
                        "text": "disconnected from server",
                        "ts": time.time(),
                        "kind": "system",
                    }
                )

    async def _handle_control(self, message: dict) -> None:
        message_type = message.get("type")
        if message_type == TCPType.PRESENCE:
            self.peers = list(message.get("peers", []))
        elif message_type == TCPType.PING:
            await self.control.send({"type": TCPType.PONG, "ts": message.get("ts")})
        elif message_type == TCPType.ERROR:
            self.error = str(message.get("reason", "server error"))
        elif message_type == TCPType.PONG:
            return

    async def _heartbeat_loop(self) -> None:
        while self.status == "online":
            await asyncio.sleep(5.0)
            try:
                await self.control.ping()
                if self.data is not None:
                    self.data.send_ping()
            except (ConnectionError, OSError, RuntimeError):
                break

    def _on_app_data(self, stream_id: int, flags: int, data: bytes) -> None:
        if not data:
            return
        try:
            if data[0] == int(AppType.FILE_CHUNK):
                transfer_id, chunk = decode_file_chunk(data[1:])
                self.transfers.handle_chunk(transfer_id, chunk)
                return
            complete = self._reassembler.feed(stream_id, data, flags)
            if complete is None:
                return
            app_type, obj, raw = decode_app(complete)
            self._dispatch_app(app_type, obj, raw)
        except (AppProtocolError, ValueError, KeyError, TypeError):
            return

    def _dispatch_app(self, app_type: AppType, obj: dict | None, raw: bytes | None) -> None:
        if app_type is AppType.TEXT and obj is not None:
            msg_id = str(obj.get("msg_id", uuid.uuid4().hex))
            if any(message.get("msg_id") == msg_id for message in self.messages):
                return
            self.add_message(
                {
                    "msg_id": msg_id,
                    "from": str(obj.get("from", "unknown")),
                    "to": str(obj.get("to", self.username)),
                    "text": str(obj.get("text", "")),
                    "ts": float(obj.get("ts", time.time())),
                    "kind": "text",
                    "offline": bool(obj.get("offline", False)),
                }
            )
        elif app_type is AppType.TEXT_ACK and obj is not None:
            self._update_message_status(str(obj.get("msg_id", "")), str(obj.get("status", "")))
        elif app_type is AppType.FILE_INCOMING and obj is not None:
            self.transfers.handle_incoming(obj)
        elif app_type is AppType.FILE_ACCEPT and obj is not None:
            self.transfers.handle_accept(obj)
        elif app_type is AppType.FILE_REJECT and obj is not None:
            self.transfers.handle_reject(obj)
        elif app_type is AppType.FILE_END and obj is not None:
            asyncio.create_task(self.transfers.handle_end(obj))
        elif app_type is AppType.FILE_RESULT and obj is not None:
            self.transfers.handle_result(obj)

    def _update_message_status(self, msg_id: str, status: str) -> None:
        for message in reversed(self.messages):
            if message.get("msg_id") == msg_id:
                message["status"] = status
                return

    def add_message(self, message: dict) -> None:
        self.messages.append(message)

    async def send_text(self, to: str, text: str) -> str:
        if not self.online or self.data is None:
            raise RuntimeError("not connected")
        to = to.strip()
        text = text.strip()
        if not to or not text:
            raise ValueError("recipient and text are required")
        msg_id = uuid.uuid4().hex
        timestamp = time.time()
        self.data.send_app(
            AppType.TEXT,
            {"msg_id": msg_id, "to": to, "text": text, "ts": timestamp},
        )
        self.add_message(
            {
                "msg_id": msg_id,
                "from": self.username,
                "to": to,
                "text": text,
                "ts": timestamp,
                "kind": "text",
                "status": "sending",
            }
        )
        return msg_id

    async def send_file(self, to: str, file_path: str | Path) -> int:
        if not self.online or self.data is None:
            raise RuntimeError("not connected")
        return await self.transfers.start_send(to, file_path)

    def get_state(self) -> dict:
        return {
            "logged_in": self.online,
            "username": self.username,
            "status": self.status,
            "error": self.error,
            "peers": self.peers,
            "messages": list(self.messages),
            "transfers": self.transfers.snapshot(),
            "server": f"{self.config.client.server_host}:{self.config.client.server_tcp_port}",
        }

    def download_path(self, transfer_id: int) -> Path | None:
        return self.transfers.download_path(transfer_id)

    async def reconnect(self) -> None:
        username = self.username
        password = self.password
        await self.logout()
        if username and password:
            await self.login(username, password)

    async def logout(self) -> None:
        self.status = "disconnected"
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None
        if self._control_task is not None:
            self._control_task.cancel()
            await asyncio.gather(self._control_task, return_exceptions=True)
            self._control_task = None
        await self.control.close()
        if self.data is not None:
            await self.data.close()
            self.data = None
        self.transfers.close()
        self.peers = []
