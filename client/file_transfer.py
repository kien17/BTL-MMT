from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from common.app_protocol import encode_file_chunk
from common.constants import AppType, Flags
from common.crypto import sha256_file
from common.protocol import random_stream_id


@dataclass
class Transfer:
    transfer_id: int
    direction: str
    peer: str
    filename: str
    size: int
    sha256: str
    status: str
    progress: int = 0
    error: str = ""
    path: Path | None = None
    temp_path: Path | None = None
    file_handle: Any = None
    accept_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    result_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)


class FileTransferManager:
    def __init__(self, core: Any) -> None:
        self.core = core
        self.transfers: dict[int, Transfer] = {}
        self.pending_chunks: dict[int, bytearray] = {}
        self.pending_end: dict[int, dict] = {}
        self.download_dir = Path(core.config.client.download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _new_id(self) -> int:
        while True:
            transfer_id = random_stream_id()
            if transfer_id not in self.transfers:
                return transfer_id

    async def start_send(self, recipient: str, file_path: str | Path) -> int:
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(str(path))
        size = path.stat().st_size
        if size > self.core.config.protocol.max_file_size:
            raise ValueError("file too large")
        digest = await asyncio.to_thread(sha256_file, path)
        transfer_id = self._new_id()
        transfer = Transfer(
            transfer_id=transfer_id,
            direction="send",
            peer=recipient,
            filename=path.name,
            size=size,
            sha256=digest,
            status="offered",
            path=path,
        )
        self.transfers[transfer_id] = transfer
        self.core.data.send_app(
            AppType.FILE_META,
            {
                "transfer_id": transfer_id,
                "to": recipient,
                "name": path.name,
                "size": size,
                "sha256": digest,
            },
        )
        try:
            await asyncio.wait_for(transfer.accept_event.wait(), timeout=30)
        except TimeoutError:
            transfer.status = "failed"
            transfer.error = "accept timeout"
            return transfer_id
        if transfer.status != "accepted":
            return transfer_id

        stream_id = transfer_id
        sent = 0
        try:
            with open(path, "rb") as handle:
                while True:
                    chunk = handle.read(self.core.config.protocol.chunk_size)
                    if not chunk:
                        break
                    while self.core.data.outstanding(stream_id) >= self.core.data.send_window:
                        if transfer.status in {"failed", "rejected"}:
                            return transfer_id
                        await asyncio.sleep(0.01)
                    self.core.data.send_chunk(
                        stream_id,
                        AppType.FILE_CHUNK,
                        raw=encode_file_chunk(transfer_id, chunk),
                    )
                    sent += len(chunk)
                    transfer.progress = sent
                    transfer.status = "transferring"
                    await asyncio.sleep(0)
            self.core.data.send_chunk(
                stream_id,
                AppType.FILE_END,
                obj={"transfer_id": transfer_id, "sha256": digest},
                flags=Flags.FIRST | Flags.LAST,
            )
            self.core.data.close_stream(stream_id)
            transfer.status = "verifying"
        except (ConnectionError, OSError, RuntimeError) as exc:
            transfer.status = "failed"
            transfer.error = str(exc)
            return transfer_id

        try:
            await asyncio.wait_for(transfer.result_event.wait(), timeout=120)
        except TimeoutError:
            if transfer.status != "completed":
                transfer.status = "failed"
                transfer.error = "result timeout"
        return transfer_id

    def handle_incoming(self, obj: dict) -> None:
        try:
            transfer_id = int(obj["transfer_id"])
            sender = str(obj["from"])
            name = Path(str(obj["name"])).name or "download.bin"
            size = int(obj["size"])
            digest = str(obj.get("sha256", ""))
        except (KeyError, TypeError, ValueError):
            return
        if transfer_id in self.transfers:
            return
        if size > self.core.config.protocol.max_file_size:
            self.core.data.send_app(
                AppType.FILE_RESULT,
                {"transfer_id": transfer_id, "ok": False, "error": "file_too_large"},
            )
            return
        temp_path = self.download_dir / f"{transfer_id}_{name}.part"
        handle = open(temp_path, "wb")
        transfer = Transfer(
            transfer_id=transfer_id,
            direction="recv",
            peer=sender,
            filename=name,
            size=size,
            sha256=digest,
            status="receiving",
            path=self.download_dir / name,
            temp_path=temp_path,
            file_handle=handle,
        )
        self.transfers[transfer_id] = transfer
        buffered = self.pending_chunks.pop(transfer_id, None)
        if buffered:
            handle.write(buffered)
            transfer.progress += len(buffered)
        pending_end = self.pending_end.pop(transfer_id, None)
        if pending_end is not None:
            asyncio.create_task(self.handle_end(pending_end))

    def handle_chunk(self, transfer_id: int, chunk: bytes) -> None:
        transfer = self.transfers.get(transfer_id)
        if transfer is None:
            buffer = self.pending_chunks.setdefault(transfer_id, bytearray())
            if len(buffer) + len(chunk) <= 2 * 1024 * 1024:
                buffer.extend(chunk)
            return
        if transfer.direction != "recv" or transfer.file_handle is None:
            return
        if transfer.status != "receiving":
            return
        transfer.file_handle.write(chunk)
        transfer.progress += len(chunk)

    async def handle_end(self, obj: dict) -> None:
        try:
            transfer_id = int(obj["transfer_id"])
        except (KeyError, TypeError, ValueError):
            return
        transfer = self.transfers.get(transfer_id)
        if transfer is None:
            self.pending_end[transfer_id] = obj
            return
        if transfer.direction != "recv":
            return
        if transfer.file_handle is not None:
            transfer.file_handle.close()
            transfer.file_handle = None
        transfer.status = "verifying"
        digest = await asyncio.to_thread(sha256_file, transfer.temp_path)
        expected = transfer.sha256 or str(obj.get("sha256", ""))
        if digest != expected:
            transfer.status = "failed"
            transfer.error = "hash mismatch"
            if transfer.temp_path is not None:
                transfer.temp_path.unlink(missing_ok=True)
            self.core.data.send_app(
                AppType.FILE_RESULT,
                {"transfer_id": transfer_id, "ok": False, "error": "hash mismatch"},
            )
            return
        final_path = self._unique_path(transfer.path or self.download_dir / transfer.filename)
        if transfer.temp_path is not None:
            transfer.temp_path.replace(final_path)
        transfer.path = final_path
        transfer.status = "completed"
        transfer.progress = transfer.size
        self.core.data.send_app(
            AppType.FILE_RESULT,
            {"transfer_id": transfer_id, "ok": True, "error": ""},
        )
        self.core.add_message(
            {
                "msg_id": f"file-{transfer_id}",
                "from": transfer.peer,
                "to": self.core.username,
                "text": f"received file {final_path.name}",
                "ts": time.time(),
                "kind": "system",
            }
        )

    def handle_accept(self, obj: dict) -> None:
        transfer = self.transfers.get(int(obj.get("transfer_id", 0)))
        if transfer is None:
            return
        transfer.status = "accepted"
        transfer.accept_event.set()

    def handle_reject(self, obj: dict) -> None:
        transfer = self.transfers.get(int(obj.get("transfer_id", 0)))
        if transfer is None:
            return
        transfer.status = "rejected"
        transfer.error = str(obj.get("reason", "rejected"))
        transfer.accept_event.set()
        transfer.result_event.set()

    def handle_result(self, obj: dict) -> None:
        transfer = self.transfers.get(int(obj.get("transfer_id", 0)))
        if transfer is None:
            return
        if bool(obj.get("ok", False)):
            transfer.status = "completed"
            transfer.progress = transfer.size
        else:
            transfer.status = "failed"
            transfer.error = str(obj.get("error", "transfer failed"))
        transfer.result_event.set()

    def snapshot(self) -> list[dict]:
        return [
            {
                "transfer_id": transfer.transfer_id,
                "direction": transfer.direction,
                "peer": transfer.peer,
                "filename": transfer.filename,
                "size": transfer.size,
                "progress": transfer.progress,
                "status": transfer.status,
                "error": transfer.error,
            }
            for transfer in sorted(self.transfers.values(), key=lambda item: item.transfer_id)
        ]

    def download_path(self, transfer_id: int) -> Path | None:
        transfer = self.transfers.get(transfer_id)
        if transfer is None or transfer.direction != "recv" or transfer.status != "completed":
            return None
        return transfer.path

    def close(self) -> None:
        for transfer in self.transfers.values():
            if transfer.file_handle is not None:
                transfer.file_handle.close()
                transfer.file_handle = None
        self.transfers.clear()
        self.pending_chunks.clear()
        self.pending_end.clear()

    def _unique_path(self, path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem
        suffix = path.suffix
        for index in range(1, 1000):
            candidate = path.with_name(f"{stem} ({index}){suffix}")
            if not candidate.exists():
                return candidate
        return path.with_name(f"{stem}-{int(time.time())}{suffix}")
