from __future__ import annotations

import asyncio
import json
import mimetypes
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

from client.api import sanitize_filename
from client.core import ClientCore


class UIServer:
    def __init__(self, core: ClientCore, host: str, port: int, ui_dir: Path) -> None:
        self.core = core
        self.host = host
        self.port = port
        self.ui_dir = ui_dir.resolve()
        self.server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, self.host, self.port)

    async def serve_forever(self) -> None:
        if self.server is None:
            await self.start()
        assert self.server is not None
        async with self.server:
            await self.server.serve_forever()

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            parts = request_line.decode("latin1").strip().split(" ")
            if len(parts) < 2:
                return
            method, target = parts[0], parts[1]
            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin1").partition(":")
                headers[name.strip().lower()] = value.strip()
            length = int(headers.get("content-length", "0"))
            path = urlsplit(target).path
            if method == "POST" and path == "/api/upload":
                await self._handle_upload(reader, writer, headers, length)
                return
            body = await reader.readexactly(length) if length > 0 else b""
            if path.startswith("/api/"):
                await self._handle_api(method, path, headers, body, writer)
            else:
                await self._serve_static(path, writer)
        except Exception as exc:
            await self._send_json(writer, 500, {"ok": False, "error": str(exc)})
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _handle_api(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
        writer: asyncio.StreamWriter,
    ) -> None:
        if method == "GET" and path == "/api/state":
            await self._send_json(writer, 200, self.core.get_state())
            return
        if method == "GET" and path.startswith("/api/download/"):
            try:
                transfer_id = int(path.rsplit("/", 1)[-1])
            except ValueError:
                await self._send_json(writer, 400, {"ok": False, "error": "invalid id"})
                return
            file_path = self.core.download_path(transfer_id)
            if file_path is None or not file_path.is_file():
                await self._send_json(writer, 404, {"ok": False, "error": "not found"})
                return
            await self._send_file(writer, file_path)
            return
        if method == "POST" and path == "/api/login":
            payload = self._json_body(body)
            try:
                await self.core.login(str(payload["username"]), str(payload["password"]))
            except Exception as exc:
                await self._send_json(writer, 401, {"ok": False, "error": str(exc)})
                return
            await self._send_json(writer, 200, {"ok": True, "state": self.core.get_state()})
            return
        if method == "POST" and path == "/api/send":
            payload = self._json_body(body)
            try:
                msg_id = await self.core.send_text(str(payload["to"]), str(payload["text"]))
            except Exception as exc:
                await self._send_json(writer, 400, {"ok": False, "error": str(exc)})
                return
            await self._send_json(writer, 200, {"ok": True, "msg_id": msg_id})
            return
        if method == "POST" and path == "/api/reconnect":
            try:
                await self.core.reconnect()
            except Exception as exc:
                await self._send_json(writer, 400, {"ok": False, "error": str(exc)})
                return
            await self._send_json(writer, 200, {"ok": True, "state": self.core.get_state()})
            return
        if method == "POST" and path == "/api/logout":
            await self.core.logout()
            await self._send_json(writer, 200, {"ok": True})
            return
        await self._send_json(writer, 404, {"ok": False, "error": "unknown endpoint"})

    def _json_body(self, body: bytes) -> dict:
        if not body:
            return {}
        value = json.loads(body.decode("utf-8"))
        return value if isinstance(value, dict) else {}

    async def _handle_upload(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        headers: dict[str, str],
        length: int,
    ) -> None:
        recipient = headers.get("x-recipient", "").strip()
        filename = sanitize_filename(unquote(headers.get("x-filename", "upload.bin")))
        if not recipient:
            await self._send_json(writer, 400, {"ok": False, "error": "missing upload metadata"})
            return
        upload_dir = Path(self.core.config.client.download_dir) / "uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        upload_path = upload_dir / filename
        remaining = length
        try:
            with open(upload_path, "wb") as handle:
                while remaining > 0:
                    chunk = await reader.read(min(64 * 1024, remaining))
                    if not chunk:
                        raise ConnectionError("upload body truncated")
                    handle.write(chunk)
                    remaining -= len(chunk)
            transfer_id = await self.core.send_file(recipient, upload_path)
        except Exception as exc:
            await self._send_json(writer, 400, {"ok": False, "error": str(exc)})
            return
        await self._send_json(writer, 200, {"ok": True, "transfer_id": transfer_id})

    async def _serve_static(self, path: str, writer: asyncio.StreamWriter) -> None:
        relative = "index.html" if path in {"", "/"} else path.lstrip("/")
        candidate = (self.ui_dir / relative).resolve()
        if candidate != self.ui_dir and self.ui_dir not in candidate.parents:
            await self._send_json(writer, 403, {"ok": False, "error": "forbidden"})
            return
        if not candidate.is_file():
            await self._send_json(writer, 404, {"ok": False, "error": "not found"})
            return
        content_type = mimetypes.guess_type(str(candidate))[0] or "application/octet-stream"
        data = candidate.read_bytes()
        writer.write(
            f"HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n"
            f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode("ascii")
        )
        writer.write(data)
        await writer.drain()

    async def _send_json(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        payload: dict,
    ) -> None:
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 403: "Forbidden", 404: "Not Found", 500: "Internal Server Error"}.get(status, "Error")
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode("ascii")
        )
        writer.write(data)
        await writer.drain()

    async def _send_file(self, writer: asyncio.StreamWriter, path: Path) -> None:
        size = path.stat().st_size
        filename = quote(path.name)
        writer.write(
            f"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n"
            f"Content-Disposition: attachment; filename*=UTF-8''{filename}\r\n"
            f"Content-Length: {size}\r\nConnection: close\r\n\r\n".encode("ascii")
        )
        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(64 * 1024)
                if not chunk:
                    break
                writer.write(chunk)
                await writer.drain()
