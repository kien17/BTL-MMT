from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from common.app_protocol import (
    AppProtocolError,
    decode_app,
    decode_file_chunk,
    encode_file_chunk,
)
from common.constants import AppType, Flags
from common.protocol import random_stream_id
from server.session import Session


@dataclass
class FileRelay:
    sender: str
    recipient: str
    transfer_id: int
    out_stream_id: int
    size: int
    received: int = 0
    ended: bool = False


class Router:
    def __init__(self, server: Any) -> None:
        self.server = server
        self.storage = server.storage
        self.sessions = server.sessions
        self.config = server.config
        self.file_relays: dict[tuple[str, int], FileRelay] = {}

    def handle_app(self, session: Session, stream_id: int, flags: int, data: bytes) -> None:
        if not data:
            return
        try:
            if data[0] == int(AppType.FILE_CHUNK):
                self._handle_file_chunk(session, stream_id, data)
                return
            complete = session.reassembler.feed(stream_id, data, flags)
            if complete is None:
                return
            app_type, obj, raw = decode_app(complete)
            self._dispatch(session, app_type, obj, raw)
        except (AppProtocolError, ValueError, KeyError, TypeError):
            return

    def _dispatch(
        self,
        session: Session,
        app_type: AppType,
        obj: dict | None,
        raw: bytes | None,
    ) -> None:
        if app_type is AppType.TEXT and obj is not None:
            self._handle_text(session, obj)
        elif app_type is AppType.FILE_META and obj is not None:
            self._handle_file_meta(session, obj)
        elif app_type is AppType.FILE_END and obj is not None:
            self._handle_file_end(session, obj)
        elif app_type is AppType.FILE_RESULT and obj is not None:
            self._handle_file_result(session, obj)

    def _handle_text(self, session: Session, obj: dict) -> None:
        to = str(obj.get("to", ""))
        text = str(obj.get("text", ""))
        msg_id = str(obj.get("msg_id", ""))
        ts = float(obj.get("ts", time.time()))
        if not to or not msg_id or len(text.encode("utf-8")) > 8192:
            self._send_text_ack(session, msg_id, to, "invalid")
            return

        payload = {
            "msg_id": msg_id,
            "from": session.username,
            "to": to,
            "text": text,
            "ts": ts,
        }

        if to == "*":
            delivered = 0
            for peer in self.sessions.all():
                if peer.username == session.username or not peer.udp_ready:
                    continue
                peer.send_app(AppType.TEXT, payload)
                delivered += 1
            self._send_text_ack(session, msg_id, to, f"broadcast:{delivered}")
            return

        recipient = self.sessions.get_by_name(to)
        if recipient is not None and recipient.udp_ready:
            recipient.send_app(AppType.TEXT, payload)
            self._send_text_ack(session, msg_id, to, "delivered")
            return
        if not self.storage.user_exists(to):
            self._send_text_ack(session, msg_id, to, "unknown_recipient")
            return

        stored = self.storage.enqueue_offline(
            sender=session.username,
            recipient=to,
            msg_id=msg_id,
            payload=json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        )
        self._send_text_ack(session, msg_id, to, "queued" if stored else "offline_queue_full")

    def _send_text_ack(self, session: Session, msg_id: str, to: str, status: str) -> None:
        session.send_app(
            AppType.TEXT_ACK,
            {"msg_id": msg_id, "to": to, "status": status, "ts": time.time()},
        )

    def _handle_file_meta(self, session: Session, obj: dict) -> None:
        try:
            transfer_id = int(obj["transfer_id"])
            to = str(obj["to"])
            name = str(obj["name"])
            size = int(obj["size"])
            sha256 = str(obj["sha256"])
        except (KeyError, TypeError, ValueError):
            return

        if size < 0 or size > self.config.protocol.max_file_size:
            self._reject_file(session, transfer_id, "file_too_large")
            return
        if len(sha256) != 64 or not name:
            self._reject_file(session, transfer_id, "invalid_metadata")
            return

        recipient = self.sessions.get_by_name(to)
        if recipient is None or not recipient.udp_ready:
            self._reject_file(session, transfer_id, "recipient_offline")
            return

        key = (session.username, transfer_id)
        if key in self.file_relays:
            self._reject_file(session, transfer_id, "duplicate_transfer")
            return

        relay = FileRelay(
            sender=session.username,
            recipient=recipient.username,
            transfer_id=transfer_id,
            out_stream_id=random_stream_id(),
            size=size,
        )
        self.file_relays[key] = relay

        session.send_app(AppType.FILE_ACCEPT, {"transfer_id": transfer_id})
        recipient.send_app(
            AppType.FILE_INCOMING,
            {
                "transfer_id": transfer_id,
                "from": session.username,
                "to": recipient.username,
                "name": name,
                "size": size,
                "sha256": sha256,
            },
        )

    def _handle_file_chunk(self, session: Session, stream_id: int, data: bytes) -> None:
        try:
            transfer_id, chunk = decode_file_chunk(data[1:])
        except AppProtocolError:
            return
        relay = self.file_relays.get((session.username, transfer_id))
        if relay is None or relay.ended:
            return
        recipient = self.sessions.get_by_name(relay.recipient)
        if recipient is None or not recipient.udp_ready:
            self._abort_relay(relay, "recipient_disconnected")
            return
        relay.received += len(chunk)
        recipient.send_app_chunk(
            relay.out_stream_id,
            AppType.FILE_CHUNK,
            raw=encode_file_chunk(transfer_id, chunk),
            flags=0,
        )

    def _handle_file_end(self, session: Session, obj: dict) -> None:
        try:
            transfer_id = int(obj["transfer_id"])
            sha256 = str(obj.get("sha256", ""))
        except (KeyError, TypeError, ValueError):
            return
        relay = self.file_relays.get((session.username, transfer_id))
        if relay is None or relay.ended:
            return
        recipient = self.sessions.get_by_name(relay.recipient)
        if recipient is None or not recipient.udp_ready:
            self._abort_relay(relay, "recipient_disconnected")
            return
        relay.ended = True
        recipient.send_app_chunk(
            relay.out_stream_id,
            AppType.FILE_END,
            obj={"transfer_id": transfer_id, "sha256": sha256, "size": relay.size},
            flags=Flags.LAST,
        )

    def _handle_file_result(self, session: Session, obj: dict) -> None:
        try:
            transfer_id = int(obj["transfer_id"])
        except (KeyError, TypeError, ValueError):
            return
        relay = None
        for candidate in self.file_relays.values():
            if candidate.recipient == session.username and candidate.transfer_id == transfer_id:
                relay = candidate
                break
        if relay is None:
            return
        sender = self.sessions.get_by_name(relay.sender)
        if sender is not None:
            sender.send_app(
                AppType.FILE_RESULT,
                {
                    "transfer_id": transfer_id,
                    "ok": bool(obj.get("ok", False)),
                    "error": str(obj.get("error", "")),
                },
            )
        self.file_relays.pop((relay.sender, relay.transfer_id), None)

    def _reject_file(self, session: Session, transfer_id: int, reason: str) -> None:
        session.send_app(AppType.FILE_REJECT, {"transfer_id": transfer_id, "reason": reason})

    def _abort_relay(self, relay: FileRelay, reason: str) -> None:
        sender = self.sessions.get_by_name(relay.sender)
        if sender is not None:
            sender.send_app(
                AppType.FILE_RESULT,
                {"transfer_id": relay.transfer_id, "ok": False, "error": reason},
            )
        self.file_relays.pop((relay.sender, relay.transfer_id), None)

    def abort_transfers_for(self, username: str) -> None:
        for key, relay in list(self.file_relays.items()):
            if relay.recipient == username:
                self._abort_relay(relay, "recipient_disconnected")
            elif relay.sender == username:
                self.file_relays.pop(key, None)

    def deliver_offline(self, session: Session) -> None:
        if not session.udp_ready:
            return
        pending_ids = set(session.pending_offline.values())
        for message in self.storage.fetch_offline(session.username):
            if message["msg_id"] in pending_ids:
                continue
            try:
                payload = json.loads(message["payload"])
            except json.JSONDecodeError:
                continue
            payload["offline"] = True
            stream_id = session.send_app(AppType.TEXT, payload)
            if stream_id is not None:
                session.pending_offline[stream_id] = message["msg_id"]

    def on_stream_acked(self, session: Session, stream_id: int) -> None:
        msg_id = session.pending_offline.pop(stream_id, None)
        if msg_id is not None:
            self.storage.mark_delivered(msg_id)

    def on_stream_failed(self, session: Session, stream_id: int) -> None:
        session.pending_offline.pop(stream_id, None)
