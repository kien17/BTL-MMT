from __future__ import annotations

import asyncio
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
            queued = 0
            live_users: set[str] = set()
            for peer in self.sessions.all():
                if peer.username == session.username or not peer.udp_ready:
                    continue
                stream_id = peer.send_app(AppType.TEXT, payload)
                if stream_id is None:
                    continue
                delivered += 1
                live_users.add(peer.username)
                peer.pending_live[stream_id] = {
                    "msg_id": msg_id,
                    "sender": session.username,
                    "recipient": peer.username,
                    "payload": payload,
                    "notify": False,
                    "broadcast": True,
                }
            for username in self.storage.list_users():
                if username == session.username or username in live_users:
                    continue
                if self._enqueue_text(session.username, username, msg_id, payload, broadcast=True):
                    queued += 1
            status = f"broadcast:{delivered}"
            if queued:
                status += f";queued:{queued}"
            self._send_text_ack(session, msg_id, to, status)
            if queued:
                self._schedule_presence_broadcast()
            return

        recipient = self.sessions.get_by_name(to)
        if recipient is not None and recipient.udp_ready:
            stream_id = recipient.send_app(AppType.TEXT, payload)
            if stream_id is not None:
                recipient.pending_live[stream_id] = {
                    "msg_id": msg_id,
                    "sender": session.username,
                    "recipient": to,
                    "payload": payload,
                    "notify": True,
                    "broadcast": False,
                }
                return
        if not self.storage.user_exists(to):
            self._send_text_ack(session, msg_id, to, "unknown_recipient")
            return

        stored = self._enqueue_text(session.username, to, msg_id, payload)
        self._send_text_ack(session, msg_id, to, "queued" if stored else "offline_queue_full")
        if stored:
            self._schedule_presence_broadcast()

    def _enqueue_text(
        self,
        sender: str,
        recipient: str,
        msg_id: str,
        payload: dict,
        *,
        broadcast: bool = False,
    ) -> bool:
        stored_id = f"{msg_id}:{recipient}" if broadcast else msg_id
        return self.storage.enqueue_offline(
            sender=sender,
            recipient=recipient,
            msg_id=stored_id,
            payload=json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        )

    def _schedule_presence_broadcast(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.sessions.broadcast_presence())
        except RuntimeError:
            pass

    def _rescue_live_message(self, info: dict) -> None:
        msg_id = str(info.get("msg_id", ""))
        sender = str(info.get("sender", ""))
        recipient = str(info.get("recipient", ""))
        payload = info.get("payload")
        if not msg_id or not sender or not recipient or not isinstance(payload, dict):
            return
        if not self.storage.user_exists(recipient):
            return
        stored = self._enqueue_text(
            sender,
            recipient,
            msg_id,
            payload,
            broadcast=bool(info.get("broadcast", False)),
        )
        sender_session = self.sessions.get_by_name(sender)
        if (
            bool(info.get("notify", True))
            and sender_session is not None
            and sender_session.udp_ready
        ):
            self._send_text_ack(
                sender_session,
                msg_id,
                recipient,
                "queued" if stored else "offline_queue_full",
            )
        if stored:
            self._schedule_presence_broadcast()

    def on_session_closed(self, session: Session) -> None:
        if not session.pending_live:
            return
        for info in list(session.pending_live.values()):
            self._rescue_live_message(info)
        session.pending_live.clear()

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
        recipient.send_app_chunk(
            relay.out_stream_id,
            AppType.FILE_INCOMING,
            obj={
                "transfer_id": transfer_id,
                "from": session.username,
                "to": recipient.username,
                "name": name,
                "size": size,
                "sha256": sha256,
            },
            flags=Flags.FIRST | Flags.LAST,
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
            flags=Flags.FIRST | Flags.LAST,
        )
        recipient.close_app_stream(relay.out_stream_id)

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
        recipient = self.sessions.get_by_name(relay.recipient)
        if recipient is not None:
            recipient.close_app_stream(relay.out_stream_id)
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
            payload["queued_at"] = message.get("created_at", "")
            stream_id = session.send_app(AppType.TEXT, payload)
            if stream_id is not None:
                session.pending_offline[stream_id] = message["msg_id"]
                session.pending_live[stream_id] = {
                    "offline_sender": str(message.get("sender", payload.get("from", ""))),
                    "msg_id": message["msg_id"],
                    "notify": payload.get("to") != "*",
                }

    def on_stream_acked(self, session: Session, stream_id: int) -> None:
        live_info = session.pending_live.pop(stream_id, None)
        msg_id = session.pending_offline.pop(stream_id, None)
        if msg_id is not None:
            self.storage.mark_delivered(msg_id)
            offline_sender = (
                str(live_info.get("offline_sender", "")) if isinstance(live_info, dict) else ""
            )
            notify = bool(live_info.get("notify", True)) if isinstance(live_info, dict) else True
            if offline_sender and notify:
                sender_session = self.sessions.get_by_name(offline_sender)
                if sender_session is not None and sender_session.udp_ready:
                    self._send_text_ack(sender_session, msg_id, session.username, "delivered")
            self._schedule_presence_broadcast()
            return
        if (
            isinstance(live_info, dict)
            and "payload" in live_info
            and bool(live_info.get("notify", True))
        ):
            sender = str(live_info.get("sender", ""))
            sender_session = self.sessions.get_by_name(sender)
            if sender_session is not None and sender_session.udp_ready:
                self._send_text_ack(
                    sender_session,
                    str(live_info.get("msg_id", "")),
                    session.username,
                    "delivered",
                )

    def on_stream_failed(self, session: Session, stream_id: int) -> None:
        session.pending_offline.pop(stream_id, None)
        live_info = session.pending_live.pop(stream_id, None)
        if isinstance(live_info, dict) and "payload" in live_info:
            self._rescue_live_message(live_info)
        for relay in list(self.file_relays.values()):
            if relay.recipient == session.username and relay.out_stream_id == stream_id:
                self._abort_relay(relay, "delivery_failed")
                break
