from __future__ import annotations

import asyncio
import base64
import json
import logging
import time

from common.config import Config, load_config
from common.constants import Flags, PacketType, TCPType
from common.crypto import (
    derive_session_key,
    generate_keypair,
    random_salt,
    random_token,
)
from common.protocol import (
    PacketHeader,
    ProtocolError,
    decode_tcp_body,
    decrypt_udp_payload,
    encrypt_udp_payload,
    pack_packet,
    read_tcp_frame,
    unpack_packet,
    write_tcp_frame,
)
from common.reliability import ReliableEndpoint
from server.auth import seed_users, verify_credentials
from server.router import Router
from server.session import Session, SessionManager
from server.storage import Storage

LOGGER = logging.getLogger("collab.server")


class UDPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: CollabServer) -> None:
        self.server = server

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.server.udp_transport = transport

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        self.server.handle_udp_datagram(data, addr)


class CollabServer:
    def __init__(self, config: Config | None = None) -> None:
        self.config = config or load_config()
        self.storage = Storage(
            self.config.server.db_path,
            offline_retention_days=self.config.server.offline_retention_days,
            offline_max_per_user=self.config.server.offline_max_per_user,
            offline_max_payload=self.config.server.offline_max_payload,
        )
        self.sessions = SessionManager()
        self.router = Router(self)
        self.tcp_server: asyncio.AbstractServer | None = None
        self.udp_transport: asyncio.DatagramTransport | None = None
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        seed_users(self.storage)
        self.storage.cleanup_expired()
        self.tcp_server = await asyncio.start_server(
            self.handle_tcp,
            self.config.server.host,
            self.config.server.tcp_port,
        )
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(
            lambda: UDPServerProtocol(self),
            local_addr=(self.config.server.host, self.config.server.udp_port),
        )
        self._tasks = [
            asyncio.create_task(self._sweep_loop(), name="session-sweep"),
            asyncio.create_task(self._tick_loop(), name="reliability-tick"),
        ]
        LOGGER.info(
            "server listening tcp=%s:%s udp=%s:%s",
            self.config.server.host,
            self.config.server.tcp_port,
            self.config.server.host,
            self.config.server.udp_port,
        )

    async def serve_forever(self) -> None:
        if self.tcp_server is None:
            await self.start()
        assert self.tcp_server is not None
        async with self.tcp_server:
            await self.tcp_server.serve_forever()

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.tcp_server is not None:
            self.tcp_server.close()
            await self.tcp_server.wait_closed()
        if self.udp_transport is not None:
            self.udp_transport.close()
        for session in self.sessions.all():
            session.writer.close()
        self.storage.close()

    async def handle_tcp(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        session: Session | None = None
        try:
            body = await asyncio.wait_for(read_tcp_frame(reader), timeout=15)
            hello = decode_tcp_body(body, None)
            if hello.get("type") != TCPType.HELLO:
                raise ProtocolError("expected HELLO")
            username = str(hello.get("username", ""))
            if not username or len(username) > 64:
                raise ProtocolError("invalid username")
            client_public = base64.b64decode(str(hello["public_key"]))
            int(hello.get("udp_port", 0))

            server_private, server_public = generate_keypair()
            salt = random_salt(16)
            key = derive_session_key(server_private, client_public, salt)
            session_id = self.sessions.next_session_id()
            await write_tcp_frame(
                writer,
                {
                    "type": TCPType.HELLO_ACK,
                    "version": 1,
                    "session_id": session_id,
                    "server_public_key": base64.b64encode(server_public).decode("ascii"),
                    "salt": base64.b64encode(salt).decode("ascii"),
                    "udp_port": self.config.server.udp_port,
                },
            )

            body = await asyncio.wait_for(read_tcp_frame(reader), timeout=15)
            auth = decode_tcp_body(body, key)
            if auth.get("type") != TCPType.AUTH_REQ:
                raise ProtocolError("expected AUTH_REQ")
            auth_username = str(auth.get("username", ""))
            password = str(auth.get("password", ""))
            if auth_username != username:
                raise ProtocolError("username mismatch")
            if not verify_credentials(self.storage, username, password):
                await write_tcp_frame(
                    writer,
                    {"type": TCPType.AUTH_ERR, "reason": "invalid_credentials"},
                    key,
                )
                return
            if self.sessions.get_by_name(username) is not None:
                await write_tcp_frame(
                    writer,
                    {"type": TCPType.AUTH_ERR, "reason": "already_online"},
                    key,
                )
                return

            session = Session(
                username=username,
                session_id=session_id,
                token=random_token(),
                key=key,
                writer=writer,
                peer=peer,
            )
            self.sessions.add(session)
            await session.send_tcp(
                {
                    "type": TCPType.AUTH_OK,
                    "session_id": session_id,
                    "token": session.token,
                    "peers": self.sessions.peers(),
                    "udp_port": self.config.server.udp_port,
                }
            )
            await self.sessions.broadcast_presence()
            LOGGER.info("user online: %s from %s", username, peer)

            while True:
                body = await read_tcp_frame(reader)
                message = decode_tcp_body(body, key)
                session.touch()
                message_type = message.get("type")
                if message_type == TCPType.PING:
                    await session.send_tcp({"type": TCPType.PONG, "ts": message.get("ts")})
                elif message_type == TCPType.BYE:
                    break
                elif message_type == TCPType.FILE_CTRL:
                    continue
                else:
                    await session.send_tcp(
                        {"type": TCPType.ERROR, "reason": "unsupported_control_message"}
                    )
        except (
            TimeoutError,
            asyncio.IncompleteReadError,
            ConnectionError,
            OSError,
            ProtocolError,
            ValueError,
            KeyError,
            Exception,
        ) as exc:
            LOGGER.debug("tcp connection closed: %s (%s)", peer, exc)
        finally:
            if session is not None:
                self.sessions.remove(session)
                self.router.abort_transfers_for(session.username)
                LOGGER.info("user offline: %s", session.username)
                await self.sessions.broadcast_presence()
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    def handle_udp_datagram(self, data: bytes, addr: tuple) -> None:
        try:
            header, payload = unpack_packet(data)
        except ProtocolError:
            return
        if header.type == PacketType.HELLO:
            self._handle_udp_hello(header, payload, addr)
            return
        session = self.sessions.get_by_id(header.session_id)
        if session is None:
            return
        session.touch()
        if session.udp_addr != addr:
            session.udp_addr = addr
        if header.type == PacketType.PING:
            self.send_udp(session, PacketHeader(type=int(PacketType.PONG)), b"")
            return
        if header.type == PacketType.PONG:
            return
        if header.flags & Flags.ENCRYPTED:
            try:
                payload = decrypt_udp_payload(session.key, header, payload)
            except Exception:
                return
        if session.endpoint is not None:
            session.endpoint.handle_packet(header, payload)

    def _handle_udp_hello(self, header: PacketHeader, payload: bytes, addr: tuple) -> None:
        session = self.sessions.get_by_id(header.session_id)
        if session is None or not (header.flags & Flags.ENCRYPTED):
            return
        try:
            plaintext = decrypt_udp_payload(session.key, header, payload)
            obj = json.loads(plaintext.decode("utf-8"))
        except Exception:
            return
        if obj.get("token") != session.token:
            return
        session.udp_addr = addr
        session.touch()
        if session.endpoint is None:
            session.endpoint = ReliableEndpoint(
                session_id=session.session_id,
                send_packet=lambda packet_header, packet_payload: self.send_udp(
                    session, packet_header, packet_payload
                ),
                on_data=lambda stream_id, flags, stream_payload: self.router.handle_app(
                    session, stream_id, flags, stream_payload
                ),
                on_stream_acked=lambda stream_id: self.router.on_stream_acked(
                    session, stream_id
                ),
                on_stream_failed=lambda stream_id: self.router.on_stream_failed(
                    session, stream_id
                ),
                chunk_size=self.config.protocol.chunk_size,
                window=self.config.protocol.window_packets,
                rto_min=self.config.protocol.rto_min,
                rto_max=self.config.protocol.rto_max,
                max_retries=self.config.protocol.max_retries,
            )
            LOGGER.info("udp bound for %s at %s", session.username, addr)
            asyncio.get_running_loop().create_task(self.sessions.broadcast_presence())
        self.send_udp(
            session,
            PacketHeader(type=int(PacketType.HELLO)),
            json.dumps({"ok": True}).encode("utf-8"),
        )
        self.router.deliver_offline(session)

    def send_udp(self, session: Session, header: PacketHeader, payload: bytes) -> None:
        if self.udp_transport is None or session.udp_addr is None:
            return
        header.session_id = session.session_id
        if header.type in (PacketType.DATA, PacketType.HELLO):
            header, payload = encrypt_udp_payload(session.key, header, payload)
        packet = pack_packet(header, payload)
        self.udp_transport.sendto(packet, session.udp_addr)

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            now = time.monotonic()
            for session in list(self.sessions.all()):
                if now - session.last_seen > self.config.server.session_timeout:
                    LOGGER.info("session timeout: %s", session.username)
                    session.writer.close()
            self.storage.cleanup_expired()

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(0.05)
            for session in self.sessions.all():
                if session.endpoint is not None:
                    try:
                        session.endpoint.tick()
                    except Exception:
                        LOGGER.exception("udp tick failed for %s", session.username)


async def _run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    server = CollabServer(load_config())
    await server.start()
    try:
        await server.serve_forever()
    finally:
        await server.stop()


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
