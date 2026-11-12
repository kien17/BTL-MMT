from __future__ import annotations

import asyncio
from pathlib import Path

from client.core import ClientCore
from common.config import Config
from server.main import CollabServer
from server.session import SessionManager
from server.storage import Storage


def test_storage_and_session_peers_include_offline_users(tmp_path: Path) -> None:
    storage = Storage(str(tmp_path / "test.db"))
    storage.seed_default_users()
    users = storage.list_users()
    assert "alice" in users
    assert "bob" in users
    assert "charlie" in users

    assert storage.enqueue_offline(
        sender="alice",
        recipient="bob",
        msg_id="msg-1",
        payload='{"msg_id":"msg-1","from":"alice","to":"bob","text":"hi bob","ts":1.0}',
    )
    counts = storage.pending_counts_by_recipient()
    assert counts.get("bob") == 1

    sm = SessionManager(storage)
    peers = sm.peers()
    by_name = {peer["username"]: peer for peer in peers}
    assert by_name["bob"]["online"] is False
    assert by_name["bob"]["queued_count"] == 1
    storage.close()


def test_offline_queue_end_to_end_and_outbox(tmp_path: Path) -> None:
    async def scenario() -> None:
        server_cfg = Config()
        server_cfg.server.host = "127.0.0.1"
        server_cfg.server.tcp_port = 19100
        server_cfg.server.udp_port = 19101
        server_cfg.server.db_path = str(tmp_path / "server.db")

        server = CollabServer(server_cfg)
        await server.start()
        try:
            def make_client_cfg() -> Config:
                cfg = Config()
                cfg.client.server_host = "127.0.0.1"
                cfg.client.server_tcp_port = 19100
                cfg.client.server_udp_port = 19101
                cfg.client.download_dir = str(tmp_path / "downloads")
                return cfg

            alice = ClientCore(make_client_cfg())
            bob = ClientCore(make_client_cfg())

            await alice.login("alice", "password123")
            # Alice sees bob in peers list even though bob is offline
            alice_state = alice.get_state()
            bob_peer = next(p for p in alice_state["peers"] if p["username"] == "bob")
            assert bob_peer["online"] is False

            # Alice sends message to offline bob -> queued on server
            msg_id_1 = await alice.send_text("bob", "tin nhan luc bob offline")
            for _ in range(30):
                await asyncio.sleep(0.05)
                msg = next(m for m in alice.messages if m.get("msg_id") == msg_id_1)
                if msg.get("status") == "queued":
                    break
            assert next(m for m in alice.messages if m.get("msg_id") == msg_id_1)["status"] == "queued"

            # Now bob logs in and receives the queued offline message
            await bob.login("bob", "password123")
            for _ in range(40):
                await asyncio.sleep(0.05)
                if any(m.get("msg_id") == msg_id_1 for m in bob.messages):
                    break

            received = next(m for m in bob.messages if m.get("msg_id") == msg_id_1)
            assert received["text"] == "tin nhan luc bob offline"
            assert received["offline"] is True

            # Alice's message status also updates to delivered once bob ACKs
            for _ in range(30):
                await asyncio.sleep(0.05)
                msg = next(m for m in alice.messages if m.get("msg_id") == msg_id_1)
                if msg.get("status") == "delivered":
                    break
            assert next(m for m in alice.messages if m.get("msg_id") == msg_id_1)["status"] == "delivered"

            live_id = await alice.send_text("bob", "tin nhan live")
            for _ in range(40):
                await asyncio.sleep(0.05)
                if any(m.get("msg_id") == live_id for m in bob.messages):
                    break
            live_received = next(m for m in bob.messages if m.get("msg_id") == live_id)
            assert live_received["text"] == "tin nhan live"
            assert live_received["offline"] is False
            for _ in range(30):
                await asyncio.sleep(0.05)
                live_status = next(
                    m for m in alice.messages if m.get("msg_id") == live_id
                )["status"]
                if live_status == "delivered":
                    break
            assert live_status == "delivered"

            broadcast_id = await alice.send_text("*", "broadcast live va offline")
            for _ in range(30):
                await asyncio.sleep(0.05)
                broadcast_status = next(
                    m for m in alice.messages if m.get("msg_id") == broadcast_id
                )["status"]
                if "queued:1" in str(broadcast_status):
                    break
            assert broadcast_status == "broadcast:1;queued:1"
            assert any(m.get("msg_id") == broadcast_id for m in bob.messages)

            charlie = ClientCore(make_client_cfg())
            await charlie.login("charlie", "password123")
            for _ in range(40):
                await asyncio.sleep(0.05)
                if any(m.get("msg_id") == broadcast_id for m in charlie.messages):
                    break
            charlie_received = next(
                m for m in charlie.messages if m.get("msg_id") == broadcast_id
            )
            assert charlie_received["text"] == "broadcast live va offline"
            assert charlie_received["offline"] is True
            await charlie.logout()

            # Bob switches to offline mode and queues a message in local Outbox
            await bob.disconnect()
            assert bob.get_state()["logged_in"] is True
            assert bob.get_state()["online"] is False

            msg_id_2 = await bob.send_text("alice", "tin nhan tu outbox cua bob")
            outbox_msg = next(m for m in bob.messages if m.get("msg_id") == msg_id_2)
            assert outbox_msg["status"] == "queued_local"

            # Bob reconnects -> outbox flushes automatically to alice
            await bob.reconnect()
            for _ in range(40):
                await asyncio.sleep(0.05)
                if any(m.get("msg_id") == msg_id_2 for m in alice.messages):
                    break

            alice_received = next(m for m in alice.messages if m.get("msg_id") == msg_id_2)
            assert alice_received["text"] == "tin nhan tu outbox cua bob"

            await alice.logout()
            await bob.logout()
        finally:
            await server.stop()

    asyncio.run(scenario())

