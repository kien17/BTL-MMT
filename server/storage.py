from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

from common.constants import DEFAULT_USERS
from common.crypto import hash_password, verify_password


class Storage:
    def __init__(
        self,
        db_path: str,
        offline_retention_days: int = 7,
        offline_max_per_user: int = 500,
        offline_max_payload: int = 64 * 1024,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.offline_retention_days = offline_retention_days
        self.offline_max_per_user = offline_max_per_user
        self.offline_max_payload = offline_max_payload
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    salt TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS offline_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg_id TEXT UNIQUE NOT NULL,
                    sender TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'text',
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    delivered_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_offline_recipient
                    ON offline_messages(recipient, delivered_at, created_at);
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def seed_default_users(self) -> None:
        for username, password in DEFAULT_USERS:
            self.create_user(username, password)

    def create_user(self, username: str, password: str) -> bool:
        salt, password_hash = hash_password(password)
        now = datetime.now(UTC).isoformat()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO users (username, password_hash, salt, created_at) "
                "VALUES (?, ?, ?, ?)",
                (username, password_hash, salt, now),
            )
            return cursor.rowcount > 0

    def verify_user(self, username: str, password: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT password_hash, salt FROM users WHERE username = ?",
                (username,),
            ).fetchone()
        if row is None:
            return False
        return verify_password(password, row["salt"], row["password_hash"])

    def user_exists(self, username: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM users WHERE username = ?",
                (username,),
            ).fetchone()
        return row is not None

    def list_users(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT username FROM users ORDER BY username ASC"
            ).fetchall()
        return [str(row["username"]) for row in rows]

    def pending_counts_by_recipient(self) -> dict[str, int]:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT recipient, COUNT(*) AS total FROM offline_messages "
                "WHERE delivered_at IS NULL AND expires_at > ? "
                "GROUP BY recipient",
                (now,),
            ).fetchall()
        return {str(row["recipient"]): int(row["total"]) for row in rows}

    def count_offline(self, recipient: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS total FROM offline_messages "
                "WHERE recipient = ? AND delivered_at IS NULL",
                (recipient,),
            ).fetchone()
        return int(row["total"]) if row else 0

    def enqueue_offline(
        self,
        sender: str,
        recipient: str,
        msg_id: str,
        payload: str,
        kind: str = "text",
    ) -> bool:
        if len(payload.encode("utf-8")) > self.offline_max_payload:
            return False
        if self.count_offline(recipient) >= self.offline_max_per_user:
            return False
        now = datetime.now(UTC)
        expires = now + timedelta(days=self.offline_retention_days)
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO offline_messages "
                "(msg_id, sender, recipient, kind, payload, created_at, expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (msg_id, sender, recipient, kind, payload, now.isoformat(), expires.isoformat()),
            )
            return cursor.rowcount > 0

    def fetch_offline(self, recipient: str) -> list[dict]:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT msg_id, sender, recipient, kind, payload, created_at "
                "FROM offline_messages "
                "WHERE recipient = ? AND delivered_at IS NULL AND expires_at > ? "
                "ORDER BY created_at ASC LIMIT ?",
                (recipient, now, self.offline_max_per_user),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_delivered(self, msg_id: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE offline_messages SET delivered_at = ? WHERE msg_id = ? AND delivered_at IS NULL",
                (now, msg_id),
            )

    def cleanup_expired(self) -> int:
        now = datetime.now(UTC).isoformat()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                "DELETE FROM offline_messages WHERE expires_at <= ?",
                (now,),
            )
            return cursor.rowcount
