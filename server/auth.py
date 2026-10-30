from __future__ import annotations

from server.storage import Storage


def seed_users(storage: Storage) -> None:
    storage.seed_default_users()


def verify_credentials(storage: Storage, username: str, password: str) -> bool:
    if not username or len(username) > 64:
        return False
    return storage.verify_user(username, password)
