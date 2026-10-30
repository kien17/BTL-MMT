from __future__ import annotations

from common.crypto import (
    derive_session_key,
    generate_keypair,
    hash_password,
    sha256_bytes,
    verify_password,
)


def test_ecdh_shared_key_matches() -> None:
    alice_private, alice_public = generate_keypair()
    bob_private, bob_public = generate_keypair()
    salt = b"s" * 16
    alice_key = derive_session_key(alice_private, bob_public, salt)
    bob_key = derive_session_key(bob_private, alice_public, salt)
    assert alice_key == bob_key
    assert len(alice_key) == 32


def test_password_hash_verify() -> None:
    salt, digest = hash_password("password123")
    assert verify_password("password123", salt, digest)
    assert not verify_password("wrong", salt, digest)


def test_sha256_bytes() -> None:
    assert sha256_bytes(b"abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
