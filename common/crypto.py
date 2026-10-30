from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

SCRYPT_N = 2**14
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_MAXMEM = 64 * 1024 * 1024
NONCE_SIZE = 12
KEY_SIZE = 32


def generate_keypair() -> tuple[X25519PrivateKey, bytes]:
    private_key = X25519PrivateKey.generate()
    public_bytes = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return private_key, public_bytes


def derive_session_key(private_key: X25519PrivateKey, peer_public: bytes, salt: bytes) -> bytes:
    peer = X25519PublicKey.from_public_bytes(peer_public)
    shared = private_key.exchange(peer)
    return HKDF(
        algorithm=hashes.SHA256(),
        length=KEY_SIZE,
        salt=salt,
        info=b"collab-session-v1",
    ).derive(shared)


def encrypt(key: bytes, plaintext: bytes, aad: bytes | None = None) -> bytes:
    nonce = os.urandom(NONCE_SIZE)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, aad)
    return nonce + ciphertext


def decrypt(key: bytes, data: bytes, aad: bytes | None = None) -> bytes:
    if len(data) < NONCE_SIZE + 16:
        raise ValueError("ciphertext too short")
    nonce = data[:NONCE_SIZE]
    return AESGCM(key).decrypt(nonce, data[NONCE_SIZE:], aad)


def hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    salt = salt or os.urandom(16)
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        dklen=32,
        maxmem=SCRYPT_MAXMEM,
    )
    return salt.hex(), digest.hex()


def verify_password(password: str, salt_hex: str, hash_hex: str) -> bool:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        dklen=32,
        maxmem=SCRYPT_MAXMEM,
    )
    return hmac.compare_digest(digest.hex(), hash_hex)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def random_token(length: int = 16) -> str:
    return secrets.token_hex(length)


def random_salt(length: int = 16) -> bytes:
    return os.urandom(length)
