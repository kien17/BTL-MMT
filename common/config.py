from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from common.constants import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_RTO_MAX,
    DEFAULT_RTO_MIN,
    DEFAULT_TCP_PORT,
    DEFAULT_UDP_PORT,
    DEFAULT_UI_PORT,
    DEFAULT_WINDOW,
    MAX_FILE_SIZE,
    OFFLINE_MAX_PAYLOAD,
    OFFLINE_MAX_PER_USER,
    OFFLINE_RETENTION_DAYS,
)

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class ServerConfig:
    host: str = "0.0.0.0"
    tcp_port: int = DEFAULT_TCP_PORT
    udp_port: int = DEFAULT_UDP_PORT
    db_path: str = str(ROOT / "server" / "data" / "app.db")
    heartbeat_interval: float = 5.0
    session_timeout: float = 15.0
    offline_retention_days: int = OFFLINE_RETENTION_DAYS
    offline_max_per_user: int = OFFLINE_MAX_PER_USER
    offline_max_payload: int = OFFLINE_MAX_PAYLOAD


@dataclass
class ClientConfig:
    server_host: str = "127.0.0.1"
    server_tcp_port: int = DEFAULT_TCP_PORT
    server_udp_port: int = DEFAULT_UDP_PORT
    ui_host: str = "127.0.0.1"
    ui_port: int = DEFAULT_UI_PORT
    download_dir: str = str(ROOT / "downloads")


@dataclass
class ProtocolConfig:
    chunk_size: int = DEFAULT_CHUNK_SIZE
    window_packets: int = DEFAULT_WINDOW
    max_file_size: int = MAX_FILE_SIZE
    rto_min: float = DEFAULT_RTO_MIN
    rto_max: float = DEFAULT_RTO_MAX
    max_retries: int = 10
    congestion: str = "reno"


@dataclass
class Config:
    server: ServerConfig = field(default_factory=ServerConfig)
    client: ClientConfig = field(default_factory=ClientConfig)
    protocol: ProtocolConfig = field(default_factory=ProtocolConfig)


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name, {})
    return value if isinstance(value, dict) else {}


def _apply(obj: Any, data: dict[str, Any]) -> None:
    for key, value in data.items():
        if hasattr(obj, key):
            setattr(obj, key, value)


def load_config(path: str | Path | None = None) -> Config:
    config = Config()
    config_path = Path(path) if path else ROOT / "config.toml"
    if config_path.exists():
        with open(config_path, "rb") as handle:
            data = tomllib.load(handle)
        _apply(config.server, _section(data, "server"))
        _apply(config.client, _section(data, "client"))
        _apply(config.protocol, _section(data, "protocol"))

    env = os.environ
    config.client.server_host = env.get("SERVER_HOST", config.client.server_host)
    config.client.server_tcp_port = int(env.get("SERVER_TCP_PORT", config.client.server_tcp_port))
    config.client.server_udp_port = int(env.get("SERVER_UDP_PORT", config.client.server_udp_port))
    config.client.ui_host = env.get("UI_HOST", config.client.ui_host)
    config.client.ui_port = int(env.get("UI_PORT", config.client.ui_port))
    config.client.download_dir = env.get("DOWNLOAD_DIR", config.client.download_dir)
    config.server.db_path = env.get("DB_PATH", config.server.db_path)
    config.server.host = env.get("SERVER_BIND_HOST", config.server.host)
    config.server.tcp_port = int(env.get("SERVER_TCP_PORT", config.server.tcp_port))
    config.server.udp_port = int(env.get("SERVER_UDP_PORT", config.server.udp_port))

    for attr in ("db_path", "download_dir"):
        target = config.server if attr == "db_path" else config.client
        value = Path(getattr(target, attr)).expanduser()
        if not value.is_absolute():
            value = ROOT / value
        setattr(target, attr, str(value))
    return config
