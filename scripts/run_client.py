from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from client.core import ClientCore
from client.ui_server import UIServer
from common.config import load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a collaboration client")
    parser.add_argument("--config", default=None)
    parser.add_argument("--username", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--server-host", default=None)
    parser.add_argument("--server-tcp-port", type=int, default=None)
    parser.add_argument("--server-udp-port", type=int, default=None)
    parser.add_argument("--ui-host", default=None)
    parser.add_argument("--ui-port", type=int, default=None)
    parser.add_argument("--download-dir", default=None)
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    if args.server_host:
        config.client.server_host = args.server_host
    if args.server_tcp_port:
        config.client.server_tcp_port = args.server_tcp_port
    if args.server_udp_port:
        config.client.server_udp_port = args.server_udp_port
    if args.ui_host:
        config.client.ui_host = args.ui_host
    if args.ui_port:
        config.client.ui_port = args.ui_port
    if args.download_dir:
        config.client.download_dir = args.download_dir

    core = ClientCore(config)
    ui = UIServer(core, config.client.ui_host, config.client.ui_port, ROOT / "ui")
    await ui.start()
    print(f"UI: http://{config.client.ui_host}:{config.client.ui_port}")

    if args.username and args.password:
        try:
            await core.login(args.username, args.password)
            print(f"logged in as {args.username}")
        except Exception as exc:
            print(f"login failed: {exc}")
    try:
        await asyncio.Event().wait()
    finally:
        await core.logout()
        await ui.close()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
