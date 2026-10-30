from __future__ import annotations

import argparse
import asyncio
import random


class ProxyState:
    def __init__(self, drop: float) -> None:
        self.drop = drop
        self.client_addr: tuple | None = None
        self.client_transport: asyncio.DatagramTransport | None = None
        self.server_transport: asyncio.DatagramTransport | None = None

    def drop_packet(self) -> bool:
        return random.random() < self.drop


class ClientProtocol(asyncio.DatagramProtocol):
    def __init__(self, state: ProxyState) -> None:
        self.state = state

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.state.client_transport = transport

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        self.state.client_addr = addr
        if self.state.drop_packet() or self.state.server_transport is None:
            return
        self.state.server_transport.sendto(data)


class ServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, state: ProxyState) -> None:
        self.state = state

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.state.server_transport = transport

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        if self.state.drop_packet() or self.state.client_transport is None:
            return
        if self.state.client_addr is not None:
            self.state.client_transport.sendto(data, self.state.client_addr)


async def run(args: argparse.Namespace) -> None:
    state = ProxyState(args.drop)
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(
        lambda: ClientProtocol(state),
        local_addr=("127.0.0.1", args.listen_port),
    )
    await loop.create_datagram_endpoint(
        lambda: ServerProtocol(state),
        remote_addr=(args.server_host, args.server_port),
    )
    print(
        f"lossy udp proxy 127.0.0.1:{args.listen_port} -> "
        f"{args.server_host}:{args.server_port} drop={args.drop:.0%} (single client)"
    )
    await asyncio.Event().wait()


def main() -> None:
    parser = argparse.ArgumentParser(description="Single-client lossy UDP proxy")
    parser.add_argument("--listen-port", type=int, default=9101)
    parser.add_argument("--server-host", default="127.0.0.1")
    parser.add_argument("--server-port", type=int, default=9001)
    parser.add_argument("--drop", type=float, default=0.2)
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
