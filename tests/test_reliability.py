from __future__ import annotations

from common.constants import Flags, PacketType
from common.reliability import ReliableEndpoint


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Pair:
    def __init__(self, clock: Clock, drop=None) -> None:
        self.clock = clock
        self.drop = drop
        self.a_to_b: list = []
        self.b_to_a: list = []
        self.received: list[tuple[int, int, bytes]] = []
        self.a = ReliableEndpoint(
            session_id=1,
            send_packet=self._send_a,
            on_data=lambda stream_id, flags, data: None,
            chunk_size=8,
            window=8,
            rto_min=0.1,
            rto_max=1.0,
            clock=clock,
        )
        self.b = ReliableEndpoint(
            session_id=2,
            send_packet=self._send_b,
            on_data=lambda stream_id, flags, data: self.received.append(
                (stream_id, flags, data)
            ),
            chunk_size=8,
            window=8,
            rto_min=0.1,
            rto_max=1.0,
            clock=clock,
        )
        self.dropped = False

    def _send_a(self, header, payload) -> None:
        if (
            self.drop is not None
            and not self.dropped
            and header.type == PacketType.DATA
            and self.drop(header)
        ):
            self.dropped = True
            return
        self.a_to_b.append((header, payload))

    def _send_b(self, header, payload) -> None:
        self.b_to_a.append((header, payload))

    def pump(self, limit: int = 200) -> None:
        for _ in range(limit):
            progress = False
            while self.a_to_b:
                header, payload = self.a_to_b.pop(0)
                self.b.handle_packet(header, payload)
                progress = True
            while self.b_to_a:
                header, payload = self.b_to_a.pop(0)
                self.a.handle_packet(header, payload)
                progress = True
            if not progress:
                return


def received_bytes(pair: Pair, stream_id: int) -> bytes:
    return b"".join(
        payload for sid, _flags, payload in pair.received if sid == stream_id
    )


def test_lossless_message_delivery() -> None:
    pair = Pair(Clock())
    pair.a.send_message(10, b"hello world")
    pair.pump()
    assert received_bytes(pair, 10) == b"hello world"


def test_retransmit_after_loss() -> None:
    clock = Clock()
    pair = Pair(clock, drop=lambda header: header.stream_id == 10 and header.seq == 0)
    pair.a.send_message(10, b"hello world")
    pair.pump()
    assert received_bytes(pair, 10) != b"hello world"
    clock.advance(0.5)
    pair.a.tick()
    pair.pump()
    assert received_bytes(pair, 10) == b"hello world"


def test_streams_do_not_block_each_other() -> None:
    clock = Clock()
    pair = Pair(clock, drop=lambda header: header.stream_id == 10 and header.seq == 0)
    pair.a.send_message(10, b"aaaaaaaaaaaaaaaa")
    pair.a.send_message(20, b"bbbbbbbbbbbbbbbb")
    pair.pump()
    assert received_bytes(pair, 20) == b"bbbbbbbbbbbbbbbb"
    assert received_bytes(pair, 10) != b"aaaaaaaaaaaaaaaa"
    clock.advance(0.5)
    pair.a.tick()
    pair.pump()
    assert received_bytes(pair, 10) == b"aaaaaaaaaaaaaaaa"


def test_file_stream_meta_chunks_end() -> None:
    pair = Pair(Clock())
    stream_id = 99
    pair.a.send_chunk(stream_id, b"meta", flags=Flags.FIRST)
    pair.a.send_chunk(stream_id, b"chunk-1", flags=0)
    pair.a.send_chunk(stream_id, b"chunk-2", flags=0)
    pair.a.send_chunk(stream_id, b"end", flags=Flags.LAST)
    pair.pump()
    assert received_bytes(pair, stream_id) == b"metachunk-1chunk-2end"
