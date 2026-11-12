from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field

from common.congestion import CongestionController, make_controller
from common.constants import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RTO_MAX,
    DEFAULT_RTO_MIN,
    DEFAULT_WINDOW,
    Flags,
    PacketType,
)
from common.protocol import PacketHeader

SendPacket = Callable[[PacketHeader, bytes], None]
DataCallback = Callable[[int, int, bytes], None]
StreamCallback = Callable[[int], None]

MAX_TRACKED_RECEIVE_STREAMS = 1024


@dataclass
class OutChunk:
    seq: int
    payload: bytes
    flags: int
    sent_at: float | None = None
    retries: int = 0
    acked: bool = False


@dataclass
class OutStream:
    stream_id: int
    chunks: dict[int, OutChunk] = field(default_factory=dict)
    next_seq: int = 0
    send_next: int = 0
    base_seq: int = 0
    final: bool = False
    last_ack: int = -1
    dup_acks: int = 0
    last_activity: float = field(default_factory=time.monotonic)


class ReliableEndpoint:
    def __init__(
        self,
        session_id: int,
        send_packet: SendPacket,
        on_data: DataCallback,
        on_stream_acked: StreamCallback | None = None,
        on_stream_failed: StreamCallback | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        window: int = DEFAULT_WINDOW,
        rto_min: float = DEFAULT_RTO_MIN,
        rto_max: float = DEFAULT_RTO_MAX,
        max_retries: int = DEFAULT_MAX_RETRIES,
        congestion: CongestionController | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.session_id = session_id
        self.send_packet = send_packet
        self.on_data = on_data
        self.on_stream_acked = on_stream_acked
        self.on_stream_failed = on_stream_failed
        self.chunk_size = max(64, chunk_size)
        self.recv_window = max(1, window)
        self.peer_window = max(1, window)
        self.rto_min = rto_min
        self.rto_max = rto_max
        self.max_retries = max_retries
        self.clock = clock
        self.congestion = congestion or make_controller("reno", 10.0, 256.0)

        self.out_streams: dict[int, OutStream] = {}
        self.in_expected: dict[int, int] = {}
        self.in_buffer: dict[int, dict[int, tuple[int, bytes]]] = {}
        self._in_stream_lru: OrderedDict[int, None] = OrderedDict()

        self.srtt: float | None = None
        self.rttvar: float = 0.0
        self.rto = rto_min

    @property
    def send_window(self) -> int:
        return max(1, min(self.peer_window, int(self.congestion.window)))

    def send_message(self, stream_id: int, payload: bytes, flags: int = 0) -> int:
        pieces = [payload[i : i + self.chunk_size] for i in range(0, len(payload), self.chunk_size)]
        if not pieces:
            pieces = [b""]
        stream = OutStream(stream_id=stream_id, last_activity=self.clock())
        for index, piece in enumerate(pieces):
            chunk_flags = flags
            if index == 0:
                chunk_flags |= Flags.FIRST
            if index == len(pieces) - 1:
                chunk_flags |= Flags.LAST
            stream.chunks[stream.next_seq] = OutChunk(
                seq=stream.next_seq,
                payload=piece,
                flags=int(chunk_flags),
            )
            stream.next_seq += 1
        stream.final = True
        self.out_streams[stream_id] = stream
        self._flush(stream)
        return stream_id

    def send_chunk(self, stream_id: int, payload: bytes, flags: int = 0) -> None:
        stream = self.out_streams.get(stream_id)
        if stream is None:
            stream = OutStream(stream_id=stream_id, last_activity=self.clock())
            self.out_streams[stream_id] = stream
        chunk = OutChunk(seq=stream.next_seq, payload=payload, flags=int(flags))
        stream.chunks[stream.next_seq] = chunk
        stream.next_seq += 1
        stream.last_activity = self.clock()
        self._flush(stream)

    def close_stream(self, stream_id: int) -> None:
        stream = self.out_streams.get(stream_id)
        if stream is None:
            return
        stream.final = True
        if stream.base_seq >= stream.next_seq:
            self._complete_stream(stream)

    def outstanding(self, stream_id: int) -> int:
        stream = self.out_streams.get(stream_id)
        if stream is None:
            return 0
        return max(0, stream.send_next - stream.base_seq)

    def has_stream(self, stream_id: int) -> bool:
        return stream_id in self.out_streams

    def _flush(self, stream: OutStream) -> None:
        while stream.send_next - stream.base_seq < self.send_window:
            chunk = stream.chunks.get(stream.send_next)
            if chunk is None:
                break
            if chunk.sent_at is not None:
                break
            if not self._send_data(stream, chunk):
                return
            stream.send_next += 1

    def _send_data(self, stream: OutStream, chunk: OutChunk) -> bool:
        if chunk.retries > self.max_retries:
            self._fail_stream(stream)
            return False
        header = PacketHeader(
            type=int(PacketType.DATA),
            flags=chunk.flags,
            session_id=self.session_id,
            stream_id=stream.stream_id,
            seq=chunk.seq,
            window=self.recv_window,
        )
        try:
            self.send_packet(header, chunk.payload)
        except Exception:
            self._fail_stream(stream)
            return False
        chunk.sent_at = self.clock()
        chunk.retries += 1
        stream.last_activity = chunk.sent_at
        return True

    def _retransmit(self, stream: OutStream, chunk: OutChunk) -> None:
        if chunk.retries > self.max_retries:
            self._fail_stream(stream)
            return
        header = PacketHeader(
            type=int(PacketType.DATA),
            flags=chunk.flags,
            session_id=self.session_id,
            stream_id=stream.stream_id,
            seq=chunk.seq,
            window=self.recv_window,
        )
        try:
            self.send_packet(header, chunk.payload)
        except Exception:
            self._fail_stream(stream)
            return
        chunk.sent_at = self.clock()
        chunk.retries += 1

    def handle_packet(self, header: PacketHeader, payload: bytes) -> None:
        packet_type = header.type
        if packet_type == PacketType.DATA:
            self._handle_data(header, payload)
        elif packet_type == PacketType.ACK:
            self._handle_ack(header)
        elif packet_type == PacketType.NACK:
            self._handle_nack(header, payload)

    def _handle_data(self, header: PacketHeader, payload: bytes) -> None:
        stream_id = header.stream_id
        seq = header.seq
        self._touch_receive_stream(stream_id)
        expected = self.in_expected.setdefault(stream_id, 0)
        buffer = self.in_buffer.setdefault(stream_id, {})

        if seq < expected:
            self._send_ack(stream_id, expected)
            self._drop_empty_receive_buffer(stream_id)
            return
        if seq > expected + self.recv_window:
            self._send_ack(stream_id, expected)
            self._drop_empty_receive_buffer(stream_id)
            return

        buffer[seq] = (header.flags, payload)
        while expected in buffer:
            flags, data = buffer.pop(expected)
            expected += 1
            self.in_expected[stream_id] = expected
            self.on_data(stream_id, flags, data)

        self.in_expected[stream_id] = expected
        self._send_ack(stream_id, expected)
        self._drop_empty_receive_buffer(stream_id)

    def _touch_receive_stream(self, stream_id: int) -> None:
        self._in_stream_lru[stream_id] = None
        self._in_stream_lru.move_to_end(stream_id)
        while len(self._in_stream_lru) > MAX_TRACKED_RECEIVE_STREAMS:
            evicted, _ = self._in_stream_lru.popitem(last=False)
            self.in_expected.pop(evicted, None)
            self.in_buffer.pop(evicted, None)

    def _drop_empty_receive_buffer(self, stream_id: int) -> None:
        if not self.in_buffer.get(stream_id):
            self.in_buffer.pop(stream_id, None)

    def _send_ack(self, stream_id: int, ack: int) -> None:
        sack = 0
        buffer = self.in_buffer.get(stream_id, {})
        for bit in range(32):
            if ack + 1 + bit in buffer:
                sack |= 1 << bit
        header = PacketHeader(
            type=int(PacketType.ACK),
            session_id=self.session_id,
            stream_id=stream_id,
            ack=ack,
            sack=sack,
            window=max(0, self.recv_window - len(buffer)),
        )
        try:
            self.send_packet(header, b"")
        except Exception:
            pass

    def _handle_ack(self, header: PacketHeader) -> None:
        self.peer_window = max(1, header.window)
        stream = self.out_streams.get(header.stream_id)
        if stream is None:
            return

        newly_acked: list[OutChunk] = []
        for seq in list(stream.chunks):
            chunk = stream.chunks[seq]
            if chunk.acked:
                continue
            in_sack = (
                seq > header.ack
                and seq - header.ack - 1 < 32
                and bool(header.sack & (1 << (seq - header.ack - 1)))
            )
            if seq < header.ack or in_sack:
                chunk.acked = True
                newly_acked.append(chunk)

        now = self.clock()
        for chunk in newly_acked:
            if chunk.retries == 1 and chunk.sent_at is not None:
                self._update_rtt(now - chunk.sent_at)
        if newly_acked:
            self.congestion.on_ack(len(newly_acked))

        while stream.base_seq in stream.chunks and stream.chunks[stream.base_seq].acked:
            del stream.chunks[stream.base_seq]
            stream.base_seq += 1

        if stream.final and stream.base_seq >= stream.next_seq:
            self._complete_stream(stream)
            return

        if header.ack == stream.last_ack and not newly_acked:
            stream.dup_acks += 1
            if stream.dup_acks == 3:
                chunk = stream.chunks.get(stream.base_seq)
                if chunk is not None:
                    self._retransmit(stream, chunk)
                    self.congestion.on_loss()
                stream.dup_acks = 0
        else:
            stream.dup_acks = 0
        stream.last_ack = header.ack
        self._flush(stream)

    def _handle_nack(self, header: PacketHeader, payload: bytes) -> None:
        stream = self.out_streams.get(header.stream_id)
        if stream is None or len(payload) < 4:
            return
        import struct

        (bitmap,) = struct.unpack("!I", payload[:4])
        for bit in range(32):
            if bitmap & (1 << bit):
                chunk = stream.chunks.get(header.ack + 1 + bit)
                if chunk is not None and not chunk.acked:
                    self._retransmit(stream, chunk)
        self.congestion.on_loss()
        self._flush(stream)

    def _update_rtt(self, sample: float) -> None:
        if self.srtt is None:
            self.srtt = sample
            self.rttvar = sample / 2.0
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - sample)
            self.srtt = 0.875 * self.srtt + 0.125 * sample
        self.rto = min(self.rto_max, max(self.rto_min, self.srtt + 4.0 * self.rttvar))

    def _complete_stream(self, stream: OutStream) -> None:
        self.out_streams.pop(stream.stream_id, None)
        if self.on_stream_acked is not None:
            self.on_stream_acked(stream.stream_id)

    def _fail_stream(self, stream: OutStream) -> None:
        self.out_streams.pop(stream.stream_id, None)
        if self.on_stream_failed is not None:
            self.on_stream_failed(stream.stream_id)

    def tick(self, now: float | None = None) -> None:
        current = self.clock() if now is None else now
        for stream in list(self.out_streams.values()):
            timed_out = False
            for chunk in list(stream.chunks.values()):
                if chunk.acked or chunk.sent_at is None:
                    continue
                if current - chunk.sent_at >= self.rto:
                    self._retransmit(stream, chunk)
                    timed_out = True
                    if stream.stream_id not in self.out_streams:
                        break
            if timed_out:
                self.congestion.on_timeout()
                self.rto = min(self.rto_max, self.rto * 2.0)
            if stream.stream_id in self.out_streams:
                self._flush(stream)
