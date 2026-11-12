from __future__ import annotations

import time


class CongestionController:
    def on_ack(self, acked: int) -> None:
        raise NotImplementedError

    def on_loss(self) -> None:
        raise NotImplementedError

    def on_timeout(self) -> None:
        raise NotImplementedError

    @property
    def window(self) -> float:
        raise NotImplementedError


class Reno(CongestionController):
    def __init__(self, initial_window: float = 10.0, max_window: float = 256.0) -> None:
        self.cwnd = initial_window
        self.ssthresh = max_window
        self.max_window = max_window

    def on_ack(self, acked: int) -> None:
        if self.cwnd < self.ssthresh:
            self.cwnd += acked
        else:
            self.cwnd += max(1.0, acked / max(self.cwnd, 1.0))
        self.cwnd = min(self.cwnd, self.max_window)

    def on_loss(self) -> None:
        self.ssthresh = max(self.cwnd / 2.0, 2.0)
        self.cwnd = self.ssthresh

    def on_timeout(self) -> None:
        self.ssthresh = max(self.cwnd / 2.0, 2.0)
        self.cwnd = 1.0

    @property
    def window(self) -> float:
        return max(1.0, self.cwnd)


class Cubic(CongestionController):
    def __init__(self, initial_window: float = 10.0, max_window: float = 256.0) -> None:
        self.cwnd = initial_window
        self.max_window = max_window
        self._origin = initial_window
        self._epoch = time.monotonic()
        self._c = 0.4
        self._beta = 0.7

    def on_ack(self, acked: int) -> None:
        elapsed = max(0.0, time.monotonic() - self._epoch)
        target = self._origin + self._c * (elapsed**3)
        self.cwnd = min(self.max_window, max(self.cwnd, target))
        self.cwnd = min(self.max_window, self.cwnd + acked / max(self.cwnd, 1.0))

    def on_loss(self) -> None:
        self._origin = self.cwnd * self._beta
        self.cwnd = max(2.0, self._origin)
        self._epoch = time.monotonic()

    def on_timeout(self) -> None:
        self.cwnd = 1.0
        self._origin = 1.0
        self._epoch = time.monotonic()

    @property
    def window(self) -> float:
        return max(1.0, self.cwnd)


class BBR(CongestionController):
    """Lightweight BBR-inspired controller.

    The real BBR keeps a bottleneck-bandwidth estimate and a minimum RTT, then
    derives the congestion window from their product (BDP).  This project only
    has ACK counts at this layer, so ACK arrival time is used as the RTT sample
    and ACKed packets divided by that interval as the delivery-rate sample.
    """

    def __init__(self, initial_window: float = 10.0, max_window: float = 256.0) -> None:
        self.cwnd = max(2.0, min(initial_window, max_window))
        self.max_window = max_window
        self._min_rtt: float | None = None
        self._bw = 0.0
        self._last_ack_at: float | None = None
        self._cwnd_gain = 2.0
        self._loss_beta = 0.7

    def on_ack(self, acked: int) -> None:
        now = time.monotonic()
        delivered = max(0, acked)
        if self._last_ack_at is not None:
            interval = max(1e-6, now - self._last_ack_at)
            self._min_rtt = interval if self._min_rtt is None else min(self._min_rtt, interval)
            sample_bw = delivered / interval
            if sample_bw > self._bw:
                self._bw = sample_bw
            bdp = self._bw * max(self._min_rtt or 0.0, 1e-3)
            target = bdp * self._cwnd_gain + 2.0
            self.cwnd = max(self.cwnd, target)
        else:
            self.cwnd += delivered
        self._last_ack_at = now
        self.cwnd = max(2.0, min(self.max_window, self.cwnd))

    def on_loss(self) -> None:
        self._bw *= self._loss_beta
        self.cwnd = max(2.0, self.cwnd * self._loss_beta)

    def on_timeout(self) -> None:
        self._bw *= 0.5
        self.cwnd = max(2.0, self.cwnd * 0.5)
        self._last_ack_at = None

    @property
    def window(self) -> float:
        return max(1.0, self.cwnd)


def make_controller(name: str, initial_window: float, max_window: float) -> CongestionController:
    normalized = name.strip().lower()
    if normalized == "bbr":
        return BBR(initial_window, max_window)
    if normalized == "cubic":
        return Cubic(initial_window, max_window)
    return Reno(initial_window, max_window)
