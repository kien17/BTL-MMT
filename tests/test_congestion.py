from __future__ import annotations

from common.congestion import BBR, Cubic, Reno, make_controller


def test_make_controller_supports_reno_cubic_bbr() -> None:
    assert isinstance(make_controller("reno", 10.0, 256.0), Reno)
    assert isinstance(make_controller("cubic", 10.0, 256.0), Cubic)
    assert isinstance(make_controller("bbr", 10.0, 256.0), BBR)
    assert isinstance(make_controller("unknown", 10.0, 256.0), Reno)


def test_controllers_keep_window_in_bounds() -> None:
    for controller in (
        make_controller("reno", 10.0, 64.0),
        make_controller("cubic", 10.0, 64.0),
        make_controller("bbr", 10.0, 64.0),
    ):
        for _ in range(20):
            controller.on_ack(4)
        assert 1.0 <= controller.window <= 64.0
        controller.on_loss()
        assert 1.0 <= controller.window <= 64.0
        controller.on_timeout()
        assert 1.0 <= controller.window <= 64.0
