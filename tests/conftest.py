"""Offline test run: socket connects are blocked except loopback (127.0.0.1 and ::1)."""
import socket
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

_real_connect = socket.socket.connect
_real_connect_ex = socket.socket.connect_ex
LOOPBACK = {"127.0.0.1", "::1"}


def pytest_configure(config):
    config.addinivalue_line("markers", "loopback: the test talks to an in-process fake server")


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    def check(sock, address):
        if sock.family == getattr(socket, "AF_UNIX", None):
            return
        host = address[0] if isinstance(address, tuple) else address
        # Loopback never leaves the machine. On Windows asyncio and socket.socketpair()
        # connect to 127.0.0.1 internally, so blocking it would break every async test.
        if host in LOOPBACK:
            return
        raise ConnectionRefusedError(f"network blocked in tests: {host}")

    def connect(sock, address):
        check(sock, address)
        return _real_connect(sock, address)

    def connect_ex(sock, address):
        check(sock, address)
        return _real_connect_ex(sock, address)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)


class FakeTime:
    """An injectable clock whose sleep() advances virtual time instantly."""

    def __init__(self, start: float = 1000.0):
        self.t = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        import asyncio

        self.sleeps.append(seconds)
        self.t += max(0.0, seconds)
        await asyncio.sleep(0)


@pytest.fixture
def fake_time():
    return FakeTime()
