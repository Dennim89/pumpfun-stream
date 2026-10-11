import socket

import pytest


def test_sockets_are_blocked():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        with pytest.raises(ConnectionRefusedError, match="network blocked"):
            s.connect(("192.0.2.1", 443))
        with pytest.raises(ConnectionRefusedError, match="network blocked"):
            s.connect(("198.51.100.7", 80))  # any address off this machine
    finally:
        s.close()


@pytest.mark.loopback
def test_loopback_marker_still_blocks_other_hosts():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        with pytest.raises(ConnectionRefusedError, match="network blocked"):
            s.connect(("198.51.100.7", 80))
    finally:
        s.close()
