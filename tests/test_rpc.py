import asyncio
import json
import random

import pytest

from pumpfun_stream.backoff import Backoff, RateLimiter
from pumpfun_stream.rpc import HttpRpc, RpcError, urllib_transport


class FakeTransport:
    def __init__(self, replies):
        self.replies = list(replies)
        self.bodies = []

    async def __call__(self, url, body, timeout):
        self.bodies.append(json.loads(body))
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


def ok(result):
    return 200, {"Content-Type": "application/json"}, json.dumps({"jsonrpc": "2.0", "id": 1, "result": result}).encode()


def make(replies, clock, rate=0.0, max_retries=5):
    t = FakeTransport(replies)
    rpc = HttpRpc("https://rpc.example.invalid", RateLimiter(rate, clock=clock, sleep=clock.sleep), transport=t,
                  sleep=clock.sleep, backoff=lambda: Backoff(rng=random.Random(0), clock=clock),
                  max_retries=max_retries)
    return rpc, t


def test_get_transaction_request_shape(fake_time):
    rpc, t = make([ok({"slot": 1})], fake_time)
    assert asyncio.run(rpc.get_transaction("sig")) == {"slot": 1}
    body = t.bodies[0]
    assert body["method"] == "getTransaction"
    assert body["params"] == ["sig", {"encoding": "json", "maxSupportedTransactionVersion": 1,
                                      "commitment": "confirmed"}]


def test_max_tx_version_is_configurable(fake_time):
    rpc, t = make([ok(None)], fake_time)
    rpc.max_tx_version = 0
    asyncio.run(rpc.get_transaction("sig", "finalized"))
    assert t.bodies[0]["params"][1] == {"encoding": "json", "maxSupportedTransactionVersion": 0,
                                        "commitment": "finalized"}


def test_wire_parser_refuses_unknown_versions():
    from pumpfun_stream.txparse import TxFormatError, parse_wire
    raw = bytes([1]) + bytes(64) + bytes([0x81]) + bytes([1, 0, 0]) + bytes([1]) + bytes(32)
    with pytest.raises(TxFormatError, match="version 1"):
        parse_wire(raw)
    v0 = raw[:65] + bytes([0x80]) + raw[66:]
    sigs, keys = parse_wire(v0)
    assert len(sigs) == 1 and len(keys) == 1


def test_429_honours_retry_after(fake_time):
    rpc, _ = make([(429, {"retry-after": "4"}, b""), ok(None)], fake_time)
    assert asyncio.run(rpc.call("getSlot", [])) is None
    assert fake_time.sleeps and fake_time.sleeps[0] >= 4.0
    assert rpc.counts["http_429"] == 1


def test_429_also_slows_the_rate_limiter(fake_time):
    rpc, _ = make([(429, {"Retry-After": "3"}, b""), ok(1), ok(2)], fake_time, rate=100)

    async def go():
        await rpc.call("getSlot", [])
        before = fake_time()
        await rpc.call("getSlot", [])
        return fake_time() - before

    assert asyncio.run(go()) < 0.1  # the penalty was already served while waiting out the 429
    assert sum(fake_time.sleeps) >= 3.0


def test_server_errors_and_network_errors_back_off(fake_time):
    rpc, _ = make([(503, {}, b""), ConnectionResetError("reset"), asyncio.TimeoutError(), ok(5)], fake_time)
    assert asyncio.run(rpc.call("getSlot", [])) == 5
    assert len(fake_time.sleeps) == 3
    assert rpc.counts["http_5xx"] == 1 and rpc.counts["network_errors"] == 2


def test_gives_up_after_max_retries(fake_time):
    rpc, t = make([(429, {}, b"")] * 3, fake_time, max_retries=2)
    with pytest.raises(RpcError, match="gave up after 3 attempts"):
        asyncio.run(rpc.call("getSlot", []))
    assert len(t.bodies) == 3


def test_json_rpc_errors(fake_time):
    err = 200, {}, json.dumps({"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "bad"}}).encode()
    limited = 200, {}, json.dumps({"jsonrpc": "2.0", "id": 1, "error": {"code": 429, "message": "slow"}}).encode()
    rpc, _ = make([err], fake_time)
    with pytest.raises(RpcError, match="bad"):
        asyncio.run(rpc.call("getSlot", []))
    rpc, _ = make([limited, ok(3)], fake_time)
    assert asyncio.run(rpc.call("getSlot", [])) == 3
    rpc, _ = make([(404, {}, b"")], fake_time)
    with pytest.raises(RpcError, match="HTTP 404"):
        asyncio.run(rpc.call("getSlot", []))


def test_real_transport_cannot_leave_the_machine():
    with pytest.raises(OSError):
        asyncio.run(urllib_transport("http://192.0.2.1:8899", b"{}", 1.0))
