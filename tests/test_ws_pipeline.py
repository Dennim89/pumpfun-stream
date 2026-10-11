"""WebSocket source and live pipeline against fake servers on 127.0.0.1."""
import asyncio
import io
import json
import random
from http import HTTPStatus

import pytest
from conftest import FakeTime
from encoder import (cpi_instruction, encode_event, gen_event, gen_pubkey, invoke_logs, logs_notification,
                     program_data_line, self_cpi_logs, synthetic_signature, tx_result)
from synthetic_idl import IDL, PROGRAM_ID
from websockets.asyncio.server import serve

from pumpfun_stream.backoff import Backoff, RateLimiter
from pumpfun_stream.decoder import Decoder
from pumpfun_stream.idl import load_idl
from pumpfun_stream.pipeline import Pipeline
from pumpfun_stream.rpc import HttpRpc
from pumpfun_stream.sinks import JsonlSink
from pumpfun_stream.sources import LogsItem
from pumpfun_stream.ws import WsLogsSource

pytestmark = pytest.mark.loopback


class FakeRpcServer:
    """Answers logsSubscribe, sends the queued notifications, then drops the connection."""

    def __init__(self, notifications_per_connection, reject_first=0, retry_after="5", silent=False):
        self.batches = list(notifications_per_connection)
        self.reject_first = reject_first
        self.retry_after = retry_after
        self.silent = silent
        self.connections = 0
        self.subscribe_requests = []

    def process_request(self, connection, request):
        if self.reject_first > 0:
            self.reject_first -= 1
            response = connection.respond(HTTPStatus.TOO_MANY_REQUESTS, "slow down\n")
            response.headers["Retry-After"] = self.retry_after
            return response
        return None

    async def handler(self, ws):
        self.connections += 1
        msg = json.loads(await ws.recv())
        self.subscribe_requests.append(msg)
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": 7}))
        if self.silent:
            await ws.wait_closed()  # say nothing until the client gives up
            return
        batch = self.batches.pop(0) if self.batches else []
        for note in batch:
            await ws.send(json.dumps(note))
        await asyncio.sleep(0.02)  # let the client read them (older websockets drop unread frames)
        ws.transport.close()  # TCP close without a close frame, as when a provider cuts the connection

    def serve(self):
        return serve(self.handler, "127.0.0.1", 0, process_request=self.process_request)


def url_of(server):
    host, port = server.sockets[0].getsockname()[:2]
    return f"ws://{host}:{port}"


def notes(n, rng):
    out = []
    for _ in range(n):
        value = gen_event(IDL, "Small", rng)
        logs = invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", value))])
        out.append(logs_notification(synthetic_signature(rng), 50, logs, sub=7))
    return out


def test_reconnects_with_backoff_after_drops():
    fake = FakeTime()
    rng = random.Random(1)
    srv = FakeRpcServer([notes(1, rng), notes(1, rng), notes(1, rng)])

    async def go():
        async with srv.serve() as server:
            src = WsLogsSource(url_of(server), {"synthetic": PROGRAM_ID}, sleep=fake.sleep, clock=fake,
                               backoff=Backoff(rng=random.Random(0), clock=fake), max_connects=3)
            items = [item async for item in src.items()]
            return src, items

    src, items = asyncio.run(go())
    assert len(items) == 3 and all(isinstance(i, LogsItem) for i in items)
    assert srv.connections == 3 and src.counts["reconnects"] == 2
    assert srv.subscribe_requests[0]["method"] == "logsSubscribe"
    assert srv.subscribe_requests[0]["params"] == [{"mentions": [PROGRAM_ID]}, {"commitment": "confirmed"}]
    assert len(src.delays) == 2
    assert 0 <= src.delays[0] <= 0.5 and 0 <= src.delays[1] <= 1.0  # attempts 0 and 1
    assert src.backoff.attempt == 2


def test_backoff_resets_after_a_stable_minute():
    fake = FakeTime()
    rng = random.Random(2)
    srv = FakeRpcServer([notes(1, rng), notes(1, rng), notes(1, rng)])

    async def go():
        async with srv.serve() as server:
            src = WsLogsSource(url_of(server), {"synthetic": PROGRAM_ID}, sleep=fake.sleep, clock=fake,
                               backoff=Backoff(rng=random.Random(0), clock=fake), max_connects=3)
            async for _ in src.items():
                fake.t += 61  # this connection stayed up for more than a minute
            return src

    src = asyncio.run(go())
    assert src.backoff.attempt == 0  # each connection was stable, so every drop reset the counter
    assert len(src.delays) == 2
    assert all(d <= 0.5 for d in src.delays)


def test_handshake_429_honours_retry_after():
    fake = FakeTime()
    srv = FakeRpcServer([notes(1, random.Random(3))], reject_first=1, retry_after="5")

    async def go():
        async with srv.serve() as server:
            src = WsLogsSource(url_of(server), {"synthetic": PROGRAM_ID}, sleep=fake.sleep, clock=fake,
                               backoff=Backoff(rng=random.Random(0), clock=fake), max_connects=2)
            items = [item async for item in src.items()]
            return src, items

    src, items = asyncio.run(go())
    assert len(items) == 1
    assert src.counts["http_429"] == 1
    assert src.delays[0] >= 5.0


def test_idle_connection_is_recycled():
    fake = FakeTime()
    srv = FakeRpcServer([], silent=True)

    async def go():
        async with srv.serve() as server:
            src = WsLogsSource(url_of(server), {"synthetic": PROGRAM_ID}, idle_timeout=0.05, sleep=fake.sleep,
                               clock=fake, backoff=Backoff(rng=random.Random(0), clock=fake), max_connects=2)
            return src, [item async for item in src.items()]

    src, items = asyncio.run(go())
    assert items == [] and src.counts["idle_timeouts"] == 2


def test_live_pipeline_fetches_emit_cpi_transactions():
    fake = FakeTime()
    rng = random.Random(4)
    v_log, v_cpi, v_plain = (gen_event(IDL, "Small", rng) for _ in range(3))
    sig_cpi, sig_plain = synthetic_signature(rng), synthetic_signature(rng)
    logs_cpi = invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", v_log))] + self_cpi_logs(PROGRAM_ID))
    logs_plain = invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", v_plain))])
    tx = tx_result(sig_cpi, [gen_pubkey(rng), PROGRAM_ID], rng, 60, logs_cpi,
                   [cpi_instruction(1, encode_event(IDL, "Small", v_cpi))], block_time=1767225600)
    srv = FakeRpcServer([[logs_notification(sig_cpi, 60, logs_cpi, sub=7),
                          logs_notification(sig_cpi, 60, logs_cpi, sub=7),  # duplicate notification
                          logs_notification(sig_plain, 61, logs_plain, sub=7)]])
    requested = []

    async def transport(url, body, timeout):
        req = json.loads(body)
        requested.append(req["params"][0])
        return 200, {}, json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": tx}).encode()

    out = io.StringIO()

    async def go():
        async with srv.serve() as server:
            src = WsLogsSource(url_of(server), {"synthetic": PROGRAM_ID}, sleep=fake.sleep, clock=fake,
                               backoff=Backoff(rng=random.Random(0), clock=fake), max_connects=1)
            rpc = HttpRpc("https://rpc.example.invalid", RateLimiter(10, clock=fake, sleep=fake.sleep),
                          transport=transport, sleep=fake.sleep)
            pipe = Pipeline(Decoder({"synthetic": load_idl(IDL)}), [JsonlSink("-", stream=out)], rpc=rpc,
                            status_interval=0, wall_clock=lambda: 1767225600.0, clock=fake, sleep=fake.sleep)
            return await pipe.run(src)

    counts = asyncio.run(go())
    recs = [json.loads(line) for line in out.getvalue().splitlines()]
    assert requested == [sig_cpi]  # fetched once, and only the transaction that may hold emit_cpi events
    got = sorted((r["signature"], r["event_index"], r["source"], json.dumps(r["fields"])) for r in recs)
    want = sorted([(sig_cpi, 0, "ws", json.dumps(v_log)), (sig_plain, 0, "ws", json.dumps(v_plain)),
                   (sig_cpi, 1, "rpc", json.dumps(v_cpi))])
    assert got == want  # the fetched transaction may land before or after the plain notification
    assert recs[0]["received_at"] == "2026-01-01T00:00:00.000Z"
    assert counts["duplicates"] == 2 and counts["fetched"] == 1


def test_live_queue_is_bounded_and_drops_instead_of_blocking():
    rng = random.Random(5)

    class Burst:
        name, live = "burst", True

        async def items(self):
            for note in notes(50, rng):
                value = note["params"]["result"]["value"]
                yield LogsItem(value["signature"], 1, value["logs"])

    fake = FakeTime()
    out = io.StringIO()
    pipe = Pipeline(Decoder({"synthetic": load_idl(IDL)}), [JsonlSink("-", stream=out)], queue_size=5,
                    status_interval=0, clock=fake, sleep=fake.sleep)
    counts = asyncio.run(pipe.run(Burst()))
    assert counts["dropped"] > 0
    assert counts["events"] + counts["dropped"] == 50


def test_status_line_reports_queue_and_rates():
    fake = FakeTime()
    status = io.StringIO()
    pipe = Pipeline(Decoder({"synthetic": load_idl(IDL)}), [], status_interval=10, status_stream=status,
                    clock=fake, sleep=fake.sleep)

    class Slow:
        name, live = "slow", False
        counts = {"reconnects": 3}

        async def items(self):
            rng = random.Random(6)
            for note in notes(4, rng):
                value = note["params"]["result"]["value"]
                yield LogsItem(value["signature"], 1, value["logs"])
                await fake.sleep(6)

    asyncio.run(pipe.run(Slow()))
    lines = status.getvalue().splitlines()
    assert lines and lines[0].startswith("[status] events=")
    assert "reconnects=3" in lines[0] and "queue=0/10000" in lines[0]


def test_failed_fetch_is_counted_and_the_pipeline_finishes():
    fake = FakeTime()
    rng = random.Random(7)
    logs = invoke_logs(PROGRAM_ID, [program_data_line(encode_event(IDL, "Small", gen_event(IDL, "Small", rng)))]
                       + self_cpi_logs(PROGRAM_ID))
    sig = synthetic_signature(rng)

    class One:
        name, live = "one", True

        async def items(self):
            yield LogsItem(sig, 1, logs)

    async def transport(url, body, timeout):
        return 404, {}, b""

    rpc = HttpRpc("https://rpc.example.invalid", RateLimiter(0), transport=transport, sleep=fake.sleep)
    status = io.StringIO()
    pipe = Pipeline(Decoder({"synthetic": load_idl(IDL)}), [], rpc=rpc, status_interval=0, status_stream=status,
                    clock=fake, sleep=fake.sleep)
    counts = asyncio.run(pipe.run(One()))
    assert counts["events"] == 1 and counts["fetch_errors"] == 1
    assert "getTransaction failed" in status.getvalue()
