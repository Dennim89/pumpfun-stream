"""logsSubscribe over a Solana RPC WebSocket, with reconnects.

One subscription per program id (`mentions` accepts a single address).  Works with any
Solana RPC that supports logsSubscribe.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from typing import Any, AsyncIterator, Callable, Mapping

from .backoff import Backoff, Clock, Sleep, parse_retry_after
from .sources import LogsItem


def _default_connect(url: str):
    from websockets.asyncio.client import connect

    return connect(url, max_size=16 * 1024 * 1024, ping_interval=20, ping_timeout=20,
                   open_timeout=15, close_timeout=5)


def _http_status(exc: BaseException) -> tuple[int | None, float | None]:
    """Status code and Retry-After of a rejected WebSocket handshake, if that is what exc is."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None) or getattr(exc, "status_code", None)
    headers = getattr(response, "headers", None)
    retry_after = parse_retry_after(headers.get("Retry-After")) if headers is not None else None
    return status, retry_after


class SubscriptionError(RuntimeError):
    pass


class WsLogsSource:
    name = "ws"
    live = True

    def __init__(self, url: str, programs: Mapping[str, str], commitment: str = "confirmed",
                 idle_timeout: float = 120.0, backoff: Backoff | None = None,
                 connect: Callable[[str], Any] = _default_connect, sleep: Sleep = asyncio.sleep,
                 clock: Clock = time.monotonic, max_connects: int | None = None,
                 log: Callable[[str], None] | None = None):
        self.url = url
        self.programs = dict(programs)  # name -> program id
        self.commitment = commitment
        self.idle_timeout = idle_timeout
        self.backoff = backoff or Backoff(clock=clock)
        self.connect = connect
        self.sleep = sleep
        self.max_connects = max_connects  # for tests: stop after this many connection attempts
        self.log = log or (lambda msg: None)
        self.counts: Counter = Counter()
        self.delays: list[float] = []

    async def _subscribe(self, ws) -> dict[int, str]:
        pending = {}
        for i, (name, address) in enumerate(self.programs.items(), start=1):
            pending[i] = name
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": i, "method": "logsSubscribe",
                "params": [{"mentions": [address]}, {"commitment": self.commitment}],
            }))
        return pending

    async def items(self) -> AsyncIterator[LogsItem]:
        attempts = 0
        while self.max_connects is None or attempts < self.max_connects:
            attempts += 1
            if attempts > 1:
                self.counts["reconnects"] += 1
            retry_after = None
            try:
                async with self.connect(self.url) as ws:
                    self.backoff.connected()
                    self.counts["connects"] += 1
                    pending = await self._subscribe(ws)
                    subs: dict[int, str] = {}
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), self.idle_timeout)
                        msg = json.loads(raw)
                        if "id" in msg and msg["id"] in pending:
                            name = pending.pop(msg["id"])
                            if "error" in msg:
                                raise SubscriptionError(f"logsSubscribe for {name} failed: {msg['error']}")
                            subs[msg["result"]] = name
                            continue
                        if msg.get("method") != "logsNotification":
                            continue
                        params = msg.get("params") or {}
                        result = params.get("result") or {}
                        value = result.get("value") or {}
                        if not isinstance(value.get("signature"), str):
                            continue
                        self.counts["notifications"] += 1
                        yield LogsItem(value["signature"], (result.get("context") or {}).get("slot"),
                                       list(value.get("logs") or []), value.get("err"), None, self.name)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                self.counts["idle_timeouts"] += 1
                self.log(f"no message for {self.idle_timeout:.0f}s, reconnecting")
            except Exception as exc:  # noqa: BLE001 - any failure means reconnect
                status, retry_after = _http_status(exc)
                if status == 429:
                    self.counts["http_429"] += 1
                self.counts["errors"] += 1
                self.log(f"websocket error: {exc.__class__.__name__}: {exc}"[:300])
            finally:
                self.backoff.disconnected()
            if self.max_connects is not None and attempts >= self.max_connects:
                break
            delay = self.backoff.next_delay(retry_after)
            self.delays.append(delay)
            await self.sleep(delay)
