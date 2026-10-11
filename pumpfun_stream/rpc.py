"""A small JSON-RPC over HTTP client with rate limiting, 429 handling and retries."""
from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from collections import Counter
from typing import Any, Awaitable, Callable, Mapping

from .backoff import Backoff, RateLimiter, Sleep, parse_retry_after

Transport = Callable[[str, bytes, float], Awaitable[tuple[int, Mapping[str, str], bytes]]]


async def urllib_transport(url: str, body: bytes, timeout: float) -> tuple[int, Mapping[str, str], bytes]:
    """POST with the standard library, in a worker thread."""

    def post():
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, dict(resp.headers.items()), resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers.items()) if exc.headers else {}, exc.read() or b""

    return await asyncio.to_thread(post)


class RpcError(RuntimeError):
    pass


class HttpRpc:
    def __init__(self, url: str, limiter: RateLimiter, transport: Transport = urllib_transport,
                 sleep: Sleep = asyncio.sleep, backoff: Callable[[], Backoff] = Backoff,
                 max_retries: int = 5, timeout: float = 30.0, max_tx_version: int = 1):
        self.url = url
        self.max_tx_version = max_tx_version
        self.limiter = limiter
        self.transport = transport
        self.sleep = sleep
        self.new_backoff = backoff
        self.max_retries = max_retries
        self.timeout = timeout
        self.counts: Counter = Counter()
        self._id = 0

    async def call(self, method: str, params: list[Any]) -> Any:
        backoff = self.new_backoff()
        last = "no attempt"
        for _ in range(self.max_retries + 1):
            await self.limiter.acquire()
            self._id += 1
            body = json.dumps({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}).encode()
            self.counts["requests"] += 1
            try:
                status, headers, data = await self.transport(self.url, body, self.timeout)
            except (OSError, asyncio.TimeoutError) as exc:
                self.counts["network_errors"] += 1
                last = f"network error: {exc.__class__.__name__}"
                await self.sleep(backoff.next_delay())
                continue
            lower = {k.lower(): v for k, v in headers.items()}
            if status == 429:
                self.counts["http_429"] += 1
                retry_after = parse_retry_after(lower.get("retry-after"))
                delay = backoff.next_delay(retry_after)
                self.limiter.penalize(delay)
                last = "HTTP 429"
                await self.sleep(delay)
                continue
            if status >= 500:
                self.counts["http_5xx"] += 1
                last = f"HTTP {status}"
                await self.sleep(backoff.next_delay(parse_retry_after(lower.get("retry-after"))))
                continue
            if status != 200:
                raise RpcError(f"{method}: HTTP {status}")
            try:
                doc = json.loads(data)
            except ValueError:
                raise RpcError(f"{method}: response is not JSON") from None
            err = doc.get("error")
            if err:
                if isinstance(err, Mapping) and err.get("code") in (429, -32429):
                    self.counts["rpc_rate_limited"] += 1
                    last = "JSON-RPC rate limit"
                    await self.sleep(backoff.next_delay())
                    continue
                raise RpcError(f"{method}: {err}")
            return doc.get("result")
        raise RpcError(f"{method}: gave up after {self.max_retries + 1} attempts ({last})")

    async def get_transaction(self, signature: str, commitment: str = "confirmed") -> Any:
        # JSON encoding: the node lists the account keys itself, so new transaction versions
        # (mainnet carries version 1 since 2026) need no wire-format parser here.
        return await self.call("getTransaction", [signature, {
            "encoding": "json",
            "maxSupportedTransactionVersion": self.max_tx_version,
            "commitment": commitment,
        }])
