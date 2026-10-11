"""Reconnect backoff, Retry-After parsing and a request-rate limiter.

Clocks, sleeps and randomness are injectable so tests run instantly and deterministically.
"""
from __future__ import annotations

import asyncio
import random
import time
from email.utils import parsedate_to_datetime
from typing import Awaitable, Callable

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]

# Never wait longer than this for a single Retry-After, whatever the server says.
MAX_RETRY_AFTER = 600.0


class Backoff:
    """Exponential backoff with full jitter: delay = uniform(0, min(cap, base * 2**attempt)).

    Call `next_delay()` after a failure.  Call `connected()` when a connection comes up and
    `disconnected()` when it drops: the attempt counter is reset only if the connection
    stayed up for at least `stable_after` seconds.
    """

    def __init__(self, base: float = 0.5, cap: float = 60.0, stable_after: float = 60.0,
                 rng: random.Random | None = None, clock: Clock = time.monotonic):
        self.base = base
        self.cap = cap
        self.stable_after = stable_after
        self.rng = rng or random.Random()
        self.clock = clock
        self.attempt = 0
        self._up_since: float | None = None

    def ceiling(self) -> float:
        return min(self.cap, self.base * (2 ** min(self.attempt, 62)))

    def next_delay(self, retry_after: float | None = None) -> float:
        delay = self.rng.uniform(0, self.ceiling())
        self.attempt += 1
        if retry_after is not None:
            delay = max(delay, min(retry_after, MAX_RETRY_AFTER))
        return delay

    def reset(self) -> None:
        self.attempt = 0

    def connected(self) -> None:
        self._up_since = self.clock()

    def disconnected(self) -> None:
        if self._up_since is not None and self.clock() - self._up_since >= self.stable_after:
            self.reset()
        self._up_since = None


def parse_retry_after(value: str | None, now: float | None = None) -> float | None:
    """Retry-After is either seconds or an HTTP date.  Returns seconds, or None."""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None
    now = time.time() if now is None else now
    return max(0.0, when.timestamp() - now)


class RateLimiter:
    """Token bucket: at most `rate` acquisitions per second on average, bursts up to `burst`.

    rate <= 0 disables the limit.
    """

    def __init__(self, rate: float, burst: float | None = None, clock: Clock = time.monotonic,
                 sleep: Sleep = asyncio.sleep):
        self.rate = rate
        self.burst = max(1.0, burst if burst is not None else rate)
        self.clock = clock
        self.sleep = sleep
        self.tokens = self.burst
        self.last = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self.clock()
        self.tokens = min(self.burst, self.tokens + (now - self.last) * self.rate)
        self.last = now

    async def acquire(self) -> None:
        if self.rate <= 0:
            return
        async with self._lock:
            while True:
                self._refill()
                if self.tokens >= 1 - 1e-9:  # tolerate float rounding
                    self.tokens -= 1
                    return
                await self.sleep(max((1 - self.tokens) / self.rate, 1e-3))

    def penalize(self, seconds: float) -> None:
        """After a 429: drain the bucket so nothing else goes out for `seconds`."""
        if self.rate > 0:
            self._refill()
            self.tokens = min(self.tokens, 0.0) - seconds * self.rate
