import asyncio
import random
from email.utils import formatdate

from conftest import FakeTime

from pumpfun_stream.backoff import MAX_RETRY_AFTER, Backoff, RateLimiter, parse_retry_after


def test_full_jitter_stays_under_the_exponential_ceiling():
    b = Backoff(base=0.5, cap=60, rng=random.Random(1))
    ceilings = []
    for _ in range(12):
        ceiling = b.ceiling()
        ceilings.append(ceiling)
        assert 0 <= b.next_delay() <= ceiling
    assert ceilings[:8] == [0.5, 1, 2, 4, 8, 16, 32, 60]
    assert ceilings[-1] == 60


def test_jitter_is_deterministic_with_a_seed():
    a = [Backoff(rng=random.Random(9)).next_delay() for _ in range(3)]
    b = [Backoff(rng=random.Random(9)).next_delay() for _ in range(3)]
    assert a == b


def test_backoff_resets_only_after_a_stable_minute():
    clock = FakeTime()
    b = Backoff(rng=random.Random(2), clock=clock)
    for _ in range(4):
        b.next_delay()
    b.connected()
    clock.t += 59
    b.disconnected()
    assert b.attempt == 4  # not stable long enough
    b.connected()
    clock.t += 60
    b.disconnected()
    assert b.attempt == 0


def test_retry_after_wins_over_a_shorter_jitter():
    b = Backoff(rng=random.Random(3))
    assert b.next_delay(retry_after=7.0) >= 7.0
    assert b.next_delay(retry_after=10_000) <= max(MAX_RETRY_AFTER, 60)


def test_parse_retry_after():
    assert parse_retry_after("3") == 3.0
    assert parse_retry_after(" 1.5 ") == 1.5
    assert parse_retry_after(None) is None
    assert parse_retry_after("soon") is None
    now = 1_800_000_000.0
    assert parse_retry_after(formatdate(now + 30, usegmt=True), now=now) == 30.0
    assert parse_retry_after(formatdate(now - 30, usegmt=True), now=now) == 0.0


def test_rate_limiter_spaces_requests():
    clock = FakeTime()

    async def go():
        lim = RateLimiter(rate=5, burst=1, clock=clock, sleep=clock.sleep)
        stamps = []
        for _ in range(11):
            await lim.acquire()
            stamps.append(clock())
        return stamps

    stamps = asyncio.run(go())
    assert abs((stamps[-1] - stamps[0]) - 2.0) < 1e-9  # 10 gaps of 0.2 s
    assert all(b - a >= 0.2 - 1e-9 for a, b in zip(stamps, stamps[1:]))


def test_rate_limiter_burst_and_penalty():
    clock = FakeTime()

    async def go():
        lim = RateLimiter(rate=10, burst=3, clock=clock, sleep=clock.sleep)
        for _ in range(3):
            await lim.acquire()
        assert clock.sleeps == []  # the burst goes out at once
        lim.penalize(2.0)
        start = clock()
        await lim.acquire()
        return clock() - start

    waited = asyncio.run(go())
    assert waited >= 2.0


def test_rate_limit_zero_means_unlimited():
    clock = FakeTime()

    async def go():
        lim = RateLimiter(rate=0, clock=clock, sleep=clock.sleep)
        for _ in range(100):
            await lim.acquire()

    asyncio.run(go())
    assert clock.sleeps == []
