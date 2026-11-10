"""Tests for f1sa.ingest.ratelimit using a fake clock (no real sleeping)."""

from __future__ import annotations

import threading

import pytest
from _fakes import FakeClock

from f1sa.ingest.ratelimit import JOLPICA_BURST_PER_S, DualRateLimiter, TokenBucket


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def make(clock: FakeClock, burst: int = 4, per_hour: int = 450) -> DualRateLimiter:
    return DualRateLimiter(burst_per_s=burst, per_hour=per_hour, clock=clock, sleep=clock.sleep)


def test_default_burst_matches_jolpica() -> None:
    assert JOLPICA_BURST_PER_S == 4


def test_token_bucket_refill_and_cap(clock: FakeClock) -> None:
    bucket = TokenBucket(capacity=4, refill_per_s=2, clock=clock)
    for _ in range(4):
        bucket.take()
    assert bucket.tokens == pytest.approx(0)
    assert bucket.time_until_available() == pytest.approx(0.5)
    clock.advance(1.0)
    assert bucket.tokens == pytest.approx(2)
    clock.advance(100)
    assert bucket.tokens == pytest.approx(4)  # capped at capacity


def test_token_bucket_take_without_tokens_raises(clock: FakeClock) -> None:
    bucket = TokenBucket(capacity=1, refill_per_s=1, clock=clock)
    bucket.take()
    with pytest.raises(RuntimeError):
        bucket.take()


@pytest.mark.parametrize(("capacity", "refill"), [(0, 1), (1, 0), (1, -1)])
def test_token_bucket_invalid(clock: FakeClock, capacity: float, refill: float) -> None:
    with pytest.raises(ValueError):
        TokenBucket(capacity, refill, clock)


def test_burst_of_four_then_waits(clock: FakeClock) -> None:
    limiter = make(clock)
    waits = [limiter.acquire() for _ in range(4)]
    assert waits == [0.0] * 4
    assert clock.sleeps == []
    assert limiter.acquire() == pytest.approx(0.25)  # 4/s refill -> one token per 0.25 s
    assert clock.sleeps == [pytest.approx(0.25)]


def test_try_acquire_does_not_block(clock: FakeClock) -> None:
    limiter = make(clock, burst=2)
    assert limiter.try_acquire() and limiter.try_acquire()
    assert limiter.try_acquire() is False
    assert limiter.wait_time() == pytest.approx(0.5)
    clock.advance(0.5)
    assert limiter.try_acquire() is True


def test_hourly_budget_exhaustion(clock: FakeClock) -> None:
    limiter = make(clock, burst=4, per_hour=10)
    start = clock.now
    for _ in range(10):
        limiter.acquire()  # sleeps only for the burst bucket (6 x 0.25 s)
    elapsed = clock.now - start
    assert elapsed == pytest.approx(1.5)
    # hourly bucket empty: one token per 360 s, minus what refilled during the burst sleeps
    expected = 360.0 - elapsed
    assert limiter.wait_time() == pytest.approx(expected, rel=1e-6)
    assert limiter.acquire() == pytest.approx(expected, rel=1e-6)
    assert limiter.wait_time() == pytest.approx(360.0, rel=1e-6)


def test_sustained_rate_over_an_hour(clock: FakeClock) -> None:
    limiter = make(clock, burst=4, per_hour=450)
    start = clock.now
    for _ in range(450 + 90):
        limiter.acquire()
    elapsed = clock.now - start
    # 450 from the full bucket (burst-paced), the extra 90 at 450/h = 8 s each
    assert elapsed == pytest.approx(90 * 8.0, rel=0.02)


@pytest.mark.parametrize(("burst", "per_hour"), [(0, 450), (4, 0), (4, 501)])
def test_invalid_limiter_args(clock: FakeClock, burst: int, per_hour: int) -> None:
    with pytest.raises(ValueError):
        make(clock, burst=burst, per_hour=per_hour)


def test_thread_safety_never_over_grants(clock: FakeClock) -> None:
    limiter = make(clock, burst=4, per_hour=50)
    granted: list[int] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker() -> None:
        barrier.wait()
        for _ in range(100):
            if limiter.try_acquire():
                with lock:
                    granted.append(1)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # clock never moves: exactly the burst capacity can be granted, never more
    assert len(granted) == 4


def test_concurrent_acquire_respects_hourly_total(clock: FakeClock) -> None:
    limiter = make(clock, burst=4, per_hour=20)
    count = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal count
        for _ in range(5):
            limiter.acquire()
            with lock:
                count += 1

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert count == 20
    assert limiter.hourly.tokens < 1.0 + 1e-6
