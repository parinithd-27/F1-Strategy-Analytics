"""Tests for f1sa.ingest.ratelimit using a fake clock (no real sleeping)."""

from __future__ import annotations

import bisect
import threading
from pathlib import Path

import pytest
from _fakes import FakeClock

from f1sa.ingest.ratelimit import (
    JOLPICA_BURST_PER_S,
    DualRateLimiter,
    SlidingWindowLog,
    TokenBucket,
)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def make(
    clock: FakeClock, burst: int = 4, per_hour: int = 450, log_path: Path | None = None
) -> DualRateLimiter:
    return DualRateLimiter(
        burst_per_s=burst,
        per_hour=per_hour,
        clock=clock,
        sleep=clock.sleep,
        wall_clock=clock,
        log_path=log_path,
    )


def max_in_window(times: list[float], window: float) -> int:
    """Largest number of events inside any closed window of length ``window``."""
    ordered = sorted(times)
    return max(
        (bisect.bisect_right(ordered, t + window) - i for i, t in enumerate(ordered)), default=0
    )


def acquire_times(limiter: DualRateLimiter, clock: FakeClock, n: int) -> list[float]:
    times = []
    for _ in range(n):
        limiter.acquire()
        times.append(clock.now)
    return times


def test_default_burst_matches_jolpica() -> None:
    assert JOLPICA_BURST_PER_S == 4


# ---------------------------------------------------------------- the guarantees
def test_never_more_than_burst_in_any_second(clock: FakeClock) -> None:
    times = acquire_times(make(clock, burst=4, per_hour=500), clock, 200)
    assert max_in_window(times, 1.0) <= 4
    # the old bucket (capacity 4 + 4/s refill) would have allowed 8 in one second


def test_never_more_than_per_hour_in_any_rolling_hour(clock: FakeClock) -> None:
    times = acquire_times(make(clock, burst=4, per_hour=50), clock, 180)
    assert max_in_window(times, 3600.0 - 1e-6) <= 50
    # and the budget is actually used: about 50 per hour over 3+ hours
    assert clock.now - times[0] == pytest.approx(3 * 3600.0, rel=0.01)


def test_fresh_limiter_cannot_double_spend_first_hour(clock: FakeClock) -> None:
    times = acquire_times(make(clock, per_hour=450), clock, 900)
    first_hour = [t for t in times if t - times[0] < 3600.0]
    assert len(first_hour) == 450  # a full 450 bucket + 450/h refill would have allowed ~900


def test_persisted_log_shares_budget_across_runs(clock: FakeClock, tmp_path: Path) -> None:
    log = tmp_path / "requests.log"
    acquire_times(make(clock, per_hour=10, log_path=log), clock, 8)
    clock.advance(60)
    second_run = make(clock, per_hour=10, log_path=log)  # e.g. a resumed process
    assert second_run.available_this_hour == 2
    assert second_run.try_acquire()
    clock.advance(1)  # per-second spacing
    assert second_run.try_acquire()
    clock.advance(1)
    assert second_run.try_acquire() is False  # hourly budget (shared with run 1) used up
    assert second_run.wait_time() > 3000  # must wait for the first run's requests to age out


def test_persisted_log_ignores_old_and_torn_lines(clock: FakeClock, tmp_path: Path) -> None:
    log = tmp_path / "requests.log"
    torn = f"{clock.now - 5:.3f}"[:3] + "."  # a kill mid-write leaves e.g. "995."... cut short
    log.write_text(f"{clock.now - 7200:.3f}\n{clock.now - 10:.3f}\ngarbage\n{torn}x")
    limiter = make(clock, per_hour=10, log_path=log)
    assert limiter.available_this_hour == 9  # only the 10-s-old entry counts
    assert log.read_text() == f"{clock.now - 10:.3f}\n"  # compacted


# ---------------------------------------------------------------- building blocks
def test_token_bucket_refill_and_cap(clock: FakeClock) -> None:
    bucket = TokenBucket(capacity=4, refill_per_s=2, clock=clock)
    for _ in range(4):
        bucket.take()
    assert bucket.tokens == pytest.approx(0)
    assert bucket.time_until_available() == pytest.approx(0.5)
    clock.advance(1.0)
    assert bucket.tokens == pytest.approx(2)
    clock.advance(100)
    assert bucket.tokens == pytest.approx(4)


def test_token_bucket_take_without_tokens_raises(clock: FakeClock) -> None:
    bucket = TokenBucket(capacity=1, refill_per_s=1, clock=clock)
    bucket.take()
    with pytest.raises(RuntimeError):
        bucket.take()


@pytest.mark.parametrize(("capacity", "refill"), [(0, 1), (1, 0), (1, -1)])
def test_token_bucket_invalid(clock: FakeClock, capacity: float, refill: float) -> None:
    with pytest.raises(ValueError):
        TokenBucket(capacity, refill, clock)


def test_sliding_window_basics(clock: FakeClock) -> None:
    window = SlidingWindowLog(limit=2, window_s=10, clock=clock)
    assert window.time_until_available() == 0
    window.record()
    clock.advance(3)
    window.record()
    assert window.used == 2 and window.time_until_available() == pytest.approx(7)
    clock.advance(7)
    assert window.used == 1 and window.time_until_available() == 0
    for bad in ((0, 10), (1, 0)):
        with pytest.raises(ValueError):
            SlidingWindowLog(bad[0], bad[1], clock)


def test_burst_pacing(clock: FakeClock) -> None:
    limiter = make(clock)
    assert limiter.acquire() == 0.0
    assert limiter.acquire() == pytest.approx(1 / 3)  # capacity 1, refill 3/s
    assert limiter.try_acquire() is False
    clock.advance(1 / 3)
    assert limiter.try_acquire() is True


def test_hourly_exhaustion_wait(clock: FakeClock) -> None:
    limiter = make(clock, per_hour=10)
    start = clock.now
    acquire_times(limiter, clock, 10)
    elapsed = clock.now - start
    assert limiter.wait_time() == pytest.approx(3600.0 - elapsed)
    assert limiter.acquire() == pytest.approx(3600.0 - elapsed)


@pytest.mark.parametrize(("burst", "per_hour"), [(1, 450), (0, 450), (4, 0), (4, 501)])
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
    assert len(granted) == 1  # clock frozen: exactly one token in the per-second bucket


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
    assert count == 20 and limiter.available_this_hour == 0
