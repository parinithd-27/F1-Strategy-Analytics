"""Thread-safe token-bucket rate limiting for the Jolpica-F1 API.

Jolpica allows 4 requests/second (burst) and 500 requests/hour (sustained) per IP. The
:class:`DualRateLimiter` enforces both at once: a request may proceed only when *both* buckets hold
a token. Clock and sleep are injectable so the behaviour is testable without real waiting.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from f1sa.config import JOLPICA_HARD_LIMIT_PER_HOUR

Clock = Callable[[], float]
Sleep = Callable[[float], None]

JOLPICA_BURST_PER_S = 4
"""Jolpica's documented burst limit (requests per second)."""


class TokenBucket:
    """A token bucket holding at most ``capacity`` tokens, refilled at ``refill_per_s``.

    Not thread-safe on its own; :class:`DualRateLimiter` guards it with a lock.
    """

    def __init__(self, capacity: float, refill_per_s: float, clock: Clock) -> None:
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        if refill_per_s <= 0:
            raise ValueError(f"refill_per_s must be > 0, got {refill_per_s}")
        self.capacity = float(capacity)
        self.refill_per_s = float(refill_per_s)
        self._clock = clock
        self._tokens = float(capacity)
        self._last = clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._last)
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_per_s)
        self._last = now

    @property
    def tokens(self) -> float:
        """Tokens currently available (after refilling up to now)."""
        self._refill()
        return self._tokens

    def time_until_available(self, n: float = 1.0) -> float:
        """Seconds until ``n`` tokens are available (0 if available now)."""
        self._refill()
        missing = n - self._tokens
        return 0.0 if missing <= 0 else missing / self.refill_per_s

    def take(self, n: float = 1.0) -> None:
        """Remove ``n`` tokens; the caller must have checked availability."""
        self._refill()
        if self._tokens + 1e-9 < n:
            raise RuntimeError("take() called without enough tokens")
        self._tokens = max(0.0, self._tokens - n)


class DualRateLimiter:
    """Burst (per-second) + sustained (per-hour) limiter shared safely between threads.

    Both buckets start full, so a fresh process may send ``burst_per_s`` requests immediately and
    at most ``per_hour`` requests in its first hour.
    """

    def __init__(
        self,
        burst_per_s: int = JOLPICA_BURST_PER_S,
        per_hour: int = 450,
        clock: Clock = time.monotonic,
        sleep: Sleep = time.sleep,
    ) -> None:
        if burst_per_s < 1:
            raise ValueError(f"burst_per_s must be >= 1, got {burst_per_s}")
        if not 1 <= per_hour <= JOLPICA_HARD_LIMIT_PER_HOUR:
            raise ValueError(
                f"per_hour must be in 1..{JOLPICA_HARD_LIMIT_PER_HOUR}, got {per_hour}"
            )
        self._lock = threading.Lock()
        self._sleep = sleep
        self.burst = TokenBucket(burst_per_s, burst_per_s, clock)
        self.hourly = TokenBucket(per_hour, per_hour / 3600.0, clock)

    def wait_time(self) -> float:
        """Seconds until one request would be allowed (0 if allowed now)."""
        with self._lock:
            return self._wait_time_locked()

    def _wait_time_locked(self) -> float:
        return max(self.burst.time_until_available(), self.hourly.time_until_available())

    def try_acquire(self) -> bool:
        """Take one token from both buckets if possible, without blocking."""
        with self._lock:
            if self._wait_time_locked() > 0:
                return False
            self.burst.take()
            self.hourly.take()
            return True

    def acquire(self) -> float:
        """Block until a request is allowed, consume it, and return the seconds spent waiting."""
        waited = 0.0
        while True:
            with self._lock:
                wait = self._wait_time_locked()
                if wait <= 0:
                    self.burst.take()
                    self.hourly.take()
                    return waited
            # sleep outside the lock so other threads can make progress / observe state
            self._sleep(wait)
            waited += wait
