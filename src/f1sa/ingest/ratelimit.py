"""Thread-safe rate limiting for the Jolpica-F1 API.

Jolpica allows 4 requests/second (burst) and 500 requests/hour (sustained) per IP.
:class:`DualRateLimiter` guarantees, for every request it admits:

* at most ``burst_per_s`` requests in **any** 1-second window (a token bucket with capacity 1
  refilled at ``burst_per_s - 1`` per second; a bucket of capacity C and rate R admits at most
  C + R*T requests in a window of T seconds, so this is exactly ``burst_per_s`` per second);
* at most ``per_hour`` requests in **any** rolling hour (a sliding-window log). The log can be
  persisted to a file so resumed or back-to-back runs share the same hourly budget.

Why not a big hourly token bucket: a bucket that starts full with capacity N and refills N/h
admits up to 2N requests in its first hour (N + N/h * 1 h), which breaks the 500/h limit.

Clocks and sleep are injectable so all behaviour is testable without real waiting.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from collections.abc import Callable
from pathlib import Path

from f1sa.config import JOLPICA_HARD_LIMIT_PER_HOUR

logger = logging.getLogger(__name__)

Clock = Callable[[], float]
Sleep = Callable[[float], None]

JOLPICA_BURST_PER_S = 4
"""Jolpica's documented burst limit (requests per second)."""
HOUR_S = 3600.0


class TokenBucket:
    """A token bucket holding at most ``capacity`` tokens, refilled at ``refill_per_s``.

    Admits at most ``capacity + refill_per_s * T`` takes in any window of T seconds.
    Not thread-safe on its own; callers guard it with a lock.
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
        return 0.0 if missing <= 1e-12 else missing / self.refill_per_s

    def take(self, n: float = 1.0) -> None:
        """Remove ``n`` tokens; the caller must have checked availability."""
        self._refill()
        if self._tokens + 1e-9 < n:
            raise RuntimeError("take() called without enough tokens")
        self._tokens = max(0.0, self._tokens - n)


class SlidingWindowLog:
    """At most ``limit`` events in any rolling ``window_s``; optionally persisted to a file.

    The file holds one wall-clock timestamp per line, appended per event. On load, entries
    older than the window and unparseable lines (e.g. a torn last line after a kill) are dropped,
    and the file is compacted atomically. Not thread-safe on its own.
    """

    def __init__(
        self,
        limit: int,
        window_s: float,
        clock: Clock,
        log_path: Path | None = None,
    ) -> None:
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        if window_s <= 0:
            raise ValueError(f"window_s must be > 0, got {window_s}")
        self.limit = limit
        self.window_s = window_s
        self._clock = clock
        self._events: deque[float] = deque()
        self._log_path = log_path
        if log_path is not None:
            self._load(log_path)

    def _load(self, path: Path) -> None:
        now = self._clock()
        if path.exists():
            kept: list[float] = []
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                try:
                    ts = float(line)
                except ValueError:
                    continue
                if now - self.window_s < ts <= now + 1.0:
                    kept.append(ts)
            self._events.extend(sorted(kept))
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text("".join(f"{ts:.3f}\n" for ts in self._events), encoding="utf-8")
        os.replace(tmp, path)
        if self._events:
            logger.info("rate limiter: %d request(s) already used this hour", len(self._events))

    def _expire(self) -> None:
        cutoff = self._clock() - self.window_s
        while self._events and self._events[0] <= cutoff:
            self._events.popleft()

    @property
    def used(self) -> int:
        """Events inside the current window."""
        self._expire()
        return len(self._events)

    def time_until_available(self) -> float:
        self._expire()
        if len(self._events) < self.limit:
            return 0.0
        return max(0.0, self._events[0] + self.window_s - self._clock())

    def record(self) -> None:
        now = self._clock()
        self._events.append(now)
        if self._log_path is not None:
            with open(self._log_path, "a", encoding="utf-8") as fh:
                fh.write(f"{now:.3f}\n")


class DualRateLimiter:
    """Per-second + per-hour limiter shared safely between threads (see module docstring)."""

    def __init__(
        self,
        burst_per_s: int = JOLPICA_BURST_PER_S,
        per_hour: int = 450,
        clock: Clock = time.monotonic,
        sleep: Sleep = time.sleep,
        *,
        wall_clock: Clock | None = None,
        log_path: Path | None = None,
    ) -> None:
        if burst_per_s < 2:
            raise ValueError(f"burst_per_s must be >= 2, got {burst_per_s}")
        if not 1 <= per_hour <= JOLPICA_HARD_LIMIT_PER_HOUR:
            raise ValueError(
                f"per_hour must be in 1..{JOLPICA_HARD_LIMIT_PER_HOUR}, got {per_hour}"
            )
        self._lock = threading.Lock()
        self._sleep = sleep
        self.burst_per_s = burst_per_s
        self.per_hour = per_hour
        self.burst = TokenBucket(1, burst_per_s - 1, clock)
        # the persisted hourly log needs wall-clock time (monotonic clocks reset on reboot)
        hourly_clock = wall_clock or (time.time if log_path is not None else clock)
        self.hourly = SlidingWindowLog(per_hour, HOUR_S, hourly_clock, log_path)

    @property
    def available_this_hour(self) -> int:
        with self._lock:
            return self.per_hour - self.hourly.used

    def wait_time(self) -> float:
        """Seconds until one request would be allowed (0 if allowed now)."""
        with self._lock:
            return self._wait_time_locked()

    def _wait_time_locked(self) -> float:
        return max(self.burst.time_until_available(), self.hourly.time_until_available())

    def _take_locked(self) -> None:
        self.burst.take()
        self.hourly.record()

    def try_acquire(self) -> bool:
        """Admit one request if possible, without blocking."""
        with self._lock:
            if self._wait_time_locked() > 0:
                return False
            self._take_locked()
            return True

    def acquire(self) -> float:
        """Block until a request is allowed, admit it, and return the seconds spent waiting."""
        waited = 0.0
        while True:
            with self._lock:
                wait = self._wait_time_locked()
                if wait <= 0:
                    self._take_locked()
                    return waited
            if wait > 60:
                logger.warning("hourly Jolpica budget used up; waiting %.0fs", wait)
            # sleep outside the lock so other threads can make progress / observe state
            self._sleep(wait)
            waited += wait
