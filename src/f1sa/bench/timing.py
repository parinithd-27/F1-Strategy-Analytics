"""Timing harness (warm-up plus timed runs), summary statistics and the memory guard.

* :func:`run_timed` runs a job ``warmup`` times (discarded), then ``repeats`` times (timed with a
  monotonic clock). Each call receives its label (``warmup-0``, ``run-0`` ...), which the Spark
  cells use as the job group for the REST metrics.
* :func:`summarise` gives median, Q1, Q3, IQR, min and max (numpy ``linear`` percentiles).
* :class:`MemoryGuard` decides whether a cell may run (projection from the previous replication
  factor) and whether a running cell must be killed (container usage or host available memory).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TypeVar

import numpy as np

T = TypeVar("T")

GIB = 1024**3


@dataclass(frozen=True)
class Summary:
    n: int
    median_s: float
    q1_s: float
    q3_s: float
    iqr_s: float
    min_s: float
    max_s: float


def summarise(times: Sequence[float]) -> Summary:
    """Median, quartiles (numpy linear interpolation), IQR, min and max of ``times``."""
    if len(times) == 0:
        raise ValueError("no timed runs to summarise")
    arr = np.asarray(times, dtype=float)
    if not np.all(np.isfinite(arr)) or np.any(arr < 0):
        raise ValueError(f"times must be finite and >= 0, got {list(times)}")
    q1, med, q3 = (float(x) for x in np.percentile(arr, [25, 50, 75], method="linear"))
    return Summary(
        n=int(arr.size),
        median_s=med,
        q1_s=q1,
        q3_s=q3,
        iqr_s=q3 - q1,
        min_s=float(arr.min()),
        max_s=float(arr.max()),
    )


@dataclass
class TimedRuns[T]:
    warmup_s: list[float] = field(default_factory=list)
    times_s: list[float] = field(default_factory=list)
    results: list[T] = field(default_factory=list)
    """The job's return value per timed run (warm-up values are discarded)."""

    def summary(self) -> Summary:
        return summarise(self.times_s)


def run_timed[T](
    job: Callable[[str], T],
    *,
    warmup: int = 1,
    repeats: int = 5,
    clock: Callable[[], float] = time.perf_counter,
    after: Callable[[str], None] | None = None,
) -> TimedRuns[T]:
    """Run ``job`` ``warmup`` times (discarded), then ``repeats`` timed times. ``after`` runs
    untimed after every call (e.g. deleting that run's output)."""
    if warmup < 0:
        raise ValueError(f"warmup must be >= 0, got {warmup}")
    if repeats < 1:
        raise ValueError(f"repeats must be >= 1, got {repeats}")
    out: TimedRuns[T] = TimedRuns()
    labels = [f"warmup-{i}" for i in range(warmup)] + [f"run-{i}" for i in range(repeats)]
    for label in labels:
        t0 = clock()
        value = job(label)
        elapsed = clock() - t0
        if label.startswith("warmup-"):
            out.warmup_s.append(elapsed)
        else:
            out.times_s.append(elapsed)
            out.results.append(value)
        if after is not None:
            after(label)
    return out


@dataclass(frozen=True)
class GuardDecision:
    ok: bool
    reason: str


@dataclass(frozen=True)
class MemoryGuard:
    """``limit_bytes`` is the container's memory limit (cgroup ``memory.max``). A running cell is
    killed when the container's memory passes ``fraction`` of it, or when the host's available
    memory falls below ``host_reserve_bytes`` (:meth:`exceeded`). A cell is skipped when a smaller
    factor of the same (phase, seasons, master) was killed by the guard or timed out
    (:meth:`precheck`).

    There is no projection of the peak: the driver JVM's footprint is mostly its fixed heap plus
    off-heap and grows far less than linearly with the input (smoke run: 3.14 GiB at x1, 3.55 GiB
    at x4, at a 2g heap), so a linear projection skipped cells that fit."""

    limit_bytes: int
    fraction: float = 0.9
    host_reserve_bytes: int = int(1.5 * GIB)

    def __post_init__(self) -> None:
        if self.limit_bytes <= 0:
            raise ValueError(f"limit_bytes must be > 0, got {self.limit_bytes}")
        if not 0 < self.fraction <= 1:
            raise ValueError(f"fraction must be in (0, 1], got {self.fraction}")

    @property
    def threshold_bytes(self) -> float:
        return self.limit_bytes * self.fraction

    def precheck(self, failed_factor: int | None, failed_status: str, factor: int) -> GuardDecision:
        """Skip ``factor`` if a smaller factor of the same (phase, seasons, master) was killed by
        the guard (``aborted_memory_guard``) or timed out; ``failed_factor`` is None otherwise."""
        if failed_factor is None:
            return GuardDecision(True, "no smaller factor was killed")
        if factor <= failed_factor:
            raise ValueError(f"factors must grow: {failed_factor} -> {factor}")
        return GuardDecision(
            False, f"x{failed_factor} of the same seasons and master ended {failed_status}"
        )

    def exceeded(self, usage_bytes: int | None, host_available_bytes: int | None) -> str | None:
        """Why a running cell must be killed now, or None."""
        if usage_bytes is not None and usage_bytes > self.threshold_bytes:
            return (
                f"container usage {usage_bytes / GIB:.2f} GiB > {self.fraction:.0%} of "
                f"{self.limit_bytes / GIB:.2f} GiB"
            )
        if host_available_bytes is not None and host_available_bytes < self.host_reserve_bytes:
            return (
                f"host available {host_available_bytes / GIB:.2f} GiB < reserve "
                f"{self.host_reserve_bytes / GIB:.2f} GiB"
            )
        return None
