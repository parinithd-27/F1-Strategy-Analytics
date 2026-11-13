"""Timing harness, summary statistics and the memory guard (Step 10)."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from pyspark.sql import SparkSession

from f1sa.bench.timing import GIB, MemoryGuard, run_timed, summarise


def make_clock(durations: list[float]) -> Callable[[], float]:
    """A clock where call 2k is a start and call 2k+1 ends after durations[k]."""
    ticks: list[float] = []
    now = 0.0
    for d in durations:
        ticks += [now, now + d]
        now += d + 1.0
    it = iter(ticks)
    return lambda: next(it)


def test_trivial_job_warmup_is_discarded_and_exactly_n_runs_are_timed() -> None:
    calls: list[str] = []
    after: list[str] = []
    clock = make_clock([9.0, 1.0, 2.0, 3.0])
    runs = run_timed(
        lambda label: calls.append(label) or len(calls),
        warmup=1,
        repeats=3,
        clock=clock,
        after=after.append,
    )
    assert calls == ["warmup-0", "run-0", "run-1", "run-2"]
    assert after == calls, "the untimed hook runs after every call"
    assert runs.warmup_s == [9.0]
    assert runs.times_s == [1.0, 2.0, 3.0]
    assert runs.results == [2, 3, 4], "warm-up return values are discarded"
    assert runs.summary().median_s == 2.0


def test_zero_warmup_and_invalid_counts() -> None:
    runs = run_timed(lambda _l: None, warmup=0, repeats=1)
    assert runs.warmup_s == [] and len(runs.times_s) == 1
    with pytest.raises(ValueError, match="repeats"):
        run_timed(lambda _l: None, repeats=0)
    with pytest.raises(ValueError, match="warmup"):
        run_timed(lambda _l: None, warmup=-1)


def test_real_spark_job_through_the_harness(spark: SparkSession) -> None:
    runs = run_timed(lambda _l: spark.range(1000).count(), warmup=1, repeats=2)
    assert runs.results == [1000, 1000]
    assert all(t > 0 for t in runs.times_s)


@pytest.mark.parametrize(
    ("times", "median", "q1", "q3"),
    [
        ([1.0, 2.0, 3.0, 4.0, 5.0], 3.0, 2.0, 4.0),  # odd
        ([4.0, 1.0, 3.0, 2.0], 2.5, 1.75, 3.25),  # even, unsorted, linear interpolation
        ([7.5], 7.5, 7.5, 7.5),  # one value
    ],
)
def test_summarise_median_iqr_min_max(
    times: list[float], median: float, q1: float, q3: float
) -> None:
    s = summarise(times)
    assert (s.n, s.median_s, s.q1_s, s.q3_s) == (len(times), median, q1, q3)
    assert s.iqr_s == pytest.approx(q3 - q1)
    assert (s.min_s, s.max_s) == (min(times), max(times))


def test_summarise_rejects_empty_and_invalid() -> None:
    with pytest.raises(ValueError, match="no timed runs"):
        summarise([])
    with pytest.raises(ValueError, match="finite"):
        summarise([1.0, float("nan")])
    with pytest.raises(ValueError, match="finite"):
        summarise([-1.0])


def test_guard_precheck_skips_only_after_a_smaller_factor_was_killed() -> None:
    guard = MemoryGuard(limit_bytes=4 * GIB, fraction=0.9)
    assert guard.precheck(None, "", 64).ok, "no linear projection: the JVM heap is fixed"
    skip = guard.precheck(16, "aborted_memory_guard", 64)
    assert (
        not skip.ok
        and skip.reason == "x16 of the same seasons and master ended aborted_memory_guard"
    )
    assert not guard.precheck(4, "timeout", 16).ok
    with pytest.raises(ValueError, match="grow"):
        guard.precheck(16, "timeout", 4)


def test_guard_runtime_check_on_container_usage_and_host_reserve() -> None:
    guard = MemoryGuard(limit_bytes=4 * GIB, fraction=0.9, host_reserve_bytes=GIB)
    assert guard.exceeded(int(3 * GIB), int(5 * GIB)) is None
    assert "container usage" in (guard.exceeded(int(3.7 * GIB), int(5 * GIB)) or "")
    assert "host available" in (guard.exceeded(int(1 * GIB), int(0.5 * GIB)) or "")
    assert guard.exceeded(None, None) is None
    with pytest.raises(ValueError):
        MemoryGuard(limit_bytes=0)
    with pytest.raises(ValueError):
        MemoryGuard(limit_bytes=1, fraction=1.5)
