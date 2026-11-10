"""RDD jobs vs DataFrame implementations of the same logic (row-for-row), and edge cases."""

from __future__ import annotations

import math

import pytest
from _feature_fixtures import frame, lap, race
from pyspark.sql import DataFrame, Row, SparkSession

from conftest import SampleLake
from f1sa.etl import rdd_jobs
from f1sa.etl.io import load_clean
from f1sa.features import schemas as FS

STINT_COLS = [f.name for f in FS.STINT_BOUNDARIES]


def _sorted_rows(df: DataFrame, cols: list[str]) -> list[tuple[object, ...]]:
    return sorted(tuple(r[c] for c in cols) for r in df.collect())


# ------------------------------------------------------------------ split_stints (pure Python)
@pytest.mark.parametrize(
    ("laps", "expected"),
    [
        ([], []),
        ([(1, False)], [(1, 1, 1)]),
        ([(1, True)], [(1, 1, 1)]),  # a stop on the only (last) lap starts no stint
        ([(n, False) for n in range(1, 11)], [(1, 1, 10)]),  # DNF / no stop
        ([(n, n == 4) for n in range(1, 11)], [(1, 1, 4), (2, 5, 10)]),
        ([(n, n in (3, 7)) for n in range(1, 11)], [(1, 1, 3), (2, 4, 7), (3, 8, 10)]),
        ([(n, n == 10) for n in range(1, 11)], [(1, 1, 10)]),  # stop on the last lap
        ([(n, None) for n in range(1, 4)], [(1, 1, 3)]),  # unknown flag = no stop
        ([(3, False), (1, True), (2, False)], [(1, 1, 1), (2, 2, 3)]),  # any input order
    ],
)
def test_split_stints(
    laps: list[tuple[int, bool | None]], expected: list[tuple[int, int, int]]
) -> None:
    assert rdd_jobs.split_stints(laps) == expected


def _fixture_laps(spark: SparkSession) -> DataFrame:
    rows = []
    for n in range(1, 11):
        rows.append(lap(2024, 1, "two_stops", n, 1, 90.0, pit_in=n in (3, 7), pit_out=n in (4, 8)))
        rows.append(lap(2024, 1, "last_lap_stop", n, 2, 91.0, pit_in=n == 10))
    for n in range(1, 4):
        rows.append(lap(2024, 1, "dnf", n, 3, 92.0))
    rows.append(lap(2024, 2, "one_lap", 1, 1, 95.0, pit_in=True))
    return frame(spark, "laps", rows)


def test_rdd_and_df_stints_agree_on_edge_case_fixture(spark: SparkSession) -> None:
    laps = _fixture_laps(spark)
    rdd = rdd_jobs.rdd_stints(laps)
    assert rdd.schema == FS.STINT_BOUNDARIES
    expected = [
        (2024, 1, "dnf", 1, 1, 3, 3),
        (2024, 1, "last_lap_stop", 1, 1, 10, 10),
        (2024, 1, "two_stops", 1, 1, 3, 3),
        (2024, 1, "two_stops", 2, 4, 7, 4),
        (2024, 1, "two_stops", 3, 8, 10, 3),
        (2024, 2, "one_lap", 1, 1, 1, 1),
    ]
    assert _sorted_rows(rdd, STINT_COLS) == expected
    assert _sorted_rows(rdd_jobs.df_stints(laps), STINT_COLS) == expected


def test_empty_laps_give_empty_stints(spark: SparkSession) -> None:
    empty = frame(spark, "laps", [])
    assert rdd_jobs.rdd_stints(empty).count() == 0
    assert rdd_jobs.df_stints(empty).count() == 0


def test_sample_rdd_stints_equal_df_and_clean_row_for_row(
    spark: SparkSession, sample_clean_lake: SampleLake
) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root, ["laps", "stints"])
    rdd = _sorted_rows(rdd_jobs.rdd_stints(c["laps"]), STINT_COLS)
    df = _sorted_rows(rdd_jobs.df_stints(c["laps"]), STINT_COLS)
    clean = _sorted_rows(c["stints"], STINT_COLS)
    assert len(rdd) == 100
    assert rdd == df == clean


# ------------------------------------------------------------------ lap-time distribution
def test_accumulator_matches_direct_statistics() -> None:
    xs = [90.4, 91.2, 90.9, 95.0, 89.99]
    left = rdd_jobs.zero_acc()
    for x in xs[:2]:
        left = rdd_jobs.seq_acc(left, x)
    right = rdd_jobs.zero_acc()
    for x in xs[2:]:
        right = rdd_jobs.seq_acc(right, x)
    n, mean, m2, lo, hi, bins = rdd_jobs.comb_acc(left, right)
    mu = sum(xs) / len(xs)
    assert n == 5 and math.isclose(mean, mu, rel_tol=1e-12)
    assert math.isclose(m2, sum((x - mu) ** 2 for x in xs), rel_tol=1e-9)
    assert (lo, hi) == (89.99, 95.0)
    assert bins == {89: 1, 90: 2, 91: 1, 95: 1}
    assert rdd_jobs.comb_acc(rdd_jobs.zero_acc(), right) == right
    assert rdd_jobs.comb_acc(left, rdd_jobs.zero_acc()) == left


def _dist_rows(df: DataFrame) -> dict[tuple[int, int, str], Row]:
    return {(r["season"], r["round"], r["circuit_id"]): r for r in df.collect()}


def _assert_same_distribution(a: DataFrame, b: DataFrame) -> None:
    ra, rb = _dist_rows(a), _dist_rows(b)
    assert ra.keys() == rb.keys()
    for key in ra:
        x, y = ra[key], rb[key]
        assert (x["n_laps"], x["min_s"], x["max_s"]) == (y["n_laps"], y["min_s"], y["max_s"])
        assert x["bins_1s"] == y["bins_1s"]
        # floating sums depend on the order of addition
        assert math.isclose(x["mean_s"], y["mean_s"], rel_tol=1e-9)
        if x["std_s"] is None or y["std_s"] is None:
            assert x["std_s"] is None and y["std_s"] is None
        else:
            assert math.isclose(x["std_s"], y["std_s"], rel_tol=1e-9)


def test_distribution_fixture_filters_and_agrees(spark: SparkSession) -> None:
    rows = [
        lap(2024, 1, "a", 1, 1, 120.0),  # lap 1: excluded
        lap(2024, 1, "a", 2, 1, 90.5),
        lap(2024, 1, "a", 3, 1, 110.0, pit_in=True),  # excluded
        lap(2024, 1, "a", 4, 1, 100.0, pit_out=True),  # excluded
        lap(2024, 1, "a", 5, 1, 150.0, neutralised=True),  # excluded
        lap(2024, 1, "a", 6, 1, None),  # excluded
        lap(2024, 1, "a", 7, 1, 91.5),
        lap(2024, 1, "b", 2, 2, 91.0),
        lap(2024, 2, "a", 2, 1, 80.25),  # single lap: std null
        lap(2024, 3, "a", 2, 1, 80.0),  # race without a circuit row: dropped
    ]
    laps = frame(spark, "laps", rows)
    races = frame(spark, "races", [race(2024, 1, "ring"), race(2024, 2, "oval")])
    rdd = rdd_jobs.rdd_lap_time_distribution(laps, races)
    assert rdd.schema == FS.LAP_TIME_DISTRIBUTION
    got = _dist_rows(rdd)
    assert set(got) == {(2024, 1, "ring"), (2024, 2, "oval")}
    ring = got[(2024, 1, "ring")]
    assert ring["n_laps"] == 3 and ring["bins_1s"] == {90: 1, 91: 2}
    assert math.isclose(ring["mean_s"], 91.0) and math.isclose(ring["std_s"], 0.5)
    assert got[(2024, 2, "oval")]["std_s"] is None
    _assert_same_distribution(rdd, rdd_jobs.df_lap_time_distribution(laps, races))


def test_sample_distribution_rdd_equals_df(
    spark: SparkSession, sample_clean_lake: SampleLake
) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root, ["laps", "races"])
    rdd = rdd_jobs.rdd_lap_time_distribution(c["laps"], c["races"])
    df = rdd_jobs.df_lap_time_distribution(c["laps"], c["races"])
    assert set(_dist_rows(rdd)) == {(2023, 5, "miami"), (2024, 5, "shanghai")}
    _assert_same_distribution(rdd, df)
    green = rdd_jobs.green_laps(c["laps"]).count()
    assert sum(r["n_laps"] for r in rdd.collect()) == green
