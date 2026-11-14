"""Regression tests for ``etl.clean.select_races`` with many race keys (Step 10 follow-up).

The old helper OR-ed one ``(season, round)`` condition per race. Spark converts a Column
recursively, so 384 races (``build_s1_x16``) and 456 races (``build_s5_x4``) overflowed the
driver's JVM stack at the ``.filter`` call (264 races still fitted). The fix is one flat
``struct(season, round) IN (...)``; these tests use synthetic race keys only.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from f1sa.etl import clean
from f1sa.etl.io import restrict

RaceKey = tuple[int, int]


def _keys(n: int) -> set[RaceKey]:
    """``n`` distinct synthetic race keys: 24 rounds per season, seasons shifted by 100 like the
    benchmark's replicas (2024, 2124, 2224, ...)."""
    return {(2024 + 100 * (i // 24), 1 + i % 24) for i in range(n)}


def _frame(spark: SparkSession, keys: set[RaceKey], extra: list[tuple]) -> DataFrame:  # type: ignore[type-arg]
    rows = [(s, r, f"{s}-{r}") for s, r in sorted(keys)] + extra
    return spark.createDataFrame(rows, "season int, round int, tag string")


NOISE: list[tuple] = [  # type: ignore[type-arg]
    (1999, 1, "unlisted season"),
    (2024, 99, "unlisted round"),
    (None, 1, "null season"),
    (2024, None, "null round"),
    (None, None, "both null"),
]


@pytest.mark.parametrize("n", [600, 2000])
def test_many_race_keys_keep_exactly_the_listed_pairs(spark: SparkSession, n: int) -> None:
    keys = _keys(n)
    wanted = {k for i, k in enumerate(sorted(keys)) if i % 10}  # 90 % of the present pairs
    df = _frame(spark, keys, NOISE)
    got = {(r["season"], r["round"]) for r in clean.select_races(df, None, wanted).collect()}
    assert got == wanted and len(wanted) >= 540


def test_the_condition_is_flat_and_becomes_an_inset(spark: SparkSession) -> None:
    keys = _keys(600)
    out = clean.select_races(_frame(spark, keys, []), None, keys)
    qe = out._jdf.queryExecution()
    analyzed = qe.analyzed().toString()
    optimized = qe.optimizedPlan().toString()
    assert " OR " not in analyzed.upper()  # no chain of ORs at any size
    assert "INSET" in optimized.upper()
    assert out.count() == 600


def test_seasons_and_races_combine_and_empty_means_no_filter(spark: SparkSession) -> None:
    keys = _keys(500)
    df = _frame(spark, keys, NOISE)
    assert clean.select_races(df, None, None).count() == 500 + len(NOISE)
    assert clean.select_races(df, None, set()).count() == 500 + len(NOISE)
    both = clean.select_races(df, [2024, 2124], keys)
    assert {r["season"] for r in both.collect()} == {2024, 2124} and both.count() == 48


def test_restrict_limits_every_table(spark: SparkSession) -> None:
    keys = _keys(520)
    wanted = {k for k in keys if k[1] <= 3}
    tables = {
        "laps": _frame(spark, keys, NOISE),
        "races": _frame(spark, keys, []),
    }
    out = restrict(tables, wanted)
    for name, df in out.items():
        assert {(r["season"], r["round"]) for r in df.collect()} == wanted, name


def test_partition_pruning_is_kept(spark: SparkSession, tmp_path: Path) -> None:
    keys = _keys(48)
    path = str(tmp_path / "t")
    _frame(spark, keys, []).write.partitionBy("season", "round").parquet(path)
    out = clean.select_races(spark.read.parquet(path), None, {(2024, 3), (2124, 5)})
    plan = out._jdf.queryExecution().executedPlan().toString()
    line = next(x for x in plan.splitlines() if "PartitionFilters" in x)
    filters = line.split("PartitionFilters:")[1].split("],")[0]
    assert "season" in filters and "round" in filters  # pruned on both partition columns
    assert sorted(r["tag"] for r in out.collect()) == ["2024-3", "2124-5"]
