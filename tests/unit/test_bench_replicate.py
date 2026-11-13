"""Synthetic replication of the lap-level clean tables (Step 10)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from conftest import SampleLake
from f1sa.bench.paths import BenchPathError, bench_file_uri
from f1sa.bench.replicate import (
    FLAG,
    check_factor,
    load_seasons,
    materialise,
    read_replicated,
    replica_driver_id,
    replica_season,
    replicate_tables,
    strip_flag,
)
from f1sa.etl.clean import table_checksum
from f1sa.lake import schemas as S

SEASONS = (2023, 2024)


@pytest.fixture(scope="module")
def clean(spark: SparkSession, sample_clean_lake: SampleLake) -> dict[str, DataFrame]:
    return load_seasons(spark, sample_clean_lake.clean_root, SEASONS)


@pytest.mark.parametrize("factor", [1, 4])
def test_rows_are_exactly_factor_times_with_unique_keys(
    clean: dict[str, DataFrame], factor: int
) -> None:
    rep = replicate_tables(clean, factor)
    for name, df in clean.items():
        n = df.count()
        out = rep[name]
        assert out.count() == factor * n, name
        keys = list(S.PRIMARY_KEYS[name])
        assert out.select(*keys).distinct().count() == factor * n, f"{name} keys not unique"
        assert out.where(F.col(FLAG)).count() == (factor - 1) * n
    assert clean["laps"].count() > 0, "the sample must have laps"


def test_race_seq_unique_per_race_and_dates_follow_seasons(clean: dict[str, DataFrame]) -> None:
    races = replicate_tables(clean, 4)["races"]
    rows = races.select(
        (F.col("season") * 100 + F.col("round")).alias("race_seq"),
        "season",
        F.year("race_date").alias("year"),
        F.year("race_start_utc").alias("start_year"),
    ).collect()
    assert len({r["race_seq"] for r in rows}) == len(rows) == 4 * len(SEASONS)
    assert all(r["season"] == r["year"] == r["start_year"] for r in rows)
    assert sorted({r["season"] for r in rows}) == [2023, 2024, 2123, 2124, 2223, 2224, 2323, 2324]


def test_replica_zero_is_the_input_and_ids_are_perturbed(clean: dict[str, DataFrame]) -> None:
    rep = strip_flag(replicate_tables(clean, 3))
    for name in ("laps", "results", "pitstops", "stints", "races"):
        orig = rep[name].where(F.col("season").isin(list(SEASONS)))
        assert table_checksum(orig) == table_checksum(clean[name]), name
    drivers = {r[0] for r in rep["laps"].select("driver_id").distinct().collect()}
    real = {r[0] for r in clean["laps"].select("driver_id").distinct().collect()}
    assert drivers == real | {f"{d}__r{i}" for d in real for i in (1, 2)}
    cons = {r[0] for r in rep["results"].select("constructor_id").distinct().collect()}
    assert cons == {r[0] for r in clean["results"].select("constructor_id").distinct().collect()}
    lap_times = rep["laps"].groupBy("lap_time_s").count().where(F.col("count") % 3 != 0)
    assert lap_times.count() == 0, "lap values are copied unchanged"
    assert set(rep["laps"].columns) == {f.name for f in S.LAPS.fields}


def test_scalar_helpers_and_factor_checks() -> None:
    assert replica_season(2024, 0) == 2024 and replica_season(2024, 63) == 8324
    assert replica_driver_id("max_verstappen", 0) == "max_verstappen"
    assert replica_driver_id("max_verstappen", 2) == "max_verstappen__r2"
    with pytest.raises(ValueError):
        replica_season(2024, -1)
    with pytest.raises(ValueError):
        replica_driver_id("x", -1)
    for bad in (0, -3):
        with pytest.raises(ValueError, match=">= 1"):
            check_factor(bad)
    check_factor(64, 2025)
    check_factor(80, 2025)  # 2025 + 100 * 79 = 9925
    with pytest.raises(ValueError, match="past 9999"):
        check_factor(81, 2025)


def test_materialise_writes_only_under_data_bench_and_reads_back(
    spark: SparkSession, sample_clean_lake: SampleLake, tmp_path: Path
) -> None:
    root = bench_file_uri("r1", "replicated", "s1_x2")
    counts = materialise(spark, sample_clean_lake.clean_root, [2024], 2, root)
    back = read_replicated(spark, root)
    for name, n in counts.items():
        assert back[name].count() == n
    one = load_seasons(spark, sample_clean_lake.clean_root, [2024])
    assert counts["laps"] == 2 * one["laps"].count()
    stored = spark.read.parquet(f"{root}/laps")
    assert stored.where(F.col(FLAG)).count() == one["laps"].count()
    assert (tmp_path / "data" / "bench" / "r1" / "replicated" / "s1_x2" / "laps").is_dir()
    with pytest.raises(BenchPathError):
        materialise(spark, sample_clean_lake.clean_root, [2024], 2, f"file://{tmp_path}/data/lake")
    with pytest.raises(ValueError, match="no seasons"):
        load_seasons(spark, sample_clean_lake.clean_root, [])
