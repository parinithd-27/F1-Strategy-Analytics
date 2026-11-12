"""Per-race priors for the stream: equal to the batch prior columns, one partition read only."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from f1sa.features import schemas as FS
from f1sa.features.build import read_feature
from f1sa.streaming import priors as PRI
from f1sa.streaming.live_features import RacePriors


@pytest.fixture(scope="module")
def sample(
    spark: SparkSession, sample_clean_lake: Any, sample_features_root: str
) -> dict[str, Any]:
    return {
        "lake": sample_clean_lake.settings().data_lake_uri,
        "attempts": read_feature(spark, sample_features_root, "undercut_attempts").collect(),
    }


def test_priors_equal_the_batch_attempt_columns(
    spark: SparkSession, sample: dict[str, Any]
) -> None:
    checked = 0
    for race in ((2023, 5), (2024, 5)):
        p = PRI.load_priors(spark, sample["lake"], *race)
        for row in (a for a in sample["attempts"] if (a["season"], a["round"]) == race):
            assert (p.pit_loss_prior_s, p.pit_loss_source) == (
                row["pit_loss_prior_s"],
                row["pit_loss_source"],
            )
            for side in ("attacker", "defender"):
                d = p.driver(row[f"{side}_id"])
                assert d.pace_residual_s == row[f"{side}_expected_outlap_pace_residual_s"]
                assert (d.pace_source or "none") == row[f"{side}_expected_outlap_pace_source"]
                assert d.stop_delta_s == row[f"{side}_expected_stop_delta_s"]
            checked += 1
    assert checked > 0
    p24 = PRI.load_priors(spark, sample["lake"], 2024, 5)
    assert p24.pit_loss_source == "global_prior" and len(p24.team_keys) == 20
    assert p24.team_keys.get("tsunoda") == "rb"


def test_only_the_target_partition_is_read(spark: SparkSession, tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    root = f"file://{lake}/features"
    df = spark.createDataFrame(
        [(2024, 5, 21.0, "global_prior")],
        "season int, round int, pit_loss_prior_s double, pit_loss_source string",
    )
    df.write.partitionBy("season", "round").parquet(f"{root}/pit_loss")
    junk = lake / "features" / "pit_loss" / "season=2025" / "round=1"
    junk.mkdir(parents=True)
    (junk / "part-0.parquet").write_bytes(b"not parquet")  # reading it would fail
    out = PRI.read_race_partition(spark, root, "pit_loss", FS.FEATURE_SCHEMAS["pit_loss"], 2024, 5)
    assert [r["pit_loss_prior_s"] for r in out.collect()] == [21.0]
    empty = PRI.read_race_partition(
        spark, root, "pit_loss", FS.FEATURE_SCHEMAS["pit_loss"], 2024, 6
    )
    assert empty.count() == 0 and empty.columns == FS.FEATURE_SCHEMAS["pit_loss"].fieldNames()


def test_a_missing_table_is_an_error(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(PRI.PriorsError, match="table not found"):
        PRI.read_race_partition(
            spark, f"file://{tmp_path}", "pit_loss", FS.FEATURE_SCHEMAS["pit_loss"], 2024, 5
        )


def test_the_test_season_is_refused_before_any_read(spark: SparkSession) -> None:
    with pytest.raises(PRI.PriorsError, match="sealed"):
        PRI.load_priors(spark, "file:///does/not/exist", 2025, 1)


def test_a_race_without_rows_has_null_priors(spark: SparkSession) -> None:
    pit = spark.createDataFrame([], FS.FEATURE_SCHEMAS["pit_loss"])
    out = spark.createDataFrame([], FS.FEATURE_SCHEMAS["outlap_features"])
    res = spark.createDataFrame(
        [(2024, 5, "a", "alphatauri"), (2024, 5, "b", None)],
        "season int, round int, driver_id string, constructor_id string",
    )
    p = PRI.priors_from_frames(pit, out, res, 2024, 5)
    assert p == RacePriors(2024, 5, None, None, {"a": "rb", "b": None}, {})


def test_two_pit_loss_rows_are_an_error(spark: SparkSession) -> None:
    pit = spark.createDataFrame(
        [(2024, 5, 1.0, "x"), (2024, 5, 2.0, "y")],
        "season int, round int, pit_loss_prior_s double, pit_loss_source string",
    )
    out = spark.createDataFrame([], FS.FEATURE_SCHEMAS["outlap_features"])
    res = spark.createDataFrame(
        [], "season int, round int, driver_id string, constructor_id string"
    )
    with pytest.raises(PRI.PriorsError, match="2 pit_loss rows"):
        PRI.priors_from_frames(pit, out, res, 2024, 5)
