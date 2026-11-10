"""Constructor lineage: renamed teams share one history key, and unknown ids fail loudly."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from conftest import REPO_ROOT, SampleLake
from f1sa.etl.clean import read_clean
from f1sa.etl.io import load_clean
from f1sa.etl.sql_jobs import run_sql_views
from f1sa.features import lineage
from f1sa.features.build import BuildError, run_build
from f1sa.features.pitloss import team_outlap_features

REAL_CLEAN = REPO_ROOT / "data" / "lake" / "clean"


def test_lineage_is_plain_data() -> None:
    assert lineage.LINEAGE == {"alphatauri": "rb", "alfa": "sauber"}
    assert set(lineage.LINEAGE.values()) <= lineage.STABLE_CONSTRUCTORS
    assert not set(lineage.LINEAGE) & lineage.STABLE_CONSTRUCTORS
    assert lineage.lineage_rows() == [("alfa", "sauber"), ("alphatauri", "rb")]


def test_renamed_teams_share_one_history_key(spark: SparkSession) -> None:
    assert lineage.history_key("alphatauri") == lineage.history_key("rb") == "rb"
    assert lineage.history_key("alfa") == lineage.history_key("sauber") == "sauber"
    assert lineage.history_key("ferrari") == "ferrari"
    df = spark.createDataFrame(
        [(2023, "alphatauri"), (2024, "rb"), (2023, "alfa"), (2024, "sauber"), (2024, "haas")],
        "season int, constructor_id string",
    )
    keys = {
        (r["season"], r["constructor_id"]): r["team_history_key"]
        for r in lineage.with_history_key(df).collect()
    }
    assert keys[(2023, "alphatauri")] == keys[(2024, "rb")] == "rb"
    assert keys[(2023, "alfa")] == keys[(2024, "sauber")] == "sauber"
    assert keys[(2024, "haas")] == "haas"


def test_unknown_constructor_ids(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("ferrari",), ("alphatauri",), ("cadillac",), (None,), ("racing_point",)],
        "constructor_id string",
    )
    assert lineage.unknown_constructor_ids(df) == ["cadillac", "racing_point"]


def test_sample_clean_zone_has_only_known_constructors(
    spark: SparkSession, sample_clean_lake: SampleLake
) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root, ["results", "constructors"])
    assert lineage.unknown_constructor_ids(c["results"]) == []
    assert lineage.unknown_constructor_ids(c["constructors"]) == []


@pytest.mark.skipif(not (REAL_CLEAN / "results").is_dir(), reason="no clean zone in data/lake")
def test_repo_clean_zone_has_only_known_constructors(spark: SparkSession) -> None:
    """Runs on whatever the repo's data/lake/clean holds (read-only). Once the full 2021-2025 data
    is loaded, a missed rename fails here."""
    results = read_clean(spark, f"file://{REAL_CLEAN}", "results")
    assert lineage.unknown_constructor_ids(results) == []


def test_sql_team_history_follows_lineage_on_sample(
    spark: SparkSession, sample_clean_lake: SampleLake
) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root)
    views = run_sql_views(spark, c)
    rows = {(r["season"], r["constructor_id"]): r for r in team_outlap_features(views).collect()}
    rb, sauber = rows[(2024, "rb")], rows[(2024, "sauber")]
    assert (rb["team_history_key"], rb["outlap_source"]) == ("rb", "team_prior")
    assert (sauber["team_history_key"], sauber["outlap_source"]) == ("sauber", "team_prior")
    assert rows[(2023, "alphatauri")]["team_history_key"] == "rb"
    at_2023 = views["outlap_deltas"].where(
        (F.col("season") == 2023) & (F.col("constructor_id") == "alphatauri")
    )
    assert rb["team_prior_n"] == at_2023.count() > 0


def test_build_fails_on_unknown_constructor(
    spark: SparkSession, sample_clean_lake: SampleLake, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lineage, "STABLE_CONSTRUCTORS", lineage.STABLE_CONSTRUCTORS - {"haas"})
    cwd = os.getcwd()
    os.chdir(Path(sample_clean_lake.base))
    try:
        with pytest.raises(BuildError, match=r"\['haas'\]"):
            run_build(spark, sample_clean_lake.settings(), source="sample", out=lambda _m: None)
    finally:
        os.chdir(cwd)
