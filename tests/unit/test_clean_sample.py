"""The whole clean job on the real 2-race sample bundle (data/sample), written to a tmp lake."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from f1sa.config import Settings
from f1sa.etl import clean
from f1sa.etl.clean import CleanError, CleanResult, read_clean, run_clean
from f1sa.lake import schemas as S

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE = REPO_ROOT / "data" / "sample"
EXPECTED_ROWS = {
    "races": 2,
    "drivers": 40,
    "constructors": 20,
    "results": 40,
    "qualifying": 40,
    "laps": 2168,  # 1138 (2023 R5) + 1030 (2024 R5)
    "pitstops": 60,  # 20 + 40
    "stints": 100,  # synthetic: 40 + 60
    "telemetry_lap": 2168,
    "telemetry_raw": 0,  # FastF1 only
}

pytestmark = pytest.mark.skipif(
    not (SAMPLE / "MANIFEST.json").exists(), reason="sample bundle missing (make sample-data)"
)


def _settings(base: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        data_lake_uri=f"file://{base}/data/lake",
        sample_dir=SAMPLE,
    )


class Run:
    def __init__(self, base: Path, result: CleanResult) -> None:
        self.base = base
        self.result = result
        self.clean = base / "data" / "lake" / "clean"


@pytest.fixture(scope="module")
def run(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Run]:
    base = tmp_path_factory.mktemp("clean_sample")
    cwd = os.getcwd()
    os.chdir(base)  # the output guard resolves ./data/lake against the working directory
    try:
        result = run_clean(spark, _settings(base), source="sample", out=lambda _m: None)
        yield Run(base, result)
    finally:
        os.chdir(cwd)


def test_exact_row_counts(run: Run) -> None:
    assert run.result.counts == EXPECTED_ROWS
    assert run.result.races == [(2023, 5), (2024, 5)]


def test_partition_folders_and_report(run: Run) -> None:
    for table in EXPECTED_ROWS:
        if EXPECTED_ROWS[table] == 0:
            continue
        for season in (2023, 2024):
            assert (run.clean / table / f"season={season}" / "round=5").is_dir(), table
    report = json.loads((run.clean / "_quality" / "report.json").read_text())
    assert report["quality"]["status"] == "PASS" and report["counts"] == EXPECTED_ROWS


def test_quality_passes_with_no_soft_issues(run: Run) -> None:
    assert run.result.ok
    assert run.result.quality is not None
    assert run.result.quality.hard_failures == [] and run.result.quality.soft_issues == []


def test_stored_schemas_match_struct_types(spark: SparkSession, run: Run) -> None:
    for table, schema in S.CLEAN_SCHEMAS.items():
        if EXPECTED_ROWS[table] == 0:
            continue
        stored = spark.read.parquet(str(run.clean / table)).schema
        assert {f.name: f.dataType for f in stored.fields} == {
            f.name: f.dataType for f in schema.fields
        }, table


def test_times_in_seconds(spark: SparkSession, run: Run) -> None:
    res = read_clean(spark, f"file://{run.clean}", "results")
    ver = res.where((F.col("season") == 2023) & (F.col("driver_id") == "max_verstappen")).first()
    assert ver is not None
    assert ver["fastest_lap_time_s"] == pytest.approx(89.708)
    assert ver["race_time_s"] == pytest.approx(5258.241)
    assert ver["finished"] is True and ver["classified"] is True
    retired = res.where((F.col("season") == 2024) & (F.col("status") == "Retired")).count()
    assert retired == 3


def test_gaps_at_the_timing_line(spark: SparkSession, run: Run) -> None:
    laps = read_clean(spark, f"file://{run.clean}", "laps")
    leaders = laps.where(F.col("position") == 1)
    assert leaders.agg(F.max(F.abs("gap_to_leader_s"))).first()[0] == 0.0  # type: ignore[index]
    assert leaders.where(F.col("gap_ahead_s").isNotNull()).count() == 0
    assert laps.where(F.col("gap_to_leader_s") < 0).count() == 0
    assert laps.where((F.col("position") > 1) & (F.col("gap_ahead_s") < 0)).count() == 0
    # a lapped car (2023: Lapped status) is more than one lap time behind on its last lap
    lapped = laps.where(
        (F.col("season") == 2023) & (F.col("driver_id") == "sargeant") & (F.col("lap") == 56)
    ).first()
    assert lapped is not None and lapped["gap_to_leader_s"] > lapped["lap_time_s"]
    first = laps.where((F.col("lap") == 1) & (F.col("driver_id") == "perez")).first()
    assert first is not None and first["cumulative_time_s"] == first["lap_time_s"]


def test_track_status_heuristic(spark: SparkSession, run: Run) -> None:
    laps = read_clean(spark, f"file://{run.clean}", "laps")
    neutral = {
        (r["season"], r["lap"])
        for r in laps.where(F.col("is_neutralised_lap"))
        .select("season", "lap")
        .distinct()
        .collect()
    }
    # 2024 Chinese GP safety-car run, including the deployment and restart laps (21, 25, 27)
    assert {(2024, lap) for lap in range(21, 32)} == neutral
    assert not any(season == 2023 for season, _ in neutral)
    assert laps.where(F.col("is_red_flag_lap")).count() == 0
    assert {r[0] for r in laps.select("track_status_source").distinct().collect()} == {"heuristic"}
    assert laps.where(F.col("is_sc_lap").isNotNull()).count() == 0  # unknown without FastF1


def test_pit_flags(spark: SparkSession, run: Run) -> None:
    laps = read_clean(spark, f"file://{run.clean}", "laps")
    pits = read_clean(spark, f"file://{run.clean}", "pitstops")
    assert laps.where(F.col("is_pit_in_lap")).count() == pits.count()  # no stop on the last lap
    assert laps.where(F.col("is_pit_out_lap")).count() == pits.count()


def test_synthetic_tables_are_flagged(spark: SparkSession, run: Run) -> None:
    for table in ("stints", "telemetry_lap"):
        df = read_clean(spark, f"file://{run.clean}", table)
        assert df.where(~F.col("is_synthetic") | F.col("is_synthetic").isNull()).count() == 0
        assert {r[0] for r in df.select("source").distinct().collect()} == {"synthetic"}


def test_rerun_is_identical_and_subset_leaves_other_race(spark: SparkSession, run: Run) -> None:
    other = run.clean / "laps" / "season=2024" / "round=5"
    own = run.clean / "laps" / "season=2023" / "round=5"
    cwd = os.getcwd()
    os.chdir(run.base)
    try:
        again = run_clean(spark, _settings(run.base), source="sample", out=lambda _m: None)
        assert again.checksums == run.result.checksums  # full rerun: identical content
        other_files = sorted(p.name for p in other.iterdir())
        own_files = sorted(p.name for p in own.iterdir())
        subset = run_clean(
            spark, _settings(run.base), source="sample", races={(2023, 5)}, out=lambda _m: None
        )
    finally:
        os.chdir(cwd)
    assert subset.races == [(2023, 5)] and subset.counts["laps"] == 1138
    assert sorted(p.name for p in other.iterdir()) == other_files  # untouched by the subset run
    assert sorted(p.name for p in own.iterdir()) != own_files  # rewritten
    assert read_clean(spark, f"file://{run.clean}", "laps").count() == 2168


def test_output_guard_and_cli_errors(tmp_path: Path) -> None:
    outside = Settings(
        _env_file=None, data_lake_uri=f"file://{tmp_path.parent}/elsewhere", sample_dir=SAMPLE
    )  # type: ignore[call-arg]
    with pytest.raises(CleanError, match=r"outside \./data/lake"):
        clean.resolve_roots(outside, "sample")
    missing = Settings(_env_file=None, sample_dir=Path("data/nope"))  # type: ignore[call-arg]
    with pytest.raises(CleanError, match="sample bundle not found"):
        clean.resolve_roots(missing, "sample")
    assert clean.main(["--source", "sample"]) == 2  # cwd is tmp: no data/sample here
    with pytest.raises(SystemExit):
        clean.main(["--source", "nowhere"])
