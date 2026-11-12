"""Additive TracingInsights clean tables on the invented fixture race, plus v1 isolation."""

from __future__ import annotations

import io
import json
import os
import shutil
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from _ti_fixtures import FakeFetcher, FakeTree, disk, make_pin
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from f1sa.config import Settings
from f1sa.etl import clean
from f1sa.etl import clean_tracinginsights as C
from f1sa.etl.clean import CleanError, run_clean
from f1sa.ingest import tracinginsights_loader as L
from f1sa.lake import schemas as S
from f1sa.lake import schemas_tracinginsights as TS

REPO = Path(__file__).resolve().parents[2]
SAMPLE = REPO / "data" / "sample"


def _settings(base: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        data_lake_uri=f"file://{base}/data/lake",
        tracinginsights_manifest_path=Path("data/manifest_tracinginsights.json"),
        sample_dir=SAMPLE,
    )


def _ingest(spark: SparkSession, base: Path, season: int, round_: int) -> None:
    pin = make_pin(season, round_)
    fetcher = FakeFetcher([pin])
    code = L.TracingInsightsIngest(
        _settings(base),
        L.RunOptions(seasons=[season], raw_telemetry_seasons=()),
        fetcher=fetcher,
        table_fetcher=fetcher,
        tree_index=FakeTree(),
        spark_factory=lambda: spark,
        disk_usage=disk(100),
        pins={(season, round_): pin},
        out=io.StringIO(),
        err=io.StringIO(),
    ).run()
    assert code == 0


def _write_drivers(spark: SparkSession, base: Path, rows: list[tuple[Any, ...]]) -> None:
    full = [
        (2024, 1, did, code, None, num, "G", "F", date(2000, 1, 1), "X", "team")
        for did, code, num in rows
    ]
    df = spark.createDataFrame(full, S.DRIVERS)
    clean.write_table(df, f"file://{base}/data/lake/clean", "drivers")


class Run:
    def __init__(self, base: Path, result: C.CleanTiResult) -> None:
        self.base = base
        self.result = result
        self.root = f"file://{base}/data/lake/clean"


@pytest.fixture(scope="module")
def run(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Run]:
    base = tmp_path_factory.mktemp("clean_ti")
    cwd = os.getcwd()
    os.chdir(base)
    (base / "data").mkdir()
    try:
        _ingest(spark, base, 2024, 1)
        # CCC has no code in the v1 table: mapped by car number
        _write_drivers(spark, base, [("aaa", "AAA", 1), ("bbb", "BBB", 2), ("ccc", None, 3)])
        yield Run(base, C.run_clean_ti(spark, _settings(base), [2024]))
    finally:
        os.chdir(cwd)


def _rows(spark: SparkSession, run: Run, table: str) -> list[dict[str, Any]]:
    df = C.read_ti_clean(spark, run.root, table)
    keys = list(TS.PRIMARY_KEYS_TI[table])
    return [r.asDict() for r in df.orderBy(*keys).collect()]


def test_counts_and_schemas(spark: SparkSession, run: Run) -> None:
    assert run.result.counts == {
        "stints_tracinginsights": 4,
        "race_control": 5,
        "race_control_track_status": 3,
        "telemetry_lap_real": 15,
        "compound_coverage": 1,
    }
    for table, schema in TS.CLEAN_SCHEMAS_TI.items():
        stored = spark.read.parquet(f"{run.root}/{table}")
        assert sorted(stored.columns) == sorted(schema.fieldNames())


def test_stints(spark: SparkSession, run: Run) -> None:
    got = {(r["driver_id"], r["stint"]): r for r in _rows(spark, run, "stints_tracinginsights")}
    a1, a2 = got[("aaa", 1)], got[("aaa", 2)]
    assert (a1["start_lap"], a1["end_lap"], a1["laps"], a1["compound"]) == (1, 3, 3, "MEDIUM")
    assert a1["tyre_age_at_start"] == 0 and a1["fresh_tyre"] is True
    assert (a2["start_lap"], a2["compound"], a2["tyre_age_at_start"], a2["fresh_tyre"]) == (
        4,
        "HARD",
        2,
        False,
    )
    b = got[("bbb", 1)]  # one "None" lap does not hide the stint's compound
    assert b["compound"] == "SOFT" and b["n_laps_compound_null"] == 1 and not b["compound_missing"]
    c = got[("ccc", 1)]  # UNKNOWN on every lap: null + missing indicator, never imputed
    assert c["compound"] is None and c["compound_missing"] and c["n_laps_compound_null"] == 5
    assert all(
        r["is_synthetic"] is False and r["source"] == "tracinginsights" for r in got.values()
    )
    assert not any(r["compound_conflict"] for r in got.values())


def test_compound_conflict_is_flagged_not_resolved(spark: SparkSession) -> None:
    laps = spark.createDataFrame(
        [
            (2024, 1, "AAA", "1", lap, comp, 1, lap, True, 1, 0.0, 1.0)
            for lap, comp in ((1, "SOFT"), (2, "MEDIUM"), (3, None))
        ],
        TS.RAW_LAPS,
    )
    mapping = spark.createDataFrame(
        [(2024, 1, "AAA", "aaa")], "season int, round int, driver_code string, driver_id string"
    )
    row = C.build_stints(laps, mapping).first()
    assert row is not None
    assert row["compound_conflict"] and row["compound"] is None and row["compound_missing"]


def test_race_control(spark: SparkSession, run: Run) -> None:
    rows = _rows(spark, run, "race_control")
    assert [r["lap"] for r in rows] == [2, 3, 4, 4, 5]
    assert [r["is_sc_deployed"] for r in rows] == [True, False, False, False, False]
    assert [r["is_vsc_deployed"] for r in rows] == [False, True, False, False, False]
    assert [r["is_red_flag"] for r in rows] == [False, False, True, False, False]
    assert [r["is_penalty"] for r in rows] == [False, False, False, True, False]
    assert rows[3]["driver_number"] == 2 and rows[3]["driver_id"] == "bbb"  # from the text
    assert rows[4]["driver_number"] == 3 and rows[4]["driver_id"] == "ccc"  # from dNum
    assert rows[0]["driver_id"] is None
    assert str(rows[0]["time_utc"]) == "2024-03-01 15:10:00"


def test_track_status_timeline_maps_to_leader_laps(spark: SparkSession, run: Run) -> None:
    rows = _rows(spark, run, "race_control_track_status")
    got = [
        (r["start_session_s"], r["end_session_s"], r["status"], r["lap_from"], r["lap_to"])
        for r in rows
    ]
    # leader AAA: lap L spans [1000 + 90 (L-1), 1000 + 90 L)
    assert got == [
        (0.0, 1150.0, "1", 1, 2),
        (1150.0, 1300.0, "4", 2, 4),
        (1300.0, None, "1", 4, 5),
    ]
    assert [r["is_sc"] for r in rows] == [False, True, False]


def test_telemetry_lap_real(spark: SparkSession, run: Run) -> None:
    rows = _rows(spark, run, "telemetry_lap_real")
    assert len(rows) == 15 and {r["driver_id"] for r in rows} == {"aaa", "bbb", "ccc"}
    ccc = [r for r in rows if r["driver_id"] == "ccc"]
    assert all(r["n_samples"] == 0 and r["telemetry_coverage"] == "none" for r in ccc)
    assert all(r["is_synthetic"] is False for r in rows)
    aaa1 = next(r for r in rows if r["driver_id"] == "aaa" and r["lap"] == 1)
    assert aaa1["n_samples"] == 6 and aaa1["mean_speed_kph"] == 212.5


def test_compound_coverage_and_report(spark: SparkSession, run: Run) -> None:
    (row,) = _rows(spark, run, "compound_coverage")
    assert row["n_lap_rows"] == 15 and row["n_compound_known"] == 9
    assert row["n_compound_unknown_label"] == 5 and row["n_compound_null"] == 1
    assert row["compound_share"] == 0.6 and row["below_95pct"] is True
    assert row["n_tyre_life_known"] == 14 and row["n_drivers_unmapped"] == 0
    assert (row["n_rcm_messages"], row["n_rcm_with_lap"], row["n_telemetry_laps"]) == (5, 5, 15)
    report = json.loads((run.base / "data/lake/clean/_quality/tracinginsights.json").read_text())
    assert report["races"] == 1 and report["races_below_95pct"][0]["round"] == 1
    csv_text = C.coverage_csv(run.result.coverage_rows)
    assert csv_text.splitlines()[0] == ",".join(TS.COMPOUND_COVERAGE.fieldNames())


def test_rerun_is_identical(spark: SparkSession, run: Run) -> None:
    cwd = os.getcwd()
    os.chdir(run.base)
    try:
        again = C.run_clean_ti(spark, _settings(run.base), [2024])
    finally:
        os.chdir(cwd)
    assert again.checksums == run.result.checksums


def test_unmapped_driver_is_counted_and_left_out(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    _ingest(spark, tmp_path, 2024, 1)
    _write_drivers(spark, tmp_path, [("aaa", "AAA", 1), ("bbb", "BBB", 2)])
    result = C.run_clean_ti(spark, _settings(tmp_path), [2024])
    assert result.coverage_rows[0]["n_drivers_unmapped"] == 1
    assert (
        result.counts["stints_tracinginsights"] == 3 and result.counts["telemetry_lap_real"] == 10
    )


def test_guards(spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    with pytest.raises(CleanError, match="no TracingInsights laps"):
        C.run_clean_ti(spark, _settings(tmp_path), [2024])
    _ingest(spark, tmp_path, 2024, 1)
    with pytest.raises(CleanError, match="no v1 clean drivers"):
        C.run_clean_ti(spark, _settings(tmp_path), [2024])
    outside = Settings(_env_file=None, data_lake_uri=f"file://{tmp_path}/elsewhere")  # type: ignore[call-arg]
    with pytest.raises(CleanError, match=r"outside \./data/lake"):
        C.run_clean_ti(spark, outside, [2024])
    assert C.main(["--results-dir", "/tmp/x"]) == 2


def test_clean_py_never_reads_tracinginsights() -> None:
    source = (REPO / "src/f1sa/etl/clean.py").read_text()
    assert "tracinginsights" not in source.lower()
    assert not set(TS.CLEAN_SCHEMAS_TI) & set(clean.CLEAN_TABLES)
    assert set(clean.CLEAN_TABLES) == {
        "races",
        "drivers",
        "constructors",
        "results",
        "qualifying",
        "laps",
        "pitstops",
        "stints",
        "telemetry_lap",
        "telemetry_raw",
    }


@pytest.mark.skipif(not (SAMPLE / "MANIFEST.json").exists(), reason="sample bundle missing")
def test_v1_clean_is_identical_with_tracinginsights_raw_present(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """v1 clean in lake mode on the sample, before and after TracingInsights rows for one of
    its races (2024 R5) land in the raw zone: every table checksum is unchanged."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    shutil.copytree(SAMPLE / "raw", tmp_path / "data/lake/raw")
    before = run_clean(spark, _settings(tmp_path), source="lake", out=lambda _m: None)
    _ingest(spark, tmp_path, 2024, 5)
    assert (tmp_path / "data/lake/raw/tracinginsights/laps/season=2024/round=5").is_dir()
    after = run_clean(spark, _settings(tmp_path), source="lake", out=lambda _m: None)
    assert before.ok and after.ok
    assert after.checksums == before.checksums and after.counts == before.counts
    stints = spark.read.parquet(f"file://{tmp_path}/data/lake/clean/stints")
    assert stints.where(F.col("source") == "tracinginsights").count() == 0
