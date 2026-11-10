"""Feature build end to end on the sample clean zone, plus the io helpers and feature wrappers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from _feature_fixtures import frame, lap
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from conftest import SAMPLE_DIR, SampleLake
from f1sa.config import Settings
from f1sa.etl.clean import CleanError
from f1sa.etl.io import load_clean, race_keys, resolve_feature_roots, restrict, sample_races
from f1sa.features import build
from f1sa.features import schemas as FS
from f1sa.features.build import BuildError, read_feature, run_build
from f1sa.features.gaps import gap_features, gaps_from_enriched
from f1sa.features.stints import stint_features

SAMPLE_RACES = [(2023, 5), (2024, 5)]


def _settings(lake: Path, sample: Path = SAMPLE_DIR) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        data_lake_uri=f"file://{lake}",
        sample_dir=sample,
    )


@pytest.fixture(scope="module")
def built(spark: SparkSession, sample_clean_lake: SampleLake) -> dict[str, Any]:
    """Two builds into the session sample lake. The first runs with a junk file in clean/_quality,
    which must never be read."""
    junk = sample_clean_lake.lake / "clean" / "_quality" / "junk.parquet"
    junk.write_bytes(b"not parquet at all")
    cwd = os.getcwd()
    os.chdir(sample_clean_lake.base)
    try:
        lines: list[str] = []
        first = run_build(spark, sample_clean_lake.settings(), source="sample", out=lines.append)
        second = run_build(spark, sample_clean_lake.settings(), source="lake", out=lambda _m: None)
    finally:
        os.chdir(cwd)
        junk.unlink()
    return {"first": first, "second": second, "lines": lines, "lake": sample_clean_lake}


def test_build_writes_every_table_for_both_sample_races(built: dict[str, Any]) -> None:
    result = built["first"]
    features = built["lake"].lake / "features"
    assert result.races == SAMPLE_RACES
    assert set(result.counts) == set(FS.FEATURE_TABLES)
    for table in FS.FEATURE_TABLES:
        assert result.partitions[table] == SAMPLE_RACES, table
        for season, round_ in SAMPLE_RACES:
            part = features / table / f"season={season}" / f"round={round_}"
            assert part.is_dir() and any(part.glob("*.parquet")), part
    assert result.counts["laps_enriched"] == result.counts["gaps"] == 2168
    assert result.counts["stints"] == 100
    assert result.counts["pit_loss"] == result.counts["race_summary"] == 2
    assert result.counts["lap_time_distribution"] == 2
    assert result.counts["team_outlap_history"] == 20
    assert result.counts["telemetry_lap_features"] == 2168
    assert result.counts["outlap_features"] == 100
    assert result.counts["undercut_attempts"] == 303  # 9 (G, k) combinations per attempt
    assert any("laps_enriched" in line and "season=2024/round=5" in line for line in built["lines"])
    assert any("SMOKE TEST ONLY" in line for line in built["lines"])


def test_sample_build_writes_smoke_test_counts(built: dict[str, Any]) -> None:
    path = built["first"].counts_csv
    assert path is not None and path.name == "undercut_counts_sample.csv"
    assert built["second"].counts_csv is None  # --source lake writes no sample counts
    lines = path.read_text().splitlines()
    assert lines[0].startswith("# SMOKE TEST ONLY")
    assert len(lines) == 2 + 2 * 9  # header line, column names, 2 seasons x 9 (G, k)
    assert all(",true," in line for line in lines[2:])


def test_results_dir_outside_results_is_refused(
    spark: SparkSession, sample_clean_lake: SampleLake, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(sample_clean_lake.base)
    with pytest.raises(BuildError, match="outside"):
        run_build(
            spark,
            sample_clean_lake.settings(),
            source="sample",
            results_dir=sample_clean_lake.base / "elsewhere",
        )
    build.check_results_dir(Path("results"))
    build.check_results_dir(Path("results") / "sub")


def test_main_rejects_results_dir_outside_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{tmp_path}/data/lake")
    monkeypatch.setenv("SAMPLE_DIR", str(SAMPLE_DIR))
    assert build.main(["--source", "sample", "--results-dir", "/tmp"]) == 2
    assert "outside" in capsys.readouterr().err


def test_sparse_table_partitions_are_deleted(spark: SparkSession, tmp_path: Path) -> None:
    root = f"file://{tmp_path}/features"
    part = tmp_path / "features" / "undercut_attempts" / "season=2023" / "round=5"
    part.mkdir(parents=True)
    (part / "x.parquet").write_bytes(b"stale")
    build.delete_partitions(spark, root, "undercut_attempts", [(2023, 5), (2024, 5)])
    assert not part.exists()


def test_rebuild_is_idempotent(built: dict[str, Any]) -> None:
    assert built["first"].checksums == built["second"].checksums
    assert built["first"].counts == built["second"].counts


def test_stored_tables_have_feature_schemas(spark: SparkSession, built: dict[str, Any]) -> None:
    root = built["first"].features_root
    for table, schema in FS.FEATURE_SCHEMAS.items():
        df = spark.read.parquet(f"{root}/{table}")
        stored = {f.name: f.dataType for f in df.schema}
        assert stored == {f.name: f.dataType for f in schema}, table
    st = read_feature(spark, root, "stints")
    assert st.where(~F.col("matches_clean")).count() == 0


def test_missing_clean_zone_is_an_error(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(BuildError, match="make etl-sample"):
        run_build(spark, _settings(tmp_path / "data" / "lake"), source="lake")


def test_sample_race_missing_from_clean_is_an_error(
    spark: SparkSession,
    sample_clean_lake: SampleLake,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sample = tmp_path / "sample"
    for s, r in [*SAMPLE_RACES, (2025, 1)]:
        (sample / "raw" / "jolpica" / "results" / f"season={s}" / f"round={r}").mkdir(parents=True)
    monkeypatch.chdir(sample_clean_lake.base)
    with pytest.raises(BuildError, match=r"\[\(2025, 1\)\] are not in the clean zone"):
        run_build(spark, _settings(sample_clean_lake.lake, sample), source="sample")


def test_main_rejects_output_outside_data_lake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{tmp_path}/elsewhere")
    assert build.main(["--source", "lake"]) == 2
    assert "refusing to write outside ./data/lake" in capsys.readouterr().err


def test_main_missing_sample_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{tmp_path}/data/lake")
    monkeypatch.setenv("SAMPLE_DIR", str(tmp_path / "nope"))
    assert build.main(["--source", "sample"]) == 2
    assert "make sample-data" in capsys.readouterr().err


class _NoStop:
    """Proxy for the shared test session: main() must not stop it."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def __getattr__(self, name: str) -> Any:
        return getattr(self._spark, name)

    def stop(self) -> None:
        return None


def test_main_empty_clean_zone_exits_1(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{tmp_path}/data/lake")
    monkeypatch.setattr(build, "build_spark", lambda *_a, **_k: _NoStop(spark))
    assert build.main(["--source", "lake"]) == 1
    assert "is empty" in capsys.readouterr().err


# ------------------------------------------------------------------ io helpers
def test_resolve_feature_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    clean, features = resolve_feature_roots(_settings(tmp_path / "data" / "lake"))
    assert clean == f"file://{tmp_path}/data/lake/clean"
    assert features == f"file://{tmp_path}/data/lake/features"
    with pytest.raises(CleanError, match="outside"):
        resolve_feature_roots(_settings(tmp_path / "other"))


def test_sample_races_scans_partitions(tmp_path: Path) -> None:
    assert sample_races(_settings(tmp_path)) == SAMPLE_RACES
    base = tmp_path / "s" / "raw" / "jolpica" / "results"
    with pytest.raises(CleanError, match="not found"):
        sample_races(_settings(tmp_path, tmp_path / "s"))
    (base / "season=2024" / "round=3").mkdir(parents=True)
    (base / "season=2024" / "notes").mkdir()
    (base / "README").parent.mkdir(parents=True, exist_ok=True)
    (base / "README").write_text("x")
    assert sample_races(_settings(tmp_path, tmp_path / "s")) == [(2024, 3)]
    (base / "season=2024" / "round=3").rmdir()
    with pytest.raises(CleanError, match="lists no races"):
        sample_races(_settings(tmp_path, tmp_path / "s"))


def test_load_clean_restrict_and_race_keys(
    spark: SparkSession, sample_clean_lake: SampleLake
) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root, ["races", "laps"])
    assert set(c) == {"races", "laps"}
    assert race_keys(c["laps"]) == SAMPLE_RACES
    only = restrict(c, {(2024, 5)})
    assert race_keys(only["laps"]) == race_keys(only["races"]) == [(2024, 5)]
    with pytest.raises(CleanError, match="unknown clean table"):
        load_clean(spark, sample_clean_lake.clean_root, ["_quality"])


# ------------------------------------------------------------------ wrappers
def test_gap_wrappers_agree(spark: SparkSession, sample_clean_lake: SampleLake) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root, ["laps"])
    direct = gap_features(c["laps"])
    assert [f.name for f in direct.schema] == [f.name for f in FS.GAPS]
    from f1sa.etl.df_jobs import timing_gaps

    via = gaps_from_enriched(timing_gaps(c["laps"].drop("cumulative_time_s")))
    assert direct.exceptAll(via).count() == 0 and direct.count() == 2168


def test_stint_features_flag_boundaries_not_in_clean(spark: SparkSession) -> None:
    laps = frame(
        spark,
        "laps",
        [lap(2024, 1, "a", n, 1, 90.0, pit_in=n == 2) for n in range(1, 5)],
    )
    clean = frame(
        spark,
        "stints",
        [
            {
                "season": 2024,
                "round": 1,
                "driver_id": "a",
                "stint": 1,
                "start_lap": 1,
                "end_lap": 2,
                "laps": 2,
                "compound": "SOFT",
                "tyre_age_at_start": 1,
                "is_synthetic": True,
                "source": "synthetic",
            },
            {
                "season": 2024,
                "round": 1,
                "driver_id": "a",
                "stint": 2,
                "start_lap": 3,
                "end_lap": 3,
                "laps": 1,
                "compound": "HARD",
                "tyre_age_at_start": 0,
                "is_synthetic": True,
                "source": "synthetic",
            },
        ],
    )
    rows = {r["stint"]: r for r in stint_features(laps, clean).collect()}
    assert rows[1]["matches_clean"] is True and rows[1]["compound"] == "SOFT"
    assert rows[1]["compound_source"] == "synthetic" and rows[1]["tyre_age_at_start"] == 1
    assert rows[2]["matches_clean"] is False and rows[2]["compound"] is None
    assert (rows[2]["start_lap"], rows[2]["end_lap"]) == (3, 4)
