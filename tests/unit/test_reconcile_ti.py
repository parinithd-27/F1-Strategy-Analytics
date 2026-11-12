"""Lap reconciliation (TracingInsights vs Jolpica): uniqueness, no fan-out, difference causes.

Hand-built frames for every rule, plus the invented TracingInsights fixture race run through
the real ingest and clean path. Counts only; nothing here writes outside tmp_path.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from test_clean_tracinginsights import _ingest, _settings, _write_drivers

from f1sa.etl import clean
from f1sa.etl import clean_tracinginsights as C
from f1sa.etl import reconcile_ti as R
from f1sa.lake import schemas as S
from f1sa.lake import schemas_tracinginsights as TS


def _df(spark: SparkSession, schema: StructType, rows: list[dict[str, Any]]) -> DataFrame:
    return spark.createDataFrame(
        [tuple(r.get(f.name) for f in schema.fields) for r in rows], schema
    )


def _laps(spark: SparkSession, keys: list[tuple[str, int]], season: int = 2024) -> DataFrame:
    rows = [{"season": season, "round": 1, "driver_id": d, "lap": lap} for d, lap in keys]
    return _df(spark, S.LAPS, rows)


def _tel(spark: SparkSession, keys: list[tuple[str, int]], season: int = 2024) -> DataFrame:
    rows = [
        {"season": season, "round": 1, "driver_id": d, "driver_code": d.upper(), "lap": lap}
        for d, lap in keys
    ]
    return _df(spark, TS.TELEMETRY_LAP_REAL, rows)


def _results(spark: SparkSession, rows: list[tuple[str, int, str]]) -> DataFrame:
    return _df(
        spark,
        S.RESULTS,
        [
            {"season": 2024, "round": 1, "driver_id": d, "laps": n, "status": st}
            for d, n, st in rows
        ],
    )


def _stints(spark: SparkSession, rows: list[tuple[str, int, int, int]]) -> DataFrame:
    return _df(
        spark,
        TS.STINTS_TRACINGINSIGHTS,
        [
            {"season": 2024, "round": 1, "driver_id": d, "stint": s, "start_lap": a, "end_lap": b}
            for d, s, a, b in rows
        ],
    )


# ============================================================================== uniqueness
def test_key_duplicates_on_each_ti_primary_key(spark: SparkSession) -> None:
    tel = _tel(spark, [("aaa", 1), ("aaa", 2), ("bbb", 1)])
    assert R.key_duplicates(tel, TS.PRIMARY_KEYS_TI["telemetry_lap_real"]) == 0
    tel_dup = tel.unionByName(_tel(spark, [("aaa", 2), ("aaa", 2)]))
    # one key (aaa, 2) occurs three times -> one duplicate key
    assert R.key_duplicates(tel_dup, TS.PRIMARY_KEYS_TI["telemetry_lap_real"]) == 1

    stints = _stints(spark, [("aaa", 1, 1, 3), ("aaa", 2, 4, 5)])
    assert R.key_duplicates(stints, TS.PRIMARY_KEYS_TI["stints_tracinginsights"]) == 0
    stints_dup = stints.unionByName(_stints(spark, [("aaa", 2, 4, 5)]))
    assert R.key_duplicates(stints_dup, TS.PRIMARY_KEYS_TI["stints_tracinginsights"]) == 1

    cov = _df(
        spark, TS.COMPOUND_COVERAGE, [{"season": 2024, "round": 1}, {"season": 2024, "round": 2}]
    )
    assert R.key_duplicates(cov, TS.PRIMARY_KEYS_TI["compound_coverage"]) == 0
    cov_dup = cov.unionByName(_df(spark, TS.COMPOUND_COVERAGE, [{"season": 2024, "round": 2}]))
    assert R.key_duplicates(cov_dup, TS.PRIMARY_KEYS_TI["compound_coverage"]) == 1


def test_stint_lap_overlaps(spark: SparkSession) -> None:
    contiguous = _stints(spark, [("aaa", 1, 1, 3), ("aaa", 2, 4, 5), ("bbb", 1, 1, 5)])
    assert R.stint_lap_overlaps(contiguous) == 0
    shared_lap = _stints(spark, [("aaa", 1, 1, 3), ("aaa", 2, 3, 5)])
    assert R.stint_lap_overlaps(shared_lap) == 1
    null_bounds = _stints(spark, [("aaa", 1, 1, 3)]).unionByName(
        _df(
            spark,
            TS.STINTS_TRACINGINSIGHTS,
            [{"season": 2024, "round": 1, "driver_id": "aaa", "stint": 2}],
        )
    )
    assert R.stint_lap_overlaps(null_bounds) == 0


# ============================================================================== fan-out
def test_left_join_keeps_jolpica_count_and_unmatched_rows(spark: SparkSession) -> None:
    jol = _laps(spark, [("aaa", 1), ("aaa", 2), ("bbb", 1)])
    tel = _tel(spark, [("aaa", 1), ("aaa", 2), ("aaa", 3)])  # bbb lap 1 unmatched
    (row,) = R.left_join_fanout(jol, tel)
    assert row == {
        "season": 2024,
        "n_jolpica": 3,
        "n_after_left_join": 3,
        "n_matched": 2,
        "fanout_ok": True,
    }


def test_left_join_detects_fanout(spark: SparkSession) -> None:
    jol = _laps(spark, [("aaa", 1), ("aaa", 2)])
    tel = _tel(spark, [("aaa", 1), ("aaa", 2), ("aaa", 2)])
    (row,) = R.left_join_fanout(jol, tel)
    assert row["n_after_left_join"] == 3 and row["fanout_ok"] is False


def test_left_join_per_season(spark: SparkSession) -> None:
    jol = _laps(spark, [("aaa", 1)], 2023).unionByName(_laps(spark, [("aaa", 1)], 2024))
    tel = _tel(spark, [("aaa", 1)], 2024)
    rows = R.left_join_fanout(jol, tel)
    assert [(r["season"], r["n_matched"], r["fanout_ok"]) for r in rows] == [
        (2023, 0, True),
        (2024, 1, True),
    ]


# ============================================================================== differences
def _by(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {(r["side"], r["cause"], r["status"]): r for r in rows}


def test_lap_differences_every_cause(spark: SparkSession) -> None:
    jol = _laps(
        spark,
        [("aaa", lap) for lap in (1, 2, 4, 5)]  # aaa lap 3 missing in Jolpica
        + [("bbb", lap) for lap in (1, 2, 3)]  # bbb retired after 3 laps
        + [("ccc", lap) for lap in (1, 2, 3, 4)]  # ccc lapped: TI starts at lap 2
        + [("ddd", 1), ("ddd", 2)],  # ddd absent from TI (mapping)
    )
    tel = _tel(
        spark,
        [("aaa", lap) for lap in (0, 1, 2, 3, 4, 5)]  # lap 0 and lap 3 only in TI
        + [("aaa", 5)]  # duplicate copy
        + [("bbb", lap) for lap in (1, 2, 3, 4)]  # TI keeps the retirement lap 4
        + [("ccc", lap) for lap in (2, 3, 4)]  # Jolpica-only lap 1 before TI's first
        + [("eee", 1)],  # eee absent from Jolpica, results laps 0
    )
    results = _results(
        spark,
        [
            ("aaa", 5, "Finished"),
            ("bbb", 3, "Accident"),
            ("ccc", 4, "+1 Lap"),
            ("ddd", 2, "Lapped"),
            ("eee", 0, "Did not start"),
        ],
    )
    incomplete = spark.createDataFrame(
        [(2024, 1, "bbb", 4, True, True), (2024, 1, "aaa", 3, False, False)],
        "season int, round int, driver_id string, lap int, _incomplete boolean, "
        "_tel_not_full boolean",
    )
    got = _by(R.lap_differences(jol, tel, results, incomplete))
    assert got[("ti_only", "lap_le_0", "finished")]["n"] == 1
    assert got[("ti_only", "duplicate_key", "finished")]["n"] == 1
    assert got[("ti_only", "gap_inside_range", "finished")]["n"] == 1
    bbb = got[("ti_only", "after_last_lap_other", "retired")]
    assert (bbb["n"], bbb["n_lap_incomplete"], bbb["n_lap_is_results_laps_plus_1"]) == (1, 1, 1)
    assert bbb["n_telemetry_not_full"] == 1
    assert got[("ti_only", "gap_inside_range", "finished")]["n_telemetry_not_full"] == 0
    eee = got[("ti_only", "driver_absent_other", "retired")]
    assert (eee["n"], eee["n_results_laps_0"]) == (1, 1)
    assert got[("jolpica_only", "before_first_lap_other", "lapped")]["n"] == 1
    assert got[("jolpica_only", "driver_absent_other", "lapped")]["n"] == 2
    assert sum(r["n"] for r in got.values()) == 8
    # totals reconcile: TI rows - Jolpica rows = ti_only - jolpica_only
    totals = {r["side"]: r["n"] for r in R.difference_totals(list(got.values()))}
    assert totals["ti_only"] - totals["jolpica_only"] == tel.count() - jol.count()


def test_lap_differences_priority_and_no_result(spark: SparkSession) -> None:
    jol = _laps(spark, [("aaa", 1)])
    tel = _tel(spark, [("aaa", 1), ("zzz", 0), ("zzz", 0)])  # lap 0 beats absent driver and dup
    rows = R.lap_differences(jol, tel, _results(spark, [("aaa", 1, "Finished")]))
    assert [(r["side"], r["cause"], r["status"], r["n"]) for r in rows] == [
        ("ti_only", "lap_le_0", "no_result", 2)
    ]
    assert rows[0]["n_lap_incomplete"] == 0 and rows[0]["n_telemetry_not_full"] == 0


def test_lap_differences_empty_when_identical(spark: SparkSession) -> None:
    keys = [("aaa", 1), ("aaa", 2)]
    assert R.lap_differences(_laps(spark, keys), _tel(spark, keys), _results(spark, [])) == []
    assert R.difference_totals([]) == []


# ============================================================================== coverage
def test_coverage_below_threshold_boundary(spark: SparkSession) -> None:
    cov = _df(
        spark,
        TS.COMPOUND_COVERAGE,
        [
            {"season": 2025, "round": 1, "compound_share": 0.95},
            {"season": 2025, "round": 2, "compound_share": 0.9499},
            {"season": 2025, "round": 3, "compound_share": 0.5},
        ],
    )
    races = _df(
        spark,
        S.RACES,
        [{"season": 2025, "round": r, "race_name": f"Race {r}"} for r in (1, 2)],
    )
    assert R.coverage_below(cov, races) == [
        {"season": 2025, "round": 2, "race_name": "Race 2", "compound_share": 0.9499},
        {"season": 2025, "round": 3, "race_name": None, "compound_share": 0.5},
    ]


def test_read_raw_lap_times_missing_is_empty(spark: SparkSession, tmp_path: Path) -> None:
    df = R.read_raw_lap_times(spark, f"file://{tmp_path}/raw", [2024])
    assert df.isEmpty() and "lap_time_s" in df.columns


def test_format_table() -> None:
    assert R.format_table([], ["a"]) == "  (none)"
    text = R.format_table([{"a": 1, "bb": "x"}], ["a", "bb"])
    assert text.splitlines() == ["  a  bb", "  1  x "]


# ============================================================================== fixture race
class Lake:
    def __init__(self, base: Path) -> None:
        self.base = base
        self.clean_root = f"file://{base}/data/lake/clean"


def _write_v1(spark: SparkSession, root: str) -> None:
    # aaa and ccc: 5 laps; bbb: 4 laps in Jolpica (retired); ddd: 2 laps, no TracingInsights rows
    keys = [("aaa", n) for n in range(1, 6)] + [("bbb", n) for n in range(1, 5)]
    keys += [("ccc", n) for n in range(1, 6)] + [("ddd", 1), ("ddd", 2)]
    clean.write_table(_laps(spark, keys), root, "laps")
    clean.write_table(
        _results(
            spark,
            [
                ("aaa", 5, "Finished"),
                ("bbb", 4, "Engine"),
                ("ccc", 5, "Finished"),
                ("ddd", 2, "Gearbox"),
            ],
        ),
        root,
        "results",
    )
    races = _df(
        spark,
        S.RACES,
        [
            {
                "season": 2024,
                "round": 1,
                "race_name": "Testland Grand Prix",
                "race_date": date(2024, 1, 1),
            }
        ],
    )
    clean.write_table(races, root, "races")


@pytest.fixture(scope="module")
def lake(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Lake]:
    base = tmp_path_factory.mktemp("reconcile_ti")
    cwd = os.getcwd()
    os.chdir(base)
    (base / "data").mkdir()
    try:
        _ingest(spark, base, 2024, 1)
        _write_drivers(spark, base, [("aaa", "AAA", 1), ("bbb", "BBB", 2), ("ccc", None, 3)])
        C.run_clean_ti(spark, _settings(base), [2024])
        _write_v1(spark, f"file://{base}/data/lake/clean")
        yield Lake(base)
    finally:
        os.chdir(cwd)


def test_fixture_tables_unique_and_no_fanout(spark: SparkSession, lake: Lake) -> None:
    ti = {t: C.read_ti_clean(spark, lake.clean_root, t) for t in TS.CLEAN_SCHEMAS_TI}
    for table in ("telemetry_lap_real", "stints_tracinginsights", "compound_coverage"):
        assert R.key_duplicates(ti[table], TS.PRIMARY_KEYS_TI[table]) == 0, table
    assert R.stint_lap_overlaps(ti["stints_tracinginsights"]) == 0
    jol = clean.read_clean(spark, lake.clean_root, "laps")
    (row,) = R.left_join_fanout(jol, ti["telemetry_lap_real"])
    assert row["fanout_ok"] and row["n_after_left_join"] == row["n_jolpica"] == 16
    assert row["n_matched"] == 14


def _snapshot(base: Path) -> dict[str, tuple[int, int]]:
    return {
        str(p.relative_to(base)): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in sorted(base.rglob("*"))
        if p.is_file()
    }


def test_run_reconcile_on_fixture_writes_nothing(
    spark: SparkSession, lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = _snapshot(lake.base)
    report = R.run_reconcile(spark, _settings(lake.base), [2024])
    assert report.ok
    assert report.seasons == [
        {
            "season": 2024,
            "jolpica_laps": 16,
            "telemetry_lap_real": 15,
            "ti_raw_laps": 15,
            "ti_raw_telemetry_agg": 15,
        }
    ]
    got = _by(report.differences)
    assert got[("ti_only", "after_last_lap_other", "retired")]["n"] == 1  # bbb lap 5
    assert got[("ti_only", "after_last_lap_other", "retired")]["n_lap_is_results_laps_plus_1"] == 1
    assert got[("jolpica_only", "driver_absent_other", "retired")]["n"] == 2  # ddd
    assert report.below_threshold == [
        {"season": 2024, "round": 1, "race_name": "Testland Grand Prix", "compound_share": 0.6}
    ]

    monkeypatch.setattr(R, "get_settings", lambda: _settings(lake.base))
    assert R.main(["--seasons", "2024"], spark=spark) == 0
    assert _snapshot(lake.base) == before


def test_main_fails_on_duplicate_and_missing_input(
    spark: SparkSession,
    lake: Lake,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    report = R.ReconcileReport(duplicates={"telemetry_lap_real": 1})
    assert not report.ok and "result: FAIL" in R.render(report)
    assert not R.ReconcileReport(fanout=[{"fanout_ok": False}]).ok
    assert not R.ReconcileReport(stint_lap_overlaps=1).ok

    monkeypatch.setattr(R, "run_reconcile", lambda *a, **k: report)
    monkeypatch.setattr(R, "get_settings", lambda: _settings(lake.base))
    assert R.main([], spark=spark) == 1
    assert "n_dup_keys" in capsys.readouterr().out
    monkeypatch.undo()

    (tmp_path / "data").mkdir()
    monkeypatch.setattr(R, "get_settings", lambda: _settings(tmp_path))
    assert R.main(["--seasons", "2024"], spark=spark) == 2
    assert "no v1 clean laps" in capsys.readouterr().err


def test_module_has_no_write_path() -> None:
    source = Path(R.__file__).read_text(encoding="utf-8")
    for forbidden in (".write", "write_text", "write_table", "Sink", "open("):
        assert forbidden not in source, forbidden
