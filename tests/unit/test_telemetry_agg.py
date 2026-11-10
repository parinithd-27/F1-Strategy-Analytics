"""Per-lap telemetry aggregation on a tiny hand-built raw fixture, and the synthetic pass-through.

Raw fixture: race 2024 R1, driver ``d``, lap windows in session seconds:
lap 1 [0,10), lap 2 [10,20), lap 3 [20,30) (no samples), lap 4 [30,40).

===  ======  =====  ========  =====  ====  =====  =======================================
t    lap     speed  throttle  brake  gear  rpm    note
===  ======  =====  ========  =====  ====  =====  =======================================
-1   -       150    100       F      2     9000   before lap 1: dropped
0    1       200    100       F      3     10000  exactly at lap 1 start
4    1       210    97.9      T      4     11000  97.9 < 98: not full throttle
8    1       220    98.0      F      4     12000  98.0 counts as full throttle
10   2       230    100       F      5     10000  exactly on the 1|2 boundary: later lap
15   2       240    50        T      5     10000
30   4       250    98.0      F      6     12000  on the 3|4 boundary; lap 3 has no samples
35   4       260    99        F      7     13000
40   -       270    100       F      7     13000  exactly at the last lap's end: dropped
===  ======  =====  ========  =====  ====  =====  =======================================

The raw rows carry a wrong ``lap`` (99) on purpose, to prove that samples are re-assigned by
session time.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest
from _feature_fixtures import frame, lap
from pyspark.sql import DataFrame, SparkSession

from f1sa.features import schemas as FS
from f1sa.features.telemetry_agg import (
    FULL_THROTTLE_PCT,
    aggregate_samples,
    assign_to_laps,
    lap_windows,
    telemetry_lap_features,
)

SAMPLES = [
    (-1.0, 150.0, 100.0, False, 2, 9000.0),
    (0.0, 200.0, 100.0, False, 3, 10000.0),
    (4.0, 210.0, 97.9, True, 4, 11000.0),
    (8.0, 220.0, 98.0, False, 4, 12000.0),
    (10.0, 230.0, 100.0, False, 5, 10000.0),
    (15.0, 240.0, 50.0, True, 5, 10000.0),
    (30.0, 250.0, 98.0, False, 6, 12000.0),
    (35.0, 260.0, 99.0, False, 7, 13000.0),
    (40.0, 270.0, 100.0, False, 7, 13000.0),
]
# lap -> (mean_speed, max_speed, pct_full_throttle, brake_fraction, gear_changes, mean_rpm, n)
EXPECTED: dict[int, tuple[Any, ...]] = {
    1: (210.0, 220.0, 0.6667, 0.3333, 1, 11000.0, 3),
    2: (235.0, 240.0, 0.5, 0.5, 0, 10000.0, 2),  # 4 -> 5 across the boundary is not counted
    3: (None, None, None, None, None, None, 0),
    4: (255.0, 260.0, 1.0, 0.0, 1, 12500.0, 2),
}
METRICS = (
    "mean_speed_kph",
    "max_speed_kph",
    "pct_full_throttle",
    "brake_fraction",
    "gear_changes",
    "mean_rpm",
    "n_samples",
)
REPO_SRC = Path(__file__).resolve().parents[2] / "src" / "f1sa"


def _raw(spark: SparkSession, season: int = 2024) -> DataFrame:
    rows = [
        {
            "season": season,
            "round": 1,
            "driver_id": "d",
            "driver_code": "DDD",
            "lap": 99,
            "session_time_s": t,
            "speed_kph": speed,
            "throttle_pct": throttle,
            "brake": brake,
            "n_gear": gear,
            "rpm": rpm,
        }
        for t, speed, throttle, brake, gear, rpm in SAMPLES
    ]
    return frame(spark, "telemetry_raw", rows)


def _laps(spark: SparkSession, season: int = 2024, windows: bool = True) -> DataFrame:
    rows = []
    for n in range(1, 5):
        row = lap(season, 1, "d", n, 1, 10.0)
        if windows:
            row |= {"lap_start_session_s": 10.0 * (n - 1), "lap_end_session_s": 10.0 * n}
        rows.append(row)
    return frame(spark, "laps", rows)


def _synthetic_tel(spark: SparkSession, season: int) -> DataFrame:
    rows = [
        {
            "season": season,
            "round": 1,
            "driver_id": "d",
            "lap": n,
            "mean_speed_kph": 200.0 + n,
            "max_speed_kph": 300.0 + n,
            "pct_full_throttle": 0.6,
            "brake_fraction": 0.2,
            "gear_changes": 40 + n,
            "mean_rpm": 11000.0,
            "is_synthetic": True,
            "source": "synthetic",
        }
        for n in range(1, 5)
    ]
    return frame(spark, "telemetry_lap", rows)


def _by_lap(df: DataFrame) -> dict[int, Any]:
    return {r["lap"]: r for r in df.collect()}


def test_threshold_is_98() -> None:
    assert FULL_THROTTLE_PCT == 98.0


def test_assign_to_laps_boundaries(spark: SparkSession) -> None:
    raw = _raw(spark).withColumnRenamed("session_time_s", "t")
    assigned = {r["t"]: r["lap"] for r in assign_to_laps(raw, lap_windows(_laps(spark))).collect()}
    assert assigned == {
        -1.0: None,
        0.0: 1,
        4.0: 1,
        8.0: 1,
        10.0: 2,
        15.0: 2,
        30.0: 4,
        35.0: 4,
        40.0: None,
    }


def test_raw_aggregation_exact_values(spark: SparkSession) -> None:
    out = telemetry_lap_features(_raw(spark), _laps(spark), frame(spark, "telemetry_lap", []))
    assert [(f.name, f.dataType) for f in out.schema] == [
        (f.name, f.dataType) for f in FS.TELEMETRY_LAP_FEATURES
    ]
    rows = _by_lap(out)
    assert set(rows) == {1, 2, 3, 4}
    for n, expected in EXPECTED.items():
        assert tuple(rows[n][m] for m in METRICS) == expected, n
        assert rows[n]["is_synthetic"] is False and rows[n]["source"] == "fastf1_raw"
    assert sum(r["n_samples"] for r in rows.values()) == 7  # 9 samples, 2 outside all laps


def test_aggregate_samples_ignores_unassigned(spark: SparkSession) -> None:
    raw = _raw(spark).withColumnRenamed("session_time_s", "t")
    agg = _by_lap(aggregate_samples(assign_to_laps(raw, lap_windows(_laps(spark)))))
    assert set(agg) == {1, 2, 4}  # no row for the lap without samples, none for null laps


def test_synthetic_passes_through_unchanged(spark: SparkSession) -> None:
    syn = _synthetic_tel(spark, 2023)
    out = telemetry_lap_features(
        frame(spark, "telemetry_raw", []), _laps(spark, 2023, windows=False), syn
    )
    assert out.count() == 4
    expected = {r["lap"]: r.asDict() for r in syn.collect()}
    for n, row in _by_lap(out).items():
        got = row.asDict()
        assert got.pop("n_samples") is None
        assert got == expected[n]
        assert got["is_synthetic"] is True and got["source"] == "synthetic"


def test_mixed_races_each_use_their_own_source(spark: SparkSession) -> None:
    laps = _laps(spark, 2024).unionByName(_laps(spark, 2023, windows=False))
    syn = _synthetic_tel(spark, 2023).unionByName(_synthetic_tel(spark, 2024))
    out = telemetry_lap_features(_raw(spark, 2024), laps, syn).collect()
    sources = {(r["season"], r["source"], r["is_synthetic"]) for r in out}
    assert sources == {(2023, "synthetic", True), (2024, "fastf1_raw", False)}
    assert len(out) == 8


def test_raw_without_windows_falls_back_to_clean_telemetry_lap(spark: SparkSession) -> None:
    out = telemetry_lap_features(
        _raw(spark, 2024), _laps(spark, 2024, windows=False), _synthetic_tel(spark, 2024)
    ).collect()
    assert len(out) == 4 and all(r["source"] == "synthetic" for r in out)


def test_streaming_code_never_imports_telemetry() -> None:
    streaming = REPO_SRC / "streaming"
    assert streaming.is_dir()
    for path in streaming.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""] + [a.name for a in node.names]
            assert not any("telemetry" in n for n in names), f"{path} imports telemetry"


@pytest.mark.parametrize("missing", ["speed_kph", "n_gear"])
def test_missing_raw_column_is_an_error(spark: SparkSession, missing: str) -> None:
    with pytest.raises(Exception, match=missing):
        telemetry_lap_features(
            _raw(spark).drop(missing), _laps(spark), frame(spark, "telemetry_lap", [])
        ).collect()
