"""Clean job with FastF1 raw data present (hand-made FastF1 Parquet + trimmed Jolpica fixtures).

Exercises: nanosecond timedeltas read as longs, abbreviation -> driverId mapping (with an
unmapped driver), exact TrackStatus flags, FastF1 stints, per-lap telemetry aggregates and
telemetry_raw lap assignment.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from _fakes import load_fixture
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from f1sa.config import Settings
from f1sa.etl.clean import CleanResult, read_clean, run_clean

LAP_S = 90.0
T0 = 3600.0  # session time at the start of lap 1
DRIVERS = {"VER": "1", "PER": "11", "SAR": "2", "ZZZ": "99"}  # ZZZ is not in the results
TRACK_STATUS = {1: "1", 2: "14", 3: "65"}  # lap 2: SC; lap 3: VSC + red flag
GEARS = [3, 4, 5, 6, 7, 8, 8, 7, 6]  # 7 gear changes per lap
NO_TIME = np.timedelta64("NaT", "ns")


def td(seconds: float) -> np.timedelta64:
    """A nanosecond timedelta (like FastF1's), without NumPy's deprecated generic unit."""
    return np.timedelta64(round(seconds * 1e9), "ns")


def _write_pages(
    base: Path, table: str, pages: list[dict[str, object]], round_: int | None
) -> None:
    part = base / "raw" / "jolpica" / table / "season=2023"
    if round_ is not None:
        part = part / f"round={round_}"
    part.mkdir(parents=True, exist_ok=True)
    (part / f"{table}.json").write_text(json.dumps(pages))


def _lap_window(lap: int) -> tuple[float, float]:
    start = T0 + (lap - 1) * LAP_S
    return start, start + LAP_S


def build_raw_zone(lake: Path) -> None:
    _write_pages(lake, "schedule", [load_fixture("races_2023")], None)
    _write_pages(lake, "drivers", [load_fixture("drivers_2023")], None)
    _write_pages(lake, "constructors", [load_fixture("constructors_2023")], None)
    _write_pages(lake, "results", [load_fixture("results_2023_5")], 5)
    _write_pages(lake, "qualifying", [load_fixture("qualifying_2023_5")], 5)
    pits = load_fixture("pitstops_2023_5")  # keep only the stop inside the 3 fixture laps
    race = pits["MRData"]["RaceTable"]["Races"][0]  # type: ignore[index]
    race["PitStops"] = [p for p in race["PitStops"] if p["driverId"] == "sargeant"]
    _write_pages(lake, "pitstops", [pits], 5)
    _write_pages(lake, "laps", [load_fixture(f"laps_2023_5_p{i}") for i in range(3)], 5)

    rows = []
    for drv, num in DRIVERS.items():
        for lap in (1, 2, 3):
            start, end = _lap_window(lap)
            if drv == "VER":
                stint, compound, life = (1, "MEDIUM", float(lap)) if lap < 3 else (2, "HARD", 4.0)
            else:
                stint, compound, life = 1, "SOFT", float(lap)
            rows.append(
                {
                    "Driver": drv,
                    "DriverNumber": num,
                    "LapNumber": float(lap),
                    "LapTime": td(LAP_S),
                    "Compound": compound,
                    "TyreLife": life,
                    "Stint": float(stint),
                    "PitInTime": NO_TIME,
                    "PitOutTime": NO_TIME,
                    "TrackStatus": TRACK_STATUS[lap],
                    "Position": 1.0,
                    "Time": td(end),
                    "LapStartTime": td(start),
                }
            )
    laps_dir = lake / "raw" / "fastf1" / "laps" / "season=2023" / "round=5"
    laps_dir.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(laps_dir / "laps.parquet", index=False)

    for drv, num in DRIVERS.items():
        samples = []
        for lap in (1, 2, 3):
            start, _end = _lap_window(lap)
            for k in range(9):
                samples.append(
                    (start + 10 * k, 200.0 + k, 100.0 if k < 6 else 50.0, k in (6, 7), GEARS[k])
                )
        samples.append((_lap_window(3)[1] + 5.0, 150.0, 0.0, True, 2))  # after the last lap
        car = pd.DataFrame(
            {
                "Driver": drv,
                "DriverNumber": num,
                "Date": [pd.Timestamp("2023-05-07 19:30:00") + td(t) for t, *_ in samples],
                "SessionTime": [td(t) for t, *_ in samples],
                "Time": [td(t - T0) for t, *_ in samples],
                "Speed": [s for _, s, *_ in samples],
                "Throttle": [th for _, _, th, *_ in samples],
                "Brake": [b for *_, b, _g in samples],
                "nGear": [g for *_, g in samples],
                "RPM": [11000.0] * len(samples),
            }
        )
        d = lake / "raw" / "fastf1" / "telemetry" / "season=2023" / "round=5" / f"driver={drv}"
        d.mkdir(parents=True)
        car.to_parquet(d / "car_data.parquet", index=False)


@pytest.fixture(scope="module")
def result(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[CleanResult, Path]]:
    base = tmp_path_factory.mktemp("clean_fastf1")
    lake = base / "data" / "lake"
    build_raw_zone(lake)
    settings = Settings(_env_file=None, data_lake_uri=f"file://{lake}")  # type: ignore[call-arg]
    cwd = os.getcwd()
    os.chdir(base)
    try:
        yield run_clean(spark, settings, source="lake", out=lambda _m: None), lake / "clean"
    finally:
        os.chdir(cwd)


def test_counts_and_quality(result: tuple[CleanResult, Path]) -> None:
    res, _ = result
    assert res.counts["laps"] == 9 and res.counts["results"] == 3
    assert res.counts["telemetry_lap"] == 9  # 3 mapped drivers x 3 laps
    assert res.counts["telemetry_raw"] == 84  # 3 drivers x (27 + 1 sample after the last lap)
    assert res.ok
    assert res.quality is not None
    soft = {c.name: c for c in res.quality.soft_issues}
    # the fixture keeps 3 of 20 drivers and 3 of 57 laps, so positions (1, 2, 20) are not 1..N
    # and results.laps != lap-table laps: correctly reported as soft issues
    assert set(soft) == {
        "fastf1_drivers_mapped",
        "positions_1_to_n_per_lap",
        "results_laps_match_lap_table",
    }
    assert soft["fastf1_drivers_mapped"].count == 1
    assert soft["fastf1_drivers_mapped"].examples[0]["Driver"] == "ZZZ"


def test_exact_track_status_flags(spark: SparkSession, result: tuple[CleanResult, Path]) -> None:
    _, clean = result
    laps = read_clean(spark, f"file://{clean}", "laps")
    flags = {
        r["lap"]: (r["is_sc_lap"], r["is_vsc_lap"], r["is_red_flag_lap"], r["is_neutralised_lap"])
        for r in laps.select(
            "lap", "is_sc_lap", "is_vsc_lap", "is_red_flag_lap", "is_neutralised_lap"
        )
        .distinct()
        .collect()
    }
    assert flags == {
        1: (False, False, False, False),
        2: (True, False, False, True),
        3: (False, True, True, True),
    }
    assert {r[0] for r in laps.select("track_status_source").distinct().collect()} == {"fastf1"}


def test_fastf1_stints_mapped_to_driver_ids(
    spark: SparkSession, result: tuple[CleanResult, Path]
) -> None:
    _, clean = result
    st = read_clean(spark, f"file://{clean}", "stints")
    assert {r[0] for r in st.select("source").distinct().collect()} == {"fastf1"}
    assert {r[0] for r in st.select("driver_id").distinct().collect()} == {
        "max_verstappen",
        "perez",
        "sargeant",
    }
    ver = [
        (r["stint"], r["start_lap"], r["end_lap"], r["compound"], r["tyre_age_at_start"])
        for r in st.where(F.col("driver_id") == "max_verstappen").orderBy("stint").collect()
    ]
    assert ver == [(1, 1, 2, "MEDIUM", 0), (2, 3, 3, "HARD", 3)]
    assert st.where(F.col("is_synthetic")).count() == 0


def test_telemetry_aggregates(spark: SparkSession, result: tuple[CleanResult, Path]) -> None:
    _, clean = result
    tel = read_clean(spark, f"file://{clean}", "telemetry_lap")
    row = tel.where((F.col("driver_id") == "perez") & (F.col("lap") == 2)).first()
    assert row is not None
    assert row["mean_speed_kph"] == pytest.approx(204.0)
    assert row["max_speed_kph"] == pytest.approx(208.0)
    assert row["pct_full_throttle"] == pytest.approx(6 / 9, abs=1e-4)
    assert row["brake_fraction"] == pytest.approx(2 / 9, abs=1e-4)
    assert row["gear_changes"] == 7
    assert row["mean_rpm"] == pytest.approx(11000.0)
    assert row["source"] == "fastf1" and row["is_synthetic"] is False


def test_telemetry_raw_lap_assignment(
    spark: SparkSession, result: tuple[CleanResult, Path]
) -> None:
    _, clean = result
    raw = read_clean(spark, f"file://{clean}", "telemetry_raw")
    per_lap = {
        r["lap"]: r["count"]
        for r in raw.where(F.col("driver_id") == "max_verstappen").groupBy("lap").count().collect()
    }
    assert per_lap == {1: 9, 2: 9, 3: 9, None: 1}  # the sample after the last lap has no lap
    first = raw.where(F.col("driver_id") == "max_verstappen").orderBy("session_time_s").first()
    assert first is not None
    assert first["session_time_s"] == pytest.approx(T0)
    assert first["driver_code"] == "VER" and first["date"] is not None
    assert raw.where(F.col("driver_code") == "ZZZ").count() == 0  # unmapped rows are dropped
