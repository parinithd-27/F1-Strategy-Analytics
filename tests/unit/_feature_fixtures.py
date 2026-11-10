"""Builders for hand-made clean-zone DataFrames (clean schemas, default values)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from f1sa.lake import schemas as S


def frame(spark: SparkSession, table: str, rows: Sequence[dict[str, Any]]) -> DataFrame:
    """A DataFrame with the clean schema of ``table``; missing fields are null."""
    schema = S.CLEAN_SCHEMAS[table]
    data = [tuple(r.get(f.name) for f in schema.fields) for r in rows]
    return spark.createDataFrame(data, schema)


def lap(
    season: int,
    round_: int,
    driver: str,
    lap_no: int,
    position: int,
    lap_time: float | None,
    *,
    pit_in: bool = False,
    pit_out: bool = False,
    neutralised: bool = False,
) -> dict[str, Any]:
    return {
        "season": season,
        "round": round_,
        "driver_id": driver,
        "lap": lap_no,
        "position": position,
        "lap_time_s": lap_time,
        "is_pit_in_lap": pit_in,
        "is_pit_out_lap": pit_out,
        "is_neutralised_lap": neutralised,
        "is_red_flag_lap": False,
        "track_status_source": "heuristic",
    }


def race(
    season: int,
    round_: int,
    circuit: str,
    start: datetime | None = None,
) -> dict[str, Any]:
    return {
        "season": season,
        "round": round_,
        "race_name": f"{circuit} GP",
        "circuit_id": circuit,
        "race_date": date(season, 1, 1),
        "race_start_utc": start or datetime(season, 1, 1, 12, 0, tzinfo=UTC),
        "has_sprint": False,
    }


def result(
    season: int, round_: int, driver: str, team: str, position: int, laps: int
) -> dict[str, Any]:
    return {
        "season": season,
        "round": round_,
        "driver_id": driver,
        "constructor_id": team,
        "grid": position,
        "position": position,
        "laps": laps,
        "status": "Finished",
        "finished": True,
        "classified": True,
    }


def pitstop(
    season: int, round_: int, driver: str, stop: int, lap_no: int, duration: float = 22.5
) -> dict[str, Any]:
    return {
        "season": season,
        "round": round_,
        "driver_id": driver,
        "stop": stop,
        "lap": lap_no,
        "duration_s": duration,
        "is_long_stop": duration > 60.0,
    }
