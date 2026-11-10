"""Clean zone: raw Jolpica/FastF1/synthetic -> typed Parquet partitioned by season and round.

    DATA_LAKE_URI=file://$PWD/data/lake python -m f1sa.etl.clean --source sample
    # hdfs mode, from inside the app container (it resolves namenode/datanode):
    docker compose exec app env PYTHONPATH=/workspace/src \\
        DATA_LAKE_URI=hdfs://namenode:8020/f1 python -m f1sa.etl.clean --source lake

Input: ``--source sample`` reads ``Settings.sample_dir/raw`` (read-only); ``--source lake`` reads
``{DATA_LAKE_URI}/raw``. Output is always ``{DATA_LAKE_URI}/clean/{table}/`` written with
``partitionBy("season", "round")`` and dynamic partition overwrite, so reruns are idempotent and a
run over a subset of races leaves other partitions alone. In file mode the job refuses to write
outside ``./data/lake``.

Normalisation: Jolpica ``driverId`` is canonical (FastF1 abbreviations are mapped through the
race results); times become seconds (double); laps carry gaps and SC/VSC/red-flag flags (exact
from FastF1 TrackStatus when present, otherwise a documented Jolpica-only heuristic). Stints and
per-lap telemetry come from FastF1 when present, else the synthetic sample tables, else (stints
only) are inferred from Jolpica pit stops with an unknown compound.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import reduce
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from f1sa.config import Settings, get_settings
from f1sa.etl.spark import build_spark, configure_session
from f1sa.features.telemetry_agg import aggregate_samples, assign_to_laps
from f1sa.lake import schemas as S
from f1sa.lake.paths import is_within, local_path
from f1sa.lake.quality import QualityReport, run_checks

NEUTRALISED_RATIO = 1.25
"""Heuristic: a lap whose field-median time exceeds this multiple of the race's green-flag median
is neutralised (safety car or VSC; Jolpica cannot tell which)."""
RED_FLAG_RATIO = 3.0
"""Heuristic: above this multiple the lap is treated as a red-flag lap (includes the stoppage)."""
LONG_STOP_S = 60.0
CLEAN_TABLES = tuple(S.CLEAN_SCHEMAS)
RaceKey = tuple[int, int]


class CleanError(RuntimeError):
    """The clean job cannot run (bad configuration or unreadable input)."""


# ============================================================================== locations
def spark_uri(uri: str) -> str:
    """URI usable by Spark: ``file://./x`` becomes an absolute ``file:///abs/x``."""
    if uri.startswith("file://"):
        return f"file://{local_path(uri).resolve()}"
    if uri.startswith("hdfs://"):
        return uri.rstrip("/")
    raise CleanError(f"unsupported URI scheme: {uri}")


def resolve_roots(settings: Settings, source: str) -> tuple[str, str]:
    """(raw input root, clean output root) for ``source`` = ``sample`` | ``lake``."""
    lake = spark_uri(settings.data_lake_uri)
    if source == "sample":
        sample_raw = settings.sample_dir / "raw"
        if not sample_raw.is_dir():
            raise CleanError(f"sample bundle not found at {sample_raw}; run `make sample-data`")
        raw = f"file://{sample_raw.resolve()}"
    elif source == "lake":
        raw = f"{lake}/raw"
    else:
        raise CleanError(f"unknown source {source!r}")
    clean = f"{lake}/clean"
    check_output_root(clean)
    return raw, clean


def check_output_root(clean_uri: str) -> None:
    """In file mode, refuse to write anywhere but under ./data/lake (gitignored)."""
    if clean_uri.startswith("file://"):
        path = Path(clean_uri.removeprefix("file://"))
        if not is_within(path, Path("data/lake")):
            raise CleanError(f"refusing to write outside ./data/lake: {path}")


# ============================================================================== column helpers
_DURATION = r"^(?:(?:(\d+):)?(\d{1,2}):)?(\d{1,2}(?:\.\d+)?)$"


def duration_seconds(col: Column) -> Column:
    """Ergast time text ('31.2', '1:31.234', '1:27:38.241') -> seconds; anything else -> null."""
    text = F.trim(col)
    hours = F.regexp_extract(text, _DURATION, 1)
    minutes = F.regexp_extract(text, _DURATION, 2)
    seconds = F.regexp_extract(text, _DURATION, 3).cast("double")
    h = F.when(hours != "", hours.cast("double")).otherwise(F.lit(0.0))
    m = F.when(minutes != "", minutes.cast("double")).otherwise(F.lit(0.0))
    valid = (
        text.rlike(_DURATION) & ~((minutes != "") & (seconds >= 60)) & ~((hours != "") & (m >= 60))
    )
    return F.when(valid, F.round(h * 3600 + m * 60 + seconds, 3))


def ns_to_seconds(col: Column) -> Column:
    """FastF1 nanosecond durations (read as long) -> seconds (double)."""
    return col.cast("double") / F.lit(1e9)


def conform(df: DataFrame, schema: StructType, what: str) -> DataFrame:
    """Select and cast ``df`` to ``schema`` (column order and types); missing columns raise."""
    missing = [f.name for f in schema.fields if f.name not in df.columns]
    if missing:
        raise CleanError(f"{what} is missing columns {missing}")
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def empty(spark: SparkSession, schema: StructType) -> DataFrame:
    return spark.createDataFrame([], schema)


def select_races(
    df: DataFrame, seasons: Sequence[int] | None, races: set[RaceKey] | None
) -> DataFrame:
    """Keep only the requested seasons / (season, round) pairs."""
    if seasons:
        df = df.where(F.col("season").isin(list(seasons)))
    if races:
        conds = [(F.col("season") == s) & (F.col("round") == r) for s, r in sorted(races)]
        df = df.where(reduce(lambda a, b: a | b, conds))
    return df


# ============================================================================== readers
def _season_glob(seasons: Sequence[int] | None) -> str:
    return "*" if not seasons else "{" + ",".join(str(s) for s in sorted(seasons)) + "}"


def read_json_pages(
    spark: SparkSession, raw_root: str, table: str, seasons: Sequence[int] | None
) -> DataFrame:
    """Raw Jolpica pages of ``table`` (one row per page); empty if none exist."""
    round_part = "" if table in ("schedule", "drivers", "constructors") else "round=*/"
    pattern = f"{raw_root}/jolpica/{table}/season={_season_glob(seasons)}/{round_part}{table}.json"
    schema = S.RAW_JSON_SCHEMAS[table]
    try:
        return (
            spark.read.schema(schema)
            .option("multiLine", True)
            .option("mode", "FAILFAST")
            .json(pattern)
        )
    except AnalysisException as exc:
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            return empty(spark, schema)
        raise


_PATH_SEASON = r"/season=(\d+)/"
_PATH_ROUND = r"/round=(\d+)/"


def read_parquet_table(
    spark: SparkSession,
    pattern: str,
    schema: StructType,
    what: str,
) -> DataFrame:
    """Raw Parquet files matching ``pattern`` conformed to ``schema``; empty if none exist.

    ``season``/``round`` are taken from the file path when the files do not contain them
    (FastF1 files); partition discovery is disabled to avoid clashes with data columns.
    """
    try:
        df = spark.read.option("recursiveFileLookup", "false").parquet(pattern)
    except AnalysisException as exc:
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            return empty(spark, schema)
        raise
    path = F.col("_metadata.file_path")
    for name, regex in (("season", _PATH_SEASON), ("round", _PATH_ROUND)):
        if name not in df.columns:
            df = df.withColumn(name, F.regexp_extract(path, regex, 1).cast("int"))
    return conform(df, schema, what)


# ============================================================================== builders: Jolpica
def explode_races(pages: DataFrame) -> DataFrame:
    """One row per race struct ``r`` (with int ``season``/``round``)."""
    return (
        pages.select(F.explode("MRData.RaceTable.Races").alias("r"))
        .withColumn("season", F.col("r.season").cast("int"))
        .withColumn("round", F.col("r.round").cast("int"))
    )


def build_results(result_pages: DataFrame) -> DataFrame:
    res = explode_races(result_pages).select("season", "round", F.explode("r.Results").alias("x"))
    status = F.col("x.status")
    df = res.select(
        "season",
        "round",
        F.col("x.Driver.driverId").alias("driver_id"),
        F.col("x.Constructor.constructorId").alias("constructor_id"),
        F.col("x.number").cast("int").alias("car_number"),
        F.col("x.grid").cast("int").alias("grid"),
        F.col("x.position").cast("int").alias("position"),
        F.col("x.positionText").alias("position_text"),
        F.col("x.points").cast("double").alias("points"),
        F.col("x.laps").cast("int").alias("laps"),
        status.alias("status"),
        ((status == "Finished") | (status == "Lapped") | status.rlike(r"^\+\d+ Laps?$")).alias(
            "finished"
        ),
        F.col("x.positionText").rlike(r"^\d+$").alias("classified"),
        (F.col("x.Time.millis").cast("double") / 1000.0).alias("race_time_s"),
        F.col("x.FastestLap.lap").cast("int").alias("fastest_lap"),
        F.col("x.FastestLap.rank").cast("int").alias("fastest_lap_rank"),
        duration_seconds(F.col("x.FastestLap.Time.time")).alias("fastest_lap_time_s"),
    )
    return conform(df, S.RESULTS, "results")


def build_qualifying(quali_pages: DataFrame) -> DataFrame:
    q = explode_races(quali_pages).select(
        "season", "round", F.explode("r.QualifyingResults").alias("x")
    )
    q1, q2, q3 = (duration_seconds(F.col(f"x.Q{i}")) for i in (1, 2, 3))
    df = q.select(
        "season",
        "round",
        F.col("x.Driver.driverId").alias("driver_id"),
        F.col("x.Constructor.constructorId").alias("constructor_id"),
        F.col("x.number").cast("int").alias("car_number"),
        F.col("x.position").cast("int").alias("position"),
        q1.alias("q1_s"),
        q2.alias("q2_s"),
        q3.alias("q3_s"),
        F.least(q1, q2, q3).alias("best_s"),
    )
    return conform(df, S.QUALIFYING, "qualifying")


def build_pitstops(pit_pages: DataFrame) -> DataFrame:
    p = explode_races(pit_pages).select("season", "round", F.explode("r.PitStops").alias("x"))
    duration = duration_seconds(F.col("x.duration"))
    df = p.select(
        "season",
        "round",
        F.col("x.driverId").alias("driver_id"),
        F.col("x.stop").cast("int").alias("stop"),
        F.col("x.lap").cast("int").alias("lap"),
        F.col("x.time").alias("time_of_day"),
        duration.alias("duration_s"),
        (duration > LONG_STOP_S).alias("is_long_stop"),
    ).dropDuplicates(["season", "round", "driver_id", "stop"])
    return conform(df, S.PITSTOPS, "pitstops")


def build_lap_timings(lap_pages: DataFrame) -> DataFrame:
    """Lap timings (deduplicated across page splits) with raw and parsed times."""
    laps = explode_races(lap_pages).select("season", "round", F.explode("r.Laps").alias("l"))
    timings = laps.select(
        "season",
        "round",
        F.col("l.number").cast("int").alias("lap"),
        F.explode("l.Timings").alias("t"),
    )
    return timings.select(
        "season",
        "round",
        F.col("t.driverId").alias("driver_id"),
        "lap",
        F.col("t.position").cast("int").alias("position"),
        F.col("t.time").alias("time_raw"),
        duration_seconds(F.col("t.time")).alias("lap_time_s"),
    ).dropDuplicates(["season", "round", "driver_id", "lap"])


def build_races(schedule_pages: DataFrame, results: DataFrame) -> DataFrame:
    """Races that have results (the races present in the lake), with calendar details."""
    r = explode_races(schedule_pages)
    start = F.to_timestamp(
        F.concat(F.col("r.date"), F.lit("T"), F.coalesce(F.col("r.time"), F.lit("00:00:00Z"))),
        "yyyy-MM-dd'T'HH:mm:ssX",
    )
    df = r.select(
        "season",
        "round",
        F.col("r.raceName").alias("race_name"),
        F.col("r.Circuit.circuitId").alias("circuit_id"),
        F.col("r.Circuit.circuitName").alias("circuit_name"),
        F.col("r.Circuit.Location.locality").alias("locality"),
        F.col("r.Circuit.Location.country").alias("country"),
        F.col("r.Circuit.Location.lat").cast("double").alias("lat"),
        F.col("r.Circuit.Location.long").cast("double").alias("long"),
        F.to_date(F.col("r.date")).alias("race_date"),
        start.alias("race_start_utc"),
        F.col("r.Sprint.date").isNotNull().alias("has_sprint"),
        F.col("r.url").alias("url"),
    ).dropDuplicates(["season", "round"])
    present = results.select("season", "round").distinct()
    return conform(df.join(present, ["season", "round"], "left_semi"), S.RACES, "races")


def build_drivers(result_pages: DataFrame, driver_pages: DataFrame) -> DataFrame:
    """Race entry list (one row per driver per race) enriched with the season driver table."""
    res = explode_races(result_pages).select("season", "round", F.explode("r.Results").alias("x"))
    entries = res.select(
        "season",
        "round",
        F.col("x.Driver.driverId").alias("driver_id"),
        F.col("x.Driver.code").alias("code_r"),
        F.col("x.Driver.permanentNumber").cast("int").alias("permanent_number_r"),
        F.col("x.number").cast("int").alias("car_number"),
        F.col("x.Driver.givenName").alias("given_name_r"),
        F.col("x.Driver.familyName").alias("family_name_r"),
        F.to_date(F.col("x.Driver.dateOfBirth")).alias("dob_r"),
        F.col("x.Driver.nationality").alias("nationality_r"),
        F.col("x.Constructor.constructorId").alias("constructor_id"),
    )
    season = (
        driver_pages.select(
            F.col("MRData.DriverTable.season").cast("int").alias("season"),
            F.explode("MRData.DriverTable.Drivers").alias("d"),
        )
        .select(
            "season",
            F.col("d.driverId").alias("driver_id"),
            F.col("d.code").alias("code_s"),
            F.col("d.permanentNumber").cast("int").alias("permanent_number_s"),
            F.col("d.givenName").alias("given_name_s"),
            F.col("d.familyName").alias("family_name_s"),
            F.to_date(F.col("d.dateOfBirth")).alias("dob_s"),
            F.col("d.nationality").alias("nationality_s"),
        )
        .dropDuplicates(["season", "driver_id"])
    )
    j = entries.join(season, ["season", "driver_id"], "left")
    df = j.select(
        "season",
        "round",
        "driver_id",
        F.coalesce("code_r", "code_s").alias("code"),
        F.coalesce("permanent_number_r", "permanent_number_s").alias("permanent_number"),
        "car_number",
        F.coalesce("given_name_r", "given_name_s").alias("given_name"),
        F.coalesce("family_name_r", "family_name_s").alias("family_name"),
        F.coalesce("dob_r", "dob_s").alias("date_of_birth"),
        F.coalesce("nationality_r", "nationality_s").alias("nationality"),
        "constructor_id",
    ).dropDuplicates(["season", "round", "driver_id"])
    return conform(df, S.DRIVERS, "drivers")


def build_constructors(result_pages: DataFrame, constructor_pages: DataFrame) -> DataFrame:
    res = explode_races(result_pages).select("season", "round", F.explode("r.Results").alias("x"))
    entries = res.select(
        "season",
        "round",
        F.col("x.Constructor.constructorId").alias("constructor_id"),
        F.col("x.Constructor.name").alias("name_r"),
        F.col("x.Constructor.nationality").alias("nationality_r"),
    ).dropDuplicates(["season", "round", "constructor_id"])
    season = (
        constructor_pages.select(
            F.col("MRData.ConstructorTable.season").cast("int").alias("season"),
            F.explode("MRData.ConstructorTable.Constructors").alias("c"),
        )
        .select(
            "season",
            F.col("c.constructorId").alias("constructor_id"),
            F.col("c.name").alias("name_s"),
            F.col("c.nationality").alias("nationality_s"),
        )
        .dropDuplicates(["season", "constructor_id"])
    )
    df = entries.join(season, ["season", "constructor_id"], "left").select(
        "season",
        "round",
        "constructor_id",
        F.coalesce("name_r", "name_s").alias("name"),
        F.coalesce("nationality_r", "nationality_s").alias("nationality"),
    )
    return conform(df, S.CONSTRUCTORS, "constructors")


# ============================================================================== laps, gaps, flags
_RACE = ["season", "round"]
_DRIVER_KEY = ["season", "round", "driver_id"]


def add_pit_flags(timings: DataFrame, pitstops: DataFrame) -> DataFrame:
    stops = pitstops.select(*_DRIVER_KEY, "lap").distinct()
    pit_in = stops.withColumn("is_pit_in_lap", F.lit(True))
    pit_out = stops.select(*_DRIVER_KEY, (F.col("lap") + 1).alias("lap")).withColumn(
        "is_pit_out_lap", F.lit(True)
    )
    keys = [*_DRIVER_KEY, "lap"]
    return (
        timings.join(pit_in, keys, "left")
        .join(pit_out, keys, "left")
        .fillna(False, subset=["is_pit_in_lap", "is_pit_out_lap"])
    )


def heuristic_track_flags(laps: DataFrame) -> DataFrame:
    """Per (season, round, lap) flags from lap times only (no FastF1).

    Lap 1 and pit in/out laps are excluded. ``ratio`` = field-median lap time / the race's median
    green lap time. ratio > RED_FLAG_RATIO -> red flag; ratio > NEUTRALISED_RATIO -> neutralised
    (SC or VSC: indistinguishable, so ``is_sc_lap``/``is_vsc_lap`` stay null).
    """
    racing = laps.where(
        (F.col("lap") > 1)
        & ~F.col("is_pit_in_lap")
        & ~F.col("is_pit_out_lap")
        & F.col("lap_time_s").isNotNull()
    )
    race_median = racing.groupBy(*_RACE).agg(F.median("lap_time_s").alias("race_median"))
    lap_median = racing.groupBy(*_RACE, "lap").agg(F.median("lap_time_s").alias("lap_median"))
    ratio = lap_median.join(race_median, _RACE).select(
        *_RACE, "lap", (F.col("lap_median") / F.col("race_median")).alias("ratio")
    )
    all_laps = laps.select(*_RACE, "lap").distinct()
    j = all_laps.join(ratio, [*_RACE, "lap"], "left")
    return j.select(
        *_RACE,
        "lap",
        F.lit(None).cast("boolean").alias("is_sc_lap"),
        F.lit(None).cast("boolean").alias("is_vsc_lap"),
        F.coalesce(F.col("ratio") > RED_FLAG_RATIO, F.lit(False)).alias("is_red_flag_lap"),
        F.coalesce(F.col("ratio") > NEUTRALISED_RATIO, F.lit(False)).alias("is_neutralised_lap"),
        F.lit("heuristic").alias("track_status_source"),
    )


def fastf1_track_flags(ff1_laps: DataFrame) -> DataFrame:
    """Exact per-lap flags from FastF1 TrackStatus codes (4 SC, 6/7 VSC, 5 red flag)."""
    codes = (
        ff1_laps.where(F.col("LapNumber").isNotNull())
        .groupBy(*_RACE, F.col("LapNumber").cast("int").alias("lap"))
        .agg(F.concat_ws("", F.collect_list(F.coalesce("TrackStatus", F.lit("")))).alias("ts"))
    )
    sc = F.col("ts").contains("4")
    vsc = F.col("ts").contains("6") | F.col("ts").contains("7")
    return codes.select(
        *_RACE,
        "lap",
        sc.alias("is_sc_lap"),
        vsc.alias("is_vsc_lap"),
        F.col("ts").contains("5").alias("is_red_flag_lap"),
        (sc | vsc).alias("is_neutralised_lap"),
        F.lit("fastf1").alias("track_status_source"),
    )


def fastf1_lap_windows(ff1_laps: DataFrame) -> DataFrame:
    """(season, round, driver_id, lap, lap_start_session_s, lap_end_session_s) from FastF1 laps
    (``LapStartTime`` and ``Time``, both session time)."""
    return (
        ff1_laps.where(F.col("LapNumber").isNotNull())
        .select(
            *_DRIVER_KEY,
            F.col("LapNumber").cast("int").alias("lap"),
            F.round(ns_to_seconds(F.col("LapStartTime")), 3).alias("lap_start_session_s"),
            F.round(ns_to_seconds(F.col("Time")), 3).alias("lap_end_session_s"),
        )
        .dropDuplicates([*_DRIVER_KEY, "lap"])
    )


def _empty_windows(laps: DataFrame) -> DataFrame:
    return laps.select(
        *_DRIVER_KEY,
        "lap",
        F.lit(None).cast("double").alias("lap_start_session_s"),
        F.lit(None).cast("double").alias("lap_end_session_s"),
    ).limit(0)


def build_laps(timings: DataFrame, pitstops: DataFrame, ff1_laps: DataFrame | None) -> DataFrame:
    """Clean laps: cumulative time, gaps at the timing line, pit and track-status flags."""
    laps = add_pit_flags(timings, pitstops)
    upto = (
        Window.partitionBy(*_DRIVER_KEY)
        .orderBy("lap")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    laps = laps.withColumn(
        "cumulative_time_s",
        F.when(
            F.sum(F.col("lap_time_s").isNull().cast("int")).over(upto) == 0,
            F.round(F.sum("lap_time_s").over(upto), 3),
        ),
    )
    same_lap = Window.partitionBy(*_RACE, "lap")
    leader = F.max(F.when(F.col("position") == 1, F.col("cumulative_time_s"))).over(same_lap)
    ahead = F.lag("cumulative_time_s").over(same_lap.orderBy("position"))
    laps = laps.withColumn(
        "gap_to_leader_s", F.round(F.col("cumulative_time_s") - leader, 3)
    ).withColumn("gap_ahead_s", F.round(F.col("cumulative_time_s") - ahead, 3))

    windows = _empty_windows(laps)
    if ff1_laps is not None:
        windows = fastf1_lap_windows(ff1_laps)
    laps = laps.join(windows, [*_DRIVER_KEY, "lap"], "left")

    flags = heuristic_track_flags(laps)
    if ff1_laps is not None:
        exact = fastf1_track_flags(ff1_laps)
        cols = [
            "is_sc_lap",
            "is_vsc_lap",
            "is_red_flag_lap",
            "is_neutralised_lap",
            "track_status_source",
        ]
        e = exact.select(*_RACE, "lap", *[F.col(c).alias(f"e_{c}") for c in cols])
        flags = flags.join(e, [*_RACE, "lap"], "left").select(
            *_RACE,
            "lap",
            *[
                F.when(F.col("e_track_status_source").isNotNull(), F.col(f"e_{c}"))
                .otherwise(F.col(c))
                .alias(c)
                for c in cols
            ],
        )
    return conform(laps.join(flags, [*_RACE, "lap"], "left"), S.LAPS, "laps")


# ============================================================================== FastF1 mapping
def map_fastf1_drivers(ff1: DataFrame, entries: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Attach the canonical Jolpica ``driver_id`` to FastF1 rows.

    ``entries`` is the clean drivers table (season, round, driver_id, code, car_number). FastF1's
    ``Driver`` abbreviation is matched to ``code`` per race; ``DriverNumber`` to ``car_number`` is
    the fallback. Returns (mapped rows, distinct unmapped (season, round, Driver, DriverNumber)).
    """
    abbr = (
        entries.where(F.col("code").isNotNull())
        .select(*_RACE, F.col("code").alias("Driver"), F.col("driver_id").alias("_id_code"))
        .dropDuplicates([*_RACE, "Driver"])
    )
    nums = (
        entries.where(F.col("car_number").isNotNull())
        .select(
            *_RACE,
            F.col("car_number").cast("string").alias("DriverNumber"),
            F.col("driver_id").alias("_id_num"),
        )
        .dropDuplicates([*_RACE, "DriverNumber"])
    )
    j = (
        ff1.join(abbr, [*_RACE, "Driver"], "left")
        .join(nums, [*_RACE, "DriverNumber"], "left")
        .withColumn("driver_id", F.coalesce("_id_code", "_id_num"))
        .drop("_id_code", "_id_num")
    )
    unmapped = (
        j.where(F.col("driver_id").isNull()).select(*_RACE, "Driver", "DriverNumber").distinct()
    )
    return j.where(F.col("driver_id").isNotNull()), unmapped


# ============================================================================== stints / telemetry
_SOURCE_PRIORITY = {"fastf1": 0, "synthetic": 1, "jolpica_inferred": 2}


def _best_source_per_race(*frames: DataFrame) -> DataFrame:
    """Union candidate frames (each with a ``source`` column) and keep, per race, the rows of
    the most preferred source available for that race."""
    union = reduce(lambda a, b: a.unionByName(b), frames)
    prio = F.create_map(*[x for k, v in _SOURCE_PRIORITY.items() for x in (F.lit(k), F.lit(v))])
    union = union.withColumn("_prio", prio[F.col("source")])
    best = union.groupBy(*_RACE).agg(F.min("_prio").alias("_best"))
    return union.join(best, _RACE).where(F.col("_prio") == F.col("_best")).drop("_prio", "_best")


def infer_stints(laps: DataFrame, pitstops: DataFrame) -> DataFrame:
    """Stints from Jolpica laps + pit stops (compound unknown). Same rules as the synthetic
    generator: a stop on lap p ends a stint at p; stops at/after the last lap start none."""
    last = laps.groupBy(*_DRIVER_KEY).agg(F.max("lap").alias("last_lap"))
    stops = (
        pitstops.select(*_DRIVER_KEY, "lap")
        .distinct()
        .join(last, _DRIVER_KEY)
        .where((F.col("lap") >= 1) & (F.col("lap") < F.col("last_lap")))
    )
    w = Window.partitionBy(*_DRIVER_KEY).orderBy("lap")
    stop_rows = stops.select(
        *_DRIVER_KEY,
        F.row_number().over(w).alias("stint"),
        F.coalesce(F.lag("lap").over(w) + 1, F.lit(1)).alias("start_lap"),
        F.col("lap").alias("end_lap"),
    )
    agg = stops.groupBy(*_DRIVER_KEY).agg(F.count("*").alias("n"), F.max("lap").alias("last_stop"))
    final = last.join(agg, _DRIVER_KEY, "left").select(
        *_DRIVER_KEY,
        (F.coalesce("n", F.lit(0)) + 1).alias("stint"),
        (F.coalesce("last_stop", F.lit(0)) + 1).alias("start_lap"),
        F.col("last_lap").alias("end_lap"),
    )
    rows = stop_rows.unionByName(final)
    return rows.select(
        *_DRIVER_KEY,
        "stint",
        "start_lap",
        "end_lap",
        (F.col("end_lap") - F.col("start_lap") + 1).alias("laps"),
        F.lit(None).cast("string").alias("compound"),
        F.lit(None).cast("int").alias("tyre_age_at_start"),
        F.lit(False).alias("is_synthetic"),
        F.lit("jolpica_inferred").alias("source"),
    )


def fastf1_stints(ff1_laps: DataFrame) -> DataFrame:
    g = ff1_laps.where(F.col("Stint").isNotNull() & F.col("LapNumber").isNotNull()).groupBy(
        *_DRIVER_KEY, F.col("Stint").cast("int").alias("stint")
    )
    df = g.agg(
        F.min("LapNumber").cast("int").alias("start_lap"),
        F.max("LapNumber").cast("int").alias("end_lap"),
        F.first("Compound", ignorenulls=True).alias("compound"),
        F.greatest(F.min("TyreLife") - 1, F.lit(0.0)).cast("int").alias("tyre_age_at_start"),
    )
    return df.select(
        *_DRIVER_KEY,
        "stint",
        "start_lap",
        "end_lap",
        (F.col("end_lap") - F.col("start_lap") + 1).alias("laps"),
        "compound",
        "tyre_age_at_start",
        F.lit(False).alias("is_synthetic"),
        F.lit("fastf1").alias("source"),
    )


def build_stints(
    laps: DataFrame, pitstops: DataFrame, synthetic: DataFrame, ff1_laps: DataFrame | None
) -> DataFrame:
    candidates = [infer_stints(laps, pitstops), synthetic.withColumn("source", F.lit("synthetic"))]
    if ff1_laps is not None:
        candidates.append(fastf1_stints(ff1_laps))
    return conform(_best_source_per_race(*candidates), S.STINTS, "stints")


def assign_laps(car: DataFrame, ff1_laps: DataFrame) -> DataFrame:
    """Give every telemetry sample the lap whose [LapStartTime, Time) window contains it
    (``features.telemetry_agg.assign_to_laps``). Samples outside every lap get ``lap = null``."""
    windows = ff1_laps.where(F.col("LapNumber").isNotNull()).select(
        *_DRIVER_KEY,
        F.col("LapNumber").cast("int").alias("lap"),
        ns_to_seconds(F.col("LapStartTime")).alias("lap_start_s"),
        ns_to_seconds(F.col("Time")).alias("lap_end_s"),
    )
    return assign_to_laps(car, windows)


def fastf1_samples(ff1_car: DataFrame, ff1_laps: DataFrame) -> DataFrame:
    car = ff1_car.select(
        *_DRIVER_KEY,
        ns_to_seconds(F.col("SessionTime")).alias("t"),
        F.col("Driver").alias("driver_code"),
        F.timestamp_micros((F.col("Date") / 1000).cast("long")).alias("date"),
        F.col("Speed").alias("speed_kph"),
        F.col("Throttle").alias("throttle_pct"),
        F.col("Brake").alias("brake"),
        F.col("nGear").alias("n_gear"),
        F.col("RPM").alias("rpm"),
    )
    return assign_laps(car, ff1_laps)


def fastf1_telemetry_lap(samples: DataFrame) -> DataFrame:
    """Per-lap aggregates (single definition: ``features.telemetry_agg.aggregate_samples``)."""
    df = aggregate_samples(samples).drop("n_samples")
    return df.withColumns({"is_synthetic": F.lit(False), "source": F.lit("fastf1")})


def build_telemetry_lap(synthetic: DataFrame, ff1_samples: DataFrame | None) -> DataFrame:
    candidates = [synthetic.withColumn("source", F.lit("synthetic"))]
    if ff1_samples is not None:
        candidates.append(fastf1_telemetry_lap(ff1_samples))
    return conform(_best_source_per_race(*candidates), S.TELEMETRY_LAP, "telemetry_lap")


def build_telemetry_raw(ff1_samples: DataFrame) -> DataFrame:
    df = ff1_samples.withColumn("session_time_s", F.round(F.col("t"), 3))
    return conform(df, S.TELEMETRY_RAW, "telemetry_raw")


# ============================================================================== write / verify
def write_table(df: DataFrame, clean_root: str, table: str) -> None:
    """Overwrite exactly the (season, round) partitions present in ``df``."""
    keys = list(S.PRIMARY_KEYS[table])
    (
        df.repartition(*S.PARTITION_COLUMNS)
        .sortWithinPartitions(*keys)
        .write.mode("overwrite")
        .partitionBy(*S.PARTITION_COLUMNS)
        .parquet(f"{clean_root}/{table}")
    )


def read_clean(spark: SparkSession, clean_root: str, table: str) -> DataFrame:
    """A clean table as stored (partition columns restored), or empty if never written."""
    schema = S.CLEAN_SCHEMAS[table]
    try:
        df = spark.read.schema(schema).parquet(f"{clean_root}/{table}")
    except AnalysisException as exc:
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            return empty(spark, schema)
        raise
    return df.select([f.name for f in schema.fields])


def table_checksum(df: DataFrame) -> str:
    """Order-independent content checksum (row count + sum of per-row xxhash64)."""
    cols = sorted(df.columns)
    row_hash = F.xxhash64(*[F.col(c) for c in cols]).cast("decimal(38,0)")
    count, total = df.agg(F.count(F.lit(1)), F.sum(row_hash)).first() or (0, 0)
    digest = hashlib.sha256(f"{count}:{total or 0}".encode()).hexdigest()
    return digest[:16]


def write_text(spark: SparkSession, uri: str, text: str) -> None:
    """Write a small text file to file:// or hdfs:// through the Hadoop FileSystem API."""
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise CleanError("Spark JVM gateway unavailable")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hadoop_conf)
    stream = fs.create(path, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


# ============================================================================== job
@dataclass
class CleanResult:
    raw_root: str
    clean_root: str
    races: list[RaceKey]
    counts: dict[str, int] = field(default_factory=dict)
    checksums: dict[str, str] = field(default_factory=dict)
    quality: QualityReport | None = None
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.quality is not None and self.quality.ok


def run_clean(
    spark: SparkSession,
    settings: Settings,
    *,
    source: str,
    seasons: Sequence[int] | None = None,
    races: set[RaceKey] | None = None,
    out: Callable[[str], None] = print,
) -> CleanResult:
    """Build, write, read back, checksum and quality-check the clean zone."""
    started = time.perf_counter()
    configure_session(spark)
    raw_root, clean_root = resolve_roots(settings, source)

    def sel(df: DataFrame) -> DataFrame:
        return select_races(df, seasons, races)

    pages = {t: read_json_pages(spark, raw_root, t, seasons) for t in S.RAW_JSON_SCHEMAS}
    results = sel(build_results(pages["results"])).cache()
    race_keys = sorted(
        (int(r["season"]), int(r["round"])) for r in results.select(*_RACE).distinct().collect()
    )
    if not race_keys:
        raise CleanError(f"no race results found under {raw_root}")

    qualifying = sel(build_qualifying(pages["qualifying"]))
    pitstops = sel(build_pitstops(pages["pitstops"])).cache()
    timings = sel(build_lap_timings(pages["laps"])).cache()
    unparseable = timings.where(
        F.col("time_raw").isNotNull() & F.col("lap_time_s").isNull()
    ).count()
    races_df = build_races(pages["schedule"], results)
    drivers = sel(build_drivers(pages["results"], pages["drivers"])).cache()
    constructors = sel(build_constructors(pages["results"], pages["constructors"]))

    ff1_laps_raw = sel(
        read_parquet_table(
            spark,
            f"{raw_root}/fastf1/laps/season={_season_glob(seasons)}/round=*/laps.parquet",
            S.RAW_FASTF1_LAPS,
            "FastF1 laps",
        )
    )
    ff1_car_raw = sel(
        read_parquet_table(
            spark,
            f"{raw_root}/fastf1/telemetry/season={_season_glob(seasons)}/round=*/driver=*/car_data.parquet",
            S.RAW_FASTF1_CAR,
            "FastF1 car telemetry",
        )
    )
    has_ff1 = not ff1_laps_raw.isEmpty()
    ff1_laps: DataFrame | None = None
    ff1_samples: DataFrame | None = None
    unmapped = empty(spark, StructType([]))
    mapping = drivers.select(*_RACE, "driver_id", "code", "car_number")
    if has_ff1:
        ff1_laps, unmapped_laps = map_fastf1_drivers(ff1_laps_raw, mapping)
        ff1_laps = ff1_laps.cache()
        unmapped = unmapped_laps
        if not ff1_car_raw.isEmpty():
            ff1_car, unmapped_car = map_fastf1_drivers(ff1_car_raw, mapping)
            unmapped = unmapped.unionByName(unmapped_car).distinct()
            ff1_samples = fastf1_samples(ff1_car, ff1_laps).cache()

    synth_stints = sel(
        read_parquet_table(
            spark,
            f"{raw_root}/synthetic/tyre_stints/season={_season_glob(seasons)}/round=*/tyre_stints.parquet",
            S.RAW_SYNTHETIC_STINTS,
            "synthetic tyre_stints",
        )
    )
    synth_tel = sel(
        read_parquet_table(
            spark,
            f"{raw_root}/synthetic/lap_telemetry/season={_season_glob(seasons)}/round=*/lap_telemetry.parquet",
            S.RAW_SYNTHETIC_TELEMETRY,
            "synthetic lap_telemetry",
        )
    )

    laps = build_laps(timings.drop("time_raw"), pitstops, ff1_laps).cache()
    tables: dict[str, DataFrame] = {
        "races": races_df,
        "drivers": drivers,
        "constructors": constructors,
        "results": results,
        "qualifying": qualifying,
        "laps": laps,
        "pitstops": pitstops,
        "stints": build_stints(laps, pitstops, synth_stints, ff1_laps),
        "telemetry_lap": build_telemetry_lap(synth_tel, ff1_samples),
        "telemetry_raw": build_telemetry_raw(ff1_samples)
        if ff1_samples is not None
        else empty(spark, S.TELEMETRY_RAW),
    }
    for name in CLEAN_TABLES:
        write_table(tables[name], clean_root, name)

    selected = set(race_keys)
    stored = {
        name: select_races(read_clean(spark, clean_root, name), None, selected).cache()
        for name in CLEAN_TABLES
    }
    result = CleanResult(raw_root, clean_root, race_keys)
    for name, df in stored.items():
        result.counts[name] = df.count()
        result.checksums[name] = table_checksum(df)
    result.quality = run_checks(
        stored,
        unmapped_fastf1=unmapped if has_ff1 else None,
        unparseable_times=unparseable,
        expected_races=race_keys,
    )
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source": source,
        "raw_root": raw_root,
        "clean_root": clean_root,
        "races": [list(k) for k in race_keys],
        "counts": result.counts,
        "checksums": result.checksums,
        "quality": result.quality.to_dict(),
    }
    write_text(spark, f"{clean_root}/_quality/report.json", json.dumps(report, indent=1) + "\n")
    result.seconds = time.perf_counter() - started
    _print_result(result, out)
    for df in stored.values():
        df.unpersist()
    return result


def _print_result(result: CleanResult, out: Callable[[str], None]) -> None:
    races = ", ".join(f"{s} R{r}" for s, r in result.races)
    out(f"clean zone: {result.raw_root} -> {result.clean_root}")
    out(f"races: {races}")
    for name in CLEAN_TABLES:
        out(f"  {name:<14} {result.counts[name]:>8} rows  checksum {result.checksums[name]}")
    assert result.quality is not None
    for line in result.quality.summary_lines():
        out(line)
    out(f"done in {result.seconds:.1f}s")


def _parse_seasons(value: str) -> list[int]:
    from f1sa.ingest.cli import parse_seasons

    return parse_seasons(value)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m f1sa.etl.clean")
    parser.add_argument("--source", choices=("sample", "lake"), required=True)
    parser.add_argument("--seasons", type=_parse_seasons, default=None)
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args(list(argv) if argv is not None else None)
    settings = get_settings()
    try:
        _raw, clean_root = resolve_roots(settings, args.source)
    except CleanError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    local_dir = None
    if clean_root.startswith("file://"):
        local_dir = Path(clean_root.removeprefix("file://")).parent / ".spark-tmp"
    spark = build_spark("f1sa-clean", args.master, local_dir=local_dir)
    try:
        result = run_clean(spark, settings, source=args.source, seasons=args.seasons)
    except CleanError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
