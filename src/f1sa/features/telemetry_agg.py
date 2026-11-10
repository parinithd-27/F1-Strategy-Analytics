"""Per-lap telemetry aggregates (#12, #4). Batch DataFrame jobs only.

Telemetry is a batch feature. It must never be imported by streaming code
(``f1sa.streaming``), and a unit test enforces this. The streaming replay carries lap-level
events only.

* ``assign_to_laps`` puts each sample in the lap whose session-time window
  ``[lap_start, lap_end)`` contains it. The window is half-open, so a sample exactly on a
  boundary goes to the **later** lap. A sample before the first lap starts, after the last lap
  ends, or in a gap between windows gets ``lap = null``.
* ``aggregate_samples`` runs ``groupBy(season, round, driver_id, lap)`` and computes mean and
  max speed, the share of samples at full throttle (``throttle >= FULL_THROTTLE_PCT``), the
  brake share, gear changes (consecutive samples within the lap whose ``n_gear`` differs) and
  mean RPM. Averages are over samples and are not time-weighted.
* ``telemetry_lap_features`` chooses the source per race. Races with real FastF1 samples in the
  clean ``telemetry_raw`` table (and lap windows in clean laps) are aggregated from raw. For any
  other race, the clean ``telemetry_lap`` rows, which are synthetic in sample mode, are passed
  through unchanged and keep ``is_synthetic``.

This module imports only pyspark and ``features.schemas``, so ``etl.clean`` can import it
without creating an import cycle.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from f1sa.features import schemas as FS

FULL_THROTTLE_PCT = 98.0
"""Throttle (percent) at or above which a sample counts as full throttle."""

_RACE = ["season", "round"]
_DRIVER = ["season", "round", "driver_id"]
AGG_COLUMNS = (
    "mean_speed_kph",
    "max_speed_kph",
    "pct_full_throttle",
    "brake_fraction",
    "gear_changes",
    "mean_rpm",
    "n_samples",
)


def _conform(df: DataFrame, schema: StructType) -> DataFrame:
    missing = [f.name for f in schema.fields if f.name not in df.columns]
    if missing:
        raise ValueError(f"telemetry frame is missing columns {missing}")
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def assign_to_laps(samples: DataFrame, windows: DataFrame) -> DataFrame:
    """Add ``lap`` to every sample. ``samples`` needs season, round, driver_id and ``t`` (session
    seconds). ``windows`` needs season, round, driver_id, lap, lap_start_s and lap_end_s.

    This is one sorted stream per driver: lap-start markers and samples are interleaved by
    time and the last marker is carried forward. That scales linearly, unlike a range join. At
    equal times the marker sorts first, so a sample at exactly ``lap_start`` belongs to that
    lap. A sample at or after the carried lap's ``lap_end`` gets ``lap = null``.
    """
    sample_cols = [c for c in samples.columns if c not in (*_DRIVER, "t", "lap")]
    markers = windows.where(
        F.col("lap").isNotNull() & F.col("lap_start_s").isNotNull() & F.col("lap_end_s").isNotNull()
    ).select(
        *_DRIVER,
        F.col("lap_start_s").cast("double").alias("t"),
        F.col("lap").cast("int").alias("lap"),
        F.col("lap_end_s").cast("double").alias("lap_end"),
        F.lit(1).alias("_marker"),
        *[F.lit(None).cast(samples.schema[c].dataType).alias(c) for c in sample_cols],
    )
    points = samples.select(
        *_DRIVER,
        F.col("t").cast("double").alias("t"),
        F.lit(None).cast("int").alias("lap"),
        F.lit(None).cast("double").alias("lap_end"),
        F.lit(0).alias("_marker"),
        *sample_cols,
    )
    w = (
        Window.partitionBy(*_DRIVER)
        .orderBy(F.col("t"), F.col("_marker").desc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    stream = markers.unionByName(points).withColumns(
        {
            "_lap": F.last("lap", ignorenulls=True).over(w),
            "_end": F.last("lap_end", ignorenulls=True).over(w),
        }
    )
    out = stream.where(F.col("_marker") == 0).withColumn(
        "lap", F.when(F.col("t") < F.col("_end"), F.col("_lap"))
    )
    return out.drop("_lap", "_end", "_marker", "lap_end")


def aggregate_samples(samples: DataFrame) -> DataFrame:
    """Per-lap aggregates of assigned samples (rows with ``lap = null`` are ignored).

    Needs season, round, driver_id, lap, t, speed_kph, throttle_pct, brake, n_gear and rpm.
    It returns one row per lap that has at least one sample, with the columns in
    ``AGG_COLUMNS``.
    """
    by_lap = samples.where(F.col("lap").isNotNull())
    w = Window.partitionBy(*_DRIVER, "lap").orderBy("t")
    changed = (F.col("n_gear") != F.lag("n_gear").over(w)).cast("int")
    return (
        by_lap.withColumn("_chg", F.coalesce(changed, F.lit(0)))
        .groupBy(*_DRIVER, "lap")
        .agg(
            F.round(F.avg("speed_kph"), 3).alias("mean_speed_kph"),
            F.round(F.max("speed_kph"), 3).alias("max_speed_kph"),
            F.round(F.avg((F.col("throttle_pct") >= FULL_THROTTLE_PCT).cast("double")), 4).alias(
                "pct_full_throttle"
            ),
            F.round(F.avg(F.col("brake").cast("double")), 4).alias("brake_fraction"),
            F.sum("_chg").cast("int").alias("gear_changes"),
            F.round(F.avg("rpm"), 1).alias("mean_rpm"),
            F.count("*").cast("int").alias("n_samples"),
        )
    )


def lap_windows(laps: DataFrame) -> DataFrame:
    """Session-time lap windows from clean laps (null without FastF1)."""
    return laps.where(
        F.col("lap_start_session_s").isNotNull() & F.col("lap_end_session_s").isNotNull()
    ).select(
        *_DRIVER,
        "lap",
        F.col("lap_start_session_s").alias("lap_start_s"),
        F.col("lap_end_session_s").alias("lap_end_s"),
    )


def telemetry_lap_features(
    telemetry_raw: DataFrame, laps: DataFrame, telemetry_lap: DataFrame
) -> DataFrame:
    """Per-lap telemetry features with the source chosen per race (see the module docstring).

    Raw races get one row per lap that has a window. A lap without samples has ``n_samples = 0``
    and null aggregates. Their rows have ``is_synthetic=false`` and ``source='fastf1_raw'``.
    Pass-through rows are unchanged, with ``n_samples`` null.
    """
    windows = lap_windows(laps)
    raw_races = (
        telemetry_raw.select(*_RACE).distinct().join(windows.select(*_RACE).distinct(), _RACE)
    )
    raw = telemetry_raw.join(raw_races, _RACE, "left_semi").select(
        *_DRIVER,
        F.col("session_time_s").alias("t"),
        "speed_kph",
        "throttle_pct",
        "brake",
        "n_gear",
        "rpm",
    )
    race_windows = windows.join(raw_races, _RACE, "left_semi")
    agg = aggregate_samples(assign_to_laps(raw, race_windows))
    from_raw = (
        race_windows.select(*_DRIVER, "lap")
        .join(agg, [*_DRIVER, "lap"], "left")
        .withColumn("n_samples", F.coalesce("n_samples", F.lit(0)))
        .withColumn("is_synthetic", F.lit(False))
        .withColumn("source", F.lit("fastf1_raw"))
    )
    passed = telemetry_lap.join(raw_races, _RACE, "left_anti").withColumn(
        "n_samples", F.lit(None).cast("int")
    )
    return _conform(
        _conform(from_raw, FS.TELEMETRY_LAP_FEATURES).unionByName(
            _conform(passed, FS.TELEMETRY_LAP_FEATURES)
        ),
        FS.TELEMETRY_LAP_FEATURES,
    )
