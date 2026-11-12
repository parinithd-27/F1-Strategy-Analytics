"""Per-lap telemetry aggregates from TracingInsights per-lap telemetry files (#4, #12).

The definitions are **not** reimplemented here. Samples are put on the session clock and passed
to the existing :func:`f1sa.features.telemetry_agg.assign_to_laps` and
:func:`~f1sa.features.telemetry_agg.aggregate_samples`, exactly as the clean FastF1 path does
(``etl.clean.fastf1_samples`` + ``fastf1_telemetry_lap``). Full throttle is therefore
``throttle >= telemetry_agg.FULL_THROTTLE_PCT``, the lap window is half-open, and a boundary
sample belongs to the later lap.

TracingInsights specifics handled before that:

* ``time`` in a lap file counts from the lap start, so the session time is
  ``lap_start_session_s + time``, rounded to the millisecond (as ``build_telemetry_raw`` does);
* consecutive lap files share their boundary sample (lap N ends with the sample that starts lap
  N+1), so exact duplicate ``(driver, t)`` samples are dropped before lap assignment, keeping
  the later lap's copy.

Added on top of the shared aggregates (TracingInsights only):

* ``telemetry_coverage``: ``none`` when the lap window has no sample; ``partial`` when the samples
  span less than ``MIN_SPAN_SHARE`` of the window or leave a gap longer than ``MAX_GAP_S``
  (the window edges count as gap ends); ``full`` otherwise;
* ``n_samples_outside_window`` per race (samples outside every window, e.g. the last sample of the
  final lap), returned by :func:`aggregate_race` as a diagnostic.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from f1sa.features.telemetry_agg import AGG_COLUMNS, aggregate_samples, assign_to_laps

MIN_SPAN_SHARE = 0.90
"""A lap whose samples cover less than this share of its window is ``partial``."""
MAX_GAP_S = 2.0
"""A lap with a gap between samples (or to a window edge) longer than this is ``partial``.
Ordinary feed dropouts of 1.0-1.3 s occur on green laps (2024 R5 sample), so 1 s would flag them."""
COVERAGE_VALUES = ("full", "partial", "none")

SAMPLE_SCHEMA = T.StructType(
    [
        T.StructField("season", T.IntegerType(), False),
        T.StructField("round", T.IntegerType(), False),
        T.StructField("driver_id", T.StringType(), False),
        T.StructField("t", T.DoubleType(), False),
        T.StructField("speed_kph", T.DoubleType(), True),
        T.StructField("throttle_pct", T.DoubleType(), True),
        T.StructField("brake", T.BooleanType(), True),
        T.StructField("n_gear", T.IntegerType(), True),
        T.StructField("rpm", T.DoubleType(), True),
    ]
)
WINDOW_SCHEMA = T.StructType(
    [
        T.StructField("season", T.IntegerType(), False),
        T.StructField("round", T.IntegerType(), False),
        T.StructField("driver_id", T.StringType(), False),
        T.StructField("lap", T.IntegerType(), False),
        T.StructField("lap_start_s", T.DoubleType(), True),
        T.StructField("lap_end_s", T.DoubleType(), True),
    ]
)
OUTPUT_COLUMNS = (
    "season",
    "round",
    "driver_code",
    "lap",
    *AGG_COLUMNS,
    "telemetry_coverage",
)
_DRIVER = ["season", "round", "driver_id"]


@dataclass
class RaceAggregate:
    frame: pd.DataFrame
    """One row per lap window, columns ``OUTPUT_COLUMNS``.

    ``driver_code`` is the TracingInsights code; the clean job maps it to ``driver_id``."""
    n_samples_in: int
    n_duplicate_samples: int
    n_samples_outside_window: int


def session_samples(telemetry: pd.DataFrame, laps: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Put per-lap samples on the session clock and drop boundary duplicates.

    ``telemetry`` has driver_code, lap, time_in_lap_s, speed_kph, throttle_pct, brake, n_gear and
    rpm (``tracinginsights_parse.parse_telemetry``). ``laps`` has driver_code, lap and
    lap_start_session_s (``parse_laps``). Samples of a lap without a start time are dropped.
    Returns (samples with ``t``, number of duplicates removed).
    """
    starts = laps[["driver_code", "lap", "lap_start_session_s"]].dropna()
    merged = telemetry.merge(starts, on=["driver_code", "lap"], how="inner")
    merged["t"] = (merged["lap_start_session_s"] + merged["time_in_lap_s"]).astype(float).round(3)
    before = len(merged)
    # keep the copy from the later lap's file (its time-0 sample): the boundary sample belongs
    # to the later lap, and the result does not depend on the input order
    merged = merged.sort_values(["driver_code", "t", "lap"], ascending=[True, True, False])
    merged = merged.drop_duplicates(["driver_code", "t"], keep="first").reset_index(drop=True)
    return merged, before - len(merged)


def _coverage(
    samples: DataFrame, windows: DataFrame, min_span_share: float, max_gap_s: float
) -> DataFrame:
    """``telemetry_coverage`` per lap window from the assigned samples."""
    w = Window.partitionBy(*_DRIVER, "lap").orderBy("t")
    gaps = (
        samples.where(F.col("lap").isNotNull())
        .withColumn("_gap", F.col("t") - F.lag("t").over(w))
        .groupBy(*_DRIVER, "lap")
        .agg(
            F.min("t").alias("_first"),
            F.max("t").alias("_last"),
            F.max("_gap").alias("_max_gap"),
        )
    )
    joined = windows.join(gaps, [*_DRIVER, "lap"], "left")
    length = F.col("lap_end_s") - F.col("lap_start_s")
    max_gap = F.greatest(
        F.coalesce(F.col("_max_gap"), F.lit(0.0)),
        F.col("_first") - F.col("lap_start_s"),
        F.col("lap_end_s") - F.col("_last"),
    )
    span = F.col("_last") - F.col("_first")
    return joined.select(
        *_DRIVER,
        "lap",
        F.when(F.col("_first").isNull(), F.lit("none"))
        .when((span < F.lit(min_span_share) * length) | (max_gap > F.lit(max_gap_s)), "partial")
        .otherwise("full")
        .alias("telemetry_coverage"),
    )


def aggregate_race(
    spark: SparkSession,
    season: int,
    round_: int,
    telemetry: pd.DataFrame,
    laps: pd.DataFrame,
    *,
    min_span_share: float = MIN_SPAN_SHARE,
    max_gap_s: float = MAX_GAP_S,
) -> RaceAggregate:
    """Aggregate one race. Every lap of ``laps`` with a start and end time gets a row; a lap
    without samples has ``n_samples = 0``, null aggregates and coverage ``none``."""
    samples, n_dup = session_samples(telemetry, laps)
    pdf_samples = pd.DataFrame(
        {
            "season": season,
            "round": round_,
            "driver_id": samples["driver_code"].astype(str),
            "t": samples["t"].astype(float),
            "speed_kph": samples["speed_kph"].astype("Float64"),
            "throttle_pct": samples["throttle_pct"].astype("Float64"),
            "brake": samples["brake"].astype("boolean"),
            "n_gear": samples["n_gear"].astype("Int64"),
            "rpm": samples["rpm"].astype("Float64"),
        }
    )
    win = laps[["driver_code", "lap", "lap_start_session_s", "lap_end_session_s"]]
    win = win.dropna(subset=["lap_start_session_s", "lap_end_session_s"])
    pdf_windows = pd.DataFrame(
        {
            "season": season,
            "round": round_,
            "driver_id": win["driver_code"].astype(str),
            "lap": win["lap"].astype(int),
            "lap_start_s": win["lap_start_session_s"].astype(float),
            "lap_end_s": win["lap_end_session_s"].astype(float),
        }
    )
    sdf = spark.createDataFrame(_records(pdf_samples), SAMPLE_SCHEMA)
    wdf = spark.createDataFrame(_records(pdf_windows), WINDOW_SCHEMA)
    assigned = assign_to_laps(sdf, wdf).cache()
    try:
        outside = assigned.where(F.col("lap").isNull()).count()
        agg = aggregate_samples(assigned)
        rows = (
            wdf.select(*_DRIVER, "lap")
            .join(agg, [*_DRIVER, "lap"], "left")
            .join(_coverage(assigned, wdf, min_span_share, max_gap_s), [*_DRIVER, "lap"], "left")
            .withColumn("n_samples", F.coalesce("n_samples", F.lit(0)))
            .withColumnRenamed("driver_id", "driver_code")
            .select(*OUTPUT_COLUMNS)
            .orderBy("driver_code", "lap")
        )
        frame = _with_nullable_ints(rows.toPandas(), rows.schema)
    finally:
        assigned.unpersist()
    return RaceAggregate(frame, len(samples) + n_dup, n_dup, int(outside))


_NULLABLE_INT: dict[type[T.DataType], str] = {
    T.IntegerType: "Int32",
    T.LongType: "Int64",
    T.ShortType: "Int16",
}


def _with_nullable_ints(pdf: pd.DataFrame, schema: T.StructType) -> pd.DataFrame:
    """Keep Spark integer columns integer after ``toPandas()``.

    ``toPandas()`` turns an integer column that holds a null (e.g. ``gear_changes`` on a lap with
    no samples) into float64, which Parquet then stores as DOUBLE and Spark refuses to read back
    as INT. Casting to pandas nullable integers keeps the physical type; values are unchanged."""
    out = pdf.copy()
    for field in schema.fields:
        dtype = _NULLABLE_INT.get(type(field.dataType))
        if dtype is not None:
            out[field.name] = out[field.name].astype(dtype)
    return out


def _records(pdf: pd.DataFrame) -> list[tuple[object, ...]]:
    """Rows as Python tuples with pandas NA turned into None (Spark-safe)."""
    clean = pdf.astype(object).where(pdf.notna(), None)
    return list(clean.itertuples(index=False, name=None))
