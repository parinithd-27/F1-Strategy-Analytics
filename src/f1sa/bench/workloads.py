"""The timed Spark workloads of the scalability benchmark.

* :func:`feature_build`: the whole Step 6-8 feature computation
  (``features.build.compute_features``, unchanged) over the given clean tables, with every
  feature table written through the production writer ``etl.io.write_partitioned`` to a
  benchmark root. ``undercut_attempts`` (which carries
  labels and outcomes) is executed in full but written to Spark's ``noop`` sink, so no label or
  outcome is ever collected, read back or persisted.
* :func:`telemetry_aggregation`: the heavy aggregation over the real 2024 raw telemetry samples
  (``raw/tracinginsights/telemetry``, read only): ``features.telemetry_agg.aggregate_samples``
  (unchanged; a lag window per lap and a groupBy) per (driver, lap), joined to the clean laps and
  summarised per round, into ``noop``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from f1sa.bench.paths import check_bench_uri
from f1sa.etl.io import race_keys, write_partitioned
from f1sa.features import schemas as FS
from f1sa.features.build import compute_features
from f1sa.features.lineage import unknown_constructor_ids
from f1sa.features.telemetry_agg import aggregate_samples

NOOP_TABLES = frozenset({"undercut_attempts"})
"""Executed but never persisted: these tables hold labels and outcomes."""

TELEMETRY_SEASON = 2024
TELEMETRY_ROUNDS_BY_SEASONS: dict[int, int] = {1: 5, 3: 14, 5: 24}
"""Seasons axis -> number of 2024 rounds of raw telemetry (about 20 / 60 / 100 % of the bytes)."""

RAW_TELEMETRY_SCHEMA = T.StructType(
    [
        T.StructField("driver_code", T.StringType()),
        T.StructField("lap", T.LongType()),
        T.StructField("time_in_lap_s", T.DoubleType()),
        T.StructField("speed_kph", T.DoubleType()),
        T.StructField("rpm", T.DoubleType()),
        T.StructField("n_gear", T.LongType()),
        T.StructField("throttle_pct", T.DoubleType()),
        T.StructField("brake", T.BooleanType()),
        T.StructField("season", T.IntegerType()),
        T.StructField("round", T.IntegerType()),
        T.StructField("driver", T.StringType()),
    ]
)
"""The columns the aggregation reads (the files hold 20); season, round and driver are the
partition columns of ``season=/round=/driver=``."""


class WorkloadError(RuntimeError):
    """A workload cannot run on the given input."""


def write_noop(df: DataFrame) -> None:
    """Execute ``df`` completely and discard the rows."""
    df.write.format("noop").mode("overwrite").save()


def feature_build(spark: SparkSession, clean: Mapping[str, DataFrame], out_root: str) -> list[str]:
    """Compute every feature table for every race in ``clean`` and write them to ``out_root``
    (except :data:`NOOP_TABLES`). The clean inputs are cached for the run, as in ``run_build``,
    and released at the end, so every run starts from storage. Returns the tables written."""
    out_root = check_bench_uri(out_root)
    inputs = {name: df.cache() for name, df in clean.items()}
    try:
        unknown = unknown_constructor_ids(inputs["results"])
        if unknown:
            raise WorkloadError(f"unknown constructor ids {unknown}")
        targets = set(race_keys(inputs["races"]))
        if not targets:
            raise WorkloadError("the clean input holds no race")
        written: list[str] = []
        for name, df in compute_features(spark, inputs, targets).items():
            if name in NOOP_TABLES:
                write_noop(df)
                continue
            write_partitioned(df, out_root, name, FS.FEATURE_KEYS[name])
            written.append(name)
        return written
    finally:
        spark.catalog.clearCache()


def telemetry_rounds(n_seasons: int, available: Sequence[int]) -> list[int]:
    """The first ``TELEMETRY_ROUNDS_BY_SEASONS[n_seasons]`` of the ``available`` 2024 rounds."""
    if n_seasons not in TELEMETRY_ROUNDS_BY_SEASONS:
        raise ValueError(f"no telemetry slice for {n_seasons} seasons")
    want = TELEMETRY_ROUNDS_BY_SEASONS[n_seasons]
    rounds = sorted(set(available))
    if len(rounds) < want:
        raise WorkloadError(f"{want} telemetry rounds needed, {len(rounds)} available")
    return rounds[:want]


def available_rounds(spark: SparkSession, telemetry_root: str, season: int) -> list[int]:
    """Rounds with a ``season=S/round=R`` folder under ``telemetry_root`` (Hadoop FS listing)."""
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise WorkloadError("Spark JVM gateway unavailable")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(f"{telemetry_root}/season={season}")
    fs = path.getFileSystem(conf)
    if not fs.exists(path):
        return []
    rounds: list[int] = []
    for status in fs.listStatus(path):
        name = str(status.getPath().getName())
        if status.isDirectory() and name.startswith("round="):
            rounds.append(int(name.removeprefix("round=")))
    return sorted(rounds)


def read_raw_telemetry(
    spark: SparkSession, telemetry_root: str, season: int, rounds: Sequence[int]
) -> DataFrame:
    """The raw samples of ``rounds`` (read only)."""
    if not rounds:
        raise WorkloadError("no telemetry rounds selected")
    paths = [f"{telemetry_root}/season={season}/round={r}" for r in sorted(rounds)]
    return (
        spark.read.schema(RAW_TELEMETRY_SCHEMA).option("basePath", telemetry_root).parquet(*paths)
    )


def lap_aggregates(raw: DataFrame) -> DataFrame:
    """Per (season, round, driver code, lap) aggregates via ``aggregate_samples`` (the driver
    code stands in for ``driver_id``)."""
    samples = raw.select(
        "season",
        "round",
        F.col("driver_code").alias("driver_id"),
        F.col("lap").cast("int").alias("lap"),
        F.col("time_in_lap_s").alias("t"),
        "speed_kph",
        "throttle_pct",
        "brake",
        F.col("n_gear").cast("int").alias("n_gear"),
        "rpm",
    )
    return aggregate_samples(samples)


def round_summary(per_lap: DataFrame, drivers: DataFrame, laps: DataFrame) -> DataFrame:
    """Per-lap aggregates joined to the clean laps (driver code -> driver id via ``drivers``) and
    summarised per round."""
    codes = drivers.select("season", "round", "driver_id", F.col("code").alias("_code"))
    lap_times = laps.select("season", "round", "driver_id", "lap", "lap_time_s")
    joined = (
        per_lap.withColumnRenamed("driver_id", "_code")
        .join(codes, ["season", "round", "_code"])
        .join(lap_times, ["season", "round", "driver_id", "lap"])
    )
    # Every per-lap aggregate is used, so the optimizer cannot prune any of them (an unused
    # gear_changes would drop the lag window from the plan; checked in the unit test).
    return joined.groupBy("season", "round").agg(
        F.count(F.lit(1)).alias("laps"),
        F.countDistinct("driver_id").alias("drivers"),
        F.round(F.avg("mean_speed_kph"), 6).alias("mean_speed_kph"),
        F.max("max_speed_kph").alias("max_speed_kph"),
        F.round(F.avg("pct_full_throttle"), 6).alias("pct_full_throttle"),
        F.round(F.avg("brake_fraction"), 6).alias("brake_fraction"),
        F.sum("gear_changes").alias("gear_changes"),
        F.round(F.avg("mean_rpm"), 3).alias("mean_rpm"),
        F.round(F.avg("lap_time_s"), 6).alias("mean_lap_time_s"),
        F.round(F.corr("mean_speed_kph", "lap_time_s"), 6).alias("speed_lap_time_corr"),
        F.sum("n_samples").alias("samples"),
    )


def telemetry_aggregation(
    spark: SparkSession,
    telemetry_root: str,
    clean: Mapping[str, DataFrame],
    rounds: Sequence[int],
    season: int = TELEMETRY_SEASON,
) -> None:
    """The heavy telemetry aggregation of ``rounds``, executed into ``noop``."""
    raw = read_raw_telemetry(spark, telemetry_root, season, rounds)
    write_noop(round_summary(lap_aggregates(raw), clean["drivers"], clean["laps"]))
