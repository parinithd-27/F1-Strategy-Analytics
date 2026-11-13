"""Synthetic replication of the lap-level clean tables for the scalability benchmark.

Replica ``r = 0`` is the input, unchanged (``is_replicated = false``). Replica ``r >= 1`` of a row:

* ``season + 100·r``, so every race key ``(season, round)`` and ``race_seq = season*100 + round``
  stay unique and the replicas sort after the real seasons (2024 → 2124, 2224, …);
* ``race_date`` and ``race_start_utc`` moved by ``100·r`` years, so date order agrees with season
  order;
* ``driver_id + "__r<r>"`` in every table that has a driver, so drivers are distinct per replica;
* every other value unchanged: lap times, gaps, pit stops and constructor ids (the feature build's
  lineage check fails on unknown constructor ids);
* ``is_replicated = true``.

The result therefore has exactly ``factor`` times the rows of every table, with unique keys. It is
synthetic: replicas are exact copies, so skew, cardinalities and compression are idealised (see
docs/experiments.md). Replicated data is written only under ``/f1/bench`` or ``./data/bench``
(:func:`f1sa.bench.paths.check_bench_uri`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.bench.paths import check_bench_uri
from f1sa.etl.clean import read_clean
from f1sa.etl.io import FEATURE_INPUTS, write_partitioned
from f1sa.lake import schemas as S

SEASON_STEP = 100
DRIVER_SUFFIX = "__r"
FLAG = "is_replicated"
MAX_YEAR = 9999
_R = "_replica"


def replica_season(season: int, replica: int) -> int:
    """The season of ``replica`` of a real ``season``."""
    if replica < 0:
        raise ValueError(f"replica must be >= 0, got {replica}")
    return season + SEASON_STEP * replica


def replica_driver_id(driver_id: str, replica: int) -> str:
    """The driver id of ``replica`` (replica 0 keeps the real id)."""
    if replica < 0:
        raise ValueError(f"replica must be >= 0, got {replica}")
    return driver_id if replica == 0 else f"{driver_id}{DRIVER_SUFFIX}{replica}"


def check_factor(factor: int, max_season: int | None = None) -> None:
    """``factor`` must be >= 1, and the last replica's season must stay a valid year."""
    if factor < 1:
        raise ValueError(f"replication factor must be >= 1, got {factor}")
    if max_season is not None and replica_season(max_season, factor - 1) > MAX_YEAR:
        raise ValueError(
            f"factor {factor} moves season {max_season} past {MAX_YEAR} "
            f"(step {SEASON_STEP} per replica)"
        )


def replicate_table(df: DataFrame, factor: int) -> DataFrame:
    """``df`` repeated ``factor`` times with perturbed race keys and driver ids plus ``FLAG``."""
    check_factor(factor)
    reps = df.sparkSession.range(factor).select(F.col("id").cast("int").alias(_R))
    out = df.crossJoin(F.broadcast(reps))
    r = F.col(_R)
    out = out.withColumn("season", (F.col("season") + F.lit(SEASON_STEP) * r).cast("int"))
    if "driver_id" in df.columns:
        out = out.withColumn(
            "driver_id",
            F.when(r == 0, F.col("driver_id")).otherwise(
                F.concat(F.col("driver_id"), F.lit(DRIVER_SUFFIX), r.cast("string"))
            ),
        )
    years = F.lit(SEASON_STEP) * r
    if "race_date" in df.columns:
        out = out.withColumn("race_date", F.add_months(F.col("race_date"), years * 12))
    if "race_start_utc" in df.columns:
        out = out.withColumn(
            "race_start_utc", F.timestamp_add("YEAR", years, F.col("race_start_utc"))
        )
    out = out.withColumn(FLAG, r > 0)
    return out.select(*df.columns, FLAG)


def replicate_tables(tables: Mapping[str, DataFrame], factor: int) -> dict[str, DataFrame]:
    """Every table replicated ``factor`` times (see the module docstring)."""
    return {name: replicate_table(df, factor) for name, df in tables.items()}


def strip_flag(tables: Mapping[str, DataFrame]) -> dict[str, DataFrame]:
    """The tables projected back to their clean schemas (``FLAG`` dropped), for the feature
    build, which expects exactly the clean columns."""
    return {
        name: df.select([f.name for f in S.CLEAN_SCHEMAS[name].fields])
        for name, df in tables.items()
    }


def load_seasons(
    spark: SparkSession,
    clean_root: str,
    seasons: Sequence[int],
    tables: Iterable[str] = FEATURE_INPUTS,
) -> dict[str, DataFrame]:
    """The clean tables of ``seasons`` (read only; the clean zone is never written here)."""
    if not seasons:
        raise ValueError("no seasons given")
    return {
        name: read_clean(spark, clean_root, name).where(F.col("season").isin(list(seasons)))
        for name in tables
    }


def materialise(
    spark: SparkSession,
    clean_root: str,
    seasons: Sequence[int],
    factor: int,
    out_root: str,
    tables: Iterable[str] = FEATURE_INPUTS,
) -> dict[str, int]:
    """Write the ``factor``-times replicated clean tables of ``seasons`` to ``out_root/<table>``
    (partitioned by season and round, ``FLAG`` kept). Returns the rows written per table."""
    out_root = check_bench_uri(out_root)
    check_factor(factor, max(seasons))
    counts: dict[str, int] = {}
    for name, df in load_seasons(spark, clean_root, seasons, tables).items():
        rep = replicate_table(df, factor).cache()
        counts[name] = rep.count()
        if counts[name]:
            write_partitioned(rep, out_root, name, S.PRIMARY_KEYS[name])
        rep.unpersist()
    return counts


def read_replicated(
    spark: SparkSession, root: str, tables: Iterable[str] = FEATURE_INPUTS
) -> dict[str, DataFrame]:
    """Replicated tables as written by :func:`materialise`, projected to the clean schemas.
    A table that had no rows (``telemetry_raw`` without FastF1) is read as empty."""
    root = check_bench_uri(root)
    return {name: read_clean(spark, root, name) for name in tables}
