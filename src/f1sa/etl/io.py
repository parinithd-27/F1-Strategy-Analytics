"""Shared read/write helpers for the Step 6 feature jobs.

The clean zone is read one table directory at a time (``{clean}/{table}``) through
``clean.read_clean``, so the ``_quality`` folder is never read. Feature tables are written to
``{lake}/features/{table}`` with ``partitionBy(season, round)`` and dynamic partition overwrite.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from pyspark.sql import DataFrame, SparkSession

from f1sa.config import Settings
from f1sa.etl.clean import CleanError, check_output_root, read_clean, select_races, spark_uri
from f1sa.lake import schemas as S

RaceKey = tuple[int, int]
FEATURE_INPUTS = ("races", "drivers", "constructors", "results", "laps", "pitstops", "stints")
_SEASON_DIR = re.compile(r"^season=(\d+)$")
_ROUND_DIR = re.compile(r"^round=(\d+)$")


def resolve_feature_roots(settings: Settings) -> tuple[str, str]:
    """(clean input root, features output root). File mode refuses output outside ./data/lake."""
    lake = spark_uri(settings.data_lake_uri)
    features = f"{lake}/features"
    check_output_root(features)
    return f"{lake}/clean", features


def sample_races(settings: Settings) -> list[RaceKey]:
    """Races in the offline sample bundle (``{sample}/raw/jolpica/results/season=S/round=R``)."""
    base = settings.sample_dir / "raw" / "jolpica" / "results"
    if not base.is_dir():
        raise CleanError(
            f"sample bundle not found at {settings.sample_dir}; run `make sample-data`"
        )
    keys: list[RaceKey] = []
    for season_dir in base.iterdir():
        s = _SEASON_DIR.match(season_dir.name)
        if not (s and season_dir.is_dir()):
            continue
        for round_dir in season_dir.iterdir():
            r = _ROUND_DIR.match(round_dir.name)
            if r and round_dir.is_dir():
                keys.append((int(s.group(1)), int(r.group(1))))
    if not keys:
        raise CleanError(f"sample bundle at {settings.sample_dir} lists no races")
    return sorted(keys)


def load_clean(
    spark: SparkSession, clean_root: str, tables: Iterable[str] = FEATURE_INPUTS
) -> dict[str, DataFrame]:
    """The named clean tables (all races), read with their explicit clean schemas."""
    out: dict[str, DataFrame] = {}
    for name in tables:
        if name not in S.CLEAN_SCHEMAS:
            raise CleanError(f"unknown clean table {name!r}")
        out[name] = read_clean(spark, clean_root, name)
    return out


def race_keys(df: DataFrame) -> list[RaceKey]:
    """Distinct (season, round) present in ``df``, sorted."""
    rows = df.select("season", "round").distinct().collect()
    return sorted((int(r["season"]), int(r["round"])) for r in rows)


def restrict(tables: Mapping[str, DataFrame], races: set[RaceKey]) -> dict[str, DataFrame]:
    """Every table limited to ``races``."""
    return {name: select_races(df, None, races) for name, df in tables.items()}


def write_partitioned(df: DataFrame, root: str, table: str, keys: Iterable[str]) -> None:
    """Overwrite exactly the (season, round) partitions present in ``df``."""
    (
        df.repartition(*S.PARTITION_COLUMNS)
        .sortWithinPartitions(*keys)
        .write.mode("overwrite")
        .partitionBy(*S.PARTITION_COLUMNS)
        .parquet(f"{root}/{table}")
    )
