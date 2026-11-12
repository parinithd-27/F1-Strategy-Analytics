"""Additive clean tables from the TracingInsights raw zone (Step 4C; #1, #3, #7, #12).

    python -m f1sa.etl.clean_tracinginsights [--seasons 2021-2025]      (f1sa-clean-ti)

Reads ``{lake}/raw/tracinginsights/*`` (written by ``ingest.tracinginsights_loader``) and,
read-only, the v1 clean ``drivers`` table for the driver mapping. Writes **new** tables only,
partitioned by season and round with dynamic overwrite:

``stints_tracinginsights``
    One row per (driver, stint) from the per-lap ``stint`` / ``compound`` / ``tyre_life`` fields.
    ``compound`` is SOFT/MEDIUM/HARD/INTERMEDIATE/WET or **null** (UNKNOWN, TEST_UNKNOWN or a
    missing value) with ``compound_missing``; it is never imputed. A stint whose laps disagree on
    the compound gets ``compound_conflict`` and a null compound. ``tyre_age_at_start`` is the
    first lap's tyre life minus one (FastF1 counts the lap itself), null if that value is missing.
``race_control``
    Race-control messages with their lap number and parsed flags (SC / VSC deployed, red flag,
    penalty), the car mapped to ``driver_id``. TracingInsights often leaves ``dNum`` empty; the
    car is then taken from the text: the target of "... PENALTY FOR CAR n", else the only car
    named as "CAR n (XXX)". A message naming several cars keeps a null driver.
``race_control_track_status``
    The session track-status timeline (exact SC / VSC / red-flag periods) with the leader laps that
    overlap each period (``lap_from`` / ``lap_to``).
``telemetry_lap_real``
    The per-lap telemetry aggregates computed at ingest (``features.telemetry_agg_ti``),
    ``is_synthetic = false``.
``compound_coverage``
    Per race: lap rows, compound and tyre-life shares, ``below_95pct``, unmapped drivers,
    race-control and telemetry coverage counts (counts and shares only).

None of these feeds a v1 table: ``etl.clean`` (stints precedence FastF1 -> synthetic -> inferred,
the neutralisation flag) never reads them, and ``CLEAN_TABLES`` does not list them.

Driver mapping: TracingInsights code -> the v1 clean ``drivers.code`` of the same race, then the car
number. Unmapped drivers are counted in ``compound_coverage`` and left out of the driver-keyed
tables.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from f1sa.config import Settings, get_settings
from f1sa.etl.clean import (
    CleanError,
    check_output_root,
    conform,
    read_clean,
    read_parquet_table,
    spark_uri,
    table_checksum,
    write_text,
)
from f1sa.etl.spark import build_spark
from f1sa.lake import schemas_tracinginsights as TS
from f1sa.lake.paths import is_within

TABLES = tuple(TS.CLEAN_SCHEMAS_TI)
QUALITY_FILE = "tracinginsights.json"
RESULTS_NAME = "tracinginsights_coverage_full.csv"
_RACE = ["season", "round"]
_PENALTY = r"PENALTY FOR CAR"
_SC = r"(?<!VIRTUAL )SAFETY CAR DEPLOYED"
_VSC = r"VIRTUAL SAFETY CAR DEPLOYED"


def _glob(seasons: Sequence[int] | None) -> str:
    return "*" if not seasons else "{" + ",".join(str(s) for s in sorted(seasons)) + "}"


def read_raw(
    spark: SparkSession, raw_root: str, seasons: Sequence[int] | None
) -> dict[str, DataFrame]:
    base = f"{raw_root}/tracinginsights"
    g = _glob(seasons)
    return {
        "laps": read_parquet_table(
            spark, f"{base}/laps/season={g}/round=*/laps.parquet", TS.RAW_LAPS, "TI laps"
        ),
        "rcm": read_parquet_table(
            spark, f"{base}/rcm/season={g}/round=*/rcm.parquet", TS.RAW_RCM, "TI rcm"
        ),
        "track_status": read_parquet_table(
            spark,
            f"{base}/session_info/season={g}/round=*/track_status.parquet",
            TS.RAW_TRACK_STATUS,
            "TI track status",
        ),
        "telemetry_lap_agg": read_parquet_table(
            spark,
            f"{base}/telemetry_lap_agg/season={g}/round=*/telemetry_lap_agg.parquet",
            TS.RAW_TELEMETRY_LAP_AGG,
            "TI telemetry aggregates",
        ),
    }


# ============================================================================== mapping
def driver_map(laps: DataFrame, drivers: DataFrame) -> DataFrame:
    """(season, round, driver_code) -> driver_id: by code, else by car number; null if unmapped."""
    codes = laps.select(
        *_RACE, "driver_code", F.col("driver_number").cast("int").alias("number")
    ).distinct()
    by_code = drivers.where(F.col("code").isNotNull()).select(
        *_RACE, F.col("code").alias("driver_code"), F.col("driver_id").alias("_by_code")
    )
    by_number = drivers.where(F.col("car_number").isNotNull()).select(
        *_RACE, F.col("car_number").alias("number"), F.col("driver_id").alias("_by_number")
    )
    mapped = codes.join(by_code, [*_RACE, "driver_code"], "left").join(
        by_number, [*_RACE, "number"], "left"
    )
    out = mapped.groupBy(*_RACE, "driver_code").agg(
        F.first(F.coalesce("_by_code", "_by_number"), ignorenulls=True).alias("driver_id")
    )
    return out


def _known_compound(col: str) -> Any:
    return F.when(F.upper(F.col(col)).isin(list(TS.KNOWN_COMPOUNDS)), F.upper(F.col(col)))


# ============================================================================== builders
def build_stints(laps: DataFrame, mapping: DataFrame) -> DataFrame:
    rows = laps.where(F.col("stint").isNotNull()).withColumn("_c", _known_compound("compound"))
    grouped = rows.groupBy(*_RACE, "driver_code", "stint").agg(
        F.min("lap").alias("start_lap"),
        F.max("lap").alias("end_lap"),
        F.count(F.lit(1)).alias("laps"),
        F.array_sort(F.collect_set("_c")).alias("_set"),
        F.sum(F.col("_c").isNull().cast("int")).alias("n_laps_compound_null"),
        F.min_by("tyre_life", "lap").alias("_life_first"),
        F.min_by("fresh_tyre", "lap").alias("fresh_tyre"),
    )
    out = (
        grouped.join(mapping, [*_RACE, "driver_code"], "inner")
        .where(F.col("driver_id").isNotNull())
        .withColumns(
            {
                "compound": F.when(F.size("_set") == 1, F.col("_set")[0]),
                "compound_conflict": F.size("_set") > 1,
                "tyre_age_at_start": F.col("_life_first") - 1,
                "is_synthetic": F.lit(False),
                "source": F.lit(TS.SOURCE_VALUE),
            }
        )
        .withColumn("compound_missing", F.col("compound").isNull())
    )
    return conform(out, TS.STINTS_TRACINGINSIGHTS, "stints_tracinginsights")


def build_race_control(rcm: DataFrame, drivers: DataFrame) -> DataFrame:
    msg = F.upper(F.coalesce(F.col("message"), F.lit("")))
    numbers = drivers.where(F.col("car_number").isNotNull()).select(
        *_RACE, F.col("car_number").alias("driver_number"), "driver_id"
    )
    penalised = F.regexp_extract(msg, r"PENALTY FOR CAR (\d+)", 1)
    named = F.regexp_extract(msg, r"CAR (\d+) \(", 1)
    single = F.size(F.split(msg, r"CAR \d+ \(")) == 2
    from_text = F.when(penalised != "", penalised).when(single & (named != ""), named)
    df = (
        rcm.withColumn(
            "driver_number",
            F.coalesce(F.col("driver_number").cast("int"), from_text.cast("int")),
        )
        .join(numbers, [*_RACE, "driver_number"], "left")
        .withColumns(
            {
                "time_utc": F.to_timestamp(F.substring("time_utc", 1, 26)),
                "is_sc_deployed": msg.rlike(_SC),
                "is_vsc_deployed": msg.rlike(_VSC),
                "is_red_flag": F.upper(F.coalesce(F.col("flag"), F.lit(""))) == "RED",
                "is_penalty": msg.rlike(_PENALTY),
                "source": F.lit(TS.SOURCE_VALUE),
            }
        )
    )
    return conform(df, TS.RACE_CONTROL, "race_control")


def leader_windows(laps: DataFrame) -> DataFrame:
    """Session-time window of every lap of the car in position 1 at the end of that lap."""
    return laps.where(
        (F.col("position") == 1)
        & F.col("lap_start_session_s").isNotNull()
        & F.col("lap_end_session_s").isNotNull()
    ).select(
        *_RACE,
        F.col("lap").cast("int").alias("lap"),
        F.col("lap_start_session_s").alias("_ws"),
        F.col("lap_end_session_s").alias("_we"),
    )


def build_track_status(track: DataFrame, laps: DataFrame) -> DataFrame:
    w = Window.partitionBy(*_RACE).orderBy("seq")
    timeline = track.withColumns(
        {
            "start_session_s": F.col("session_time_s"),
            "end_session_s": F.lead("session_time_s").over(w),
        }
    )
    windows = leader_windows(laps)
    end = F.coalesce(F.col("end_session_s"), F.lit(float("inf")))
    overlap = (
        timeline.alias("t")
        .join(windows.alias("w"), _RACE, "left")
        .where(
            F.col("w.lap").isNull()
            | ((F.col("_ws") < end) & (F.col("_we") > F.col("start_session_s")))
        )
        .groupBy(*_RACE, "seq")
        .agg(F.min("w.lap").alias("lap_from"), F.max("w.lap").alias("lap_to"))
    )
    code = F.coalesce(F.col("status"), F.lit(""))
    df = timeline.join(overlap, [*_RACE, "seq"], "left").withColumns(
        {
            "is_green": code == "1",
            "is_yellow": code == "2",
            "is_sc": code == "4",
            "is_red_flag": code == "5",
            "is_vsc": code == "6",
            "is_vsc_ending": code == "7",
            "source": F.lit(TS.SOURCE_VALUE),
        }
    )
    return conform(df, TS.RACE_CONTROL_TRACK_STATUS, "race_control_track_status")


def build_telemetry_lap_real(agg: DataFrame, mapping: DataFrame) -> DataFrame:
    df = (
        agg.join(mapping, [*_RACE, "driver_code"], "inner")
        .where(F.col("driver_id").isNotNull())
        .withColumns({"is_synthetic": F.lit(False), "source": F.lit(TS.SOURCE_VALUE)})
    )
    return conform(df, TS.TELEMETRY_LAP_REAL, "telemetry_lap_real")


def build_coverage(
    laps: DataFrame, rcm: DataFrame, agg: DataFrame, mapping: DataFrame
) -> DataFrame:
    known = _known_compound("compound").isNotNull()
    lap_counts = laps.groupBy(*_RACE).agg(
        F.count(F.lit(1)).alias("n_lap_rows"),
        F.sum(known.cast("int")).alias("n_compound_known"),
        F.sum((F.col("compound").isNotNull() & ~known).cast("int")).alias(
            "n_compound_unknown_label"
        ),
        F.sum(F.col("compound").isNull().cast("int")).alias("n_compound_null"),
        F.sum(F.col("tyre_life").isNotNull().cast("int")).alias("n_tyre_life_known"),
        F.sum(F.col("stint").isNull().cast("int")).alias("n_stint_null"),
        F.countDistinct("driver_code").alias("n_drivers"),
    )
    unmapped = mapping.groupBy(*_RACE).agg(
        F.sum(F.col("driver_id").isNull().cast("int")).alias("n_drivers_unmapped")
    )
    rcm_counts = rcm.groupBy(*_RACE).agg(
        F.count(F.lit(1)).alias("n_rcm_messages"),
        F.sum(F.col("lap").isNotNull().cast("int")).alias("n_rcm_with_lap"),
    )
    tel_counts = agg.groupBy(*_RACE).agg(
        F.count(F.lit(1)).alias("n_telemetry_laps"),
        *[
            F.sum((F.col("telemetry_coverage") == v).cast("int")).alias(f"n_telemetry_{v}")
            for v in ("full", "partial", "none")
        ],
    )
    df = (
        lap_counts.join(unmapped, _RACE, "left")
        .join(rcm_counts, _RACE, "left")
        .join(tel_counts, _RACE, "left")
        .fillna(
            0,
            subset=[
                "n_drivers_unmapped",
                "n_rcm_messages",
                "n_rcm_with_lap",
                "n_telemetry_laps",
                "n_telemetry_full",
                "n_telemetry_partial",
                "n_telemetry_none",
            ],
        )
        .withColumns(
            {
                "compound_share": F.round(F.col("n_compound_known") / F.col("n_lap_rows"), 4),
                "tyre_life_share": F.round(F.col("n_tyre_life_known") / F.col("n_lap_rows"), 4),
            }
        )
        .withColumn("below_95pct", F.col("compound_share") < F.lit(TS.COVERAGE_THRESHOLD))
    )
    return conform(df, TS.COMPOUND_COVERAGE, "compound_coverage")


# ============================================================================== write / run
def write_ti_table(df: DataFrame, clean_root: str, table: str) -> None:
    """Overwrite exactly the (season, round) partitions present in ``df``."""
    (
        df.repartition(*_RACE)
        .sortWithinPartitions(*TS.PRIMARY_KEYS_TI[table])
        .write.mode("overwrite")
        .partitionBy(*_RACE)
        .parquet(f"{clean_root}/{table}")
    )


def read_ti_clean(spark: SparkSession, clean_root: str, table: str) -> DataFrame:
    schema = TS.CLEAN_SCHEMAS_TI[table]
    return spark.read.schema(schema).parquet(f"{clean_root}/{table}").select(schema.fieldNames())


def duplicate_keys(df: DataFrame, table: str) -> int:
    keys = list(TS.PRIMARY_KEYS_TI[table])
    return df.groupBy(*keys).count().where(F.col("count") > 1).count()


@dataclass
class CleanTiResult:
    counts: dict[str, int] = field(default_factory=dict)
    checksums: dict[str, str] = field(default_factory=dict)
    races: int = 0
    season_summary: list[dict[str, Any]] = field(default_factory=list)
    coverage_rows: list[dict[str, Any]] = field(default_factory=list)


def run_clean_ti(
    spark: SparkSession, settings: Settings, seasons: Sequence[int] | None = None
) -> CleanTiResult:
    lake = spark_uri(settings.data_lake_uri)
    raw_root, clean_root = f"{lake}/raw", f"{lake}/clean"
    check_output_root(clean_root)
    raw = read_raw(spark, raw_root, seasons)
    laps = raw["laps"].cache()
    if laps.isEmpty():
        raise CleanError(f"no TracingInsights laps under {raw_root}/tracinginsights/laps")
    races = laps.select(*_RACE).distinct()
    drivers = read_clean(spark, clean_root, "drivers").join(races, _RACE, "left_semi").cache()
    missing = races.join(drivers.select(*_RACE).distinct(), _RACE, "left_anti").count()
    if missing:
        raise CleanError(f"{missing} race(s) have no v1 clean drivers table; run the v1 clean job")
    mapping = driver_map(laps, drivers).cache()
    tables = {
        "stints_tracinginsights": build_stints(laps, mapping),
        "race_control": build_race_control(raw["rcm"], drivers),
        "race_control_track_status": build_track_status(raw["track_status"], laps),
        "telemetry_lap_real": build_telemetry_lap_real(raw["telemetry_lap_agg"], mapping),
        "compound_coverage": build_coverage(laps, raw["rcm"], raw["telemetry_lap_agg"], mapping),
    }
    result = CleanTiResult()
    for name, df in tables.items():
        dups = duplicate_keys(df, name)
        if dups:
            raise CleanError(f"{name}: {dups} duplicate primary keys")
        write_ti_table(df, clean_root, name)
        stored = read_ti_clean(spark, clean_root, name).join(races, _RACE, "left_semi")
        result.counts[name] = stored.count()
        result.checksums[name] = table_checksum(stored)
    result.races = races.count()
    cov = read_ti_clean(spark, clean_root, "compound_coverage").join(races, _RACE, "left_semi")
    result.coverage_rows = [r.asDict() for r in cov.orderBy(*_RACE).collect()]
    result.season_summary = season_summary(result.coverage_rows)
    report = {
        "seasons": sorted({int(r["season"]) for r in result.coverage_rows}),
        "races": result.races,
        "counts": result.counts,
        "checksums": result.checksums,
        "season_summary": result.season_summary,
        "races_below_95pct": [
            {"season": r["season"], "round": r["round"], "compound_share": r["compound_share"]}
            for r in result.coverage_rows
            if r["below_95pct"] and int(r["season"]) != 2025
        ],
        "races_below_95pct_2025_count": sum(
            1 for r in result.coverage_rows if r["below_95pct"] and int(r["season"]) == 2025
        ),
    }
    write_text(
        spark,
        f"{clean_root}/_quality/{QUALITY_FILE}",
        json.dumps(report, indent=1, sort_keys=True) + "\n",
    )
    laps.unpersist()
    drivers.unpersist()
    mapping.unpersist()
    return result


def season_summary(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by: dict[int, dict[str, Any]] = {}
    for r in rows:
        s = by.setdefault(
            int(r["season"]),
            {
                "season": int(r["season"]),
                "races": 0,
                "lap_rows": 0,
                "compound_known": 0,
                "tyre_life_known": 0,
                "races_below_95pct": 0,
                "drivers_unmapped": 0,
                "races_with_rcm": 0,
                "telemetry_laps": 0,
                "telemetry_full": 0,
                "telemetry_partial": 0,
                "telemetry_none": 0,
            },
        )
        s["races"] += 1
        s["lap_rows"] += r["n_lap_rows"]
        s["compound_known"] += r["n_compound_known"]
        s["tyre_life_known"] += r["n_tyre_life_known"]
        s["races_below_95pct"] += int(bool(r["below_95pct"]))
        s["drivers_unmapped"] += r["n_drivers_unmapped"]
        s["races_with_rcm"] += int(r["n_rcm_messages"] > 0)
        s["telemetry_laps"] += r["n_telemetry_laps"]
        for v in ("full", "partial", "none"):
            s[f"telemetry_{v}"] += r[f"n_telemetry_{v}"]
    out = []
    for s in sorted(by.values(), key=lambda x: x["season"]):
        s["compound_share"] = (
            round(s["compound_known"] / s["lap_rows"], 4) if s["lap_rows"] else None
        )
        s["tyre_life_share"] = (
            round(s["tyre_life_known"] / s["lap_rows"], 4) if s["lap_rows"] else None
        )
        out.append(s)
    return out


CSV_COLUMNS = tuple(TS.COMPOUND_COVERAGE.fieldNames())


def coverage_csv(rows: Iterable[dict[str, Any]]) -> str:
    """Per-race coverage (counts and shares only, also for 2025)."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(CSV_COLUMNS), lineterminator="\n")
    writer.writeheader()
    for r in rows:
        writer.writerow({c: r[c] for c in CSV_COLUMNS})
    return buf.getvalue()


def main(argv: Iterable[str] | None = None) -> int:
    from f1sa.ingest.cli import parse_seasons

    parser = argparse.ArgumentParser(prog="f1sa-clean-ti")
    parser.add_argument("--seasons", type=parse_seasons, default=None)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--results-dir", type=Path, default=Path("results"))
    parser.add_argument("--no-results", action="store_true", help="do not write the results CSV")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.no_results and not is_within(args.results_dir, Path("results")):
        print(
            f"error: --results-dir must be under ./results, got {args.results_dir}", file=sys.stderr
        )
        return 2
    settings = get_settings()
    lake = spark_uri(settings.data_lake_uri)
    local_dir = None
    if lake.startswith("file://"):
        local_dir = Path(lake.removeprefix("file://")) / ".spark-tmp"
    spark = build_spark("f1sa-clean-ti", args.master, local_dir=local_dir)
    try:
        result = run_clean_ti(spark, settings, args.seasons)
    except CleanError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    print(f"TracingInsights clean: {result.races} races")
    for name in TABLES:
        print(f"  {name}: {result.counts[name]} rows, checksum {result.checksums[name]}")
    for s in result.season_summary:
        print(
            f"  {s['season']}: {s['races']} races, compound {s['compound_share']}, tyre life "
            f"{s['tyre_life_share']}, below 95% {s['races_below_95pct']}, unmapped drivers "
            f"{s['drivers_unmapped']}, races with rcm {s['races_with_rcm']}, telemetry laps "
            f"{s['telemetry_laps']} (full {s['telemetry_full']}, partial {s['telemetry_partial']}, "
            f"none {s['telemetry_none']})"
        )
    if not args.no_results:
        args.results_dir.mkdir(parents=True, exist_ok=True)
        target = args.results_dir / RESULTS_NAME
        target.write_text(coverage_csv(result.coverage_rows), encoding="utf-8")
        print(f"  wrote {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
