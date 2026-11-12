"""Read-only lap reconciliation: TracingInsights ``telemetry_lap_real`` vs Jolpica clean ``laps``.

Reports counts only (never lap times, positions, labels or model outcomes) and writes nothing:
no sink, no ``write``, no results file. Run in the app container against HDFS with
``make reconcile-ti-full``.

Checks
* Key uniqueness of the TracingInsights clean tables on their primary keys
  (``PRIMARY_KEYS_TI``), plus lap-level uniqueness of ``stints_tracinginsights``: every
  (season, round, driver_id, lap) inside ``start_lap..end_lap`` belongs to at most one stint.
* Left join Jolpica laps -> ``telemetry_lap_real`` on (season, round, driver_id, lap): the
  row count must equal the Jolpica row count (no fan-out).
* Rows on one side only, classified by the first matching cause (``CAUSES``). The cause rules
  are checked against the data, so each bucket count is exact; *why* a source has the row
  (e.g. TracingInsights keeping a retiring car's incomplete lap) is an interpretation, and the
  ``n_lap_incomplete`` / ``n_lap_is_results_laps_plus_1`` cross-tabs are the supporting counts.
* Races below the compound-coverage threshold (names and shares only).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

from f1sa.config import Settings, get_settings
from f1sa.etl.clean import read_clean, read_parquet_table, spark_uri
from f1sa.etl.clean_tracinginsights import read_raw, read_ti_clean
from f1sa.etl.spark import build_spark
from f1sa.lake import schemas_tracinginsights as TS

KEY = ("season", "round", "driver_id", "lap")
_RACE = ["season", "round"]
_DRIVER = ["season", "round", "driver_id"]
SIDES = ("ti_only", "jolpica_only")
# Priority order: a row gets the first cause that applies.
CAUSES = (
    "lap_le_0",  # lap < 1: lap 0 / formation lap
    "duplicate_key",  # extra copies of a key on this side
    "driver_absent_other",  # the other source has no lap of this driver in this race
    "after_last_lap_other",  # lap > the driver's last lap in the other source
    "before_first_lap_other",  # lap < the driver's first lap in the other source
    "gap_inside_range",  # inside the other source's lap range, but that lap is missing there
)
# Jolpica results status classes (used to split the driver-level causes).
STATUS_CLASSES = (
    "finished",
    "lapped",
    "retired",
    "no_result",
)  # "no_result": no Jolpica results row
_LAPPED = r"^(\+\d+ Laps?|Lapped)$"
# The clean job's RAW_LAPS subset plus the lap time (null = the lap was not completed).
RAW_LAPS_WITH_TIME = StructType(
    [*TS.RAW_LAPS.fields, StructField("lap_time_s", DoubleType(), True)]
)
_FLAGS = ("_incomplete", "_tel_not_full")


class ReconcileError(RuntimeError):
    """An input table needed for the reconciliation is missing or empty."""


# ============================================================================== checks
def key_duplicates(df: DataFrame, keys: Sequence[str]) -> int:
    """Number of distinct keys that occur more than once."""
    return df.groupBy(*keys).count().where(F.col("count") > 1).count()


def stint_lap_overlaps(stints: DataFrame) -> int:
    """Number of (season, round, driver_id, lap) covered by more than one stint."""
    laps = stints.where(F.col("start_lap").isNotNull() & F.col("end_lap").isNotNull()).select(
        *_DRIVER, F.explode(F.sequence("start_lap", "end_lap")).alias("lap")
    )
    return key_duplicates(laps, KEY)


def _season_counts(df: DataFrame, name: str) -> dict[int, int]:
    return {
        int(r["season"]): int(r[name])
        for r in df.groupBy("season").agg(F.count(F.lit(1)).alias(name)).collect()
    }


def left_join_fanout(jolpica_laps: DataFrame, tel: DataFrame) -> list[dict[str, Any]]:
    """Per season: Jolpica rows, rows after a left join to ``tel`` on ``KEY``, matched rows."""
    right = tel.select(*KEY, F.lit(True).alias("_m"))
    joined = jolpica_laps.select(*KEY).join(right, list(KEY), "left")
    left_n = _season_counts(jolpica_laps, "n")
    after = {
        int(r["season"]): (int(r["n"]), int(r["m"]))
        for r in joined.groupBy("season")
        .agg(F.count(F.lit(1)).alias("n"), F.count("_m").alias("m"))
        .collect()
    }
    out = []
    for season in sorted(left_n):
        n_after, n_matched = after.get(season, (0, 0))
        out.append(
            {
                "season": season,
                "n_jolpica": left_n[season],
                "n_after_left_join": n_after,
                "n_matched": n_matched,
                "fanout_ok": n_after == left_n[season],
            }
        )
    return out


def _status_class(results: DataFrame) -> DataFrame:
    status = F.coalesce(F.col("status"), F.lit(""))
    cls = (
        F.when(status == "Finished", "finished")
        .when(status.rlike(_LAPPED), "lapped")
        .otherwise("retired")
    )
    return results.select(*_DRIVER, F.col("laps").alias("_results_laps"), cls.alias("_status"))


def _one_side(
    side: str,
    rows: DataFrame,
    other: DataFrame,
    status: DataFrame,
    flags: DataFrame | None,
) -> DataFrame:
    """Classified rows of ``rows`` without a key match in ``other``, plus duplicate extras."""
    w = Window.partitionBy(*KEY).orderBy(F.lit(1))
    numbered = rows.select(*KEY).withColumn("_rn", F.row_number().over(w))
    other_keys = other.select(*KEY).distinct()
    ranges = other.groupBy(*_DRIVER).agg(F.min("lap").alias("_min"), F.max("lap").alias("_max"))
    dup = numbered.where(F.col("_rn") > 1).drop("_rn")
    only = numbered.where(F.col("_rn") == 1).drop("_rn").join(other_keys, list(KEY), "left_anti")
    only = only.join(ranges, _DRIVER, "left")
    cause: Column = (
        F.when(F.col("lap") < 1, "lap_le_0")
        .when(F.col("_min").isNull(), "driver_absent_other")
        .when(F.col("lap") > F.col("_max"), "after_last_lap_other")
        .when(F.col("lap") < F.col("_min"), "before_first_lap_other")
        .otherwise("gap_inside_range")
    )
    classified = only.withColumn("cause", cause).drop("_min", "_max")
    dup_cause = F.when(F.col("lap") < 1, "lap_le_0").otherwise("duplicate_key")
    both = classified.unionByName(dup.withColumn("cause", dup_cause))
    both = both.join(status, _DRIVER, "left").withColumn(
        "_status", F.coalesce(F.col("_status"), F.lit("no_result"))
    )
    if flags is not None:
        both = both.join(flags.select(*KEY, *_FLAGS), list(KEY), "left")
    else:
        both = both.withColumns({f: F.lit(None).cast("boolean") for f in _FLAGS})
    return both.withColumn("side", F.lit(side))


def lap_differences(
    jolpica_laps: DataFrame,
    tel: DataFrame,
    results: DataFrame,
    ti_flags: DataFrame | None = None,
) -> list[dict[str, Any]]:
    """Rows on one side only, by (season, side, cause, status class), with cross-tab counts.

    ``ti_flags``: optional (season, round, driver_id, lap, _incomplete, _tel_not_full) for
    TracingInsights rows: ``_incomplete`` = the raw TI lap has no lap time (not completed),
    ``_tel_not_full`` = ``telemetry_coverage`` is not ``full``.
    """
    status = _status_class(results)
    parts = _one_side("ti_only", tel, jolpica_laps, status, ti_flags).unionByName(
        _one_side("jolpica_only", jolpica_laps, tel, status, None)
    )
    grouped = parts.groupBy("season", "side", "cause", "_status").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.coalesce(F.col("_incomplete"), F.lit(False)).cast("int")).alias("n_lap_incomplete"),
        F.sum(F.coalesce(F.col("_tel_not_full"), F.lit(False)).cast("int")).alias(
            "n_telemetry_not_full"
        ),
        F.sum((F.col("lap") == F.col("_results_laps") + 1).cast("int")).alias(
            "n_lap_is_results_laps_plus_1"
        ),
        F.sum((F.col("_results_laps") == 0).cast("int")).alias("n_results_laps_0"),
    )
    order = {c: i for i, c in enumerate(CAUSES)}
    rows = [
        {
            "season": int(r["season"]),
            "side": r["side"],
            "cause": r["cause"],
            "status": r["_status"],
            "n": int(r["n"]),
            "n_lap_incomplete": int(r["n_lap_incomplete"] or 0),
            "n_telemetry_not_full": int(r["n_telemetry_not_full"] or 0),
            "n_lap_is_results_laps_plus_1": int(r["n_lap_is_results_laps_plus_1"] or 0),
            "n_results_laps_0": int(r["n_results_laps_0"] or 0),
        }
        for r in grouped.collect()
    ]
    return sorted(
        rows,
        key=lambda r: (r["season"], SIDES.index(r["side"]), order[r["cause"]], r["status"]),
    )


def coverage_below(
    coverage: DataFrame, races: DataFrame, threshold: float = TS.COVERAGE_THRESHOLD
) -> list[dict[str, Any]]:
    """Races with compound share strictly below ``threshold``: season, round, name, share."""
    df = (
        coverage.where(F.col("compound_share") < F.lit(threshold))
        .select(*_RACE, "compound_share")
        .join(races.select(*_RACE, "race_name"), _RACE, "left")
        .orderBy(*_RACE)
    )
    return [
        {
            "season": int(r["season"]),
            "round": int(r["round"]),
            "race_name": r["race_name"],
            "compound_share": float(r["compound_share"]),
        }
        for r in df.collect()
    ]


# ============================================================================== run
@dataclass
class ReconcileReport:
    seasons: list[dict[str, Any]] = field(default_factory=list)
    duplicates: dict[str, int] = field(default_factory=dict)
    stint_lap_overlaps: int = 0
    fanout: list[dict[str, Any]] = field(default_factory=list)
    differences: list[dict[str, Any]] = field(default_factory=list)
    below_threshold: list[dict[str, Any]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            all(n == 0 for n in self.duplicates.values())
            and self.stint_lap_overlaps == 0
            and all(f["fanout_ok"] for f in self.fanout)
        )


def _filter(df: DataFrame, seasons: Sequence[int] | None) -> DataFrame:
    return df if not seasons else df.where(F.col("season").isin(list(seasons)))


def season_counts(
    jolpica_laps: DataFrame, tel: DataFrame, ti_raw_laps: DataFrame, ti_raw_agg: DataFrame
) -> list[dict[str, Any]]:
    """Per season row counts of the four lap-level inputs."""
    counts = {
        name: _season_counts(df, name)
        for name, df in (
            ("jolpica_laps", jolpica_laps),
            ("telemetry_lap_real", tel),
            ("ti_raw_laps", ti_raw_laps),
            ("ti_raw_telemetry_agg", ti_raw_agg),
        )
    }
    seasons = sorted(set().union(*(c.keys() for c in counts.values())))
    return [{"season": s, **{k: v.get(s, 0) for k, v in counts.items()}} for s in seasons]


def read_raw_lap_times(
    spark: SparkSession, raw_root: str, seasons: Sequence[int] | None
) -> DataFrame:
    """Raw TracingInsights laps (``RAW_LAPS_WITH_TIME``), read only; empty if none exist."""
    g = "*" if not seasons else "{" + ",".join(str(s) for s in sorted(seasons)) + "}"
    pattern = f"{raw_root}/tracinginsights/laps/season={g}/round=*/laps.parquet"
    return read_parquet_table(spark, pattern, RAW_LAPS_WITH_TIME, "TI laps")


def run_reconcile(
    spark: SparkSession, settings: Settings, seasons: Sequence[int] | None = None
) -> ReconcileReport:
    lake = spark_uri(settings.data_lake_uri)
    clean_root = f"{lake}/clean"
    jol = _filter(read_clean(spark, clean_root, "laps"), seasons).select(*KEY).cache()
    if jol.isEmpty():
        raise ReconcileError(f"no v1 clean laps under {clean_root}/laps")
    results = _filter(read_clean(spark, clean_root, "results"), seasons)
    races = _filter(read_clean(spark, clean_root, "races"), seasons)
    ti = {t: _filter(read_ti_clean(spark, clean_root, t), seasons) for t in TS.CLEAN_SCHEMAS_TI}
    tel = ti["telemetry_lap_real"].select(*KEY, "driver_code", "telemetry_coverage").cache()
    if tel.isEmpty():
        raise ReconcileError(f"no telemetry_lap_real under {clean_root}")
    raw = read_raw(spark, f"{lake}/raw", seasons)
    raw_laps = read_raw_lap_times(spark, f"{lake}/raw", seasons)
    flags = (
        raw_laps.join(tel, [*_RACE, "driver_code", "lap"], "inner")
        .groupBy(*KEY)
        .agg(
            F.max(F.col("lap_time_s").isNull()).alias("_incomplete"),
            F.max(F.col("telemetry_coverage") != "full").alias("_tel_not_full"),
        )
    )
    report = ReconcileReport(
        seasons=season_counts(jol, tel, raw_laps, raw["telemetry_lap_agg"]),
        duplicates={t: key_duplicates(ti[t], TS.PRIMARY_KEYS_TI[t]) for t in TS.CLEAN_SCHEMAS_TI},
        stint_lap_overlaps=stint_lap_overlaps(ti["stints_tracinginsights"]),
        fanout=left_join_fanout(jol, tel),
        differences=lap_differences(jol, tel, results, flags),
        below_threshold=coverage_below(ti["compound_coverage"], races),
    )
    report.duplicates["telemetry_lap_real (with driver_code)"] = key_duplicates(
        tel, [*KEY, "driver_code"]
    )
    jol.unpersist()
    tel.unpersist()
    return report


# ============================================================================== output
def format_table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> str:
    """Fixed-width text table; ``(none)`` when there are no rows."""
    if not rows:
        return "  (none)"
    cells = [[str(r.get(c, "")) for c in columns] for r in rows]
    widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(columns)]
    lines = ["  " + "  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True))]
    lines += [
        "  " + "  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)) for row in cells
    ]
    return "\n".join(lines)


def difference_totals(differences: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Per (season, side): total rows on that side only."""
    acc: dict[tuple[int, str], int] = {}
    for d in differences:
        k = (int(d["season"]), str(d["side"]))
        acc[k] = acc.get(k, 0) + int(d["n"])
    return [
        {"season": s, "side": side, "n": n}
        for (s, side), n in sorted(acc.items(), key=lambda kv: (kv[0][0], SIDES.index(kv[0][1])))
    ]


def render(report: ReconcileReport) -> str:
    out = ["== per-season row counts"]
    out.append(
        format_table(
            report.seasons,
            ["season", "jolpica_laps", "telemetry_lap_real", "ti_raw_laps", "ti_raw_telemetry_agg"],
        )
    )
    out.append("== key uniqueness (distinct keys occurring more than once)")
    dup_rows = [
        {"table": t, "keys": ",".join(TS.PRIMARY_KEYS_TI.get(t, ())), "n_dup_keys": n}
        for t, n in report.duplicates.items()
    ]
    dup_rows.append(
        {
            "table": "stints_tracinginsights (lap level)",
            "keys": ",".join(KEY),
            "n_dup_keys": report.stint_lap_overlaps,
        }
    )
    out.append(format_table(dup_rows, ["table", "keys", "n_dup_keys"]))
    out.append("== left join jolpica laps -> telemetry_lap_real")
    out.append(
        format_table(
            report.fanout, ["season", "n_jolpica", "n_after_left_join", "n_matched", "fanout_ok"]
        )
    )
    out.append("== rows on one side only (totals)")
    out.append(format_table(difference_totals(report.differences), ["season", "side", "n"]))
    out.append("== rows on one side only, by cause and Jolpica result status class")
    out.append(
        format_table(
            report.differences,
            [
                "season",
                "side",
                "cause",
                "status",
                "n",
                "n_lap_incomplete",
                "n_telemetry_not_full",
                "n_lap_is_results_laps_plus_1",
                "n_results_laps_0",
            ],
        )
    )
    out.append(f"== races below {TS.COVERAGE_THRESHOLD:.0%} compound coverage")
    out.append(
        format_table(report.below_threshold, ["season", "round", "race_name", "compound_share"])
    )
    out.append(f"== result: {'PASS' if report.ok else 'FAIL'}")
    return "\n".join(out)


def main(argv: Iterable[str] | None = None, spark: SparkSession | None = None) -> int:
    """Exit 0 when every check passes, 1 on a duplicate key or fan-out, 2 on missing input."""
    from f1sa.ingest.cli import parse_seasons

    parser = argparse.ArgumentParser(prog="f1sa-reconcile-ti")
    parser.add_argument("--seasons", type=parse_seasons, default=None)
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args(list(argv) if argv is not None else None)
    owned = spark is None
    session = spark or build_spark("f1sa-reconcile-ti", args.master)
    try:
        report = run_reconcile(session, get_settings(), args.seasons)
    except ReconcileError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    finally:
        if owned:
            session.stop()
    print(render(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
