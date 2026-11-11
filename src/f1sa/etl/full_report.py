"""Full-data quality report: what the 2021-2025 clean and feature zones actually contain.

    python -m f1sa.etl.full_report          (``make report-full``, inside the compose network)

Reads ``{DATA_LAKE_URI}/clean`` and ``/features`` and writes nothing to the lake. It writes
``results/full_data_quality.json``, ``results/constructors_full.csv`` and
``results/neutralisation_by_race_full.csv``. Every output is deterministic, so reruns are
byte-identical.

The job exits 1 on a hard quality failure, an unknown constructor id, a provenance failure or a
failed ``real`` feature-set check. Unexpected race counts are reported but do not fail it.

Label handling: only attempt and labelled **counts** per season and outcome category are read from
``undercut_counts_full.csv``. Success rates are never read here, the test split stays sealed and no
model is fitted.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.config import Settings, get_settings
from f1sa.etl.clean import CLEAN_TABLES, CleanError, read_clean, spark_uri
from f1sa.etl.spark import build_spark, configure_session
from f1sa.features import schemas as FS
from f1sa.features.build import FULL_COUNTS_FILE, BuildError, check_results_dir, read_feature
from f1sa.features.lineage import LINEAGE, STABLE_CONSTRUCTORS
from f1sa.features.undercut import OUTCOME_CATEGORIES
from f1sa.features.undercut_rule import DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS
from f1sa.model import features as MF
from f1sa.model.dataset import SplitSpec, build_dataset, load_attempts
from f1sa.model.train import check_feature_policy

EXPECTED_RACES_PER_SEASON = (22, 24)
EXPECTED_DRIVERS_PER_RACE = 20
MIN_LABELLED_PER_SEASON = 50
REPORT_FILE = "full_data_quality.json"
CONSTRUCTORS_FILE = "constructors_full.csv"
NEUTRALISATION_FILE = "neutralisation_by_race_full.csv"
_RACE = ["season", "round"]


class ReportError(RuntimeError):
    """The report cannot run (missing input)."""


@dataclass(frozen=True)
class ProvenanceCheck:
    """One assertion about synthetic or missing data; ``count`` offending rows, 0 to pass."""

    name: str
    count: int
    detail: str

    @property
    def passed(self) -> bool:
        return self.count == 0


# ============================================================================== counts
def season_summary(tables: Mapping[str, DataFrame]) -> list[dict[str, int]]:
    """Per season: distinct races (from ``results``) and the row count of every table."""
    races = tables["results"].select(*_RACE).distinct().groupBy("season").count().collect()
    out: dict[int, dict[str, int]] = {
        int(r["season"]): {"season": int(r["season"]), "races": int(r["count"])} for r in races
    }
    for name, df in tables.items():
        counts = {int(r["season"]): int(r["count"]) for r in df.groupBy("season").count().collect()}
        for season, row in out.items():
            row[name] = counts.get(season, 0)
    return [out[s] for s in sorted(out)]


def race_summary(results: DataFrame, laps: DataFrame, pitstops: DataFrame) -> list[dict[str, int]]:
    """Per race: drivers classified, drivers with laps, race laps, lap rows and pit stops."""
    res = results.groupBy(*_RACE).agg(F.countDistinct("driver_id").alias("drivers"))
    lap = laps.groupBy(*_RACE).agg(
        F.countDistinct("driver_id").alias("drivers_with_laps"),
        F.max("lap").alias("race_laps"),
        F.count(F.lit(1)).alias("lap_rows"),
    )
    pit = pitstops.groupBy(*_RACE).agg(F.count(F.lit(1)).alias("pit_stops"))
    joined = res.join(lap, _RACE, "left").join(pit, _RACE, "left")
    keys = ("drivers", "drivers_with_laps", "race_laps", "lap_rows", "pit_stops")
    rows = [
        {
            "season": int(r["season"]),
            "round": int(r["round"]),
            **{k: int(r[k] or 0) for k in keys},
        }
        for r in joined.collect()
    ]
    return sorted(rows, key=lambda r: (r["season"], r["round"]))


def unexpected_seasons(
    seasons: Sequence[Mapping[str, int]],
    expected: tuple[int, int] = EXPECTED_RACES_PER_SEASON,
) -> list[dict[str, int]]:
    """Seasons whose race count is outside ``expected`` (inclusive)."""
    low, high = expected
    return [
        {"season": s["season"], "races": s["races"]}
        for s in seasons
        if not low <= s["races"] <= high
    ]


def unexpected_races(
    races: Sequence[Mapping[str, int]], expected_drivers: int = EXPECTED_DRIVERS_PER_RACE
) -> list[dict[str, Any]]:
    """Races with an unexpected driver count, drivers without laps, or no pit stops."""
    out: list[dict[str, Any]] = []
    for r in races:
        reasons = []
        if r["drivers"] != expected_drivers:
            reasons.append(f"{r['drivers']} drivers in results")
        if r["drivers_with_laps"] != r["drivers"]:
            reasons.append(f"{r['drivers_with_laps']} of {r['drivers']} drivers have laps")
        if r["pit_stops"] == 0:
            reasons.append("no pit stops")
        if reasons:
            out.append({**r, "reasons": reasons})
    return out


# ============================================================================== constructors
def constructor_status(constructor_id: str) -> str:
    """``lineage`` (renamed later), ``stable`` (current id) or ``unknown`` (fails the report)."""
    if constructor_id in LINEAGE:
        return "lineage"
    return "stable" if constructor_id in STABLE_CONSTRUCTORS else "unknown"


def constructors_by_season(results: DataFrame) -> list[dict[str, Any]]:
    """Every distinct (season, constructor_id) in ``results`` with its lineage status."""
    rows = results.select("season", "constructor_id").distinct().collect()
    pairs = sorted((int(r["season"]), str(r["constructor_id"])) for r in rows)
    return [{"season": s, "constructor_id": c, "status": constructor_status(c)} for s, c in pairs]


# ============================================================================== neutralisation
def neutralisation_by_race(laps: DataFrame) -> list[dict[str, Any]]:
    """Per race: race laps, laps flagged neutralised or red flag, and the neutralised share."""
    per_lap = laps.groupBy(*_RACE, "lap").agg(
        F.max(F.col("is_neutralised_lap").cast("int")).alias("neutralised"),
        F.max(F.col("is_red_flag_lap").cast("int")).alias("red_flag"),
    )
    rows = (
        per_lap.groupBy(*_RACE)
        .agg(
            F.count(F.lit(1)).alias("race_laps"),
            F.sum(F.coalesce("neutralised", F.lit(0))).alias("neutralised_laps"),
            F.sum(F.coalesce("red_flag", F.lit(0))).alias("red_flag_laps"),
        )
        .collect()
    )
    out = [
        {
            "season": int(r["season"]),
            "round": int(r["round"]),
            "race_laps": int(r["race_laps"]),
            "neutralised_laps": int(r["neutralised_laps"]),
            "red_flag_laps": int(r["red_flag_laps"]),
            "neutralised_share": round(int(r["neutralised_laps"]) / int(r["race_laps"]), 4),
        }
        for r in rows
    ]
    return sorted(out, key=lambda r: (r["season"], r["round"]))


def neutralisation_outliers(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Tukey fences (Q1 - 1.5 IQR, Q3 + 1.5 IQR) on the share, plus races with a zero share.

    Descriptive only: nothing here feeds the heuristic's thresholds.
    """
    shares = [float(r["neutralised_share"]) for r in rows]
    if len(shares) < 2:
        return {"median": shares[0] if shares else None, "high": [], "low": [], "zero": []}
    q1, q2, q3 = statistics.quantiles(shares, n=4, method="inclusive")
    iqr = q3 - q1
    low_fence, high_fence = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    return {
        "q1": round(q1, 4),
        "median": round(q2, 4),
        "q3": round(q3, 4),
        "low_fence": round(low_fence, 4),
        "high_fence": round(high_fence, 4),
        "high": [dict(r) for r in rows if float(r["neutralised_share"]) > high_fence],
        "low": [dict(r) for r in rows if float(r["neutralised_share"]) < low_fence],
        "zero": [dict(r) for r in rows if float(r["neutralised_share"]) == 0.0],
    }


# ============================================================================== provenance
def check_provenance(
    stints: DataFrame,
    laps_enriched: DataFrame,
    telemetry_lap: DataFrame,
    telemetry_features: DataFrame,
    attempts: DataFrame,
) -> list[ProvenanceCheck]:
    """Full mode without FastF1: nothing synthetic, compound null, telemetry absent, age flagged."""
    covered = laps_enriched.where(F.col("tyre_age_laps").isNotNull())
    return [
        ProvenanceCheck(
            "stints_all_inferred",
            stints.where(~F.coalesce(F.col("source") == "jolpica_inferred", F.lit(False))).count(),
            "stints not from jolpica_inferred",
        ),
        ProvenanceCheck(
            "stints_not_synthetic",
            stints.where(F.coalesce(F.col("is_synthetic"), F.lit(True))).count(),
            "stints flagged is_synthetic (or null)",
        ),
        ProvenanceCheck(
            "stint_compound_null",
            stints.where(F.col("compound").isNotNull()).count(),
            "stints with a compound",
        ),
        ProvenanceCheck(
            "lap_compound_null",
            laps_enriched.where(F.col("compound").isNotNull()).count(),
            "enriched laps with a compound",
        ),
        ProvenanceCheck(
            "tyre_age_flagged_assumed_new",
            covered.where(~F.coalesce(F.col("tyre_age_assumed_new"), F.lit(False))).count(),
            "laps with a tyre age not flagged tyre_age_assumed_new",
        ),
        ProvenanceCheck(
            "clean_telemetry_absent", telemetry_lap.count(), "rows in clean telemetry_lap"
        ),
        ProvenanceCheck(
            "feature_telemetry_absent",
            telemetry_features.count(),
            "rows in telemetry_lap_features",
        ),
        ProvenanceCheck(
            "attempt_compound_null",
            attempts.where(
                F.col("attacker_compound").isNotNull() | F.col("defender_compound").isNotNull()
            ).count(),
            "attempts with a compound",
        ),
        ProvenanceCheck(
            "attempts_not_synthetic",
            attempts.where(F.coalesce(F.col("compound_is_synthetic"), F.lit(True))).count(),
            "attempts flagged compound_is_synthetic (or null)",
        ),
    ]


def real_feature_set_violations(feature_set: str = "real") -> list[str]:
    """Inputs of ``feature_set`` whose provenance can be synthetic (must be empty for ``real``)."""
    return [s.name for s in MF.specs_for(feature_set) if s.provenance in MF.SYNTHETIC_FLAGS]


def check_real_feature_set(
    spark: SparkSession, features_root: str, *, g: float, k: int
) -> dict[str, Any]:
    """Build the Step 9 ``real`` dataset from the stored features. No fit; test stays sealed.

    Returns only row and race counts. Class balance is deliberately not returned.
    """
    attempts = load_attempts(spark, features_root).cache()
    flags = [c for flags in MF.SYNTHETIC_FLAGS.values() for c in flags]
    flagged = {
        flag: attempts.where(F.coalesce(F.col(flag), F.lit(False))).count() for flag in flags
    }
    check_feature_policy(attempts, "real", "full")
    dataset = build_dataset(attempts, g=g, k=k, feature_set="real")
    violations = real_feature_set_violations("real")
    result = {
        "feature_set": dataset.feature_set,
        "assembled_columns": len(dataset.columns),
        "synthetic_capable_inputs": violations,
        "rows_flagged": flagged,
        "train": {k_: dataset.counts["train"][k_] for k_ in ("n", "races")},
        "validation": {k_: dataset.counts["validation"][k_] for k_ in ("n", "races")},
        "test_opened": dataset.test.opened,
        "passed": not violations and not any(flagged.values()) and dataset.test.opened == 0,
    }
    dataset.train.unpersist()
    dataset.validation.unpersist()
    attempts.unpersist()
    return result


# ============================================================================== undercut counts
def read_counts_csv(path: Path) -> list[dict[str, str]]:
    """The rows of a counts CSV, keeping only count columns (success rates are never read)."""
    if not path.is_file():
        raise ReportError(f"{path} not found; run `make features-full` first")
    keep = (
        "season",
        "gap_threshold_s",
        "response_window_laps",
        "candidates",
        "labelled",
        *(f"n_{c}" for c in OUTCOME_CATEGORIES),
    )
    with path.open(newline="", encoding="utf-8") as fh:
        return [{c: row[c] for c in keep} for row in csv.DictReader(fh)]


def rows_at(rows: Sequence[Mapping[str, str]], g: float, k: int) -> list[Mapping[str, str]]:
    """Rows of one (G, k) setting."""
    return [
        r for r in rows if float(r["gap_threshold_s"]) == g and int(r["response_window_laps"]) == k
    ]


def split_counts(
    rows: Sequence[Mapping[str, str]], g: float, k: int, split: SplitSpec | None = None
) -> dict[str, int]:
    """Labelled attempts at (G, k) for train, validation and test."""
    spec = split or SplitSpec()
    at = rows_at(rows, g, k)

    def total(seasons: Iterable[int]) -> int:
        wanted = set(seasons)
        return sum(int(r["labelled"]) for r in at if int(r["season"]) in wanted)

    return {
        "train": total(spec.train),
        "validation": total(spec.validation),
        "test": total(spec.test),
    }


def seasons_below(
    rows: Sequence[Mapping[str, str]],
    g: float,
    k: int,
    seasons: Iterable[int],
    minimum: int = MIN_LABELLED_PER_SEASON,
) -> list[int]:
    """``seasons`` with fewer than ``minimum`` labelled attempts at (G, k); a missing one is 0."""
    labelled = {int(r["season"]): int(r["labelled"]) for r in rows_at(rows, g, k)}
    return sorted(s for s in seasons if labelled.get(s, 0) < minimum)


# ============================================================================== output
def read_text(spark: SparkSession, uri: str) -> str:
    """A small text file from file:// or hdfs:// (whole-file read; raises if it does not exist)."""
    return "\n".join(r["value"] for r in spark.read.text(uri, wholetext=True).collect())


def write_csv(rows: Sequence[Mapping[str, Any]], columns: Sequence[str], path: Path) -> None:
    """Write ``rows`` atomically with a fixed column order and ``\\n`` line endings."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns), lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in columns})
    tmp.replace(path)


def report_ok(report: Mapping[str, Any]) -> bool:
    """True when nothing in ``report`` is a failure (unexpected counts are not failures)."""
    return (
        report["quality"]["hard_failures"] == 0
        and not [c for c in report["constructors"] if c["status"] == "unknown"]
        and all(c["count"] == 0 for c in report["provenance"])
        and bool(report["real_feature_set"]["passed"])
    )


def run_report(
    spark: SparkSession, settings: Settings, *, results_dir: Path = Path("results")
) -> dict[str, Any]:
    """Build the report and write the three output files."""
    check_results_dir(results_dir)
    configure_session(spark)
    lake = spark_uri(settings.data_lake_uri)
    clean_root, features_root = f"{lake}/clean", f"{lake}/features"
    clean = {name: read_clean(spark, clean_root, name).cache() for name in CLEAN_TABLES}
    if clean["results"].isEmpty():
        raise ReportError(f"clean zone at {clean_root} is empty; run `make etl-full` first")
    features = {name: read_feature(spark, features_root, name) for name in FS.FEATURE_TABLES}
    quality = json.loads(read_text(spark, f"{clean_root}/_quality/report.json"))["quality"]

    seasons = season_summary(clean)
    races = race_summary(clean["results"], clean["laps"], clean["pitstops"])
    neutralisation = neutralisation_by_race(clean["laps"])
    constructors = constructors_by_season(clean["results"])
    provenance = check_provenance(
        clean["stints"],
        features["laps_enriched"],
        clean["telemetry_lap"],
        features["telemetry_lap_features"],
        features["undercut_attempts"],
    )
    real = check_real_feature_set(spark, features_root, g=DEFAULT_GAP_S, k=DEFAULT_WINDOW_LAPS)
    counts = read_counts_csv(results_dir / FULL_COUNTS_FILE)
    season_ids = [s["season"] for s in seasons]
    report: dict[str, Any] = {
        "quality": quality,
        "seasons": seasons,
        "feature_rows_by_season": season_summary({"results": clean["results"], **features}),
        "unexpected_seasons": unexpected_seasons(seasons),
        "races": races,
        "unexpected_races": unexpected_races(races),
        "constructors": constructors,
        "provenance": [{**asdict(c), "passed": c.passed} for c in provenance],
        "real_feature_set": real,
        "neutralisation": neutralisation_outliers(neutralisation),
        "default_setting": {"g": DEFAULT_GAP_S, "k": DEFAULT_WINDOW_LAPS},
        "labelled_by_split": split_counts(counts, DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS),
        "seasons_below_minimum": seasons_below(
            counts, DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS, season_ids
        ),
        "counts_by_season_at_default": [
            dict(r) for r in rows_at(counts, DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS)
        ],
    }
    path = (results_dir / REPORT_FILE).resolve()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)
    write_csv(constructors, ("season", "constructor_id", "status"), results_dir / CONSTRUCTORS_FILE)
    write_csv(
        neutralisation,
        ("season", "round", "race_laps", "neutralised_laps", "red_flag_laps", "neutralised_share"),
        results_dir / NEUTRALISATION_FILE,
    )
    for df in clean.values():
        df.unpersist()
    return report


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m f1sa.etl.full_report")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--results-dir", type=Path, default=Path("results"))
    args = parser.parse_args(list(argv) if argv is not None else None)
    settings = get_settings()
    try:
        check_results_dir(args.results_dir)
    except BuildError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    spark = build_spark("f1sa-full-report", args.master)
    try:
        report = run_report(spark, settings, results_dir=args.results_dir)
    except (ReportError, CleanError, BuildError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    ok = report_ok(report)
    print(f"full report: {'PASS' if ok else 'FAIL'} -> {args.results_dir / REPORT_FILE}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
