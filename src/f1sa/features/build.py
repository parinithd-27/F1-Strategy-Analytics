"""Build the Step 6 feature tables from the clean zone.

    uv run python -m f1sa.features.build --source sample

Input is ``{DATA_LAKE_URI}/clean/{table}``, read one table at a time, so ``_quality`` is never
read. Output is ``{DATA_LAKE_URI}/features/{table}/season=S/round=R``, written with dynamic
partition overwrite. In file mode the job refuses to write outside ``./data/lake``.

``--source sample`` builds the races in the offline sample bundle, and every one of them must be
in the clean zone (``make etl-sample``). ``--source lake`` builds every race in the clean zone.
With ``--source sample`` the build also writes the undercut smoke-test counts to
``{--results-dir}/undercut_counts_sample.csv`` (default ``results/``). ``--source lake
--full-counts`` writes the real counts to ``{--results-dir}/undercut_counts_full.csv`` and refuses
if any attempt is flagged synthetic (``make features-full``).
Prior-race history (pit loss, team out-lap) is always computed over **all** clean races and then
filtered to the target races, so a race's history includes every earlier race the lake holds and
never the race itself or a later one.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.config import Settings, get_settings
from f1sa.etl.clean import CleanError, select_races, table_checksum
from f1sa.etl.df_jobs import enrich_laps
from f1sa.etl.io import (
    RaceKey,
    load_clean,
    race_keys,
    resolve_feature_roots,
    restrict,
    sample_races,
    write_partitioned,
)
from f1sa.etl.rdd_jobs import green_laps, rdd_lap_time_distribution
from f1sa.etl.spark import build_spark, configure_session
from f1sa.etl.sql_jobs import run_sql_views
from f1sa.features import schemas as FS
from f1sa.features.gaps import gaps_from_enriched
from f1sa.features.lineage import unknown_constructor_ids
from f1sa.features.neutral_split import neutralised_kinds
from f1sa.features.outlap import outlap_features
from f1sa.features.pitloss import pit_loss_features, race_summary_features, team_outlap_features
from f1sa.features.stints import stint_features
from f1sa.features.telemetry_agg import telemetry_lap_features
from f1sa.features.undercut import undercut_attempts, undercut_counts, write_counts_csv

SPARSE_TABLES = frozenset({"undercut_attempts", "telemetry_lap_features"})
"""Tables where a race may legitimately have no rows (no undercut attempt; no telemetry before the
FastF1 pass). Their stale target partitions are deleted instead of being required (file mode only;
see ``delete_partitions``)."""
COUNTS_FILE = "undercut_counts_sample.csv"
FULL_COUNTS_FILE = "undercut_counts_full.csv"


class BuildError(RuntimeError):
    """The feature build cannot run or produced inconsistent output."""


@dataclass
class BuildResult:
    clean_root: str
    features_root: str
    races: list[RaceKey]
    counts: dict[str, int] = field(default_factory=dict)
    partitions: dict[str, list[RaceKey]] = field(default_factory=dict)
    checksums: dict[str, str] = field(default_factory=dict)
    seconds: float = 0.0
    counts_csv: Path | None = None


def compute_features(
    spark: SparkSession, clean: dict[str, DataFrame], targets: set[RaceKey]
) -> dict[str, DataFrame]:
    """All feature tables for ``targets``. ``clean`` holds every clean race (the history)."""
    views = run_sql_views(spark, clean)
    t = restrict(clean, targets)
    enriched = enrich_laps(
        t["laps"],
        t["results"],
        t["pitstops"],
        t["drivers"],
        t["constructors"],
        t["stints"],
        t["races"],
    ).cache()

    def sel(df: DataFrame) -> DataFrame:
        return select_races(df, None, targets)

    pit_loss = sel(pit_loss_features(views))
    outlap = sel(
        outlap_features(
            clean["laps"], clean["stints"], clean["pitstops"], clean["results"], clean["races"]
        )
    )
    return {
        "laps_enriched": enriched,
        "gaps": gaps_from_enriched(enriched),
        "stints": stint_features(t["laps"], t["stints"]),
        "lap_time_distribution": rdd_lap_time_distribution(t["laps"], t["races"]),
        "pit_loss": pit_loss,
        "team_outlap_history": sel(team_outlap_features(views)),
        "race_summary": sel(race_summary_features(views)),
        "telemetry_lap_features": telemetry_lap_features(
            t["telemetry_raw"], t["laps"], t["telemetry_lap"]
        ),
        "outlap_features": outlap,
        "undercut_attempts": undercut_attempts(
            spark, enriched, t["pitstops"], t["stints"], pit_loss, outlap
        ),
    }


def read_feature(spark: SparkSession, features_root: str, table: str) -> DataFrame:
    """A stored feature table with its explicit schema."""
    schema = FS.FEATURE_SCHEMAS[table]
    df = spark.read.schema(schema).parquet(f"{features_root}/{table}")
    return df.select([f.name for f in schema.fields])


def run_build(
    spark: SparkSession,
    settings: Settings,
    *,
    source: str,
    results_dir: Path = Path("results"),
    full_counts: bool = False,
    out: Callable[[str], None] = print,
) -> BuildResult:
    """Read the clean zone, compute the features, write them and verify every partition.

    With ``source == "sample"`` the undercut counts go to ``results_dir/COUNTS_FILE``. With
    ``full_counts`` (``source == "lake"`` only) they go to ``results_dir/FULL_COUNTS_FILE``.
    ``results_dir`` must resolve to ``./results`` (or below it).
    """
    started = time.perf_counter()
    if full_counts and source != "lake":
        raise BuildError("--full-counts needs --source lake")
    if source == "sample" or full_counts:
        check_results_dir(results_dir)
    configure_session(spark)
    clean_root, features_root = resolve_feature_roots(settings)
    clean = {name: df.cache() for name, df in load_clean(spark, clean_root).items()}
    available = race_keys(clean["races"])
    if not available or clean["laps"].isEmpty():
        raise BuildError(f"clean zone at {clean_root} is empty; run `make etl-sample` first")
    if source == "sample":
        wanted = sample_races(settings)
        missing = sorted(set(wanted) - set(available))
        if missing:
            raise BuildError(
                f"sample races {missing} are not in the clean zone; run `make etl-sample` first"
            )
    elif source == "lake":
        wanted = available
    else:
        raise BuildError(f"unknown source {source!r}")
    targets = set(wanted)
    unknown = unknown_constructor_ids(clean["results"])
    if unknown:
        raise BuildError(
            f"constructor ids {unknown} are neither in features.lineage.LINEAGE nor in "
            "STABLE_CONSTRUCTORS; add the rename (or the stable id) before building team history"
        )

    tables = compute_features(spark, clean, targets)
    expected_laps = select_races(clean["laps"], None, targets).count()
    enriched_laps = tables["laps_enriched"].count()
    if enriched_laps != expected_laps:
        raise BuildError(f"laps_enriched has {enriched_laps} rows, clean laps {expected_laps}")
    expected: dict[str, list[RaceKey]] = {}
    for name in FS.FEATURE_TABLES:
        df = tables[name]
        if name in SPARSE_TABLES:
            df = df.cache()
            tables[name] = df
            expected[name] = expected_partitions(name, df, clean["laps"], targets)
            delete_partitions(spark, features_root, name, sorted(targets - set(expected[name])))
        else:
            expected[name] = expected_partitions(name, df, clean["laps"], targets)
        write_partitioned(df, features_root, name, FS.FEATURE_KEYS[name])

    result = BuildResult(clean_root, features_root, sorted(targets))
    for name in FS.FEATURE_TABLES:
        stored = select_races(read_feature(spark, features_root, name), None, targets).cache()
        result.counts[name] = stored.count()
        result.partitions[name] = race_keys(stored)
        result.checksums[name] = table_checksum(stored.drop(*_map_columns(name)))
        if name == "undercut_attempts" and source == "sample":
            path = (results_dir / COUNTS_FILE).resolve()
            kinds = neutralised_kinds(stored, clean["laps"], clean["pitstops"])
            write_counts_csv(undercut_counts(stored, source, kinds), path, smoke_test=True)
            result.counts_csv = path
        if name == "undercut_attempts" and full_counts:
            kinds = neutralised_kinds(stored, clean["laps"], clean["pitstops"])
            result.counts_csv = write_full_counts(stored, results_dir, kinds)
        stored.unpersist()
        if result.partitions[name] != expected[name]:
            raise BuildError(
                f"{name}: wrote partitions {result.partitions[name]}, expected {expected[name]}"
            )
    for df in clean.values():
        df.unpersist()
    tables["laps_enriched"].unpersist()
    for name in SPARSE_TABLES:
        tables[name].unpersist()
    result.seconds = time.perf_counter() - started
    _print_result(result, out)
    return result


def expected_partitions(
    name: str, df: DataFrame, laps: DataFrame, targets: set[RaceKey]
) -> list[RaceKey]:
    """The races table ``name`` must hold after the write.

    A sparse table holds whatever it computed. ``lap_time_distribution`` needs every race with at
    least one green lap (a race run entirely behind the safety car, such as 2021 R12, has none);
    that set comes from ``laps``, not from the job's own output, so a dropped race still fails.
    """
    if name in SPARSE_TABLES:
        return race_keys(df)
    if name == "lap_time_distribution":
        return race_keys(green_laps(select_races(laps, None, targets)))
    return sorted(targets)


def write_full_counts(
    attempts: DataFrame,
    results_dir: Path,
    neutral_kinds: Mapping[tuple[int, int, int, str, str, float, int], str] | None = None,
) -> Path:
    """Write the real undercut counts, refusing attempts that carry a synthetic compound.
    ``neutral_kinds`` fills the reporting-only split of ``n_excluded_neutralised``."""
    flagged = attempts.where(F.coalesce(F.col("compound_is_synthetic"), F.lit(True))).count()
    if flagged:
        raise BuildError(
            f"{flagged} attempts are flagged compound_is_synthetic (or unproven); "
            f"refusing to write {FULL_COUNTS_FILE} from them"
        )
    path = (results_dir / FULL_COUNTS_FILE).resolve()
    write_counts_csv(undercut_counts(attempts, "full", neutral_kinds), path, smoke_test=False)
    return path


def check_results_dir(results_dir: Path) -> None:
    """Results may only be written under ``./results``, or in a run folder under ``./data/demo``
    (``make demo`` keeps its counts out of the committed results; relative to the working
    directory)."""
    allowed = (Path.cwd() / "results").resolve()
    demo = (Path.cwd() / "data" / "demo").resolve()
    target = results_dir.resolve()
    if target == allowed or allowed in target.parents or demo in target.parents:
        return
    raise BuildError(f"results dir {results_dir} is outside {allowed} and {demo}/<run>")


def delete_partitions(
    spark: SparkSession, features_root: str, table: str, races: Iterable[RaceKey]
) -> None:
    """Delete ``{root}/{table}/season=S/round=R`` for ``races`` (stale sparse partitions).

    On ``hdfs://`` a stale partition is an error instead: this job never deletes from HDFS.
    """
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise BuildError("Spark JVM gateway unavailable")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for season, round_ in races:
        path = jvm.org.apache.hadoop.fs.Path(
            f"{features_root}/{table}/season={season}/round={round_}"
        )
        fs = path.getFileSystem(conf)
        if features_root.startswith("hdfs://") and fs.exists(path):
            raise BuildError(f"stale partition {path} in HDFS; refusing to delete it")
        if fs.exists(path) and not fs.delete(path, True):
            raise BuildError(f"could not delete stale partition {path}")


def _map_columns(table: str) -> list[str]:
    """Map-typed columns (xxhash64 cannot hash maps; they are left out of the checksum)."""
    return [f.name for f in FS.FEATURE_SCHEMAS[table].fields if f.dataType.typeName() == "map"]


def _print_result(result: BuildResult, out: Callable[[str], None]) -> None:
    races = ", ".join(f"{s} R{r}" for s, r in result.races)
    out(f"features: {result.clean_root} -> {result.features_root}")
    out(f"races: {races}")
    for name in FS.FEATURE_TABLES:
        parts = " ".join(f"season={s}/round={r}" for s, r in result.partitions[name])
        n, digest = result.counts[name], result.checksums[name]
        out(f"  {name:<22} {n:>6} rows  {parts}  checksum {digest}")
    if result.counts_csv is not None:
        digest = hashlib.sha256(result.counts_csv.read_bytes()).hexdigest()[:16]
        label = (
            "undercut counts"
            if result.counts_csv.name == FULL_COUNTS_FILE
            else ("undercut counts (SMOKE TEST ONLY)")
        )
        out(f"{label}: {result.counts_csv}  sha256 {digest}")
    out(f"done in {result.seconds:.1f}s")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m f1sa.features.build")
    parser.add_argument("--source", choices=("sample", "lake"), required=True)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="where the undercut counts CSV is written (under ./results or ./data/demo/<run>)",
    )
    parser.add_argument(
        "--full-counts",
        action="store_true",
        help="with --source lake: write the real counts to undercut_counts_full.csv",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    settings = get_settings()
    try:
        _clean, features_root = resolve_feature_roots(settings)
        if args.full_counts and args.source != "lake":
            raise BuildError("--full-counts needs --source lake")
        if args.source == "sample":
            sample_races(settings)
        if args.source == "sample" or args.full_counts:
            check_results_dir(args.results_dir)
    except (CleanError, BuildError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    local_dir = None
    if features_root.startswith("file://"):
        local_dir = Path(features_root.removeprefix("file://")).parent / ".spark-tmp"
    spark = build_spark("f1sa-features", args.master, local_dir=local_dir)
    try:
        run_build(
            spark,
            settings,
            source=args.source,
            results_dir=args.results_dir,
            full_counts=args.full_counts,
        )
    except (BuildError, CleanError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
