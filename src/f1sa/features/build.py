"""Build the Step 6 feature tables from the clean zone.

    uv run python -m f1sa.features.build --source sample

Input is ``{DATA_LAKE_URI}/clean/{table}``, read one table at a time, so ``_quality`` is never
read. Output is ``{DATA_LAKE_URI}/features/{table}/season=S/round=R``, written with dynamic
partition overwrite. In file mode the job refuses to write outside ``./data/lake``.

``--source sample`` builds the races in the offline sample bundle, and every one of them must be
in the clean zone (``make etl-sample``). ``--source lake`` builds every race in the clean zone.
With ``--source sample`` the build also writes the undercut smoke-test counts to
``{--results-dir}/undercut_counts_sample.csv`` (default ``results/``).
Prior-race history (pit loss, team out-lap) is always computed over **all** clean races and then
filtered to the target races, so a race's history includes every earlier race the lake holds and
never the race itself or a later one.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

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
from f1sa.etl.rdd_jobs import rdd_lap_time_distribution
from f1sa.etl.spark import build_spark, configure_session
from f1sa.etl.sql_jobs import run_sql_views
from f1sa.features import schemas as FS
from f1sa.features.gaps import gaps_from_enriched
from f1sa.features.lineage import unknown_constructor_ids
from f1sa.features.outlap import outlap_features
from f1sa.features.pitloss import pit_loss_features, race_summary_features, team_outlap_features
from f1sa.features.stints import stint_features
from f1sa.features.telemetry_agg import telemetry_lap_features
from f1sa.features.undercut import undercut_attempts, undercut_counts, write_counts_csv

SPARSE_TABLES = frozenset({"undercut_attempts"})
"""Tables where a race may legitimately have no rows (no undercut attempt). Their stale target
partitions are deleted instead of being required."""
COUNTS_FILE = "undercut_counts_sample.csv"


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
    out: Callable[[str], None] = print,
) -> BuildResult:
    """Read the clean zone, compute the features, write them and verify every partition.

    With ``source == "sample"`` the undercut counts go to ``results_dir/COUNTS_FILE``.
    ``results_dir`` must resolve to ``./results`` (or below it).
    """
    started = time.perf_counter()
    if source == "sample":
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
            expected[name] = race_keys(df)
            delete_partitions(spark, features_root, name, sorted(targets - set(expected[name])))
        else:
            expected[name] = sorted(targets)
        write_partitioned(df, features_root, name, FS.FEATURE_KEYS[name])

    result = BuildResult(clean_root, features_root, sorted(targets))
    for name in FS.FEATURE_TABLES:
        stored = select_races(read_feature(spark, features_root, name), None, targets).cache()
        result.counts[name] = stored.count()
        result.partitions[name] = race_keys(stored)
        result.checksums[name] = table_checksum(stored.drop(*_map_columns(name)))
        if name == "undercut_attempts" and source == "sample":
            path = (results_dir / COUNTS_FILE).resolve()
            write_counts_csv(undercut_counts(stored, source), path, smoke_test=True)
            result.counts_csv = path
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


def check_results_dir(results_dir: Path) -> None:
    """Results may only be written under ``./results`` (relative to the working directory)."""
    allowed = (Path.cwd() / "results").resolve()
    target = results_dir.resolve()
    if target != allowed and allowed not in target.parents:
        raise BuildError(f"results dir {results_dir} is outside {allowed}")


def delete_partitions(
    spark: SparkSession, features_root: str, table: str, races: Iterable[RaceKey]
) -> None:
    """Delete ``{root}/{table}/season=S/round=R`` for ``races`` (stale sparse partitions)."""
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise BuildError("Spark JVM gateway unavailable")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for season, round_ in races:
        path = jvm.org.apache.hadoop.fs.Path(
            f"{features_root}/{table}/season={season}/round={round_}"
        )
        fs = path.getFileSystem(conf)
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
        out(f"undercut counts (SMOKE TEST ONLY): {result.counts_csv}  sha256 {digest}")
    out(f"done in {result.seconds:.1f}s")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m f1sa.features.build")
    parser.add_argument("--source", choices=("sample", "lake"), required=True)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="where --source sample writes undercut_counts_sample.csv (must be under ./results)",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    settings = get_settings()
    try:
        _clean, features_root = resolve_feature_roots(settings)
        if args.source == "sample":
            sample_races(settings)
            check_results_dir(args.results_dir)
    except (CleanError, BuildError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    local_dir = None
    if features_root.startswith("file://"):
        local_dir = Path(features_root.removeprefix("file://")).parent / ".spark-tmp"
    spark = build_spark("f1sa-features", args.master, local_dir=local_dir)
    try:
        run_build(spark, settings, source=args.source, results_dir=args.results_dir)
    except (BuildError, CleanError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
