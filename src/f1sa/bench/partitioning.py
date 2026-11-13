"""Partitioning and caching benchmark: the same queries on an unpartitioned copy and on a copy
partitioned by season/round, each without caching, with ``cache()`` + ``count()`` and with
``persist(DISK_ONLY)`` + ``count()``, on file:// and hdfs://.

Two datasets (docs/experiments.md):

* ``laps``: clean laps and results of 2021-2025 replicated x16 (synthetic,
  :mod:`f1sa.bench.replicate`);
* ``telemetry``: the real 2024 raw telemetry samples (read from the raw zone, copied once per
  layout), with the 2024 clean laps and drivers for the join.

Queries per dataset (all return a few hundred rows at most, collected to the driver):

* ``one_race`` (2024 R5), ``one_season`` (2024; laps only, because the telemetry is one season, so
  it would equal ``full_scan``), ``full_scan``, and ``join_agg`` (a join plus an aggregation).

Every result has a checksum, which must be identical across layouts, variants and file systems.
Speed-up is the baseline median over the cell's median, against two baselines: the simplest
(file://, unpartitioned, no cache) and unpartitioned without cache on the same file system.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from f1sa.bench.paths import check_bench_uri
from f1sa.lake import schemas as S

LAYOUTS = ("unpartitioned", "partitioned")
VARIANTS = ("none", "cache", "persist_disk")
ONE_RACE = (2024, 5)
ONE_SEASON = 2024
SIMPLEST = ("file", "unpartitioned", "none")


class PartitioningError(RuntimeError):
    """Inconsistent benchmark input or results."""


def write_layout(df: DataFrame, root: str, layout: str, files: int) -> None:
    """``df`` to ``root`` either as ``files`` files in one folder (``unpartitioned``) or as one
    file per ``season=/round=`` folder (``partitioned``)."""
    root = check_bench_uri(root)
    if layout == "unpartitioned":
        if files < 1:
            raise ValueError(f"files must be >= 1, got {files}")
        df.repartition(files).write.mode("overwrite").parquet(root)
    elif layout == "partitioned":
        (
            df.repartition(*S.PARTITION_COLUMNS)
            .write.mode("overwrite")
            .partitionBy(*S.PARTITION_COLUMNS)
            .parquet(root)
        )
    else:
        raise ValueError(f"unknown layout {layout!r}")


def read_layout(spark: SparkSession, root: str, schema: T.StructType) -> DataFrame:
    """A copy written by :func:`write_layout` (either layout), with ``schema``'s column order."""
    root = check_bench_uri(root)
    return spark.read.schema(schema).parquet(root).select([f.name for f in schema.fields])


Tables = Mapping[str, DataFrame]


@dataclass(frozen=True)
class Query:
    name: str
    run: Callable[[Tables], DataFrame]


def _avg(col: str, alias: str) -> Column:
    """Mean over values cast to ``decimal(20,6)``: a decimal sum is exact, so the result does not
    depend on the summation order (layout, partitioning or caching). A double average can differ
    in the last bit, which flipped a 6-decimal rounding in the sample (108.706062 vs .063)."""
    return F.round(F.avg(F.col(col).cast("decimal(20,6)")), 6).alias(alias)


def lap_queries() -> list[Query]:
    season, round_ = ONE_RACE

    def one_race(t: Tables) -> DataFrame:
        return (
            t["laps"]
            .where((F.col("season") == season) & (F.col("round") == round_))
            .groupBy("driver_id")
            .agg(F.count(F.lit(1)).alias("laps"), _avg("lap_time_s", "mean_lap_time_s"))
        )

    def one_season(t: Tables) -> DataFrame:
        return (
            t["laps"]
            .where(F.col("season") == ONE_SEASON)
            .groupBy("round")
            .agg(F.count(F.lit(1)).alias("laps"), _avg("lap_time_s", "mean_lap_time_s"))
        )

    def full_scan(t: Tables) -> DataFrame:
        return (
            t["laps"]
            .groupBy("season")
            .agg(
                F.count(F.lit(1)).alias("laps"),
                _avg("lap_time_s", "mean_lap_time_s"),
                F.max("lap").alias("max_lap"),
            )
        )

    def join_agg(t: Tables) -> DataFrame:
        results = t["results"].select("season", "round", "driver_id", "constructor_id")
        return (
            t["laps"]
            .join(results, ["season", "round", "driver_id"])
            .groupBy("season", "constructor_id")
            .agg(F.count(F.lit(1)).alias("laps"), _avg("lap_time_s", "mean_lap_time_s"))
        )

    return [
        Query("one_race", one_race),
        Query("one_season", one_season),
        Query("full_scan", full_scan),
        Query("join_agg", join_agg),
    ]


def telemetry_queries() -> list[Query]:
    season, round_ = ONE_RACE

    def one_race(t: Tables) -> DataFrame:
        return (
            t["telemetry"]
            .where((F.col("season") == season) & (F.col("round") == round_))
            .groupBy("driver_code")
            .agg(
                F.count(F.lit(1)).alias("samples"),
                _avg("speed_kph", "mean_speed_kph"),
                F.max("speed_kph").alias("max_speed_kph"),
            )
        )

    def full_scan(t: Tables) -> DataFrame:
        return (
            t["telemetry"]
            .groupBy("season", "round")
            .agg(
                F.count(F.lit(1)).alias("samples"),
                _avg("speed_kph", "mean_speed_kph"),
                _avg("throttle_pct", "mean_throttle_pct"),
            )
        )

    def join_agg(t: Tables) -> DataFrame:
        per_lap = (
            t["telemetry"]
            .groupBy("season", "round", "driver_code", "lap")
            .agg(F.avg(F.col("speed_kph").cast("decimal(20,6)")).alias("lap_speed_kph"))
        )
        codes = t["drivers"].select(
            "season", "round", "driver_id", F.col("code").alias("driver_code")
        )
        laps = t["laps"].select("season", "round", "driver_id", "lap", "lap_time_s")
        return (
            per_lap.withColumn("lap", F.col("lap").cast("int"))
            .join(codes, ["season", "round", "driver_code"])
            .join(laps, ["season", "round", "driver_id", "lap"])
            .groupBy("season", "round")
            .agg(
                F.count(F.lit(1)).alias("laps"),
                _avg("lap_speed_kph", "mean_speed_kph"),
                _avg("lap_time_s", "mean_lap_time_s"),
            )
        )

    return [
        Query("one_race", one_race),
        Query("full_scan", full_scan),
        Query("join_agg", join_agg),
    ]


TELEMETRY_COPY_SCHEMA = T.StructType(
    [
        T.StructField("season", T.IntegerType()),
        T.StructField("round", T.IntegerType()),
        T.StructField("driver_code", T.StringType()),
        T.StructField("lap", T.LongType()),
        T.StructField("time_in_lap_s", T.DoubleType()),
        T.StructField("speed_kph", T.DoubleType()),
        T.StructField("rpm", T.DoubleType()),
        T.StructField("n_gear", T.LongType()),
        T.StructField("throttle_pct", T.DoubleType()),
        T.StructField("brake", T.BooleanType()),
    ]
)
"""The telemetry columns copied into the benchmark layouts."""


@dataclass(frozen=True)
class Dataset:
    name: str
    schemas: Mapping[str, T.StructType]
    files: Mapping[str, int]
    """Files per table in the unpartitioned layout."""
    queries: Sequence[Query]


def laps_dataset() -> Dataset:
    return Dataset(
        "laps",
        {"laps": S.LAPS, "results": S.RESULTS},
        {"laps": 8, "results": 2},
        lap_queries(),
    )


def telemetry_dataset() -> Dataset:
    return Dataset(
        "telemetry",
        {"telemetry": TELEMETRY_COPY_SCHEMA, "laps": S.LAPS, "drivers": S.DRIVERS},
        {"telemetry": 16, "laps": 2, "drivers": 1},
        telemetry_queries(),
    )


def table_root(fs_root: str, dataset: str, layout: str, table: str) -> str:
    return check_bench_uri(f"{fs_root.rstrip('/')}/partitioning/{dataset}/{layout}/{table}")


def prepare(spark: SparkSession, dataset: Dataset, sources: Tables, fs_root: str) -> dict[str, int]:
    """Write every table of ``dataset`` in both layouts under ``fs_root``; returns the rows per
    table."""
    counts: dict[str, int] = {}
    for table, schema in dataset.schemas.items():
        src = sources[table].select([f.name for f in schema.fields]).cache()
        counts[table] = src.count()
        for layout in LAYOUTS:
            write_layout(
                src, table_root(fs_root, dataset.name, layout, table), layout, dataset.files[table]
            )
        src.unpersist()
    return counts


def load(spark: SparkSession, dataset: Dataset, fs_root: str, layout: str) -> dict[str, DataFrame]:
    return {
        table: read_layout(spark, table_root(fs_root, dataset.name, layout, table), schema)
        for table, schema in dataset.schemas.items()
    }


def apply_variant(tables: Tables, variant: str) -> dict[str, DataFrame]:
    """``none``: as read; ``cache``: ``cache()`` + ``count()``; ``persist_disk``:
    ``persist(DISK_ONLY)`` + ``count()``. The count materialises the cache."""
    if variant == "none":
        return dict(tables)
    if variant not in VARIANTS:
        raise ValueError(f"unknown variant {variant!r}")
    out: dict[str, DataFrame] = {}
    for name, df in tables.items():
        kept = df.cache() if variant == "cache" else df.persist(StorageLevel.DISK_ONLY)
        kept.count()
        out[name] = kept
    return out


def result_checksum(rows: Sequence[Row]) -> str:
    """Order-independent checksum of a small collected result."""
    lines = sorted(repr(tuple(r)) for r in rows)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]


def speedups(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """``rows`` with ``speedup_vs_simplest`` and ``speedup_vs_unpartitioned_same_fs`` (baseline
    median / this median; empty when the baseline is missing) and ``checksum_match`` (the result
    equals the simplest baseline's)."""

    def key(r: Mapping[str, object], fs: object, layout: str, variant: str) -> tuple[object, ...]:
        return (r["dataset"], r["query"], fs, layout, variant)

    by = {key(r, r["fs"], str(r["layout"]), str(r["variant"])): r for r in rows}
    out: list[dict[str, object]] = []
    for r in rows:
        row = dict(r)
        simplest = by.get(key(r, *SIMPLEST))
        same_fs = by.get(key(r, r["fs"], "unpartitioned", "none"))
        med = float(str(r["median_s"]))
        for col, base in (
            ("speedup_vs_simplest", simplest),
            ("speedup_vs_unpartitioned_same_fs", same_fs),
        ):
            row[col] = (float(str(base["median_s"])) / med) if base is not None and med > 0 else ""
        row["checksum_match"] = (
            "" if simplest is None else str(simplest["result_checksum"] == r["result_checksum"])
        )
        out.append(row)
    return out


def check_checksums(rows: Sequence[Mapping[str, object]]) -> list[str]:
    """Each (dataset, query) whose results differ across layouts, variants or file systems."""
    seen: dict[tuple[object, object], set[object]] = {}
    for r in rows:
        seen.setdefault((r["dataset"], r["query"]), set()).add(r["result_checksum"])
    return [f"{d}/{q}" for (d, q), sums in sorted(seen.items(), key=str) if len(sums) > 1]


def estimate_partitioning(
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    *,
    laps_factor: int,
    telemetry_round_ratio: float,
    runs: int = 6,
) -> float:
    """Seconds for the full partitioning run estimated from a smoke run (laps x1, 1 run):
    every query time, materialisation and preparation scaled linearly by the data size ratio,
    with ``runs`` (1 warm-up + 5 timed) runs per query."""
    ratio = {"laps": float(laps_factor), "telemetry": telemetry_round_ratio}
    total = 0.0
    materialised: set[tuple[object, ...]] = set()
    for r in rows:
        k = ratio[str(r["dataset"])]
        total += runs * float(r["median_s"]) * k
        key = (r["dataset"], r["fs"], r["layout"], r["variant"])
        if key not in materialised:
            materialised.add(key)
            total += float(r.get("materialise_s") or 0.0) * k
    for name, info in (summary.get("prep") or {}).items():
        total += float(info["seconds"]) * ratio[str(name).split("/")[0]]
    return total


GroupKey = tuple[str, str, str, str]
"""(dataset, fs, layout, variant): the unit a resumed run skips or reruns."""


def group_key(row: Mapping[str, Any]) -> GroupKey:
    return (str(row["dataset"]), str(row["fs"]), str(row["layout"]), str(row["variant"]))


def done_groups(rows: Sequence[Mapping[str, Any]], datasets: Sequence[Dataset]) -> set[GroupKey]:
    """Groups whose every query has a row (a group is committed as a whole, so a partial group
    can only come from a hand-edited file and is rerun)."""
    queries = {d.name: {q.name for q in d.queries} for d in datasets}
    seen: dict[GroupKey, set[str]] = {}
    for r in rows:
        seen.setdefault(group_key(r), set()).add(str(r["query"]))
    return {k for k, qs in seen.items() if k[0] in queries and qs >= queries[k[0]]}


def expected_groups(datasets: Sequence[Dataset], file_systems: Sequence[str]) -> set[GroupKey]:
    return {
        (d.name, fs, layout, variant)
        for d in datasets
        for fs in file_systems
        for layout in LAYOUTS
        for variant in VARIANTS
    }


def append_jsonl_atomic(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    """Append ``rows`` as JSON lines; the whole file is rewritten and renamed (atomic)."""
    from f1sa.bench.report import replace_atomically

    current = path.read_text(encoding="utf-8") if path.exists() else ""
    if current and not current.endswith("\n"):
        current = current.rsplit("\n", 1)[0] + "\n" if "\n" in current else ""
    lines = "".join(json.dumps(dict(r), sort_keys=True) + "\n" for r in rows)
    replace_atomically(path, current + lines)


def prepare_or_reuse(
    spark: SparkSession, dataset: Dataset, sources: Tables, fs_root: str
) -> tuple[dict[str, int], bool]:
    """Reuse the complete copies of ``dataset`` under ``fs_root`` (marker present), or delete a
    partial copy and :func:`prepare` it again. Returns (rows per table, reused)."""
    from f1sa.bench.cell import delete_uri, read_marker, write_marker

    root = check_bench_uri(f"{fs_root.rstrip('/')}/partitioning/{dataset.name}")
    marker = read_marker(spark, root)
    if marker is not None and isinstance(marker.get("rows"), dict):
        return {str(k): int(v) for k, v in marker["rows"].items()}, True
    delete_uri(spark, root)
    counts = prepare(spark, dataset, sources, fs_root)
    write_marker(spark, root, {"rows": counts, "dataset": dataset.name})
    return counts, False


# ============================================================================== runner
def run_dataset(
    spark: SparkSession,
    dataset: Dataset,
    fs: str,
    fs_root: str,
    rows_in: Mapping[str, int],
    *,
    warmup: int,
    repeats: int,
    emit_group: Callable[[list[dict[str, Any]]], None],
    done: frozenset[GroupKey] | set[GroupKey] = frozenset(),
) -> None:
    """Time every (layout, variant, query) of ``dataset`` on one file system. A group (layout,
    variant) is emitted as a whole once all its queries are timed; groups in ``done`` (already
    committed by an earlier, interrupted run) are skipped."""
    from f1sa.bench.spark_metrics import RestMetrics
    from f1sa.bench.timing import run_timed, summarise

    rest = RestMetrics.for_session(spark)
    sc = spark.sparkContext
    for layout in LAYOUTS:
        for variant in VARIANTS:
            if (dataset.name, fs, layout, variant) in done:
                continue
            group_rows: list[dict[str, Any]] = []
            t0 = time.perf_counter()
            tables = apply_variant(load(spark, dataset, fs_root, layout), variant)
            materialise_s = time.perf_counter() - t0 if variant != "none" else 0.0
            for query in dataset.queries:
                group = f"{dataset.name}-{fs}-{layout}-{variant}-{query.name}"

                def job(
                    label: str, q: Query = query, g: str = group, t: Tables = tables
                ) -> tuple[str, int, Any]:
                    sc.setJobGroup(f"{g}-{label}", f"{g}-{label}")
                    rows = q.run(t).collect()
                    totals = rest.totals_for_group(f"{g}-{label}")
                    return result_checksum(rows), len(rows), totals

                runs = run_timed(job, warmup=warmup, repeats=repeats)
                sums = {r[0] for r in runs.results}
                s = summarise(runs.times_s)
                group_rows.append(
                    {
                        "dataset": dataset.name,
                        "fs": fs,
                        "layout": layout,
                        "variant": variant,
                        "query": query.name,
                        "rows_in": sum(rows_in.values()),
                        "warmup_runs": warmup,
                        "n_runs": s.n,
                        "median_s": s.median_s,
                        "q1_s": s.q1_s,
                        "q3_s": s.q3_s,
                        "iqr_s": s.iqr_s,
                        "min_s": s.min_s,
                        "max_s": s.max_s,
                        "materialise_s": materialise_s,
                        "input_bytes": int(median(r[2].input_bytes for r in runs.results)),
                        "shuffle_read_bytes": int(
                            median(r[2].shuffle_read_bytes for r in runs.results)
                        ),
                        "result_rows": runs.results[-1][1],
                        "result_checksum": runs.results[-1][0] if len(sums) == 1 else "unstable",
                        "files_unpartitioned": dataset.files.get(next(iter(dataset.schemas))),
                    }
                )
            spark.catalog.clearCache()
            emit_group(group_rows)


def lap_sources(
    spark: SparkSession, clean_root: str, seasons: Sequence[int], factor: int
) -> dict[str, DataFrame]:
    """Clean laps and results of ``seasons`` replicated ``factor`` times (flag dropped)."""
    from f1sa.bench.replicate import load_seasons, replicate_tables, strip_flag

    clean = load_seasons(spark, clean_root, seasons, ("laps", "results"))
    return strip_flag(replicate_tables(clean, factor))


def telemetry_sources(
    spark: SparkSession, clean_root: str, telemetry_root: str, rounds: Sequence[int]
) -> dict[str, DataFrame]:
    """The real 2024 raw telemetry of ``rounds`` plus the 2024 clean laps and drivers."""
    from f1sa.bench.replicate import load_seasons
    from f1sa.bench.workloads import TELEMETRY_SEASON, read_raw_telemetry

    raw = read_raw_telemetry(spark, telemetry_root, TELEMETRY_SEASON, rounds)
    clean = load_seasons(spark, clean_root, [TELEMETRY_SEASON], ("laps", "drivers"))
    keep = F.col("round").isin(list(rounds))
    return {
        "telemetry": raw.select([f.name for f in TELEMETRY_COPY_SCHEMA.fields]),
        "laps": clean["laps"].where(keep),
        "drivers": clean["drivers"].where(keep),
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Exit 0 when every group is done and every result checksum agrees, 1 otherwise."""
    from f1sa.bench.cell import MASTERS, build_session, delete_uri, spark_conf
    from f1sa.bench.grid import ALL_SEASONS, CLEAN_ROOT, TELEMETRY_ROOT
    from f1sa.bench.paths import bench_file_uri, bench_hdfs_uri
    from f1sa.bench.resources import ProcessSampler
    from f1sa.bench.workloads import TELEMETRY_SEASON, available_rounds

    p = argparse.ArgumentParser(prog="f1sa-bench-partitioning", description=__doc__.splitlines()[0])
    p.add_argument("--run-dir", type=Path, required=True, help="./data/bench/<run_id>")
    p.add_argument("--laps-factor", type=int, default=16)
    p.add_argument("--telemetry-rounds", type=int, default=0, help="first N 2024 rounds; 0 = all")
    p.add_argument("--fs", nargs="+", default=["file", "hdfs"], choices=["file", "hdfs"])
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--keep-data", action="store_true")
    p.add_argument("--resume", action="store_true", help="skip groups already in the JSONL")
    args = p.parse_args(list(argv) if argv is not None else None)
    run_dir: Path = args.run_dir.resolve()
    run_id = run_dir.name
    bench_file_uri(run_id)  # checks the run dir name and root
    run_dir.mkdir(parents=True, exist_ok=True)
    out_path = run_dir / "partitioning.jsonl"
    summary_path = run_dir / "partitioning_summary.json"
    if out_path.exists() and not args.resume:
        print(f"partitioning: {out_path} exists; use --resume", file=sys.stderr)
        return 2
    datasets_def = [laps_dataset(), telemetry_dataset()]
    previous: list[dict[str, Any]] = []
    if out_path.exists():
        previous = [json.loads(ln) for ln in out_path.read_text().splitlines() if ln.strip()]
    done = done_groups(previous, datasets_def)
    expected = expected_groups(datasets_def, args.fs)
    old_summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    prep: dict[str, Any] = dict(old_summary.get("prep") or {})
    if args.resume:
        print(f"partitioning: resuming, {len(done & expected)} of {len(expected)} groups done")
    roots = {"file": bench_file_uri(run_id, "data"), "hdfs": bench_hdfs_uri(run_id)}
    master = MASTERS["local4"]
    spark = build_session(master, "f1sa-bench-partitioning", spark_conf(master))
    sampler = ProcessSampler(os.getpid(), interval_s=0.5).start()
    started = time.time()
    rounds: list[int] = []

    def emit_group(rows: list[dict[str, Any]]) -> None:
        stamped = [r | {"run_id": run_id, "master": master} for r in rows]
        append_jsonl_atomic(out_path, stamped)
        done.update(group_key(r) for r in stamped)
        for r in stamped:
            print(
                f"[{time.strftime('%H:%M:%S')}] {r['dataset']} {r['fs']} {r['layout']} "
                f"{r['variant']} {r['query']}: median {r['median_s']:.3f} s",
                flush=True,
            )

    try:
        rounds = available_rounds(spark, TELEMETRY_ROOT, TELEMETRY_SEASON)
        if args.telemetry_rounds:
            rounds = rounds[: args.telemetry_rounds]
        sources = {
            "laps": lambda: lap_sources(spark, CLEAN_ROOT, ALL_SEASONS, args.laps_factor),
            "telemetry": lambda: telemetry_sources(spark, CLEAN_ROOT, TELEMETRY_ROOT, rounds),
        }
        for fs in args.fs:
            for dataset in datasets_def:
                todo = {k for k in expected if k[:2] == (dataset.name, fs)} - done
                if not todo:
                    continue
                t0 = time.perf_counter()
                counts, reused = prepare_or_reuse(
                    spark, dataset, sources[dataset.name](), roots[fs]
                )
                prep[f"{dataset.name}/{fs}"] = {
                    "rows": counts,
                    "seconds": time.perf_counter() - t0,
                    "reused": reused,
                }
                print(f"{'reused' if reused else 'prepared'} {dataset.name} on {fs}: {counts}")
                run_dataset(
                    spark,
                    dataset,
                    fs,
                    roots[fs],
                    counts,
                    warmup=args.warmup,
                    repeats=args.repeats,
                    emit_group=emit_group,
                    done=done,
                )
    finally:
        peaks = sampler.stop()
        complete = expected <= done
        summary = {
            "run_id": run_id,
            "laps_factor": args.laps_factor,
            "telemetry_rounds": len(rounds),
            "conf": spark_conf(master),
            "peaks": asdict(peaks),
            "started_at": old_summary.get("started_at", started),
            "ended_at": time.time(),
            "prep": prep,
            "complete": complete,
            "groups_done": len(expected & done),
            "groups_expected": len(expected),
        }
        from f1sa.bench.report import replace_atomically

        replace_atomically(summary_path, json.dumps(summary, indent=1, sort_keys=True, default=str))
        if complete and not args.keep_data:
            for fs in args.fs:
                delete_uri(spark, f"{roots[fs]}/partitioning")
        spark.stop()
    rows = [json.loads(ln) for ln in out_path.read_text(encoding="utf-8").splitlines() if ln]
    bad = check_checksums(rows)
    if bad:
        print(f"partitioning: results differ across layouts/variants: {bad}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
