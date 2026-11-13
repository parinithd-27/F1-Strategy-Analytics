"""Benchmark workloads: feature build into a bench root, telemetry aggregation, and the
partitioning queries (Step 10)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from conftest import SampleLake
from f1sa.bench import partitioning as P
from f1sa.bench.paths import BenchPathError, bench_file_uri
from f1sa.bench.replicate import load_seasons, replicate_tables, strip_flag
from f1sa.bench.workloads import (
    NOOP_TABLES,
    WorkloadError,
    available_rounds,
    feature_build,
    lap_aggregates,
    read_raw_telemetry,
    round_summary,
    telemetry_aggregation,
    telemetry_rounds,
)
from f1sa.features.telemetry_agg import aggregate_samples


def test_feature_build_on_replicated_input_writes_every_table_but_the_labels(
    spark: SparkSession, sample_clean_lake: SampleLake, tmp_path: Path
) -> None:
    clean = strip_flag(
        replicate_tables(load_seasons(spark, sample_clean_lake.clean_root, [2023, 2024]), 2)
    )
    out = bench_file_uri("r1", "features")
    written = feature_build(spark, clean, out)
    assert "undercut_attempts" not in written and {"undercut_attempts"} == NOOP_TABLES
    root = tmp_path / "data" / "bench" / "r1" / "features"
    assert sorted(p.name for p in root.iterdir()) == sorted(written)
    assert not (root / "undercut_attempts").exists(), "labels are never persisted"
    seasons = {
        r[0]
        for r in spark.read.parquet(f"{out}/laps_enriched").select("season").distinct().collect()
    }
    assert seasons == {2023, 2024, 2123, 2124}
    with pytest.raises(BenchPathError):
        feature_build(spark, clean, f"file://{tmp_path}/data/lake/features")


def _raw_fixture(base: Path) -> tuple[str, pd.DataFrame]:
    """Two drivers, two laps, a few samples each, in ``season=/round=/driver=`` folders."""
    rows = []
    for code in ("VER", "NOR"):
        for lap in (1, 2):
            for i, t in enumerate((0.1, 0.5, 0.9)):
                rows.append(
                    {
                        "driver_code": code,
                        "lap": lap,
                        "time_in_lap_s": t,
                        "speed_kph": 200.0 + 10 * i + lap,
                        "rpm": 11000.0 + i,
                        "n_gear": 6 + (i % 2),
                        "throttle_pct": 100.0 if i else 50.0,
                        "brake": i == 0,
                        "drs": 0,
                    }
                )
    df = pd.DataFrame(rows)
    root = base / "data" / "bench" / "raw_tel"
    for code, part in df.groupby("driver_code"):
        folder = root / "season=2024" / "round=5" / f"driver={code}"
        folder.mkdir(parents=True)
        part.to_parquet(folder / "telemetry.parquet", index=False)
        (folder / "_manifest.json").write_text("{}")
    (root / "season=2024" / "round=7").mkdir(parents=True)
    return f"file://{root}", df


def test_telemetry_aggregation_reuses_aggregate_samples(
    spark: SparkSession, tmp_path: Path
) -> None:
    root, df = _raw_fixture(tmp_path)
    assert available_rounds(spark, root, 2024) == [5, 7]
    assert available_rounds(spark, root, 2023) == []
    raw = read_raw_telemetry(spark, root, 2024, [5])
    assert raw.count() == len(df)
    got = lap_aggregates(raw)
    direct = aggregate_samples(
        spark.createDataFrame(df).select(
            F.lit(2024).alias("season"),
            F.lit(5).alias("round"),
            F.col("driver_code").alias("driver_id"),
            F.col("lap").cast("int").alias("lap"),
            F.col("time_in_lap_s").alias("t"),
            "speed_kph",
            "throttle_pct",
            "brake",
            F.col("n_gear").cast("int").alias("n_gear"),
            "rpm",
        )
    )
    cols = sorted(got.columns)
    assert sorted(got.select(cols).collect()) == sorted(direct.select(cols).collect())
    drivers = spark.createDataFrame(
        [(2024, 5, "max_verstappen", "VER"), (2024, 5, "lando_norris", "NOR")],
        "season int, round int, driver_id string, code string",
    )
    laps = spark.createDataFrame(
        [
            (2024, 5, d, lap, 90.0 + lap)
            for d in ("max_verstappen", "lando_norris")
            for lap in (1, 2)
        ],
        "season int, round int, driver_id string, lap int, lap_time_s double",
    )
    summary_df = round_summary(got, drivers, laps)
    plan = summary_df._jdf.queryExecution().optimizedPlan().toString()
    assert "Window" in plan, "the gear-change lag window must not be pruned"
    summary = summary_df.collect()
    assert len(summary) == 1 and summary[0]["laps"] == 4 and summary[0]["drivers"] == 2
    assert summary[0]["samples"] == len(df)
    telemetry_aggregation(spark, root, {"drivers": drivers, "laps": laps}, [5])
    with pytest.raises(WorkloadError):
        read_raw_telemetry(spark, root, 2024, [])


def test_telemetry_rounds_by_seasons_axis() -> None:
    avail = list(range(1, 25))
    assert telemetry_rounds(1, avail) == [1, 2, 3, 4, 5]
    assert len(telemetry_rounds(3, avail)) == 14 and telemetry_rounds(5, avail) == avail
    with pytest.raises(ValueError):
        telemetry_rounds(2, avail)
    with pytest.raises(WorkloadError):
        telemetry_rounds(5, avail[:10])


@pytest.fixture(scope="module")
def lap_tables(spark: SparkSession, sample_clean_lake: SampleLake) -> dict[str, DataFrame]:
    return P.lap_sources(spark, sample_clean_lake.clean_root, [2023, 2024], 2)


def test_partitioning_queries_give_identical_results_across_layouts_and_variants(
    spark: SparkSession, lap_tables: dict[str, DataFrame], tmp_path: Path
) -> None:
    dataset = P.laps_dataset()
    fs_root = bench_file_uri("r1")
    counts = P.prepare(spark, dataset, lap_tables, fs_root)
    assert counts["laps"] == lap_tables["laps"].count()
    part = tmp_path / "data" / "bench" / "r1" / "partitioning" / "laps"
    assert (part / "partitioned" / "laps" / "season=2124").is_dir()
    assert not any(
        p.name.startswith("season=") for p in (part / "unpartitioned" / "laps").iterdir()
    )
    sums: dict[str, set[str]] = {}
    for layout in P.LAYOUTS:
        for variant in P.VARIANTS:
            tables = P.apply_variant(P.load(spark, dataset, fs_root, layout), variant)
            for q in dataset.queries:
                rows = q.run(tables).collect()
                assert rows, f"{q.name} returned nothing"
                sums.setdefault(q.name, set()).add(P.result_checksum(rows))
            spark.catalog.clearCache()
    assert {k: len(v) for k, v in sums.items()} == {q.name: 1 for q in dataset.queries}
    with pytest.raises(ValueError):
        P.apply_variant({}, "nope")
    with pytest.raises(ValueError):
        P.write_layout(lap_tables["laps"], fs_root + "/x", "sideways", 1)
    with pytest.raises(BenchPathError):
        P.write_layout(lap_tables["laps"], f"file://{tmp_path}/data/lake/x", "partitioned", 1)


def test_run_dataset_emits_one_row_per_layout_variant_query(
    spark: SparkSession, lap_tables: dict[str, DataFrame], monkeypatch: pytest.MonkeyPatch
) -> None:
    from f1sa.bench import spark_metrics
    from f1sa.bench.spark_metrics import StageTotals

    class Fake:
        def totals_for_group(self, group: str) -> StageTotals:
            return StageTotals(10, 0, 5, 5, 1, 1, True)

    monkeypatch.setattr(
        spark_metrics.RestMetrics, "for_session", classmethod(lambda cls, s: Fake())
    )
    dataset = P.laps_dataset()
    fs_root = bench_file_uri("r2")
    counts = P.prepare(spark, dataset, lap_tables, fs_root)
    rows: list[dict[str, object]] = []
    groups: list[int] = []

    def emit_group(group: list[dict[str, object]]) -> None:
        groups.append(len(group))
        rows.extend(group)

    P.run_dataset(
        spark, dataset, "file", fs_root, counts, warmup=0, repeats=1, emit_group=emit_group
    )
    assert groups == [len(dataset.queries)] * len(P.LAYOUTS) * len(P.VARIANTS), "whole groups"
    resumed: list[dict[str, object]] = []
    done = {("laps", "file", "unpartitioned", "none"), ("laps", "file", "partitioned", "cache")}
    P.run_dataset(
        spark,
        dataset,
        "file",
        fs_root,
        counts,
        warmup=0,
        repeats=1,
        emit_group=resumed.extend,
        done=done,
    )
    assert {P.group_key(r) for r in resumed} == {P.group_key(r) for r in rows} - done
    assert len(rows) == len(P.LAYOUTS) * len(P.VARIANTS) * len(dataset.queries)
    assert P.check_checksums(rows) == []
    assert {r["input_bytes"] for r in rows} == {10}
    with_speedups = P.speedups(rows)
    base = [r for r in with_speedups if (r["layout"], r["variant"]) == ("unpartitioned", "none")]
    assert all(r["speedup_vs_simplest"] == 1.0 for r in base)
    assert all(r["checksum_match"] == "True" for r in with_speedups)


def test_speedups_and_checksum_check_are_pure() -> None:
    def row(fs: str, layout: str, variant: str, med: float, cs: str = "a") -> dict[str, object]:
        return {
            "dataset": "laps",
            "query": "q",
            "fs": fs,
            "layout": layout,
            "variant": variant,
            "median_s": med,
            "result_checksum": cs,
        }

    rows = [
        row("file", "unpartitioned", "none", 4.0),
        row("file", "partitioned", "cache", 1.0),
        row("hdfs", "unpartitioned", "none", 8.0),
        row("hdfs", "partitioned", "none", 2.0, cs="b"),
    ]
    out = {(r["fs"], r["layout"], r["variant"]): r for r in P.speedups(rows)}
    assert out[("file", "partitioned", "cache")]["speedup_vs_simplest"] == 4.0
    assert out[("hdfs", "partitioned", "none")]["speedup_vs_unpartitioned_same_fs"] == 4.0
    assert out[("hdfs", "partitioned", "none")]["speedup_vs_simplest"] == 2.0
    assert out[("hdfs", "partitioned", "none")]["checksum_match"] == "False"
    assert P.check_checksums(rows) == ["laps/q"]
    est = P.estimate_partitioning(
        [row("file", "unpartitioned", "none", 1.0) | {"materialise_s": 0.0}],
        {"prep": {"laps/file": {"seconds": 2.0}}},
        laps_factor=16,
        telemetry_round_ratio=4.8,
    )
    assert est == pytest.approx(6 * 16 + 2 * 16)


def test_group_helpers_and_atomic_jsonl(tmp_path: Path) -> None:
    datasets = [P.laps_dataset(), P.telemetry_dataset()]
    expected = P.expected_groups(datasets, ["file", "hdfs"])
    assert len(expected) == 2 * 2 * len(P.LAYOUTS) * len(P.VARIANTS)
    full = [
        {
            "dataset": "laps",
            "fs": "file",
            "layout": "partitioned",
            "variant": "none",
            "query": q.name,
        }
        for q in P.laps_dataset().queries
    ]
    partial = [r | {"variant": "cache"} for r in full[:2]]
    assert P.done_groups(full + partial, datasets) == {("laps", "file", "partitioned", "none")}
    path = tmp_path / "p.jsonl"
    P.append_jsonl_atomic(path, full[:1])
    path.write_text(path.read_text() + '{"dataset": "la')  # a partly written last line
    P.append_jsonl_atomic(path, full[1:2])
    lines = path.read_text().splitlines()
    assert [json.loads(x)["query"] for x in lines] == [full[0]["query"], full[1]["query"]]


def test_prepare_or_reuse_uses_the_marker(
    spark: SparkSession, lap_tables: dict[str, DataFrame], tmp_path: Path
) -> None:
    from f1sa.bench.cell import PREP_MARKER, read_marker

    dataset = P.laps_dataset()
    fs_root = bench_file_uri("r3")
    counts, reused = P.prepare_or_reuse(spark, dataset, lap_tables, fs_root)
    assert not reused and counts["laps"] == lap_tables["laps"].count()
    root = tmp_path / "data" / "bench" / "r3" / "partitioning" / "laps"
    assert (root / PREP_MARKER).exists()
    assert read_marker(spark, f"{fs_root}/partitioning/laps") == {"rows": counts, "dataset": "laps"}
    stamp = (root / "partitioned" / "laps").stat().st_mtime_ns
    again, reused = P.prepare_or_reuse(spark, dataset, lap_tables, fs_root)
    assert reused and again == counts
    assert (root / "partitioned" / "laps").stat().st_mtime_ns == stamp, "not rewritten"
    (root / PREP_MARKER).unlink()  # an interrupted prepare: no marker
    (root / "partitioned" / "laps" / "stale.txt").write_text("partial")
    _, reused = P.prepare_or_reuse(spark, dataset, lap_tables, fs_root)
    assert not reused and not (root / "partitioned" / "laps" / "stale.txt").exists()


def test_cell_prep_reuses_a_complete_replicated_copy(
    spark: SparkSession, sample_clean_lake: SampleLake, tmp_path: Path
) -> None:
    from f1sa.bench import cell as C

    root = bench_file_uri("r4", "replicated", "s1_x2")
    spec = {
        "cell_id": "prep_s1_x2_local4",
        "clean_root": sample_clean_lake.clean_root,
        "seasons": [2024],
        "all_seasons": [2023, 2024],
        "factor": 2,
        "input_root": root,
    }
    first = C._prep(spark, spec)
    assert not first["reused"] and first["rows"]["laps"] > 0
    assert C.read_marker(spark, root) == {"rows": first["rows"], "cell_id": "prep_s1_x2_local4"}
    second = C._prep(spark, spec)
    assert second["reused"] and second["rows"] == first["rows"]
    assert second["laps_by_season"] == first["laps_by_season"]
    marker = tmp_path / "data" / "bench" / "r4" / "replicated" / "s1_x2" / C.PREP_MARKER
    marker.unlink()
    third = C._prep(spark, spec)
    assert not third["reused"] and third["rows"] == first["rows"]
    assert C.read_marker(spark, bench_file_uri("r4", "nothing")) is None
