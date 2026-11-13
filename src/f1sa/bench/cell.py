"""One benchmark cell in its own process (a fresh JVM).

    python -m f1sa.bench.cell --spec S --out O

The spec (JSON) names a phase:

* ``prep``: replicate the clean tables of the cell's seasons ``factor`` times into the benchmark
  root (:func:`f1sa.bench.replicate.materialise`, untimed in the grid) and count the clean laps per
  season (used by the time estimate);
* ``build``: the feature build (:func:`f1sa.bench.workloads.feature_build`) on the replicated copy;
* ``telemetry``: the heavy telemetry aggregation
  (:func:`f1sa.bench.workloads.telemetry_aggregation`).

``build`` and ``telemetry`` run ``warmup`` discarded runs and ``repeats`` timed runs, each in its
own Spark job group, with the REST metrics per run. The result (JSON) is written to ``--out``; the
grid process adds the resource peaks and the guard decision.

Spark configuration per master (docs/experiments.md, "Memory"):

* ``local[N]``: driver heap 2g (the same for every local cell), ``MALLOC_ARENA_MAX=2``, driver
  bound to 127.0.0.1;
* ``spark://spark-master:7077``: client-mode driver 1g in the app container (reachable on its
  container IP), one executor per worker with 768m and 2 cores, 4 cores in total. The current
  ``src/f1sa`` is shipped to the executors as a zip (``addPyFile``), because the worker image holds
  the ``f1sa`` of its build; the cell checks which copy the executors import.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import traceback
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from f1sa.bench.paths import check_bench_uri
from f1sa.bench.replicate import load_seasons, materialise, read_replicated
from f1sa.bench.spark_metrics import UI_CONF, RestMetrics, StageTotals
from f1sa.bench.timing import run_timed
from f1sa.bench.workloads import (
    TELEMETRY_SEASON,
    available_rounds,
    feature_build,
    telemetry_aggregation,
    telemetry_rounds,
)
from f1sa.etl.clean import write_text
from f1sa.etl.spark import RUNTIME_CONF, hadoop_config

MASTERS: dict[str, str] = {
    "local1": "local[1]",
    "local2": "local[2]",
    "local4": "local[4]",
    "cluster": "spark://spark-master:7077",
}
"""CLI names (no shell glob characters) -> Spark master URLs."""
LOCAL_DRIVER_MEMORY = "2g"
"""Heap of every local[N] driver. The JVM adds about 0.9 GB off-heap (measured: RSS 3.4 GB at a
2560m heap, which hit the 90 % guard of the 4 GiB container at 1 season, x1)."""
MALLOC_ARENA_MAX = "2"
"""glibc malloc arenas for the driver JVM (default 8 x cores), to bound its off-heap growth."""
CLUSTER_DRIVER_MEMORY = "1g"
CLUSTER_EXECUTOR_MEMORY = "768m"
CLUSTER_EXECUTOR_CORES = 2
CLUSTER_CORES_MAX = 4
SHUFFLE_PARTITIONS = 8
"""2 x the most cores any master has, so every master can keep all its cores busy."""
PHASES = ("prep", "build", "telemetry")


class CellError(RuntimeError):
    """The cell cannot run as specified."""


def spark_conf(master: str, *, driver_host: str | None = None) -> dict[str, str]:
    """The Spark settings of a bench session on ``master``."""
    conf: dict[str, str] = {
        "spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS),
        **UI_CONF,
        **RUNTIME_CONF,
        **hadoop_config(),
    }
    if master.startswith("local["):
        conf |= {
            "spark.driver.memory": LOCAL_DRIVER_MEMORY,
            "spark.driver.host": "127.0.0.1",
            "spark.driver.bindAddress": "127.0.0.1",
        }
    elif master.startswith("spark://"):
        conf |= {
            "spark.driver.memory": CLUSTER_DRIVER_MEMORY,
            "spark.executor.memory": CLUSTER_EXECUTOR_MEMORY,
            "spark.executor.cores": str(CLUSTER_EXECUTOR_CORES),
            "spark.cores.max": str(CLUSTER_CORES_MAX),
            "spark.driver.host": driver_host or socket.gethostbyname(socket.gethostname()),
            "spark.driver.bindAddress": "0.0.0.0",
        }
    else:
        raise CellError(f"unsupported master {master!r}")
    return conf


def build_session(master: str, app: str, conf: Mapping[str, str]) -> SparkSession:
    builder = SparkSession.builder.master(master).appName(app)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def executor_f1sa_path(spark: SparkSession) -> str:
    """Where the executors import ``f1sa`` from (a one-task job)."""

    def where(_: object) -> str:
        import f1sa

        return str(f1sa.__file__)

    paths = spark.sparkContext.parallelize([0], 1).map(where).collect()
    return paths[0]


def delete_uri(spark: SparkSession, uri: str) -> None:
    """Recursive delete of a benchmark path (refused outside the benchmark roots)."""
    uri = check_bench_uri(uri)
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise CellError("Spark JVM gateway unavailable")
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if fs.exists(path):
        fs.delete(path, True)


PREP_MARKER = "_BENCH_PREP.json"
"""Written last by a prep (replication or layout copy), so its presence means the copy is complete
and a resumed run may reuse it."""


def _fs_path(spark: SparkSession, uri: str) -> tuple[Any, Any]:
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise CellError("Spark JVM gateway unavailable")
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), path


def read_marker(spark: SparkSession, root: str) -> dict[str, Any] | None:
    """The JSON of ``root/PREP_MARKER``, or None if the copy under ``root`` is not complete."""
    uri = check_bench_uri(f"{root.rstrip('/')}/{PREP_MARKER}")
    fs, path = _fs_path(spark, uri)
    if not fs.exists(path):
        return None
    stream = fs.open(path)
    try:
        reader = spark.sparkContext._jvm.java.io.BufferedReader(  # type: ignore[union-attr]
            spark.sparkContext._jvm.java.io.InputStreamReader(stream, "UTF-8")  # type: ignore[union-attr]
        )
        lines: list[str] = []
        while (line := reader.readLine()) is not None:
            lines.append(str(line))
    finally:
        stream.close()
    try:
        obj = json.loads("\n".join(lines))
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def write_marker(spark: SparkSession, root: str, payload: Mapping[str, Any]) -> None:
    """Write ``root/PREP_MARKER`` (last step of a prep)."""
    uri = check_bench_uri(f"{root.rstrip('/')}/{PREP_MARKER}")
    write_text(spark, uri, json.dumps(dict(payload), sort_keys=True))


def _metrics(rest: RestMetrics, label: str) -> dict[str, Any]:
    totals: StageTotals = rest.totals_for_group(label)
    return asdict(totals)


def run_cell(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Run the cell ``spec`` and return its result (never raises for a failed workload)."""
    phase = str(spec["phase"])
    if phase not in PHASES:
        raise CellError(f"unknown phase {phase!r}")
    master = MASTERS[str(spec["master"])]
    conf = spark_conf(master)
    result: dict[str, Any] = {"status": "ok", "error": "", "conf": conf, "master_url": master}
    os.environ["MALLOC_ARENA_MAX"] = MALLOC_ARENA_MAX  # inherited by the driver JVM
    t0 = time.perf_counter()
    spark = build_session(master, f"f1sa-bench-{spec['cell_id']}", conf)
    result["startup_s"] = time.perf_counter() - t0
    try:
        if spec.get("pyfiles"):
            spark.sparkContext.addPyFile(str(spec["pyfiles"]))
        if master.startswith("spark://"):
            where = executor_f1sa_path(spark)
            result["executor_f1sa_path"] = where
            if ".zip" not in where:
                raise CellError(f"executors import a stale f1sa from {where}, not the shipped zip")
        if phase == "prep":
            result |= _prep(spark, spec)
        else:
            result |= _timed(spark, spec, phase)
    except Exception as exc:  # recorded per cell; the grid goes on with the next cell
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc(limit=20)
    finally:
        spark.stop()
    return result


def _prep(spark: SparkSession, spec: Mapping[str, Any]) -> dict[str, Any]:
    """Replicate into ``input_root``, or reuse a complete copy there (a resumed run). A copy without
    its marker (an interrupted prep) is deleted and prepared again."""
    root = str(spec["input_root"])
    t0 = time.perf_counter()
    marker = read_marker(spark, root)
    if marker is not None and isinstance(marker.get("rows"), dict):
        counts = {str(k): int(v) for k, v in marker["rows"].items()}
        reused = True
    else:
        delete_uri(spark, root)
        counts = materialise(
            spark,
            str(spec["clean_root"]),
            [int(s) for s in spec["seasons"]],
            int(spec["factor"]),
            root,
        )
        write_marker(spark, root, {"rows": counts, "cell_id": spec["cell_id"]})
        reused = False
    elapsed = time.perf_counter() - t0
    laps = load_seasons(spark, str(spec["clean_root"]), [int(s) for s in spec["all_seasons"]])
    by_season = {
        str(r["season"]): int(r["n"])
        for r in laps["laps"].groupBy("season").agg(F.count(F.lit(1)).alias("n")).collect()
    }
    return {"prep_s": elapsed, "rows": counts, "laps_by_season": by_season, "reused": reused}


def _timed(spark: SparkSession, spec: Mapping[str, Any], phase: str) -> dict[str, Any]:
    rest = RestMetrics.for_session(spark)
    clean = read_replicated(spark, str(spec["input_root"]))
    laps_rows = clean["laps"].count()
    out_root = check_bench_uri(str(spec["out_root"]))
    delete_uri(spark, out_root)  # leftovers of an interrupted attempt of this cell
    sc = spark.sparkContext
    extra: dict[str, Any] = {"laps_rows": laps_rows}
    if phase == "telemetry":
        avail = available_rounds(spark, str(spec["telemetry_root"]), TELEMETRY_SEASON)
        rounds = telemetry_rounds(int(spec["n_seasons"]), avail)
        extra["rounds"] = rounds

        def job(label: str) -> dict[str, Any]:
            sc.setJobGroup(label, label)
            telemetry_aggregation(spark, str(spec["telemetry_root"]), clean, rounds)
            return _metrics(rest, label)

    else:

        def job(label: str) -> dict[str, Any]:
            sc.setJobGroup(label, label)
            feature_build(spark, clean, f"{out_root}/{label}")
            return _metrics(rest, label)

    runs = run_timed(
        job,
        warmup=int(spec["warmup"]),
        repeats=int(spec["repeats"]),
        after=lambda label: delete_uri(spark, f"{out_root}/{label}"),
    )
    delete_uri(spark, out_root)
    return extra | {
        "warmup_s": runs.warmup_s,
        "times_s": runs.times_s,
        "metrics": runs.results,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="f1sa-bench-cell", description=__doc__.splitlines()[0])
    p.add_argument("--spec", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args(argv)
    spec = json.loads(args.spec.read_text(encoding="utf-8"))
    result = run_cell(spec)
    tmp = args.out.with_suffix(args.out.suffix + ".tmp")
    tmp.write_text(json.dumps(result, indent=1, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(args.out)
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
