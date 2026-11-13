"""Scalability grid: cells, guard decisions, subprocess cells, estimate, reduced grid (S10)."""

from __future__ import annotations

import json
import sys
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from f1sa.bench import grid as G
from f1sa.bench.cell import (
    CLUSTER_EXECUTOR_MEMORY,
    LOCAL_DRIVER_MEMORY,
    MASTERS,
    SHUFFLE_PARTITIONS,
    CellError,
    spark_conf,
)
from f1sa.bench.timing import GIB, MemoryGuard

REPO = Path(__file__).resolve().parents[2]


def test_default_grid_cells_and_order() -> None:
    cells = list(G.cells(G.GridSpec()))
    # per (seasons, factor): 1 prep + 4 build, plus 4 telemetry at x1
    assert len(cells) == 3 * (4 * 5 + 4)
    first = [c.cell_id for c in cells[:9]]
    assert first == [
        "prep_s1_x1_local4",
        "build_s1_x1_local1",
        "build_s1_x1_local2",
        "build_s1_x1_local4",
        "build_s1_x1_cluster",
        "telemetry_s1_x1_local1",
        "telemetry_s1_x1_local2",
        "telemetry_s1_x1_local4",
        "telemetry_s1_x1_cluster",
    ]
    assert all(c.factor == 1 for c in cells if c.phase == "telemetry")
    assert {(c.warmup, c.repeats) for c in cells if c.phase != "prep"} == {(1, 5)}


def test_large_factor_overrides_and_validation() -> None:
    spec = G.GridSpec(large_repeats=3, large_masters=("local4", "cluster"))
    big = [c for c in G.cells(spec) if c.phase == "build" and c.factor >= 16]
    assert {c.master for c in big} == {"local4", "cluster"} and {c.repeats for c in big} == {3}
    small = [c for c in G.cells(spec) if c.phase == "build" and c.factor == 4]
    assert len({c.master for c in small}) == 4 and {c.repeats for c in small} == {5}
    for bad in (
        {"seasons": (2,)},
        {"factors": (4, 1)},
        {"factors": (0,)},
        {"masters": ("local[1]",)},
        {"repeats": 0},
        {"large_masters": ("nope",)},
    ):
        with pytest.raises(ValueError):
            G.GridSpec(**bad)  # type: ignore[arg-type]
    args = spec.cli_args()
    assert args[:2] == ["--seasons", "1"] and "--large-masters" in args
    assert not any("[" in a for a in args), "no shell glob characters on the command line"


def test_spark_conf_per_master() -> None:
    local = spark_conf("local[2]")
    assert local["spark.driver.memory"] == LOCAL_DRIVER_MEMORY == "2g"
    assert local["spark.driver.host"] == "127.0.0.1" and "spark.executor.memory" not in local
    assert local["spark.sql.shuffle.partitions"] == str(SHUFFLE_PARTITIONS)
    assert local["spark.ui.enabled"] == "true" and local["spark.hadoop.dfs.replication"] == "1"
    cluster = spark_conf("spark://spark-master:7077", driver_host="172.18.0.7")
    assert cluster["spark.executor.memory"] == CLUSTER_EXECUTOR_MEMORY == "768m"
    assert cluster["spark.cores.max"] == "4" and cluster["spark.executor.cores"] == "2"
    assert cluster["spark.driver.host"] == "172.18.0.7"
    assert cluster["spark.driver.bindAddress"] == "0.0.0.0"
    assert set(MASTERS) == {"local1", "local2", "local4", "cluster"}
    with pytest.raises(CellError):
        spark_conf("yarn")


def test_cell_spec_paths_are_benchmark_paths() -> None:
    cell = G.Cell("build", 3, 16, "cluster", 1, 5)
    spec = G.cell_spec(cell, "bench_1", "/w/data/bench/bench_1/f1sa_src.zip")
    assert spec["input_root"] == "hdfs://namenode:8020/f1/bench/bench_1/replicated/s3_x16"
    assert spec["out_root"] == "hdfs://namenode:8020/f1/bench/bench_1/scal/build_s3_x16_cluster"
    assert spec["clean_root"] == "hdfs://namenode:8020/f1/clean", "read only"
    assert spec["seasons"] == [2022, 2023, 2024] and spec["pyfiles"]
    assert G.cell_spec(G.Cell("build", 1, 1, "local2", 1, 5), "bench_1", "z")["pyfiles"] is None


def test_pyfiles_zip_holds_the_current_package(tmp_path: Path) -> None:
    dest = G.make_pyfiles_zip(REPO / "src", tmp_path / "f1sa_src.zip")
    names = zipfile.ZipFile(dest).namelist()
    assert "f1sa/__init__.py" in names and "f1sa/bench/workloads.py" in names
    assert "f1sa/etl/sql/pit_loss_prior.sql" in names
    assert not any("__pycache__" in n or n.endswith(".pyc") for n in names)
    with pytest.raises(FileNotFoundError):
        G.make_pyfiles_zip(tmp_path, tmp_path / "x.zip")


LAPS = {2021: 25_000, 2022: 25_000, 2023: 25_000, 2024: 26_000, 2025: 26_000}


def smoke(build: float = 10.0) -> G.SmokeTimes:
    return G.SmokeTimes(
        startup_s={m: 5.0 for m in MASTERS},
        build_s={m: build for m in MASTERS},
        telemetry_s={m: 20.0 for m in MASTERS},
        prep_s=4.0,
        laps_by_season=LAPS,
    )


def test_rows_ratio_and_estimate_arithmetic() -> None:
    assert G.rows_ratio(1, 1, LAPS) == 1.0
    assert G.rows_ratio(1, 4, LAPS) == 4.0
    assert G.rows_ratio(3, 1, LAPS) == pytest.approx(76_000 / 26_000)
    spec = G.GridSpec(seasons=(1,), factors=(1, 4), masters=("local4",), warmup=1, repeats=2)
    est = G.estimate(spec, smoke())
    per = dict(est.per_cell)
    assert per["prep_s1_x1_local4"] == pytest.approx(4.0 + 5.0)
    assert per["build_s1_x1_local4"] == pytest.approx(5.0 + 3 * 10.0)
    assert per["telemetry_s1_x1_local4"] == pytest.approx(5.0 + 3 * 20.0)
    assert per["build_s1_x4_local4"] == pytest.approx(5.0 + 3 * 40.0)
    assert per["prep_s1_x4_local4"] == pytest.approx(16.0 + 5.0)
    assert est.total_s == pytest.approx(sum(per.values())) and est.missing == []
    partial = G.SmokeTimes({"local4": 5.0}, {"local4": 1.0}, {}, 1.0, LAPS)
    assert "telemetry_s1_x1_local4" in G.estimate(spec, partial).missing


def test_reduced_grid_only_when_over_budget() -> None:
    full = G.GridSpec()
    assert G.reduced_grid(full, smoke(build=0.01), budget_s=10**9) == []
    steps = G.reduced_grid(full, smoke(build=10.0), budget_s=G.BUDGET_S)
    assert steps, "a 10 s build per x1 run is over 8 h for the full grid"
    totals = [s[2] for s in steps]
    assert totals == sorted(totals, reverse=True)
    assert steps[0][0] == "3 timed runs at x16 and x64" and steps[0][1].large_repeats == 3


def test_smoke_times_from_records() -> None:
    records = [
        {
            "cell": {"phase": "prep", "master": "local4"},
            "result": {"status": "ok", "prep_s": 3.0, "laps_by_season": {"2024": 26000}},
        },
        {
            "cell": {"phase": "build", "master": "local2"},
            "result": {"status": "ok", "startup_s": 4.0, "warmup_s": [], "times_s": [12.0]},
        },
        {
            "cell": {"phase": "telemetry", "master": "local2"},
            "result": {"status": "ok", "startup_s": 4.0, "warmup_s": [], "times_s": [30.0]},
        },
        {"cell": {"phase": "build", "master": "cluster"}, "result": {"status": "failed"}},
    ]
    s = G.smoke_times(records)
    assert s.prep_s == 3.0 and s.laps_by_season == {2024: 26000}
    assert s.build_s == {"local2": 12.0} and s.telemetry_s == {"local2": 30.0}
    with pytest.raises(ValueError, match="prep"):
        G.smoke_times(records[1:])


class FakeRunner:
    """Returns canned outcomes per cell id; records which cells ran."""

    def __init__(
        self, peaks: dict[str, int], fail: Sequence[str] = (), aborted: Sequence[str] = ()
    ) -> None:
        self.peaks = peaks
        self.fail = set(fail)
        self.aborted = set(aborted)
        self.ran: list[str] = []

    def __call__(self, argv: Sequence[str], result_path: Path, guard: MemoryGuard) -> G.CellOutcome:
        cell_id = result_path.name.removesuffix(".result.json")
        self.ran.append(cell_id)
        status = "failed" if cell_id in self.fail else "ok"
        if cell_id in self.aborted:
            status = "aborted_memory_guard"
        res: dict[str, Any] = {"status": status, "error": "boom" if status != "ok" else ""}
        peak = self.peaks.get(cell_id, GIB)
        return G.CellOutcome(status, res["error"], res, {"peak_cgroup_bytes": peak}, 1.0, 2.0)


def test_run_grid_skips_larger_factors_after_a_guard_kill(tmp_path: Path) -> None:
    spec = G.GridSpec(
        seasons=(1,), factors=(1, 4, 16), masters=("local2", "local4"), warmup=0, repeats=1
    )
    runner = FakeRunner({}, aborted=["build_s1_x4_local2"])
    guard = MemoryGuard(limit_bytes=4 * GIB)
    not_ok = G.run_grid(
        spec, tmp_path / "r", "r", guard=guard, src=REPO / "src", runner=runner, out=lambda _m: None
    )
    records = {r["cell_id"]: r for r in G.read_records(tmp_path / "r" / "cells.jsonl")}
    assert records["build_s1_x4_local2"]["status"] == "aborted_memory_guard"
    skipped = records["build_s1_x16_local2"]
    assert skipped["status"] == "skipped_memory_guard"
    assert skipped["reason"] == "x4 of the same seasons and master ended aborted_memory_guard"
    assert records["build_s1_x16_local4"]["status"] == "ok", "other masters are unaffected"
    assert "build_s1_x16_local2" not in runner.ran and "prep_s1_x16_local4" in runner.ran
    assert not_ok == 2
    assert json.loads((tmp_path / "r" / "grid_spec.json").read_text())["factors"] == [1, 4, 16]
    assert not (tmp_path / "r" / "f1sa_src.zip").exists(), "no cluster cell, no zip"


def test_run_grid_skips_cells_whose_prep_failed(tmp_path: Path) -> None:
    spec = G.GridSpec(
        seasons=(1,), factors=(1,), masters=("local2", "cluster"), warmup=0, repeats=1
    )
    runner = FakeRunner({}, fail=["prep_s1_x1_local4"])
    not_ok = G.run_grid(
        spec,
        tmp_path / "r",
        "r",
        guard=MemoryGuard(4 * GIB),
        src=REPO / "src",
        runner=runner,
        out=lambda _m: None,
    )
    statuses = [r["status"] for r in G.read_records(tmp_path / "r" / "cells.jsonl")]
    assert statuses == ["failed"] + ["skipped_no_input"] * 4
    assert not_ok == 5 and runner.ran == ["prep_s1_x1_local4"]
    assert (tmp_path / "r" / "f1sa_src.zip").exists()


CHILD_OK = (
    "import json, sys\n"
    "out = sys.argv[sys.argv.index('--out') + 1]\n"
    "json.dump({'status': 'ok', 'error': '', 'times_s': [0.1]}, open(out, 'w'))\n"
)
CHILD_HOG = (
    "import time\n"
    "block = bytearray(64 * 1024 * 1024)\n"
    "for i in range(0, len(block), 4096): block[i] = 1\n"
    "time.sleep(60)\n"
)


def test_subprocess_cell_ok(tmp_path: Path) -> None:
    # Fixed memory readings (a quiet cgroup, plenty of host memory): the outcome must not depend
    # on how much memory the machine running the tests happens to use.
    cg = tmp_path / "cgroup"
    cg.mkdir()
    (cg / "memory.current").write_text(str(GIB // 4))
    (cg / "memory.stat").write_text("inactive_file 0\n")
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(f"MemTotal: {32 * GIB // 1024} kB\nMemAvailable: {16 * GIB // 1024} kB\n")
    result = tmp_path / "c.result.json"
    outcome = G.run_subprocess_cell(
        [sys.executable, "-c", CHILD_OK, "--out", str(result)],
        result,
        MemoryGuard(4 * GIB),
        interval_s=0.05,
        sampler_kwargs={"cgroup_root": cg, "meminfo": meminfo},
    )
    assert outcome.status == "ok" and outcome.result == {
        "status": "ok",
        "error": "",
        "times_s": [0.1],
    }
    assert outcome.ended_at >= outcome.started_at


def test_subprocess_cell_is_killed_by_the_runtime_guard(tmp_path: Path) -> None:
    cg = tmp_path / "cgroup"
    cg.mkdir()
    (cg / "memory.current").write_text(str(3 * GIB))  # above 0.5 x 4 GiB
    (cg / "memory.stat").write_text("inactive_file 0\n")
    result = tmp_path / "c.result.json"
    outcome = G.run_subprocess_cell(
        [sys.executable, "-c", CHILD_HOG],
        result,
        MemoryGuard(4 * GIB, fraction=0.5),
        interval_s=0.05,
        sampler_kwargs={"cgroup_root": cg},
    )
    assert outcome.status == "aborted_memory_guard" and "container usage 3.00 GiB" in outcome.reason
    assert outcome.ended_at - outcome.started_at < 30, "killed, not waited for"


def test_subprocess_cell_timeout_and_missing_result(tmp_path: Path) -> None:
    result = tmp_path / "c.result.json"
    timed_out = G.run_subprocess_cell(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        result,
        MemoryGuard(64 * GIB, host_reserve_bytes=0),
        interval_s=0.05,
        timeout_s=0.5,
    )
    assert timed_out.status == "timeout"
    no_result = G.run_subprocess_cell(
        [sys.executable, "-c", "raise SystemExit(3)"],
        result,
        MemoryGuard(64 * GIB, host_reserve_bytes=0),
        interval_s=0.05,
    )
    assert no_result.status == "failed" and "exit 3" in no_result.reason


def test_growth_affine_fit_through_x1_and_x4() -> None:
    # 40 s at x1, 52 s at x4: 4 s per unit of rows, 36 s fixed
    assert G.growth(40.0, 52.0, 1.0) == pytest.approx(1.0)
    assert G.growth(40.0, 52.0, 4.0) == pytest.approx(1.3)
    assert G.growth(40.0, 52.0, 64.0) == pytest.approx((36 + 4 * 64) / 40)
    assert G.growth(40.0, None, 16.0) == 16.0, "linear without a second point"
    assert G.growth(40.0, 30.0, 64.0) == 1.0, "a faster x4 is treated as no growth"


def test_estimate_uses_the_x4_point_when_present() -> None:
    spec = G.GridSpec(seasons=(1,), factors=(16,), masters=("local2",), warmup=0, repeats=1)
    base = smoke(build=40.0)
    lin = G.estimate(spec, base)
    fitted = G.estimate(
        spec,
        G.SmokeTimes(base.startup_s, base.build_s, base.telemetry_s, 4.0, LAPS, 52.0, 5.0),
    )
    per_lin, per_fit = dict(lin.per_cell), dict(fitted.per_cell)
    assert lin.method == "linear" and fitted.method == "affine x1/x4 fit"
    assert per_lin["build_s1_x16_local2"] == pytest.approx(5.0 + 40.0 * 16)
    assert per_fit["build_s1_x16_local2"] == pytest.approx(5.0 + 40.0 * (36 + 4 * 16) / 40)
    assert per_fit["prep_s1_x16_local4"] == pytest.approx(5.0 + (11 / 3 + 16 / 3))


def test_smoke_times_pick_up_the_x4_points() -> None:
    def rec(phase: str, master: str, factor: int, **res: Any) -> dict[str, Any]:
        return {
            "cell": {"phase": phase, "master": master, "factor": factor, "n_seasons": 1},
            "result": {"status": "ok", "startup_s": 1.0, "warmup_s": [], **res},
        }

    laps = {"2024": 26000}
    s = G.smoke_times(
        [
            rec("prep", "local4", 1, prep_s=3.0, laps_by_season=laps),
            rec("prep", "local4", 4, prep_s=6.0, laps_by_season=laps),
            rec("build", "local4", 1, times_s=[40.0]),
            rec("build", "local4", 4, times_s=[52.0]),
        ]
    )
    assert (s.prep_s, s.prep_x4_s, s.build_s, s.build_x4_s) == (3.0, 6.0, {"local4": 40.0}, 52.0)


def test_large_seasons_limit_the_big_factors_to_some_season_sets() -> None:
    spec = G.GridSpec(factors=(1, 4, 16), large_seasons=(1,), large_masters=("local4",))
    ids = {c.cell_id for c in G.cells(spec)}
    assert "prep_s1_x16_local4" in ids and "build_s1_x16_local4" in ids
    assert not any("x16" in i for i in ids if "_s3_" in i or "_s5_" in i)
    assert "build_s5_x4_local1" in ids, "x4 is below large_from and runs everywhere"
    assert spec.cli_args()[-2:] == ["--large-seasons", "1"]
    with pytest.raises(ValueError, match="subset"):
        G.GridSpec(seasons=(1, 3), large_seasons=(5,))
    parser = __import__("argparse").ArgumentParser()
    G.add_grid_args(parser)
    assert G.spec_from_args(parser.parse_args(spec.cli_args())) == spec
