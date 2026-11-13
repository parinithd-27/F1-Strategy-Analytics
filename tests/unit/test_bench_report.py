"""CSV rows, plots, fingerprint and the host entry points (Step 10). No test writes the repo's
results/: record runs use a tmp repo."""

from __future__ import annotations

import csv
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from f1sa.bench import grid as G
from f1sa.bench import host as H
from f1sa.bench import report as R
from f1sa.bench.resources import ContainerSample
from f1sa.bench.timing import MemoryGuard

REPO_SRC = Path(__file__).resolve().parents[2] / "src"
NOW = datetime(2026, 11, 13, 22, 0, 0, tzinfo=UTC)


def _record(
    cell_id: str, phase: str, master: str, factor: int, times: list[float]
) -> dict[str, Any]:
    return {
        "run_id": "bench_x",
        "cell_id": cell_id,
        "cell": {"phase": phase, "n_seasons": 1, "factor": factor, "master": master},
        "status": "ok",
        "reason": "",
        "started_at": 100.0,
        "ended_at": 200.0,
        "guard": {"ok": True, "reason": ""},
        "peaks": {
            "peak_rss_bytes": 10,
            "peak_cpu_pct": 90.0,
            "mean_cpu_pct": 50.0,
            "peak_cgroup_bytes": 20,
            "min_host_available_bytes": 30,
        },
        "result": {
            "status": "ok",
            "master_url": "local[2]",
            "startup_s": 3.0,
            "laps_rows": 26000 * factor,
            "warmup_s": [9.0],
            "times_s": times,
            "metrics": [
                {
                    "input_bytes": 100 + i,
                    "output_bytes": 1,
                    "shuffle_read_bytes": 5,
                    "shuffle_write_bytes": 5,
                    "complete": True,
                }
                for i in range(len(times))
            ],
            "conf": {"spark.driver.memory": "2560m", "spark.sql.shuffle.partitions": "8"},
        },
    }


SAMPLES = [
    ContainerSample(150.0, "f1sa-app-1", 2000, 4000, 120.0),
    ContainerSample(150.0, "f1sa-spark-worker-1-1", 500, 1500, 10.0),
    ContainerSample(160.0, "f1sa-app-1", 3000, 4000, 300.0),
    ContainerSample(500.0, "f1sa-app-1", 9999, 4000, 999.0),
]


def test_container_key() -> None:
    assert R.container_key("f1sa-spark-worker-1-1") == "spark_worker_1"
    assert R.container_key("f1sa-app-1") == "app"
    assert R.container_key("f1sa-spark-master-1") == "spark_master"


def test_scalability_row_combines_cell_result_and_docker_window() -> None:
    row = R.scalability_row(
        _record("build_s1_x1_local2", "build", "local2", 1, [1.0, 2.0, 3.0, 4.0, 5.0]),
        SAMPLES,
        git_sha="abc",
        git_dirty="false",
    )
    assert (row["median_s"], row["q1_s"], row["q3_s"], row["iqr_s"]) == (3.0, 2.0, 4.0, 2.0)
    assert (row["n_runs"], row["warmup_runs"], row["min_s"], row["max_s"]) == (5, 1, 1.0, 5.0)
    assert row["input_bytes"] == 102 and row["metrics_complete"] == "True"
    assert row["peak_mem_app_bytes"] == 3000, "the sample outside the window is ignored"
    assert row["peak_cpu_app_pct"] == 300.0 and row["peak_mem_spark_worker_1_bytes"] == 500
    assert row["peak_all_containers_mem_bytes"] == 3000
    assert row["peak_mem_namenode_bytes"] == "" and row["driver_memory"] == "2560m"
    assert row["started_at_utc"] == "1970-01-01T00:01:40Z" and row["git_sha"] == "abc"
    skipped = {
        "run_id": "r",
        "cell_id": "build_s5_x64_local1",
        "cell": {"phase": "build", "n_seasons": 5, "factor": 64, "master": "local1"},
        "status": "skipped_memory_guard",
        "reason": "x16 ended aborted_memory_guard",
    }
    srow = R.scalability_row(skipped, SAMPLES, git_sha="abc", git_dirty="false")
    assert srow["status"] == "skipped_memory_guard" and srow["n_runs"] == 0
    assert "median_s" not in srow
    assert srow["reason"] == "x16 ended aborted_memory_guard"


def test_write_csv_and_plots(tmp_path: Path) -> None:
    rows = [
        R.scalability_row(
            _record(f"build_s1_x{f}_{m}", "build", m, f, [f * 1.0, f * 1.1]),
            SAMPLES,
            git_sha="a",
            git_dirty="false",
        )
        for f in (1, 4, 16)
        for m in ("local1", "local4", "cluster")
    ]
    R.write_csv(tmp_path / "s.csv", rows, R.SCALABILITY_COLUMNS)
    back = list(csv.DictReader((tmp_path / "s.csv").open()))
    assert len(back) == 9 and list(back[0]) == list(R.SCALABILITY_COLUMNS)
    assert R.plot_scaling(
        rows, tmp_path / "b.png", phase="build", x_col="laps_rows", x_label="rows"
    )
    assert (tmp_path / "b.png").stat().st_size > 1000
    assert not R.plot_scaling(rows, tmp_path / "t.png", phase="telemetry", x_col="x", x_label="x")
    prow = [
        {
            "dataset": "laps",
            "query": "q",
            "fs": fs,
            "layout": lay,
            "variant": v,
            "speedup_vs_simplest": 1.5,
        }
        for fs in ("file", "hdfs")
        for lay in ("unpartitioned", "partitioned")
        for v in ("none", "cache", "persist_disk")
    ]
    assert R.plot_partitioning(prow, tmp_path / "p.png") and (tmp_path / "p.png").exists()
    assert not R.plot_partitioning([], tmp_path / "none.png")


def test_fingerprint_and_readme_append(tmp_path: Path) -> None:
    def run(cmd: Sequence[str]) -> str:
        return "4294967296" if cmd[:2] == ["docker", "inspect"] else "cid1"

    pairs = R.fingerprint(now=NOW, git_sha="abc", git_dirty="true", params={"a": 1}, run=run)
    d = dict(pairs)
    assert d["measured_at_utc"] == "2026-11-13T22:00:00Z" and d["git_sha"] == "abc"
    assert "app=4294967296" in d["container_mem_limits_bytes"] and d["params"] == "a=1"
    readme = tmp_path / "README.md"
    readme.write_text("# Results\n\nexisting\n")
    R.append_readme(readme, "scalability.csv", pairs)
    text = readme.read_text()
    assert text.startswith("# Results\n\nexisting\n")
    assert "## `scalability.csv` (2026-11-13T22:00:00Z)" in text and "| git_sha | abc |" in text


class NoDocker:
    def __init__(self, *_a: Any, **_k: Any) -> None:
        pass

    def __enter__(self) -> NoDocker:
        return self

    def __exit__(self, *_e: object) -> None:
        return None


@pytest.fixture
def fake_host(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(H, "DockerStatsSampler", NoDocker)
    monkeypatch.setattr(
        H, "fingerprint", lambda **kw: R.fingerprint(**kw, run=lambda _c: "unavailable")
    )
    monkeypatch.setattr(H, "git_info", lambda: ("abc", "false"))
    return tmp_path


def fake_cell_result(cell_id: str, *, fast: bool = False) -> dict[str, Any]:
    """What a real cell would return, by phase (no Spark)."""
    phase, _s, factor, master = cell_id.split("_", 3)
    f = int(factor.removeprefix("x"))
    if phase == "prep":
        return {
            "status": "ok",
            "error": "",
            "prep_s": 30.0 * f,
            "startup_s": 5.0,
            "rows": {"laps": 26000 * f},
            "laps_by_season": {str(s): 25000 for s in range(2021, 2026)},
        }
    t = (60.0 if f == 1 else 120.0) if not fast else 0.1
    return {
        "status": "ok",
        "error": "",
        "startup_s": 5.0,
        "laps_rows": 26000 * f,
        "warmup_s": [],
        "times_s": [t],
        "metrics": [],
        "master_url": master,
        **({"rounds": [1, 2, 3, 4, 5]} if phase == "telemetry" else {}),
    }


class FakeContainer:
    """Stands in for ``docker compose exec app python -m f1sa.bench.grid``: runs the real
    ``run_grid`` in-process on the host path, with instant fake cells. ``stop_after`` cells it
    "loses the container" (stops, exit 137)."""

    def __init__(self, repo: Path, stop_after: int | None = None) -> None:
        self.repo = repo
        self.stop_after = stop_after
        self.cmds: list[list[str]] = []
        self.ran: list[str] = []

    def __call__(self, cmd: Sequence[str]) -> int:
        assert cmd[:4] == ["docker", "compose", "exec", "-T"]
        self.cmds.append(list(cmd))
        i = cmd.index("f1sa.bench.grid")
        args = G.build_parser().parse_args(
            [a.replace("/workspace", str(self.repo), 1) for a in cmd[i + 1 :]]
        )
        env = dict(e.split("=", 1) for e in cmd[cmd.index("-T") + 1 : cmd.index("app")] if "=" in e)

        class Stop(BaseException):
            pass

        def cell(argv: Sequence[str], result_path: Path, guard: Any) -> G.CellOutcome:
            cid = result_path.name.removesuffix(".result.json")
            if self.stop_after is not None and len(self.ran) >= self.stop_after:
                raise Stop
            self.ran.append(cid)
            return G.CellOutcome("ok", "", fake_cell_result(cid), {}, 1.0, 2.0)

        try:
            G.run_grid(
                G.spec_from_args(args),
                args.run_dir,
                args.run_dir.name,
                guard=MemoryGuard(64 * 2**30, host_reserve_bytes=0),
                src=REPO_SRC,
                runner=cell,
                resume=args.resume,
                out=lambda _m: None,
                git_sha=env.get("F1SA_GIT_SHA", ""),
                git_dirty=env.get("F1SA_GIT_DIRTY", ""),
                docker_wait_s=0.0,
            )
        except Stop:
            return 137
        return 0


SMALL = ["--seasons", "1", "--factors", "1", "--masters", "local1", "local2"]


def test_scalability_default_writes_to_data_bench_only(fake_host: Path) -> None:
    lines: list[str] = []
    box = FakeContainer(fake_host)
    code = H.main_scalability(
        ["--keep-data", "--repo", str(fake_host), *SMALL], runner=box, out=lines.append, now=NOW
    )
    assert code == 0
    rdir = fake_host / "data" / "bench" / "bench_20261113T220000Z"
    rows = list(csv.DictReader((rdir / "scalability.csv").open()))
    assert [r["cell_id"] for r in rows] == [
        c.cell_id
        for c in G.cells(G.spec_from_args(G.build_parser().parse_args(["--run-dir", "x", *SMALL])))
    ]
    assert {r["git_sha"] for r in rows} == {"abc"}, "the host's git sha reaches the container"
    assert (rdir / "scalability_build.png").exists()
    assert not (fake_host / "results").exists(), "results/ only with --record"
    assert "/workspace/data/bench/bench_20261113T220000Z" in box.cmds[0]
    assert "F1SA_GIT_SHA=abc" in box.cmds[0]
    assert (
        lines[1]
        == "scalability: to resume this run: make experiments-spark RESUME=bench_20261113T220000Z"
    )


def test_record_waits_for_a_complete_grid_then_resume_writes_results(fake_host: Path) -> None:
    lines: list[str] = []
    rid = "bench_20261113T230000Z"
    first = FakeContainer(fake_host, stop_after=2)
    code = H.main_scalability(
        ["--record", "--run-id", rid, "--repo", str(fake_host), *SMALL],
        runner=first,
        out=lines.append,
        now=NOW,
    )
    assert code == 1
    assert (
        lines[1] == f"scalability: to resume this run: make experiments-spark RECORD=1 RESUME={rid}"
    )
    assert "incomplete" in lines[-1] and f"RECORD=1 RESUME={rid}" in lines[-1]
    assert not (fake_host / "results").exists(), "no results/ for an incomplete grid"
    rdir = fake_host / "data" / "bench" / rid
    assert len(list(csv.DictReader((rdir / "scalability.csv").open()))) == 2

    second = FakeContainer(fake_host)
    code = H.main_scalability(
        ["--record", "--resume", "--run-id", rid, "--keep-data", "--repo", str(fake_host)],
        runner=second,
        out=lines.append,
        now=NOW,
    )
    assert code == 0 and "--resume" in second.cmds[0]
    ids = [r["cell_id"] for r in csv.DictReader((fake_host / "results" / "scalability.csv").open())]
    assert len(ids) == len(set(ids)) == 5, "prep + build and telemetry on 2 masters, each once"
    readme = (fake_host / "results" / "README.md").read_text()
    assert readme.count(f"run_id={rid},") == 1
    # resuming a complete run records nothing twice
    H.main_scalability(
        ["--record", "--resume", "--run-id", rid, "--keep-data", "--repo", str(fake_host)],
        runner=FakeContainer(fake_host),
        out=lines.append,
        now=NOW,
    )
    assert (fake_host / "results" / "README.md").read_text().count(f"run_id={rid},") == 1


def test_resume_refusals(fake_host: Path) -> None:
    lines: list[str] = []
    box = FakeContainer(fake_host)
    assert (
        H.main_scalability(
            ["--resume", "--run-id", "bench_nope", "--repo", str(fake_host)],
            runner=box,
            out=lines.append,
        )
        == 2
    )
    assert "nothing to resume" in lines[-1] and box.cmds == []
    rid = "bench_20261113T230500Z"
    H.main_scalability(
        ["--run-id", rid, "--repo", str(fake_host), "--keep-data", *SMALL],
        runner=FakeContainer(fake_host, stop_after=1),
        out=lines.append,
    )
    assert (
        H.main_scalability(
            ["--run-id", rid, "--repo", str(fake_host), *SMALL], runner=box, out=lines.append
        )
        == 2
    )
    assert f"resume it with RESUME={rid}" in lines[-1]
    assert (
        H.main_scalability(
            ["--resume", "--run-id", rid, "--repo", str(fake_host), "--factors", "1", "4"],
            runner=box,
            out=lines.append,
        )
        == 2
    )
    assert "GRID_ARGS differ" in lines[-1] and box.cmds == []
    with pytest.raises(SystemExit):
        H.main_scalability(["--resume", "--repo", str(fake_host)], runner=box)


def test_smoke_prints_the_estimate_and_a_reduced_grid(fake_host: Path) -> None:
    lines: list[str] = []
    H.main_scalability(
        ["--smoke", "--keep-data", "--repo", str(fake_host)],
        runner=FakeContainer(fake_host),
        out=lines.append,
        now=NOW,
    )
    text = "\n".join(lines)
    assert "uv run python experiments/scalability.py --smoke --resume --run-id" in lines[1]
    assert "estimate: full grid (72 cells) ~" in text and "(affine x1/x4 fit;" in text
    assert "reduced grid step: 3 timed runs at x16 and x64" in text
    assert 'proposed: make experiments-spark RECORD=1 GRID_ARGS="--seasons 1 3 5' in text


def _partitioning_container(raw: list[dict[str, Any]], *, complete: bool) -> Any:
    def runner(cmd: Sequence[str]) -> int:
        rdir = Path(cmd[cmd.index("--run-dir") + 1].replace("/workspace", str(Path.cwd()), 1))
        with (rdir / "partitioning.jsonl").open("a") as fh:
            for r in raw:
                fh.write(json.dumps(r) + "\n")
        (rdir / "partitioning_summary.json").write_text(
            json.dumps({"complete": complete, "prep": {}})
        )
        runner.cmd = list(cmd)  # type: ignore[attr-defined]
        return 0 if complete else 137

    return runner


RAW = [
    {
        "run_id": "r",
        "dataset": "laps",
        "query": "q",
        "fs": "file",
        "layout": lay,
        "variant": "none",
        "median_s": med,
        "result_checksum": "x",
        "materialise_s": 0.0,
    }
    for lay, med in (("unpartitioned", 2.0), ("partitioned", 1.0))
]


def test_partitioning_host_writes_speedups(fake_host: Path) -> None:
    lines: list[str] = []
    H.main_partitioning(
        ["--smoke", "--keep-data", "--repo", str(fake_host)],
        runner=_partitioning_container(RAW, complete=True),
        out=lines.append,
        now=NOW,
    )
    rdir = fake_host / "data" / "bench" / "bench_20261113T220000Z_psmoke"
    rows = list(csv.DictReader((rdir / "partitioning.csv").open()))
    assert [r["speedup_vs_simplest"] for r in rows] == ["1.0000", "2.0000"]
    assert any(line.startswith("estimate: full partitioning run") for line in lines)
    assert not (fake_host / "results").exists()


def test_partitioning_record_only_when_complete_and_resume_flag(fake_host: Path) -> None:
    lines: list[str] = []
    rid = "bench_20261113T231000Z"
    code = H.main_partitioning(
        ["--record", "--run-id", rid, "--repo", str(fake_host)],
        runner=_partitioning_container(RAW, complete=False),
        out=lines.append,
        now=NOW,
    )
    assert code == 1 and not (fake_host / "results").exists()
    assert (
        lines[1]
        == f"partitioning: to resume this run: make experiments-spark RECORD=1 RESUME={rid}"
    )
    second = _partitioning_container([], complete=True)
    code = H.main_partitioning(
        ["--record", "--resume", "--run-id", rid, "--keep-data", "--repo", str(fake_host)],
        runner=second,
        out=lines.append,
        now=NOW,
    )
    assert code == 0 and "--resume" in second.cmd
    assert (fake_host / "results" / "partitioning.csv").exists()
    fresh = _partitioning_container(RAW, complete=True)
    H.main_partitioning(
        ["--resume", "--run-id", "bench_20261113T232000Z", "--keep-data", "--repo", str(fake_host)],
        runner=fresh,
        out=lines.append,
        now=NOW,
    )
    assert "--resume" not in fresh.cmd, "resuming a run whose partitioning never started starts it"


def test_exec_cmd_and_container_path(tmp_path: Path) -> None:
    cmd = H.exec_cmd("f1sa.bench.grid", ["--x"], {"F1SA_GIT_SHA": "abc"})
    assert cmd[:4] == ["docker", "compose", "exec", "-T"] and cmd[-3:] == [
        "-m",
        "f1sa.bench.grid",
        "--x",
    ]
    assert "F1SA_GIT_SHA=abc" in cmd and "PYTHONPATH=/workspace/src" in cmd
    assert "up" not in cmd and "restart" not in cmd
    assert (
        H.container_path(tmp_path / "data" / "bench" / "r", tmp_path) == "/workspace/data/bench/r"
    )
