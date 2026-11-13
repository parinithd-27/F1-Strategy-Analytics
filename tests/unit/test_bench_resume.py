"""Resumable grid: per-cell CSV commits, kill and resume, stale cells, atomic CSV (Step 10)."""

from __future__ import annotations

import collections
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import psutil  # type: ignore[import-untyped]
import pytest

from f1sa.bench import grid as G
from f1sa.bench.report import SCALABILITY_COLUMNS, append_rows_atomic, read_csv_rows
from f1sa.bench.timing import GIB, MemoryGuard

REPO = Path(__file__).resolve().parents[2]
SPEC = G.GridSpec(seasons=(1,), factors=(1, 4), masters=("local1", "local2"), warmup=1, repeats=2)
# prep x1, build x2, telemetry x2, prep x4, build x2 = 8 cells
ALL_IDS = [c.cell_id for c in G.cells(SPEC)]


class Killed(BaseException):
    """Stands in for the grid process being killed (not an Exception: nothing catches it)."""


class CountingRunner:
    """Runs cells instantly; raises :class:`Killed` when asked to run call number ``kill_at``."""

    def __init__(self, kill_at: int | None = None, status: dict[str, str] | None = None) -> None:
        self.kill_at = kill_at
        self.status = status or {}
        self.ran: list[str] = []

    def __call__(self, argv: Sequence[str], result_path: Path, guard: MemoryGuard) -> G.CellOutcome:
        cell_id = result_path.name.removesuffix(".result.json")
        if self.kill_at is not None and len(self.ran) + 1 == self.kill_at:
            raise Killed(cell_id)
        self.ran.append(cell_id)
        status = self.status.get(cell_id, "ok")
        res: dict[str, Any] = {"status": status, "error": "", "times_s": [0.1, 0.2]}
        return G.CellOutcome(status, "", res, {"peak_rss_bytes": 1}, time.time(), time.time())


def _grid(run_dir: Path, runner: CountingRunner, *, resume: bool, spec: G.GridSpec = SPEC) -> int:
    return G.run_grid(
        spec,
        run_dir,
        run_dir.name,
        guard=MemoryGuard(64 * GIB, host_reserve_bytes=0),
        src=REPO / "src",
        runner=runner,
        resume=resume,
        out=lambda _m: None,
        git_sha="abc",
        git_dirty="false",
        docker_wait_s=0.0,
    )


def _csv_ids(run_dir: Path) -> list[str]:
    return [r["cell_id"] for r in read_csv_rows(run_dir / G.CSV_NAME)]


def test_kill_after_two_cells_then_resume_gives_every_cell_exactly_once(tmp_path: Path) -> None:
    run_dir = tmp_path / "data" / "bench" / "bench_r1"
    first = CountingRunner(kill_at=3)
    with pytest.raises(Killed):
        _grid(run_dir, first, resume=False)
    assert first.ran == ALL_IDS[:2]
    assert _csv_ids(run_dir) == ALL_IDS[:2], "each finished cell is committed at once"
    assert G.pending_cells(SPEC, run_dir / G.CSV_NAME) == ALL_IDS[2:]

    second = CountingRunner()
    assert _grid(run_dir, second, resume=True) == 0
    ids = _csv_ids(run_dir)
    assert collections.Counter(ids) == collections.Counter(ALL_IDS), "every cell exactly once"
    assert ids == ALL_IDS, "rows keep the grid order"
    # the interrupted cell is rerun from the start; finished cells are not; the prep of x1 is rerun
    # (its build cells are pending) to reuse or rebuild its data, without a second row
    assert second.ran[0] == "prep_s1_x1_local4" and ALL_IDS[2] in second.ran
    assert ALL_IDS[1] not in second.ran
    records = G.read_records(run_dir / G.RECORDS_NAME)
    rerun = [r for r in records if r.get("rerun_for_resume")]
    assert [r["cell_id"] for r in rerun] == ["prep_s1_x1_local4"]
    assert G.pending_cells(SPEC, run_dir / G.CSV_NAME) == []
    rows = read_csv_rows(run_dir / G.CSV_NAME)
    assert list(rows[0]) == list(SCALABILITY_COLUMNS) and {r["git_sha"] for r in rows} == {"abc"}


def test_resume_of_a_complete_grid_runs_nothing(tmp_path: Path) -> None:
    run_dir = tmp_path / "data" / "bench" / "bench_r2"
    _grid(run_dir, CountingRunner(), resume=False)
    again = CountingRunner()
    _grid(run_dir, again, resume=True)
    assert again.ran == [] and _csv_ids(run_dir) == ALL_IDS


def test_resume_keeps_the_guard_state_from_the_csv(tmp_path: Path) -> None:
    spec = G.GridSpec(seasons=(1,), factors=(1, 4, 16), masters=("local2",), warmup=0, repeats=1)
    run_dir = tmp_path / "data" / "bench" / "bench_r3"
    ids = [c.cell_id for c in G.cells(spec)]
    first = CountingRunner(
        kill_at=ids.index("prep_s1_x16_local4") + 1,
        status={"build_s1_x4_local2": "aborted_memory_guard"},
    )
    with pytest.raises(Killed):
        _grid(run_dir, first, resume=False, spec=spec)
    second = CountingRunner()
    _grid(run_dir, second, resume=True, spec=spec)
    rows = {r["cell_id"]: r for r in read_csv_rows(run_dir / G.CSV_NAME)}
    assert rows["build_s1_x16_local2"]["status"] == "skipped_memory_guard"
    assert "build_s1_x16_local2" not in second.ran


def test_new_run_refuses_an_existing_grid_and_resume_needs_the_same_grid(tmp_path: Path) -> None:
    run_dir = tmp_path / "data" / "bench" / "bench_r4"
    with pytest.raises(G.GridError, match="nothing to resume"):
        _grid(run_dir, CountingRunner(), resume=True)
    with pytest.raises(Killed):
        _grid(run_dir, CountingRunner(kill_at=2), resume=False)
    with pytest.raises(G.GridError, match="use --resume"):
        _grid(run_dir, CountingRunner(), resume=False)
    other = G.GridSpec(seasons=(1,), factors=(1,), masters=("local1",))
    with pytest.raises(G.GridError, match="different grid"):
        _grid(run_dir, CountingRunner(), resume=True, spec=other)


def test_duplicate_rows_in_the_csv_are_refused(tmp_path: Path) -> None:
    path = tmp_path / "s.csv"
    append_rows_atomic(path, [{"cell_id": "a"}, {"cell_id": "a"}], SCALABILITY_COLUMNS)
    with pytest.raises(G.GridError, match="twice"):
        G.completed_cells(path)


def test_atomic_append_creates_appends_and_refuses_another_header(tmp_path: Path) -> None:
    path = tmp_path / "x.csv"
    append_rows_atomic(path, [{"a": 1, "b": 2.5}], ("a", "b"))
    append_rows_atomic(path, [{"a": 3}], ("a", "b"))
    assert path.read_text() == "a,b\n1,2.5000\n3,\n"
    assert not (tmp_path / "x.csv.tmp").exists()
    path.write_text("a,b\n1,2\n4,5")  # a last line without its newline
    append_rows_atomic(path, [{"a": 6, "b": 7}], ("a", "b"))
    assert path.read_text() == "a,b\n1,2\n4,5\n6,7\n"
    with pytest.raises(ValueError, match="different header"):
        append_rows_atomic(path, [{"c": 1}], ("c",))
    assert read_csv_rows(tmp_path / "none.csv") == []


def test_kill_stale_cells_matches_only_this_runs_cells(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "data" / "bench" / "bench_r5"

    class P:
        def __init__(self, pid: int, cmd: list[str]) -> None:
            self.info = {"pid": pid, "cmdline": cmd}

    procs = [
        P(11, ["python", "-m", "f1sa.bench.cell", "--spec", f"{run_dir}/cells/a.spec.json"]),
        P(12, ["python", "-m", "f1sa.bench.cell", "--spec", f"{run_dir}2/cells/a.spec.json"]),
        P(13, ["python", "-c", "print(1)"]),
    ]
    killed: list[int] = []
    monkeypatch.setattr(G.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(G.os, "killpg", lambda pgid, sig: killed.append(pgid))
    assert G.kill_stale_cells(run_dir, procs=lambda: procs) == [11]
    assert killed == [11]


def test_waiting_for_docker_samples(tmp_path: Path) -> None:
    path = tmp_path / "docker_stats.jsonl"
    assert G.wait_for_docker_samples(path, 10.0) == [], "no host sampler: no wait"
    path.write_text(
        json.dumps({"ts": 5.0, "name": "a", "mem_bytes": 1, "mem_limit_bytes": 2, "cpu_pct": 1.0})
        + "\n"
        + '{"ts": 6.0, "na'
    )  # partial last line
    sleeps: list[float] = []
    got = G.wait_for_docker_samples(path, 10.0, timeout_s=1.0, sleep=sleeps.append)
    assert len(got) == 1 and sleeps == [0.5, 0.5], "waits for a sample past the cell's end"


def _grid_cmd(run_dir: Path, *resume: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "f1sa.bench.grid",
        "--run-dir",
        str(run_dir),
        *SPEC.cli_args(),
        "--cell-module",
        "_fake_cell",
        "--src",
        str(REPO / "src"),
        *resume,
    ]


def test_real_grid_process_killed_mid_cell_then_resumed(tmp_path: Path) -> None:
    """SIGKILL the grid process while its third cell runs; the orphaned cell is killed on resume
    and the CSV ends with every cell exactly once."""
    run_dir = tmp_path / "data" / "bench" / "bench_r6"
    env = {
        **os.environ,
        "PYTHONPATH": f"{REPO / 'src'}{os.pathsep}{REPO / 'tests' / 'unit'}",
        "FAKE_SLOW_CELL": ALL_IDS[2],
    }
    grid = subprocess.Popen(
        _grid_cmd(run_dir),
        cwd=tmp_path,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 60
    orphan: int | None = None
    while time.time() < deadline:
        if len(_csv_ids(run_dir)) == 2:
            kids = [
                p
                for p in psutil.Process(grid.pid).children()
                if "_fake_cell" in " ".join(p.cmdline())
            ]
            if kids:
                orphan = kids[0].pid
                break
        time.sleep(0.1)
    assert orphan is not None, "the grid never reached its third cell"
    grid.send_signal(signal.SIGKILL)
    grid.wait()
    assert psutil.pid_exists(orphan), "a hard-killed grid leaves its cell behind"
    assert _csv_ids(run_dir) == ALL_IDS[:2]

    env.pop("FAKE_SLOW_CELL")
    done = subprocess.run(
        _grid_cmd(run_dir, "--resume"),
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert "killed 1 stale cell process" in done.stdout
    time.sleep(0.2)
    assert not psutil.pid_exists(orphan) or psutil.Process(orphan).status() == psutil.STATUS_ZOMBIE
    assert collections.Counter(_csv_ids(run_dir)) == collections.Counter(ALL_IDS)
    assert "0 cell(s) pending" in done.stdout


def test_a_second_grid_on_the_same_run_is_refused(tmp_path: Path) -> None:
    import fcntl

    run_dir = tmp_path / "data" / "bench" / "bench_r7"
    run_dir.mkdir(parents=True)
    with (run_dir / G.LOCK_NAME).open("a") as held:
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(G.GridError, match="another grid is running"):
            _grid(run_dir, CountingRunner(), resume=False)
    assert _grid(run_dir, CountingRunner(), resume=False) == 0, "free once the holder is gone"
