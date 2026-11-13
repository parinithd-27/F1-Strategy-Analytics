"""The scalability grid: cells, the per-cell subprocesses with the memory guard, and the time
estimate. ``python -m f1sa.bench.grid --run-dir /workspace/data/bench/<run_id> ...`` runs inside the
app container (``make bench-smoke`` / ``make experiments-spark``, through
``experiments/scalability.py`` on the host).

Order: for each seasons set, for each replication factor (ascending): one ``prep`` cell
(replication into ``/f1/bench/<run_id>/replicated/s<n>_x<f>``), then a ``build`` cell per master
and, at factor 1 only, a ``telemetry`` cell per master; then the replicated copy is deleted. The
running cell is killed (process group SIGKILL) if the container's memory passes the guard or the
host's available memory drops below the reserve; after that, larger factors of the same (phase,
seasons, master) are skipped (:class:`f1sa.bench.timing.MemoryGuard`).
Every cell (also skipped, aborted and failed ones) is one JSON line in ``cells.jsonl``.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import signal
import subprocess
import sys
import time
import zipfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

import psutil  # type: ignore[import-untyped]  # no stubs (types-psutil not a dependency)

from f1sa.bench.cell import MASTERS
from f1sa.bench.paths import bench_hdfs_uri, check_bench_uri
from f1sa.bench.report import (
    SCALABILITY_COLUMNS,
    append_rows_atomic,
    read_csv_rows,
    scalability_row,
)
from f1sa.bench.resources import (
    ContainerSample,
    ProcessSample,
    ProcessSampler,
    cgroup_memory_limit,
    read_container_samples,
)
from f1sa.bench.timing import MemoryGuard
from f1sa.bench.workloads import TELEMETRY_ROUNDS_BY_SEASONS

SEASON_SETS: dict[int, tuple[int, ...]] = {
    1: (2024,),
    3: (2022, 2023, 2024),
    5: (2021, 2022, 2023, 2024, 2025),
}
"""1 season = 2024 (the telemetry year), 3 = 2022-2024, 5 = 2021-2025 (2025 timing data only: no
label or outcome is read)."""
ALL_SEASONS = SEASON_SETS[5]
CLEAN_ROOT = "hdfs://namenode:8020/f1/clean"
TELEMETRY_ROOT = "hdfs://namenode:8020/f1/raw/tracinginsights/telemetry"
CELL_TIMEOUT_S = 4 * 3600
BUDGET_S = 8 * 3600


@dataclass(frozen=True)
class GridSpec:
    seasons: tuple[int, ...] = (1, 3, 5)
    factors: tuple[int, ...] = (1, 4, 16, 64)
    masters: tuple[str, ...] = ("local1", "local2", "local4", "cluster")
    warmup: int = 1
    repeats: int = 5
    large_from: int = 16
    """Factors >= this use ``large_repeats`` and ``large_masters``."""
    large_repeats: int | None = None
    large_masters: tuple[str, ...] | None = None
    large_seasons: tuple[int, ...] | None = None
    """Season sets that run factors >= ``large_from`` (None = all)."""

    def __post_init__(self) -> None:
        for n in self.seasons:
            if n not in SEASON_SETS:
                raise ValueError(f"seasons must be in {sorted(SEASON_SETS)}, got {n}")
        for n in self.large_seasons or ():
            if n not in self.seasons:
                raise ValueError(f"large_seasons must be a subset of seasons, got {n}")
        for m in (*self.masters, *(self.large_masters or ())):
            if m not in MASTERS:
                raise ValueError(f"master must be one of {sorted(MASTERS)}, got {m!r}")
        if not self.factors or min(self.factors) < 1 or list(self.factors) != sorted(self.factors):
            raise ValueError(f"factors must be ascending and >= 1, got {self.factors}")
        if self.repeats < 1 or self.warmup < 0 or (self.large_repeats or 1) < 1:
            raise ValueError("repeats >= 1 and warmup >= 0 are required")

    def repeats_for(self, factor: int) -> int:
        if factor >= self.large_from and self.large_repeats is not None:
            return self.large_repeats
        return self.repeats

    def masters_for(self, factor: int) -> tuple[str, ...]:
        if factor >= self.large_from and self.large_masters is not None:
            return tuple(m for m in self.masters if m in self.large_masters)
        return self.masters

    def runs_factor(self, n_seasons: int, factor: int) -> bool:
        if factor >= self.large_from and self.large_seasons is not None:
            return n_seasons in self.large_seasons
        return True

    def cli_args(self) -> list[str]:
        args = [
            "--seasons",
            *map(str, self.seasons),
            "--factors",
            *map(str, self.factors),
            "--masters",
            *self.masters,
            "--warmup",
            str(self.warmup),
            "--repeats",
            str(self.repeats),
            "--large-from",
            str(self.large_from),
        ]
        if self.large_repeats is not None:
            args += ["--large-repeats", str(self.large_repeats)]
        if self.large_masters is not None:
            args += ["--large-masters", *self.large_masters]
        if self.large_seasons is not None:
            args += ["--large-seasons", *map(str, self.large_seasons)]
        return args


@dataclass(frozen=True)
class Cell:
    phase: str
    n_seasons: int
    factor: int
    master: str
    warmup: int
    repeats: int

    @property
    def cell_id(self) -> str:
        return f"{self.phase}_s{self.n_seasons}_x{self.factor}_{self.master}"


def cells(spec: GridSpec) -> Iterator[Cell]:
    """Every cell of ``spec`` in run order (prep cells run on local[4], no repetitions)."""
    for n in spec.seasons:
        for f in spec.factors:
            masters = spec.masters_for(f)
            if not spec.runs_factor(n, f) or not masters:
                continue
            yield Cell("prep", n, f, "local4", 0, 1)
            reps = spec.repeats_for(f)
            for m in masters:
                yield Cell("build", n, f, m, spec.warmup, reps)
            if f == 1:
                for m in spec.masters_for(f):
                    yield Cell("telemetry", n, f, m, spec.warmup, reps)


# ============================================================================== estimate
@dataclass(frozen=True)
class SmokeTimes:
    """From a smoke run: per master, session start-up and one run of each phase at 1 season, x1
    (5 telemetry rounds); the local[4] build and the prep at x4 (second size point, optional);
    and the clean laps per season."""

    startup_s: Mapping[str, float]
    build_s: Mapping[str, float]
    telemetry_s: Mapping[str, float]
    prep_s: float
    laps_by_season: Mapping[int, int]
    build_x4_s: float | None = None
    prep_x4_s: float | None = None


def smoke_times(records: Sequence[Mapping[str, Any]]) -> SmokeTimes:
    """Collect :class:`SmokeTimes` from a smoke run's ``cells.jsonl`` records."""
    startup: dict[str, float] = {}
    build: dict[str, float] = {}
    tel: dict[str, float] = {}
    prep: dict[int, float] = {}
    build_x4: float | None = None
    laps: dict[int, int] = {}
    for r in records:
        res = r.get("result") or {}
        if res.get("status") != "ok" or r["cell"].get("n_seasons", 1) != 1:
            continue
        cell = r["cell"]
        factor = int(cell.get("factor", 1))
        if cell["phase"] == "prep":
            prep[factor] = float(res["prep_s"])
            laps = {int(k): int(v) for k, v in res["laps_by_season"].items()}
            continue
        runs = [*res.get("warmup_s", []), *res.get("times_s", [])]
        if not runs:
            continue
        per_run = sorted(runs)[len(runs) // 2]
        if factor == 4 and cell["phase"] == "build" and cell["master"] == "local4":
            build_x4 = per_run
        if factor != 1:
            continue
        startup[cell["master"]] = float(res["startup_s"])
        (build if cell["phase"] == "build" else tel)[cell["master"]] = per_run
    if 1 not in prep or not laps:
        raise ValueError("the smoke run has no successful prep cell")
    return SmokeTimes(startup, build, tel, prep[1], laps, build_x4, prep.get(4))


def rows_ratio(n_seasons: int, factor: int, laps_by_season: Mapping[int, int]) -> float:
    """Lap rows of (n_seasons, factor) relative to (1 season, x1)."""
    base = sum(laps_by_season[s] for s in SEASON_SETS[1])
    return sum(laps_by_season[s] for s in SEASON_SETS[n_seasons]) * factor / base


def growth(t1: float, t4: float | None, ratio: float) -> float:
    """Runtime multiplier at ``ratio`` x the rows of the x1 point: an affine fit through the x1
    and x4 points (fixed cost + per-row cost), or linear (``ratio``) without an x4 point. The
    multiplier never drops below 1 for ratio >= 1."""
    if t4 is None or t1 <= 0:
        return ratio
    per_unit = max(t4 - t1, 0.0) / 3.0
    fixed = t1 - per_unit
    return max((fixed + per_unit * ratio) / t1, 1.0 if ratio >= 1 else ratio)


@dataclass
class Estimate:
    total_s: float
    per_cell: list[tuple[str, float]] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    """Cells without a smoke measurement for their master (not counted)."""
    method: str = "linear"


def estimate(spec: GridSpec, smoke: SmokeTimes) -> Estimate:
    """Grid time from a smoke run. With the x4 points: an affine fit (fixed + per-row cost),
    applied to every master as the local[4] multiplier. Costs that grow faster than the rows beyond
    x4 (single-partition history windows, many small partitions, spills) are NOT captured, so the
    real grid can be slower. Without the x4 points: linear in the rows. Telemetry scales linearly
    in the rounds; one session start-up per cell."""
    base_rounds = TELEMETRY_ROUNDS_BY_SEASONS[1]
    method = "affine x1/x4 fit" if smoke.build_x4_s is not None else "linear"
    est = Estimate(0.0, method=method)
    t1 = smoke.build_s.get("local4")
    for c in cells(spec):
        ratio = rows_ratio(c.n_seasons, c.factor, smoke.laps_by_season)
        if c.phase == "prep":
            seconds = smoke.prep_s * growth(smoke.prep_s, smoke.prep_x4_s, ratio)
            seconds += smoke.startup_s.get("local4", 0.0)
        else:
            table = smoke.build_s if c.phase == "build" else smoke.telemetry_s
            if c.master not in table:
                est.missing.append(c.cell_id)
                continue
            if c.phase == "build":
                scale = growth(t1, smoke.build_x4_s, ratio) if t1 else ratio
            else:
                scale = TELEMETRY_ROUNDS_BY_SEASONS[c.n_seasons] / base_rounds
            seconds = smoke.startup_s[c.master] + (c.warmup + c.repeats) * table[c.master] * scale
        est.per_cell.append((c.cell_id, seconds))
        est.total_s += seconds
    return est


REDUCTIONS: tuple[tuple[str, Callable[[GridSpec], GridSpec]], ...] = (
    ("3 timed runs at x16 and x64", lambda s: replace(s, large_repeats=3)),
    (
        "no x64 (local x4 at 1 season already peaks at ~3.6 GiB of app container memory)",
        lambda s: replace(s, factors=tuple(f for f in s.factors if f < 64)),
    ),
    ("x16 only for 1 season", lambda s: replace(s, large_seasons=(1,))),
    (
        "x16 only on local[4] and the cluster",
        lambda s: replace(s, large_masters=("local4", "cluster")),
    ),
    ("1 warm-up + 3 timed runs in every cell", lambda s: replace(s, repeats=3, large_repeats=3)),
)
"""Applied in order until the estimate fits the budget. The order keeps the commitment's axes
(1/3/5 seasons x every master at x1 and x4) longest and cuts the largest, least feasible cells
first."""


def reduced_grid(
    spec: GridSpec, smoke: SmokeTimes, budget_s: float = BUDGET_S
) -> list[tuple[str, GridSpec, float]]:
    """Apply :data:`REDUCTIONS` in order until the estimate is within ``budget_s``; returns every
    step as (what changed, spec, estimated seconds)."""
    steps: list[tuple[str, GridSpec, float]] = []
    current = spec
    for what, change in REDUCTIONS:
        if estimate(current, smoke).total_s <= budget_s:
            break
        current = change(current)
        steps.append((what, current, estimate(current, smoke).total_s))
    return steps


# ============================================================================== running
def make_pyfiles_zip(src: Path, dest: Path) -> Path:
    """Zip the ``f1sa`` package under ``src`` (``.py`` and package data, no caches) to ``dest``."""
    pkg = src / "f1sa"
    if not (pkg / "__init__.py").is_file():
        raise FileNotFoundError(f"no f1sa package under {src}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(pkg.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
                zf.write(path, path.relative_to(src).as_posix())
    return dest


def cell_spec(cell: Cell, run_id: str, pyfiles: str | None) -> dict[str, Any]:
    base = bench_hdfs_uri(run_id)
    return {
        "cell_id": cell.cell_id,
        "phase": cell.phase,
        "master": cell.master,
        "n_seasons": cell.n_seasons,
        "seasons": list(SEASON_SETS[cell.n_seasons]),
        "all_seasons": list(ALL_SEASONS),
        "factor": cell.factor,
        "warmup": cell.warmup,
        "repeats": cell.repeats,
        "clean_root": CLEAN_ROOT,
        "telemetry_root": TELEMETRY_ROOT,
        "input_root": f"{base}/replicated/s{cell.n_seasons}_x{cell.factor}",
        "out_root": f"{base}/scal/{cell.cell_id}",
        "pyfiles": pyfiles if cell.master == "cluster" else None,
    }


@dataclass
class CellOutcome:
    status: str
    reason: str
    result: dict[str, Any] | None
    peaks: dict[str, Any]
    started_at: float
    ended_at: float


def run_subprocess_cell(
    argv: Sequence[str],
    result_path: Path,
    guard: MemoryGuard,
    *,
    interval_s: float = 0.5,
    timeout_s: float = CELL_TIMEOUT_S,
    sampler_kwargs: Mapping[str, Any] | None = None,
) -> CellOutcome:
    """Run ``argv`` in its own process group under the sampler and the runtime guard."""
    started = time.time()
    proc = subprocess.Popen(list(argv), start_new_session=True)
    killed: list[str] = []

    def check(sample: ProcessSample) -> None:
        reason = guard.exceeded(sample.cgroup_bytes, sample.host_available_bytes)
        if reason is not None and not killed:
            killed.append(reason)
            _kill_group(proc)

    sampler = ProcessSampler(
        proc.pid, interval_s=interval_s, on_sample=check, **(sampler_kwargs or {})
    )
    sampler.start()
    try:
        proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        killed.append(f"timeout after {timeout_s:.0f} s")
        _kill_group(proc)
        proc.wait()
    except BaseException:
        # the grid itself is stopping (SIGTERM / Ctrl+C): never leave the cell's JVM behind
        _kill_group(proc)
        proc.wait()
        raise
    finally:
        peaks = sampler.stop()
    ended = time.time()
    result: dict[str, Any] | None = None
    if result_path.exists():
        result = json.loads(result_path.read_text(encoding="utf-8"))
    if killed:
        status = "timeout" if killed[0].startswith("timeout") else "aborted_memory_guard"
        reason = killed[0]
    elif result is None:
        status, reason = "failed", f"exit {proc.returncode} without a result"
    else:
        status, reason = str(result["status"]), str(result.get("error", ""))
    return CellOutcome(status, reason, result, asdict(peaks), started, ended)


def _kill_group(proc: subprocess.Popen[bytes]) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)


def guard_limit_bytes() -> int:
    """The container's memory limit, or the machine's memory outside a container."""
    return cgroup_memory_limit() or int(psutil.virtual_memory().total)


CSV_NAME = "scalability.csv"
RECORDS_NAME = "cells.jsonl"
SPEC_NAME = "grid_spec.json"
LOCK_NAME = ".grid.lock"
_KILLED = ("aborted_memory_guard", "timeout")


class GridError(RuntimeError):
    """The grid cannot start or resume as asked."""


def spec_from_json(obj: Mapping[str, Any]) -> GridSpec:
    """A :class:`GridSpec` from its ``grid_spec.json`` form (lists back to tuples)."""

    def tup(v: Any) -> Any:
        return tuple(v) if isinstance(v, list) else v

    return GridSpec(**{k: tup(v) for k, v in obj.items()})


def completed_cells(csv_path: Path) -> dict[str, dict[str, str]]:
    """Cells that already have their row in the run's CSV (the commit point of a cell). A cell id
    that appears twice means the CSV was edited by hand or two grids wrote it: refused."""
    done: dict[str, dict[str, str]] = {}
    for row in read_csv_rows(csv_path):
        cid = row["cell_id"]
        if cid in done:
            raise GridError(f"{csv_path} lists {cid} twice")
        done[cid] = row
    return done


def kill_stale_cells(
    run_dir: Path, *, procs: Callable[[], Iterable[Any]] | None = None
) -> list[int]:
    """Kill (process group, SIGKILL) cell processes left running by an earlier grid of this run,
    e.g. after the grid itself was killed hard; returns their pids."""
    marker = str(run_dir / "cells") + os.sep
    killed: list[int] = []
    for proc in (procs or (lambda: psutil.process_iter(["pid", "cmdline"])))():
        try:
            cmd = proc.info.get("cmdline") or []
        except (psutil.Error, AttributeError):
            continue
        if any(str(arg).startswith(marker) for arg in cmd) and "--spec" in cmd:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.info["pid"]), signal.SIGKILL)
                killed.append(int(proc.info["pid"]))
    return killed


def wait_for_docker_samples(
    path: Path,
    until_ts: float,
    *,
    timeout_s: float = 6.0,
    poll_s: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
) -> list[ContainerSample]:
    """The host's docker samples, after waiting (at most ``timeout_s``) for one taken at or after
    ``until_ts`` so the cell's window is covered. Without the file (no host sampler), none."""
    waited = 0.0
    while True:
        samples = read_container_samples(path)
        if not path.exists() or any(x.ts >= until_ts for x in samples) or waited >= timeout_s:
            return samples
        sleep(poll_s)
        waited += poll_s


@dataclass
class GridSession:
    """Where one grid run writes, and the values every CSV row carries."""

    run_dir: Path
    run_id: str
    git_sha: str = ""
    git_dirty: str = ""
    docker_wait_s: float = 6.0
    out: Callable[[str], None] = print

    @property
    def csv_path(self) -> Path:
        return self.run_dir / CSV_NAME

    def commit(self, record: Mapping[str, Any], *, row: bool = True) -> None:
        """Log ``record`` to ``cells.jsonl``; with ``row``, append its CSV row atomically (the
        commit point: a cell without a row is rerun from its warm-up on resume)."""
        _append(self.run_dir, record, self.out)
        if not row:
            return
        samples: list[ContainerSample] = []
        if record.get("ended_at") is not None:
            samples = wait_for_docker_samples(
                self.run_dir / "docker_stats.jsonl",
                float(record["ended_at"]),
                timeout_s=self.docker_wait_s,
            )
        csv_row = scalability_row(record, samples, git_sha=self.git_sha, git_dirty=self.git_dirty)
        append_rows_atomic(self.csv_path, [csv_row], SCALABILITY_COLUMNS)


def run_grid(
    spec: GridSpec,
    run_dir: Path,
    run_id: str,
    *,
    guard: MemoryGuard,
    src: Path,
    out: Callable[[str], None] = print,
    runner: Callable[..., CellOutcome] = run_subprocess_cell,
    resume: bool = False,
    cell_module: str = "f1sa.bench.cell",
    git_sha: str = "",
    git_dirty: str = "",
    docker_wait_s: float = 6.0,
) -> int:
    """Run every cell not yet in the run's CSV; returns the number of cells that ran this session
    and did not finish ``ok``. Each finished cell's row is appended to ``scalability.csv`` at once.

    ``resume`` continues ``run_dir``: the stored ``grid_spec.json`` must equal ``spec``; cells with
    a row are skipped; a prep whose cells are all done is not rerun; a prep with pending cells is
    rerun (reusing its complete copy, or preparing it again) without a second row."""
    session = GridSession(run_dir, run_id, git_sha, git_dirty, docker_wait_s, out)
    run_dir.mkdir(parents=True, exist_ok=True)
    lock = (run_dir / LOCK_NAME).open("a")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        raise GridError(f"another grid is running {run_dir.name}") from None
    try:
        return _run_grid_locked(
            spec,
            session,
            guard=guard,
            src=src,
            runner=runner,
            resume=resume,
            cell_module=cell_module,
        )
    finally:
        lock.close()  # releases the flock (the kernel also releases it if the grid is killed)


def _run_grid_locked(
    spec: GridSpec,
    session: GridSession,
    *,
    guard: MemoryGuard,
    src: Path,
    runner: Callable[..., CellOutcome],
    resume: bool,
    cell_module: str,
) -> int:
    run_dir, run_id, out = session.run_dir, session.run_id, session.out
    spec_path = run_dir / SPEC_NAME
    if resume:
        if not spec_path.exists():
            raise GridError(f"nothing to resume: {spec_path} does not exist")
        stored = spec_from_json(json.loads(spec_path.read_text(encoding="utf-8")))
        if stored != spec:
            raise GridError(f"{spec_path} holds a different grid; resume with the same grid")
    else:
        if session.csv_path.exists() or spec_path.exists():
            raise GridError(f"{run_dir} already holds a grid; use --resume")
        spec_path.write_text(json.dumps(asdict(spec), indent=1), encoding="utf-8")
    stale = kill_stale_cells(run_dir)
    if stale:
        out(f"grid: killed {len(stale)} stale cell process(es) of this run: {stale}")
    done = completed_cells(session.csv_path)
    all_cells = list(cells(spec))
    deps: dict[tuple[int, int], list[str]] = {}
    for c in all_cells:
        if c.phase != "prep":
            deps.setdefault((c.n_seasons, c.factor), []).append(c.cell_id)
    if resume:
        out(f"grid: resuming, {len(done)} of {len(all_cells)} cells already in {CSV_NAME}")
    pyfiles = None
    if any(c.master == "cluster" and c.cell_id not in done for c in all_cells):
        pyfiles = str(make_pyfiles_zip(src, run_dir / "f1sa_src.zip"))
    killed_at: dict[tuple[str, int, str], tuple[int, str]] = {}
    not_ok = 0
    prep_ok = False
    for cell in all_cells:
        key = (cell.phase, cell.n_seasons, cell.master)
        if cell.phase == "prep":
            pending = [d for d in deps.get((cell.n_seasons, cell.factor), []) if d not in done]
            if not pending:
                prep_ok = done.get(cell.cell_id, {}).get("status") == "ok"
                continue
        elif cell.cell_id in done:
            status = done[cell.cell_id]["status"]
            if status in _KILLED:
                killed_at.setdefault(key, (cell.factor, status))
            continue
        record: dict[str, Any] = {"run_id": run_id, "cell": asdict(cell), "cell_id": cell.cell_id}
        if cell.phase != "prep" and not prep_ok:
            record |= {"status": "skipped_no_input", "reason": "the prep cell failed"}
            session.commit(record)
            not_ok += 1
            continue
        prev = killed_at.get(key)
        decision = guard.precheck(prev[0] if prev else None, prev[1] if prev else "", cell.factor)
        record["guard"] = asdict(decision)
        if not decision.ok:
            record |= {"status": "skipped_memory_guard", "reason": decision.reason}
            session.commit(record)
            not_ok += 1
            continue
        cell_spec_path = run_dir / "cells" / f"{cell.cell_id}.spec.json"
        result_path = run_dir / "cells" / f"{cell.cell_id}.result.json"
        cell_spec_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.unlink(missing_ok=True)  # never read an earlier attempt's result
        cell_spec_path.write_text(json.dumps(cell_spec(cell, run_id, pyfiles), indent=1))
        argv = [sys.executable, "-m", cell_module, "--spec", str(cell_spec_path)]
        outcome = runner([*argv, "--out", str(result_path)], result_path, guard)
        record |= {
            "status": outcome.status,
            "reason": outcome.reason,
            "result": outcome.result,
            "peaks": outcome.peaks,
            "started_at": outcome.started_at,
            "ended_at": outcome.ended_at,
        }
        first_time = cell.cell_id not in done
        if not first_time:
            record["rerun_for_resume"] = True
        session.commit(record, row=first_time)
        if outcome.status != "ok":
            not_ok += 1
        if cell.phase == "prep":
            prep_ok = outcome.status == "ok"
        elif outcome.status in _KILLED:
            # a larger factor of the same (phase, seasons, master) cannot do better
            killed_at.setdefault(key, (cell.factor, outcome.status))
    return not_ok


def pending_cells(spec: GridSpec, csv_path: Path) -> list[str]:
    """Cell ids of ``spec`` without a row in ``csv_path`` (empty = the grid is complete)."""
    done = completed_cells(csv_path)
    return [c.cell_id for c in cells(spec) if c.cell_id not in done]


def _append(run_dir: Path, record: Mapping[str, Any], out: Callable[[str], None]) -> None:
    with (run_dir / RECORDS_NAME).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True, default=str) + "\n")
    res = record.get("result") or {}
    times = res.get("times_s") or []
    shown = f" median {sorted(times)[len(times) // 2]:.1f} s over {len(times)}" if times else ""
    if res.get("prep_s") is not None:
        shown = f" prep {res['prep_s']:.1f} s" + (" (reused)" if res.get("reused") else "")
    why = (
        f" ({record.get('reason')})"
        if record.get("status") != "ok" and record.get("reason")
        else ""
    )
    out(f"[{time.strftime('%H:%M:%S')}] {record['cell_id']}: {record['status']}{shown}{why}")


def read_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(ln) for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="f1sa-bench-grid", description="Run the scalability grid.")
    p.add_argument("--run-dir", type=Path, required=True, help="./data/bench/<run_id>")
    add_grid_args(p)
    p.add_argument("--resume", action="store_true", help="continue the grid in --run-dir")
    p.add_argument("--guard-fraction", type=float, default=0.9)
    p.add_argument("--src", type=Path, default=Path("src"))
    p.add_argument("--cell-module", default="f1sa.bench.cell", help=argparse.SUPPRESS)
    return p


def add_grid_args(p: argparse.ArgumentParser) -> None:
    d = GridSpec()
    p.add_argument("--seasons", type=int, nargs="+", default=list(d.seasons))
    p.add_argument("--factors", type=int, nargs="+", default=list(d.factors))
    p.add_argument("--masters", nargs="+", default=list(d.masters), choices=sorted(MASTERS))
    p.add_argument("--warmup", type=int, default=d.warmup)
    p.add_argument("--repeats", type=int, default=d.repeats)
    p.add_argument("--large-from", type=int, default=d.large_from)
    p.add_argument("--large-repeats", type=int, default=None)
    p.add_argument("--large-masters", nargs="+", default=None, choices=sorted(MASTERS))
    p.add_argument("--large-seasons", type=int, nargs="+", default=None)


def spec_from_args(args: argparse.Namespace) -> GridSpec:
    return GridSpec(
        seasons=tuple(args.seasons),
        factors=tuple(args.factors),
        masters=tuple(args.masters),
        warmup=args.warmup,
        repeats=args.repeats,
        large_from=args.large_from,
        large_repeats=args.large_repeats,
        large_masters=tuple(args.large_masters) if args.large_masters else None,
        large_seasons=tuple(args.large_seasons) if args.large_seasons else None,
    )


def _raise_on_sigterm(signum: int, _frame: object) -> None:
    raise SystemExit(128 + signum)


def main(argv: Sequence[str] | None = None) -> int:
    """Exit 0 when every cell of the grid has its row (whatever the cells' statuses: an aborted or
    skipped cell is a recorded result), 1 when cells are pending, 2 when the grid cannot start."""
    signal.signal(signal.SIGTERM, _raise_on_sigterm)
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    spec = spec_from_args(args)
    run_dir: Path = args.run_dir.resolve()
    check_bench_uri(f"file://{run_dir}")
    guard = MemoryGuard(guard_limit_bytes(), fraction=args.guard_fraction)
    print(
        f"grid: {sum(1 for _ in cells(spec))} cells, "
        f"guard {guard.threshold_bytes / 1024**3:.2f} GiB"
        f" of {guard.limit_bytes / 1024**3:.2f} GiB, run dir {run_dir}",
        flush=True,
    )
    try:
        not_ok = run_grid(
            spec,
            run_dir,
            run_dir.name,
            guard=guard,
            src=args.src.resolve(),
            resume=args.resume,
            cell_module=args.cell_module,
            git_sha=os.environ.get("F1SA_GIT_SHA", "unavailable"),
            git_dirty=os.environ.get("F1SA_GIT_DIRTY", "unknown"),
            out=lambda m: print(m, flush=True),
        )
    except GridError as exc:
        print(f"grid: {exc}", file=sys.stderr)
        return 2
    pending = pending_cells(spec, run_dir / CSV_NAME)
    print(f"grid: {len(pending)} cell(s) pending, {not_ok} cell(s) not ok this session")
    return 0 if not pending else 1


if __name__ == "__main__":
    sys.exit(main())
