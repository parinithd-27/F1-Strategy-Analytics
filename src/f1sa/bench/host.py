"""Host side of the Step 10 experiments.

``experiments/scalability.py`` and ``experiments/partitioning.py`` call this module.

Each entry point:

1. makes ``./data/bench/<run_id>/`` (host-owned; the app container sees it at
   ``/workspace/data/bench/<run_id>``);
2. starts the :class:`~f1sa.bench.resources.DockerStatsSampler` on the host (the app container has
   no Docker socket);
3. runs the container side (``python -m f1sa.bench.grid`` or ``python -m f1sa.bench.partitioning``)
   with ``docker compose exec -T app`` — no service is started, stopped, rebuilt or recreated;
4. stops the sampler; the grid has already appended each finished cell's row to
   ``./data/bench/<run_id>/scalability.csv`` (partitioning: each finished group to its JSONL);
5. only when the run is complete: deletes its HDFS benchmark data (``/f1/bench/<run_id>``, unless
   ``--keep-data``) and, with ``--record`` (``RECORD=1``), writes ``./results/`` plus a fingerprint
   in ``results/README.md``. An incomplete run keeps its data and prints the resume command
   (``make experiments-spark [RECORD=1] RESUME=<run_id>``), which is also printed at the start.

``--smoke`` (``make bench-smoke``) runs a tiny grid and prints the estimate for the full grid, and
the reduced grid if the estimate is over 8 hours.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from f1sa.bench import grid as G
from f1sa.bench.partitioning import estimate_partitioning, speedups
from f1sa.bench.paths import HDFS_BENCH, HttpFsBench, new_run_id, output_dir, run_dir
from f1sa.bench.report import (
    PARTITIONING_COLUMNS,
    SCALABILITY_COLUMNS,
    append_readme,
    fingerprint,
    plot_partitioning,
    plot_scaling,
    read_csv_rows,
    write_csv,
)
from f1sa.bench.resources import DockerStatsSampler

CONTAINER_WORKSPACE = Path("/workspace")
SMOKE_GRID = G.GridSpec(
    seasons=(1,),
    factors=(1, 4),
    warmup=0,
    repeats=1,
    large_from=4,
    large_repeats=1,
    large_masters=("local4",),
)
"""1 season: every master at x1 (both phases), plus local[4] at x4 as the second size point of
the estimate (:func:`f1sa.bench.grid.growth`)."""
Runner = Callable[[Sequence[str]], int]


def say(message: str) -> None:
    """Print and flush, so the host's lines (the resume command first of all) appear before the
    container's output even when stdout is a file."""
    print(message, flush=True)


def _stream(cmd: Sequence[str]) -> int:
    return subprocess.run(list(cmd), check=False).returncode


def git_info(run: Callable[[Sequence[str]], str] | None = None) -> tuple[str, str]:
    """(sha, dirty) of the host checkout."""

    def default(cmd: Sequence[str]) -> str:
        out = subprocess.run(list(cmd), capture_output=True, text=True, check=False)
        return out.stdout.strip() if out.returncode == 0 else "unavailable"

    r = run or default
    sha = r(["git", "rev-parse", "HEAD"])
    status = r(["git", "status", "--porcelain"])
    dirty = "unknown" if status == "unavailable" else str(bool(status)).lower()
    return sha, dirty


def exec_cmd(module: str, args: Sequence[str], env: Mapping[str, str] | None = None) -> list[str]:
    """``docker compose exec`` of a bench module in the running app container."""
    env_args: list[str] = []
    for key, value in {"PYTHONPATH": "/workspace/src", **(env or {})}.items():
        env_args += ["-e", f"{key}={value}"]
    return ["docker", "compose", "exec", "-T", *env_args, "app", "python", "-m", module, *args]


def resume_command(run_id: str, *, record: bool, smoke: bool, script: str) -> str:
    """The exact command that continues ``run_id``."""
    if smoke:
        return f"uv run python experiments/{script}.py --smoke --resume --run-id {run_id}"
    return f"make experiments-spark {'RECORD=1 ' if record else ''}RESUME={run_id}"


class RunError(RuntimeError):
    """A run cannot start or resume as asked."""


def container_path(host_path: Path, repo: Path) -> str:
    return str(CONTAINER_WORKSPACE / host_path.resolve().relative_to(repo.resolve()))


def hours(seconds: float) -> str:
    return f"{seconds / 3600:.1f} h ({seconds / 60:.0f} min)"


def print_estimate(records: Sequence[Mapping[str, Any]], out: Callable[[str], None]) -> None:
    """The full-grid estimate from a smoke run, and the reduced grid if it exceeds 8 h."""
    try:
        smoke = G.smoke_times(records)
    except (ValueError, KeyError) as exc:
        out(f"estimate: not available ({exc})")
        return
    full = G.GridSpec()
    est = G.estimate(full, smoke)
    out(
        f"estimate: full grid ({len(est.per_cell)} cells) ~ {hours(est.total_s)} "
        f"({est.method}; costs growing faster than the rows beyond x4 are not included)"
    )
    if est.missing:
        out(f"estimate: no smoke time for {len(est.missing)} cell(s): {', '.join(est.missing[:6])}")
    if est.total_s <= G.BUDGET_S:
        out("estimate: within the 8 h budget; run `make experiments-spark RECORD=1`")
        return
    steps = G.reduced_grid(full, smoke)
    for what, _spec, seconds in steps:
        out(f"reduced grid step: {what} -> {hours(seconds)}")
    if steps:
        final = steps[-1][1]
        args = " ".join(final.cli_args())
        out(f'proposed: make experiments-spark RECORD=1 GRID_ARGS="{args}"')
        if steps[-1][2] > G.BUDGET_S:
            out("estimate: still over 8 h after every reduction; split the run over two nights")


def _cleanup_hdfs(run_id: str, out: Callable[[str], None]) -> None:
    from f1sa.config import get_settings

    settings = get_settings()
    client = HttpFsBench(settings.hdfs_httpfs_url, settings.hdfs_user)
    try:
        if f"{HDFS_BENCH}/{run_id}" in client.children(HDFS_BENCH):
            client.delete(f"{HDFS_BENCH}/{run_id}")
            out(f"deleted hdfs {HDFS_BENCH}/{run_id}")
    except (requests.RequestException, KeyError, ValueError) as exc:
        out(f"could not delete hdfs {HDFS_BENCH}/{run_id} ({exc}); run `make bench-clean`")


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--record", action="store_true", help="write to results/ (RECORD=1)")
    p.add_argument("--run-id", default=None, help="run id (default: a new bench_<UTC time>)")
    p.add_argument(
        "--resume", action="store_true", help="continue --run-id (RESUME=<run_id> in make)"
    )
    p.add_argument("--keep-data", action="store_true", help="keep /f1/bench/<run_id>")
    p.add_argument("--docker-interval", type=float, default=2.0)
    p.add_argument("--repo", type=Path, default=Path.cwd())


def main_scalability(
    argv: Sequence[str] | None = None,
    *,
    runner: Runner = _stream,
    out: Callable[[str], None] = say,
    now: datetime | None = None,
) -> int:
    """Exit 0 when the grid is complete, 1 when cells are pending (resume it), 2 when it cannot
    start. ``results/`` is written only with ``--record`` and only when the grid is complete."""
    p = argparse.ArgumentParser(prog="scalability", description="Spark scalability grid (S10).")
    p.add_argument(
        "--smoke", action="store_true", help="1 season, x1 all masters + x4 local[4], 1 run"
    )
    _common(p)
    G.add_grid_args(p)
    args = p.parse_args(list(argv) if argv is not None else None)
    if args.smoke and args.record:
        p.error("--smoke cannot be recorded")
    if args.resume and not args.run_id:
        p.error("--resume needs --run-id (RESUME=<run_id>)")
    repo: Path = args.repo.resolve()
    now = now or datetime.now(UTC)
    run_id = args.run_id or (new_run_id(now) + ("_smoke" if args.smoke else ""))
    try:
        rdir = run_dir(run_id, repo)
        spec = _scalability_spec(args, rdir)
    except (RunError, ValueError) as exc:
        out(f"scalability: {exc}")
        return 2
    if not args.resume:
        rdir.mkdir(parents=True, exist_ok=False)
    resume_cmd = resume_command(run_id, record=args.record, smoke=args.smoke, script="scalability")
    out(f"scalability: run {run_id}, {sum(1 for _ in G.cells(spec))} cells -> {rdir}")
    out(f"scalability: to resume this run: {resume_cmd}")
    sha, dirty = git_info()
    grid_args = ["--run-dir", container_path(rdir, repo), "--src", "/workspace/src"]
    grid_args += [*spec.cli_args(), *(["--resume"] if args.resume else [])]
    cmd = exec_cmd("f1sa.bench.grid", grid_args, {"F1SA_GIT_SHA": sha, "F1SA_GIT_DIRTY": dirty})
    with DockerStatsSampler(rdir / "docker_stats.jsonl", interval_s=args.docker_interval):
        code = runner(cmd)
    csv_path = rdir / G.CSV_NAME
    rows = read_csv_rows(csv_path)
    _scalability_plots(rows, rdir)
    pending = G.pending_cells(spec, csv_path)
    if pending:
        out(
            f"scalability: incomplete (grid exit {code}): {len(pending)} of "
            f"{sum(1 for _ in G.cells(spec))} cells pending; results/ not written; "
            f"HDFS data kept. Resume with: {resume_cmd}"
        )
        return 1
    out(f"scalability: complete, {len(rows)} rows in {csv_path}")
    if not args.keep_data:
        _cleanup_hdfs(run_id, out)
    if args.record:
        dest = output_dir(run_id, record=True, base=repo)
        write_csv(dest / G.CSV_NAME, rows, SCALABILITY_COLUMNS)
        _scalability_plots(rows, dest)
        pairs = fingerprint(
            now=now, git_sha=sha, git_dirty=dirty, params={"run_id": run_id, **_spec_params(spec)}
        )
        if not fingerprinted(dest / "README.md", run_id):
            append_readme(dest / "README.md", G.CSV_NAME, pairs)
        out(f"scalability: wrote {dest / G.CSV_NAME} and the fingerprint of {run_id}")
    if args.smoke:
        print_estimate(G.read_records(rdir / G.RECORDS_NAME), out)
    return 0


def _scalability_spec(args: argparse.Namespace, rdir: Path) -> G.GridSpec:
    """The grid of a new run (from the options) or of a resumed run (from its grid_spec.json)."""
    given = SMOKE_GRID if args.smoke else G.spec_from_args(args)
    if not args.resume:
        if rdir.exists():
            raise RunError(f"{rdir} already exists; resume it with RESUME={rdir.name}")
        return given
    stored_path = rdir / G.SPEC_NAME
    if not stored_path.exists():
        raise RunError(f"nothing to resume: {stored_path} does not exist")
    stored = G.spec_from_json(json.loads(stored_path.read_text(encoding="utf-8")))
    if not args.smoke and given not in (G.GridSpec(), stored):
        raise RunError(f"GRID_ARGS differ from the grid of {rdir.name}; resume without GRID_ARGS")
    return stored


def _scalability_plots(rows: Sequence[Mapping[str, Any]], dest: Path) -> None:
    plot_scaling(
        rows,
        dest / "scalability_build.png",
        phase="build",
        x_col="laps_rows",
        x_label="lap rows (seasons x replication)",
    )
    plot_scaling(
        rows,
        dest / "scalability_telemetry.png",
        phase="telemetry",
        x_col="telemetry_rounds",
        x_label="2024 rounds of raw telemetry",
    )


def _spec_params(spec: G.GridSpec) -> dict[str, Any]:
    return {
        "seasons": "/".join(map(str, spec.seasons)),
        "factors": "/".join(map(str, spec.factors)),
        "masters": "/".join(spec.masters),
        "warmup": spec.warmup,
        "repeats": spec.repeats,
        "large_from": spec.large_from,
        "large_repeats": spec.large_repeats,
        "large_masters": "/".join(spec.large_masters) if spec.large_masters else "all",
    }


def main_partitioning(
    argv: Sequence[str] | None = None,
    *,
    runner: Runner = _stream,
    out: Callable[[str], None] = say,
    now: datetime | None = None,
) -> int:
    """Exit 0 when the partitioning run is complete, 1 when it is not (resume it), 2 when it
    cannot start. ``--run-id R`` uses ``data/bench/R_part`` (``make experiments-spark`` passes the
    scalability run id); ``--resume`` continues that folder, or starts it if it does not exist."""
    p = argparse.ArgumentParser(prog="partitioning", description="Partitioning and caching (S10).")
    p.add_argument("--smoke", action="store_true", help="laps x1, 5 telemetry rounds, 1 run")
    p.add_argument("--laps-factor", type=int, default=16)
    p.add_argument("--telemetry-rounds", type=int, default=0)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--repeats", type=int, default=5)
    _common(p)
    args = p.parse_args(list(argv) if argv is not None else None)
    if args.smoke and args.record:
        p.error("--smoke cannot be recorded")
    if args.resume and not args.run_id:
        p.error("--resume needs --run-id (RESUME=<run_id>)")
    if args.smoke:
        args.laps_factor, args.telemetry_rounds, args.warmup, args.repeats = 1, 5, 0, 1
    repo: Path = args.repo.resolve()
    now = now or datetime.now(UTC)
    base_id = args.run_id or new_run_id(now)
    run_id = f"{base_id}_{'psmoke' if args.smoke else 'part'}"
    try:
        rdir = run_dir(run_id, repo)
    except ValueError as exc:
        out(f"partitioning: {exc}")
        return 2
    resuming = args.resume and rdir.exists()
    if rdir.exists() and not resuming:
        out(f"partitioning: {rdir} already exists; resume it with RESUME={base_id}")
        return 2
    rdir.mkdir(parents=True, exist_ok=True)
    resume_cmd = resume_command(
        base_id, record=args.record, smoke=args.smoke, script="partitioning"
    )
    out(f"partitioning: run {run_id} -> {rdir}{' (resuming)' if resuming else ''}")
    out(f"partitioning: to resume this run: {resume_cmd}")
    sha, dirty = git_info()
    cargs = [
        "--run-dir",
        container_path(rdir, repo),
        "--laps-factor",
        str(args.laps_factor),
        "--telemetry-rounds",
        str(args.telemetry_rounds),
        "--warmup",
        str(args.warmup),
        "--repeats",
        str(args.repeats),
    ]
    if args.keep_data:
        cargs.append("--keep-data")
    if resuming:
        cargs.append("--resume")
    with DockerStatsSampler(rdir / "docker_stats.jsonl", interval_s=args.docker_interval):
        code = runner(exec_cmd("f1sa.bench.partitioning", cargs))
    summary_path = rdir / "partitioning_summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    raw = read_jsonl(rdir / "partitioning.jsonl")
    rows = [r | {"git_sha": sha, "git_dirty": dirty} for r in speedups(raw)]
    if not summary.get("complete"):
        out(
            f"partitioning: incomplete (exit {code}); {len(raw)} rows so far; results/ not "
            f"written; data kept. Resume with: {resume_cmd}"
        )
        return 1
    write_csv(rdir / "partitioning.csv", rows, PARTITIONING_COLUMNS)
    plot_partitioning(rows, rdir / "partitioning_speedup.png")
    out(f"partitioning: complete, wrote {rdir / 'partitioning.csv'} ({len(rows)} rows)")
    if not args.keep_data:
        _cleanup_hdfs(run_id, out)
    if args.record:
        dest = output_dir(run_id, record=True, base=repo)
        write_csv(dest / "partitioning.csv", rows, PARTITIONING_COLUMNS)
        plot_partitioning(rows, dest / "partitioning_speedup.png")
        params = {
            "run_id": run_id,
            "laps_factor": args.laps_factor,
            "telemetry_rounds": args.telemetry_rounds or "all",
            "warmup": args.warmup,
            "repeats": args.repeats,
            "master": "local[4]",
        }
        if not fingerprinted(dest / "README.md", run_id):
            append_readme(
                dest / "README.md",
                "partitioning.csv",
                fingerprint(now=now, git_sha=sha, git_dirty=dirty, params=params),
            )
        out(f"partitioning: wrote {dest / 'partitioning.csv'} and the fingerprint of {run_id}")
    if args.smoke:
        seconds = estimate_partitioning(raw, summary, laps_factor=16, telemetry_round_ratio=24 / 5)
        out(f"estimate: full partitioning run ~ {hours(seconds)} (linear in the data size)")
    return 0 if code == 0 else 1


def fingerprinted(readme: Path, run_id: str) -> bool:
    """True if ``readme`` already has a fingerprint of ``run_id`` (a resumed, already complete run
    is recorded once)."""
    return readme.exists() and f"run_id={run_id}," in readme.read_text(encoding="utf-8")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """JSON lines (a partly written last line is skipped)."""
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            with contextlib.suppress(json.JSONDecodeError):
                rows.append(json.loads(line))
    return rows


def scalability_entry() -> None:
    sys.exit(main_scalability())


def partitioning_entry() -> None:
    sys.exit(main_partitioning())
