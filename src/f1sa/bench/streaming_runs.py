"""Streaming latency and throughput on a quiet stack (Step 16, traceability #31, #35, #36, #38).

``make stream-latency-suite [RECORD=1]`` replays 2024 R5 from the sample lake on the host (real
Jolpica laps and pit stops, synthetic tyres) three times, one after another, through Kafka,
Structured Streaming, Redis and the API's SSE stream (``f1sa.streaming.demo`` with ``--sse-url``):

* ``s10``: speed 10, the full race;
* ``s60``: speed 60, the full race;
* ``s1w900``: speed 1, the first 900 wall seconds (a 15-minute window).

``make stream-latency-ui [RECORD=1]`` adds one more speed-60 run (``s60ui``) while a gated
Playwright spec keeps the dashboard open (``NEXT_PUBLIC_DEBUG_LATENCY=1``, API ``ENABLE_DEBUG=1``),
and joins the UI render samples to the run by event id. The page posts at most 50 samples at once
and drops ids beyond that, so the UI samples are a subset of the events.

Per run: the segments produced -> batch -> Redis -> Pub/Sub subscriber and Redis -> SSE client as
p50/p95/p99/max, for all messages, the first micro-batch of the race (which loads the priors with
Spark) and the rest; throughput (messages/s and lap messages/s received, events/s produced); peak
memory and CPU per container (``docker stats``, 2 s) and of the host job's process tree (psutil:
the demo, the stream job's JVM and the producer). Every timestamp comes from one kernel clock.

Outputs go to ``data/demo/<suite_id>/`` only. ``--record`` copies them to NEW names in ``results/``
(never the Step 12 ``streaming_latency.csv`` / ``stream_*.jsonl``), refuses if any target exists,
and appends one fingerprint section per recorded file to ``results/README.md``. Services are never
started, stopped or recreated here.
"""

from __future__ import annotations

import argparse
import csv
import os
import platform
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from f1sa.streaming import latency as L

SEASON, ROUND = 2024, 5
DEMO_ROOT = Path("data/demo")
RESULTS_DIR = Path("results")
README_NAME = "README.md"
SSE_URL = "http://localhost:8000/api/races/{season}/{round}/events"
API_URL = "http://localhost:8000"
UI_CSV = Path("data/demo/ui_latency/ui_latency.csv")
"""Host path of the debug API's sample file: start the API with
``make api-up DEBUG=1 UI_LATENCY_CSV=data/demo/ui_latency/ui_latency.csv``."""
PAUSE_S = 20.0
DEMO_SCRIPT = Path(__file__).resolve().parents[3] / "experiments" / "streaming_latency.py"
"""The demo entry point (an explicit ``--results-dir`` is passed through unchanged)."""
CLOCK_NOTE = "producer, stream job, subscribers and API on one kernel clock (WSL2 + Docker Desktop)"


@dataclass(frozen=True)
class RunSpec:
    tag: str
    speed: float
    window_s: float | None = None


SUITE: tuple[RunSpec, ...] = (
    RunSpec("s10", 10.0),
    RunSpec("s60", 60.0),
    RunSpec("s1w900", 1.0, 900.0),
)
UI_RUN = RunSpec("s60ui", 60.0)

RESOURCE_COLUMNS: tuple[str, ...] = (
    "run", "scope", "name", "samples", "peak_mem_bytes", "peak_cpu_pct", "mean_cpu_pct",
    "peak_rss_bytes", "min_host_available_bytes", "window_start_utc", "window_s",
)  # fmt: skip
UI_COLUMNS: tuple[str, ...] = (
    "event_id", "event_type", "produced_at_ms", "batch_received_at_ms", "redis_written_at_ms",
    "sse_sent_at_ms", "ui_rendered_at_ms", "recorded_at_ms", "sse_to_ui_ms", "written_to_ui_ms",
    "produced_to_ui_ms", "harness_matched", "harness_sse_received_at_ms",
)  # fmt: skip
UI_SUMMARY_COLUMNS: tuple[str, ...] = (
    "run", "segment", "n", "p50_ms", "p95_ms", "p99_ms", "max_ms", "events_in_run",
    "samples", "subset_fraction", "ui_subset", "note",
)  # fmt: skip
UI_NOTE = (
    "UI samples may be a SUBSET: the page posts at most 50 samples at once and drops ids beyond "
    "that (subset_fraction = samples / messages of the run); all ids applied in one render share "
    "one paint time; alerts and session_end carry API-made ids, so only lap/pit samples join the "
    "harness rows by event_id"
)


class SuiteError(RuntimeError):
    """A run is refused or failed."""


# ------------------------------------------------------------------ names
def record_names(tag: str) -> dict[str, str]:
    """Run-folder file -> new ``results/`` name (never a Step 12 name)."""
    return {
        "streaming_latency_sse.csv": f"streaming_latency_{tag}.csv",
        "stream_batches.jsonl": f"stream_batches_{tag}.jsonl",
        "stream_progress.jsonl": f"stream_progress_{tag}.jsonl",
    }


SUITE_FILES = (
    "streaming_latency_summary.csv",
    "streaming_throughput.csv",
    "streaming_resources.csv",
)
UI_FILES = ("ui_latency.csv", "ui_latency_summary.csv")
STEP12_FILES = ("streaming_latency.csv", "stream_batches.jsonl", "stream_progress.jsonl")


def record_targets(runs: Sequence[RunSpec], ui: bool) -> list[str]:
    names = [n for r in runs for n in record_names(r.tag).values()]
    names += [f"ui_{n}" if ui else n for n in SUITE_FILES]
    if ui:
        names += list(UI_FILES)
    if set(names) & set(STEP12_FILES):
        raise SuiteError("a record name collides with a Step 12 file")
    return names


def check_record(results_dir: Path, names: Sequence[str]) -> None:
    existing = [n for n in names if (results_dir / n).exists()]
    if existing:
        raise SuiteError(f"{existing} exist in {results_dir} and are never overwritten")


# ------------------------------------------------------------------ one run
def demo_command(spec: RunSpec, out_dir: Path, sse_url: str) -> list[str]:
    cmd = [
        sys.executable, str(DEMO_SCRIPT), "--season", str(SEASON), "--round",
        str(ROUND), "--speed", f"{spec.speed:g}", "--results-dir", str(out_dir),
        "--sse-url", sse_url,
    ]  # fmt: skip
    if spec.window_s is not None:
        cmd += ["--window-s", f"{spec.window_s:g}"]
    return cmd


@dataclass
class RunResult:
    spec: RunSpec
    out_dir: Path
    rc: int
    start: float
    end: float
    process: dict[str, Any]


Launcher = Callable[[list[str]], Any]


def _popen(cmd: list[str]) -> Any:
    return subprocess.Popen(cmd)


def run_one(
    spec: RunSpec,
    suite_dir: Path,
    *,
    sse_url: str,
    launch: Launcher = _popen,
    sampler: Callable[[int], Any] | None = None,
    clock: Callable[[], float] = time.time,
    out: Callable[[str], None] = print,
) -> RunResult:
    """One demo run in ``suite_dir/<tag>/`` with the host job's process tree sampled."""
    out_dir = suite_dir / spec.tag
    out_dir.mkdir(parents=True, exist_ok=False)
    cmd = demo_command(spec, out_dir, sse_url)
    out(f"== {spec.tag}: {' '.join(cmd)}")
    start = clock()
    proc = launch(cmd)
    if sampler is None:
        from f1sa.bench.resources import ProcessSampler

        s = ProcessSampler(int(proc.pid), interval_s=0.5).start()
    else:
        s = sampler(int(proc.pid))
    rc = int(proc.wait())
    peaks = s.stop()
    end = clock()
    return RunResult(spec, out_dir, rc, start, end, asdict(peaks))


def run_rows(result: RunResult) -> list[dict[str, Any]]:
    path = result.out_dir / "streaming_latency_sse.csv"
    if not path.is_file():
        raise SuiteError(f"{result.spec.tag}: {path} missing (exit {result.rc})")
    return L.read_rows(path)


def resource_rows(result: RunResult, containers: Mapping[str, Any]) -> list[dict[str, Any]]:
    started = datetime.fromtimestamp(result.start, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    base = {"run": result.spec.tag, "window_start_utc": started,
            "window_s": round(result.end - result.start, 1)}  # fmt: skip
    p = result.process
    rows = [
        {
            **base,
            "scope": "host_job",
            "name": "demo + stream job JVM + producer (psutil tree)",
            "samples": p.get("samples"),
            "peak_cpu_pct": p.get("peak_cpu_pct"),
            "mean_cpu_pct": p.get("mean_cpu_pct"),
            "peak_rss_bytes": p.get("peak_rss_bytes"),
            "min_host_available_bytes": p.get("min_host_available_bytes"),
        }
    ]
    for name, c in sorted(containers.items()):
        rows.append(
            {
                **base,
                "scope": "container",
                "name": name,
                "samples": c.samples,
                "peak_mem_bytes": c.peak_mem_bytes,
                "peak_cpu_pct": c.peak_cpu_pct,
            }
        )
    return rows


def write_csv(path: Path, rows: Iterable[Mapping[str, Any]], columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(columns), lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: "" if r.get(c) is None else r[c] for c in columns})


# ------------------------------------------------------------------ UI join
def ui_rows(ui_csv: Path, harness: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The API's UI samples, with the UI segments and the harness row of the same event id."""
    with ui_csv.open(encoding="utf-8", newline="") as fh:
        samples = list(csv.DictReader(fh))
    by_id: dict[str, Mapping[str, Any]] = {}
    for r in harness:
        if r.get("event_id"):
            by_id.setdefault(str(r["event_id"]), r)

    def num(v: Any) -> int | None:
        return int(v) if v not in (None, "") else None

    out = []
    for s in samples:
        ui, sent = num(s.get("ui_rendered_at_ms")), num(s.get("sse_sent_at_ms"))
        written, produced = num(s.get("redis_written_at_ms")), num(s.get("produced_at_ms"))
        h = by_id.get(str(s.get("event_id")))
        out.append(
            {
                **{c: s.get(c, "") for c in UI_COLUMNS[:8]},
                "sse_to_ui_ms": ui - sent if ui is not None and sent is not None else None,
                "written_to_ui_ms": ui - written
                if ui is not None and written is not None
                else None,
                "produced_to_ui_ms": ui - produced
                if ui is not None and produced is not None
                else None,
                "harness_matched": h is not None,
                "harness_sse_received_at_ms": h.get("sse_received_at_ms") if h else None,
            }
        )
    return out


def ui_summary(
    rows: Sequence[Mapping[str, Any]], harness: Sequence[Mapping[str, Any]], run: str
) -> list[dict[str, Any]]:
    events = len(harness)  # every published message: the page samples alerts and session_end too
    out = []
    for seg in ("sse_to_ui_ms", "written_to_ui_ms", "produced_to_ui_ms"):
        vals = [float(r[seg]) for r in rows if r.get(seg) is not None]
        if not vals:
            continue
        out.append(
            {
                "run": run,
                "segment": seg,
                "n": len(vals),
                "p50_ms": round(L.percentile(vals, 50), 1),
                "p95_ms": round(L.percentile(vals, 95), 1),
                "p99_ms": round(L.percentile(vals, 99), 1),
                "max_ms": round(max(vals), 1),
                "events_in_run": events,
                "samples": len(rows),
                "subset_fraction": round(len(rows) / events, 3) if events else "",
                "ui_subset": "true",
                "note": UI_NOTE,
            }
        )
    return out


def resummarise_ui(suite_dir: Path) -> list[dict[str, Any]]:
    """Rebuild ``ui_latency.csv`` and ``ui_latency_summary.csv`` of a UI suite folder from its raw
    API samples and the run's rows (no measurement)."""
    harness = L.read_rows(suite_dir / UI_RUN.tag / "streaming_latency_sse.csv")
    joined = ui_rows(suite_dir / "ui_latency_raw.csv", harness)
    write_csv(suite_dir / "ui_latency.csv", joined, UI_COLUMNS)
    summary = ui_summary(joined, harness, UI_RUN.tag)
    write_csv(suite_dir / "ui_latency_summary.csv", summary, UI_SUMMARY_COLUMNS)
    return summary


def check_debug_api(api_url: str = API_URL) -> None:
    """The API must have ENABLE_DEBUG=1: an unknown event id gets 404 (403 when debug is off)."""
    import requests

    try:
        r = requests.post(
            f"{api_url}/api/debug/latency",
            json={"event_id": "00000000-0000-4000-8000-000000000000", "ui_rendered_at_ms": 1},
            timeout=5,
        )
    except requests.RequestException as exc:
        raise SuiteError(f"API at {api_url} is not reachable: {exc}") from exc
    if r.status_code == 403:
        raise SuiteError("the API's debug endpoint is off: make api-up DEBUG=1 UI_LATENCY_CSV=...")
    if r.status_code != 404:
        raise SuiteError(f"unexpected debug probe answer {r.status_code}")


def port_free(port: int, host: str = "127.0.0.1") -> bool:
    import socket

    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) != 0


# ------------------------------------------------------------------ fingerprint
def fingerprint(now: datetime, params: Mapping[str, Any]) -> list[tuple[str, str]]:
    from f1sa.serving.latency_bench import _cpu_model, _mem_total, _run

    dirty = _run(["git", "status", "--porcelain"])
    names = _run(["docker", "ps", "--format", "{{.Names}}"])
    return [
        ("measured_at_utc", now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("git_sha", _run(["git", "rev-parse", "HEAD"])),
        ("git_dirty", "unknown" if dirty == "unavailable" else str(bool(dirty)).lower()),
        ("python", platform.python_version()),
        ("os", f"{platform.system()} {platform.release()}"),
        ("cpu", f"{_cpu_model()} ({os.cpu_count()} logical CPUs)"),
        ("memory", _mem_total()),
        (
            "containers_running",
            ", ".join(sorted(names.split())) if names != "unavailable" else names,
        ),
        ("clock", CLOCK_NOTE),
        ("params", ", ".join(f"{k}={v}" for k, v in params.items())),
    ]


def record(
    suite_dir: Path,
    results: Sequence[RunResult],
    ui: bool,
    pairs: Sequence[tuple[str, str]],
    results_dir: Path = RESULTS_DIR,
) -> list[str]:
    """Copy the suite's files to their new ``results/`` names and append one fingerprint section
    per file; returns the names written."""
    from f1sa.serving.latency_bench import append_readme

    names = record_targets([r.spec for r in results], ui)
    check_record(results_dir, names)
    written = []
    for r in results:
        for src, dst in record_names(r.spec.tag).items():
            shutil.copyfile(r.out_dir / src, results_dir / dst)
            written.append(dst)
    for n in SUITE_FILES:
        dst = f"ui_{n}" if ui else n
        shutil.copyfile(suite_dir / n, results_dir / dst)
        written.append(dst)
    if ui:
        for n in UI_FILES:
            shutil.copyfile(suite_dir / n, results_dir / n)
            written.append(n)
    for n in written:
        append_readme(results_dir / README_NAME, n, pairs)
    return written


# ------------------------------------------------------------------ suite
def summarise(
    suite_dir: Path, results: Sequence[RunResult], docker_jsonl: Path, prefix: str = ""
) -> dict[str, list[dict[str, Any]]]:
    from f1sa.bench.resources import read_container_samples, window_peaks

    samples = read_container_samples(docker_jsonl)
    seg, thr, res = [], [], []
    for r in results:
        rows = run_rows(r)
        seg += L.segment_summary(rows, r.spec.tag, r.spec.speed, r.spec.window_s)
        thr.append(L.throughput_row(rows, r.spec.tag, r.spec.speed, r.spec.window_s))
        res += resource_rows(r, window_peaks(samples, r.start, r.end))
    write_csv(suite_dir / f"{prefix}streaming_latency_summary.csv", seg, L.SUMMARY_COLUMNS)
    write_csv(suite_dir / f"{prefix}streaming_throughput.csv", thr, L.THROUGHPUT_COLUMNS)
    write_csv(suite_dir / f"{prefix}streaming_resources.csv", res, RESOURCE_COLUMNS)
    return {"segments": seg, "throughput": thr, "resources": res}


def print_summary(
    tables: Mapping[str, Sequence[Mapping[str, Any]]], out: Callable[[str], None]
) -> None:
    for r in tables["segments"]:
        if r["scope"] in ("all", "first_batch", "steady"):
            out(
                f"  {r['run']:7s} {r['scope']:11s} {r['segment']:22s} n={r['n']:5d} "
                f"p50={r['p50_ms']:8.1f} p95={r['p95_ms']:8.1f} p99={r['p99_ms']:8.1f} ms"
            )
    for r in tables["throughput"]:
        out(f"  {r['run']:7s} throughput: {r['messages_per_s']} msg/s, {r['lap_messages_per_s']} "
            f"lap msg/s received; produced {r['produced_per_s']} events/s")  # fmt: skip
    for r in tables["resources"]:
        mem = r.get("peak_mem_bytes") or r.get("peak_rss_bytes")
        mem_txt = f"{int(mem) / 2**20:.0f} MiB" if mem else "n/a"
        out(f"  {r['run']:7s} {r['name'][:46]:46s} peak mem {mem_txt:>9s} "
            f"peak cpu {r.get('peak_cpu_pct')}%")  # fmt: skip


def describe(r: RunSpec) -> str:
    return f"{r.tag} speed {r.speed:g}" + (
        f" window {r.window_s:g} s" if r.window_s else " full race"
    )


def suite_id(now: datetime, ui: bool) -> str:
    return f"streamlat_{now.strftime('%Y%m%dT%H%M%SZ')}" + ("_ui" if ui else "")


def main(argv: Iterable[str] | None = None, *, out: Callable[[str], None] = print) -> int:
    p = argparse.ArgumentParser(prog="experiments/streaming_latency.py --suite|--ui")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--suite", action="store_true", help="s10, s60 and s1w900")
    mode.add_argument("--ui", action="store_true", help="one s60 run with the dashboard open")
    p.add_argument("--record", action="store_true", help="copy to new results/ names")
    p.add_argument("--only", nargs="+", default=None, help="suite: run only these tags")
    p.add_argument("--pause-s", type=float, default=PAUSE_S)
    p.add_argument("--ui-csv", type=Path, default=UI_CSV)
    args = p.parse_args(list(argv) if argv is not None else None)
    runs = [UI_RUN] if args.ui else [r for r in SUITE if not args.only or r.tag in args.only]
    now = datetime.now(UTC)
    try:
        if args.record:
            check_record(RESULTS_DIR, record_targets(runs, args.ui))
        if args.ui:
            check_debug_api()
            if args.ui_csv.exists() and args.ui_csv.stat().st_size > 0:
                raise SuiteError(f"{args.ui_csv} already has samples; move it away first")
            if not port_free(3000):
                raise SuiteError(
                    "port 3000 is busy: the web container must be stopped for the UI run"
                )
    except SuiteError as exc:
        out(f"refused: {exc}")
        return 2
    sid = suite_id(now, args.ui)
    suite_dir = DEMO_ROOT / sid
    suite_dir.mkdir(parents=True)
    sse_url = SSE_URL.format(season=SEASON, round=ROUND)
    out(f"suite {sid}: {[r.tag for r in runs]} -> {suite_dir}")
    from f1sa.bench.resources import DockerStatsSampler

    docker_jsonl = suite_dir / "docker_stats.jsonl"
    playwright: Any = None
    results: list[RunResult] = []
    with DockerStatsSampler(docker_jsonl, interval_s=2.0):
        if args.ui:
            playwright = start_ui(suite_dir, out)
        for i, spec in enumerate(runs):
            if i:
                time.sleep(args.pause_s)
            results.append(run_one(spec, suite_dir, sse_url=sse_url, out=out))
        if playwright is not None:
            try:
                rc = playwright.wait(timeout=300)
            except subprocess.TimeoutExpired:
                playwright.terminate()
                rc = -1
            out(f"playwright exit {rc}")
    failed = [r.spec.tag for r in results if r.rc != 0]
    if failed:
        out(f"error: runs {failed} failed; nothing recorded (outputs in {suite_dir})")
        return 1
    tables = summarise(suite_dir, results, docker_jsonl, prefix="")
    print_summary(tables, out)
    if args.ui:
        harness = run_rows(results[0])
        if not args.ui_csv.is_file():
            out(f"error: no UI samples at {args.ui_csv}")
            return 1
        shutil.copyfile(args.ui_csv, suite_dir / "ui_latency_raw.csv")
        joined = ui_rows(args.ui_csv, harness)
        write_csv(suite_dir / "ui_latency.csv", joined, UI_COLUMNS)
        summary = ui_summary(joined, harness, UI_RUN.tag)
        write_csv(suite_dir / "ui_latency_summary.csv", summary, UI_SUMMARY_COLUMNS)
        matched = sum(bool(r["harness_matched"]) for r in joined)
        out(f"UI samples: {len(joined)} ({matched} joined to the harness by event_id); {UI_NOTE}")
        for r in summary:
            out(
                f"  ui {r['segment']:18s} n={r['n']} p50={r['p50_ms']} p95={r['p95_ms']} "
                f"p99={r['p99_ms']} ms (subset {r['subset_fraction']} of {r['events_in_run']})"
            )
        if playwright is not None and playwright.returncode != 0:
            out("error: the Playwright spec failed; nothing recorded")
            return 1
    if args.record:
        params = {
            "race": f"{SEASON} R{ROUND} (sample lake: real Jolpica laps/pit stops, "
            "synthetic tyres)",
            "runs": "; ".join(describe(r) for r in runs),
            "segments": "produced->batch->Redis->Pub/Sub and Redis->SSE (host client)",
            "resources": "docker stats 2 s per container; psutil 0.5 s host job tree",
            "suite": str(suite_dir),
        }
        written = record(suite_dir, results, args.ui, fingerprint(now, params))
        out(f"recorded {written}; fingerprints appended to results/{README_NAME}")
    return 0


def start_ui(suite_dir: Path, out: Callable[[str], None], web: Path = Path("web")) -> Any:
    """Build the dashboard with the latency reporter on, start the gated Playwright spec (which
    starts ``next start``) and wait until it reports the page live."""
    env = {
        **os.environ,
        "NEXT_TELEMETRY_DISABLED": "1",
        "NEXT_PUBLIC_API_URL": API_URL,
        "NEXT_PUBLIC_DEBUG_LATENCY": "1",
    }
    out("building the dashboard with NEXT_PUBLIC_DEBUG_LATENCY=1 ...")
    subprocess.run(["npm", "run", "build"], cwd=web, env=env, check=True)
    ready = (suite_dir / "ui_ready").resolve()
    proc = subprocess.Popen(
        ["npx", "playwright", "test", "e2e/latency.spec.ts"],
        cwd=web,
        env={**env, "E2E_LATENCY": "1", "E2E_READY_FILE": str(ready)},
    )
    deadline = time.monotonic() + 240
    while not ready.exists():
        if proc.poll() is not None:
            raise SuiteError(f"the Playwright spec exited ({proc.returncode}) before it was ready")
        if time.monotonic() > deadline:
            proc.terminate()
            raise SuiteError("the dashboard was not live after 240 s")
        time.sleep(1)
    out("dashboard live (SSE connected); starting the replay")
    return proc


if __name__ == "__main__":
    sys.exit(main())
