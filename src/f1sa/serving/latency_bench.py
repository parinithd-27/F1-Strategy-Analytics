"""Redis read/write latency at streaming rates (#37), behind ``experiments/redis_latency.py``.

For each rate (micro-batches per second; default 1, 10, 60) and ``duration_s`` (default 60 s), an
absolute-schedule tick loop runs. Each tick:

* **write**: one ``RaceStore.write_batch`` for ``N_DRIVERS`` (20) drivers. That is one
  ``pipeline(transaction=False)`` with 20 x (HSET + EXPIRE), ZADD + EXPIRE and one PUBLISH, timed
  from the first queued command to the end of ``execute()``.
* **hgetall**: 20 HGETALL, one per driver, each timed on its own, on a second client.
* **zrange**: one ``ZRANGE 0 -1 WITHSCORES`` of the order, on the second client.

A tick that starts more than one period after its scheduled time is counted as ``late_ticks``.
The achieved rate is the number of ticks divided by the elapsed time. Percentiles use NumPy's
default linear interpolation.

Then ``redis-cli --latency -i 1`` runs inside the Redis container (``docker compose exec -T``) and
measures PING round trips there, with no host or Docker network hop, as a floor for the numbers
above. Without a TTY, ``redis-cli --latency -i 1`` prints one raw summary (``min max avg samples``)
after 1 s and exits. So the container runs it ``CLI_SECONDS`` (30) times back to back, and the 30
one-second summaries are combined: the overall min and max, the sample-weighted mean and the total
sample count. redis-cli gives no percentiles. If it fails, if its output cannot be parsed, or if it
gives fewer than 30 intervals, the run stops with an error, and no row is invented.

The benchmark writes only to its own race, season 2100 round 30 (drivers ``bench_01`` to
``bench_20``). It refuses to start if that race already has keys, and deletes them afterwards.
"""

from __future__ import annotations

import argparse
import csv
import os
import platform
import re
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import redis

from f1sa.config import get_settings
from f1sa.serving import keys as K
from f1sa.serving.redis_store import COMPOUNDS, Batch, DriverState, Event, RaceStore

BENCH_SEASON, BENCH_ROUND = 2100, 30
N_DRIVERS = 20
DRIVERS: tuple[str, ...] = tuple(f"bench_{i:02d}" for i in range(1, N_DRIVERS + 1))
RATES: tuple[float, ...] = (1.0, 10.0, 60.0)
DURATION_S = 60.0
CLI_SECONDS = 30
CSV_NAME = "redis_latency.csv"
README_NAME = "README.md"
COLUMNS = (
    "source",
    "rate_per_s",
    "operation",
    "n",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "mean_ms",
    "min_ms",
    "max_ms",
    "achieved_rate_per_s",
    "late_ticks",
    "duration_s",
    "drivers",
)
CLI_COMMAND: tuple[str, ...] = (
    "docker",
    "compose",
    "exec",
    "-T",
    "redis",
    "sh",
    "-c",
    f"for i in $(seq {CLI_SECONDS}); do redis-cli --latency -i 1 || exit 1; done",
)
"""``redis-cli --latency -i 1`` exits after one 1 s summary without a TTY, so it is repeated."""

_CLI_RAW = re.compile(r"^\s*(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)\s+(\d+)\s*$")
_CLI_TTY = re.compile(
    r"min:\s*(\d+(?:\.\d+)?),\s*max:\s*(\d+(?:\.\d+)?),\s*avg:\s*(\d+(?:\.\d+)?)\s*\((\d+)\s+samples\)"
)


class BenchError(RuntimeError):
    """The benchmark cannot run or its output cannot be trusted."""


def summarise_ms(values_ns: Sequence[int]) -> dict[str, float | int]:
    """n, p50/p95/p99, mean, min and max in milliseconds."""
    if not values_ns:
        raise BenchError("no samples")
    ms = np.asarray(values_ns, dtype=np.float64) / 1e6
    p50, p95, p99 = np.percentile(ms, [50, 95, 99])
    return {
        "n": int(ms.size),
        "p50_ms": float(p50),
        "p95_ms": float(p95),
        "p99_ms": float(p99),
        "mean_ms": float(ms.mean()),
        "min_ms": float(ms.min()),
        "max_ms": float(ms.max()),
    }


@dataclass(frozen=True)
class CliLatency:
    min_ms: float
    max_ms: float
    avg_ms: float
    samples: int


def parse_redis_cli(output: str) -> list[CliLatency]:
    """Every summary line of ``redis-cli --latency``: raw (``min max avg samples``, no TTY) or the
    TTY form (``min: a, max: b, avg: c (n samples)``). A line with 0 samples is an error."""
    found: list[CliLatency] = []
    for line in output.replace("\r", "\n").splitlines():
        m = _CLI_RAW.match(line) or _CLI_TTY.search(line)
        if m:
            found.append(CliLatency(float(m[1]), float(m[2]), float(m[3]), int(m[4])))
    if not found or any(c.samples < 1 for c in found):
        raise BenchError(f"cannot parse redis-cli --latency output: {output!r}")
    return found


def combine_cli(intervals: Sequence[CliLatency]) -> CliLatency:
    """Overall min and max, sample-weighted mean and total samples of several summaries."""
    if not intervals:
        raise BenchError("no redis-cli intervals")
    n = sum(c.samples for c in intervals)
    return CliLatency(
        min_ms=min(c.min_ms for c in intervals),
        max_ms=max(c.max_ms for c in intervals),
        avg_ms=sum(c.avg_ms * c.samples for c in intervals) / n,
        samples=n,
    )


def bench_state(i: int, tick: int) -> DriverState:
    """A plausible state that changes on every tick (so every HSET writes new values)."""
    return DriverState(
        position=i + 1,
        gap_ahead_s=None if i == 0 else round(0.4 + 0.1 * ((i + tick) % 17), 3),
        gap_leader_s=0.0 if i == 0 else round(1.1 * i + 0.01 * tick, 3),
        tyre_compound=COMPOUNDS[i % 3],
        tyre_age_laps=(tick + i) % 40,
        last_lap_time_s=round(90.0 + 0.05 * i + 0.001 * (tick % 100), 3),
        undercut_flag=(i + tick) % 7 == 0,
        undercut_prob=round(((i * 31 + tick) % 100) / 100.0, 2),
    )


def bench_batch(tick: int) -> Batch:
    return Batch(
        states={d: bench_state(i, tick) for i, d in enumerate(DRIVERS)},
        order={d: i + 1 for i, d in enumerate(DRIVERS)},
        events=(Event("lap", {"tick": tick}),),
    )


def run_rate(
    store: RaceStore,
    reader: redis.Redis,
    rate: float,
    duration_s: float,
    *,
    clock: Callable[[], float] = time.perf_counter,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """One rate: rows for ``write``, ``hgetall`` and ``zrange``."""
    if rate <= 0 or duration_s <= 0:
        raise BenchError(f"rate and duration must be > 0, got {rate}, {duration_s}")
    period = 1.0 / rate
    n_ticks = max(1, round(rate * duration_s))
    writes: list[int] = []
    hgetalls: list[int] = []
    zranges: list[int] = []
    late = 0
    driver_keys = [K.driver_key(BENCH_SEASON, BENCH_ROUND, d) for d in DRIVERS]
    order_key = K.order_key(BENCH_SEASON, BENCH_ROUND)
    start = clock()
    for tick in range(n_ticks):
        target = start + tick * period
        now = clock()
        if now < target:
            sleep(target - now)
        elif now - target > period:
            late += 1
        batch = bench_batch(tick)
        t0 = clock_ns()
        store.write_batch(BENCH_SEASON, BENCH_ROUND, batch)
        writes.append(clock_ns() - t0)
        for key in driver_keys:
            t0 = clock_ns()
            got = reader.hgetall(key)
            hgetalls.append(clock_ns() - t0)
            if len(got) != len(K.DRIVER_FIELDS):
                raise BenchError(f"{key} has {len(got)} fields after a write")
        t0 = clock_ns()
        order = reader.zrange(order_key, 0, -1, withscores=True)
        zranges.append(clock_ns() - t0)
        if len(order) != N_DRIVERS:
            raise BenchError(f"order has {len(order)} members after a write")
    end = start + n_ticks * period  # the run lasts its full schedule, so achieved = n / elapsed
    now = clock()
    if now < end:
        sleep(end - now)
    elapsed = max(clock(), end) - start if now < end else now - start
    common = {
        "source": "redis-py",
        "rate_per_s": rate,
        "achieved_rate_per_s": n_ticks / elapsed if elapsed > 0 else float("nan"),
        "late_ticks": late,
        "duration_s": elapsed,
        "drivers": N_DRIVERS,
    }
    return [
        {**common, "operation": op, **summarise_ms(samples)}
        for op, samples in (("write", writes), ("hgetall", hgetalls), ("zrange", zranges))
    ]


def run_redis_cli(
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """``CLI_SECONDS`` one-second ``redis-cli --latency -i 1`` intervals inside the container."""
    try:
        proc = runner(
            list(CLI_COMMAND),
            capture_output=True,
            text=True,
            timeout=CLI_SECONDS * 3 + 30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise BenchError(f"redis-cli --latency did not run: {exc}") from exc
    if proc.returncode != 0:
        raise BenchError(f"redis-cli --latency exited {proc.returncode}: {proc.stderr.strip()}")
    intervals = parse_redis_cli(proc.stdout)
    if len(intervals) != CLI_SECONDS:
        raise BenchError(f"expected {CLI_SECONDS} redis-cli intervals, got {len(intervals)}")
    cli = combine_cli(intervals)
    return {
        "source": f"redis-cli --latency -i 1 x {CLI_SECONDS} (in container)",
        "rate_per_s": "",
        "operation": "ping",
        "n": cli.samples,
        "p50_ms": "",
        "p95_ms": "",
        "p99_ms": "",
        "mean_ms": cli.avg_ms,
        "min_ms": cli.min_ms,
        "max_ms": cli.max_ms,
        "achieved_rate_per_s": "",
        "late_ticks": "",
        "duration_s": CLI_SECONDS,
        "drivers": "",
    }


def check_results_dir(results_dir: Path) -> None:
    allowed = (Path.cwd() / "results").resolve()
    target = results_dir.resolve()
    if target != allowed and allowed not in target.parents:
        raise BenchError(f"results dir {results_dir} is outside {allowed}")


def _fmt(v: Any) -> Any:
    return f"{v:.4f}" if isinstance(v, float) else v


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(COLUMNS), lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: _fmt(r.get(c, "")) for c in COLUMNS})
    tmp.replace(path)


def _run(cmd: Sequence[str]) -> str:
    try:
        out = subprocess.run(list(cmd), capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"
    return out.stdout.strip() if out.returncode == 0 and out.stdout.strip() else "unavailable"


def _cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unavailable"


def _mem_total() -> str:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return f"{int(line.split()[1]) / 1024 / 1024:.1f} GiB"
    except (OSError, ValueError):
        pass
    return "unavailable"


def _container_mem(run: Callable[[Sequence[str]], str]) -> str:
    cid = run(["docker", "compose", "ps", "-q", "redis"])
    if cid == "unavailable":
        return cid
    return run(["docker", "inspect", "-f", "{{.HostConfig.Memory}}", cid.splitlines()[0]])


def fingerprint(
    client: redis.Redis,
    *,
    now: datetime,
    run: Callable[[Sequence[str]], str] = _run,
    params: Mapping[str, Any],
) -> list[tuple[str, str]]:
    """``(name, value)`` pairs describing where the numbers were measured."""
    try:
        info: Mapping[str, Any] = client.info("server")
    except redis.ResponseError:  # a server without INFO (e.g. fakeredis in tests)
        info = {}
    kw = client.connection_pool.connection_kwargs
    dirty = run(["git", "status", "--porcelain"])
    return [
        ("measured_at_utc", now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("git_sha", run(["git", "rev-parse", "HEAD"])),
        ("git_dirty", "unknown" if dirty == "unavailable" else str(bool(dirty)).lower()),
        ("python", platform.python_version()),
        ("redis_py", redis.__version__),
        ("redis_server", str(info.get("redis_version", "unavailable"))),
        ("redis_mode", f"{info.get('redis_mode', '?')}, {info.get('os', '?')}"),
        ("os", f"{platform.system()} {platform.release()}"),
        ("cpu", f"{_cpu_model()} ({os.cpu_count()} logical CPUs)"),
        ("memory", _mem_total()),
        ("redis_container_mem_limit_bytes", _container_mem(run)),
        ("client_endpoint", f"{kw.get('host', '?')}:{kw.get('port', '?')} db {kw.get('db', '?')}"),
        ("params", ", ".join(f"{k}={v}" for k, v in params.items())),
    ]


def append_readme(path: Path, csv_name: str, pairs: Sequence[tuple[str, str]]) -> None:
    """Append a fingerprint section; the file is created with a heading if it does not exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", encoding="utf-8") as fh:
        if new:
            fh.write(
                "# Results\n\nEnvironment fingerprints of the experiments that wrote files here.\n"
            )
        fh.write(f"\n## `{csv_name}` ({pairs[0][1]})\n\n| Item | Value |\n|---|---|\n")
        for k, v in pairs:
            fh.write(f"| {k} | {v.replace('|', '/')} |\n")


def cleanup(store: RaceStore) -> int:
    """Expire, then delete, every benchmark key."""
    store.expire_race(BENCH_SEASON, BENCH_ROUND)
    keys = store.race_keys(BENCH_SEASON, BENCH_ROUND)
    if keys:
        store.client.delete(*keys)
    return len(keys)


def run_benchmark(
    store: RaceStore,
    reader: redis.Redis,
    *,
    rates: Sequence[float] = RATES,
    duration_s: float = DURATION_S,
    with_cli: bool = True,
    cli_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    out: Callable[[str], None] = print,
) -> list[dict[str, Any]]:
    if store.race_keys(BENCH_SEASON, BENCH_ROUND):
        raise BenchError(
            f"race:{BENCH_SEASON}:{BENCH_ROUND}:* already has keys; refusing to overwrite them"
        )
    rows: list[dict[str, Any]] = []
    try:
        for rate in rates:
            out(f"rate {rate:g}/s for {duration_s:g} s ...")
            rate_rows = run_rate(store, reader, rate, duration_s)
            for r in rate_rows:
                out(
                    f"  {r['operation']:8s} n={r['n']:6d} p50={r['p50_ms']:.3f} "
                    f"p95={r['p95_ms']:.3f} p99={r['p99_ms']:.3f} ms "
                    f"(achieved {r['achieved_rate_per_s']:.2f}/s, late {r['late_ticks']})"
                )
            rows += rate_rows
    finally:
        cleanup(store)
    if with_cli:
        out(f"redis-cli --latency -i 1, {CLI_SECONDS} one-second intervals in the container ...")
        row = run_redis_cli(cli_runner)
        out(
            f"  ping min={row['min_ms']} avg={row['mean_ms']} max={row['max_ms']} ms (n={row['n']})"
        )
        rows.append(row)
    return rows


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--rates", type=float, nargs="+", default=list(RATES))
    ap.add_argument("--duration", type=float, default=DURATION_S)
    ap.add_argument("--skip-cli", action="store_true", help="skip redis-cli --latency")
    ap.add_argument("--results-dir", type=Path, default=Path("results"))
    args = ap.parse_args(argv)
    try:
        check_results_dir(args.results_dir)
        settings = get_settings()
        store = RaceStore.from_settings(settings)
        reader = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        try:
            store.client.ping()
            rows = run_benchmark(
                store,
                reader,
                rates=args.rates,
                duration_s=args.duration,
                with_cli=not args.skip_cli,
            )
            csv_path = args.results_dir / CSV_NAME
            write_csv(csv_path, rows)
            params = {
                "rates_per_s": "/".join(f"{r:g}" for r in args.rates),
                "duration_s": f"{args.duration:g}",
                "drivers": N_DRIVERS,
                "redis_cli": "skipped" if args.skip_cli else f"{CLI_SECONDS} x 1 s intervals",
            }
            append_readme(
                args.results_dir / README_NAME,
                CSV_NAME,
                fingerprint(store.client, now=datetime.now(UTC), params=params),
            )
            print(f"wrote {csv_path} and appended to {args.results_dir / README_NAME}")
        finally:
            store.client.close()
            reader.close()
    except (BenchError, K.InvalidKeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except redis.RedisError as exc:
        print(f"error: Redis at the configured REDIS_URL is not reachable: {exc}", file=sys.stderr)
        return 3
    return 0
