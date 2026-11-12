"""Replay one race end to end and measure its latency (``make stream-demo``, #31).

    make up                                            # kafka + redis must be running
    make stream-demo SEASON=2024 ROUND=5 SPEED=10      # sample lake, all on the WSL host
    make stream-demo ... FULL=1                        # HDFS lake, all inside the app container

Steps: check that Redis and the Kafka topic answer (services are never started or stopped);
subscribe to the race's Pub/Sub channel; start the stream job with a fresh checkpoint
``data/checkpoints/demo_<S>_<R>_<unix>`` and wait until its query has run its first trigger
(``startingOffsets=latest`` must be resolved before the first event is sent); run the producer;
wait for the job to process ``session_end``; write ``results/streaming_latency.csv`` and print the
latency per segment, the throughput and the Redis state. Producer, job and subscriber run on one
host, so every timestamp comes from one clock.

Exit codes: 0 ok, 1 a step failed, 2 a service is not reachable.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

from f1sa.config import Settings, get_settings
from f1sa.streaming import latency as L

READY_TIMEOUT_S = 300.0
"""The first run on the host resolves the Kafka connector from Maven (once), then Spark starts."""


class DemoError(RuntimeError):
    pass


def job_command(
    season: int, round_: int, checkpoint: Path, ready: Path, results: Path, timeout_s: float
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "f1sa.streaming.stream_job",
        "--checkpoint",
        str(checkpoint),
        "--stop-after-session-end",
        f"{season}:{round_}",
        "--ready-file",
        str(ready),
        "--results-dir",
        str(results),
        "--timeout-s",
        str(int(timeout_s)),
    ]


def producer_command(season: int, round_: int, speed: float) -> list[str]:
    return [
        sys.executable,
        "-m",
        "f1sa.streaming.producer",
        "--season",
        str(season),
        "--round",
        str(round_),
        "--speed",
        f"{speed:g}",
    ]


def check_services(settings: Settings) -> int:
    """Ping Redis and look up the topic; returns the topic's partition count."""
    import redis

    from f1sa.streaming.producer import ProducerError, topic_partitions

    try:
        redis.Redis.from_url(settings.redis_url, socket_timeout=5).ping()
    except redis.RedisError as exc:
        raise DemoError(
            f"Redis at {settings.redis_url} is not reachable ({exc}); run make up"
        ) from exc
    try:
        return topic_partitions(settings.kafka_bootstrap, settings.kafka_topic_events)
    except Exception as exc:  # confluent-kafka raises KafkaException on timeouts
        if isinstance(exc, ProducerError):
            raise DemoError(f"{exc}; run make up") from exc
        raise DemoError(f"Kafka at {settings.kafka_bootstrap} is not reachable ({exc})") from exc


def wait_ready(
    proc: Any, ready: Path, timeout_s: float, *, sleep: Callable[[float], None] = time.sleep
) -> None:
    """Wait until ``ready`` exists; fail if the job exits first or the timeout passes."""
    deadline = time.monotonic() + timeout_s
    while not ready.exists():
        if proc.poll() is not None:
            raise DemoError(f"the stream job exited with {proc.returncode} before it was ready")
        if time.monotonic() > deadline:
            raise DemoError(f"the stream job was not ready after {timeout_s:.0f} s")
        sleep(0.5)


def report(rows: Sequence[dict[str, Any]], out: Callable[[str], None] = print) -> None:
    out(f"{len(rows)} messages received")
    counts: dict[str, int] = {}
    for r in rows:
        counts[str(r["type"])] = counts.get(str(r["type"]), 0) + 1
    out("  by type: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    reasons: dict[str, int] = {}
    for r in rows:
        if r["type"] == "undercut_alert":
            key = f"{r.get('reason')}/{r.get('trigger')}"
            reasons[key] = reasons.get(key, 0) + 1
    if reasons:
        out("  alerts: " + ", ".join(f"{k}={v}" for k, v in sorted(reasons.items())))
    for s in L.summarise(rows):
        out(
            f"  {s.segment:24s} n={s.n:5d}  p50={s.p50:8.1f}  p95={s.p95:8.1f}  p99={s.p99:8.1f} ms"
        )
    tp = L.throughput(rows)
    if tp is not None:
        out(f"  lap messages received: {tp:.1f}/s")


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="f1sa-stream-demo", description=__doc__.split("\n\n")[0])
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--round", type=int, required=True)
    p.add_argument("--speed", type=float, default=10.0)
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    p.add_argument(
        "--race-seconds", type=float, default=7200.0, help="upper bound of the race span"
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(
    argv: Iterable[str] | None = None,
    *,
    popen: Callable[[list[str]], Any] = subprocess.Popen,
    run: Callable[[list[str]], int] = lambda cmd: subprocess.run(cmd, check=False).returncode,
    check: Callable[[Settings], int] = check_services,
    subscriber: Callable[[Settings, int, int, float], list[dict[str, Any]]] | None = None,
    state: Callable[[Settings, int, int], str] | None = None,
) -> int:
    args = parse_args(argv)
    if not args.speed > 0:
        print("error: --speed must be > 0", file=sys.stderr)
        return 2
    settings = get_settings()
    try:
        n = check(settings)
    except DemoError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    timeout = args.race_seconds / args.speed + READY_TIMEOUT_S
    name = f"demo_{args.season}_{args.round}_{int(time.time())}"
    checkpoint = Path("data/checkpoints") / name
    ready = Path("data/checkpoints") / f"{name}.ready"
    print(f"topic {settings.kafka_topic_events}: {n} partitions; checkpoint {checkpoint}")

    rows: list[dict[str, Any]] = []
    sub = subscriber or _redis_subscriber
    listener = threading.Thread(
        target=lambda: rows.extend(sub(settings, args.season, args.round, timeout)), daemon=True
    )
    listener.start()
    job = popen(job_command(args.season, args.round, checkpoint, ready, args.results_dir, timeout))
    try:
        wait_ready(job, ready, READY_TIMEOUT_S)
        rc = run(producer_command(args.season, args.round, args.speed))
        if rc != 0:
            raise DemoError(f"the producer failed with exit code {rc}")
        job_rc = job.wait(timeout=timeout)
        if job_rc != 0:
            raise DemoError(f"the stream job failed with exit code {job_rc}")
        listener.join(timeout=60)
    except (DemoError, subprocess.TimeoutExpired) as exc:
        print(f"error: {exc}", file=sys.stderr)
        if job.poll() is None:
            job.terminate()
        return 1
    path = args.results_dir / "streaming_latency.csv"
    L.write_csv(rows, path)
    print(f"wrote {path}")
    report(rows)
    print((state or redis_state)(settings, args.season, args.round))
    if not any(r["type"] == "session_end" for r in rows):
        print("error: the subscriber did not receive session_end", file=sys.stderr)
        return 1
    return 0


def redis_state(settings: Settings, season: int, round_: int) -> str:
    """One line: drivers in the order sorted set and the smallest / largest key TTL."""
    from f1sa.serving.redis_store import RaceStore

    store = RaceStore.from_settings(settings)
    keys = store.race_keys(season, round_)
    ttls = [int(store.client.ttl(k)) for k in keys]
    order = store.get_order(season, round_)
    return (
        f"redis: {len(order)} drivers in the order set, {len(keys)} keys, "
        f"TTL {min(ttls, default=-2)}..{max(ttls, default=-2)} s"
    )


def _redis_subscriber(
    settings: Settings, season: int, round_: int, timeout: float
) -> list[dict[str, Any]]:
    import redis

    client = redis.Redis.from_url(settings.redis_url)
    pubsub = client.pubsub()  # type: ignore[no-untyped-call]
    L.subscribe_channel(pubsub, season, round_)
    try:
        rows, _ended = L.collect(pubsub, timeout_s=timeout)
    finally:
        pubsub.close()
    return rows
