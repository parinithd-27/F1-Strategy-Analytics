"""Event-to-dashboard latency of the stream (#31, metrics #35/#36 in S16).

A Pub/Sub subscriber on ``race:{season}:{round}:events`` records the receive time of every
message and writes ``results/streaming_latency.csv``, one row per message:

* ``produced_to_batch_ms``: producer send -> ``foreachBatch`` start (Kafka, the 500 ms trigger
  interval and Spark planning);
* ``batch_to_written_ms``: ``foreachBatch`` start -> the Redis pipeline is sent (parsing, state,
  rule, scoring, building the batch);
* ``written_to_received_ms``: pipeline sent -> subscriber received (Redis applies the batch's
  writes, then delivers its PUBLISHes);
* ``end_to_end_ms``: producer send -> subscriber received.

All four use one clock as long as the producer, the stream job and the subscriber run on one host
(``make stream-demo``: all on the WSL host, or all in the app container with ``FULL=1``).
"""

from __future__ import annotations

import csv
import json
import math
import os
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from f1sa.serving import keys as K
from f1sa.streaming.redis_sink import now_ms

COLUMNS: tuple[str, ...] = (
    "type",
    "reason",
    "trigger",
    "event_id",
    "driver_id",
    "lap",
    "attacker_id",
    "defender_id",
    "produced_at_ms",
    "batch_received_at_ms",
    "redis_written_at_ms",
    "subscriber_received_at_ms",
    "produced_to_batch_ms",
    "batch_to_written_ms",
    "written_to_received_ms",
    "end_to_end_ms",
)
SEGMENTS: tuple[str, ...] = (
    "produced_to_batch_ms",
    "batch_to_written_ms",
    "written_to_received_ms",
    "end_to_end_ms",
)


def _diff(b: Any, a: Any) -> int | None:
    return int(b) - int(a) if a is not None and b is not None else None


def latency_row(message: Mapping[str, Any], received_ms: int) -> dict[str, Any]:
    """One CSV row from a decoded event JSON (``serving.redis_store.encode_event`` format)."""
    p = message.get("payload") or {}
    produced, batch, written = (
        p.get("produced_at_ms"),
        p.get("batch_received_at_ms"),
        p.get("redis_written_at_ms"),
    )
    return {
        "type": message.get("type"),
        "reason": p.get("reason"),
        "trigger": p.get("trigger"),
        "event_id": p.get("event_id") or p.get("trigger_event_id"),
        "driver_id": p.get("driver_id"),
        "lap": p.get("lap") if "lap" in p else p.get("decision_lap"),
        "attacker_id": p.get("attacker_id"),
        "defender_id": p.get("defender_id"),
        "produced_at_ms": produced,
        "batch_received_at_ms": batch,
        "redis_written_at_ms": written,
        "subscriber_received_at_ms": received_ms,
        "produced_to_batch_ms": _diff(batch, produced),
        "batch_to_written_ms": _diff(written, batch),
        "written_to_received_ms": _diff(received_ms, written),
        "end_to_end_ms": _diff(received_ms, produced),
    }


def percentile(values: Sequence[float], q: float) -> float:
    """Linear-interpolation percentile (numpy's default), ``q`` in [0, 100]."""
    if not values:
        raise ValueError("percentile of no values")
    if not 0 <= q <= 100:
        raise ValueError(f"q must be in [0, 100], got {q}")
    xs = sorted(values)
    pos = (len(xs) - 1) * q / 100.0
    lo, hi = math.floor(pos), math.ceil(pos)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


@dataclass(frozen=True)
class SegmentSummary:
    segment: str
    n: int
    p50: float
    p95: float
    p99: float
    max: float


def summarise(rows: Iterable[Mapping[str, Any]]) -> list[SegmentSummary]:
    rows = list(rows)
    out = []
    for seg in SEGMENTS:
        vals = [float(r[seg]) for r in rows if r.get(seg) is not None]
        if vals:
            out.append(
                SegmentSummary(
                    seg,
                    len(vals),
                    percentile(vals, 50),
                    percentile(vals, 95),
                    percentile(vals, 99),
                    max(vals),
                )
            )
    return out


def throughput(rows: Iterable[Mapping[str, Any]]) -> float | None:
    """Messages per second received between the first and the last ``lap`` message."""
    times = [int(r["subscriber_received_at_ms"]) for r in rows if r.get("type") == "lap"]
    if len(times) < 2 or max(times) == min(times):
        return None
    return (len(times) - 1) / ((max(times) - min(times)) / 1000.0)


def write_csv(rows: Sequence[Mapping[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(COLUMNS), lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: "" if r.get(c) is None else r[c] for c in COLUMNS})
    os.replace(tmp, path)


def subscribe_channel(pubsub: Any, season: int, round_: int) -> str:
    channel = K.events_channel(season, round_)
    pubsub.subscribe(channel)
    return channel


def collect(
    pubsub: Any,
    *,
    timeout_s: float,
    clock_ms: Callable[[], int] = now_ms,
    stop: Callable[[], bool] = lambda: False,
) -> tuple[list[dict[str, Any]], bool]:
    """Read messages until ``session_end`` (True) or ``timeout_s`` / ``stop()`` (False)."""
    rows: list[dict[str, Any]] = []
    deadline = clock_ms() + int(timeout_s * 1000)
    while clock_ms() < deadline and not stop():
        msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
        if msg is None or msg.get("type") != "message":
            continue
        received = clock_ms()
        data = msg["data"]
        body = json.loads(data.decode() if isinstance(data, bytes) else data)
        row = latency_row(body, received)
        rows.append(row)
        if row["type"] == "session_end":
            return rows, True
    return rows, False
