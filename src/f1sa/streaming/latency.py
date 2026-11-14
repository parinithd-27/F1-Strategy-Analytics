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
    on_row: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """Read messages until ``session_end`` (True) or ``timeout_s`` / ``stop()`` (False).
    ``on_row`` sees every row as it arrives (the Step 16 window run watches for idleness)."""
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
        if on_row is not None:
            on_row(row)
        if row["type"] == "session_end":
            return rows, True
    return rows, False


# ============================================================================== Step 16
# The quiet-stack runs add the API's SSE stream as a fourth hop (Redis -> FastAPI -> client) and
# report the first micro-batch of a race (which loads the race's priors with Spark) apart from the
# steady state. Nothing above changes: ``COLUMNS`` keeps the Step 12 layout.

SSE_COLUMNS: tuple[str, ...] = (
    *COLUMNS,
    "sse_sent_at_ms",
    "sse_received_at_ms",
    "written_to_sse_ms",
    "produced_to_sse_ms",
    "first_batch",
)
EXT_SEGMENTS: tuple[str, ...] = (*SEGMENTS, "written_to_sse_ms", "produced_to_sse_ms")
SCOPES: tuple[str, ...] = ("all", "first_batch", "steady")
JoinKey = tuple[Any, ...]


def join_key(row: Mapping[str, Any]) -> JoinKey:
    """Identity of one published message across the Pub/Sub and SSE copies (alerts share the
    trigger's event id, so the pair, reason and lap are part of the key)."""
    return tuple(
        row.get(c)
        for c in ("type", "event_id", "driver_id", "attacker_id", "defender_id", "reason", "lap")
    )


def parse_sse(lines: Iterable[str]) -> Iterable[tuple[str, str, str]]:
    """``(event, id, data)`` per SSE frame from decoded lines (comments and ``retry`` skipped)."""
    event = eid = ""
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data:
                yield event, eid, "\n".join(data)
            event, eid, data = "", "", []
        elif line.startswith(":"):
            continue
        else:
            name, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if name == "event":
                event = value
            elif name == "id":
                eid = value
            elif name == "data":
                data.append(value)
    if data:
        yield event, eid, "\n".join(data)


def sse_row(data: str, received_ms: int) -> dict[str, Any]:
    """A latency row from one SSE frame's JSON (the Pub/Sub message plus ``sse_sent_at_ms``)."""
    msg = json.loads(data)
    row = latency_row(msg, received_ms)
    row["sse_sent_at_ms"] = msg.get("sse_sent_at_ms")
    return row


def join_sse(
    rows: Sequence[Mapping[str, Any]],
    sse_rows: Sequence[Mapping[str, Any]],
    first_batch_ms: int | None = None,
) -> list[dict[str, Any]]:
    """The Pub/Sub rows with the SSE receive time of the same message (empty if the SSE client
    did not see it) and the first-batch flag."""
    by_key: dict[JoinKey, Mapping[str, Any]] = {}
    for r in sse_rows:
        by_key.setdefault(join_key(r), r)
    out = []
    for r in rows:
        s = by_key.get(join_key(r))
        received = s.get("subscriber_received_at_ms") if s else None
        ext = {
            **r,
            "sse_sent_at_ms": s.get("sse_sent_at_ms") if s else None,
            "sse_received_at_ms": received,
            "written_to_sse_ms": _diff(received, r.get("redis_written_at_ms")),
            "produced_to_sse_ms": _diff(received, r.get("produced_at_ms")),
            "first_batch": first_batch_ms is not None
            and r.get("batch_received_at_ms") == first_batch_ms,
        }
        out.append(ext)
    return out


def first_race_batch_ms(batch_log: Path) -> int | None:
    """``batch_received_at_ms`` of the first micro-batch that carried race states (it loads the
    race's priors with Spark), from the stream job's ``stream_batches.jsonl``."""
    if not batch_log.is_file():
        return None
    for line in batch_log.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        b = json.loads(line)
        if any(int(r.get("n_states", 0)) > 0 for r in b.get("races", [])):
            return int(b["batch_received_at_ms"])
    return None


def write_rows(rows: Sequence[Mapping[str, Any]], path: Path, columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(columns), lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: "" if r.get(c) is None else r[c] for c in columns})
    os.replace(tmp, path)


def read_rows(path: Path) -> list[dict[str, Any]]:
    """Rows of a latency CSV with the timing columns as ints (empty -> None)."""
    with path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    out = []
    for r in rows:
        d: dict[str, Any] = dict(r)
        for c in d:
            if c.endswith("_ms") or c == "lap":
                d[c] = int(d[c]) if d[c] not in ("", None) else None
        if "first_batch" in d:
            d["first_batch"] = d["first_batch"] == "True"
        out.append(d)
    return out


SUMMARY_COLUMNS: tuple[str, ...] = (
    "run", "speed", "window_s", "scope", "segment", "n", "p50_ms", "p95_ms", "p99_ms", "max_ms",
)  # fmt: skip


def segment_summary(
    rows: Sequence[Mapping[str, Any]], run: str, speed: float, window_s: float | None
) -> list[dict[str, Any]]:
    """p50/p95/p99/max per segment for all rows, the first (prior-loading) batch and the rest."""
    out = []
    for scope in SCOPES:
        part = [
            r
            for r in rows
            if scope == "all" or bool(r.get("first_batch")) == (scope == "first_batch")
        ]
        for seg in EXT_SEGMENTS:
            vals = [float(r[seg]) for r in part if r.get(seg) is not None]
            if not vals:
                continue
            out.append(
                {
                    "run": run,
                    "speed": f"{speed:g}",
                    "window_s": "" if window_s is None else f"{window_s:g}",
                    "scope": scope,
                    "segment": seg,
                    "n": len(vals),
                    "p50_ms": round(percentile(vals, 50), 1),
                    "p95_ms": round(percentile(vals, 95), 1),
                    "p99_ms": round(percentile(vals, 99), 1),
                    "max_ms": round(max(vals), 1),
                }
            )
    return out


THROUGHPUT_COLUMNS: tuple[str, ...] = (
    "run", "speed", "window_s", "messages", "lap_messages", "received_span_s",
    "messages_per_s", "lap_messages_per_s", "produced_span_s", "produced_per_s",
)  # fmt: skip


def throughput_row(
    rows: Sequence[Mapping[str, Any]], run: str, speed: float, window_s: float | None
) -> dict[str, Any]:
    """Messages per second received (all types, and ``lap`` only via :func:`throughput`) and the
    producer's send rate over the produced span."""
    recv = [int(r["subscriber_received_at_ms"]) for r in rows]
    prod = [int(r["produced_at_ms"]) for r in rows if r.get("produced_at_ms") is not None]
    span = (max(recv) - min(recv)) / 1000.0 if len(recv) > 1 else 0.0
    pspan = (max(prod) - min(prod)) / 1000.0 if len(prod) > 1 else 0.0
    laps = sum(r.get("type") == "lap" for r in rows)
    lap_rate = throughput(rows)
    return {
        "run": run,
        "speed": f"{speed:g}",
        "window_s": "" if window_s is None else f"{window_s:g}",
        "messages": len(rows),
        "lap_messages": laps,
        "received_span_s": round(span, 2),
        "messages_per_s": round((len(recv) - 1) / span, 2) if span > 0 else "",
        "lap_messages_per_s": "" if lap_rate is None else round(lap_rate, 2),
        "produced_span_s": round(pspan, 2),
        "produced_per_s": round((len(prod) - 1) / pspan, 2) if pspan > 0 else "",
    }
