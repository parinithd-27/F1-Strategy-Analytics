"""Latency subscriber: rows and segments, stop on session_end, percentiles, CSV."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from f1sa.serving import keys as K
from f1sa.serving.redis_store import Batch, Event, RaceStore
from f1sa.streaming import latency as L


def body(kind: str, **payload: Any) -> dict[str, Any]:
    return {"type": kind, "season": 2024, "round": 5, "payload": payload}


def test_latency_row_segments() -> None:
    r = L.latency_row(
        body(
            "lap",
            event_id="e",
            driver_id="a",
            lap=3,
            produced_at_ms=1000,
            batch_received_at_ms=1400,
            redis_written_at_ms=1410,
        ),
        1412,
    )
    assert (r["produced_to_batch_ms"], r["batch_to_written_ms"], r["written_to_received_ms"]) == (
        400,
        10,
        2,
    )
    assert r["end_to_end_ms"] == 412 and r["lap"] == 3


def test_alert_and_session_end_rows() -> None:
    a = L.latency_row(
        body(
            "undercut_alert",
            trigger_event_id="t",
            attacker_id="b",
            defender_id="a",
            decision_lap=9,
            produced_at_ms=1,
        ),
        5,
    )
    assert (a["event_id"], a["lap"], a["attacker_id"], a["end_to_end_ms"]) == ("t", 9, "b", 4)
    assert a["batch_to_written_ms"] is None
    e = L.latency_row(body("session_end"), 7)
    assert e["type"] == "session_end" and e["end_to_end_ms"] is None


def test_percentile_matches_linear_interpolation() -> None:
    xs = [1.0, 2.0, 3.0, 4.0]
    assert (
        L.percentile(xs, 50) == 2.5 and L.percentile(xs, 0) == 1.0 and L.percentile(xs, 100) == 4.0
    )
    assert L.percentile([5.0], 99) == 5.0
    with pytest.raises(ValueError):
        L.percentile([], 50)
    with pytest.raises(ValueError):
        L.percentile(xs, 101)


def test_summarise_and_throughput() -> None:
    rows = [
        {"type": "lap", "end_to_end_ms": v, "subscriber_received_at_ms": 1000 * i}
        for i, v in enumerate([10, 20, 30])
    ]
    (s,) = L.summarise(rows)
    assert (s.segment, s.n, s.p50, s.max) == ("end_to_end_ms", 3, 20.0, 30.0)
    assert L.throughput(rows) == 1.0
    assert L.throughput(rows[:1]) is None


def test_write_csv(tmp_path: Path) -> None:
    path = tmp_path / "results" / "streaming_latency.csv"
    L.write_csv([L.latency_row(body("lap", lap=1, produced_at_ms=1), 2)], path)
    with path.open() as fh:
        rows = list(csv.DictReader(fh))
    assert (
        tuple(rows[0]) == L.COLUMNS
        and rows[0]["end_to_end_ms"] == "1"
        and rows[0]["driver_id"] == ""
    )


def test_collect_reads_until_session_end() -> None:
    client = fakeredis.FakeRedis(decode_responses=True)
    pubsub = client.pubsub()
    assert L.subscribe_channel(pubsub, 2024, 5) == K.events_channel(2024, 5)
    store = RaceStore(client)
    store.write_batch(2024, 5, Batch(events=(Event("lap", {"produced_at_ms": 1}),)))
    store.end_session(2024, 5)
    rows, ended = L.collect(pubsub, timeout_s=5)
    assert ended and [r["type"] for r in rows] == ["lap", "session_end"]


def test_collect_times_out_without_session_end() -> None:
    client = fakeredis.FakeRedis()
    pubsub = client.pubsub()
    L.subscribe_channel(pubsub, 2024, 5)
    client.publish(K.events_channel(2024, 5), json.dumps(body("lap")))
    ticks = iter(range(0, 10_000, 400))
    rows, ended = L.collect(pubsub, timeout_s=1, clock_ms=lambda: next(ticks))
    assert not ended and len(rows) == 1
