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


# ------------------------------------------------------------------ Step 16 helpers
def test_parse_sse_frames_skips_comments_and_joins_data_lines() -> None:
    from f1sa.streaming import latency as L

    lines = [
        "retry: 3000", ": connected", "",
        "event: lap", "id: abc", 'data: {"a":', "data: 1}", "",
        ": heartbeat", "",
        "event: session_end", "id: z", 'data: {"type":"session_end"}',
    ]  # fmt: skip
    assert list(L.parse_sse(lines)) == [
        ("lap", "abc", '{"a":\n1}'),
        ("session_end", "z", '{"type":"session_end"}'),
    ]


def test_sse_row_and_join_key_match_the_pubsub_row() -> None:
    import json

    from f1sa.streaming import latency as L

    msg = {"type": "undercut_alert", "season": 2024, "round": 5,
           "payload": {"trigger_event_id": "t1", "attacker_id": "a", "defender_id": "d",
                       "reason": "rule", "decision_lap": 7, "produced_at_ms": 10,
                       "batch_received_at_ms": 20, "redis_written_at_ms": 30}}  # fmt: skip
    pub = L.latency_row(msg, 40)
    sse = L.sse_row(json.dumps({**msg, "event_id": "x", "sse_sent_at_ms": 45}), 50)
    assert L.join_key(pub) == L.join_key(sse) and sse["sse_sent_at_ms"] == 45
    other = L.latency_row({**msg, "payload": {**msg["payload"], "defender_id": "e"}}, 40)
    assert L.join_key(other) != L.join_key(pub)


def test_first_race_batch_and_summaries(tmp_path: Path) -> None:
    import json

    from f1sa.streaming import latency as L

    log = tmp_path / "b.jsonl"
    assert L.first_race_batch_ms(log) is None
    log.write_text(
        json.dumps({"batch_received_at_ms": 1, "races": [{"n_states": 0}]}) + "\n\n"
        + json.dumps({"batch_received_at_ms": 7, "races": [{"n_states": 3}]}) + "\n"
    )  # fmt: skip
    assert L.first_race_batch_ms(log) == 7
    rows = [
        {"type": "lap", "produced_at_ms": 0, "batch_received_at_ms": 7, "redis_written_at_ms": 9,
         "subscriber_received_at_ms": 3000, "end_to_end_ms": 3000, "first_batch": True},
        {"type": "lap", "produced_at_ms": 1000, "batch_received_at_ms": 1500,
         "redis_written_at_ms": 1510, "subscriber_received_at_ms": 1520, "end_to_end_ms": 520,
         "first_batch": False},
        {"type": "pit_in", "produced_at_ms": 2000, "subscriber_received_at_ms": 4000,
         "end_to_end_ms": 2000, "first_batch": False},
    ]  # fmt: skip
    summary = L.segment_summary(rows, "s10", 10.0, None)
    e2e = {r["scope"]: r for r in summary if r["segment"] == "end_to_end_ms"}
    assert (e2e["all"]["n"], e2e["first_batch"]["n"], e2e["steady"]["n"]) == (3, 1, 2)
    assert e2e["first_batch"]["p50_ms"] == 3000.0 and e2e["steady"]["max_ms"] == 2000.0
    assert e2e["all"]["window_s"] == "" and e2e["all"]["speed"] == "10"
    t = L.throughput_row(rows, "s10", 10.0, 900.0)
    assert t["messages"] == 3 and t["lap_messages"] == 2 and t["window_s"] == "900"
    assert t["received_span_s"] == 2.48 and t["produced_span_s"] == 2.0
    assert t["produced_per_s"] == 1.0


def test_write_and_read_rows_round_trip(tmp_path: Path) -> None:
    from f1sa.streaming import latency as L

    rows = [{"type": "lap", "lap": 3, "end_to_end_ms": 12, "first_batch": True, "reason": None}]
    L.write_rows(
        rows, tmp_path / "x.csv", ("type", "lap", "end_to_end_ms", "first_batch", "reason")
    )
    back = L.read_rows(tmp_path / "x.csv")
    assert back == [
        {"type": "lap", "lap": 3, "end_to_end_ms": 12, "first_batch": True, "reason": ""}
    ]


def test_collect_reports_each_row_as_it_arrives() -> None:
    import json

    from f1sa.streaming import latency as L

    msgs = [{"type": "message", "data": json.dumps({"type": "session_end", "payload": {}})}]

    class P:
        def get_message(self, **kw: Any) -> Any:
            return msgs.pop(0) if msgs else None

    seen: list[dict[str, Any]] = []
    rows, ended = L.collect(P(), timeout_s=5, on_row=seen.append)
    assert ended and seen == rows
