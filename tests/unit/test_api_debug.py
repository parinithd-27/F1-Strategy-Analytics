"""``POST /api/debug/latency``: flag, join with the sent event, CSV rows, 404/409/422."""

from __future__ import annotations

import csv
import uuid
from pathlib import Path

import pytest
from _api_fixtures import make_api, make_settings

from f1sa.api.latency import CSV_COLUMNS, AlreadyRecordedError, LatencyRecorder, UnknownEventError

pytestmark = pytest.mark.anyio

EID = str(uuid.uuid4())
SENT = {
    "type": "lap",
    "season": 2024,
    "round": 5,
    "payload": {
        "event_id": EID,
        "produced_at_ms": 1000,
        "batch_received_at_ms": 1400,
        "redis_written_at_ms": 1450,
    },
    "event_id": EID,
    "sse_sent_at_ms": 1460,
}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


async def test_403_when_debug_is_disabled(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    api.deps.latency.remember(EID, SENT)
    async with api.client() as c:
        resp = await c.post("/api/debug/latency", json={"event_id": EID, "ui_rendered_at_ms": 1})
    assert resp.status_code == 403
    assert not api.deps.settings.api_ui_latency_csv.exists()


async def test_201_appends_a_joined_row(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, enable_debug=True)
    api = make_api(settings)
    api.deps.latency.remember(EID, SENT)
    other = str(uuid.uuid4())
    api.deps.latency.remember(
        other,
        {"type": "session_end", "season": 2024, "round": 5, "payload": {}, "sse_sent_at_ms": 2000},
    )
    async with api.client() as c:
        first = await c.post(
            "/api/debug/latency", json={"event_id": EID, "ui_rendered_at_ms": 1500}
        )
        second = await c.post(
            "/api/debug/latency", json={"event_id": other, "ui_rendered_at_ms": 2010}
        )
    assert first.status_code == 201 and first.json() == {"event_id": EID, "event_type": "lap"}
    assert second.status_code == 201
    path = settings.api_ui_latency_csv
    text = path.read_text(encoding="utf-8")
    assert text.splitlines()[0] == ",".join(CSV_COLUMNS)
    assert text.count("event_id,event_type") == 1
    rows = _rows(path)
    assert rows[0] == {
        "event_id": EID,
        "event_type": "lap",
        "season": "2024",
        "round": "5",
        "produced_at_ms": "1000",
        "batch_received_at_ms": "1400",
        "redis_written_at_ms": "1450",
        "sse_sent_at_ms": "1460",
        "ui_rendered_at_ms": "1500",
        "recorded_at_ms": rows[0]["recorded_at_ms"],
    }
    assert int(rows[0]["recorded_at_ms"]) > 0
    assert rows[1]["event_type"] == "session_end" and rows[1]["produced_at_ms"] == ""


async def test_404_unknown_and_409_duplicate(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path, enable_debug=True))
    api.deps.latency.remember(EID, SENT)
    async with api.client() as c:
        unknown = await c.post(
            "/api/debug/latency", json={"event_id": str(uuid.uuid4()), "ui_rendered_at_ms": 5}
        )
        ok = await c.post("/api/debug/latency", json={"event_id": EID, "ui_rendered_at_ms": 5})
        dup = await c.post("/api/debug/latency", json={"event_id": EID, "ui_rendered_at_ms": 6})
    assert (unknown.status_code, ok.status_code, dup.status_code) == (404, 201, 409)
    assert len(_rows(api.deps.settings.api_ui_latency_csv)) == 1


@pytest.mark.parametrize(
    "body",
    [
        {"event_id": "abc", "ui_rendered_at_ms": 5},
        {"event_id": EID},
        {"event_id": EID, "ui_rendered_at_ms": 0},
        {"event_id": EID, "ui_rendered_at_ms": "5"},
        {"event_id": EID, "ui_rendered_at_ms": True},
        {"event_id": EID, "ui_rendered_at_ms": 1.5},
        {"event_id": EID, "ui_rendered_at_ms": 5, "sse_sent_at_ms": 1},
    ],
)
async def test_422_for_a_bad_body(tmp_path: Path, body: dict[str, object]) -> None:
    api = make_api(make_settings(tmp_path, enable_debug=True))
    api.deps.latency.remember(EID, SENT)
    async with api.client() as c:
        resp = await c.post("/api/debug/latency", json=body)
    assert resp.status_code == 422


def test_remember_is_a_no_op_when_disabled(tmp_path: Path) -> None:
    rec = LatencyRecorder(tmp_path / "x.csv", enabled=False)
    rec.remember(EID, SENT)
    assert rec._sent == {}


async def test_recorder_is_bounded(tmp_path: Path) -> None:
    rec = LatencyRecorder(tmp_path / "x.csv", enabled=True, capacity=2, clock_ms=lambda: 7)
    ids = [str(uuid.uuid4()) for _ in range(3)]
    for i in ids:
        rec.remember(i, SENT)
    with pytest.raises(UnknownEventError):
        await rec.record(ids[0], 1)
    row = await rec.record(ids[2], 9)
    assert row["recorded_at_ms"] == 7 and row["ui_rendered_at_ms"] == 9
    with pytest.raises(AlreadyRecordedError):
        await rec.record(ids[2], 9)
    with pytest.raises(ValueError):
        LatencyRecorder(tmp_path / "y.csv", enabled=True, capacity=0)


async def test_sse_stream_feeds_the_recorder(tmp_path: Path) -> None:
    import asyncio

    from f1sa.serving.redis_store import AsyncRaceStore, Batch, Event

    api = make_api(make_settings(tmp_path, enable_debug=True))
    store = AsyncRaceStore(api.redis, api.deps.settings)

    async def publisher() -> None:
        while (await api.redis.pubsub_numsub("race:2024:5:events"))[0][1] < 1:
            await asyncio.sleep(0.005)
        await store.write_batch(
            2024, 5, Batch(events=(Event("lap", {"event_id": EID, "produced_at_ms": 11}),))
        )
        await store.end_session(2024, 5)

    async with api.client() as c:
        await asyncio.wait_for(asyncio.gather(c.get("/api/races/2024/5/events"), publisher()), 10)
        resp = await c.post("/api/debug/latency", json={"event_id": EID, "ui_rendered_at_ms": 99})
    assert resp.status_code == 201
    row = _rows(api.deps.settings.api_ui_latency_csv)[0]
    assert row["produced_at_ms"] == "11" and int(row["sse_sent_at_ms"]) > 0
