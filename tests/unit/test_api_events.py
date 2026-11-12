"""``GET /api/races/{season}/{round}/events``: SSE framing, heartbeat, close after
``session_end`` and unsubscribe on client disconnect (both ASGI spec paths)."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from _api_fixtures import ApiHarness, make_api, make_settings, parse_sse
from starlette.requests import ClientDisconnect

from f1sa.api.sse import EventStream, event_id_for, format_event
from f1sa.serving import keys as K
from f1sa.serving.redis_store import AsyncRaceStore, Batch, DriverState, Event

pytestmark = pytest.mark.anyio

CHANNEL = K.events_channel(2024, 5)
LAP_ID = str(uuid.uuid4())
PIT_ID = str(uuid.uuid4())


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


async def _numsub(api: ApiHarness) -> int:
    rows = await api.redis.pubsub_numsub(CHANNEL)
    return int(rows[0][1])


async def _wait_subscribed(api: ApiHarness, timeout_s: float = 5.0) -> None:
    async def poll() -> None:
        while await _numsub(api) < 1:
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout_s)


def _store(api: ApiHarness) -> AsyncRaceStore:
    return AsyncRaceStore(api.redis, api.deps.settings)


async def _publish(api: ApiHarness, *events: Event) -> None:
    await _store(api).write_batch(2024, 5, Batch(events=events))


def _lap(eid: str = LAP_ID) -> Event:
    return Event("lap", {"event_id": eid, "driver_id": "norris", "lap": 7, "produced_at_ms": 1})


def _pit(driver: str = "norris", eid: str | None = None) -> Event:
    return Event("pit_in", {"event_id": eid or PIT_ID, "driver_id": driver, "lap": 8})


# ------------------------------------------------------------------ pure helpers
def test_event_id_is_the_payload_id_or_a_stable_uuid5() -> None:
    lap = {"type": "lap", "payload": {"event_id": LAP_ID}}
    assert event_id_for(json.dumps(lap), lap) == LAP_ID
    alert = {"type": "undercut_alert", "payload": {"attacker_id": "a", "trigger_event_id": "x"}}
    raw = json.dumps(alert)
    first = event_id_for(raw, alert)
    assert first == event_id_for(raw, alert)
    assert uuid.UUID(first).version == 5
    other = json.dumps({**alert, "published_at": "2024-05-05T20:00:00.001Z"})
    assert event_id_for(other, alert) != first
    bad = {"type": "lap", "payload": {"event_id": "not-a-uuid\nevent: x"}}
    assert uuid.UUID(event_id_for(json.dumps(bad), bad)).version == 5
    end = {"type": "session_end", "payload": {}}
    assert uuid.UUID(event_id_for(json.dumps(end), end)).version == 5


def test_format_event_is_one_frame() -> None:
    frame = format_event("lap", LAP_ID, {"a": "line\nbreak", "b": 1})
    assert frame == (f'event: lap\nid: {LAP_ID}\ndata: {{"a":"line\\nbreak","b":1}}\n\n'.encode())
    with pytest.raises(ValueError):
        format_event("bogus", LAP_ID, {})
    with pytest.raises(ValueError):
        format_event("lap", "x\ny", {})
    with pytest.raises(ValueError):
        format_event("lap", LAP_ID, {"x": float("nan")})


# ------------------------------------------------------------------ through httpx
async def test_stream_events_heartbeat_and_close_after_session_end(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path, api_sse_heartbeat_s=0.05))

    async def publisher() -> None:
        await _wait_subscribed(api)
        await _publish(api, _lap())
        await _publish(api, _pit())
        await asyncio.sleep(0.15)
        await _store(api).end_session(2024, 5)

    async with api.client() as c:
        resp, _ = await asyncio.wait_for(
            asyncio.gather(c.get("/api/races/2024/5/events"), publisher()), 10
        )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    assert resp.headers["cache-control"] == "no-cache"
    assert resp.headers["x-accel-buffering"] == "no"
    frames = parse_sse(resp.text)
    assert frames[0] == {"retry": "3000", "comments": ["connected"]}
    # A snapshot follows the connect and the first lap event of each new lap (snapshot tests).
    assert [f.get("event") for f in frames if "event" in f] == [
        "snapshot",
        "lap",
        "snapshot",
        "pit_in",
        "session_end",
    ]
    events = [f for f in frames if "event" in f and f["event"] != "snapshot"]
    first, second = events[0], events[1]
    assert first["id"] == LAP_ID and second["id"] == PIT_ID
    for e in events:
        assert e["data"]["type"] == e["event"]
        assert e["data"]["event_id"] == e["id"]
        assert (e["data"]["season"], e["data"]["round"]) == (2024, 5)
        assert isinstance(e["data"]["sse_sent_at_ms"], int) and e["data"]["sse_sent_at_ms"] > 0
    assert first["data"]["payload"]["driver_id"] == "norris"
    assert {"comments": ["heartbeat"]} in frames
    assert "event" in frames[-1] and frames[-1]["event"] == "session_end"
    assert await _numsub(api) == 0


async def test_malformed_and_foreign_messages_are_skipped(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    foreign = {"type": "lap", "season": 2023, "round": 5, "payload": {}}

    async def publisher() -> None:
        await _wait_subscribed(api)
        for raw in (
            "not json",
            json.dumps({"type": "bogus", "season": 2024, "round": 5}),
            json.dumps(foreign),
            '{"type": "lap", "season": 2024, "round": 5, "payload": {"x": NaN}}',
            "[1, 2]",
        ):
            await api.redis.publish(CHANNEL, raw)
        await _publish(api, _lap())
        await _store(api).end_session(2024, 5)

    async with api.client() as c:
        resp, _ = await asyncio.wait_for(
            asyncio.gather(c.get("/api/races/2024/5/events"), publisher()), 10
        )
    events = [f for f in parse_sse(resp.text) if "event" in f and f["event"] != "snapshot"]
    assert [e["event"] for e in events] == ["lap", "session_end"]


@pytest.mark.parametrize("path", ["1949/5", "2024/31", "x/5"])
async def test_events_422_for_bad_path_params(tmp_path: Path, path: str) -> None:
    api = make_api(make_settings(tmp_path))
    async with api.client() as c:
        assert (await c.get(f"/api/races/{path}/events")).status_code == 422


# ------------------------------------------------------------------ raw ASGI: disconnect
def _scope(spec_version: str) -> dict[str, Any]:
    path = "/api/races/2024/5/events"
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"testserver")],
        "client": ("127.0.0.1", 50000),
        "server": ("testserver", 80),
    }


async def _run_until_disconnect(
    api: ApiHarness, spec_version: str, on_first_event: Callable[[], None] | None = None
) -> tuple[list[dict[str, Any]], BaseException | None]:
    sent: list[dict[str, Any]] = []
    gone = asyncio.Event()
    requested = False

    async def receive() -> dict[str, Any]:
        nonlocal requested
        if not requested:
            requested = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await gone.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if gone.is_set() and spec_version >= "2.4":
            raise OSError("connection reset by peer")
        sent.append(message)
        if message["type"] == "http.response.body" and b"event: lap" in message.get("body", b""):
            gone.set()

    async def publisher() -> None:
        await _wait_subscribed(api)
        await _publish(api, _lap())

    error: BaseException | None = None
    try:
        await asyncio.wait_for(
            asyncio.gather(api.app(_scope(spec_version), receive, send), publisher()), 10
        )
    except (ClientDisconnect, OSError) as exc:
        error = exc
    return sent, error


async def test_disconnect_unsubscribes_spec_2_3(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    sent, error = await _run_until_disconnect(api, "2.3")
    assert error is None
    assert sent[0]["type"] == "http.response.start" and sent[0]["status"] == 200
    assert any(b"event: lap" in m.get("body", b"") for m in sent)
    assert await _numsub(api) == 0


async def test_disconnect_unsubscribes_spec_2_4(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path, api_sse_heartbeat_s=0.05))
    sent, error = await _run_until_disconnect(api, "2.4")
    assert isinstance(error, ClientDisconnect)
    assert any(b"event: lap" in m.get("body", b"") for m in sent)
    assert await _numsub(api) == 0


async def test_close_is_idempotent_and_safe_before_subscribe(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    stream = EventStream(api.redis, 2024, 5, heartbeat_s=1.0)
    await stream.close()
    await stream.close()
    assert stream.closed
    with pytest.raises(ValueError):
        EventStream(api.redis, 2024, 5, heartbeat_s=0)


# ------------------------------------------------------------------ snapshots (pushed, not polled)
def _state(pos: int, flag: bool = False, prob: float | None = None) -> DriverState:
    return DriverState(
        pos, None if pos == 1 else 1.0, 0.0 if pos == 1 else float(pos), "HARD", 5, 95.0, flag, prob
    )


async def _seed(api: ApiHarness, flags: dict[str, bool]) -> None:
    ids = list(flags)
    await _store(api).write_batch(
        2024,
        5,
        Batch(
            states={
                d: _state(i + 1, flags[d], 0.7 if flags[d] else None) for i, d in enumerate(ids)
            },
            order={d: i + 1 for i, d in enumerate(ids)},
        ),
    )


def _lap_event(driver: str, lap: int) -> Event:
    return Event("lap", {"event_id": str(uuid.uuid4()), "driver_id": driver, "lap": lap})


async def _stream(api: ApiHarness, publisher) -> list[dict[str, Any]]:  # type: ignore[no-untyped-def]
    async with api.client() as c:
        resp, _ = await asyncio.wait_for(
            asyncio.gather(c.get("/api/races/2024/5/events"), publisher()), 10
        )
    return [f for f in parse_sse(resp.text) if "event" in f]


async def test_snapshot_after_connect_and_once_per_leader_lap(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    await _seed(api, {"alpha": False, "bravo": True, "charlie": False})

    async def publisher() -> None:
        await _wait_subscribed(api)
        for d in ("alpha", "bravo", "charlie"):  # three events of lap 1: one snapshot
            await _publish(api, _lap_event(d, 1))
        await _publish(api, _pit("bravo"))
        await _publish(
            api, Event("undercut_alert", {"attacker_id": "bravo", "defender_id": "alpha"})
        )
        for d in ("alpha", "bravo"):  # lap 2: one snapshot
            await _publish(api, _lap_event(d, 2))
        await _publish(api, _lap_event("charlie", 1))  # a late lap-1 event: no snapshot
        await _publish(api, _lap_event("charlie", 2))  # lap 2 already snapshotted
        await _store(api).end_session(2024, 5)

    events = await _stream(api, publisher)
    names = [e["event"] for e in events]
    assert names == [
        "snapshot", "lap", "snapshot", "lap", "lap", "pit_in", "undercut_alert",
        "lap", "snapshot", "lap", "lap", "lap", "session_end",
    ]  # fmt: skip
    snaps = [e for e in events if e["event"] == "snapshot"]
    assert [s["id"] for s in snaps] == ["snapshot-0", "snapshot-1", "snapshot-2"]
    for snap, lap in zip(snaps, (0, 1, 2), strict=True):
        data = snap["data"]
        assert data["type"] == "snapshot" and data["lap"] == lap and data["event_id"] == snap["id"]
        assert (data["season"], data["round"]) == (2024, 5)
        assert isinstance(data["sse_sent_at_ms"], int) and data["sse_sent_at_ms"] > 0
        assert sorted(data["drivers"]) == ["alpha", "bravo", "charlie"]
        for d in data["drivers"].values():  # JSON keys are sorted on the wire
            assert sorted(d) == sorted(K.DRIVER_FIELDS) and len(d) == 9
        assert data["order"] == [
            {"driver_id": "alpha", "position": 1},
            {"driver_id": "bravo", "position": 2},
            {"driver_id": "charlie", "position": 3},
        ]
    assert snaps[0]["data"]["drivers"]["alpha"]["gap_ahead_s"] is None  # "" -> null
    assert snaps[0]["data"]["drivers"]["bravo"]["undercut_flag"] is True


async def test_snapshot_refreshes_a_stale_undercut_flag(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    await _seed(api, {"alpha": False, "bravo": True})

    async def publisher() -> None:
        await _wait_subscribed(api)
        await _publish(api, _lap_event("alpha", 10))
        await asyncio.sleep(0.2)  # let the stream send snapshot-10 first
        await _seed(api, {"alpha": False, "bravo": False})  # bravo is no longer a candidate
        await _publish(api, _lap_event("alpha", 11))
        await _store(api).end_session(2024, 5)

    snaps = [e for e in await _stream(api, publisher) if e["event"] == "snapshot"]
    assert [s["data"]["drivers"]["bravo"]["undercut_flag"] for s in snaps] == [True, True, False]
    assert snaps[-1]["data"]["drivers"]["bravo"]["undercut_prob"] is None


async def test_per_lap_snapshots_use_one_pipeline_and_no_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = make_api(make_settings(tmp_path))
    await _seed(api, {"alpha": False, "bravo": False})
    calls = {"scan": 0, "pipeline": 0}
    real_scan, real_pipeline = api.redis.scan, api.redis.pipeline

    def scan(*a: Any, **k: Any) -> Any:
        calls["scan"] += 1
        return real_scan(*a, **k)

    def pipeline(*a: Any, **k: Any) -> Any:
        calls["pipeline"] += 1
        return real_pipeline(*a, **k)

    marks: list[dict[str, int]] = []

    async def publisher() -> None:
        await _wait_subscribed(api)
        await asyncio.sleep(0.05)  # the connect snapshot is done
        marks.append(dict(calls))
        for lap in (1, 2, 3):
            await _publish(api, _lap_event("alpha", lap))
        await asyncio.sleep(0.05)
        marks.append(dict(calls))
        await _store(api).end_session(2024, 5)

    monkeypatch.setattr(api.redis, "scan", scan)
    monkeypatch.setattr(api.redis, "pipeline", pipeline)
    events = await _stream(api, publisher)
    assert [e["id"] for e in events if e["event"] == "snapshot"] == [
        "snapshot-0",
        "snapshot-1",
        "snapshot-2",
        "snapshot-3",
    ]
    assert marks[0]["scan"] >= 1, "the connect snapshot finds the drivers with SCAN"
    # _publish also uses one pipeline per event (write_batch): 3 events + 3 snapshots.
    assert marks[1]["pipeline"] - marks[0]["pipeline"] == 3 + 3
    assert marks[1]["scan"] == marks[0]["scan"], "per-lap snapshots never SCAN"


async def test_unreadable_snapshot_is_skipped_and_the_stream_goes_on(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    await _seed(api, {"alpha": False})

    async def publisher() -> None:
        await _wait_subscribed(api)
        await api.redis.hset(K.driver_key(2024, 5, "alpha"), "undercut_flag", "maybe")
        await _publish(api, _lap_event("alpha", 1))
        await _store(api).end_session(2024, 5)

    names = [e["event"] for e in await _stream(api, publisher)]
    assert names == ["snapshot", "lap", "session_end"]  # the lap-1 snapshot was skipped


async def test_snapshots_are_not_latency_samples(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path, enable_debug=True))
    await _seed(api, {"alpha": False})

    async def publisher() -> None:
        await _wait_subscribed(api)
        await _publish(api, _lap_event("alpha", 1))
        await _store(api).end_session(2024, 5)

    await _stream(api, publisher)
    remembered = list(api.deps.latency._sent)
    assert remembered and not any(i.startswith("snapshot") for i in remembered)
