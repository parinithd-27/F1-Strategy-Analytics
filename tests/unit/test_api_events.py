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
from f1sa.serving.redis_store import AsyncRaceStore, Batch, Event

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


def _pit(eid: str = PIT_ID) -> Event:
    return Event("pit_in", {"event_id": eid, "driver_id": "norris", "lap": 8})


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
    events = [f for f in frames if "event" in f]
    assert [e["event"] for e in events] == ["lap", "pit_in", "session_end"]
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
    events = [f for f in parse_sse(resp.text) if "event" in f]
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
