"""The API against the real Redis and Kafka of ``make up`` (Step 13), served by uvicorn over
TCP. That covers what httpx's in-process transport cannot: live SSE, and the unsubscribe after
a real client disconnect."""

from __future__ import annotations

import socket
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import uvicorn

from f1sa.api.app import create_app
from f1sa.config import Settings
from f1sa.serving import keys as K
from f1sa.serving import redis_store as R

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO / "models" / "undercut" / "deploy_real_G3.5_k2"
SEASON, ROUND = 2100, 28  # a namespace no real race uses
CHANNEL = K.events_channel(SEASON, ROUND)


@pytest.fixture
def store() -> Iterator[R.RaceStore]:
    s = R.RaceStore.from_settings(Settings(), socket_timeout=5)
    s.client.ping()
    assert s.race_keys(SEASON, ROUND) == [], "integration namespace is not empty"
    yield s
    keys = s.race_keys(SEASON, ROUND)
    if keys:
        s.client.delete(*keys)
    s.client.close()


@pytest.fixture
def base_url(tmp_path: Path) -> Iterator[str]:
    settings = Settings(
        api_model_dir=MODEL_DIR,
        api_sse_heartbeat_s=0.3,
        data_lake_uri=f"file://{REPO / 'data' / 'lake'}",
        api_ui_latency_csv=tmp_path / "ui_latency.csv",
    )
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings), host="127.0.0.1", port=port, log_config=None, access_log=False
        )
    )
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started:
        assert time.monotonic() < deadline, "uvicorn did not start"
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(15)


def _numsub(store: R.RaceStore) -> int:
    return int(store.client.pubsub_numsub(CHANNEL)[0][1])


def _state(pos: int) -> R.DriverState:
    return R.DriverState(pos, None if pos == 1 else 1.5, 0.0, "SOFT", pos, 90.25, False, None)


def test_health_is_ok_with_real_redis_and_kafka(base_url: str) -> None:
    if not (MODEL_DIR / "lr" / "scorer.json").is_file():
        pytest.skip("deployment model missing (make model-save-full)")
    resp = httpx.get(f"{base_url}/api/health", timeout=10)
    assert resp.status_code == 200, resp.text
    checks = resp.json()["checks"]
    assert checks["redis"]["ok"] and checks["kafka"]["ok"] and checks["model"]["ok"]
    assert checks["model"]["feature_set"] == "real" and checks["model"]["git_sha"]


def test_state_reads_real_redis(base_url: str, store: R.RaceStore) -> None:
    assert httpx.get(f"{base_url}/api/races/{SEASON}/{ROUND}/state").status_code == 404
    store.write_batch(
        SEASON,
        ROUND,
        R.Batch(states={"a_one": _state(1), "b_two": _state(2)}, order={"a_one": 1, "b_two": 2}),
    )
    body = httpx.get(f"{base_url}/api/races/{SEASON}/{ROUND}/state").json()
    assert tuple(body["drivers"]["a_one"]) == K.DRIVER_FIELDS
    assert body["drivers"]["a_one"]["gap_ahead_s"] is None
    assert body["drivers"]["b_two"]["gap_ahead_s"] == 1.5
    assert body["order"] == [
        {"driver_id": "a_one", "position": 1},
        {"driver_id": "b_two", "position": 2},
    ]


def test_live_sse_events_heartbeat_and_disconnect_cleanup(
    base_url: str, store: R.RaceStore
) -> None:
    ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    events: list[tuple[str, str]] = []
    heartbeats = 0
    snapshots = 0
    url = f"{base_url}/api/races/{SEASON}/{ROUND}/events"
    with httpx.Client(timeout=httpx.Timeout(10.0)) as client, client.stream("GET", url) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        lines = r.iter_lines()
        for line in lines:
            if line == ": connected":
                break
        assert _numsub(store) == 1
        store.write_batch(
            SEASON, ROUND, R.Batch(events=(R.Event("lap", {"event_id": ids[0], "lap": 1}),))
        )
        store.write_batch(
            SEASON, ROUND, R.Batch(events=(R.Event("pit_in", {"event_id": ids[1], "lap": 2}),))
        )
        name = None
        for line in lines:
            if line.startswith("event: "):
                name = line.removeprefix("event: ")
                if name == "snapshot":
                    snapshots += 1
                    name = None
            elif line.startswith("id: ") and name is not None:
                events.append((name, line.removeprefix("id: ")))
            elif line == ": heartbeat":
                heartbeats += 1
            if len(events) == 2 and heartbeats >= 1:
                break
    assert events == [("lap", ids[0]), ("pit_in", ids[1])]
    assert snapshots == 2, "one snapshot after connect, one for the first lap-1 event"
    deadline = time.monotonic() + 5
    while _numsub(store) != 0:
        assert time.monotonic() < deadline, "subscription survived the client disconnect"
        time.sleep(0.05)


def test_live_sse_closes_after_session_end(base_url: str, store: R.RaceStore) -> None:
    url = f"{base_url}/api/races/{SEASON}/{ROUND}/events"
    names: list[str] = []
    with httpx.Client(timeout=httpx.Timeout(10.0)) as client, client.stream("GET", url) as r:
        lines = r.iter_lines()
        for line in lines:
            if line == ": connected":
                break
        store.end_session(SEASON, ROUND)
        names = [ln.removeprefix("event: ") for ln in lines if ln.startswith("event: ")]
    assert names == ["snapshot", "session_end"]  # the connect snapshot (empty race) first
    assert _numsub(store) == 0
