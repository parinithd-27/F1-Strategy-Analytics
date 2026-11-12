"""RaceStore / AsyncRaceStore against the real Redis 8.10 of `make up` (#21-#24)."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterator

import pytest

from f1sa.config import Settings
from f1sa.serving import keys as K
from f1sa.serving import redis_store as R

pytestmark = pytest.mark.integration

SEASON, ROUND = 2100, 29  # a namespace no real race uses
AFTER, SAFETY = 30, 600


def _state(pos: int) -> R.DriverState:
    return R.DriverState(
        position=pos,
        gap_ahead_s=None if pos == 1 else 0.8 * pos,
        gap_leader_s=0.0 if pos == 1 else 1.6 * pos,
        tyre_compound="HARD",
        tyre_age_laps=pos,
        last_lap_time_s=91.5,
        undercut_flag=pos == 3,
        undercut_prob=None if pos == 1 else 0.25,
    )


@pytest.fixture
def store() -> Iterator[R.RaceStore]:
    settings = Settings(redis_ttl_after_session_s=AFTER, redis_safety_ttl_s=SAFETY)
    s = R.RaceStore.from_settings(settings, socket_timeout=5)
    s.client.ping()
    assert s.race_keys(SEASON, ROUND) == [], "integration namespace is not empty"
    yield s
    keys = s.race_keys(SEASON, ROUND)
    if keys:
        s.client.delete(*keys)
    s.client.close()


def test_batch_pubsub_and_ttl_on_real_redis(store: R.RaceStore) -> None:
    sub = store.client.pubsub()
    sub.subscribe(K.events_channel(SEASON, ROUND))
    deadline = time.monotonic() + 5
    while (m := sub.get_message(timeout=1)) is None or m["type"] != "subscribe":
        assert time.monotonic() < deadline
    drivers = {f"drv_{i}": _state(i) for i in range(1, 21)}
    batch = R.Batch(
        states=drivers,
        order={d: s.position for d, s in drivers.items()},
        events=(R.Event("pit_in", {"driver": "drv_3", "lap": 18}),),
    )
    assert store.write_batch(SEASON, ROUND, batch) == 20 * 2 + 2 + 1
    h = store.client.hgetall(K.driver_key(SEASON, ROUND, "drv_1"))
    assert set(h) == set(K.DRIVER_FIELDS) and h["gap_ahead_s"] == "" and h["undercut_prob"] == ""
    got = store.get_driver_state(SEASON, ROUND, "drv_3")
    assert got is not None and got.state == _state(3)
    assert store.get_order(SEASON, ROUND) == [(f"drv_{i}", i) for i in range(1, 21)]
    keys = store.race_keys(SEASON, ROUND)
    assert len(keys) == 21 and all(AFTER < store.client.ttl(k) <= SAFETY for k in keys)
    assert store.end_session(SEASON, ROUND) == 21
    assert all(0 < store.client.ttl(k) <= AFTER for k in keys)
    types = []
    while (m := sub.get_message(timeout=1)) is not None:
        if m["type"] == "message":
            types.append(json.loads(m["data"])["type"])
    assert types == ["pit_in", "session_end"]
    sub.close()


def test_async_store_on_real_redis(store: R.RaceStore) -> None:
    async def run() -> None:
        aio = R.AsyncRaceStore.from_settings(store.settings, socket_timeout=5)
        try:
            batch = R.Batch(states={"drv_1": _state(1)}, order={"drv_1": 1})
            assert await aio.write_batch(SEASON, ROUND, batch) == 4
            got = await aio.get_driver_state(SEASON, ROUND, "drv_1")
            assert got is not None and got.state == _state(1)
            assert await aio.end_session(SEASON, ROUND) == 2
            assert 0 < await aio.client.ttl(K.order_key(SEASON, ROUND)) <= AFTER
        finally:
            await aio.client.aclose()

    asyncio.run(run())
