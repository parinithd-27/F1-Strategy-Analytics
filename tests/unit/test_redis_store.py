"""RaceStore / AsyncRaceStore against fakeredis: encoding, order, TTL, Pub/Sub, pipelines."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import fakeredis
import pytest

from f1sa.config import Settings
from f1sa.serving import keys as K
from f1sa.serving import redis_store as R

SAFETY, AFTER = 21600, 120
T0 = datetime(2024, 5, 5, 20, 14, 3, 125999, tzinfo=UTC)
ISO_MS = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")


def _settings() -> Settings:
    return Settings(redis_safety_ttl_s=SAFETY, redis_ttl_after_session_s=AFTER)


def _clock(start: datetime = T0) -> Callable[[], datetime]:
    ticks = iter(start + timedelta(milliseconds=i) for i in range(10**6))
    return lambda: next(ticks)


def state(pos: int = 2, **kw: Any) -> R.DriverState:
    base: dict[str, Any] = {
        "position": pos,
        "gap_ahead_s": 1.25,
        "gap_leader_s": 3.5,
        "tyre_compound": "MEDIUM",
        "tyre_age_laps": 12,
        "last_lap_time_s": 92.481,
        "undercut_flag": True,
        "undercut_prob": 0.4125,
    }
    base.update(kw)
    return R.DriverState(**base)


LEADER = state(
    1,
    gap_ahead_s=None,
    gap_leader_s=0.0,
    undercut_flag=False,
    undercut_prob=None,
    tyre_age_laps=None,
)


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


@pytest.fixture
def client(server: fakeredis.FakeServer) -> Iterator[fakeredis.FakeRedis]:
    c = fakeredis.FakeRedis(server=server, decode_responses=True)
    yield c
    c.close()


@pytest.fixture
def store(client: fakeredis.FakeRedis) -> R.RaceStore:
    return R.RaceStore(client, _settings(), now=_clock())


def _batch() -> R.Batch:
    return R.Batch(
        states={"max_verstappen": LEADER, "norris": state(2), "leclerc": state(3)},
        order={"max_verstappen": 1, "norris": 2, "leclerc": 3},
        events=(R.Event("lap", {"lap": 31}, T0), R.Event("undercut_alert", {"attacker": "norris"})),
    )


# ------------------------------------------------------------------ encoding
def test_hash_has_exactly_the_nine_fields_as_strings(
    store: R.RaceStore, client: fakeredis.FakeRedis
) -> None:
    store.write_batch(2024, 5, _batch())
    h = client.hgetall("race:2024:5:driver:norris")
    assert tuple(h) == K.DRIVER_FIELDS or set(h) == set(K.DRIVER_FIELDS)
    assert len(h) == 9 and all(isinstance(v, str) for v in h.values())
    assert h == {
        "position": "2",
        "gap_ahead_s": "1.25",
        "gap_leader_s": "3.5",
        "tyre_compound": "MEDIUM",
        "tyre_age_laps": "12",
        "last_lap_time_s": "92.481",
        "undercut_flag": "1",
        "undercut_prob": "0.4125",
        "updated_at": h["updated_at"],
    }
    assert ISO_MS.match(h["updated_at"])
    leader = client.hgetall("race:2024:5:driver:max_verstappen")
    assert leader["gap_ahead_s"] == "" and leader["gap_leader_s"] == "0.0"
    assert leader["undercut_prob"] == "" and leader["tyre_age_laps"] == ""
    assert leader["undercut_flag"] == "0" and len(leader) == 9


def test_updated_at_is_utc_with_milliseconds() -> None:
    assert R.iso_ms(T0) == "2024-05-05T20:14:03.125Z"
    ist = timezone(timedelta(hours=5, minutes=30))
    assert R.iso_ms(datetime(2024, 5, 6, 1, 44, 3, 7000, tzinfo=ist)) == "2024-05-05T20:14:03.007Z"
    with pytest.raises(R.StoreError, match="timezone-aware"):
        R.iso_ms(datetime(2024, 5, 5, 20, 14, 3))


def test_decode_round_trip(store: R.RaceStore) -> None:
    store.write_batch(2024, 5, _batch())
    got = store.get_driver_state(2024, 5, "norris")
    assert got is not None and got.state == state(2) and ISO_MS.match(got.updated_at)
    leader = store.get_driver_state(2024, 5, "max_verstappen")
    assert leader is not None and leader.state == LEADER
    assert store.get_driver_state(2024, 5, "sainz") is None


def test_decode_rejects_hashes_outside_the_contract() -> None:
    good = R.encode_state(state(), T0)
    with pytest.raises(R.StoreError, match="differ from the contract"):
        R.decode_state({k: v for k, v in good.items() if k != "undercut_prob"})
    with pytest.raises(R.StoreError, match="undercut_flag"):
        R.decode_state({**good, "undercut_flag": "true"})
    with pytest.raises(R.StoreError, match="position is empty"):
        R.decode_state({**good, "position": ""})
    assert R.decode_state({k.encode(): v.encode() for k, v in good.items()}) is not None


@pytest.mark.parametrize(
    "bad",
    [
        {"position": 0},
        {"position": True},
        {"position": 2.0},
        {"gap_ahead_s": -0.1},
        {"gap_leader_s": math.nan},
        {"last_lap_time_s": math.inf},
        {"undercut_prob": 1.01},
        {"undercut_prob": -0.01},
        {"undercut_prob": "0.5"},
        {"tyre_age_laps": -1},
        {"tyre_age_laps": 3.0},
        {"tyre_compound": "soft"},
        {"tyre_compound": ""},
        {"undercut_flag": 1},
    ],
)
def test_invalid_states_rejected(bad: dict[str, Any]) -> None:
    with pytest.raises(R.StoreError):
        state(**bad)


# ------------------------------------------------------------------ order
def test_order_is_sorted_by_position(store: R.RaceStore, client: fakeredis.FakeRedis) -> None:
    store.write_batch(2024, 5, R.Batch(order={"leclerc": 3, "max_verstappen": 1, "norris": 2}))
    assert store.get_order(2024, 5) == [("max_verstappen", 1), ("norris", 2), ("leclerc", 3)]
    assert client.zscore("race:2024:5:order", "leclerc") == 3.0
    store.write_batch(2024, 5, R.Batch(order={"leclerc": 1, "max_verstappen": 2, "norris": 3}))
    assert [d for d, _ in store.get_order(2024, 5)] == ["leclerc", "max_verstappen", "norris"]


@pytest.mark.parametrize(
    "order",
    [{}, {"norris": 1, "leclerc": 1}, {"norris": 0}, {"NOR": 1}, {"norris": True}],
)
def test_invalid_orders_rejected(order: dict[str, Any], store: R.RaceStore) -> None:
    with pytest.raises((R.StoreError, K.InvalidKeyError)):
        store.write_batch(2024, 5, R.Batch(order=order))


# ------------------------------------------------------------------ TTL
def test_every_write_sets_the_safety_ttl(store: R.RaceStore, client: fakeredis.FakeRedis) -> None:
    store.write_batch(2024, 5, _batch())
    keys = store.race_keys(2024, 5)
    assert keys == sorted(
        [
            "race:2024:5:driver:leclerc",
            "race:2024:5:driver:max_verstappen",
            "race:2024:5:driver:norris",
            "race:2024:5:order",
        ]
    )
    assert all(AFTER < client.ttl(k) <= SAFETY for k in keys)


def test_ttl_after_session_end_on_every_race_key_only(
    store: R.RaceStore, client: fakeredis.FakeRedis
) -> None:
    store.write_batch(2024, 2, _batch())
    store.write_batch(2024, 25, _batch())
    store.write_batch(2024, 12, _batch())
    client.set("race:2024:2:extra", "x")  # any key of the race, even without a TTL
    assert client.ttl("race:2024:2:extra") == -1
    assert store.end_session(2024, 2) == 5
    for key in store.race_keys(2024, 2):
        assert 0 < client.ttl(key) <= AFTER, key
    assert 0 < client.ttl("race:2024:2:order") <= AFTER
    for rnd in (25, 12):
        keys = store.race_keys(2024, rnd)
        assert len(keys) == 4 and all(client.ttl(k) > AFTER for k in keys)


def test_expire_race_scans_every_page(store: R.RaceStore, client: fakeredis.FakeRedis) -> None:
    states = {f"d{i:04d}": state(i + 1) for i in range(1200)}
    store.write_batch(2024, 5, R.Batch(states=states))
    assert store.expire_race(2024, 5) == 1200
    assert all(0 < client.ttl(K.driver_key(2024, 5, d)) <= AFTER for d in states)
    assert store.expire_race(2024, 6) == 0


# ------------------------------------------------------------------ pub/sub
def test_events_are_json_on_the_race_channel(
    store: R.RaceStore, client: fakeredis.FakeRedis
) -> None:
    sub = client.pubsub()
    sub.subscribe("race:2024:5:events")
    assert sub.get_message(timeout=1)["type"] == "subscribe"
    other = client.pubsub()
    other.subscribe("race:2024:6:events")
    other.get_message(timeout=1)
    store.write_batch(2024, 5, _batch())
    store.end_session(2024, 5, T0)
    got = []
    while (m := sub.get_message(timeout=0.2)) is not None:
        got.append(json.loads(m["data"]))
    assert [e["type"] for e in got] == ["lap", "undercut_alert", "session_end"]
    for e in got:
        assert set(e) == {"type", "season", "round", "event_ts", "published_at", "payload"}
        assert (e["season"], e["round"]) == (2024, 5) and ISO_MS.match(e["published_at"])
    assert got[0]["event_ts"] == "2024-05-05T20:14:03.125Z" and got[0]["payload"] == {"lap": 31}
    assert got[1]["event_ts"] is None and got[1]["payload"] == {"attacker": "norris"}
    assert other.get_message(timeout=0.2) is None
    sub.close()
    other.close()


@pytest.mark.parametrize("event_type", ["pit", "LAP", "", "undercut"])
def test_unknown_event_types_rejected(event_type: str, store: R.RaceStore) -> None:
    with pytest.raises(R.StoreError, match="event type"):
        store.write_batch(2024, 5, R.Batch(events=(R.Event(event_type),)))


def test_non_finite_payload_rejected(store: R.RaceStore) -> None:
    with pytest.raises(R.StoreError, match="finite JSON"):
        store.write_batch(2024, 5, R.Batch(events=(R.Event("lap", {"x": math.nan}),)))
    with pytest.raises(R.StoreError, match="finite JSON"):
        store.write_batch(2024, 5, R.Batch(events=(R.Event("lap", {"x": object()}),)))


# ------------------------------------------------------------------ pipelines
class _Spy:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.calls: list[dict[str, Any]] = []
        self.executes = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def pipeline(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        pipe = self.client.pipeline(**kwargs)
        real = pipe.execute
        spy = self

        def execute(*a: Any, **k: Any) -> Any:
            spy.executes += 1
            return real(*a, **k)

        pipe.execute = execute
        return pipe


def test_one_pipeline_and_one_execute_per_batch(client: fakeredis.FakeRedis) -> None:
    spy = _Spy(client)
    store = R.RaceStore(spy, _settings(), now=_clock())  # type: ignore[arg-type]
    n = store.write_batch(2024, 5, _batch())
    assert n == 3 * 2 + 2 + 2
    assert spy.calls == [{"transaction": False}] and spy.executes == 1
    store.write_batch(2024, 5, _batch())
    assert spy.calls == [{"transaction": False}] * 2 and spy.executes == 2


def test_invalid_batch_sends_nothing(store: R.RaceStore, client: fakeredis.FakeRedis) -> None:
    with pytest.raises(R.StoreError, match="empty micro-batch"):
        store.write_batch(2024, 5, R.Batch())
    bad = R.Batch(states={"norris": state(2)}, order={"norris": 1, "leclerc": 1})
    with pytest.raises(R.StoreError, match="duplicate"):
        store.write_batch(2024, 5, bad)
    with pytest.raises(K.InvalidKeyError):
        store.write_batch(2024, 31, R.Batch(states={"norris": state(2)}))
    assert client.dbsize() == 0  # validation fails while queueing, before execute


def test_store_uses_the_redis_url_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDIS_URL", "redis://cache.internal:6390/3")
    monkeypatch.setenv("REDIS_SAFETY_TTL_S", "999")
    from f1sa.config import get_settings

    get_settings.cache_clear()
    sync = R.RaceStore.from_settings()
    kw = sync.client.connection_pool.connection_kwargs
    assert (kw["host"], kw["port"], kw["db"]) == ("cache.internal", 6390, 3)
    assert sync.settings.redis_safety_ttl_s == 999
    aio = R.AsyncRaceStore.from_settings()
    akw = aio.client.connection_pool.connection_kwargs
    assert (akw["host"], akw["port"], akw["db"]) == ("cache.internal", 6390, 3)
    sync.client.close()
    asyncio.run(aio.client.aclose())


# ------------------------------------------------------------------ async
def test_async_store_matches_the_sync_store(server: fakeredis.FakeServer) -> None:
    sync_client = fakeredis.FakeRedis(server=server, decode_responses=True)
    sync_store = R.RaceStore(sync_client, _settings(), now=_clock())

    async def run() -> None:
        client = fakeredis.FakeAsyncRedis(server=server, decode_responses=True)
        spy = _Spy(client)
        store = R.AsyncRaceStore(spy, _settings(), now=_clock())  # type: ignore[arg-type]
        sub = client.pubsub()
        await sub.subscribe("race:2025:7:events")
        await sub.get_message(timeout=1)
        assert await store.write_batch(2025, 7, _batch()) == 10
        assert spy.calls == [{"transaction": False}] and spy.executes == 1
        got = await store.get_driver_state(2025, 7, "norris")
        assert got is not None and got.state == state(2)
        assert await store.get_order(2025, 7) == [
            ("max_verstappen", 1),
            ("norris", 2),
            ("leclerc", 3),
        ]
        keys = await store.race_keys(2025, 7)
        assert len(keys) == 4
        assert all(AFTER < ttl <= SAFETY for ttl in [await client.ttl(k) for k in keys])
        assert await store.end_session(2025, 7) == 4
        assert all(0 < ttl <= AFTER for ttl in [await client.ttl(k) for k in keys])
        types = []
        while (m := await sub.get_message(timeout=0.2)) is not None:
            types.append(json.loads(m["data"])["type"])
        assert types == ["lap", "undercut_alert", "session_end"]
        await sub.aclose()
        await client.aclose()

    asyncio.run(run())
    sync_store.write_batch(2024, 5, _batch())
    a = sync_client.hgetall("race:2025:7:driver:norris")
    b = sync_client.hgetall("race:2024:5:driver:norris")
    assert {k: v for k, v in a.items() if k != "updated_at"} == {
        k: v for k, v in b.items() if k != "updated_at"
    }
    sync_client.close()
