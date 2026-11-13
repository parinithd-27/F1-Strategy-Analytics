"""``POST /api/races/{season}/{round}/whatif`` (S15): 200 shape, 404 / 409 / 422 / 503, the lap
history feeds and the what-if lake readers' limits."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from _api_fixtures import (
    DEFAULT_RACES,
    ApiHarness,
    DirFakeHttpFS,
    httpfs_session,
    make_api,
    make_settings,
    write_file_lake,
)
from _sim_fixtures import RaceSpec, lake_readers

from f1sa.api.catalog import (
    SIM_CLEAN_TABLES,
    SIM_FEATURE_TABLES,
    CatalogError,
    HttpFsLakeReader,
    SimReaders,
    sim_readers,
    zone_reader,
)
from f1sa.serving.redis_store import AsyncRaceStore, Batch, DriverState, Event
from f1sa.sim import tyre_model as T

pytestmark = pytest.mark.anyio
URL = "/api/races/2024/5/whatif"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _api(tmp_path: Path, *, tyre_model: bool = True) -> ApiHarness:
    write_file_lake(tmp_path / "lake", DEFAULT_RACES)  # 2024 R5 is synthetic in the catalog
    specs = [RaceSpec(2024, 4, drivers=4), RaceSpec(2024, 5, drivers=4)]
    clean, features = lake_readers(specs)
    if tyre_model:
        art = T.build_artefact(clean, features, source="sample", lake_scheme="file")
        T.write_artefact(art, T.default_output("sample", tmp_path), root=tmp_path)
    settings = make_settings(tmp_path, api_sim_dir=tmp_path / "models" / "sim")
    return make_api(settings, sim_reader_factory=lambda: SimReaders(clean, features))


def _ds(pos: int, gap: float | None, prob: float | None = None) -> DriverState:
    return DriverState(pos, gap, None, "MEDIUM", 15, 90.0, prob is not None, prob)


async def _seed(api: ApiHarness, laps: int = 12) -> None:
    store = AsyncRaceStore(api.redis, api.deps.settings)
    states = {
        "drv0": _ds(1, None),
        "drv1": _ds(2, 2.0, 0.21),
        "drv2": _ds(3, 1.4, 0.44),
        "drv3": _ds(4, 9.0),
    }
    await store.write_batch(
        2024, 5, Batch(states=states, order={d: s.position for d, s in states.items()})
    )


async def _laps(api: ApiHarness, laps: int = 12) -> None:
    """Publish laps 1..laps of every car on the race channel (after the recorder subscribed)."""
    store = AsyncRaceStore(api.redis, api.deps.settings)
    offsets = {"drv0": 0.0, "drv1": 2.0, "drv2": 3.4, "drv3": 12.4}
    events = []
    for lap in range(1, laps + 1):
        for d, off in offsets.items():
            t = off + 90.0 * lap
            events.append(
                Event(
                    "lap",
                    {
                        "driver_id": d,
                        "lap": lap,
                        "race_time_s": t,
                        "lap_time_s": 90.0,
                        "compound": "MEDIUM",
                        "tyre_age": lap,
                    },
                )
            )
    await store.write_batch(2024, 5, Batch(events=events))
    log = api.deps.lap_logs.get(2024, 5)
    assert log is not None

    async def wait() -> None:
        while log.latest_lap("drv3") != laps:
            await asyncio.sleep(0.02)

    await asyncio.wait_for(wait(), 5)


async def test_200_three_strategies_next_to_the_model(tmp_path: Path) -> None:
    api = _api(tmp_path)
    await _seed(api)
    async with api.client() as c:
        await api.deps.lap_logs.ensure(2024, 5)
        await _laps(api)
        r = await c.post(URL, json={"attacker": "drv2", "defender": "drv1"})
        again = await c.post(URL, json={"attacker": "drv2", "defender": "drv1"})
        other = await c.post(
            URL, json={"attacker": "drv2", "defender": "drv1", "seed": 7, "n": 200}
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert [s["strategy"] for s in body["strategies"]] == ["pit_now", "pit_next_lap", "stay_out"]
    assert [s["response"] for s in body["sensitivity"]] == ["sensitivity"] * 3
    now = body["strategies"][0]
    assert (now["attacker_pit_lap"], now["defender_pit_lap"], now["evaluation_lap"]) == (13, 14, 15)
    assert 0.0 <= now["p_attacker_ahead"] <= 1.0 and len(now["interval80_s"]) == 2
    assert body["model"] == {
        "probability": 0.44,
        "undercut_flag": True,
        "model_version": "deploy_real_G3.5_k2",
        "source": "redis:undercut_prob",
    }
    i = body["inputs"]
    assert (
        i["current_lap"] == 12
        and i["history"] == "race_channel"
        and i["gap_s"] == pytest.approx(1.4)
    )
    assert i["synthetic"] is True and i["race_laps"] == 56 and i["traffic_cars"] == 2
    assert (body["n"], body["seed"]) == (500, 42) and body["runtime_ms"] < 1000
    strip = {k: v for k, v in body.items() if k != "runtime_ms"}
    assert strip == {k: v for k, v in again.json().items() if k != "runtime_ms"}
    assert other.json()["n"] == 200 and other.json()["strategies"] != body["strategies"]


async def test_without_history_it_still_answers(tmp_path: Path) -> None:
    api = _api(tmp_path)
    await _seed(api)
    async with api.client() as c:
        r = await c.post(URL, json={"attacker": "drv2", "defender": "drv1"})
    assert r.status_code == 200
    assert (
        r.json()["inputs"]["history"] == "none"
        and "no_lap_history" in r.json()["inputs"]["warnings"]
    )


@pytest.mark.parametrize(
    ("body", "status", "detail"),
    [
        ({"attacker": "drv3", "defender": "drv1"}, 409, "not directly ahead"),
        ({"attacker": "drv1", "defender": "drv2"}, 409, "not directly ahead"),
        ({"attacker": "drv2", "defender": "ghost"}, 404, "ghost"),
    ],
)
async def test_404_and_409(tmp_path: Path, body: dict[str, Any], status: int, detail: str) -> None:
    api = _api(tmp_path)
    await _seed(api)
    async with api.client() as c:
        r = await c.post(URL, json=body)
    assert r.status_code == status and detail in r.json()["detail"]


async def test_unknown_race_or_no_state_is_404(tmp_path: Path) -> None:
    api = _api(tmp_path)
    async with api.client() as c:
        no_state = await c.post(URL, json={"attacker": "drv2", "defender": "drv1"})
        not_lake = await c.post(
            "/api/races/2022/1/whatif", json={"attacker": "drv2", "defender": "drv1"}
        )
        sealed = await c.post(
            "/api/races/2025/6/whatif", json={"attacker": "drv2", "defender": "drv1"}
        )
    assert no_state.status_code == 404 and "no live state" in no_state.json()["detail"]
    assert not_lake.status_code == 404 and sealed.status_code == 404


@pytest.mark.parametrize(
    "body",
    [
        {"attacker": "drv2"},
        {"attacker": "drv2", "defender": "drv2"},
        {"attacker": "DRV2", "defender": "drv1"},
        {"attacker": "drv2", "defender": "drv1", "n": 99},
        {"attacker": "drv2", "defender": "drv1", "n": 5001},
        {"attacker": "drv2", "defender": "drv1", "n": "500"},
        {"attacker": "drv2", "defender": "drv1", "seed": -1},
        {"attacker": "drv2", "defender": "drv1", "extra": 1},
    ],
)
async def test_422_bad_input(tmp_path: Path, body: dict[str, Any]) -> None:
    api = _api(tmp_path)
    async with api.client() as c:
        r = await c.post(URL, json=body)
        bad_path = await c.post(
            "/api/races/2024/99/whatif", json={"attacker": "a", "defender": "b"}
        )
    assert r.status_code == 422 and bad_path.status_code == 422


async def test_503_without_a_tyre_model(tmp_path: Path) -> None:
    api = _api(tmp_path, tyre_model=False)
    await _seed(api)
    async with api.client() as c:
        r = await c.post(URL, json={"attacker": "drv2", "defender": "drv1"})
    assert r.status_code == 503 and "sim-tyre-model-sample" in r.json()["detail"]


async def test_sse_feeds_the_lap_log_without_a_second_subscription(tmp_path: Path) -> None:
    api = _api(tmp_path)
    store = AsyncRaceStore(api.redis, api.deps.settings)
    channel = "race:2024:5:events"

    async def publisher() -> None:
        while (await api.redis.pubsub_numsub(channel))[0][1] < 1:
            await asyncio.sleep(0.01)
        assert (await api.redis.pubsub_numsub(channel))[0][1] == 1  # only the SSE stream
        await store.write_batch(
            2024,
            5,
            Batch(
                events=[
                    Event(
                        "lap",
                        {"driver_id": "drv1", "lap": 3, "race_time_s": 270.0, "lap_time_s": 90.0},
                    )
                ]
            ),
        )
        await store.write_batch(2024, 5, Batch(events=[Event("session_end", {})]))

    async with api.client() as c:
        await asyncio.wait_for(asyncio.gather(c.get("/api/races/2024/5/events"), publisher()), 10)
    log = api.deps.lap_logs.get(2024, 5)
    assert log is not None and log.latest_lap("drv1") == 3 and log.ended
    assert api.deps.lap_logs.running() == []


# ------------------------------------------------------------------ what-if readers
def test_zone_readers_are_limited_to_their_tables(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    r = sim_readers(settings)
    assert r.clean.tables == SIM_CLEAN_TABLES and r.features.tables == SIM_FEATURE_TABLES  # type: ignore[attr-defined]
    for reader, table in (
        (r.clean, "results"),
        (r.clean, "pit_loss"),
        (r.features, "laps"),
        (r.features, "undercut_attempts"),
    ):
        with pytest.raises(CatalogError, match="not readable"):
            reader.read_rows(table, 2024, 5, ["x"])
    for zone in ("raw", "models", "../clean"):
        with pytest.raises(CatalogError):
            zone_reader(settings, zone, ("laps",))
    with pytest.raises(CatalogError, match="invalid table"):
        zone_reader(settings, "clean", ("../raw",))


def test_httpfs_what_if_readers_stay_in_their_zone(tmp_path: Path) -> None:
    fake = DirFakeHttpFS()
    fake.add_tree("/f1/features", {"pit_loss/season=2024/round=5/part-0.parquet": b"x"})
    fake.add_tree("/f1/raw", {"jolpica/laps/season=2024/round=5/laps.json": b"[]"})
    settings = make_settings(tmp_path, data_lake_uri="hdfs://namenode:8020/f1")
    r = sim_readers(settings, session=httpfs_session(fake))
    feat = r.features
    assert isinstance(feat, HttpFsLakeReader) and feat.clean == "/f1/features"
    assert feat.partitions("pit_loss") == [(2024, 5)]
    for path in (
        "/f1/raw/jolpica",
        "/f1/features/../raw",
        "/f1/features/undercut_attempts",
        "/f1/clean/laps",
    ):
        with pytest.raises(CatalogError):
            feat._get("LISTSTATUS", path)
    with pytest.raises(CatalogError, match="not allowed"):
        feat._get("DELETE", "/f1/features/pit_loss")
    assert all(p.startswith("/f1/features/") for _, p in fake.ops) and set(fake.methods) == {"GET"}
