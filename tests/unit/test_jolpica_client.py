"""Tests for f1sa.ingest.jolpica_client against recorded fixtures (no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pytest
import requests
from _fakes import OK_PAGE, FakeClock, FixtureAdapter, ScriptedAdapter, load_fixture, page

from f1sa.config import Settings
from f1sa.ingest import jolpica_client as jc
from f1sa.ingest.jolpica_client import (
    JolpicaClient,
    JolpicaHTTPError,
    JolpicaResponseError,
    page_item_count,
    parse_duration_s,
    parse_laps,
)
from f1sa.ingest.ratelimit import DualRateLimiter


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def make_client(
    clock: FakeClock, transport: requests.adapters.HTTPAdapter, **kwargs: object
) -> JolpicaClient:
    limiter = DualRateLimiter(per_hour=450, clock=clock, sleep=clock.sleep)
    return JolpicaClient(
        Settings(),
        limiter=limiter,
        transport=transport,
        cache_backend="memory",
        sleep=clock.sleep,
        rng=lambda: 0.0,
        **kwargs,  # type: ignore[arg-type]
    )


@pytest.fixture
def fixtures() -> FixtureAdapter:
    return FixtureAdapter()


@pytest.fixture
def client(clock: FakeClock, fixtures: FixtureAdapter) -> JolpicaClient:
    return make_client(clock, fixtures)


def _dtypes(df: pd.DataFrame) -> dict[str, str]:
    return {c: str(t) for c, t in df.dtypes.items()}


# ---------------------------------------------------------------- typed endpoints
def test_results_typed(client: JolpicaClient) -> None:
    df = client.results(2023, 5)
    assert _dtypes(df) == jc.RESULTS_SCHEMA
    assert len(df) == 3
    ver = df.set_index("driver_id").loc["max_verstappen"]
    assert ver["position"] == 1 and ver["grid"] == 9 and ver["points"] == 26.0
    assert ver["time_ms"] == 5258241
    assert ver["fastest_lap_time_s"] == pytest.approx(89.708)
    sar = df.set_index("driver_id").loc["sargeant"]
    assert sar["status"] == "Lapped" and sar["laps"] == 56


def test_qualifying_missing_sessions_are_na(client: JolpicaClient) -> None:
    df = client.qualifying(2023, 5).set_index("driver_id")
    assert _dtypes(client.qualifying(2023, 5)) == jc.QUALIFYING_SCHEMA
    assert df.loc["perez", "q3_s"] == pytest.approx(86.841)
    assert pd.isna(df.loc["sargeant", "q2_s"]) and pd.isna(df.loc["sargeant", "q3_s"])


def test_race_and_races(client: JolpicaClient) -> None:
    race = client.race(2023, 5)
    assert _dtypes(race) == jc.RACES_SCHEMA
    row = race.iloc[0]
    assert row["race_name"] == "Miami Grand Prix" and row["circuit_id"] == "miami"
    assert row["race_start_utc"] == pd.Timestamp("2023-05-07T19:30:00Z")
    assert list(client.races(2023)["round"]) == [4, 5, 6]


def test_seasons(client: JolpicaClient) -> None:
    df = client.seasons()
    assert _dtypes(df) == jc.SEASONS_SCHEMA
    assert list(df["season"]) == [1950, 2021, 2022, 2023, 2024, 2025]


def test_drivers_and_constructors(client: JolpicaClient) -> None:
    drivers = client.drivers(2023, 5)
    assert _dtypes(drivers) == jc.DRIVERS_SCHEMA
    assert set(drivers["driver_id"]) == {"max_verstappen", "perez", "sargeant"}
    assert (drivers["season"] == 2023).all() and (drivers["round"] == 5).all()
    cons = client.constructors(2023, 5)
    assert _dtypes(cons) == jc.CONSTRUCTORS_SCHEMA
    assert set(cons["constructor_id"]) == {"red_bull", "williams"}


def test_pitstops_sorted_and_parsed(client: JolpicaClient) -> None:
    df = client.pitstops(2023, 5)
    assert _dtypes(df) == jc.PITSTOPS_SCHEMA
    assert list(df["driver_id"]) == ["max_verstappen", "perez", "sargeant"]
    assert df.set_index("driver_id").loc["sargeant", "duration_s"] == pytest.approx(48.938)


# ---------------------------------------------------------------- pagination
def test_laps_paginate_with_server_capped_limit(
    client: JolpicaClient, fixtures: FixtureAdapter
) -> None:
    df = client.laps(2023, 5)
    assert _dtypes(df) == jc.LAPS_SCHEMA
    queries = [parse_qs(urlparse(r.url or "").query) for r in fixtures.requests]
    assert [q["offset"] for q in queries] == [["0"], ["4"], ["8"]]  # server said limit=4
    assert all(q["limit"] == ["100"] for q in queries)  # we always ask for the documented max
    assert len(df) == 9
    assert df.groupby("lap").size().to_dict() == {1: 3, 2: 3, 3: 3}  # split laps merged
    lap2 = df[df["lap"] == 2].sort_values("position")
    assert list(lap2["position"]) == sorted(lap2["position"])
    assert df["time_s"].notna().all()


def test_page_item_count_counts_timings() -> None:
    assert page_item_count(load_fixture("laps_2023_5_p0")) == 4
    assert page_item_count(load_fixture("laps_2023_5_p2")) == 1
    assert page_item_count(load_fixture("results_2023_5")) == 3
    assert page_item_count(load_fixture("race_2023_5")) == 1
    assert page_item_count(load_fixture("drivers_2023_5")) == 3
    assert page_item_count(load_fixture("seasons")) == 6
    with pytest.raises(JolpicaResponseError):
        page_item_count({"MRData": {"Unknown": {}}})


def test_multi_page_generic(clock: FakeClock) -> None:
    transport = ScriptedAdapter(
        [(200, {}, page(3, 2, 0, ["2021", "2022"])), (200, {}, page(3, 2, 2, ["2023"]))]
    )
    df = make_client(clock, transport).seasons()
    assert list(df["season"]) == [2021, 2022, 2023]
    assert transport.calls == 2


def test_empty_page_before_total_is_an_error(clock: FakeClock) -> None:
    transport = ScriptedAdapter([(200, {}, page(5, 2, 0, []))])
    with pytest.raises(JolpicaResponseError, match="empty page"):
        make_client(clock, transport).seasons()


def test_zero_limit_is_an_error(clock: FakeClock) -> None:
    transport = ScriptedAdapter([(200, {}, page(5, 0, 0, ["2021"]))])
    with pytest.raises(JolpicaResponseError, match="limit=0"):
        make_client(clock, transport).seasons()


def test_empty_result_is_empty_typed_frame(clock: FakeClock) -> None:
    transport = ScriptedAdapter([(200, {}, page(0, 100, 0, []))])
    df = make_client(clock, transport).seasons()
    assert df.empty and _dtypes(df) == jc.SEASONS_SCHEMA


# ---------------------------------------------------------------- caching / rate limiting
def test_cache_hit_uses_no_token_and_no_network(
    client: JolpicaClient, fixtures: FixtureAdapter
) -> None:
    client.results(2023, 5)
    budget_after_first = client.limiter.available_this_hour
    assert client.stats.network == 1 and client.stats.cache_hits == 0
    client.results(2023, 5)
    assert client.stats.network == 1
    assert client.stats.cache_hits == 1
    assert len(fixtures.requests) == 1
    assert client.limiter.available_this_hour == budget_after_first == 449


def test_refresh_bypasses_cache(client: JolpicaClient, fixtures: FixtureAdapter) -> None:
    client.results(2023, 5)
    client.results(2023, 5, refresh=True)
    assert len(fixtures.requests) == 2 and client.stats.network == 2


def test_network_requests_take_rate_limit_tokens(client: JolpicaClient, clock: FakeClock) -> None:
    client.laps(2023, 5)  # 3 network pages
    assert client.limiter.available_this_hour == 447
    assert clock.sleeps == [pytest.approx(1 / 3)] * 2  # paced: at most 4 per rolling second


def test_429_without_retry_after_backs_off_long(clock: FakeClock) -> None:
    transport = ScriptedAdapter([(429, {}, b"{}"), (429, {}, b"{}"), (200, {}, OK_PAGE())])
    client = make_client(clock, transport)
    client.seasons()
    assert [s for s in clock.sleeps if s >= 1] == [5.0, 10.0]
    assert client.stats.throttled == 2 and client.stats.retries == 2


def test_default_limiter_persists_request_log() -> None:
    settings = Settings()
    with JolpicaClient(settings, transport=FixtureAdapter(), cache_backend="memory") as client:
        client.seasons()
    log = settings.jolpica_cache_dir / "requests.log"
    assert len(log.read_text().splitlines()) == 1
    with JolpicaClient(settings, transport=FixtureAdapter(), cache_backend="memory") as again:
        assert again.limiter.available_this_hour == settings.jolpica_max_per_hour - 1


def test_user_agent_and_accept_headers(client: JolpicaClient, fixtures: FixtureAdapter) -> None:
    client.seasons()
    headers = fixtures.requests[0].headers
    assert headers["User-Agent"] == Settings().jolpica_user_agent
    assert "parinith" not in headers["User-Agent"].lower()
    assert headers["Accept"] == "application/json"


def test_unknown_endpoint_404_is_not_retried(
    client: JolpicaClient, fixtures: FixtureAdapter
) -> None:
    with pytest.raises(JolpicaHTTPError) as err:
        client.get_pages("1999/99/laps")
    assert err.value.status == 404
    assert len(fixtures.requests) == 1


# ---------------------------------------------------------------- retries
def test_429_honours_retry_after_seconds(clock: FakeClock) -> None:
    transport = ScriptedAdapter([(429, {"Retry-After": "3"}, b"{}"), (200, {}, OK_PAGE())])
    client = make_client(clock, transport)
    assert list(client.seasons()["season"]) == [2023]
    assert 3.0 in clock.sleeps
    assert client.stats.retries == 1 and transport.calls == 2


def test_429_honours_retry_after_http_date(clock: FakeClock) -> None:
    when = format_datetime(datetime.now(UTC) + timedelta(seconds=30), usegmt=True)
    transport = ScriptedAdapter([(429, {"Retry-After": when}, b"{}"), (200, {}, OK_PAGE())])
    make_client(clock, transport).seasons()
    retry_sleeps = [s for s in clock.sleeps if s > 1]
    assert retry_sleeps and 25 <= retry_sleeps[0] <= 30


def test_5xx_exponential_backoff(clock: FakeClock) -> None:
    transport = ScriptedAdapter([(503, {}, b""), (502, {}, b""), (200, {}, OK_PAGE())])
    client = make_client(clock, transport, backoff_base_s=1.0)
    client.seasons()
    assert [s for s in clock.sleeps if s >= 1.0] == [1.0, 2.0]  # rng=0 -> no jitter


def test_backoff_has_cap_and_jitter(clock: FakeClock) -> None:
    client = make_client(clock, ScriptedAdapter([]), backoff_base_s=1.0, backoff_max_s=5.0)
    client._rng = lambda: 1.0
    assert client._backoff(0) == pytest.approx(1.25)
    assert client._backoff(10) == pytest.approx(5.0 * 1.25)


def test_retries_exhausted_raises(clock: FakeClock) -> None:
    transport = ScriptedAdapter([(500, {}, b"")] * 3)
    with pytest.raises(JolpicaHTTPError) as err:
        make_client(clock, transport, max_retries=2).seasons()
    assert err.value.status == 500 and transport.calls == 3


def test_connection_error_retried_then_raises(clock: FakeClock) -> None:
    transport = ScriptedAdapter(
        [requests.ConnectionError("boom"), requests.Timeout("slow"), (200, {}, OK_PAGE())]
    )
    client = make_client(clock, transport)
    client.seasons()
    assert client.stats.retries == 2
    failing = ScriptedAdapter([requests.ConnectionError("down")] * 2)
    with pytest.raises(JolpicaHTTPError) as err:
        make_client(clock, failing, max_retries=1).seasons()
    assert err.value.status is None


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"[1, 2]",
        json.dumps({"MRData": {"limit": "x", "offset": "0", "total": "1"}}).encode(),
    ],
)
def test_bad_response_body(clock: FakeClock, body: bytes) -> None:
    with pytest.raises(JolpicaResponseError):
        make_client(clock, ScriptedAdapter([(200, {}, body)])).seasons()


@pytest.mark.parametrize(
    "kwargs",
    [{"max_retries": -1}, {"backoff_base_s": 0}, {"backoff_base_s": 5, "backoff_max_s": 1}],
)
def test_invalid_client_args(clock: FakeClock, kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        make_client(clock, ScriptedAdapter([]), **kwargs)


@pytest.mark.parametrize("path", ["", "/", "../etc"])
def test_invalid_path(client: JolpicaClient, path: str) -> None:
    with pytest.raises(ValueError):
        client.url_for(path)


def test_sqlite_cache_created_under_settings_dir(tmp_path: object) -> None:
    settings = Settings()
    with JolpicaClient(settings, transport=FixtureAdapter()) as client:
        client.seasons()
    assert (settings.jolpica_cache_dir / "http_cache.sqlite").exists()


# ---------------------------------------------------------------- parsing helpers
@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        ("31.234", 31.234),
        ("1:31.234", 91.234),
        ("1:27:38.241", 5258.241),
        ("21:03.1", 1263.1),
        (" 22.688 ", 22.688),
        ("", None),
        (None, None),
    ],
)
def test_parse_duration_s(text: str | None, seconds: float | None) -> None:
    result = parse_duration_s(text)
    assert result == (None if seconds is None else pytest.approx(seconds))


@pytest.mark.parametrize("text", ["+34.388", "1:2:3:4", "abc", "1:61.0x", "1:61.0", "1:60:00.0"])
def test_parse_duration_rejects_malformed(text: str) -> None:
    with pytest.raises(ValueError):
        parse_duration_s(text)


def test_parse_laps_dedupes_identical_and_rejects_conflicts() -> None:
    p0 = load_fixture("laps_2023_5_p0")
    df = parse_laps([p0, p0])  # same page twice -> identical duplicates dropped
    assert len(df) == 4
    conflicting = json.loads(json.dumps(p0))
    conflicting["MRData"]["RaceTable"]["Races"][0]["Laps"][0]["Timings"][0]["time"] = "9:59.999"
    with pytest.raises(JolpicaResponseError, match="conflicting"):
        parse_laps([p0, conflicting])


def test_non_integer_field_raises() -> None:
    bad = load_fixture("results_2023_5")
    bad["MRData"]["RaceTable"]["Races"][0]["Results"][0]["grid"] = "pit"
    with pytest.raises(JolpicaResponseError, match="integer"):
        jc.parse_results([bad])


# ---------------------------------------------------------------- sprint + cache probes
def test_sprint_results_same_schema_as_results(client: JolpicaClient) -> None:
    df = client.sprint_results(2023, 4)
    assert _dtypes(df) == jc.RESULTS_SCHEMA
    per = df.set_index("driver_id").loc["perez"]
    assert per["position"] == 1 and per["points"] == 8.0
    assert page_item_count(load_fixture("sprint_2023_4")) == 3


def test_season_level_drivers_have_no_round(client: JolpicaClient) -> None:
    df = client.drivers(2023)
    assert (df["season"] == 2023).all() and df["round"].isna().all()
    assert len(client.constructors(2023)) == 2


def test_cached_page_and_requests_needed_never_touch_network(
    client: JolpicaClient, fixtures: FixtureAdapter
) -> None:
    assert client.cached_page("2023/5/laps") is None
    assert client.requests_needed("2023/5/laps", 12) == 12  # nothing cached: default estimate
    assert fixtures.requests == [] and client.stats.network == 0
    client.get_pages("2023/5/laps")  # 3 pages (server limit 4, total 9)
    sent = len(fixtures.requests)
    assert client.cached_page("2023/5/laps", 4) is not None
    assert client.requests_needed("2023/5/laps", 12) == 0  # all pages cached
    assert len(fixtures.requests) == sent  # probes were cache-only
