"""Typed client for the Jolpica-F1 (Ergast-compatible) API.

* Base URL and User-Agent come from :class:`f1sa.config.Settings`.
* Responses are cached in SQLite (``Settings.jolpica_cache_dir``) and never expire, since the
  data is historical. ``refresh=True`` forces a re-download.
* Every request that actually goes to the network passes through a :class:`DualRateLimiter`
  (4 req/s burst, ``Settings.jolpica_max_per_hour`` sustained). Cache hits never touch it.
* 429, 5xx and connection errors are retried with exponential backoff + jitter, honouring
  ``Retry-After`` when the server sends it.
* Jolpica caps ``limit`` at 100. Paging always advances by the ``MRData.limit`` the server
  reports, until ``MRData.total`` is reached.

Data is CC BY-NC-SA 4.0 (Jolpica-F1); see DATA_LICENSES.md.
"""

from __future__ import annotations

import logging
import random
import re
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, cast

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests_cache import NEVER_EXPIRE, CachedSession

from f1sa.config import Settings
from f1sa.ingest.ratelimit import DualRateLimiter, Sleep

logger = logging.getLogger(__name__)

PAGE_LIMIT = 100
"""Largest page size Jolpica serves (larger requests are silently capped to this)."""
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
MAX_RETRY_AFTER_S = 600.0
THROTTLE_BACKOFF_BASE_S = 5.0
"""First wait after a 429 without Retry-After (then 10, 20, 40, capped at 60 s)."""
REQUEST_LOG_NAME = "requests.log"
SQLITE_BUSY_TIMEOUT_MS = 30_000
REQUEST_TIMEOUT_S = 30.0
_MAX_PAGES = 1000

JsonPage = dict[str, Any]


class JolpicaError(RuntimeError):
    """Base class for Jolpica client errors."""


class JolpicaHTTPError(JolpicaError):
    """A request failed with a non-retryable status, or retries were exhausted."""

    def __init__(self, status: int | None, url: str, detail: str = "") -> None:
        self.status = status
        self.url = url
        what = f"HTTP {status}" if status is not None else "connection error"
        super().__init__(f"Jolpica request failed ({what}): {url}{': ' + detail if detail else ''}")


class JolpicaResponseError(JolpicaError):
    """The response body is not the expected Ergast-style JSON."""


@dataclass
class RequestStats:
    """Counters for one client instance."""

    network: int = 0
    cache_hits: int = 0
    retries: int = 0
    throttled: int = 0
    waited_s: float = 0.0


class _RateLimitedAdapter(HTTPAdapter):
    """Transport adapter that takes a rate-limit token before each *network* send.

    requests-cache serves cache hits before the adapter is reached, so hits are free.
    """

    def __init__(self, limiter: DualRateLimiter, stats: RequestStats, inner: HTTPAdapter) -> None:
        super().__init__()
        self._limiter = limiter
        self._stats = stats
        self._inner = inner

    def send(  # type: ignore[override]
        self, request: requests.PreparedRequest, **kwargs: Any
    ) -> requests.Response:
        self._stats.waited_s += self._limiter.acquire()
        self._stats.network += 1
        return self._inner.send(request, **kwargs)

    def close(self) -> None:
        self._inner.close()
        super().close()


class JolpicaClient:
    """Rate-limited, cached, retrying client returning typed DataFrames."""

    def __init__(
        self,
        settings: Settings,
        *,
        limiter: DualRateLimiter | None = None,
        transport: HTTPAdapter | None = None,
        cache_backend: str = "sqlite",
        sleep: Sleep = time.sleep,
        rng: Callable[[], float] = random.random,
        max_retries: int = 5,
        backoff_base_s: float = 1.0,
        backoff_max_s: float = 60.0,
        jitter: float = 0.25,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if backoff_base_s <= 0 or backoff_max_s < backoff_base_s:
            raise ValueError("need 0 < backoff_base_s <= backoff_max_s")
        self.base_url = settings.jolpica_base_url
        self.stats = RequestStats()
        self.limiter = limiter or default_limiter(settings, settings.jolpica_max_per_hour)
        self._sleep = sleep
        self._rng = rng
        self._max_retries = max_retries
        self._backoff_base_s = backoff_base_s
        self._backoff_max_s = backoff_max_s
        self._jitter = jitter
        backend_kwargs: dict[str, Any] = {}
        if cache_backend == "sqlite":
            settings.jolpica_cache_dir.mkdir(parents=True, exist_ok=True)
            cache_name = str(settings.jolpica_cache_dir / "http_cache")
            # another ingest process may be writing the same cache: wait, don't fail
            backend_kwargs["busy_timeout"] = SQLITE_BUSY_TIMEOUT_MS
        else:
            cache_name = "f1sa-jolpica"
        self.session = CachedSession(
            cache_name=cache_name,
            backend=cache_backend,
            expire_after=NEVER_EXPIRE,
            allowable_codes=(200,),
            cache_control=False,
            **backend_kwargs,
        )
        self.session.headers.update(
            {"User-Agent": settings.jolpica_user_agent, "Accept": "application/json"}
        )
        adapter = _RateLimitedAdapter(self.limiter, self.stats, transport or HTTPAdapter())
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def close(self) -> None:
        self.session.close()  # type: ignore[no-untyped-call]

    def __enter__(self) -> JolpicaClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ transport
    def url_for(self, path: str) -> str:
        """Full endpoint URL for a path such as ``2023/5/laps``."""
        path = path.strip("/")
        if not path or ".." in path:
            raise ValueError(f"invalid Jolpica path {path!r}")
        return f"{self.base_url}/{path}.json"

    def _backoff(self, attempt: int, base_s: float | None = None) -> float:
        base = self._backoff_base_s if base_s is None else base_s
        delay = min(self._backoff_max_s, base * (2.0**attempt))
        return delay * (1.0 + self._jitter * self._rng())

    @staticmethod
    def _retry_after(resp: requests.Response) -> float | None:
        value = resp.headers.get("Retry-After")
        if value is None:
            return None
        value = value.strip()
        try:
            seconds = float(value)
        except ValueError:
            try:
                when = parsedate_to_datetime(value)
            except (TypeError, ValueError):
                return None
            if when.tzinfo is None:
                when = when.replace(tzinfo=UTC)
            seconds = (when - datetime.now(UTC)).total_seconds()
        return min(MAX_RETRY_AFTER_S, max(0.0, seconds))

    def get_json(
        self, url: str, params: Mapping[str, int | str] | None = None, *, refresh: bool = False
    ) -> JsonPage:
        """GET one page with retries; returns the decoded JSON object (must contain MRData)."""
        for attempt in range(self._max_retries + 1):
            last = attempt == self._max_retries
            try:
                resp = self.session.get(
                    url, params=params, timeout=REQUEST_TIMEOUT_S, force_refresh=refresh
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                if last:
                    raise JolpicaHTTPError(None, url, type(exc).__name__) from exc
                delay = self._backoff(attempt)
                logger.warning("Jolpica %s on %s; retry in %.1fs", type(exc).__name__, url, delay)
                self.stats.retries += 1
                self._sleep(delay)
                continue
            if getattr(resp, "from_cache", False):
                self.stats.cache_hits += 1
            if resp.status_code == 200:
                return self._decode(resp, url)
            if resp.status_code in RETRY_STATUSES and not last:
                retry_after = self._retry_after(resp)
                if resp.status_code == 429:
                    # throttled: back off much longer than for 5xx so we stop adding load
                    self.stats.throttled += 1
                    fallback = self._backoff(attempt, THROTTLE_BACKOFF_BASE_S)
                else:
                    fallback = self._backoff(attempt)
                delay = retry_after if retry_after is not None else fallback
                logger.warning(
                    "Jolpica HTTP %s on %s; retry %d/%d in %.1fs",
                    resp.status_code,
                    url,
                    attempt + 1,
                    self._max_retries,
                    delay,
                )
                self.stats.retries += 1
                self._sleep(delay)
                continue
            raise JolpicaHTTPError(resp.status_code, url)
        raise AssertionError("unreachable")  # pragma: no cover

    @staticmethod
    def _decode(resp: requests.Response, url: str) -> JsonPage:
        try:
            body = resp.json()
        except ValueError as exc:
            raise JolpicaResponseError(f"invalid JSON from {url}") from exc
        if not isinstance(body, dict) or not isinstance(body.get("MRData"), dict):
            raise JolpicaResponseError(f"response from {url} has no MRData object")
        for key in ("limit", "offset", "total"):
            try:
                int(body["MRData"][key])
            except (KeyError, TypeError, ValueError) as exc:
                raise JolpicaResponseError(f"MRData.{key} missing or invalid in {url}") from exc
        return cast(JsonPage, body)

    def get_pages(self, path: str, *, refresh: bool = False) -> list[JsonPage]:
        """Fetch every page of an endpoint, exactly as returned by the API."""
        url = self.url_for(path)
        pages: list[JsonPage] = []
        offset = 0
        while True:
            page = self.get_json(url, {"limit": PAGE_LIMIT, "offset": offset}, refresh=refresh)
            mr = page["MRData"]
            total, limit = int(mr["total"]), int(mr["limit"])
            pages.append(page)
            if limit <= 0:
                raise JolpicaResponseError(f"server returned limit={limit} for {url}")
            if offset < total and page_item_count(page) == 0:
                raise JolpicaResponseError(
                    f"empty page at offset {offset} of {total} for {url}; refusing to continue"
                )
            offset += limit
            if offset >= total:
                return pages
            if len(pages) >= _MAX_PAGES:
                raise JolpicaResponseError(f"more than {_MAX_PAGES} pages for {url}")

    def cached_page(self, path: str, offset: int = 0) -> JsonPage | None:
        """The cached page at ``offset`` without any network access (None if not cached)."""
        url = self.url_for(path)
        resp = self.session.get(
            url,
            params={"limit": PAGE_LIMIT, "offset": offset},
            only_if_cached=True,
            timeout=REQUEST_TIMEOUT_S,
        )
        if resp.status_code != 200 or not getattr(resp, "from_cache", False):
            return None
        return self._decode(resp, url)

    def requests_needed(self, path: str, default_pages: int) -> int:
        """Network requests ``get_pages(path)`` would make now (cached pages are free).

        Exact once the first page is cached (its ``total``/``limit`` give the page count);
        otherwise ``default_pages`` is the estimate.
        """
        first = self.cached_page(path)
        if first is None:
            return default_pages
        total, limit = int(first["MRData"]["total"]), int(first["MRData"]["limit"])
        if limit <= 0:
            return default_pages
        return sum(1 for off in range(limit, total, limit) if self.cached_page(path, off) is None)

    # ------------------------------------------------------------------ typed endpoints
    def seasons(self, *, refresh: bool = False) -> pd.DataFrame:
        return parse_seasons(self.get_pages("seasons", refresh=refresh))

    def races(self, season: int, *, refresh: bool = False) -> pd.DataFrame:
        return parse_races(self.get_pages(f"{season}/races", refresh=refresh))

    def race(self, season: int, round_: int, *, refresh: bool = False) -> pd.DataFrame:
        return parse_races(self.get_pages(f"{season}/{round_}", refresh=refresh))

    def results(self, season: int, round_: int, *, refresh: bool = False) -> pd.DataFrame:
        return parse_results(self.get_pages(f"{season}/{round_}/results", refresh=refresh))

    def sprint_results(self, season: int, round_: int, *, refresh: bool = False) -> pd.DataFrame:
        return parse_sprint_results(self.get_pages(f"{season}/{round_}/sprint", refresh=refresh))

    def qualifying(self, season: int, round_: int, *, refresh: bool = False) -> pd.DataFrame:
        return parse_qualifying(self.get_pages(f"{season}/{round_}/qualifying", refresh=refresh))

    def drivers(
        self, season: int, round_: int | None = None, *, refresh: bool = False
    ) -> pd.DataFrame:
        return parse_drivers(self.get_pages(_scoped(season, round_, "drivers"), refresh=refresh))

    def constructors(
        self, season: int, round_: int | None = None, *, refresh: bool = False
    ) -> pd.DataFrame:
        pages = self.get_pages(_scoped(season, round_, "constructors"), refresh=refresh)
        return parse_constructors(pages)

    def pitstops(self, season: int, round_: int, *, refresh: bool = False) -> pd.DataFrame:
        return parse_pitstops(self.get_pages(f"{season}/{round_}/pitstops", refresh=refresh))

    def laps(self, season: int, round_: int, *, refresh: bool = False) -> pd.DataFrame:
        return parse_laps(self.get_pages(f"{season}/{round_}/laps", refresh=refresh))


def default_limiter(settings: Settings, per_hour: int) -> DualRateLimiter:
    """Limiter whose hourly window is persisted next to the HTTP cache, so separate runs
    (resumes, back-to-back commands) share one hourly budget."""
    settings.jolpica_cache_dir.mkdir(parents=True, exist_ok=True)
    return DualRateLimiter(
        per_hour=per_hour, log_path=settings.jolpica_cache_dir / REQUEST_LOG_NAME
    )


def _scoped(season: int, round_: int | None, endpoint: str) -> str:
    return f"{season}/{endpoint}" if round_ is None else f"{season}/{round_}/{endpoint}"


# ====================================================================== parsing helpers
_TIME_RE = re.compile(r"^(?:(?:(\d+):)?(\d{1,2}):)?(\d{1,2}(?:\.\d+)?)$")


def parse_duration_s(value: object) -> float | None:
    """Parse Ergast time strings to seconds: '31.234', '1:31.234', '1:27:38.241'.

    ``None``/empty -> ``None``. Anything else malformed raises ``ValueError``.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _TIME_RE.match(text)
    if match is None:
        raise ValueError(f"unparseable time {value!r}")
    hours, minutes, seconds = match.groups()
    if (minutes is not None and float(seconds) >= 60) or (hours is not None and int(minutes) >= 60):
        raise ValueError(f"out-of-range time component in {value!r}")
    return int(hours or 0) * 3600 + int(minutes or 0) * 60 + float(seconds)


def _int(value: object) -> int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return int(str(value))
    except ValueError as exc:
        raise JolpicaResponseError(f"expected integer, got {value!r}") from exc


def _float(value: object) -> float | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return float(str(value))
    except ValueError as exc:
        raise JolpicaResponseError(f"expected number, got {value!r}") from exc


def _utc(date: object, time_: object) -> pd.Timestamp | None:
    if not date:
        return None
    return pd.Timestamp(f"{date}T{time_ or '00:00:00Z'}").tz_convert("UTC")


def _frame(rows: Iterable[Mapping[str, object]], schema: Mapping[str, str]) -> pd.DataFrame:
    df = pd.DataFrame(list(rows), columns=list(schema))
    return df.astype(dict(schema)).reset_index(drop=True)


def _table(page: JsonPage, name: str) -> dict[str, Any]:
    table = page["MRData"].get(name)
    if not isinstance(table, dict):
        raise JolpicaResponseError(f"MRData.{name} missing")
    return table


def _races(pages: Iterable[JsonPage]) -> Iterable[dict[str, Any]]:
    for page in pages:
        yield from _table(page, "RaceTable").get("Races", [])


_COUNTED_KEYS = ("Results", "SprintResults", "QualifyingResults", "PitStops")


def page_item_count(page: JsonPage) -> int:
    """Number of items Jolpica counted on this page (laps count timings, like the API does)."""
    mr = page["MRData"]
    if "RaceTable" in mr:
        count = 0
        for race in mr["RaceTable"].get("Races", []):
            if "Laps" in race:
                count += sum(len(lap.get("Timings", [])) for lap in race["Laps"])
            elif any(k in race for k in _COUNTED_KEYS):
                for key in _COUNTED_KEYS:
                    count += len(race.get(key, []))
            else:
                count += 1
        return count
    for table, key in (
        ("SeasonTable", "Seasons"),
        ("DriverTable", "Drivers"),
        ("ConstructorTable", "Constructors"),
    ):
        if table in mr:
            return len(mr[table].get(key, []))
    raise JolpicaResponseError("unknown MRData table")


SEASONS_SCHEMA = {"season": "Int16", "url": "string"}
RACES_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "race_name": "string",
    "circuit_id": "string",
    "circuit_name": "string",
    "locality": "string",
    "country": "string",
    "lat": "Float64",
    "long": "Float64",
    "race_start_utc": "datetime64[ns, UTC]",
    "url": "string",
}
RESULTS_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "driver_id": "string",
    "driver_code": "string",
    "car_number": "Int16",
    "constructor_id": "string",
    "grid": "Int8",
    "position": "Int8",
    "position_text": "string",
    "points": "Float64",
    "laps": "Int16",
    "status": "string",
    "time_ms": "Int64",
    "fastest_lap": "Int16",
    "fastest_lap_rank": "Int8",
    "fastest_lap_time_s": "Float64",
}
QUALIFYING_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "driver_id": "string",
    "constructor_id": "string",
    "car_number": "Int16",
    "position": "Int8",
    "q1_s": "Float64",
    "q2_s": "Float64",
    "q3_s": "Float64",
}
DRIVERS_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "driver_id": "string",
    "code": "string",
    "permanent_number": "Int16",
    "given_name": "string",
    "family_name": "string",
    "date_of_birth": "datetime64[ns]",
    "nationality": "string",
    "url": "string",
}
CONSTRUCTORS_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "constructor_id": "string",
    "name": "string",
    "nationality": "string",
    "url": "string",
}
PITSTOPS_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "driver_id": "string",
    "stop": "Int8",
    "lap": "Int16",
    "time_of_day": "string",
    "duration_s": "Float64",
}
LAPS_SCHEMA = {
    "season": "Int16",
    "round": "Int8",
    "lap": "Int16",
    "driver_id": "string",
    "position": "Int8",
    "time_s": "Float64",
}


def parse_seasons(pages: Iterable[JsonPage]) -> pd.DataFrame:
    rows = (
        {"season": _int(s["season"]), "url": s.get("url")}
        for page in pages
        for s in _table(page, "SeasonTable").get("Seasons", [])
    )
    return _frame(rows, SEASONS_SCHEMA)


def parse_races(pages: Iterable[JsonPage]) -> pd.DataFrame:
    rows = []
    for race in _races(pages):
        circuit = race.get("Circuit", {})
        loc = circuit.get("Location", {})
        rows.append(
            {
                "season": _int(race["season"]),
                "round": _int(race["round"]),
                "race_name": race.get("raceName"),
                "circuit_id": circuit.get("circuitId"),
                "circuit_name": circuit.get("circuitName"),
                "locality": loc.get("locality"),
                "country": loc.get("country"),
                "lat": _float(loc.get("lat")),
                "long": _float(loc.get("long")),
                "race_start_utc": _utc(race.get("date"), race.get("time")),
                "url": race.get("url"),
            }
        )
    return _frame(rows, RACES_SCHEMA)


def parse_results(pages: Iterable[JsonPage]) -> pd.DataFrame:
    """Race classification (``Results``)."""
    return _parse_classification(pages, "Results")


def parse_sprint_results(pages: Iterable[JsonPage]) -> pd.DataFrame:
    """Sprint classification (``SprintResults``); same columns and dtypes as race results."""
    return _parse_classification(pages, "SprintResults")


def _parse_classification(pages: Iterable[JsonPage], key: str) -> pd.DataFrame:
    rows = []
    for race in _races(pages):
        for res in race.get(key, []):
            fastest = res.get("FastestLap", {})
            rows.append(
                {
                    "season": _int(race["season"]),
                    "round": _int(race["round"]),
                    "driver_id": res["Driver"]["driverId"],
                    "driver_code": res["Driver"].get("code"),
                    "car_number": _int(res.get("number")),
                    "constructor_id": res.get("Constructor", {}).get("constructorId"),
                    "grid": _int(res.get("grid")),
                    "position": _int(res.get("position")),
                    "position_text": res.get("positionText"),
                    "points": _float(res.get("points")),
                    "laps": _int(res.get("laps")),
                    "status": res.get("status"),
                    "time_ms": _int(res.get("Time", {}).get("millis")),
                    "fastest_lap": _int(fastest.get("lap")),
                    "fastest_lap_rank": _int(fastest.get("rank")),
                    "fastest_lap_time_s": parse_duration_s(fastest.get("Time", {}).get("time")),
                }
            )
    return _frame(rows, RESULTS_SCHEMA)


def parse_qualifying(pages: Iterable[JsonPage]) -> pd.DataFrame:
    rows = []
    for race in _races(pages):
        for q in race.get("QualifyingResults", []):
            rows.append(
                {
                    "season": _int(race["season"]),
                    "round": _int(race["round"]),
                    "driver_id": q["Driver"]["driverId"],
                    "constructor_id": q.get("Constructor", {}).get("constructorId"),
                    "car_number": _int(q.get("number")),
                    "position": _int(q.get("position")),
                    "q1_s": parse_duration_s(q.get("Q1")),
                    "q2_s": parse_duration_s(q.get("Q2")),
                    "q3_s": parse_duration_s(q.get("Q3")),
                }
            )
    return _frame(rows, QUALIFYING_SCHEMA)


def parse_drivers(pages: Iterable[JsonPage]) -> pd.DataFrame:
    rows = []
    for page in pages:
        table = _table(page, "DriverTable")
        for d in table.get("Drivers", []):
            dob = d.get("dateOfBirth")
            rows.append(
                {
                    "season": _int(table.get("season")),
                    "round": _int(table.get("round")),
                    "driver_id": d["driverId"],
                    "code": d.get("code"),
                    "permanent_number": _int(d.get("permanentNumber")),
                    "given_name": d.get("givenName"),
                    "family_name": d.get("familyName"),
                    "date_of_birth": pd.Timestamp(dob) if dob else None,
                    "nationality": d.get("nationality"),
                    "url": d.get("url"),
                }
            )
    return _frame(rows, DRIVERS_SCHEMA)


def parse_constructors(pages: Iterable[JsonPage]) -> pd.DataFrame:
    rows = []
    for page in pages:
        table = _table(page, "ConstructorTable")
        for c in table.get("Constructors", []):
            rows.append(
                {
                    "season": _int(table.get("season")),
                    "round": _int(table.get("round")),
                    "constructor_id": c["constructorId"],
                    "name": c.get("name"),
                    "nationality": c.get("nationality"),
                    "url": c.get("url"),
                }
            )
    return _frame(rows, CONSTRUCTORS_SCHEMA)


def parse_pitstops(pages: Iterable[JsonPage]) -> pd.DataFrame:
    rows = []
    for race in _races(pages):
        for p in race.get("PitStops", []):
            rows.append(
                {
                    "season": _int(race["season"]),
                    "round": _int(race["round"]),
                    "driver_id": p["driverId"],
                    "stop": _int(p.get("stop")),
                    "lap": _int(p.get("lap")),
                    "time_of_day": p.get("time"),
                    "duration_s": parse_duration_s(p.get("duration")),
                }
            )
    df = _frame(rows, PITSTOPS_SCHEMA)
    return df.sort_values(["driver_id", "stop"], kind="stable").reset_index(drop=True)


def parse_laps(pages: Iterable[JsonPage]) -> pd.DataFrame:
    """Lap timings, one row per (lap, driver). Laps split across pages are merged."""
    rows = []
    for race in _races(pages):
        for lap in race.get("Laps", []):
            for t in lap.get("Timings", []):
                rows.append(
                    {
                        "season": _int(race["season"]),
                        "round": _int(race["round"]),
                        "lap": _int(lap["number"]),
                        "driver_id": t["driverId"],
                        "position": _int(t.get("position")),
                        "time_s": parse_duration_s(t.get("time")),
                    }
                )
    df = _frame(rows, LAPS_SCHEMA)
    dupes = df.duplicated(["season", "round", "lap", "driver_id"], keep=False)
    if dupes.any():
        conflicting = df[dupes].drop_duplicates()
        if conflicting.duplicated(["season", "round", "lap", "driver_id"]).any():
            raise JolpicaResponseError("conflicting duplicate lap timings across pages")
        df = df.drop_duplicates(["season", "round", "lap", "driver_id"])
    return df.sort_values(["lap", "position"], kind="stable").reset_index(drop=True)
