"""Races available for replay, read from the clean zone (``GET /api/races``, replay checks).

A race is listed when the clean zone has its ``races`` and ``laps`` partitions; the producer
needs the laps and treats missing pit-stop, stint and results partitions as empty. ``laps`` is the
producer's race distance (``etl.df_jobs.race_distance``): the winner's ``results.laps``
(position 1), or the race's highest lap number when there is no winner row. ``synthetic`` is true
when the race's clean ``stints`` carry ``is_synthetic = true`` (the sample bundle's generated tyre
data, ``data/sample``); a race without a stints partition is not synthetic.

Two readers, both read-only and limited to the clean zone:

* ``FileLakeReader`` for ``file://`` lakes (pyarrow over the partition directories).
* ``HttpFsLakeReader`` for ``hdfs://`` lakes, through the HttpFS gateway
  (``Settings.hdfs_httpfs_url``):

  - Only ``LISTSTATUS`` and ``OPEN`` are sent, always as HTTP GET. Any other op is refused before
    a request is made.
  - Every path is normalised and must lie in ``{lake}/clean/<table>`` for one of ``TABLES``. So
    ``/f1/raw``, ``/f1/features`` and ``..`` tricks are refused before any request.

Both readers skip ``season=2025`` directories (the sealed test season) unless
``allow_test_season`` is set: those directories are never listed into or opened. Spark writes
the partition columns into the directory names only, so ``season`` and ``round`` come from the
path.
"""

from __future__ import annotations

import asyncio
import posixpath
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from f1sa.config import Settings
from f1sa.lake.paths import hdfs_path, local_path
from f1sa.streaming.processor import SEALED_SEASON

TABLES: tuple[str, ...] = ("races", "results", "laps", "stints")
"""The only clean tables the API reads."""
ALLOWED_OPS: frozenset[str] = frozenset({"LISTSTATUS", "OPEN"})
"""The only HttpFS operations the API sends (both read-only)."""

_SEASON_DIR = re.compile(r"season=(\d{4})")
_ROUND_DIR = re.compile(r"round=(\d{1,2})")


class CatalogError(RuntimeError):
    """The clean zone cannot be read, or a request outside the reader's limits was attempted."""


@dataclass(frozen=True)
class RaceInfo:
    season: int
    round: int
    name: str | None
    laps: int
    synthetic: bool = False


def _check_table(table: str) -> None:
    if table not in TABLES:
        raise CatalogError(f"table {table!r} is not readable by the API (allowed: {TABLES})")


def _is_data_file(name: str) -> bool:
    return name.endswith(".parquet") and not name.startswith((".", "_"))


def _decode(data: pa.NativeFile | str, columns: Sequence[str]) -> list[dict[str, Any]]:
    try:
        rows: list[dict[str, Any]] = pq.ParquetFile(data).read(columns=list(columns)).to_pylist()
    except (pa.ArrowException, OSError, KeyError, ValueError) as exc:
        raise CatalogError(f"unreadable Parquet file: {type(exc).__name__}") from exc
    return rows


class LakeReader(Protocol):
    def partitions(self, table: str, *, required: bool = True) -> list[tuple[int, int]]:
        """``(season, round)`` of every partition of ``table`` (2025 skipped unless allowed).
        A missing table raises ``CatalogError`` when ``required``, else gives ``[]``."""
        ...

    def read_rows(
        self, table: str, season: int, round_: int, columns: Sequence[str]
    ) -> list[dict[str, Any]]:
        """Rows of one partition (only ``columns``)."""
        ...


class _PartitionWalk(ABC):
    """The directory walk shared by both readers."""

    def __init__(self, allow_test_season: bool) -> None:
        self.allow_test_season = allow_test_season

    def _season_allowed(self, season: int) -> bool:
        return self.allow_test_season or season != SEALED_SEASON

    @abstractmethod
    def _list_dirs(self, path: str) -> list[str] | None:
        """Names of the sub-directories of ``path``; ``None`` if ``path`` does not exist."""

    @abstractmethod
    def _table_path(self, table: str) -> str:
        """The directory of a clean table."""

    def partitions(self, table: str, *, required: bool = True) -> list[tuple[int, int]]:
        _check_table(table)
        base = self._table_path(table)
        seasons = self._list_dirs(base)
        if seasons is None:
            if required:
                raise CatalogError(f"clean table {table!r} not found")
            return []
        out: list[tuple[int, int]] = []
        for name in sorted(seasons):
            m = _SEASON_DIR.fullmatch(name)
            if m is None:
                continue
            season = int(m.group(1))
            if not self._season_allowed(season):  # never listed into or opened
                continue
            for rname in sorted(self._list_dirs(f"{base}/{name}") or []):
                rm = _ROUND_DIR.fullmatch(rname)
                if rm is not None:
                    out.append((season, int(rm.group(1))))
        return sorted(out)


class FileLakeReader(_PartitionWalk):
    """Clean zone on the local filesystem (``file://`` lake)."""

    def __init__(self, clean_root: Path, *, allow_test_season: bool) -> None:
        super().__init__(allow_test_season)
        self.clean_root = clean_root

    def _table_path(self, table: str) -> str:
        return str(self.clean_root / table)

    def _list_dirs(self, path: str) -> list[str] | None:
        p = Path(path)
        if not p.is_dir():
            return None
        return [c.name for c in p.iterdir() if c.is_dir()]

    def read_rows(
        self, table: str, season: int, round_: int, columns: Sequence[str]
    ) -> list[dict[str, Any]]:
        _check_table(table)
        if not self._season_allowed(season):
            raise CatalogError(f"season {season} is the sealed test season")
        part = self.clean_root / table / f"season={season}" / f"round={round_}"
        if not part.is_dir():
            return []
        rows: list[dict[str, Any]] = []
        for f in sorted(part.iterdir()):
            if f.is_file() and _is_data_file(f.name):
                rows.extend(_decode(str(f), columns))
        return rows


class HttpFsLakeReader(_PartitionWalk):
    """Clean zone in HDFS, read through HttpFS with ``LISTSTATUS`` and ``OPEN`` only."""

    def __init__(
        self,
        settings: Settings,
        lake_path: str,
        *,
        allow_test_season: bool,
        session: requests.Session | None = None,
        timeout_s: float = 10.0,
    ) -> None:
        super().__init__(allow_test_season)
        if not lake_path.startswith("/"):
            raise CatalogError(f"HDFS lake path must be absolute, got {lake_path!r}")
        self.clean = posixpath.normpath(f"{lake_path}/clean")
        self._base = f"{settings.hdfs_httpfs_url}/webhdfs/v1"
        self._user = settings.hdfs_user
        self._session = session or requests.Session()
        self._timeout_s = timeout_s

    def _guard(self, op: str, path: str) -> str:
        """The normalised path, or ``CatalogError`` for a forbidden op or path."""
        if op not in ALLOWED_OPS:
            raise CatalogError(f"HttpFS op {op!r} is not allowed (only {sorted(ALLOWED_OPS)})")
        if not path.startswith("/"):
            raise CatalogError(f"HttpFS path must be absolute, got {path!r}")
        norm = posixpath.normpath(path)
        if not norm.startswith(self.clean + "/"):
            raise CatalogError(f"HttpFS path {norm!r} is outside the clean zone {self.clean!r}")
        table = norm[len(self.clean) + 1 :].split("/", 1)[0]
        _check_table(table)
        return norm

    def _get(self, op: str, path: str) -> requests.Response | None:
        """One GET; ``None`` on 404. Guards run before any I/O."""
        norm = self._guard(op, path)
        try:
            resp = self._session.get(
                f"{self._base}{norm}",
                params={"op": op, "user.name": self._user},
                timeout=self._timeout_s,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            raise CatalogError(f"HttpFS {op} failed: {type(exc).__name__}") from exc
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise CatalogError(f"HttpFS {op} {norm}: HTTP {resp.status_code}")
        return resp

    def _table_path(self, table: str) -> str:
        return f"{self.clean}/{table}"

    def _list(self, path: str) -> list[tuple[str, str]] | None:
        resp = self._get("LISTSTATUS", path)
        if resp is None:
            return None
        try:
            entries = resp.json()["FileStatuses"]["FileStatus"]
            return [(str(e["pathSuffix"]), str(e["type"])) for e in entries]
        except (ValueError, KeyError, TypeError) as exc:
            raise CatalogError("HttpFS LISTSTATUS: unexpected response") from exc

    def _list_dirs(self, path: str) -> list[str] | None:
        entries = self._list(path)
        return None if entries is None else [n for n, t in entries if t == "DIRECTORY"]

    def read_rows(
        self, table: str, season: int, round_: int, columns: Sequence[str]
    ) -> list[dict[str, Any]]:
        _check_table(table)
        if not self._season_allowed(season):
            raise CatalogError(f"season {season} is the sealed test season")
        part = f"{self.clean}/{table}/season={season}/round={round_}"
        entries = self._list(part)
        if entries is None:
            return []
        rows: list[dict[str, Any]] = []
        for name, kind in sorted(entries):
            if kind != "FILE" or not _is_data_file(name):
                continue
            resp = self._get("OPEN", f"{part}/{name}")
            if resp is None:
                raise CatalogError(f"HttpFS OPEN {part}/{name}: file vanished")
            rows.extend(_decode(pa.BufferReader(resp.content), columns))
        return rows


def reader_for(settings: Settings, *, session: requests.Session | None = None) -> LakeReader:
    """The reader for ``settings.data_lake_uri`` (``file://`` or ``hdfs://``)."""
    uri = settings.data_lake_uri
    allow = settings.allow_test_season
    if uri.startswith("file://"):
        return FileLakeReader(local_path(uri) / "clean", allow_test_season=allow)
    if uri.startswith("hdfs://"):
        try:
            path = hdfs_path(uri)
        except ValueError as exc:
            raise CatalogError("DATA_LAKE_URI has no absolute HDFS path") from exc
        return HttpFsLakeReader(settings, path, allow_test_season=allow, session=session)
    raise CatalogError(f"unsupported lake URI scheme in {uri.split('://', 1)[0]!r}")


def race_laps(results: Sequence[dict[str, Any]], laps: Sequence[dict[str, Any]]) -> int | None:
    """The producer's race distance: the winner's laps, else the highest lap number."""
    winner = [r["laps"] for r in results if r.get("position") == 1 and r.get("laps") is not None]
    if winner:
        return int(max(winner))
    lap_numbers = [r["lap"] for r in laps if r.get("lap") is not None]
    return int(max(lap_numbers)) if lap_numbers else None


def load_races(reader: LakeReader, *, allow_test_season: bool) -> list[RaceInfo]:
    """Every replayable race of the clean zone, sorted by (season, round)."""
    with_laps = set(reader.partitions("laps"))
    with_results = set(reader.partitions("results", required=False))
    with_stints = set(reader.partitions("stints", required=False))
    out: list[RaceInfo] = []
    for season, round_ in reader.partitions("races"):
        if (season, round_) not in with_laps:
            continue
        if season == SEALED_SEASON and not allow_test_season:
            continue
        names = reader.read_rows("races", season, round_, ["race_name"])
        results = (
            reader.read_rows("results", season, round_, ["position", "laps"])
            if (season, round_) in with_results
            else []
        )
        winner_known = any(r.get("position") == 1 and r.get("laps") is not None for r in results)
        laps = [] if winner_known else reader.read_rows("laps", season, round_, ["lap"])
        n = race_laps(results, laps)
        if n is None:
            continue
        name = names[0].get("race_name") if names else None
        stints = (
            reader.read_rows("stints", season, round_, ["is_synthetic"])
            if (season, round_) in with_stints
            else []
        )
        synthetic = any(r.get("is_synthetic") is True for r in stints)
        out.append(RaceInfo(season=season, round=round_, name=name, laps=n, synthetic=synthetic))
    return sorted(out, key=lambda r: (r.season, r.round))


class RaceCatalog:
    """``load_races`` cached in memory for ``ttl_s`` seconds; loads run in a worker thread."""

    def __init__(
        self,
        reader_factory: Callable[[], LakeReader],
        *,
        allow_test_season: bool,
        ttl_s: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._factory = reader_factory
        self.allow_test_season = allow_test_season
        self.ttl_s = ttl_s
        self._clock = clock
        self._cached: tuple[float, list[RaceInfo]] | None = None
        self._lock = asyncio.Lock()
        self.loads = 0

    async def races(self) -> list[RaceInfo]:
        async with self._lock:
            now = self._clock()
            if self._cached is not None and now - self._cached[0] < self.ttl_s:
                return list(self._cached[1])
            reader = self._factory()
            races = await asyncio.to_thread(
                load_races, reader, allow_test_season=self.allow_test_season
            )
            self.loads += 1
            self._cached = (now, races)
            return list(races)

    def cached(self) -> list[RaceInfo] | None:
        """The last loaded list (any age), without loading; ``None`` before the first load."""
        return None if self._cached is None else list(self._cached[1])

    async def get(self, season: int, round_: int) -> RaceInfo | None:
        for race in await self.races():
            if (race.season, race.round) == (season, round_):
                return race
        return None
