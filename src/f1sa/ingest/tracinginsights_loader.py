"""TracingInsights loader: Race-session tables and per-lap telemetry aggregates (Step 4C, #4).

TracingInsights publishes FastF1-derived JSON per season (``github.com/TracingInsights/<season>``,
no login). It replaces FastF1 as the source of compounds, tyre life, stints, race control, exact
track status and telemetry. The FastF1 loader stays as an optional path.

Reproducibility
    Every file is fetched from ``raw.githubusercontent.com/TracingInsights/<season>/<commit>/...``
    with the commit **pinned** in ``tracinginsights_pins.json`` (recorded by the audit). Each body
    is checked against its git blob SHA-1 (from the pins for the four table files, from the
    pinned git tree for telemetry) before anything is written, and its sha256 goes into the
    partition ``_manifest.json`` and ``data/manifest_tracinginsights.json``.

Per race, two manifest items (resumable, ``--only-failed``):
    ``tracinginsights``: the four table files. Each is written as received (provenance) plus typed
    Parquet with the ``"None"`` strings converted to nulls and counted per field.
    ``tracinginsights_telemetry``: the race's per-lap telemetry files are downloaded into memory
    (never to disk), aggregated with the shared ``telemetry_agg`` definitions
    (``features.telemetry_agg_ti``), and only the aggregates are written. For the seasons in
    ``raw_telemetry_seasons`` (default 2024, ``--skip-raw-telemetry`` turns it off) the samples are
    also kept as Parquet per driver.

Raw layout:
``raw/tracinginsights/{laps,rcm,session_info,drivers,telemetry_lap_agg}/season=/round=/``
and ``raw/tracinginsights/telemetry/season=/round=/driver=/``.

A disk guard checks free space on ``TI_DISK_GUARD_PATH`` (``/mnt/c``, the Windows drive backing WSL)
at start and before every telemetry race, and refuses below ``TI_MIN_FREE_GB``.

Output policy: progress lines show counts, sizes and times only, never lap times, positions or pit
times, so the sealed 2025 season is never printed.

Exit codes: 0 ok | 1 some items failed | 2 usage error or disk guard | 130 interrupted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from types import FrameType
from typing import Any, Protocol, TextIO
from urllib.parse import quote, urlparse

import pandas as pd
import requests

from f1sa.config import Settings, get_settings
from f1sa.ingest import tracinginsights_parse as P
from f1sa.ingest.manifest import FileRecord, Item, Manifest, ManifestLockedError
from f1sa.ingest.sinks import RawSink, WrittenFile, check_sink_matches_lake, make_sink
from f1sa.lake.paths import is_within, raw_table_uri

logger = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILURES, EXIT_USAGE, EXIT_INTERRUPTED = 0, 1, 2, 130
SOURCE = "tracinginsights"
TABLES_ITEM = "tracinginsights"
TELEMETRY_ITEM = "tracinginsights_telemetry"
RAW_HOST = "raw.githubusercontent.com"
GIT_HOST = "github.com"
ALLOWED_HOSTS = frozenset({RAW_HOST, GIT_HOST})
TABLE_FILES = ("session_laptimes.json", "rcm.json", "session_info.json", "drivers.json")
DEFAULT_RAW_TELEMETRY_SEASONS = (2024,)
DEFAULT_CONCURRENCY = 4
MAX_CONCURRENCY = 8
TABLE_MIN_INTERVAL_S = 1.0
LICENCE_NOTE = (
    "TracingInsights repository licence {licence}; the underlying timing data is Formula 1 / FOM "
    "data (via FastF1) and is not covered by it. Private academic use only, never redistributed."
)


# ============================================================================== pins
@dataclass(frozen=True)
class RacePin:
    season: int
    round: int
    event: str
    folder: str
    commit: str
    repo: str
    licence: str
    files: dict[str, str]
    """table file name -> git blob SHA-1"""

    @property
    def label(self) -> str:
        return f"{self.season} R{self.round}"


def load_pins(path: Path | None = None) -> dict[tuple[int, int], RacePin]:
    """The pinned commit, race folder and table-file blob ids for every (season, round)."""
    if path is None:
        text = (
            resources.files("f1sa.ingest").joinpath("tracinginsights_pins.json").read_text("utf-8")
        )
    else:
        text = path.read_text("utf-8")
    raw = json.loads(text)
    pins: dict[tuple[int, int], RacePin] = {}
    for season_s, season in raw["seasons"].items():
        commit = season["commit"]
        if len(commit) != 40 or any(c not in "0123456789abcdef" for c in commit):
            raise ValueError(f"pins: season {season_s} commit is not a full SHA-1: {commit!r}")
        for round_s, race in season["rounds"].items():
            files = dict(race["files"])
            if set(files) != set(TABLE_FILES):
                raise ValueError(f"pins: {season_s} R{round_s} must list {TABLE_FILES}")
            pins[(int(season_s), int(round_s))] = RacePin(
                season=int(season_s),
                round=int(round_s),
                event=race["event"],
                folder=race["folder"],
                commit=commit,
                repo=season["repo"],
                licence=season["licence"],
                files=files,
            )
    return pins


def raw_url(pin: RacePin, path: str) -> str:
    """URL of one file at the pinned commit."""
    return f"https://{RAW_HOST}/{pin.repo}/{pin.commit}/{quote(path)}"


def check_host(url: str) -> None:
    host = urlparse(url).hostname
    if host not in ALLOWED_HOSTS:
        raise ValueError(f"host {host!r} is not allowed (only {sorted(ALLOWED_HOSTS)})")


# ============================================================================== fetching
class FetchError(RuntimeError):
    """A file could not be downloaded or failed its integrity check."""


class Fetcher(Protocol):
    def get(self, url: str) -> bytes: ...


class HttpFetcher:
    """Thread-safe GET with an allowlist, a minimum interval between request starts, and
    backoff on 429/5xx/connection errors."""

    RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

    def __init__(
        self,
        *,
        user_agent: str = "F1-Strategy-Analytics (academic project)",
        timeout_s: float = 30.0,
        max_retries: int = 4,
        min_interval_s: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._local = threading.local()
        self._ua = user_agent
        self._timeout = timeout_s
        self._max_retries = max_retries
        self._min_interval = min_interval_s
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.Lock()
        self._next_start = 0.0
        self.requests = 0
        self.retries = 0
        self.bytes = 0

    def _session(self) -> requests.Session:
        session: requests.Session | None = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers["User-Agent"] = self._ua
            self._local.session = session
        return session

    def _pace(self) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            now = self._clock()
            wait = self._next_start - now
            self._next_start = max(now, self._next_start) + self._min_interval
        if wait > 0:
            self._sleep(wait)

    def get(self, url: str) -> bytes:
        check_host(url)
        for attempt in range(self._max_retries + 1):
            self._pace()
            with self._lock:
                self.requests += 1
            try:
                resp = self._session().get(url, timeout=self._timeout)
            except (requests.ConnectionError, requests.Timeout) as exc:
                why = type(exc).__name__
            else:
                if resp.status_code == 200:
                    with self._lock:
                        self.bytes += len(resp.content)
                    return resp.content
                if resp.status_code not in self.RETRY_STATUSES:
                    raise FetchError(f"GET {url}: HTTP {resp.status_code}")
                why = f"HTTP {resp.status_code}"
            if attempt == self._max_retries:
                raise FetchError(f"GET {url}: {why} after {attempt + 1} attempts")
            with self._lock:
                self.retries += 1
            delay = min(60.0, 2.0 * 2**attempt)
            logger.warning("GET %s: %s; retry in %.0fs", url, why, delay)
            self._sleep(delay)
        raise AssertionError("unreachable")  # pragma: no cover


def fetch_verified(fetcher: Fetcher, url: str, blob_sha1: str) -> bytes:
    """Download and check the git blob id. Raises FetchError on a mismatch."""
    data = fetcher.get(url)
    got = P.git_blob_sha1(data)
    if got != blob_sha1:
        raise FetchError(f"{url}: git blob {got} does not match the pinned {blob_sha1}")
    return data


# ============================================================================== git tree
@dataclass(frozen=True)
class TelemetryFile:
    driver_code: str
    lap: int
    path: str
    blob_sha1: str


class TreeIndex(Protocol):
    def telemetry_files(self, pin: RacePin) -> list[TelemetryFile]: ...


def telemetry_from_listing(lines: Iterable[str], folder: str) -> list[TelemetryFile]:
    """Parse ``git ls-tree -r`` lines (``mode type sha<TAB>path``) of one race's telemetry."""
    prefix = f"{folder}/Race/"
    out: list[TelemetryFile] = []
    for line in lines:
        meta, _, path = line.rstrip("\n").partition("\t")
        if not path.startswith(prefix) or not path.endswith("_tel.json"):
            continue
        parts = path[len(prefix) :].split("/")
        if len(parts) != 2:
            continue
        fields = meta.split()
        if len(fields) != 3 or fields[1] != "blob":
            raise ValueError(f"unexpected ls-tree line {line!r}")
        out.append(TelemetryFile(parts[0], P.telemetry_lap_number(parts[1]), path, fields[2]))
    return sorted(out, key=lambda f: (f.driver_code, f.lap))


class GitTreeIndex:
    """File listing of a pinned commit from a blob-free shallow fetch (a few MB of trees, no file
    contents), cached as ``<cache>/trees/<repo>-<commit>.tsv``."""

    def __init__(self, cache_dir: Path, *, git: str = "git") -> None:
        if not is_within(cache_dir, Path("data")):
            raise ValueError(f"TRACINGINSIGHTS_CACHE_DIR must be inside ./data, got {cache_dir}")
        self._dir = cache_dir / "trees"
        self._git = git
        self._listings: dict[str, list[str]] = {}

    def _listing(self, pin: RacePin) -> list[str]:
        key = f"{pin.repo.replace('/', '_')}-{pin.commit}"
        if key in self._listings:
            return self._listings[key]
        target = self._dir / f"{key}.tsv"
        if not target.exists():
            self._dir.mkdir(parents=True, exist_ok=True)
            url = f"https://{GIT_HOST}/{pin.repo}.git"
            check_host(url)
            with tempfile.TemporaryDirectory(dir=self._dir) as tmp:
                env_cmds = [
                    [self._git, "init", "-q", tmp],
                    [self._git, "-C", tmp, "remote", "add", "origin", url],
                    [
                        self._git,
                        "-C",
                        tmp,
                        "fetch",
                        "-q",
                        "--depth=1",
                        "--filter=blob:none",
                        "origin",
                        pin.commit,
                    ],
                ]
                env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
                for cmd in env_cmds:
                    subprocess.run(cmd, check=True, capture_output=True, timeout=600, env=env)
                listing = subprocess.run(
                    [
                        self._git,
                        "-C",
                        tmp,
                        "-c",
                        "core.quotePath=false",
                        "ls-tree",
                        "-r",
                        pin.commit,
                    ],
                    check=True,
                    capture_output=True,
                    timeout=600,
                    env=env,
                ).stdout
                part = target.with_suffix(".tsv.part")
                part.write_bytes(listing)
                part.replace(target)
        lines = target.read_text("utf-8").splitlines()
        self._listings[key] = lines
        return lines

    def telemetry_files(self, pin: RacePin) -> list[TelemetryFile]:
        return telemetry_from_listing(self._listing(pin), pin.folder)


# ============================================================================== disk guard
class DiskGuardError(RuntimeError):
    """Not enough free space on the guarded drive."""


def check_disk(
    path: Path, min_free_gb: float, usage: Callable[[str], Any] = shutil.disk_usage
) -> float:
    """Free GB on ``path``. Raises DiskGuardError below ``min_free_gb`` or for a missing path."""
    try:
        free_gb = usage(str(path)).free / 1e9
    except FileNotFoundError as exc:
        raise DiskGuardError(
            f"disk guard: {path} does not exist; pass --disk-guard-path (on WSL: /mnt/c)"
        ) from exc
    if free_gb < min_free_gb:
        raise DiskGuardError(
            f"disk guard: {free_gb:.1f} GB free on {path}, below the minimum {min_free_gb:g} GB"
        )
    return float(free_gb)


# ============================================================================== run
@dataclass
class RunOptions:
    seasons: Sequence[int]
    sink: str = "file"
    rounds: set[int] | None = None
    only_failed: bool = False
    tables_only: bool = False
    raw_telemetry_seasons: tuple[int, ...] = DEFAULT_RAW_TELEMETRY_SEASONS
    concurrency: int = DEFAULT_CONCURRENCY
    disk_guard_path: Path | None = None
    min_free_gb: float | None = None
    dry_run: bool = False


@dataclass
class RunStats:
    done: int = 0
    failed: int = 0
    not_selected: int = 0
    failures: list[str] = field(default_factory=list)


SinkFactory = Callable[[Settings, str, str], RawSink]
SparkFactory = Callable[[], Any]


def _default_spark() -> Any:
    from f1sa.etl.spark import build_spark

    return build_spark("f1sa-ingest-tracinginsights")


class TracingInsightsIngest:
    def __init__(
        self,
        settings: Settings,
        opts: RunOptions,
        *,
        fetcher: Fetcher | None = None,
        table_fetcher: Fetcher | None = None,
        tree_index: TreeIndex | None = None,
        sink_factory: SinkFactory = make_sink,
        spark_factory: SparkFactory = _default_spark,
        disk_usage: Callable[[str], Any] = shutil.disk_usage,
        pins: dict[tuple[int, int], RacePin] | None = None,
        out: TextIO | None = None,
        err: TextIO | None = None,
    ) -> None:
        if not 1 <= opts.concurrency <= MAX_CONCURRENCY:
            raise ValueError(f"concurrency must be in 1..{MAX_CONCURRENCY}")
        self.settings = settings
        self.opts = opts
        self._fetcher = fetcher or HttpFetcher()
        self._table_fetcher = table_fetcher or (
            fetcher or HttpFetcher(min_interval_s=TABLE_MIN_INTERVAL_S)
        )
        self._tree_index = tree_index
        self._sink_factory = sink_factory
        self._spark_factory = spark_factory
        self._spark: Any = None
        self._disk_usage = disk_usage
        self._pins = pins if pins is not None else load_pins()
        self._out = out or sys.stdout
        self._err = err or sys.stderr
        self.stats = RunStats()
        self._guard_path = opts.disk_guard_path or settings.ti_disk_guard_path
        self._min_free = opts.min_free_gb or settings.ti_min_free_gb

    def _print(self, msg: str) -> None:
        print(msg, file=self._out, flush=True)

    def _eprint(self, msg: str) -> None:
        print(msg, file=self._err, flush=True)

    def _guard(self) -> float:
        free = check_disk(self._guard_path, self._min_free, self._disk_usage)
        self._print(
            f"disk guard: {free:.1f} GB free on {self._guard_path} (minimum {self._min_free:g} GB)"
        )
        return free

    def plan(self) -> list[RacePin]:
        races = [
            pin
            for (season, round_), pin in sorted(self._pins.items())
            if season in self.opts.seasons
            and (self.opts.rounds is None or round_ in self.opts.rounds)
        ]
        return races

    # ------------------------------------------------------------------ entry point
    def run(self) -> int:
        try:
            check_sink_matches_lake(self.settings, self.opts.sink)
            manifest = Manifest(self.settings.tracinginsights_manifest_path)
        except ValueError as exc:
            self._eprint(f"error: {exc}")
            return EXIT_USAGE
        races = self.plan()
        missing = sorted(set(self.opts.seasons) - {p.season for p in self._pins.values()})
        if missing or not races:
            self._eprint(f"error: no pinned races for seasons {missing or list(self.opts.seasons)}")
            return EXIT_USAGE
        items = [(TABLES_ITEM, p) for p in races]
        if not self.opts.tables_only:
            items += [(TELEMETRY_ITEM, p) for p in races]
        self._print(
            f"Plan: {len(races)} races | items {len(items)} | sink {self.opts.sink} "
            f"({self.settings.data_lake_uri}) | manifest "
            f"{self.settings.tracinginsights_manifest_path}"
            + (" | tables only" if self.opts.tables_only else "")
            + (" | only failed" if self.opts.only_failed else "")
        )
        try:
            self._guard()
        except DiskGuardError as exc:
            self._eprint(f"error: {exc}")
            return EXIT_USAGE
        if self.opts.dry_run:
            todo = sum(
                Manifest.should_run(
                    manifest.peek(self.opts.sink, kind, p.season, p.round),
                    only_failed=self.opts.only_failed,
                )
                for kind, p in items
            )
            self._print(f"  dry run: {todo} item(s) to run; nothing fetched or written")
            return EXIT_OK
        started = time.perf_counter()
        code = self._execute(items, manifest)
        self._print(
            f"Summary: done {self.stats.done}, failed {self.stats.failed}, already done "
            f"{self.stats.not_selected} | {time.perf_counter() - started:.1f}s"
        )
        for failure in self.stats.failures:
            self._eprint(f"  failed: {failure}")
        return code

    def _execute(self, items: Sequence[tuple[str, RacePin]], manifest: Manifest) -> int:
        previous = signal.getsignal(signal.SIGTERM)

        def _on_sigterm(_sig: int, _frame: FrameType | None) -> None:
            raise KeyboardInterrupt

        try:
            signal.signal(signal.SIGTERM, _on_sigterm)
        except ValueError:
            previous = None
        try:
            with manifest:
                for kind, pin in sorted(items, key=lambda kp: (kp[1].season, kp[1].round, kp[0])):
                    item = manifest.get(self.opts.sink, kind, pin.season, pin.round)
                    if not Manifest.should_run(item, only_failed=self.opts.only_failed):
                        self.stats.not_selected += 1
                        continue
                    if kind == TELEMETRY_ITEM:
                        try:
                            self._guard()
                        except DiskGuardError as exc:
                            self._eprint(f"error: {exc}")
                            return EXIT_USAGE
                    self._run_item(kind, pin, item, manifest)
        except KeyboardInterrupt:
            self._eprint("interrupted: manifest saved; rerun the same command to resume")
            return EXIT_INTERRUPTED
        finally:
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)
            if self._spark is not None and self._spark_factory is _default_spark:
                self._spark.stop()  # only a session this run created
            self._spark = None
        return EXIT_FAILURES if self.stats.failed else EXIT_OK

    def _run_item(self, kind: str, pin: RacePin, item: Item, manifest: Manifest) -> None:
        manifest.mark_running(item)
        t0 = time.perf_counter()
        label = f"{pin.label} {'tables' if kind == TABLES_ITEM else 'telemetry'}"
        try:
            if kind == TABLES_ITEM:
                files, summary = self._tables(pin)
            else:
                files, summary = self._telemetry(pin)
        except KeyboardInterrupt:
            manifest.mark_interrupted(item)
            raise
        except Exception as exc:
            manifest.mark_failed(item, exc)
            self.stats.failed += 1
            self.stats.failures.append(f"{label}: {type(exc).__name__}: {exc}")
            self._eprint(f"  FAILED {label}: {type(exc).__name__}: {exc}")
            return
        manifest.mark_done(item, files)
        self.stats.done += 1
        self._print(f"  ok {label}: {summary} | {time.perf_counter() - t0:.1f}s")

    # ------------------------------------------------------------------ writing
    def _uri(self, table: str, pin: RacePin, driver: str | None = None) -> str:
        return raw_table_uri(self.settings, SOURCE, table, pin.season, pin.round, driver)

    def _write(
        self,
        uri: str,
        writes: Sequence[tuple[str, bytes | pd.DataFrame, int | None]],
        meta: dict[str, Any],
    ) -> list[FileRecord]:
        sink = self._sink_factory(self.settings, self.opts.sink, uri)
        sink.cleanup_temp()
        written: list[WrittenFile] = []
        for name, payload, rows in writes:
            if isinstance(payload, pd.DataFrame):
                written.append(sink.write_parquet(name, payload))
            else:
                written.append(sink.write_bytes(name, payload, rows))
        written.append(sink.write_manifest(meta))
        return [FileRecord(f"{uri}/{w.name}", w.rows, w.bytes, w.sha256) for w in written]

    def _meta(self, pin: RacePin, **extra: Any) -> dict[str, Any]:
        return {
            "source": SOURCE,
            "season": pin.season,
            "round": pin.round,
            "event": pin.event,
            "repo": pin.repo,
            "commit": pin.commit,
            "licence": LICENCE_NOTE.format(licence=pin.licence),
            **extra,
        }

    # ------------------------------------------------------------------ tables item
    def _tables(self, pin: RacePin) -> tuple[list[FileRecord], str]:
        raw: dict[str, bytes] = {}
        for name in TABLE_FILES:
            path = f"{pin.folder}/Race/{name}"
            raw[name] = fetch_verified(self._table_fetcher, raw_url(pin, path), pin.files[name])
        laps = P.parse_laps(raw["session_laptimes.json"], f"{pin.label} session_laptimes.json")
        rcm = P.parse_rcm(raw["rcm.json"], f"{pin.label} rcm.json")
        info = P.parse_session_info(raw["session_info.json"], f"{pin.label} session_info.json")
        drivers = parse_drivers(raw["drivers.json"], f"{pin.label} drivers.json")
        if (
            info.meeting_name is not None
            and info.meeting_name != pin.event
            and info.meeting_name != pin.folder
        ):
            raise P.TracingInsightsSchemaError(
                f"{pin.label}: session_info names {info.meeting_name!r}, pinned event {pin.event!r}"
            )

        def source(name: str) -> dict[str, Any]:
            data = raw[name]
            return {
                "path": f"{pin.folder}/Race/{name}",
                "git_blob_sha1": pin.files[name],
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
            }

        laps_frame = laps.frame.copy()
        files: list[FileRecord] = []
        files += self._write(
            self._uri("laps", pin),
            [
                ("session_laptimes.json", raw["session_laptimes.json"], len(laps_frame)),
                ("laps.parquet", laps_frame, None),
            ],
            self._meta(
                pin,
                inputs=[source("session_laptimes.json")],
                none_strings=laps.none_strings,
                nulls=laps.nulls,
            ),
        )
        files += self._write(
            self._uri("rcm", pin),
            [("rcm.json", raw["rcm.json"], len(rcm.frame)), ("rcm.parquet", rcm.frame, None)],
            self._meta(
                pin, inputs=[source("rcm.json")], none_strings=rcm.none_strings, nulls=rcm.nulls
            ),
        )
        track = info.track_status.frame.copy()
        files += self._write(
            self._uri("session_info", pin),
            [
                ("session_info.json", raw["session_info.json"], None),
                ("track_status.parquet", track, None),
            ],
            self._meta(
                pin,
                inputs=[source("session_info.json")],
                t0_date=info.t0_date,
                session_start_s=info.session_start_s,
                total_laps=info.total_laps,
                meeting_name=info.meeting_name,
                none_strings=info.track_status.none_strings,
            ),
        )
        files += self._write(
            self._uri("drivers", pin),
            [
                ("drivers.json", raw["drivers.json"], len(drivers)),
                ("drivers.parquet", drivers, None),
            ],
            self._meta(pin, inputs=[source("drivers.json")]),
        )
        n_none = sum(laps.none_strings.values()) + sum(rcm.none_strings.values())
        summary = (
            f"{len(laps_frame)} lap rows, {laps_frame['driver_code'].nunique()} drivers, "
            f"{len(rcm.frame)} messages, {len(track)} track-status rows, "
            f"{n_none} 'None' strings converted"
        )
        return files, summary

    # ------------------------------------------------------------------ telemetry item
    def _tree(self) -> TreeIndex:
        if self._tree_index is None:
            self._tree_index = GitTreeIndex(self.settings.tracinginsights_cache_dir)
        return self._tree_index

    def _read_laps(self, pin: RacePin) -> pd.DataFrame:
        """The race's laps, re-read from the raw zone and re-verified against the pin."""
        sink = self._sink_factory(self.settings, self.opts.sink, self._uri("laps", pin))
        data = sink.read_bytes("session_laptimes.json")
        if P.git_blob_sha1(data) != pin.files["session_laptimes.json"]:
            raise FetchError(f"{pin.label}: stored session_laptimes.json does not match its pin")
        return P.parse_laps(data, f"{pin.label} session_laptimes.json").frame

    def _telemetry(self, pin: RacePin) -> tuple[list[FileRecord], str]:
        from f1sa.features.telemetry_agg_ti import aggregate_race

        laps = self._read_laps(pin)
        tel_files = self._tree().telemetry_files(pin)
        if not tel_files:
            raise P.TracingInsightsDataError(f"{pin.label}: no telemetry files in the pinned tree")
        t0 = time.perf_counter()
        n_bytes = 0
        none_counts: dict[str, int] = {}
        frames: list[pd.DataFrame] = []
        inputs_hash = hashlib.sha256()

        def one(tf: TelemetryFile) -> tuple[TelemetryFile, P.ParseResult, int]:
            data = fetch_verified(self._fetcher, raw_url(pin, tf.path), tf.blob_sha1)
            return (
                tf,
                P.parse_telemetry(data, tf.driver_code, tf.lap, f"{pin.label} {tf.path}"),
                len(data),
            )

        pool = ThreadPoolExecutor(max_workers=self.opts.concurrency)
        try:
            for tf, parsed, size in pool.map(one, tel_files):
                n_bytes += size
                inputs_hash.update(f"{tf.path}\t{tf.blob_sha1}\n".encode())
                P.merge_counts(none_counts, parsed.none_strings)
                frames.append(parsed.frame)
        finally:  # on an error or Ctrl+C, do not wait for the queued downloads
            pool.shutdown(wait=True, cancel_futures=True)
        t_download = time.perf_counter() - t0
        telemetry = pd.concat(frames, ignore_index=True)
        del frames
        if self._spark is None:
            self._spark = self._spark_factory()
        t1 = time.perf_counter()
        agg = aggregate_race(self._spark, pin.season, pin.round, telemetry, laps)
        t_agg = time.perf_counter() - t1
        coverage = agg.frame["telemetry_coverage"].value_counts().to_dict()
        meta = self._meta(
            pin,
            telemetry_files=len(tel_files),
            telemetry_bytes=n_bytes,
            telemetry_inputs_sha256=inputs_hash.hexdigest(),
            n_samples_in=agg.n_samples_in,
            n_duplicate_samples=agg.n_duplicate_samples,
            n_samples_outside_window=agg.n_samples_outside_window,
            coverage_counts={k: int(v) for k, v in sorted(coverage.items())},
            none_strings=none_counts,
            definitions="features.telemetry_agg.assign_to_laps + aggregate_samples",
        )
        files = self._write(
            self._uri("telemetry_lap_agg", pin),
            [("telemetry_lap_agg.parquet", agg.frame, None)],
            meta,
        )
        kept = ""
        if pin.season in self.opts.raw_telemetry_seasons:
            files += self._write_raw_telemetry(pin, telemetry, laps, tel_files)
            kept = ", raw samples kept"
        summary = (
            f"{len(tel_files)} files, {n_bytes / 1e6:.0f} MB, {agg.n_samples_in} samples, "
            f"{len(agg.frame)} laps {dict(sorted(coverage.items()))}{kept} | download "
            f"{t_download:.0f}s, aggregate {t_agg:.0f}s"
        )
        return files, summary

    def _write_raw_telemetry(
        self,
        pin: RacePin,
        telemetry: pd.DataFrame,
        laps: pd.DataFrame,
        tel_files: Sequence[TelemetryFile],
    ) -> list[FileRecord]:
        starts = laps[["driver_code", "lap", "lap_start_session_s"]]
        merged = telemetry.merge(starts, on=["driver_code", "lap"], how="left")
        merged["session_time_s"] = (
            (merged["lap_start_session_s"] + merged["time_in_lap_s"]).astype("Float64").round(3)
        )
        merged = merged.drop(columns=["lap_start_session_s"])
        files: list[FileRecord] = []
        for driver, frame in merged.groupby("driver_code", sort=True):
            code = str(driver)
            inputs = [
                {"path": tf.path, "git_blob_sha1": tf.blob_sha1}
                for tf in tel_files
                if tf.driver_code == code
            ]
            files += self._write(
                self._uri("telemetry", pin, code),
                [
                    (
                        "telemetry.parquet",
                        frame.sort_values(["lap", "time_in_lap_s"]).reset_index(drop=True),
                        None,
                    )
                ],
                self._meta(pin, driver_code=code, inputs=inputs),
            )
        return files


def parse_drivers(data: bytes, what: str) -> pd.DataFrame:
    """``{"drivers": [{"driver": "VER", "team": "..."}]}`` -> driver_code, team."""
    obj = P.load_json(data, what)
    rows = obj.get("drivers") if isinstance(obj, dict) else None
    if not isinstance(rows, list) or not all(
        isinstance(r, dict) and isinstance(r.get("driver"), str) for r in rows
    ):
        raise P.TracingInsightsSchemaError(
            f"{what}: expected {{'drivers': [{{'driver', 'team'}}]}}"
        )
    if not rows:
        raise P.TracingInsightsDataError(f"{what}: no drivers")
    return pd.DataFrame(
        {
            "driver_code": pd.array([r["driver"] for r in rows], dtype="string"),
            "team": pd.array([r.get("team") for r in rows], dtype="string"),
        }
    )


# ============================================================================== CLI
def _parse_rounds(value: str) -> set[int]:
    try:
        rounds = {int(x) for x in value.split(",") if x.strip()}
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--rounds must be a comma list of integers ({exc})"
        ) from exc
    if not rounds or min(rounds) < 1:
        raise argparse.ArgumentTypeError("--rounds must list rounds >= 1")
    return rounds


def build_parser() -> argparse.ArgumentParser:
    from f1sa.ingest.cli import parse_seasons

    p = argparse.ArgumentParser(prog="f1sa-ingest-ti", description=__doc__.split("\n\n")[0])
    p.add_argument("--seasons", type=parse_seasons, required=True)
    p.add_argument("--rounds", type=_parse_rounds, default=None)
    p.add_argument("--sink", choices=("file", "hdfs"), default="file")
    p.add_argument("--only-failed", action="store_true")
    p.add_argument("--tables-only", action="store_true", help="skip the telemetry items")
    p.add_argument(
        "--skip-raw-telemetry",
        action="store_true",
        help="do not keep raw telemetry samples (default: kept for 2024)",
    )
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--disk-guard-path", type=Path, default=None)
    p.add_argument("--min-free-gb", type=float, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    if not 1 <= args.concurrency <= MAX_CONCURRENCY:
        parser.error(f"--concurrency must be in 1..{MAX_CONCURRENCY}")
    if args.min_free_gb is not None and args.min_free_gb <= 0:
        parser.error("--min-free-gb must be > 0")
    opts = RunOptions(
        seasons=args.seasons,
        sink=args.sink,
        rounds=args.rounds,
        only_failed=args.only_failed,
        tables_only=args.tables_only,
        raw_telemetry_seasons=() if args.skip_raw_telemetry else DEFAULT_RAW_TELEMETRY_SEASONS,
        concurrency=args.concurrency,
        disk_guard_path=args.disk_guard_path,
        min_free_gb=args.min_free_gb,
        dry_run=args.dry_run,
    )
    try:
        return TracingInsightsIngest(get_settings(), opts).run()
    except ManifestLockedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
