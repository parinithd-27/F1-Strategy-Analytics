"""Bulk, resumable ingestion of seasons/rounds into the raw zone.

Per work item: ``mark_running`` -> fetch everything -> validate -> write files (atomic) ->
``mark_done``. Failure policy:

* ``FastF1AuthError`` / ``FastF1RateLimitError``: item marked failed, manifest flushed, the run
  aborts immediately (exit 3); continuing would only fail every later race.
* any other exception: item marked failed with the error text; the run continues (exit 1 at end).
* Ctrl+C / SIGTERM: item marked interrupted, manifest flushed, exit 130. Files are only ever
  renamed into place when complete, so no partial file is left behind.
"""

from __future__ import annotations

import gc
import signal
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from types import FrameType
from typing import Any, TextIO

import pandas as pd

from f1sa.config import Settings
from f1sa.ingest import fastf1_loader
from f1sa.ingest.fastf1_loader import FastF1AuthError, FastF1RateLimitError, RaceData
from f1sa.ingest.jolpica_client import (
    JolpicaClient,
    JsonPage,
    default_limiter,
    parse_constructors,
    parse_drivers,
    parse_laps,
    parse_pitstops,
    parse_qualifying,
    parse_results,
    parse_sprint_results,
)
from f1sa.ingest.manifest import FileRecord, Item, Manifest
from f1sa.ingest.planner import (
    JOLPICA_PATHS,
    WorkItem,
    build_plan,
    describe,
    estimate,
    jolpica_budget,
    parse_schedule,
)
from f1sa.ingest.ratelimit import TokenBucket
from f1sa.ingest.sinks import RawSink, WrittenFile, check_sink_matches_lake, make_sink
from f1sa.lake.paths import is_within, raw_table_uri

EXIT_OK, EXIT_FAILURES, EXIT_USAGE, EXIT_ABORT, EXIT_INTERRUPTED = 0, 1, 2, 3, 130
JOLPICA_LICENCE = "CC BY-NC-SA 4.0 (Jolpica-F1)"

PARSERS: dict[str, Callable[[list[JsonPage]], Any]] = {
    "schedule": lambda pages: parse_schedule(pages),
    "drivers": parse_drivers,
    "constructors": parse_constructors,
    "results": parse_results,
    "qualifying": parse_qualifying,
    "pitstops": parse_pitstops,
    "laps": parse_laps,
    "sprint_results": parse_sprint_results,
}
REQUIRED_NON_EMPTY = frozenset({"schedule", "drivers", "constructors", "results", "laps"})

ClientFactory = Callable[[Settings, int], JolpicaClient]
SinkFactory = Callable[[Settings, str, str], RawSink]


class AbortRun(RuntimeError):
    """Stop the whole run (auth / hard rate limit)."""


def default_client_factory(settings: Settings, per_hour: int) -> JolpicaClient:
    return JolpicaClient(settings, limiter=default_limiter(settings, per_hour))


@dataclass
class RunOptions:
    seasons: Sequence[int]
    sources: Sequence[str]
    sink: str
    rounds: set[int] | None = None
    only_failed: bool = False
    dry_run: bool = False
    offline: bool = False
    refresh: bool = False


@dataclass
class RunStats:
    done: int = 0
    failed: int = 0
    skipped: int = 0
    not_selected: int = 0
    failures: list[str] = field(default_factory=list)


class BulkIngest:
    def __init__(
        self,
        settings: Settings,
        options: RunOptions,
        *,
        client_factory: ClientFactory = default_client_factory,
        sink_factory: SinkFactory = make_sink,
        loader: Callable[..., RaceData] | None = None,
        auth_check: Callable[[Settings], None] | None = None,
        today: Callable[[], date] = lambda: datetime.now(UTC).date(),
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        out: TextIO = sys.stdout,
        err: TextIO = sys.stderr,
    ) -> None:
        self.settings = settings
        self.opts = options
        self._client_factory = client_factory
        self._sink_factory = sink_factory
        self._loader = loader
        self._auth_check = auth_check
        self._today = today
        self._clock = clock
        self._sleep = sleep
        self._out = out
        self._err = err
        self.stats = RunStats()
        self._fastf1_pacer = TokenBucket(
            capacity=1, refill_per_s=settings.fastf1_max_races_per_hour / 3600.0, clock=clock
        )

    def _print(self, msg: str) -> None:
        print(msg, file=self._out, flush=True)

    def _eprint(self, msg: str) -> None:
        print(msg, file=self._err, flush=True)

    # ------------------------------------------------------------------ entry point
    def run(self) -> int:
        opts, settings = self.opts, self.settings
        try:
            check_sink_matches_lake(settings, opts.sink)
            manifest = Manifest(settings.manifest_path)
        except ValueError as exc:
            self._eprint(f"error: {exc}")
            return EXIT_USAGE
        if "fastf1" in opts.sources:
            try:
                if opts.dry_run:  # validate only: a dry run creates nothing
                    if not is_within(settings.fastf1_cache_dir, Path("data")):
                        cache = settings.fastf1_cache_dir
                        raise ValueError(f"FASTF1_CACHE_DIR must be inside ./data, got {cache}")
                else:
                    fastf1_loader.configure_cache(settings, offline=opts.offline)
            except ValueError as exc:
                self._eprint(f"error: {exc}")
                return EXIT_USAGE
            if not opts.offline and not opts.dry_run:
                try:  # fail fast, before any Jolpica work
                    (self._auth_check or fastf1_loader.ensure_authenticated)(settings)
                except FastF1AuthError as exc:
                    self._eprint(f"ABORT: {exc}")
                    return EXIT_ABORT

        per_hour = jolpica_budget(
            settings.jolpica_max_per_hour, opts.sources, settings.fastf1_max_races_per_hour
        )
        started = time.perf_counter()
        with self._client_factory(settings, per_hour) as client:
            try:
                schedules_pages = {
                    s: client.get_pages(
                        JOLPICA_PATHS["schedule"].format(season=s), refresh=opts.refresh
                    )
                    for s in opts.seasons
                }
                schedules = {s: parse_schedule(p) for s, p in schedules_pages.items()}
                plan = build_plan(opts.seasons, opts.sources, schedules, self._today(), opts.rounds)
            except Exception as exc:
                self._eprint(f"error: could not build the plan: {type(exc).__name__}: {exc}")
                return EXIT_FAILURES
            schedule_network = client.stats.network
            estimates = estimate(
                plan,
                manifest,
                opts.sink,
                only_failed=opts.only_failed,
                jolpica_requests_needed=client.requests_needed,
                jolpica_per_hour=per_hour,
                fastf1_races_per_hour=settings.fastf1_max_races_per_hour,
                fastf1_cache_dir=settings.fastf1_cache_dir,
                jolpica_available=client.limiter.available_this_hour,
            )
            seasons = (
                f"{min(opts.seasons)}-{max(opts.seasons)}"
                if len(opts.seasons) > 1
                else str(opts.seasons[0])
            )
            self._print(
                f"Plan: seasons {seasons} | sources {','.join(opts.sources)} | sink {opts.sink} "
                f"({settings.data_lake_uri}) | manifest {settings.manifest_path}"
                + (" | only failed" if opts.only_failed else "")
            )
            used = per_hour - client.limiter.available_this_hour
            if used:
                self._print(
                    f"  rate limiter: {used} Jolpica request(s) already used in the last hour"
                )
            for line in describe(
                estimates,
                sources=opts.sources,
                jolpica_per_hour=per_hour,
                settings_per_hour=settings.jolpica_max_per_hour,
                fastf1_races_per_hour=settings.fastf1_max_races_per_hour,
            ):
                self._print(line)
            if opts.dry_run:
                self._print(
                    f"  dry run: schedules needed {schedule_network} network request(s) "
                    f"({len(opts.seasons) - schedule_network} from cache); nothing written, "
                    "manifest unchanged"
                )
                return EXIT_OK
            code = self._execute(plan, schedules_pages, manifest, client)
            stats = client.stats
        self._print(
            f"Summary: done {self.stats.done}, failed {self.stats.failed}, skipped "
            f"{self.stats.skipped}, already done {self.stats.not_selected} | jolpica "
            f"network={stats.network} cache_hits={stats.cache_hits} retries={stats.retries} "
            f"throttled(429)={stats.throttled} | "
            f"{time.perf_counter() - started:.1f}s"
        )
        for failure in self.stats.failures:
            self._eprint(f"  failed: {failure}")
        return code

    # ------------------------------------------------------------------ execution
    def _execute(
        self,
        plan: Sequence[WorkItem],
        schedules_pages: dict[int, list[JsonPage]],
        manifest: Manifest,
        client: JolpicaClient,
    ) -> int:
        previous = signal.getsignal(signal.SIGTERM)

        def _on_sigterm(_sig: int, _frame: FrameType | None) -> None:
            raise KeyboardInterrupt

        try:
            signal.signal(signal.SIGTERM, _on_sigterm)
        except ValueError:  # not in the main thread (tests); Ctrl+C handling still applies
            previous = None
        try:
            with manifest:
                for work in plan:
                    item = manifest.get(self.opts.sink, work.source, work.season, work.round)
                    if work.skip_reason is not None:
                        if not self.opts.only_failed:
                            manifest.mark_skipped(item, work.skip_reason)
                        self.stats.skipped += 1
                        continue
                    if not Manifest.should_run(item, only_failed=self.opts.only_failed):
                        self.stats.not_selected += 1
                        continue
                    try:
                        self._run_item(work, item, manifest, client, schedules_pages)
                    except AbortRun:
                        return EXIT_ABORT
        except KeyboardInterrupt:
            self._eprint("interrupted: manifest saved; rerun the same command to resume")
            return EXIT_INTERRUPTED
        finally:
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)
        return EXIT_FAILURES if self.stats.failed else EXIT_OK

    def _run_item(
        self,
        work: WorkItem,
        item: Item,
        manifest: Manifest,
        client: JolpicaClient,
        schedules_pages: dict[int, list[JsonPage]],
    ) -> None:
        manifest.mark_running(item)
        t0 = time.perf_counter()
        try:
            if work.source == "jolpica":
                files = self._jolpica_item(work, client, schedules_pages)
            else:
                files = self._fastf1_item(work)
        except KeyboardInterrupt:
            manifest.mark_interrupted(item)
            raise
        except (FastF1AuthError, FastF1RateLimitError) as exc:
            manifest.mark_failed(item, exc)
            self.stats.failed += 1
            self._eprint(f"ABORT {work.label}: {type(exc).__name__}: {exc}")
            raise AbortRun from exc
        except Exception as exc:
            manifest.mark_failed(item, exc)
            self.stats.failed += 1
            self.stats.failures.append(f"{work.label}: {type(exc).__name__}: {exc}")
            self._eprint(f"  FAILED {work.label}: {type(exc).__name__}: {exc}")
            return
        manifest.mark_done(item, files)
        self.stats.done += 1
        rows = sum(f.rows or 0 for f in files if not f.uri.endswith("_manifest.json"))
        self._print(
            f"  ok {work.label}: {len(files)} files, {rows} rows, {time.perf_counter() - t0:.1f}s"
        )

    # ------------------------------------------------------------------ writers
    def _write_partition(
        self, uri: str, writes: Sequence[tuple[str, Any, int | None]], meta: dict[str, Any]
    ) -> list[FileRecord]:
        """Write ``(name, payload, rows)`` files plus ``_manifest.json`` into one partition.

        DataFrames become Parquet; anything else is written as JSON.
        """
        sink = self._sink_factory(self.settings, self.opts.sink, uri)
        sink.cleanup_temp()
        written: list[WrittenFile] = []
        for name, payload, rows in writes:
            if isinstance(payload, pd.DataFrame):
                written.append(sink.write_parquet(name, payload))
            else:
                written.append(sink.write_json(name, payload, rows=rows))
        written.append(sink.write_manifest(meta))
        return [FileRecord(f"{uri}/{w.name}", w.rows, w.bytes, w.sha256) for w in written]

    def _jolpica_item(
        self, work: WorkItem, client: JolpicaClient, schedules_pages: dict[int, list[JsonPage]]
    ) -> list[FileRecord]:
        fetched: dict[str, tuple[list[JsonPage], int]] = {}
        for table in work.tables:  # fetch + validate everything before writing anything
            if table == "schedule":
                pages = schedules_pages[work.season]
            else:
                path = JOLPICA_PATHS[table].format(season=work.season, round=work.round)
                pages = client.get_pages(path, refresh=self.opts.refresh)
            rows = len(PARSERS[table](pages))
            if table in REQUIRED_NON_EMPTY and rows == 0:
                raise ValueError(f"Jolpica returned no {table} rows")
            fetched[table] = (pages, rows)
        files: list[FileRecord] = []
        fetched_at = datetime.now(UTC).isoformat()
        for table, (pages, rows) in fetched.items():
            path = JOLPICA_PATHS[table].format(season=work.season, round=work.round)
            meta = {
                "source": "jolpica",
                "table": table,
                "season": work.season,
                "round": work.round,
                "fetched_at": fetched_at,
                "source_url": client.url_for(path),
                "licence": JOLPICA_LICENCE,
                "is_synthetic": False,
            }
            uri = raw_table_uri(self.settings, "jolpica", table, work.season, work.round)
            files += self._write_partition(uri, [(f"{table}.json", pages, rows)], meta)
        return files

    def _pace_fastf1(self) -> None:
        wait = self._fastf1_pacer.time_until_available()
        if wait > 0:
            per_hour = self.settings.fastf1_max_races_per_hour
            self._print(f"  pacing FastF1: waiting {wait:.0f}s ({per_hour} races/h)")
            self._sleep(wait)
        self._fastf1_pacer.take()

    def _fastf1_item(self, work: WorkItem) -> list[FileRecord]:
        if work.round is None:
            raise ValueError("FastF1 items are per race")
        self._pace_fastf1()
        loader = self._loader or fastf1_loader.load_race
        data: RaceData | None = loader(
            work.season, work.round, self.settings, offline=self.opts.offline, check_auth=False
        )
        try:
            assert data is not None
            return self._write_fastf1(work.season, work.round, data)
        finally:
            data = None  # drop the only reference: never keep more than one race in memory
            gc.collect()

    def _write_fastf1(self, season: int, round_: int, data: RaceData) -> list[FileRecord]:
        base = {
            "source": "fastf1",
            "season": season,
            "round": round_,
            "fastf1_version": version("fastf1"),
            "offline": self.opts.offline,
            "is_synthetic": False,
        }
        settings = self.settings
        files = self._write_partition(
            raw_table_uri(settings, "fastf1", "laps", season, round_),
            [("laps.parquet", data.laps, None)],
            {**base, "table": "laps"},
        )
        files += self._write_partition(
            raw_table_uri(settings, "fastf1", "session", season, round_),
            [("session.json", data.session_info, None)],
            {**base, "table": "session"},
        )
        for table, per_driver, name in (
            ("telemetry", data.car_data, "car_data.parquet"),
            ("position", data.pos_data, "position.parquet"),
        ):
            for driver in sorted(per_driver):
                files += self._write_partition(
                    raw_table_uri(settings, "fastf1", table, season, round_, driver),
                    [(name, per_driver[driver], None)],
                    {**base, "table": table, "driver": driver},
                )
        return files
