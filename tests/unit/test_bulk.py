"""End-to-end bulk-ingestion tests (fixture Jolpica, fake FastF1, tmp data/; no network)."""

from __future__ import annotations

import argparse
import io
import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests
from _fakes import FakeClock, FakeHttpFS, FixtureAdapter

from f1sa.config import Settings
from f1sa.ingest import cli
from f1sa.ingest.bulk import (
    EXIT_ABORT,
    EXIT_FAILURES,
    EXIT_INTERRUPTED,
    EXIT_OK,
    EXIT_USAGE,
    BulkIngest,
    RunOptions,
)
from f1sa.ingest.fastf1_loader import FastF1AuthError, FastF1RateLimitError, RaceData
from f1sa.ingest.jolpica_client import JolpicaClient
from f1sa.ingest.manifest import Manifest
from f1sa.ingest.ratelimit import DualRateLimiter
from f1sa.ingest.sinks import FileSink, HdfsSink, make_sink

LAKE = Path("data/lake/raw")
TODAY = date(2026, 11, 10)


class Harness:
    def __init__(self, fail_paths: set[str] | None = None) -> None:
        self.clock = FakeClock()
        self.adapter = FixtureAdapter(fail_paths)
        self.loader_calls: list[tuple[int, int]] = []
        self.out = io.StringIO()
        self.err = io.StringIO()

    def client(self, settings: Settings, per_hour: int) -> JolpicaClient:
        self.per_hour = per_hour
        return JolpicaClient(
            settings,
            limiter=DualRateLimiter(per_hour=per_hour, clock=self.clock, sleep=self.clock.sleep),
            transport=self.adapter,
            cache_backend="memory",
            sleep=self.clock.sleep,
        )

    def loader(self, season: int, round_: int, settings: Settings, **kw: Any) -> RaceData:
        self.loader_calls.append((season, round_))
        tel = pd.DataFrame({"Driver": ["VER"], "Speed": [300.0]})
        return RaceData(
            season,
            round_,
            laps=pd.DataFrame({"Driver": ["VER", "PER"], "LapNumber": [1.0, 1.0]}),
            car_data={"VER": tel, "PER": tel.assign(Driver="PER")},
            pos_data={"VER": pd.DataFrame({"X": [1.0]}), "PER": pd.DataFrame({"X": [2.0]})},
            session_info={"event_name": "x", "is_synthetic": False},
        )

    def run(self, options: RunOptions, settings: Settings | None = None, **kw: Any) -> int:
        bulk = BulkIngest(
            settings or Settings(),
            options,
            client_factory=self.client,
            loader=kw.pop("loader", self.loader),
            auth_check=kw.pop("auth_check", lambda s: None),
            today=lambda: TODAY,
            clock=self.clock,
            sleep=self.clock.sleep,
            out=self.out,
            err=self.err,
            **kw,
        )
        return bulk.run()


def opts(**kw: Any) -> RunOptions:
    base: dict[str, Any] = {
        "seasons": [2023],
        "sources": ["jolpica"],
        "sink": "file",
        "rounds": {4, 5},
    }
    base.update(kw)
    return RunOptions(**base)


def manifest() -> Manifest:
    return Manifest(Path("data/manifest.json"))


def test_jolpica_run_writes_table_first_layout_and_resumes() -> None:
    h = Harness()
    assert h.run(opts()) == EXIT_OK
    for rel in [
        "jolpica/schedule/season=2023/schedule.json",
        "jolpica/drivers/season=2023/drivers.json",
        "jolpica/constructors/season=2023/constructors.json",
        "jolpica/results/season=2023/round=4/results.json",
        "jolpica/sprint_results/season=2023/round=4/sprint_results.json",
        "jolpica/laps/season=2023/round=5/laps.json",
        "jolpica/pitstops/season=2023/round=5/_manifest.json",
    ]:
        assert (LAKE / rel).is_file(), rel
    assert not (
        LAKE / "jolpica/sprint_results/season=2023/round=5"
    ).exists()  # not a sprint weekend
    m = manifest()
    assert m.counts() == {"done": 3}
    laps = m.peek("file", "jolpica", 2023, 5)
    assert laps is not None and any(f.uri.endswith("laps.json") and f.rows == 9 for f in laps.files)
    network_first = len(h.adapter.requests)

    h2 = Harness()
    assert h2.run(opts()) == EXIT_OK
    assert "already done 3" in h2.out.getvalue()
    schedule_only = [r for r in h2.adapter.requests if "/races" not in (r.url or "")]
    assert schedule_only == []  # rerun skips every item (only the schedule is read)
    assert network_first > 10


def test_per_race_error_is_recorded_and_run_continues() -> None:
    h = Harness(fail_paths={"2023/4/qualifying"})
    code = h.run(opts())
    assert code == EXIT_FAILURES
    m = manifest()
    failed = m.peek("file", "jolpica", 2023, 4)
    assert failed is not None and failed.status == "failed" and "HTTP 500" in (failed.error or "")
    done = m.peek("file", "jolpica", 2023, 5)
    assert done is not None and done.status == "done"
    assert not (LAKE / "jolpica/results/season=2023/round=4").exists()  # nothing half-written

    h2 = Harness()  # the outage is over
    assert h2.run(opts(only_failed=True)) == EXIT_OK
    assert manifest().counts() == {"done": 3}
    fetched = {r.url.split("/f1/")[1].split(".json")[0] for r in h2.adapter.requests if r.url}
    assert fetched == {
        "2023/races",
        "2023/4/results",
        "2023/4/qualifying",
        "2023/4/pitstops",
        "2023/4/laps",
        "2023/4/sprint",
    }


def test_missing_round_404_is_a_recorded_failure() -> None:
    h = Harness()
    assert h.run(opts(rounds={6})) == EXIT_FAILURES
    item = manifest().peek("file", "jolpica", 2023, 6)
    assert item is not None and item.status == "failed" and "404" in (item.error or "")


def test_dry_run_writes_nothing_and_prints_estimate() -> None:
    h = Harness()
    assert h.run(opts(dry_run=True, rounds=None, sources=["jolpica", "fastf1"])) == EXIT_OK
    out = h.out.getvalue()
    assert "Plan: seasons 2023" in out and "expected network requests" in out
    assert "budget split" in out and "390/h" in out and "dry run" in out
    assert not Path("data/manifest.json").exists() and not LAKE.exists()
    assert h.loader_calls == []


def test_fastf1_preflight_auth_failure_aborts_before_any_work() -> None:
    h = Harness()

    def no_auth(_s: Settings) -> None:
        raise FastF1AuthError("needs F1 TV login")

    code = h.run(opts(sources=["jolpica", "fastf1"]), auth_check=no_auth)
    assert code == EXIT_ABORT
    assert "ABORT" in h.err.getvalue()
    assert h.adapter.requests == [] and not Path("data/manifest.json").exists()


@pytest.mark.parametrize(
    "error", [FastF1AuthError("token rejected"), FastF1RateLimitError("500/h")]
)
def test_fastf1_auth_error_mid_run_aborts_with_intact_manifest(error: Exception) -> None:
    h = Harness()
    calls = 0

    def loader(season: int, round_: int, settings: Settings, **kw: Any) -> RaceData:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise error
        return h.loader(season, round_, settings)

    code = h.run(opts(sources=["fastf1"], rounds=None), loader=loader)
    assert code == EXIT_ABORT
    m = manifest()
    statuses = {r: m.peek("file", "fastf1", 2023, r) for r in (4, 5, 6)}
    assert statuses[4] is not None and statuses[4].status == "done"
    assert statuses[5] is not None and statuses[5].status == "failed"
    assert statuses[6] is None  # never started
    assert calls == 2


def test_fastf1_writes_per_driver_partitions_race_by_race() -> None:
    h = Harness()
    assert h.run(opts(sources=["fastf1"], rounds=None)) == EXIT_OK
    assert h.loader_calls == [(2023, 4), (2023, 5), (2023, 6)]
    for rel in [
        "fastf1/laps/season=2023/round=5/laps.parquet",
        "fastf1/session/season=2023/round=5/session.json",
        "fastf1/telemetry/season=2023/round=5/driver=VER/car_data.parquet",
        "fastf1/telemetry/season=2023/round=5/driver=PER/car_data.parquet",
        "fastf1/position/season=2023/round=5/driver=PER/position.parquet",
    ]:
        assert (LAKE / rel).is_file(), rel
    df = pd.read_parquet(LAKE / "fastf1/telemetry/season=2023/round=5/driver=PER/car_data.parquet")
    assert df["Driver"].tolist() == ["PER"]
    meta = json.loads(
        (LAKE / "fastf1/telemetry/season=2023/round=5/driver=VER/_manifest.json").read_text()
    )
    assert meta["driver"] == "VER" and meta["is_synthetic"] is False


def test_fastf1_races_are_paced() -> None:
    h = Harness()
    assert h.run(opts(sources=["fastf1"], rounds=None)) == EXIT_OK
    pacing = [s for s in h.clock.sleeps if s > 100]
    assert pacing == [pytest.approx(180.0), pytest.approx(180.0)]  # 20 races/h


def test_keyboard_interrupt_flushes_manifest_and_leaves_no_partial_files() -> None:
    h = Harness()
    writes = 0

    class InterruptingSink(FileSink):
        def write_bytes(self, name: str, data: bytes, rows: int | None = None) -> Any:
            nonlocal writes
            writes += 1
            if writes == 5:  # in the middle of an item
                raise KeyboardInterrupt
            return super().write_bytes(name, data, rows)

    code = h.run(opts(), sink_factory=lambda s, sink, uri: InterruptingSink(uri))
    assert code == EXIT_INTERRUPTED
    m = manifest()
    statuses = sorted(i.status for i in m)
    assert "interrupted" in statuses and "running" not in statuses
    assert not list(Path("data").rglob(".*.tmp"))  # no temp/partial files anywhere
    assert "rerun the same command" in h.err.getvalue()

    h2 = Harness()  # resume finishes the job
    assert h2.run(opts()) == EXIT_OK
    assert manifest().counts() == {"done": 3}


def test_skipped_future_race_not_marked_done() -> None:
    h = Harness()
    bulk_today = date(2023, 5, 1)  # round 5 (May 7) not run yet
    bulk = BulkIngest(
        Settings(), opts(), client_factory=h.client, today=lambda: bulk_today, out=h.out, err=h.err
    )
    assert bulk.run() == EXIT_OK
    item = manifest().peek("file", "jolpica", 2023, 5)
    assert item is not None and item.status == "skipped" and "future" in (item.note or "")


def test_hdfs_sink_end_to_end_with_fake_httpfs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_LAKE_URI", "hdfs://namenode:8020/f1")
    settings = Settings()
    fake = FakeHttpFS()
    session = requests.Session()
    session.mount("http://", fake)

    def sink_factory(s: Settings, sink: str, uri: str) -> HdfsSink:
        assert sink == "hdfs"
        return HdfsSink(uri, s, session=session, sleep=lambda _x: None)

    h = Harness()
    assert h.run(opts(sink="hdfs"), settings=settings, sink_factory=sink_factory) == EXIT_OK
    assert "/f1/raw/jolpica/laps/season=2023/round=5/laps.json" in fake.files
    assert not any("/_tmp." in p for p in fake.files)
    item = manifest().peek("hdfs", "jolpica", 2023, 5)
    assert item is not None and item.status == "done"
    assert all(f.uri.startswith("hdfs://namenode:8020/f1/raw/") for f in item.files)
    assert not LAKE.exists()  # nothing went to the local lake


def test_sink_must_match_lake_scheme() -> None:
    h = Harness()
    assert h.run(opts(sink="hdfs")) == EXIT_USAGE
    assert "DATA_LAKE_URI=hdfs://namenode:8020/f1" in h.err.getvalue()


def test_fastf1_cache_outside_data_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("FASTF1_CACHE_DIR", str(tmp_path.parent / "elsewhere"))
    h = Harness()
    assert h.run(opts(sources=["fastf1"])) == EXIT_USAGE
    assert "inside ./data" in h.err.getvalue()


def test_make_sink_default_file() -> None:
    assert isinstance(make_sink(Settings(), "file", "file://./data/lake/x"), FileSink)


# ---------------------------------------------------------------- CLI wiring
def test_cli_parse_seasons() -> None:
    assert cli.parse_seasons("2021-2023", current_year=2026) == [2021, 2022, 2023]
    assert cli.parse_seasons("2023,2021,2023", current_year=2026) == [2021, 2023]
    for bad in ("2025-2021", "2017-2019", "2030", "abc", "2021,,2022", ""):
        with pytest.raises(argparse.ArgumentTypeError):
            cli.parse_seasons(bad, current_year=2026)


@pytest.mark.parametrize(
    "argv",
    [
        ["season-range", "--seasons", "2021", "--sources", "ergast"],
        ["season-range", "--seasons", "2021", "--sink", "s3"],
        ["season-range"],
        ["race", "--season", "2021", "--round", "0"],
        ["race", "--season", "1999", "--round", "1"],
        [],
    ],
)
def test_cli_rejects_bad_args(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2


def test_cli_passes_options(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, RunOptions] = {}

    class FakeBulk:
        def __init__(self, settings: Settings, options: RunOptions) -> None:
            seen["opts"] = options

        def run(self) -> int:
            return 0

    monkeypatch.setattr(cli, "BulkIngest", FakeBulk)
    argv = [
        "season-range",
        "--seasons",
        "2021-2022",
        "--sources",
        "jolpica",
        "--sink",
        "hdfs",
        "--only-failed",
        "--dry-run",
    ]
    assert cli.main(argv) == 0
    o = seen["opts"]
    assert list(o.seasons) == [2021, 2022] and o.sink == "hdfs" and o.only_failed and o.dry_run
    assert o.rounds is None
    assert cli.main(["race", "--season", "2023", "--round", "5", "--sources", "jolpica"]) == 0
    assert seen["opts"].rounds == {5} and list(seen["opts"].seasons) == [2023]
