"""TracingInsights loader on the invented fixture race: layout, integrity, resume, kill-safety,
disk guard, raw-telemetry flag, output policy and the HTTP fetcher (no network)."""

from __future__ import annotations

import hashlib
import io
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests
from _ti_fixtures import COMMIT, FakeFetcher, FakeTree, disk, make_pin
from pyspark.sql import SparkSession

from f1sa.config import Settings
from f1sa.ingest import tracinginsights_loader as L
from f1sa.ingest.manifest import Manifest

UNIT_DIR = Path(__file__).resolve().parent
REPO = UNIT_DIR.parents[1]


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    yield tmp_path


def _settings() -> Settings:
    return Settings(
        data_lake_uri="file://./data/lake",
        tracinginsights_manifest_path=Path("data/manifest_tracinginsights.json"),
        tracinginsights_cache_dir=Path("data/cache/tracinginsights"),
        ti_disk_guard_path=Path("/mnt/c"),
    )


def _run(
    spark: SparkSession,
    pins: list[L.RacePin],
    *,
    fetcher: FakeFetcher | None = None,
    free_gb: float = 100.0,
    **opts: Any,
) -> tuple[int, str, str, FakeFetcher]:
    fetcher = fetcher or FakeFetcher(pins)
    out, err = io.StringIO(), io.StringIO()
    seasons = sorted({p.season for p in pins})
    ingest = L.TracingInsightsIngest(
        _settings(),
        L.RunOptions(seasons=seasons, **opts),
        fetcher=fetcher,
        table_fetcher=fetcher,
        tree_index=FakeTree(),
        spark_factory=lambda: spark,
        disk_usage=disk(free_gb),
        pins={(p.season, p.round): p for p in pins},
        out=out,
        err=err,
    )
    code = ingest.run()
    return code, out.getvalue(), err.getvalue(), fetcher


def _data_files(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file() and p.name != "_manifest.json"
    }


LAKE = Path("data/lake/raw/tracinginsights")


def test_full_run_layout_and_manifest(workdir: Path, spark: SparkSession) -> None:
    pin = make_pin()
    code, out, err, fetcher = _run(spark, [pin])
    assert code == 0, err
    part = "season=2024/round=1"
    for table, names in {
        "laps": ("session_laptimes.json", "laps.parquet"),
        "rcm": ("rcm.json", "rcm.parquet"),
        "session_info": ("session_info.json", "track_status.parquet"),
        "drivers": ("drivers.json", "drivers.parquet"),
        "telemetry_lap_agg": ("telemetry_lap_agg.parquet",),
    }.items():
        for name in (*names, "_manifest.json"):
            assert (LAKE / table / part / name).is_file(), f"{table}/{name}"
    # raw JSON is stored exactly as received (provenance)
    assert (LAKE / "laps" / part / "session_laptimes.json").read_bytes() == fetcher.urls[
        L.raw_url(pin, "Testland Grand Prix/Race/session_laptimes.json")
    ]
    meta = json.loads((LAKE / "laps" / part / "_manifest.json").read_text())
    assert meta["commit"] == COMMIT and meta["none_strings"]["compound"] == 1
    assert meta["inputs"][0]["git_blob_sha1"] == pin.files["session_laptimes.json"]
    assert "not covered" in meta["licence"]
    agg = pd.read_parquet(LAKE / "telemetry_lap_agg" / part / "telemetry_lap_agg.parquet")
    assert len(agg) == 15 and set(agg["driver_code"]) == {"AAA", "BBB", "CCC"}
    tmeta = json.loads((LAKE / "telemetry_lap_agg" / part / "_manifest.json").read_text())
    assert tmeta["telemetry_files"] == 10 and tmeta["n_duplicate_samples"] == 8
    manifest = Manifest(Path("data/manifest_tracinginsights.json"))
    assert manifest.counts() == {"done": 2}
    assert all(f.sha256 for item in manifest for f in item.files)
    assert "ok 2024 R1 tables: 15 lap rows, 3 drivers, 5 messages" in out
    assert all(
        c.startswith(f"https://raw.githubusercontent.com/TracingInsights/2024/{COMMIT}/")
        for c in fetcher.calls
    )


def test_raw_telemetry_kept_per_driver_for_listed_seasons(
    workdir: Path, spark: SparkSession
) -> None:
    code, _, err, _ = _run(spark, [make_pin()], raw_telemetry_seasons=(2024,))
    assert code == 0, err
    tel = LAKE / "telemetry" / "season=2024" / "round=1"
    assert sorted(p.name for p in tel.iterdir()) == ["driver=AAA", "driver=BBB"]
    frame = pd.read_parquet(tel / "driver=AAA" / "telemetry.parquet")
    assert len(frame) == 7 * 4 + 8 and frame["session_time_s"].iloc[0] == 1000.0
    assert frame["lap"].is_monotonic_increasing


def test_skip_raw_telemetry(workdir: Path, spark: SparkSession) -> None:
    code, _, err, _ = _run(spark, [make_pin()], raw_telemetry_seasons=())
    assert code == 0, err
    assert not (LAKE / "telemetry").exists()
    assert (LAKE / "telemetry_lap_agg").exists()


def test_tables_only(workdir: Path, spark: SparkSession) -> None:
    code, _, _, fetcher = _run(spark, [make_pin()], tables_only=True)
    assert code == 0 and len(fetcher.calls) == 4
    assert not (LAKE / "telemetry_lap_agg").exists()


def test_rerun_fetches_nothing(workdir: Path, spark: SparkSession) -> None:
    pin = make_pin()
    assert _run(spark, [pin])[0] == 0
    before = _data_files(Path("data/lake"))
    code, out, _, fetcher = _run(spark, [pin])
    assert code == 0 and fetcher.calls == []
    assert "already done 2" in out
    assert _data_files(Path("data/lake")) == before


def test_blob_mismatch_fails_the_item_and_writes_nothing(
    workdir: Path, spark: SparkSession
) -> None:
    pin = make_pin()
    fetcher = FakeFetcher([pin], corrupt={"rcm.json"})
    code, _, err, _ = _run(spark, [pin], fetcher=fetcher, tables_only=True)
    assert code == 1 and "does not match the pinned" in err
    assert not LAKE.exists()
    item = Manifest(Path("data/manifest_tracinginsights.json")).items["file:tracinginsights:2024:1"]
    assert item.status == "failed"
    # --only-failed retries just that item, now with good bytes
    code, out, _, _ = _run(spark, [pin], tables_only=True, only_failed=True)
    assert code == 0 and "ok 2024 R1 tables" in out


def test_telemetry_blob_mismatch(workdir: Path, spark: SparkSession) -> None:
    pin = make_pin()
    fetcher = FakeFetcher([pin], corrupt={"AAA/3_tel.json"})
    code, _, err, _ = _run(spark, [pin], fetcher=fetcher)
    assert code == 1 and "AAA/3_tel.json" in err
    assert not (LAKE / "telemetry_lap_agg").exists()


def test_disk_guard_refuses_and_prints_the_number(workdir: Path, spark: SparkSession) -> None:
    code, out, err, fetcher = _run(spark, [make_pin()], free_gb=19.9)
    assert code == 2 and fetcher.calls == []
    assert "19.9 GB free on /mnt/c, below the minimum 20 GB" in err
    code, out, _, _ = _run(spark, [make_pin()], free_gb=20.0, tables_only=True)
    assert code == 0 and "disk guard: 20.0 GB free on /mnt/c" in out


def test_disk_guard_missing_path() -> None:
    def missing(_p: str) -> Any:
        raise FileNotFoundError(_p)

    with pytest.raises(L.DiskGuardError, match="does not exist; pass --disk-guard-path"):
        L.check_disk(Path("/nope"), 20.0, missing)


def test_disk_guard_rechecked_before_each_telemetry_race(
    workdir: Path, spark: SparkSession
) -> None:
    calls: list[str] = []
    levels = iter([100.0, 100.0, 5.0])

    def usage(path: str) -> Any:
        calls.append(path)
        from _ti_fixtures import Usage

        return Usage(next(levels))

    pins = [make_pin(2024, 1), make_pin(2024, 2)]
    fetcher = FakeFetcher(pins)
    ingest = L.TracingInsightsIngest(
        _settings(),
        L.RunOptions(seasons=[2024]),
        fetcher=fetcher,
        table_fetcher=fetcher,
        tree_index=FakeTree(),
        spark_factory=lambda: spark,
        disk_usage=usage,
        pins={(p.season, p.round): p for p in pins},
        out=io.StringIO(),
        err=io.StringIO(),
    )
    assert ingest.run() == 2  # start ok, R1 telemetry ok, R2 telemetry refused
    assert len(calls) == 3


def test_output_never_shows_2025_values(workdir: Path, spark: SparkSession) -> None:
    pin = make_pin(2025, 1, licence="Apache-2.0")
    code, out, err, _ = _run(spark, [pin])
    assert code == 0, err
    text = out + err
    for value in ("90.0", "1265", "1275", "1000.0", "1090", "MEDIUM", "SOFT", "HARD"):
        assert value not in text, value
    assert "lap rows" in text and "messages" in text


def test_interrupt_marks_item_and_exits_130(workdir: Path, spark: SparkSession) -> None:
    pin = make_pin()

    def stop(url: str, n: int) -> None:
        if url.endswith("_tel.json"):
            raise KeyboardInterrupt

    code, _, err, _ = _run(spark, [pin], fetcher=FakeFetcher([pin], on_get=stop))
    assert code == 130 and "rerun the same command to resume" in err
    items = Manifest(Path("data/manifest_tracinginsights.json")).items
    assert items["file:tracinginsights:2024:1"].status == "done"
    assert items["file:tracinginsights_telemetry:2024:1"].status == "interrupted"


KILL_SCRIPT = textwrap.dedent(
    """
    import io, sys, time
    from pathlib import Path
    sys.path.insert(0, {unit!r})
    from _ti_fixtures import FakeFetcher, FakeTree, disk, make_pin
    from f1sa.config import Settings
    from f1sa.ingest import tracinginsights_loader as L

    def block(url, n):
        if url.endswith("BBB/2_tel.json"):
            Path("killme").write_text("x")
            time.sleep(120)

    pin = make_pin()
    fetcher = FakeFetcher([pin], on_get=block)
    s = Settings(data_lake_uri="file://./data/lake",
                 tracinginsights_manifest_path=Path("data/manifest_tracinginsights.json"))
    L.TracingInsightsIngest(s, L.RunOptions(seasons=[2024], concurrency=1), fetcher=fetcher,
        table_fetcher=fetcher, tree_index=FakeTree(), spark_factory=lambda: None,
        disk_usage=disk(100), pins={{(2024, 1): pin}}).run()
    """
)


def test_sigkill_mid_race_then_resume_is_identical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, spark: SparkSession
) -> None:
    fresh = tmp_path / "fresh"
    killed = tmp_path / "killed"
    for d in (fresh, killed):
        (d / "data").mkdir(parents=True)
    monkeypatch.chdir(fresh)
    assert _run(spark, [make_pin()])[0] == 0
    expected = _data_files(fresh / "data" / "lake")

    script = tmp_path / "kill_me.py"
    script.write_text(KILL_SCRIPT.format(unit=str(UNIT_DIR)))
    env = {**os.environ, "PYTHONPATH": str(REPO / "src")}
    proc = subprocess.Popen([sys.executable, str(script)], cwd=killed, env=env)
    try:
        deadline = time.monotonic() + 60
        while not (killed / "killme").exists():
            assert proc.poll() is None, "child exited before the kill point"
            assert time.monotonic() < deadline, "child never reached the kill point"
            time.sleep(0.05)
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
    monkeypatch.chdir(killed)
    items = Manifest(Path("data/manifest_tracinginsights.json")).items
    assert items["file:tracinginsights:2024:1"].status == "done"
    assert items["file:tracinginsights_telemetry:2024:1"].status == "running"
    assert not (Path(LAKE) / "telemetry_lap_agg").exists()
    stale_dir = Path(LAKE) / "telemetry_lap_agg" / "season=2024" / "round=1"
    stale_dir.mkdir(parents=True)
    stale = stale_dir / ".telemetry_lap_agg.parquet.x.tmp"
    stale.write_bytes(b"partial")  # as a write killed half-way would leave
    code, _out, err, fetcher = _run(spark, [make_pin()])
    assert code == 0, err
    assert all("_tel.json" in u for u in fetcher.calls)  # the done tables item is not refetched
    assert not stale.exists()  # the rerun removes the killed write's temp file
    assert _data_files(killed / "data" / "lake") == expected


def test_load_pins_from_package() -> None:
    pins = L.load_pins()
    assert len(pins) == 114
    assert {s for s, _ in pins} == {2021, 2022, 2023, 2024, 2025}
    assert sum(1 for s, _ in pins if s == 2024) == 24
    pin = pins[(2024, 5)]
    assert (
        pin.folder == "Chinese Grand Prix"
        and pin.commit == "d1543a71370d9b617b627319f8ad80b2f85dfd5e"
    )
    assert pins[(2025, 1)].licence == "Apache-2.0" and pins[(2021, 1)].licence == "MIT"
    assert set(pin.files) == set(L.TABLE_FILES)


def test_load_pins_rejects_short_sha(tmp_path: Path) -> None:
    bad = {
        "seasons": {
            "2024": {
                "repo": "TracingInsights/2024",
                "commit": "abc",
                "licence": "MIT",
                "rounds": {},
            }
        }
    }
    path = tmp_path / "p.json"
    path.write_text(json.dumps(bad))
    with pytest.raises(ValueError, match="full SHA-1"):
        L.load_pins(path)


def test_url_and_host_allowlist() -> None:
    pin = make_pin()
    url = L.raw_url(pin, "São Paulo Grand Prix/Race/rcm.json")
    assert url == (
        f"https://raw.githubusercontent.com/TracingInsights/2024/{COMMIT}/"
        "S%C3%A3o%20Paulo%20Grand%20Prix/Race/rcm.json"
    )
    L.check_host(url)
    L.check_host("https://github.com/TracingInsights/2024.git")
    for bad in ("https://huggingface.co/x", "https://api.openf1.org/v1/stints", "http://evil/x"):
        with pytest.raises(ValueError, match="not allowed"):
            L.check_host(bad)


def test_telemetry_from_listing() -> None:
    lines = [
        "100644 blob aaa\tTestland Grand Prix/Race/VER/12_tel.json",
        "100644 blob bbb\tTestland Grand Prix/Race/VER/laptimes.json",
        "100644 blob ccc\tTestland Grand Prix/Race/VER/radio/x.mp3",
        "100644 blob ddd\tTestland Grand Prix/Qualifying/VER/1_tel.json",
        "100644 blob eee\tTestland Grand Prix/Race/HAM/2_tel.json",
        "100644 blob fff\tOther Grand Prix/Race/HAM/1_tel.json",
    ]
    files = L.telemetry_from_listing(lines, "Testland Grand Prix")
    assert [(f.driver_code, f.lap, f.blob_sha1) for f in files] == [
        ("HAM", 2, "eee"),
        ("VER", 12, "aaa"),
    ]
    with pytest.raises(ValueError, match="unexpected ls-tree"):
        L.telemetry_from_listing(
            ["040000 tree x\tTestland Grand Prix/Race/VER/1_tel.json"], "Testland Grand Prix"
        )


def test_git_tree_index_cache_must_be_inside_data() -> None:
    with pytest.raises(ValueError, match=r"inside \./data"):
        L.GitTreeIndex(Path("/tmp/x"))


class _Resp:
    def __init__(self, status: int, content: bytes = b"") -> None:
        self.status_code = status
        self.content = content


class _Session:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.headers: dict[str, str] = {}

    def get(self, url: str, timeout: float) -> Any:
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def _fetcher(responses: list[Any], **kw: Any) -> tuple[L.HttpFetcher, list[float]]:
    sleeps: list[float] = []
    f = L.HttpFetcher(sleep=sleeps.append, **kw)
    session = _Session(responses)
    f._session = lambda: session  # type: ignore[method-assign]
    return f, sleeps


def test_http_fetcher_retries_429_and_5xx_then_succeeds() -> None:
    f, sleeps = _fetcher([_Resp(429), requests.ConnectionError(), _Resp(503), _Resp(200, b"ok")])
    assert f.get(f"https://raw.githubusercontent.com/x/{COMMIT}/a") == b"ok"
    assert sleeps == [2.0, 4.0, 8.0] and f.retries == 3 and f.bytes == 2


def test_http_fetcher_gives_up_and_refuses_404() -> None:
    f, _ = _fetcher([_Resp(500)] * 3, max_retries=2)
    with pytest.raises(L.FetchError, match="after 3 attempts"):
        f.get("https://raw.githubusercontent.com/x")
    f, _ = _fetcher([_Resp(404)])
    with pytest.raises(L.FetchError, match="HTTP 404"):
        f.get("https://raw.githubusercontent.com/x")
    with pytest.raises(ValueError, match="not allowed"):
        f.get("https://example.com/x")


def test_http_fetcher_paces_request_starts() -> None:
    now = [0.0]
    f, sleeps = _fetcher([_Resp(200, b"a"), _Resp(200, b"b")], min_interval_s=1.0)
    f._clock = lambda: now[0]
    f.get("https://raw.githubusercontent.com/a")
    f.get("https://raw.githubusercontent.com/b")
    assert sleeps == [1.0]


def test_fetch_verified() -> None:
    from f1sa.ingest.tracinginsights_parse import git_blob_sha1

    class One:
        def get(self, url: str) -> bytes:
            return b"data"

    assert L.fetch_verified(One(), "u", git_blob_sha1(b"data")) == b"data"
    with pytest.raises(L.FetchError, match="does not match"):
        L.fetch_verified(One(), "u", git_blob_sha1(b"other"))


def test_parse_drivers() -> None:
    df = L.parse_drivers(b'{"drivers": [{"driver": "AAA", "team": "T"}]}', "d")
    assert list(df["driver_code"]) == ["AAA"]
    with pytest.raises(Exception, match="no drivers"):
        L.parse_drivers(b'{"drivers": []}', "d")
    with pytest.raises(Exception, match="expected"):
        L.parse_drivers(b'{"x": 1}', "d")


def test_cli_validation(workdir: Path) -> None:
    with pytest.raises(SystemExit):
        L.main(["--seasons", "2024", "--concurrency", "9"])
    with pytest.raises(SystemExit):
        L.main(["--seasons", "2024", "--rounds", "0"])
    with pytest.raises(SystemExit):
        L.main(["--seasons", "2024", "--min-free-gb", "0"])


def test_usage_errors(workdir: Path, spark: SparkSession) -> None:
    ingest = L.TracingInsightsIngest(
        _settings(),
        L.RunOptions(seasons=[2024], sink="hdfs"),
        pins={},
        err=io.StringIO(),
        out=io.StringIO(),
    )
    assert ingest.run() == 2
    ingest = L.TracingInsightsIngest(
        _settings(),
        L.RunOptions(seasons=[2019]),
        pins={(2024, 1): make_pin()},
        err=io.StringIO(),
        out=io.StringIO(),
        disk_usage=disk(100),
    )
    assert ingest.run() == 2
    with pytest.raises(ValueError, match="concurrency"):
        L.TracingInsightsIngest(_settings(), L.RunOptions(seasons=[2024], concurrency=0), pins={})


def test_dry_run_writes_nothing(workdir: Path, spark: SparkSession) -> None:
    code, out, _, fetcher = _run(spark, [make_pin()], dry_run=True)
    assert code == 0 and fetcher.calls == [] and "2 item(s) to run" in out
    assert not Path("data/manifest_tracinginsights.json").exists()
