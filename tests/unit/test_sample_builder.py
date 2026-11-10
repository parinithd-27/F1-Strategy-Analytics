"""Tests for the sample bundle builder/validator (fixture-backed Jolpica; no network)."""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import subprocess
from pathlib import Path

import pandas as pd
import pytest
from _fakes import FIXTURES, FakeClock, FixtureAdapter

from f1sa.config import Settings
from f1sa.ingest import sample_builder as sb
from f1sa.ingest.jolpica_client import JolpicaClient
from f1sa.ingest.ratelimit import DualRateLimiter

RACES = [(2023, 5)]
SAMPLE = Path("data/sample")
REPO_ROOT = Path(__file__).resolve().parents[2]


class Clients:
    """Fixture-backed client factory that records how often it is used."""

    def __init__(self) -> None:
        self.adapter = FixtureAdapter()
        self.created = 0
        self.clock = FakeClock()
        self.instance: JolpicaClient | None = None

    def __call__(self, settings: Settings) -> JolpicaClient:
        self.created += 1
        if self.instance is None:
            limiter = DualRateLimiter(per_hour=450, clock=self.clock, sleep=self.clock.sleep)
            self.instance = JolpicaClient(
                settings, limiter=limiter, transport=self.adapter, cache_backend="memory"
            )
        return self.instance

    def paths(self) -> list[str]:
        return [(r.url or "").split("/ergast/f1/")[1] for r in self.adapter.requests]


def no_client(_settings: Settings) -> JolpicaClient:
    raise AssertionError("validation must never create an HTTP client")


def build(clients: Clients | None = None, **kw: object) -> sb.BuildReport:
    return sb.build(
        Settings(), client_factory=clients or Clients(), races=RACES, out=lambda _m: None, **kw
    )  # type: ignore[arg-type]


def test_build_writes_bundle_and_validates() -> None:
    clients = Clients()
    report = build(clients)
    assert sb.validate(SAMPLE) == []
    for rel in [
        "raw/jolpica/schedule/season=2023/schedule.json",
        "raw/jolpica/drivers/season=2023/drivers.json",
        "raw/jolpica/laps/season=2023/round=5/laps.json",
        "raw/jolpica/pitstops/season=2023/round=5/pitstops.json",
        "raw/synthetic/tyre_stints/season=2023/round=5/tyre_stints.parquet",
        "raw/synthetic/lap_telemetry/season=2023/round=5/lap_telemetry.parquet",
        "LICENSE",
        "README.md",
        "MANIFEST.json",
    ]:
        assert (SAMPLE / rel).is_file(), rel
    manifest = json.loads((SAMPLE / "MANIFEST.json").read_text())
    kinds = {f["path"].split("/")[2]: f["kind"] for f in manifest["files"]}
    assert kinds["tyre_stints"] == "synthetic" and kinds["laps"] == "real"
    assert report.origins == {"network": 7} and report.network_requests == 9  # laps: 3 pages
    license_text = (SAMPLE / "LICENSE").read_text()
    assert (
        "CC BY-NC-SA 4.0" in license_text
        and "https://github.com/jolpica/jolpica-f1" in license_text
    )
    readme = (SAMPLE / "README.md").read_text()
    assert "NOT real Formula 1 compound or telemetry" in readme and "smoke test only" in readme
    assert not list(Path("data").glob("sample.tmp-*")) and not list(
        Path("data").glob("sample.old-*")
    )


def test_raw_zone_first_then_cache_then_network() -> None:
    # raw zone already has results for 2023 R5 (as written by a bulk ingestion)
    raw = Path("data/lake/raw/jolpica/results/season=2023/round=5")
    raw.mkdir(parents=True)
    (raw / "results.json").write_text(
        json.dumps([json.loads((FIXTURES / "results_2023_5.json").read_text())])
    )
    clients = Clients()
    warm = clients(Settings())
    warm.get_pages("2023/5/laps")  # laps are in the HTTP cache
    before = len(clients.adapter.requests)
    report = build(clients)
    fetched = clients.paths()[before:]
    assert not any(p.startswith("2023/5/results") for p in fetched)  # served from the raw zone
    assert not any(p.startswith("2023/5/laps") for p in fetched)  # served from the cache
    assert report.origins == {"raw": 1, "cache": 1, "network": 5}


def test_validate_mode_is_offline() -> None:
    build()
    assert sb.run(Settings(), client_factory=no_client, races=RACES, out=lambda _m: None) == 0


def test_tampered_file_is_detected() -> None:
    build()
    target = SAMPLE / "raw/jolpica/laps/season=2023/round=5/laps.json"
    target.write_text(target.read_text().replace("1:3", "1:4", 1))
    problems = sb.validate(SAMPLE)
    assert any("checksum mismatch" in p and "laps.json" in p for p in problems)
    messages: list[str] = []
    assert sb.run(Settings(), client_factory=no_client, races=RACES, out=messages.append) == 1
    assert messages[0].startswith("INVALID")


def test_fastf1_data_is_rejected() -> None:
    build()
    planted = SAMPLE / "raw/fastf1/laps/season=2023/round=5"
    planted.mkdir(parents=True)
    (planted / "laps.parquet").write_bytes(b"x")
    (SAMPLE / "cache.ff1pkl").write_bytes(b"x")
    problems = "\n".join(sb.validate(SAMPLE))
    assert "FastF1 data is not allowed" in problems and "ff1pkl" in problems


def _replace_synthetic(table: str, frame: pd.DataFrame) -> None:
    path = next(SAMPLE.rglob(f"{table}.parquet"))
    buf = io.BytesIO()
    frame.to_parquet(buf, index=False)
    path.write_bytes(buf.getvalue())
    manifest = json.loads((SAMPLE / "MANIFEST.json").read_text())
    for f in manifest["files"]:
        if f["path"].endswith(f"{table}.parquet"):
            f["sha256"] = hashlib.sha256(buf.getvalue()).hexdigest()
            f["bytes"] = len(buf.getvalue())
    (SAMPLE / "MANIFEST.json").write_text(json.dumps(manifest))


def test_fastf1_columns_and_missing_flags_are_rejected() -> None:
    build()
    path = next(SAMPLE.rglob("lap_telemetry.parquet"))
    frame = pd.read_parquet(path)
    frame["Speed"] = 300.0  # a FastF1 telemetry column
    frame.loc[0, "is_synthetic"] = False
    _replace_synthetic("lap_telemetry", frame)
    problems = "\n".join(sb.validate(SAMPLE))
    assert "FastF1 columns ['Speed']" in problems
    assert "without is_synthetic=true" in problems


def test_regeneration_mismatch_is_detected() -> None:
    build()
    manifest = json.loads((SAMPLE / "MANIFEST.json").read_text())
    manifest["seed"] = manifest["seed"] + 1
    (SAMPLE / "MANIFEST.json").write_text(json.dumps(manifest))
    assert any("does not regenerate identically" in p for p in sb.validate(SAMPLE))


def test_rebuild_is_deterministic_for_synthetic_tables() -> None:
    build()
    first = {
        f["path"]: f["sha256"] for f in json.loads((SAMPLE / "MANIFEST.json").read_text())["files"]
    }
    build()
    second = {
        f["path"]: f["sha256"] for f in json.loads((SAMPLE / "MANIFEST.json").read_text())["files"]
    }
    assert first == second  # same raw pages + same seed -> identical bundle content


def test_size_limit_and_failed_build_keeps_old_bundle() -> None:
    build()
    before = (SAMPLE / "MANIFEST.json").read_bytes()
    with pytest.raises(sb.SampleError, match="limit 1000"):
        build(size_limit=1000)
    assert (SAMPLE / "MANIFEST.json").read_bytes() == before  # untouched
    assert sb.validate(SAMPLE) == []
    assert not list(Path("data").glob("sample.tmp-*"))


def test_unlisted_and_missing_files() -> None:
    build()
    (SAMPLE / "notes.txt").write_text("x")
    (SAMPLE / "LICENSE").unlink()
    assert sb.validate(SAMPLE) == ["missing LICENSE"]
    shutil.copy(REPO_ROOT / "LICENSE", SAMPLE / "LICENSE")
    assert any("unlisted file notes.txt" in p for p in sb.validate(SAMPLE))


def test_sample_dir_must_be_inside_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SAMPLE_DIR", str(tmp_path.parent / "elsewhere"))
    assert sb.run(Settings(), client_factory=no_client, out=lambda _m: None) == 2
    monkeypatch.setenv("SAMPLE_DIR", "data")
    with pytest.raises(sb.SampleError):
        sb.build(Settings(), client_factory=no_client)


def test_empty_required_table_fails_build() -> None:
    clients = Clients()
    empty = json.loads((FIXTURES / "results_2023_5.json").read_text())
    empty["MRData"]["RaceTable"]["Races"][0]["Results"] = []
    empty["MRData"]["total"] = "0"
    raw = Path("data/lake/raw/jolpica/results/season=2023/round=5")
    raw.mkdir(parents=True)
    (raw / "results.json").write_text(json.dumps([empty]))
    with pytest.raises(sb.SampleError, match=r"results .* is empty"):
        build(clients)
    assert not SAMPLE.exists()


@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
@pytest.mark.parametrize(
    ("path", "ignored"),
    [
        ("data/sample/raw/jolpica/laps/season=2023/round=5/laps.json", False),
        ("data/sample/MANIFEST.json", False),
        ("data/lake/raw/jolpica/laps/season=2023/round=5/laps.json", True),
        ("data/cache/jolpica/http_cache.sqlite", True),
        ("data/manifest.json", True),
        ("data/sample.tmp-123/MANIFEST.json", True),
    ],
)
def test_gitignore_tracks_only_data_sample(path: str, ignored: bool) -> None:
    result = subprocess.run(
        ["git", "check-ignore", "-q", "--no-index", path], cwd=REPO_ROOT, check=False
    )
    assert (result.returncode == 0) is ignored
