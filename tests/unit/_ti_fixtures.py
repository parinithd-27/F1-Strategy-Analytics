"""Shared helpers for the TracingInsights tests: pins, a fake fetcher and a fake git tree that
serve the invented fixture race in ``tests/fixtures/tracinginsights`` (no network)."""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path

from f1sa.ingest import tracinginsights_parse as P
from f1sa.ingest.tracinginsights_loader import (
    TABLE_FILES,
    RacePin,
    TelemetryFile,
    raw_url,
)

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tracinginsights"
FOLDER = "Testland Grand Prix"
RACE_DIR = FIXTURE_ROOT / FOLDER / "Race"
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def fixture_files() -> dict[str, bytes]:
    """Repository path -> bytes for every fixture file."""
    return {
        str(p.relative_to(FIXTURE_ROOT)): p.read_bytes() for p in sorted(RACE_DIR.rglob("*.json"))
    }


def make_pin(season: int = 2024, round_: int = 1, licence: str = "MIT") -> RacePin:
    files = fixture_files()
    return RacePin(
        season=season,
        round=round_,
        event=FOLDER,
        folder=FOLDER,
        commit=COMMIT,
        repo=f"TracingInsights/{season}",
        licence=licence,
        files={n: P.git_blob_sha1(files[f"{FOLDER}/Race/{n}"]) for n in TABLE_FILES},
    )


class FakeFetcher:
    """Serves the fixture files for the given pins; records every URL."""

    def __init__(
        self,
        pins: list[RacePin],
        *,
        corrupt: set[str] | None = None,
        on_get: Callable[[str, int], None] | None = None,
    ) -> None:
        self.urls: dict[str, bytes] = {}
        for pin in pins:
            for path, data in fixture_files().items():
                self.urls[raw_url(pin, path)] = data
        self.corrupt = corrupt or set()
        self.calls: list[str] = []
        self._on_get = on_get
        self._lock = threading.Lock()

    def get(self, url: str) -> bytes:
        with self._lock:
            self.calls.append(url)
            n = len(self.calls)
        if self._on_get is not None:
            self._on_get(url, n)
        if url not in self.urls:
            raise AssertionError(f"unexpected URL {url}")
        data = self.urls[url]
        if any(url.endswith(c) for c in self.corrupt):
            return data + b" "
        return data


class FakeTree:
    """The fixture race's telemetry listing with real git blob ids."""

    def __init__(self) -> None:
        self.calls = 0

    def telemetry_files(self, pin: RacePin) -> list[TelemetryFile]:
        self.calls += 1
        out = []
        for path, data in fixture_files().items():
            if path.endswith("_tel.json"):
                driver, name = path.split("/")[-2:]
                out.append(
                    TelemetryFile(driver, P.telemetry_lap_number(name), path, P.git_blob_sha1(data))
                )
        return sorted(out, key=lambda f: (f.driver_code, f.lap))


class Usage:
    def __init__(self, free_gb: float) -> None:
        self.free = int(free_gb * 1e9)


def disk(free_gb: float) -> Callable[[str], Usage]:
    return lambda _path: Usage(free_gb)
