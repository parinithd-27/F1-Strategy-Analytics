"""Test doubles shared by unit tests: a fake clock and fake HTTP transports (no network)."""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3 import HTTPResponse

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "jolpica"
BASE_PATH = "/ergast/f1/"


class FakeClock:
    """Monotonic clock that only moves when told to (or when ``sleep`` is called)."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    def advance(self, seconds: float) -> None:
        self.now += seconds


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def _build(
    adapter: HTTPAdapter,
    request: requests.PreparedRequest,
    status: int,
    body: bytes,
    headers: dict[str, str] | None = None,
) -> requests.Response:
    raw = HTTPResponse(
        body=io.BytesIO(body),
        status=status,
        headers={"Content-Type": "application/json", **(headers or {})},
        preload_content=False,
        decode_content=False,
    )
    return adapter.build_response(request, raw)


# path after /ergast/f1/ (without .json) -> fixture base name; laps are paged by offset
ROUTES = {
    "seasons": "seasons",
    "2023/races": "races_2023",
    "2023/5": "race_2023_5",
    "2023/5/results": "results_2023_5",
    "2023/5/qualifying": "qualifying_2023_5",
    "2023/5/drivers": "drivers_2023_5",
    "2023/5/constructors": "constructors_2023_5",
    "2023/5/pitstops": "pitstops_2023_5",
}
LAPS_PAGES = {0: "laps_2023_5_p0", 4: "laps_2023_5_p1", 8: "laps_2023_5_p2"}


class FixtureAdapter(HTTPAdapter):
    """Serves the recorded Jolpica fixtures; unknown URLs get a 404. Records every request."""

    def __init__(self) -> None:
        super().__init__()
        self.requests: list[requests.PreparedRequest] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.requests.append(request)
        parsed = urlparse(request.url or "")
        path = parsed.path.removeprefix(BASE_PATH).removesuffix(".json")
        offset = int(parse_qs(parsed.query).get("offset", ["0"])[0])
        name = LAPS_PAGES.get(offset) if path == "2023/5/laps" else ROUTES.get(path)
        if name is None:
            return _build(self, request, 404, b'{"detail": "Not found."}')
        return _build(self, request, 200, (FIXTURES / f"{name}.json").read_bytes())


Step = tuple[int, dict[str, str], bytes] | Exception


class ScriptedAdapter(HTTPAdapter):
    """Replies with a fixed sequence of (status, headers, body) or raises the given exception."""

    def __init__(self, steps: list[Step]) -> None:
        super().__init__()
        self.steps = list(steps)
        self.calls = 0

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.calls += 1
        if not self.steps:
            raise AssertionError("ScriptedAdapter ran out of steps")
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        status, headers, body = step
        return _build(self, request, status, body, headers)


def page(total: int, limit: int, offset: int, seasons: list[str]) -> bytes:
    """A minimal Ergast-style SeasonTable page (used for paging/retry edge cases)."""
    body = {
        "MRData": {
            "limit": str(limit),
            "offset": str(offset),
            "total": str(total),
            "SeasonTable": {"Seasons": [{"season": s, "url": ""} for s in seasons]},
        }
    }
    return json.dumps(body).encode()


OK_PAGE: Callable[[], bytes] = lambda: page(1, 100, 0, ["2023"])  # noqa: E731
