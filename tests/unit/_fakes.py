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
    "2023/drivers": "drivers_2023",
    "2023/constructors": "constructors_2023",
    "2023/4/sprint": "sprint_2023_4",
    "2023/4/results": "results_2023_4",
    "2023/4/qualifying": "qualifying_2023_4",
    "2023/4/pitstops": "pitstops_2023_4",
}
LAPS_PAGES = {0: "laps_2023_5_p0", 4: "laps_2023_5_p1", 8: "laps_2023_5_p2"}
PAGED = {"2023/5/laps": LAPS_PAGES, "2023/4/laps": {0: "laps_2023_4_p0"}}


class FixtureAdapter(HTTPAdapter):
    """Serves the recorded Jolpica fixtures; unknown URLs get a 404. Records every request."""

    def __init__(self, fail_paths: set[str] | None = None) -> None:
        super().__init__()
        self.requests: list[requests.PreparedRequest] = []
        self.fail_paths = fail_paths or set()

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.requests.append(request)
        parsed = urlparse(request.url or "")
        path = parsed.path.removeprefix(BASE_PATH).removesuffix(".json")
        offset = int(parse_qs(parsed.query).get("offset", ["0"])[0])
        name = PAGED[path].get(offset) if path in PAGED else ROUTES.get(path)
        if path in self.fail_paths:
            return _build(self, request, 500, b'{"detail": "boom"}')
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


class FakeHttpFS(HTTPAdapter):
    """In-memory HttpFS/WebHDFS gateway with the real server's quirks.

    * ``RENAME`` onto an existing file answers 200 ``{"boolean": false}`` (verified on 3.4.3).
    * ``CREATE`` requires ``data=true`` (no redirect support here, as with HttpFS).
    * ``faults`` maps an op name to a list of HTTP statuses returned before succeeding.
    """

    def __init__(self, faults: dict[str, list[int]] | None = None) -> None:
        super().__init__()
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = {"/"}
        self.ops: list[tuple[str, str]] = []
        self.faults = {k: list(v) for k, v in (faults or {}).items()}
        self.truncate_create = False

    def _json(
        self, request: requests.PreparedRequest, status: int, obj: object
    ) -> requests.Response:
        return _build(self, request, status, json.dumps(obj).encode())

    def _missing(self, request: requests.PreparedRequest, path: str) -> requests.Response:
        return self._json(
            request,
            404,
            {
                "RemoteException": {
                    "message": f"File does not exist: {path}",
                    "exception": "FileNotFoundException",
                }
            },
        )

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        parsed = urlparse(request.url or "")
        q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        path = parsed.path.removeprefix("/webhdfs/v1") or "/"
        op = q["op"]
        self.ops.append((op, path))
        assert q.get("user.name") == "hadoop"
        if self.faults.get(op):
            return _build(self, request, self.faults[op].pop(0), b"{}")
        if op == "MKDIRS":
            parts = path.strip("/").split("/")
            for i in range(1, len(parts) + 1):
                self.dirs.add("/" + "/".join(parts[:i]))
            return self._json(request, 200, {"boolean": True})
        if op == "CREATE":
            assert q.get("data") == "true", "HttpFS client must stream data directly"
            body = request.body if isinstance(request.body, bytes) else (request.body or b"")
            if isinstance(body, str):
                body = body.encode()
            self.files[path] = body[:-1] if self.truncate_create and body else body
            return self._json(request, 201, {"Location": path})
        if op == "GETFILESTATUS":
            if path in self.files:
                return self._json(
                    request, 200, {"FileStatus": {"type": "FILE", "length": len(self.files[path])}}
                )
            if path in self.dirs:
                return self._json(request, 200, {"FileStatus": {"type": "DIRECTORY", "length": 0}})
            return self._missing(request, path)
        if op == "DELETE":
            existed = self.files.pop(path, None) is not None
            return self._json(request, 200, {"boolean": existed})
        if op == "RENAME":
            dest = q["destination"]
            if path not in self.files or dest in self.files:
                return self._json(request, 200, {"boolean": False})
            self.files[dest] = self.files.pop(path)
            return self._json(request, 200, {"boolean": True})
        if op == "OPEN":
            if path not in self.files:
                return self._missing(request, path)
            return _build(
                self, request, 200, self.files[path], {"Content-Type": "application/octet-stream"}
            )
        if op == "LISTSTATUS":
            if path not in self.dirs:
                return self._missing(request, path)
            prefix = path.rstrip("/") + "/"
            entries = [
                {"pathSuffix": f[len(prefix) :], "type": "FILE", "length": len(b)}
                for f, b in self.files.items()
                if f.startswith(prefix) and "/" not in f[len(prefix) :]
            ]
            return self._json(request, 200, {"FileStatuses": {"FileStatus": entries}})
        return self._json(request, 400, {"RemoteException": {"message": f"bad op {op}"}})
