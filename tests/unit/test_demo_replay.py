"""``f1sa.demo_replay`` (make demo, Step 17) against a scripted fake API and a real HTTP server."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from f1sa import demo_replay as DR

RID = "2024_r5_20261114T120000Z"


def _health(**over: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "status": "ok",
        "checks": {
            "redis": {"ok": True},
            "kafka": {"ok": True},
            "model": {"ok": True, "model_version": DR.DEFAULT_MODEL_VERSION},
        },
        "lake": {"scheme": "file", "mode": "sample", "synthetic": True},
    }
    body.update(over)
    return body


@dataclass
class FakeApi:
    """Answers like the API; ``state_after`` polls until /state has 20 cars, ``finish_after``
    polls of /api/replays until the run is finished (or ``fail`` makes it fail)."""

    health: list[tuple[int, Any]] = field(default_factory=lambda: [(200, _health())])
    races: tuple[int, Any] = (
        200,
        [{"season": 2024, "round": 5, "name": "Chinese Grand Prix", "synthetic": True}],
    )
    enabled: bool = True
    post: tuple[int, Any] = (202, {"run_id": RID, "results_dir": f"data/demo/{RID}"})
    state_after: int = 2
    finish_after: int = 3
    fail: str | None = None
    calls: list[tuple[str, str, Any]] = field(default_factory=list)
    _state_polls: int = 0
    _replay_polls: int = 0

    def __call__(self, method: str, url: str, body: Mapping[str, Any] | None) -> tuple[int, Any]:
        path = url.removeprefix("http://api:8000")
        self.calls.append((method, path, body))
        if path == "/api/health":
            return self.health.pop(0) if len(self.health) > 1 else self.health[0]
        if path == "/api/races":
            return self.races
        if path == "/api/replays":
            return 200, self._replays()
        if method == "POST" and path == "/api/races/2024/5/replay":
            return self.post
        if path == "/api/races/2024/5/state":
            self._state_polls += 1
            if self._state_polls < self.state_after:
                return 404, {"detail": "no live state"}
            return 200, {"drivers": {f"d{i}": {} for i in range(20)}, "order": []}
        return 404, {"detail": f"unknown {path}"}

    def _replays(self) -> dict[str, Any]:
        started = any(m == "POST" for m, _p, _b in self.calls)
        if not started:
            return {"enabled": self.enabled, "max_running": 1, "running": [], "recent": []}
        self._replay_polls += 1
        if self.fail is not None:
            job = {"run_id": RID, "state": "failed", "error": self.fail}
            return {"enabled": True, "running": [], "recent": [job]}
        if self._replay_polls >= self.finish_after:
            job = {"run_id": RID, "state": "finished", "ended_at": "2026-11-14T12:02:00.000Z"}
            return {"enabled": True, "running": [], "recent": [job]}
        return {"enabled": True, "running": [{"run_id": RID, "state": "running"}], "recent": []}


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


def _demo(api: FakeApi, **cfg: Any) -> tuple[DR.Demo, list[str]]:
    lines: list[str] = []
    clock = Clock()
    demo = DR.Demo(
        DR.DemoConfig(**{"timeout_s": 30.0, **cfg}),
        fetch=api,
        clock=clock,
        sleep=clock.sleep,
        out=lines.append,
    )
    return demo, lines


def test_happy_path_starts_a_60x_replay_and_prints_the_dashboard() -> None:
    api = FakeApi()
    demo, lines = _demo(api)
    demo.run()
    assert ("POST", "/api/races/2024/5/replay", {"speed": 60}) in api.calls
    assert lines[-1] == "Dashboard: http://localhost:3000/race/2024/5  (SYNTHETIC DEMO DATA)"
    assert "state: 20 cars in Redis" in lines
    assert not any("finished" in line for line in lines)


def test_wait_finished_waits_for_the_replay_to_end() -> None:
    api = FakeApi(finish_after=4)
    demo, lines = _demo(api, wait="finished")
    demo.run()
    assert any(line.startswith("replay: finished") for line in lines)


def test_health_waits_for_the_api_to_answer() -> None:
    api = FakeApi(health=[(0, None), (0, None), (200, _health())])
    demo, _lines = _demo(api)
    demo.check_health()
    assert [p for _m, p, _b in api.calls].count("/api/health") == 3


@pytest.mark.parametrize(
    ("body", "match"),
    [
        (_health(checks={"redis": {"ok": False, "error": "down"}}), "redis not ok"),
        (
            _health(
                checks={
                    "redis": {"ok": True},
                    "kafka": {"ok": True},
                    "model": {"ok": False, "error": "JobError: missing"},
                }
            ),
            "model not ok",
        ),
        (
            _health(
                checks={
                    "redis": {"ok": True},
                    "kafka": {"ok": True},
                    "model": {"ok": True, "model_version": "deploy_real_G3.5_k2"},
                }
            ),
            "expected 'sample_deploy_real_G3.5_k2'",
        ),
        (_health(lake={"scheme": "hdfs", "mode": "full", "synthetic": False}), "not the synthetic"),
    ],
)
def test_health_failures_are_explained(body: dict[str, Any], match: str) -> None:
    demo, _lines = _demo(FakeApi(health=[(503, body)]))
    with pytest.raises(DR.DemoError, match=match):
        demo.check_health()


def test_health_times_out_when_the_api_never_answers() -> None:
    demo, _lines = _demo(FakeApi(health=[(0, None)]), timeout_s=3.0)
    with pytest.raises(DR.DemoError, match="timed out after 3 s"):
        demo.check_health()


def test_race_must_be_listed_and_synthetic() -> None:
    missing = FakeApi(races=(200, [{"season": 2023, "round": 5, "synthetic": True}]))
    with pytest.raises(DR.DemoError, match="not in /api/races"):
        _demo(missing)[0].check_race()
    real = FakeApi(races=(200, [{"season": 2024, "round": 5, "synthetic": False}]))
    with pytest.raises(DR.DemoError, match="not synthetic"):
        _demo(real)[0].check_race()


def test_replay_disabled_conflict_and_bad_answers_stop_the_demo() -> None:
    with pytest.raises(DR.DemoError, match="REPLAY=1"):
        _demo(FakeApi(enabled=False))[0].start_replay()
    conflict = FakeApi(post=(409, {"detail": "a replay of 2024 R5 is running"}))
    with pytest.raises(DR.DemoError, match="is running; wait"):
        _demo(conflict)[0].start_replay()
    with pytest.raises(DR.DemoError, match="status 403"):
        _demo(FakeApi(post=(403, {"detail": "ENABLE_REPLAY is off"})))[0].start_replay()


def test_a_failed_replay_stops_the_wait_with_its_error() -> None:
    api = FakeApi(state_after=99, fail="stream job not ready within 60 s")
    demo, _lines = _demo(api)
    with pytest.raises(DR.DemoError, match="failed: stream job not ready"):
        demo.run()


def test_state_wait_times_out() -> None:
    demo, _lines = _demo(FakeApi(state_after=10_000, finish_after=10_000), timeout_s=5.0)
    with pytest.raises(DR.DemoError, match="20 cars in /state"):
        demo.run()


def test_config_validation_and_cli_exit_codes(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(ValueError):
        DR.DemoConfig(speed=5)
    with pytest.raises(ValueError):
        DR.DemoConfig(wait="forever")
    assert DR.main(["--min-drivers", "0"], fetch=FakeApi()) == 2
    assert DR.main([], fetch=FakeApi(enabled=False)) == 1
    assert "demo failed: replay is disabled" in capsys.readouterr().err
    assert DR.main(["--timeout-s", "30"], fetch=FakeApi(state_after=1)) == 0
    assert "SYNTHETIC DEMO DATA" in capsys.readouterr().out


# ------------------------------------------------------------------ http_json, real sockets
class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        code, body = (200, {"ok": True}) if self.path == "/ok" else (404, {"detail": "nope"})
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:
        n = int(self.headers.get("content-length", "0"))
        raw = json.dumps({"echo": json.loads(self.rfile.read(n))}).encode()
        self.send_response(202)
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args: Any) -> None:
        return


@pytest.fixture()
def server() -> Iterator[str]:
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_http_json_statuses_bodies_and_connection_errors(server: str) -> None:
    assert DR.http_json("GET", f"{server}/ok") == (200, {"ok": True})
    assert DR.http_json("GET", f"{server}/missing") == (404, {"detail": "nope"})
    assert DR.http_json("POST", f"{server}/x", {"speed": 60}) == (202, {"echo": {"speed": 60}})
    assert DR.http_json("GET", "http://127.0.0.1:9/closed", timeout_s=1.0) == (0, None)
