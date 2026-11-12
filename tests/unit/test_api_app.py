"""App wiring: OpenAPI docs, exact route set, CORS, JSON access log, bind address."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from _api_fixtures import make_api, make_settings

from f1sa.api.__main__ import PortInUseError, check_port_free, main, port_holder, uvicorn_options
from f1sa.api.log import JsonFormatter
from f1sa.config import Settings

pytestmark = pytest.mark.anyio

ROUTES = {
    "/api/health": {"get"},
    "/api/races": {"get"},
    "/api/races/{season}/{round}/state": {"get"},
    "/api/races/{season}/{round}/events": {"get"},
    "/api/races/{season}/{round}/replay": {"post"},
    "/api/replays": {"get"},
    "/api/debug/latency": {"post"},
}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


async def test_docs_and_exact_routes(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    async with api.client() as c:
        docs = await c.get("/api/docs")
        spec = (await c.get("/api/openapi.json")).json()
    assert docs.status_code == 200 and "swagger" in docs.text.lower()
    assert {p: set(ops) for p, ops in spec["paths"].items()} == ROUTES
    assert not any("whatif" in p for p in spec["paths"])
    state = spec["paths"]["/api/races/{season}/{round}/state"]["get"]
    assert {"200", "404", "422", "500", "503"} <= set(state["responses"])
    replay = spec["paths"]["/api/races/{season}/{round}/replay"]["post"]
    assert {"202", "403", "404", "409", "422", "500"} <= set(replay["responses"])


async def test_cors_allows_only_the_dashboard(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path))
    preflight = {"Access-Control-Request-Method": "GET"}
    async with api.client() as c:
        ok = await c.options("/api/races", headers={"Origin": "http://localhost:3000", **preflight})
        evil = await c.options("/api/races", headers={"Origin": "http://evil.example", **preflight})
        simple = await c.get("/api/health", headers={"Origin": "http://evil.example"})
    assert ok.status_code == 200
    assert ok.headers["access-control-allow-origin"] == "http://localhost:3000"
    assert "access-control-allow-credentials" not in ok.headers
    assert evil.status_code == 400
    assert "access-control-allow-origin" not in evil.headers
    assert "access-control-allow-origin" not in simple.headers


async def test_access_log_is_one_structured_line_per_request(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    api = make_api(make_settings(tmp_path))
    with caplog.at_level(logging.INFO, logger="f1sa.api.access"):
        async with api.client() as c:
            await c.get("/api/races/2024/5/state?token=hunter2")
    records = [r for r in caplog.records if r.name == "f1sa.api.access"]
    assert len(records) == 1
    line = json.loads(JsonFormatter().format(records[0]))
    assert line["msg"] == "request"
    assert (line["method"], line["path"], line["status"]) == (
        "GET",
        "/api/races/2024/5/state",
        404,
    )
    assert isinstance(line["duration_ms"], float)
    assert line["level"] == "INFO" and line["ts"].endswith("Z")
    assert "hunter2" not in caplog.text


def test_json_formatter_logs_exception_class_only() -> None:
    try:
        raise ConnectionError("redis://user:pw@host:6379 refused")
    except ConnectionError:
        import sys

        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "boom", None, sys.exc_info())
    out = json.loads(JsonFormatter().format(record))
    assert out["exc_type"] == "ConnectionError"
    assert "pw@host" not in json.dumps(out)


def test_binds_localhost_by_default() -> None:
    opts = uvicorn_options(Settings())
    assert (opts["host"], opts["port"]) == ("127.0.0.1", 8000)
    assert opts["access_log"] is False and opts["log_config"] is None
    assert uvicorn_options(Settings(api_host="0.0.0.0", api_port=9000))["host"] == "0.0.0.0"


async def test_lifespan_shuts_down_replays_and_redis(tmp_path: Path) -> None:
    api = make_api(make_settings(tmp_path, enable_replay=True))
    api.launcher.hold_producer = True
    await api.deps.replays.start(2024, 5, 10)
    async with api.app.router.lifespan_context(api.app):
        pass
    assert all(p.terminated for p in api.launcher.processes)
    assert api.deps.replays.running() == [] and api.deps.replays.jobs == {}


def test_port_clash_names_the_holder_and_the_remedy() -> None:
    import os
    import socket

    with socket.socket() as holder:
        holder.bind(("127.0.0.1", 0))
        holder.listen()
        port = holder.getsockname()[1]
        assert port_holder(port) is not None and f"pid {os.getpid()}" in str(port_holder(port))
        with pytest.raises(PortInUseError) as err:
            check_port_free("127.0.0.1", port)
        msg = str(err.value)
        assert f"127.0.0.1:{port} is already in use by pid {os.getpid()}" in msg
        assert f"make api API_PORT={port + 1}" in msg and "docker compose stop api" in msg
        with pytest.raises(PortInUseError, match="unknown process"):
            check_port_free("127.0.0.1", port, holder=lambda p: None)
    check_port_free("127.0.0.1", port)  # free again


def test_main_exits_2_on_a_port_clash(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    started: list[object] = []

    def clash(host: str, port: int) -> None:
        raise PortInUseError(f"port {host}:{port} is already in use by Docker container f1sa-api-1")

    monkeypatch.setenv("API_PORT", "8001")
    assert main(run=lambda *a, **k: started.append(a), check=clash) == 2
    assert started == []
    assert "8001 is already in use by Docker container f1sa-api-1" in capsys.readouterr().err
    assert main(run=lambda *a, **k: started.append(k), check=lambda h, p: None) == 0
    assert started and started[0]["port"] == 8001  # type: ignore[index]
