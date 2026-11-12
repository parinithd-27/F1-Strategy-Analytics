"""``make api`` / ``make test-api`` and the compose ``api`` service (Step 13)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]


def _make(*args: str) -> str:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    result = subprocess.run(
        ["make", "-n", "-C", str(REPO), *args], capture_output=True, text=True, check=True
    )
    return result.stdout


def _compose() -> dict[str, Any]:
    doc: dict[str, Any] = yaml.safe_load((REPO / "docker-compose.yml").read_text())
    return doc


def test_make_api_runs_uvicorn_entry_point_on_the_file_lake_port_8001() -> None:
    out = _make("api")
    assert f"DATA_LAKE_URI=file://{REPO}/data/lake" in out
    assert "API_PORT=8001" in out and "python -m f1sa.api" in out
    assert "API_PORT=8123" in _make("api", "API_PORT=8123")


def _api_up(*args: str) -> str:
    return _make("api-up", *args)


def test_api_up_default_is_sample_mode_with_flags_off() -> None:
    out = _api_up()
    assert "API_DATA_LAKE_URI=file:///workspace/data/lake" in out
    assert "ENABLE_REPLAY=0 ENABLE_DEBUG=0" in out and "ALLOW_TEST_SEASON=0" in out
    assert "wait_for.sh -t 5 namenode" not in out


def test_api_up_full_mode_and_flags() -> None:
    out = _api_up("FULL=1", "REPLAY=1", "DEBUG=0", "TEST_SEASON=1")
    assert "API_DATA_LAKE_URI=hdfs://namenode:8020/f1" in out
    assert "ENABLE_REPLAY=1 ENABLE_DEBUG=0" in out and "ALLOW_TEST_SEASON=1" in out
    assert "wait_for.sh -t 5 namenode datanode httpfs" in out
    assert "ENABLE_DEBUG=1" in _api_up("DEBUG=1")
    assert "ENABLE_REPLAY=0" in _api_up("REPLAY=yes"), "only 1 enables a flag"


def test_api_up_recreates_only_the_api_service() -> None:
    out = _api_up("FULL=1", "REPLAY=1")
    compose = [ln for ln in out.splitlines() if "docker compose" in ln]
    assert compose == [
        "docker compose --profile core up -d --no-deps --no-build --force-recreate api"
    ]
    for forbidden in (" down", " -v", "--build ", " restart", " rm ", "volume", "nuke"):
        assert forbidden not in out, forbidden


def test_make_test_api_runs_the_api_tests() -> None:
    out = _make("test-api")
    assert "pytest" in out and "tests/unit/test_api_" in out and "test_demo_clean.py" in out


def test_compose_api_service() -> None:
    api = _compose()["services"]["api"]
    assert set(api["profiles"]) == {"core", "full"}
    assert api["image"] == "f1sa-app:dev"
    assert api["command"] == ["python", "-m", "f1sa.api"]
    assert api["volumes"] == [
        "./:/workspace:ro",
        "./data:/workspace/data:ro",
        "./data/demo:/workspace/data/demo",
        "./data/checkpoints:/workspace/data/checkpoints",
        "./results:/workspace/results",
        "./models:/workspace/models:ro",
    ]
    vols = {v.split(":")[1]: v for v in api["volumes"]}
    assert not vols["/workspace/results"].endswith(":ro")
    assert api["ports"] == ["127.0.0.1:${API_HOST_PORT:-8000}:8000"]
    env = api["environment"]
    assert env["DATA_LAKE_URI"] == "${API_DATA_LAKE_URI:-file:///workspace/data/lake}"
    assert env["HDFS_HTTPFS_URL"] == "http://httpfs:14000"
    assert env["API_HOST"] == "0.0.0.0"
    assert (
        env["API_CORS_ORIGINS"]
        == "${API_CORS_ORIGINS:-http://localhost:3000,http://127.0.0.1:3000}"
    )
    assert env["PYTHONPATH"] == "/workspace/src"
    assert env["ENABLE_REPLAY"] == "${ENABLE_REPLAY:-0}"
    assert env["ENABLE_DEBUG"] == "${ENABLE_DEBUG:-0}"
    assert env["ALLOW_TEST_SEASON"] == "${ALLOW_TEST_SEASON:-0}"
    assert api["mem_limit"] == "2048m"
    assert set(api["depends_on"]) == {"kafka", "kafka-init", "redis"}
    assert "healthcheck" in api


def test_app_service_is_unchanged() -> None:
    app = _compose()["services"]["app"]
    assert app["volumes"] == [
        "./:/workspace:ro",
        "./data:/workspace/data",
        "./results:/workspace/results",
        "./models:/workspace/models",
    ]
    assert "ports" not in app


def test_make_up_waits_for_the_api() -> None:
    assert "api" in _make("up").split("wait_for.sh", 1)[1].split("\n", 1)[0]


@pytest.mark.parametrize("target", ["up", "up-full", "api-up"])
def test_rw_mount_folders_are_created_first(target: str) -> None:
    lines = [ln for ln in _make(target).splitlines() if not ln.startswith("make")]
    assert lines[0] == "mkdir -p data/demo data/checkpoints"
