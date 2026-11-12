"""API settings (Step 13): defaults, flag parsing and validation."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from f1sa.config import Settings
from f1sa.streaming.stream_job import DEFAULT_MODEL_DIR


def test_defaults_are_closed() -> None:
    s = Settings()
    assert s.api_host == "127.0.0.1"
    assert s.api_port == 8000
    assert s.api_cors_origins == ["http://localhost:3000", "http://127.0.0.1:3000"]
    assert (s.enable_replay, s.enable_debug, s.allow_test_season) == (False, False, False)
    assert s.api_max_replays == 1
    assert s.api_sse_heartbeat_s == 15.0
    assert s.api_ui_latency_csv == Path("results/ui_latency.csv")
    assert s.api_replay_ready_timeout_s == 60.0
    assert s.demo_dir == Path("data/demo")


def test_model_dir_is_the_stream_jobs_model() -> None:
    assert Settings().api_model_dir == DEFAULT_MODEL_DIR


@pytest.mark.parametrize(("raw", "expected"), [("1", True), ("0", False), ("true", True)])
def test_flags_parse_from_env(monkeypatch: pytest.MonkeyPatch, raw: str, expected: bool) -> None:
    for name in ("ENABLE_REPLAY", "ENABLE_DEBUG", "ALLOW_TEST_SEASON"):
        monkeypatch.setenv(name, raw)
    s = Settings()
    assert (s.enable_replay, s.enable_debug, s.allow_test_season) == (expected,) * 3


@pytest.mark.parametrize(
    "origin",
    ["*", "http://*.example", "localhost:3000", "ftp://localhost", "http://localhost:3000/app"],
)
def test_cors_origins_must_be_http_origins(origin: str) -> None:
    with pytest.raises(ValidationError, match="api_cors_origins"):
        Settings(api_cors_origins=origin)
    with pytest.raises(ValidationError, match="api_cors_origins"):
        Settings(api_cors_origins=f"http://localhost:3000,{origin}")


def test_cors_origins_from_env_are_comma_separated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "API_CORS_ORIGINS", " http://localhost:3000/ , http://127.0.0.1:3000,http://localhost:3000"
    )
    assert Settings().api_cors_origins == ["http://localhost:3000", "http://127.0.0.1:3000"]
    monkeypatch.setenv("API_CORS_ORIGINS", " , ")
    with pytest.raises(ValidationError, match="api_cors_origins"):
        Settings()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("api_port", 0),
        ("api_max_replays", 0),
        ("api_sse_heartbeat_s", 0),
        ("api_kafka_timeout_s", 0),
        ("api_races_cache_s", -1),
        ("api_host", "bad host"),
        ("api_replay_ready_timeout_s", 0),
        ("api_replay_end_timeout_s", 0),
    ],
)
def test_invalid_values_are_refused(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        Settings(**{field: value})
