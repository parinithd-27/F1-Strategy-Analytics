"""Tests for f1sa.config."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from f1sa.config import JOLPICA_HARD_LIMIT_PER_HOUR, Settings, get_settings


def test_defaults() -> None:
    s = Settings()
    assert s.data_lake_uri == "file://./data/lake"
    assert s.kafka_bootstrap == "localhost:9092"
    assert s.kafka_topic_events == "f1.race.events"
    assert s.redis_url == "redis://localhost:6379/0"
    assert s.redis_ttl_after_session_s == 3600
    assert s.jolpica_base_url == "https://api.jolpi.ca/ergast/f1"
    assert s.jolpica_max_per_hour == 450
    assert s.fastf1_cache_dir == Path("data/cache/fastf1")
    assert s.jolpica_cache_dir == Path("data/cache/jolpica")
    assert s.jolpica_user_agent.startswith("F1-Strategy-Analytics/")
    assert s.hdfs_httpfs_url == "http://localhost:14000"
    assert s.hdfs_user == "hadoop"
    assert s.manifest_path == Path("data/manifest.json")
    assert s.fastf1_max_races_per_hour == 20
    assert s.f1_username is None
    assert s.f1_password is None
    assert s.has_f1_credentials is False


def test_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_LAKE_URI", "hdfs://namenode:9000/f1/")
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "kafka:9092, kafka2:9093")
    monkeypatch.setenv("REDIS_URL", "rediss://cache.example:6380/1")
    monkeypatch.setenv("KAFKA_TOPIC_EVENTS", " f1.race.events-test ")
    monkeypatch.setenv("REDIS_TTL_AFTER_SESSION_S", "120")
    monkeypatch.setenv("JOLPICA_BASE_URL", "http://localhost:8000/ergast/f1/")
    monkeypatch.setenv("JOLPICA_MAX_PER_HOUR", "100")
    monkeypatch.setenv("FASTF1_CACHE_DIR", "/tmp/ff1")
    s = Settings()
    assert s.data_lake_uri == "hdfs://namenode:9000/f1"
    assert s.kafka_bootstrap == "kafka:9092,kafka2:9093"
    assert s.redis_url == "rediss://cache.example:6380/1"
    assert s.kafka_topic_events == "f1.race.events-test"
    assert s.redis_ttl_after_session_s == 120
    assert s.jolpica_base_url == "http://localhost:8000/ergast/f1"
    assert s.jolpica_max_per_hour == 100
    assert s.fastf1_cache_dir == Path("/tmp/ff1")


def test_dotenv_file_is_loaded(tmp_path: Path) -> None:
    # conftest chdirs into tmp_path, so Settings() resolves this .env
    (tmp_path / ".env").write_text(
        "REDIS_TTL_AFTER_SESSION_S=42\nUNRELATED_VAR=ignored\n", encoding="utf-8"
    )
    assert Settings().redis_ttl_after_session_s == 42


def test_env_var_beats_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("REDIS_TTL_AFTER_SESSION_S=42\n", encoding="utf-8")
    monkeypatch.setenv("REDIS_TTL_AFTER_SESSION_S", "7")
    assert Settings().redis_ttl_after_session_s == 7


@pytest.mark.parametrize(
    ("var", "value"),
    [
        ("DATA_LAKE_URI", "s3://bucket/lake"),
        ("DATA_LAKE_URI", "/abs/path/no/scheme"),
        ("DATA_LAKE_URI", "file://"),
        ("REDIS_TTL_AFTER_SESSION_S", "0"),
        ("REDIS_TTL_AFTER_SESSION_S", "-5"),
        ("JOLPICA_MAX_PER_HOUR", "0"),
        ("JOLPICA_MAX_PER_HOUR", str(JOLPICA_HARD_LIMIT_PER_HOUR + 1)),
        ("REDIS_URL", "http://localhost:6379"),
        ("REDIS_URL", "redis://"),
        ("KAFKA_BOOTSTRAP", ""),
        ("KAFKA_BOOTSTRAP", "localhost"),
        ("KAFKA_BOOTSTRAP", "localhost:9092,"),
        ("KAFKA_BOOTSTRAP", "localhost:99999"),
        ("JOLPICA_BASE_URL", "ftp://api.jolpi.ca"),
        ("KAFKA_TOPIC_EVENTS", ""),
        ("HDFS_HTTPFS_URL", "localhost:14000"),
        ("HDFS_HTTPFS_URL", "http://localhost:14000/webhdfs/v1"),
        ("HDFS_USER", "Root User"),
        ("FASTF1_MAX_RACES_PER_HOUR", "0"),
        ("FASTF1_MAX_RACES_PER_HOUR", "61"),
        ("JOLPICA_USER_AGENT", "   "),
        ("JOLPICA_USER_AGENT", "a\nb"),
        ("KAFKA_TOPIC_EVENTS", "bad topic"),
        ("KAFKA_TOPIC_EVENTS", ".."),
        ("KAFKA_TOPIC_EVENTS", "x" * 250),
    ],
)
def test_invalid_values_rejected(monkeypatch: pytest.MonkeyPatch, var: str, value: str) -> None:
    monkeypatch.setenv(var, value)
    with pytest.raises(ValidationError):
        Settings()


def test_jolpica_limit_boundary_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JOLPICA_MAX_PER_HOUR", str(JOLPICA_HARD_LIMIT_PER_HOUR))
    assert Settings().jolpica_max_per_hour == JOLPICA_HARD_LIMIT_PER_HOUR


@pytest.mark.parametrize("present", ["F1_USERNAME", "F1_PASSWORD"])
def test_credentials_must_be_paired(monkeypatch: pytest.MonkeyPatch, present: str) -> None:
    monkeypatch.setenv(present, "someone")
    with pytest.raises(ValidationError, match="both F1_USERNAME and F1_PASSWORD"):
        Settings()


def test_blank_credentials_treated_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("F1_USERNAME", "")
    monkeypatch.setenv("F1_PASSWORD", "  ")
    s = Settings()
    assert s.f1_username is None
    assert s.has_f1_credentials is False


def test_credentials_are_masked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("F1_USERNAME", "driver@example.com")
    monkeypatch.setenv("F1_PASSWORD", "hunter2-not-real")
    s = Settings()
    assert s.has_f1_credentials is True
    assert s.f1_password is not None
    assert s.f1_password.get_secret_value() == "hunter2-not-real"
    for text in (repr(s), str(s), s.model_dump_json()):
        assert "hunter2-not-real" not in text
        assert "driver@example.com" not in text


def test_get_settings_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    first = get_settings()
    assert get_settings() is first
    monkeypatch.setenv("REDIS_TTL_AFTER_SESSION_S", "99")
    assert get_settings().redis_ttl_after_session_s == 3600
    get_settings.cache_clear()
    assert get_settings().redis_ttl_after_session_s == 99
