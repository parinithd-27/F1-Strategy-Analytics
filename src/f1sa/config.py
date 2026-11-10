"""Central runtime configuration.

All storage locations, service endpoints and limits are read from here. Values come from
environment variables (no prefix, case-insensitive) or a local ``.env`` file. Other modules must
never hardcode paths or endpoints; they call :func:`get_settings`.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Self
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

JOLPICA_HARD_LIMIT_PER_HOUR = 500
"""Jolpica's published per-hour request limit; the configured budget may not exceed it."""

DEFAULT_USER_AGENT = "F1-Strategy-Analytics/0.1.0 (IISc academic project; python-requests)"

_LAKE_SCHEMES = frozenset({"file", "hdfs"})
_REDIS_SCHEMES = frozenset({"redis", "rediss"})
_HTTP_SCHEMES = frozenset({"http", "https"})
_HOST_PORT = re.compile(r"^[A-Za-z0-9.\-]+:(\d{1,5})$")
_KAFKA_TOPIC = re.compile(r"^[A-Za-z0-9._\-]{1,249}$")


class Settings(BaseSettings):
    """Typed application settings loaded from the environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    data_lake_uri: str = "file://./data/lake"
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic_events: str = "f1.race.events"
    redis_url: str = "redis://localhost:6379/0"
    redis_ttl_after_session_s: int = Field(default=3600, gt=0)
    jolpica_base_url: str = "https://api.jolpi.ca/ergast/f1"
    jolpica_max_per_hour: int = Field(default=450, ge=1, le=JOLPICA_HARD_LIMIT_PER_HOUR)
    jolpica_cache_dir: Path = Path("data/cache/jolpica")
    jolpica_user_agent: str = DEFAULT_USER_AGENT
    fastf1_cache_dir: Path = Path("data/cache/fastf1")
    hdfs_httpfs_url: str = "http://localhost:14000"
    hdfs_user: str = "hadoop"
    manifest_path: Path = Path("data/manifest.json")
    sample_dir: Path = Path("data/sample")
    fastf1_max_races_per_hour: int = Field(default=20, ge=1, le=60)
    f1_username: SecretStr | None = None
    f1_password: SecretStr | None = None

    @field_validator("data_lake_uri")
    @classmethod
    def _check_lake_uri(cls, value: str) -> str:
        value = value.strip()
        scheme = urlparse(value).scheme
        if scheme not in _LAKE_SCHEMES or "://" not in value:
            raise ValueError(f"data_lake_uri must start with file:// or hdfs://, got {value!r}")
        location = value.split("://", 1)[1].rstrip("/")
        if not location:
            raise ValueError(f"data_lake_uri has no path: {value!r}")
        return f"{scheme}://{location}"

    @field_validator("kafka_bootstrap")
    @classmethod
    def _check_kafka_bootstrap(cls, value: str) -> str:
        servers = [part.strip() for part in value.split(",")]
        if not servers or any(not s for s in servers):
            raise ValueError("kafka_bootstrap must be a non-empty comma-separated host:port list")
        for server in servers:
            match = _HOST_PORT.match(server)
            if match is None or not 1 <= int(match.group(1)) <= 65535:
                raise ValueError(f"invalid kafka bootstrap server {server!r}; expected host:port")
        return ",".join(servers)

    @field_validator("kafka_topic_events")
    @classmethod
    def _check_kafka_topic(cls, value: str) -> str:
        value = value.strip()
        if not _KAFKA_TOPIC.match(value) or value in {".", ".."}:
            raise ValueError(
                f"invalid Kafka topic name {value!r}; use 1-249 chars of [A-Za-z0-9._-]"
            )
        return value

    @field_validator("redis_url")
    @classmethod
    def _check_redis_url(cls, value: str) -> str:
        parsed = urlparse(value.strip())
        if parsed.scheme not in _REDIS_SCHEMES or not parsed.hostname:
            raise ValueError(
                f"redis_url must be redis://host[:port][/db] or rediss://..., got {value!r}"
            )
        return value.strip()

    @field_validator("jolpica_base_url")
    @classmethod
    def _check_jolpica_url(cls, value: str) -> str:
        parsed = urlparse(value.strip())
        if parsed.scheme not in _HTTP_SCHEMES or not parsed.netloc:
            raise ValueError(f"jolpica_base_url must be an http(s) URL, got {value!r}")
        return value.strip().rstrip("/")

    @field_validator("hdfs_httpfs_url")
    @classmethod
    def _check_httpfs_url(cls, value: str) -> str:
        parsed = urlparse(value.strip())
        if parsed.scheme not in _HTTP_SCHEMES or not parsed.netloc or parsed.path not in ("", "/"):
            raise ValueError(f"hdfs_httpfs_url must be http(s)://host:port, got {value!r}")
        return value.strip().rstrip("/")

    @field_validator("hdfs_user")
    @classmethod
    def _check_hdfs_user(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", value.strip()):
            raise ValueError(f"invalid hdfs_user {value!r}")
        return value.strip()

    @field_validator("jolpica_user_agent")
    @classmethod
    def _check_user_agent(cls, value: str) -> str:
        value = value.strip()
        if not value or any(c in value for c in "\r\n"):
            raise ValueError("jolpica_user_agent must be a non-empty single line")
        return value

    @field_validator("f1_username", "f1_password", mode="before")
    @classmethod
    def _blank_secret_is_none(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @model_validator(mode="after")
    def _check_f1_credentials_pair(self) -> Self:
        if (self.f1_username is None) != (self.f1_password is None):
            raise ValueError("set both F1_USERNAME and F1_PASSWORD, or neither")
        return self

    @property
    def has_f1_credentials(self) -> bool:
        """True when an F1 TV Access login is configured for FastF1."""
        return self.f1_username is not None and self.f1_password is not None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings instance (cached; call ``cache_clear()`` in tests)."""
    return Settings()
