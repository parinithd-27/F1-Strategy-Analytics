"""Central runtime configuration.

All storage locations, service endpoints and limits are read from here. Values come from
environment variables (no prefix, case-insensitive) or a local ``.env`` file. Other modules must
never hardcode paths or endpoints; they call :func:`get_settings`.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Self
from urllib.parse import urlparse

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

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
    redis_safety_ttl_s: int = Field(default=21600, gt=0)
    """TTL set on every race-key write (6 h, longer than any race including red flags), so keys of a
    session that never sends ``session_end`` still expire. ``session_end`` then sets the shorter
    ``redis_ttl_after_session_s``."""
    jolpica_base_url: str = "https://api.jolpi.ca/ergast/f1"
    jolpica_max_per_hour: int = Field(default=450, ge=1, le=JOLPICA_HARD_LIMIT_PER_HOUR)
    jolpica_cache_dir: Path = Path("data/cache/jolpica")
    jolpica_user_agent: str = DEFAULT_USER_AGENT
    fastf1_cache_dir: Path = Path("data/cache/fastf1")
    hdfs_httpfs_url: str = "http://localhost:14000"
    hdfs_user: str = "hadoop"
    hdfs_replication: int = Field(default=1, ge=1, le=3)
    """Block replication for files we write to HDFS (Spark jobs and HttpFS). The dev cluster
    has one datanode, so the Hadoop client default of 3 leaves every block under-replicated."""
    manifest_path: Path = Path("data/manifest.json")
    sample_dir: Path = Path("data/sample")
    fastf1_max_races_per_hour: int = Field(default=20, ge=1, le=60)
    tracinginsights_cache_dir: Path = Path("data/cache/tracinginsights")
    tracinginsights_manifest_path: Path = Path("data/manifest_tracinginsights.json")
    ti_disk_guard_path: Path = Path("/mnt/c")
    """Free space is checked here: on WSL the Windows drive backs the Docker/WSL disks, while the
    WSL filesystem itself reports its virtual size."""
    ti_min_free_gb: float = Field(default=20.0, gt=0)
    f1_username: SecretStr | None = None
    f1_password: SecretStr | None = None

    # ---------------------------------------------------------------- API (Step 13)
    api_host: str = "127.0.0.1"
    """Bind address of ``python -m f1sa.api``; only the compose service sets 0.0.0.0 (inside its
    container, published on the host's 127.0.0.1 only)."""
    api_port: int = Field(default=8000, ge=1, le=65535)
    api_cors_origins: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["http://localhost:3000", "http://127.0.0.1:3000"]
    )
    """Browser origins allowed by CORS (the Next.js dashboard, by both of its local names).
    ``API_CORS_ORIGINS`` is a comma-separated list; ``*`` is refused."""
    enable_replay: bool = False
    """``ENABLE_REPLAY=1`` enables ``POST /api/races/{s}/{r}/replay``."""
    enable_debug: bool = False
    """``ENABLE_DEBUG=1`` enables ``POST /api/debug/latency``."""
    allow_test_season: bool = False
    """``ALLOW_TEST_SEASON=1`` lists and replays the sealed test season (2025) in the API."""
    api_max_replays: int = Field(default=1, ge=1, le=4)
    """Replays running at the same time (each starts a Spark JVM in the API container)."""
    api_sse_heartbeat_s: float = Field(default=15.0, gt=0, le=300)
    api_races_cache_s: float = Field(default=300.0, ge=0)
    api_kafka_timeout_s: float = Field(default=2.0, gt=0, le=60)
    api_model_dir: Path = Path("models/undercut/deploy_real_G3.5_k2")
    """The deployed model reported by ``/api/health`` (the stream job's default model)."""
    api_ui_latency_csv: Path = Path("results/ui_latency.csv")
    api_replay_ready_timeout_s: float = Field(default=60.0, gt=0, le=600)
    """A replay's stream job must be ready (first trigger done) within this; else it is killed."""
    api_replay_end_timeout_s: float = Field(default=180.0, gt=0, le=3600)
    """After the producer exits, the stream job must stop (session_end processed) within this."""
    demo_dir: Path = Path("data/demo")
    """Outputs of demo runs (``make stream-demo``, API replays): ``data/demo/<run_id>/``. Only
    ``RECORD=1`` writes to ``results/``."""

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

    @field_validator("api_cors_origins", mode="before")
    @classmethod
    def _check_cors_origins(cls, value: Any) -> list[str]:
        items = value.split(",") if isinstance(value, str) else value
        if not isinstance(items, list | tuple):
            raise ValueError("api_cors_origins must be a comma-separated list of origins")
        out: list[str] = []
        for raw in items:
            origin = str(raw).strip().rstrip("/")
            parsed = urlparse(origin)
            if (
                parsed.scheme not in _HTTP_SCHEMES
                or not parsed.hostname
                or parsed.path
                or parsed.query
                or parsed.fragment
                or "*" in origin
            ):
                raise ValueError(
                    f"api_cors_origins entries must be http(s)://host[:port] origins, got {raw!r}"
                )
            if origin not in out:
                out.append(origin)
        if not out:
            raise ValueError("api_cors_origins must name at least one origin")
        return out

    @field_validator("api_host")
    @classmethod
    def _check_api_host(cls, value: str) -> str:
        value = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9.\-:]+", value):
            raise ValueError(f"invalid api_host {value!r}")
        return value

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
