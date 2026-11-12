"""Health checks for ``GET /api/health``: Redis ping, Kafka topic reachable, deployed model.

The model is loaded once at start-up with the stream job's own loader
(``streaming.stream_job.load_model``: feature set ``real``, G = 3.5, k = 2, scorer inputs equal
to the metadata features), so the API reports exactly the model the detector scores with.
Failures are reported by exception class and a fixed text; no URL, host or credential is put
in a response.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

from f1sa.config import Settings
from f1sa.streaming.stream_job import JobError, load_model

log = logging.getLogger(__name__)

KafkaCheck = Callable[[], Awaitable[None]]


class KafkaUnavailableError(RuntimeError):
    """The events topic could not be described by the broker."""


@dataclass(frozen=True)
class CheckResult:
    ok: bool
    error: str | None = None


@dataclass(frozen=True)
class ModelStatus:
    """What ``/api/health`` reports about the deployed model."""

    ok: bool
    model_version: str
    error: str | None = None
    git_sha: str | None = None
    git_dirty: bool | None = None
    scorer_sha256: str | None = None
    created_utc: str | None = None
    feature_set: str | None = None
    g: float | None = None
    k: int | None = None

    @classmethod
    def load(cls, model_dir: Path) -> ModelStatus:
        """Never raises: a model that cannot be loaded gives ``ok=False`` and the reason."""
        version = model_dir.name
        try:
            _scorer, _params, meta = load_model(model_dir)
            sha = hashlib.sha256((model_dir / "lr" / "scorer.json").read_bytes()).hexdigest()
        except JobError as exc:
            log.warning("deployment model not loaded", extra={"reason": str(exc)})
            return cls(ok=False, model_version=version, error=f"JobError: {exc}")
        except Exception as exc:
            log.warning("deployment model not loaded", exc_info=True)
            return cls(
                ok=False,
                model_version=version,
                error=f"{type(exc).__name__}: deployment model files are not readable",
            )
        return cls(
            ok=True,
            model_version=version,
            git_sha=_opt_str(meta.get("git_sha")),
            git_dirty=meta.get("git_dirty") if isinstance(meta.get("git_dirty"), bool) else None,
            scorer_sha256=sha,
            created_utc=_opt_str(meta.get("created_utc")),
            feature_set=_opt_str(meta.get("feature_set")),
            g=float(meta["g"]),
            k=int(meta["k"]),
        )


def _opt_str(value: Any) -> str | None:
    return None if value is None else str(value)


async def check_redis(client: aioredis.Redis, timeout_s: float) -> CheckResult:
    try:
        await asyncio.wait_for(client.ping(), timeout_s)
    except Exception as exc:
        log.warning("redis check failed", extra={"exc_class": type(exc).__name__})
        return CheckResult(False, f"{type(exc).__name__}: Redis did not answer PING")
    return CheckResult(True)


async def run_kafka_check(check: KafkaCheck, timeout_s: float) -> CheckResult:
    try:
        await asyncio.wait_for(check(), timeout_s + 1.0)
    except Exception as exc:
        log.warning("kafka check failed", extra={"exc_class": type(exc).__name__})
        return CheckResult(False, f"{type(exc).__name__}: Kafka events topic not reachable")
    return CheckResult(True)


def describe_topic(bootstrap: str, topic: str, timeout_s: float) -> int:
    """Partition count of ``topic``; raises ``KafkaUnavailableError`` otherwise."""
    from confluent_kafka.admin import AdminClient

    try:
        meta = AdminClient({"bootstrap.servers": bootstrap}).list_topics(topic, timeout=timeout_s)
    except Exception as exc:
        raise KafkaUnavailableError(type(exc).__name__) from exc
    t = meta.topics.get(topic)
    if t is None or t.error is not None or not t.partitions:
        raise KafkaUnavailableError("topic missing or without partitions")
    return len(t.partitions)


def kafka_check_for(settings: Settings) -> KafkaCheck:
    """The production check: describe ``kafka_topic_events`` in a worker thread."""

    async def check() -> None:
        await asyncio.to_thread(
            describe_topic,
            settings.kafka_bootstrap,
            settings.kafka_topic_events,
            settings.api_kafka_timeout_s,
        )

    return check
