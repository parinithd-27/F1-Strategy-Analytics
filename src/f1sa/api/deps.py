"""Shared objects of one API instance and the FastAPI dependencies that hand them out."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated

import redis.asyncio as aioredis
import requests
from fastapi import Depends, HTTPException, Path, Request
from fastapi.exceptions import RequestValidationError

from f1sa.api.catalog import LakeReader, RaceCatalog, SimReaders, reader_for, sim_readers
from f1sa.api.health import KafkaCheck, ModelStatus, kafka_check_for
from f1sa.api.lap_log import LapLogManager
from f1sa.api.latency import LatencyRecorder
from f1sa.api.replay import Launcher, ReplayManager, subprocess_launcher
from f1sa.config import Settings
from f1sa.serving import keys as K
from f1sa.serving.redis_store import AsyncRaceStore
from f1sa.sim.whatif import WhatIfService


@dataclass
class ApiDeps:
    settings: Settings
    redis: aioredis.Redis
    store: AsyncRaceStore
    model: ModelStatus
    catalog: RaceCatalog
    replays: ReplayManager
    latency: LatencyRecorder
    kafka_check: KafkaCheck
    whatif: WhatIfService
    lap_logs: LapLogManager

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        redis: aioredis.Redis | None = None,
        kafka_check: KafkaCheck | None = None,
        launcher: Launcher = subprocess_launcher,
        reader_factory: Callable[[], LakeReader] | None = None,
        http_session: requests.Session | None = None,
        model: ModelStatus | None = None,
        sim_reader_factory: Callable[[], SimReaders] | None = None,
    ) -> ApiDeps:
        client = redis or aioredis.Redis.from_url(settings.redis_url, decode_responses=True)
        sim_factory = sim_reader_factory or (lambda: sim_readers(settings, session=http_session))

        def readers() -> tuple[LakeReader, LakeReader]:
            r = sim_factory()
            return r.clean, r.features

        return cls(
            settings=settings,
            redis=client,
            store=AsyncRaceStore(client, settings),
            model=model or ModelStatus.load(settings.api_model_dir),
            catalog=RaceCatalog(
                reader_factory or (lambda: reader_for(settings, session=http_session)),
                allow_test_season=settings.allow_test_season,
                ttl_s=settings.api_races_cache_s,
            ),
            replays=ReplayManager(
                launcher,
                max_running=settings.api_max_replays,
                allow_test_season=settings.allow_test_season,
                demo_dir=settings.demo_dir,
                ready_timeout_s=settings.api_replay_ready_timeout_s,
                end_timeout_s=settings.api_replay_end_timeout_s,
                model_dir=settings.api_model_dir,
            ),
            latency=LatencyRecorder(settings.api_ui_latency_csv, enabled=settings.enable_debug),
            kafka_check=kafka_check or kafka_check_for(settings),
            whatif=WhatIfService(readers=readers, sim_dir=settings.api_sim_dir),
            lap_logs=LapLogManager(client),
        )


def get_deps(request: Request) -> ApiDeps:
    deps: ApiDeps = request.app.state.deps
    return deps


Deps = Annotated[ApiDeps, Depends(get_deps)]


@dataclass(frozen=True)
class RaceRef:
    season: int
    round: int


def race_ref(
    season: Annotated[int, Path(ge=K.SEASON_MIN, le=K.SEASON_MAX, description="e.g. 2024")],
    round: Annotated[int, Path(ge=K.ROUND_MIN, le=K.ROUND_MAX, description="e.g. 5")],
) -> RaceRef:
    """Path parameters checked with the Redis key rules (``serving.keys``); 422 otherwise."""
    for name, value, check in (
        ("season", season, K.validate_season),
        ("round", round, K.validate_round),
    ):
        try:
            check(value)
        except K.InvalidKeyError as exc:
            raise RequestValidationError(
                [{"type": "value_error", "loc": ("path", name), "msg": str(exc), "input": value}]
            ) from exc
    return RaceRef(season, round)


Race = Annotated[RaceRef, Depends(race_ref)]


def require_replay(deps: Deps) -> None:
    if not deps.settings.enable_replay:
        raise HTTPException(403, "replay is disabled (set ENABLE_REPLAY=1)")


def require_debug(deps: Deps) -> None:
    if not deps.settings.enable_debug:
        raise HTTPException(403, "debug endpoints are disabled (set ENABLE_DEBUG=1)")
