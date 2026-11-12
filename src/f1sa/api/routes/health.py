from __future__ import annotations

import asyncio

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from f1sa.api.deps import Deps
from f1sa.api.health import check_redis, run_kafka_check
from f1sa.api.models import CheckOut, HealthChecks, HealthOut, ModelCheckOut

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthOut,
    responses={503: {"model": HealthOut, "description": "at least one check failed"}},
)
async def health(deps: Deps) -> JSONResponse:
    """Redis PING, the Kafka events topic and the deployed model; 503 names the failed check."""
    timeout = deps.settings.api_kafka_timeout_s
    redis_res, kafka_res = await asyncio.gather(
        check_redis(deps.redis, timeout), run_kafka_check(deps.kafka_check, timeout)
    )
    m = deps.model
    body = HealthOut(
        status="ok" if redis_res.ok and kafka_res.ok and m.ok else "degraded",
        checks=HealthChecks(
            redis=CheckOut(ok=redis_res.ok, error=redis_res.error),
            kafka=CheckOut(ok=kafka_res.ok, error=kafka_res.error),
            model=ModelCheckOut(
                ok=m.ok,
                error=m.error,
                model_version=m.model_version,
                git_sha=m.git_sha,
                git_dirty=m.git_dirty,
                scorer_sha256=m.scorer_sha256,
                created_utc=m.created_utc,
                feature_set=m.feature_set,
                g=m.g,
                k=m.k,
            ),
        ),
    )
    return JSONResponse(body.model_dump(), status_code=200 if body.status == "ok" else 503)
