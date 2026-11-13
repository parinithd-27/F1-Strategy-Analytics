"""The FastAPI application (#25, API half). ``create_app`` builds everything eagerly; the
lifespan only shuts down (running replays, the Redis pool).

Routes, all under ``/api`` (OpenAPI at ``/api/docs`` and ``/api/openapi.json``)::

    GET  /api/health                         Redis, Kafka, deployed model (200 / 503)
    GET  /api/races                          replayable races from the clean zone
    GET  /api/races/{season}/{round}/state   driver hashes + order (the Redis contract)
    GET  /api/races/{season}/{round}/events  Server-Sent Events from the race channel
    POST /api/races/{season}/{round}/replay  start the producer (ENABLE_REPLAY=1)
    POST /api/debug/latency                  UI render time per event (ENABLE_DEBUG=1)
    POST /api/races/{season}/{round}/whatif  pit now / pit next lap / stay out (Step 15)
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from f1sa import __version__
from f1sa.api.deps import ApiDeps
from f1sa.api.log import AccessLogMiddleware
from f1sa.api.routes import debug, events, health, races, replay, whatif
from f1sa.config import Settings, get_settings

log = logging.getLogger(__name__)

ROUTERS = (
    health.router,
    races.router,
    events.router,
    replay.router,
    debug.router,
    whatif.router,
)


def create_app(settings: Settings | None = None, *, deps: ApiDeps | None = None) -> FastAPI:
    settings = settings or get_settings()
    deps = deps or ApiDeps.from_settings(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        await deps.replays.shutdown()
        await deps.lap_logs.aclose()
        await deps.redis.aclose()

    app = FastAPI(
        title="F1 Strategy Analytics API",
        version=__version__,
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    app.state.deps = deps
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.api_cors_origins),
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
        allow_credentials=False,
    )
    app.add_middleware(AccessLogMiddleware)

    @app.exception_handler(RedisError)
    async def redis_unavailable(_request: Request, exc: RedisError) -> JSONResponse:
        log.error("redis error", extra={"exc_class": type(exc).__name__})
        return JSONResponse({"detail": "Redis unavailable"}, status_code=503)

    for router in ROUTERS:
        app.include_router(router, prefix="/api")
    return app
