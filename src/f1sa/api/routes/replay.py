from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from f1sa.api.catalog import CatalogError
from f1sa.api.deps import Deps, Race, require_replay
from f1sa.api.models import ErrorOut, ReplayIn, ReplayOut, ReplayStatusOut
from f1sa.api.replay import ReplayConflictError, ReplayRefusedError
from f1sa.streaming.processor import SEALED_SEASON

log = logging.getLogger(__name__)
router = APIRouter(tags=["replay"])


@router.post(
    "/races/{season}/{round}/replay",
    status_code=202,
    response_model=ReplayOut,
    dependencies=[Depends(require_replay)],
    responses={
        403: {"model": ErrorOut, "description": "ENABLE_REPLAY is off, or the sealed season"},
        404: {"model": ErrorOut, "description": "race not in the clean zone"},
        409: {"model": ErrorOut, "description": "already running, or the replay limit"},
        500: {"model": ErrorOut, "description": "the producer could not be started"},
        503: {"model": ErrorOut, "description": "clean zone unavailable"},
    },
)
async def start_replay(body: ReplayIn, race: Race, deps: Deps) -> ReplayOut:
    """Replay the race through the whole pipeline (stream job, then producer) in the background
    (``ENABLE_REPLAY=1``); progress and errors in ``GET /api/replays``."""
    s, r = race.season, race.round
    if s == SEALED_SEASON and not deps.settings.allow_test_season:
        raise HTTPException(403, f"season {s} is the sealed test season")
    try:
        info = await deps.catalog.get(s, r)
    except CatalogError as exc:
        log.error("race catalog unavailable", extra={"reason": str(exc)})
        raise HTTPException(503, "clean zone unavailable") from exc
    if info is None:
        raise HTTPException(404, f"{s} R{r} is not in the clean zone")
    try:
        job = await deps.replays.start(s, r, body.speed)
    except ReplayRefusedError as exc:
        raise HTTPException(403, str(exc)) from exc
    except ReplayConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except OSError as exc:
        log.error("replay launch failed", extra={"season": s, "exc_class": type(exc).__name__})
        raise HTTPException(500, "the producer could not be started") from exc
    return ReplayOut(**job.to_dict())


@router.get("/replays", response_model=ReplayStatusOut)
async def replay_status(deps: Deps) -> ReplayStatusOut:
    """Running replays and the last finished or failed ones (with their error)."""
    st = deps.replays.status()
    return ReplayStatusOut(
        enabled=deps.settings.enable_replay,
        max_running=st["max_running"],
        running=[ReplayOut(**j) for j in st["running"]],
        recent=[ReplayOut(**j) for j in st["recent"]],
    )
