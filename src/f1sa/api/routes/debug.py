from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from f1sa.api.deps import Deps, require_debug
from f1sa.api.latency import AlreadyRecordedError, UnknownEventError
from f1sa.api.models import ErrorOut, LatencyIn, LatencyOut

router = APIRouter(tags=["debug"])


@router.post(
    "/debug/latency",
    status_code=201,
    response_model=LatencyOut,
    dependencies=[Depends(require_debug)],
    responses={
        403: {"model": ErrorOut, "description": "ENABLE_DEBUG is off"},
        404: {"model": ErrorOut, "description": "event id not sent by this API"},
        409: {"model": ErrorOut, "description": "event id already recorded"},
    },
)
async def ui_latency(body: LatencyIn, deps: Deps) -> LatencyOut:
    """Append one UI render time to ``results/ui_latency.csv`` (``ENABLE_DEBUG=1``)."""
    try:
        row = await deps.latency.record(body.event_id, body.ui_rendered_at_ms)
    except UnknownEventError as exc:
        raise HTTPException(404, "event id not sent by this API") from exc
    except AlreadyRecordedError as exc:
        raise HTTPException(409, "event id already recorded") from exc
    return LatencyOut(event_id=body.event_id, event_type=row.get("event_type"))
