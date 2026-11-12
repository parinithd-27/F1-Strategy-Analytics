from __future__ import annotations

from fastapi import APIRouter

from f1sa.api.deps import Deps, Race
from f1sa.api.sse import EventStream, SSEResponse

router = APIRouter(tags=["events"])


@router.get(
    "/races/{season}/{round}/events",
    response_class=SSEResponse,
    responses={
        200: {
            "content": {"text/event-stream": {}},
            "description": "event: <type>, id: <event_id>, data: JSON with sse_sent_at_ms; "
            "': heartbeat' every API_SSE_HEARTBEAT_S; ends after session_end",
        }
    },
)
async def race_events(race: Race, deps: Deps) -> SSEResponse:
    """Server-Sent Events from ``race:{season}:{round}:events``."""
    stream = EventStream(
        deps.redis,
        race.season,
        race.round,
        heartbeat_s=deps.settings.api_sse_heartbeat_s,
        on_sent=deps.latency.remember,
    )
    return SSEResponse(stream)
