"""Server-Sent Events from a race's Pub/Sub channel (``GET /api/races/{s}/{r}/events``).

Framing (one frame per Redis message, UTF-8)::

    event: <type>             lap | pit_in | pit_out | undercut_alert | session_end
    id: <event_id>
    data: <the stored JSON message + "event_id" + "sse_sent_at_ms">

* The stream subscribes first, then sends ``retry: 3000`` and a ``: connected`` comment, so
  nothing published after the client sees ``connected`` is missed.
* ``event_id`` is ``payload.event_id`` for ``lap``, ``pit_in`` and ``pit_out`` (the producer's
  uuid4). ``undercut_alert`` and ``session_end`` carry no id in the Step 12 payload, so they get
  a deterministic ``uuid5`` of the raw message: the same message always gets the same id.
* A ``: heartbeat`` comment is sent every ``heartbeat_s`` seconds (15 by default), whatever the
  traffic.
* Messages that are not JSON, have an unknown type or belong to another race are skipped and
  counted.
* After ``session_end`` the stream ends. Pub/Sub keeps no history, so ``Last-Event-ID`` cannot
  be honoured and is ignored; ``/state`` gives the current state after a reconnect.

Clean-up: Starlette does not close the body iterator when the client goes away. So
``SSEResponse`` closes the subscription itself, in a ``finally`` shielded from cancellation. That
covers ASGI spec < 2.4 (disconnect cancels the stream), >= 2.4 (``send`` raises ``OSError``) and
the normal end after ``session_end``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any

import anyio
import redis.asyncio as aioredis
from redis.exceptions import RedisError
from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from f1sa.serving import keys as K
from f1sa.streaming.redis_sink import now_ms

log = logging.getLogger(__name__)

PREAMBLE = b"retry: 3000\n: connected\n\n"
HEARTBEAT = b": heartbeat\n\n"
IDLE_BACKOFF_S = 0.02
SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "f1sa:race-events")
"""Namespace of the derived ids of messages without ``payload.event_id``."""
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

OnSent = Callable[[str, Mapping[str, Any]], None]


def event_id_for(raw: str, message: Mapping[str, Any]) -> str:
    """``payload.event_id`` when it is a canonical uuid, else ``uuid5(ID_NAMESPACE, raw)``."""
    payload = message.get("payload")
    if isinstance(payload, Mapping):
        eid = payload.get("event_id")
        if isinstance(eid, str) and _UUID.fullmatch(eid):
            return eid
    return str(uuid.uuid5(ID_NAMESPACE, raw))


def format_event(name: str, event_id: str, data: Mapping[str, Any]) -> bytes:
    """One SSE frame; ``json.dumps`` never emits a newline, so ``data`` is a single line."""
    if name not in K.EVENT_TYPES or "\n" in event_id or "\r" in event_id:
        raise ValueError(f"invalid SSE event {name!r} / id {event_id!r}")
    body = json.dumps(data, allow_nan=False, separators=(",", ":"), sort_keys=True)
    return f"event: {name}\nid: {event_id}\ndata: {body}\n\n".encode()


class EventStream:
    """One client's subscription to ``race:{s}:{r}:events``."""

    def __init__(
        self,
        client: aioredis.Redis,
        season: int,
        round_: int,
        *,
        heartbeat_s: float,
        on_sent: OnSent | None = None,
        clock_ms: Callable[[], int] = now_ms,
    ) -> None:
        if not heartbeat_s > 0:
            raise ValueError(f"heartbeat_s must be > 0, got {heartbeat_s}")
        self.client = client
        self.season = K.validate_season(season)
        self.round = K.validate_round(round_)
        self.channel = K.events_channel(season, round_)
        self.heartbeat_s = heartbeat_s
        self.on_sent = on_sent
        self.clock_ms = clock_ms
        self.pubsub: Any = None
        self.closed = False
        self.sent = 0
        self.skipped = 0
        self.heartbeats = 0

    def frame(self, data: str | bytes) -> tuple[bytes, str] | None:
        """``(frame, type)`` for one Pub/Sub message body, or ``None`` if it is skipped."""
        raw = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else data
        try:
            msg = json.loads(raw)
        except ValueError:
            msg = None
        if (
            not isinstance(msg, dict)
            or msg.get("type") not in K.EVENT_TYPES
            or msg.get("season") != self.season
            or msg.get("round") != self.round
        ):
            self.skipped += 1
            log.warning("skipped message", extra={"channel": self.channel, "skipped": self.skipped})
            return None
        eid = event_id_for(raw, msg)
        out = {**msg, "event_id": eid, "sse_sent_at_ms": self.clock_ms()}
        try:
            frame = format_event(msg["type"], eid, out)
        except ValueError:
            self.skipped += 1
            log.warning("skipped message", extra={"channel": self.channel, "skipped": self.skipped})
            return None
        if self.on_sent is not None:
            self.on_sent(eid, out)
        return frame, msg["type"]

    async def frames(self) -> AsyncIterator[bytes]:
        """Subscribe, then yield the preamble, event frames and heartbeats until ``session_end``."""
        self.pubsub = self.client.pubsub(ignore_subscribe_messages=True)
        try:
            await self.pubsub.subscribe(self.channel)
            yield PREAMBLE
            loop = asyncio.get_running_loop()
            next_beat = loop.time() + self.heartbeat_s
            while True:
                remaining = next_beat - loop.time()
                if remaining <= 0:
                    self.heartbeats += 1
                    yield HEARTBEAT
                    next_beat += self.heartbeat_s
                    continue
                msg = await self.pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=remaining
                )
                if msg is None or msg.get("type") != "message":
                    # get_message returns early for subscribe confirmations (and some clients
                    # do not block at all): back off briefly instead of spinning.
                    await asyncio.sleep(min(IDLE_BACKOFF_S, max(next_beat - loop.time(), 0.0)))
                    continue
                framed = self.frame(msg["data"])
                if framed is None:
                    continue
                self.sent += 1
                yield framed[0]
                if framed[1] == "session_end":
                    return
        finally:
            await self.close()

    async def close(self) -> None:
        """Unsubscribe and release the connection; idempotent, shielded from cancellation."""
        if self.closed:
            return
        self.closed = True
        pubsub, self.pubsub = self.pubsub, None
        if pubsub is None:
            return
        with anyio.CancelScope(shield=True):
            try:
                await pubsub.unsubscribe(self.channel)
            except (RedisError, OSError) as exc:
                log.warning("unsubscribe failed", extra={"exc_class": type(exc).__name__})
            try:
                await pubsub.aclose()
            except (RedisError, OSError) as exc:
                log.warning("pubsub close failed", extra={"exc_class": type(exc).__name__})
        log.info(
            "event stream closed",
            extra={
                "channel": self.channel,
                "sent": self.sent,
                "skipped": self.skipped,
                "heartbeats": self.heartbeats,
            },
        )


class SSEResponse(StreamingResponse):
    """A ``text/event-stream`` response that always closes its ``EventStream``."""

    media_type = "text/event-stream"

    def __init__(self, stream: EventStream, status_code: int = 200) -> None:
        super().__init__(
            stream.frames(), status_code, headers=SSE_HEADERS, media_type=self.media_type
        )
        self.stream = stream

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.stream.close()
