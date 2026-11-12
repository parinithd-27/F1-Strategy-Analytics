"""UI latency samples (``POST /api/debug/latency``, only with ``ENABLE_DEBUG=1``).

While debug is on, the SSE stream remembers the timings of every event it sends (bounded, newest
10,000). The dashboard posts ``{event_id, ui_rendered_at_ms}``. The row appended to
``results/ui_latency.csv`` joins that with the server-side timings of the event as it was sent,
so the client cannot supply them. Columns, in order:

``event_id, event_type, season, round, produced_at_ms, batch_received_at_ms,
redis_written_at_ms, sse_sent_at_ms, ui_rendered_at_ms, recorded_at_ms``

``produced_at_ms`` comes from the producer, ``batch_received_at_ms`` / ``redis_written_at_ms``
from the stream job (``payload``), ``sse_sent_at_ms`` from this API, ``ui_rendered_at_ms`` from
the browser and ``recorded_at_ms`` from this API when the sample arrives. Every timing is epoch
ms and empty when the event does not carry it (for example ``session_end``).
"""

from __future__ import annotations

import asyncio
import csv
from collections import OrderedDict
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from f1sa.streaming.redis_sink import now_ms

CSV_COLUMNS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "season",
    "round",
    "produced_at_ms",
    "batch_received_at_ms",
    "redis_written_at_ms",
    "sse_sent_at_ms",
    "ui_rendered_at_ms",
    "recorded_at_ms",
)
_PAYLOAD_TIMINGS = ("produced_at_ms", "batch_received_at_ms", "redis_written_at_ms")


class UnknownEventError(KeyError):
    """An event id this API process has not sent (or has already forgotten)."""


class AlreadyRecordedError(ValueError):
    """A sample for this event id was already appended."""


def _ms(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


class LatencyRecorder:
    def __init__(
        self,
        path: Path,
        *,
        enabled: bool,
        capacity: int = 10_000,
        clock_ms: Callable[[], int] = now_ms,
    ) -> None:
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        self.path = path
        self.enabled = enabled
        self.capacity = capacity
        self.clock_ms = clock_ms
        self._sent: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._recorded: OrderedDict[str, None] = OrderedDict()
        self._lock = asyncio.Lock()

    def remember(self, event_id: str, data: Mapping[str, Any]) -> None:
        """Called by the SSE stream for every frame it sends (no-op unless enabled)."""
        if not self.enabled:
            return
        payload = data.get("payload")
        payload = payload if isinstance(payload, Mapping) else {}
        self._sent[event_id] = {
            "event_type": data.get("type"),
            "season": data.get("season"),
            "round": data.get("round"),
            **{k: _ms(payload.get(k)) for k in _PAYLOAD_TIMINGS},
            "sse_sent_at_ms": _ms(data.get("sse_sent_at_ms")),
        }
        self._sent.move_to_end(event_id)
        while len(self._sent) > self.capacity:
            self._sent.popitem(last=False)

    async def record(self, event_id: str, ui_rendered_at_ms: int) -> dict[str, Any]:
        """Append one CSV row; returns it. Raises ``UnknownEventError`` or
        ``AlreadyRecordedError``."""
        async with self._lock:
            if event_id in self._recorded:
                raise AlreadyRecordedError(event_id)
            sent = self._sent.get(event_id)
            if sent is None:
                raise UnknownEventError(event_id)
            row = {
                "event_id": event_id,
                **sent,
                "ui_rendered_at_ms": ui_rendered_at_ms,
                "recorded_at_ms": self.clock_ms(),
            }
            await asyncio.to_thread(self._append, row)
            self._recorded[event_id] = None
            while len(self._recorded) > self.capacity:
                self._recorded.popitem(last=False)
            return row

    def _append(self, row: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        new = not self.path.exists() or self.path.stat().st_size == 0
        with self.path.open("a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, lineterminator="\n")
            if new:
                writer.writeheader()
            writer.writerow({c: "" if row.get(c) is None else row[c] for c in CSV_COLUMNS})
