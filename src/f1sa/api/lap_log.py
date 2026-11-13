"""The lap history of a race, recorded from its Redis channel for the what-if simulator (S15).

The Redis hash holds only each car's latest state, but the simulator needs the current lap and
each car's recent laps. ``LapLog`` keeps the last ``MAX_LAPS_PER_DRIVER`` laps of every car from
the ``lap`` / ``pit_in`` / ``pit_out`` messages of ``race:{s}:{r}:events`` (the same channel the
SSE stream forwards; no other data source). ``LapLogManager`` keeps one log per race, fed by:

* the SSE stream of a connected dashboard: ``attach()`` returns the log and the route passes its
  ``observe`` to the stream's ``on_sent`` hook, so no second subscription is opened;
* one Pub/Sub subscriber task per race, started without waiting by a replay start
  (``start_soon(fresh=True)`` drops an older log of the same race) and by the what-if route
  (``ensure()``, which waits until the subscription is active).

A message seen by both feeds is applied twice with the same result (laps are keyed by lap number).
A subscriber stops after ``session_end`` (the log is kept for later what-if requests), after
``idle_s`` without use, when more than ``max_races`` races are kept (least recently used first),
and on API shutdown.

Pub/Sub keeps no history: laps published before the subscriber started are not in the log. The
what-if then works with fewer laps or none (``history: none``) and says so.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import anyio
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from f1sa.serving import keys as K
from f1sa.sim.whatif import LapRecord

log = logging.getLogger(__name__)

MAX_LAPS_PER_DRIVER = 30
MAX_RACES = 4
IDLE_S = 1800.0
POLL_S = 1.0
SUBSCRIBE_TIMEOUT_S = 5.0


def _int(v: Any) -> int | None:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _float(v: Any) -> float | None:
    if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v):
        return None
    return float(v)


@dataclass
class LapLog:
    """The recorded laps of one race (pure; fed one parsed channel message at a time)."""

    season: int
    round: int
    max_laps: int = MAX_LAPS_PER_DRIVER
    ended: bool = False
    messages: int = 0
    skipped: int = 0
    _laps: dict[str, dict[int, dict[str, Any]]] = field(default_factory=dict, repr=False)
    _pit_in: dict[str, set[int]] = field(default_factory=dict, repr=False)
    _pit_out: dict[str, set[int]] = field(default_factory=dict, repr=False)

    def observe(self, msg: Mapping[str, Any]) -> bool:
        """Apply one message (the stored JSON envelope); ``False`` if it was skipped."""
        if msg.get("season") != self.season or msg.get("round") != self.round:
            self.skipped += 1
            return False
        kind = msg.get("type")
        if kind == "session_end":
            self.ended = True
            self.messages += 1
            return True
        payload = msg.get("payload")
        if kind not in ("lap", "pit_in", "pit_out") or not isinstance(payload, Mapping):
            self.skipped += 1
            return False
        driver, lap = payload.get("driver_id"), _int(payload.get("lap"))
        try:
            driver = K.validate_driver_id(driver) if isinstance(driver, str) else None
        except K.InvalidKeyError:
            driver = None
        if driver is None or lap is None or lap < 1:
            self.skipped += 1
            return False
        self.messages += 1
        if kind == "pit_in":
            self._pit_in.setdefault(driver, set()).add(lap)
        elif kind == "pit_out":
            self._pit_out.setdefault(driver, set()).add(lap)
        else:
            t = _float(payload.get("race_time_s"))
            if t is None:
                self.skipped += 1
                return False
            compound = payload.get("compound")
            self._laps.setdefault(driver, {})[lap] = {
                "race_time_s": t,
                "lap_time_s": _float(payload.get("lap_time_s")),
                "position": _int(payload.get("position")),
                "compound": compound if isinstance(compound, str) else None,
                "tyre_age": _int(payload.get("tyre_age")),
            }
        self._prune(driver)
        return True

    def _prune(self, driver: str) -> None:
        laps = self._laps.get(driver, {})
        if len(laps) <= self.max_laps:
            return
        keep = sorted(laps)[-self.max_laps :]
        floor = keep[0]
        self._laps[driver] = {n: laps[n] for n in keep}
        for flags in (self._pit_in, self._pit_out):
            if driver in flags:
                flags[driver] = {n for n in flags[driver] if n >= floor}

    def latest_lap(self, driver_id: str) -> int | None:
        laps = self._laps.get(driver_id)
        return max(laps) if laps else None

    def records(self) -> list[LapRecord]:
        """Every recorded lap, with the pit flags merged in (sorted by driver, lap)."""
        out: list[LapRecord] = []
        for d in sorted(self._laps):
            pin, pout = self._pit_in.get(d, set()), self._pit_out.get(d, set())
            for lap, v in sorted(self._laps[d].items()):
                out.append(
                    LapRecord(driver_id=d, lap=lap, pit_in=lap in pin, pit_out=lap in pout, **v)
                )
        return out


@dataclass
class _Entry:
    log: LapLog
    last_used: float
    task: asyncio.Task[None] | None = None
    subscribed: asyncio.Event = field(default_factory=asyncio.Event)


class LapLogManager:
    """One log per race, fed by attached SSE streams and/or one subscriber task (see the module
    docstring). All methods run on the event loop thread."""

    def __init__(
        self,
        client: aioredis.Redis,
        *,
        max_races: int = MAX_RACES,
        idle_s: float = IDLE_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client = client
        self.max_races = max_races
        self.idle_s = idle_s
        self._clock = clock
        self._races: dict[tuple[int, int], _Entry] = {}
        self._cancelled: set[asyncio.Task[None]] = set()

    def get(self, season: int, round_: int) -> LapLog | None:
        entry = self._races.get((season, round_))
        return None if entry is None else entry.log

    def running(self) -> list[tuple[int, int]]:
        """Races whose subscriber task is running."""
        return sorted(k for k, v in self._races.items() if v.task is not None and not v.task.done())

    def attach(self, season: int, round_: int) -> LapLog:
        """The race's log, created if needed, without starting a subscriber (for the SSE feed)."""
        return self._entry(season, round_, fresh=False).log

    def start_soon(self, season: int, round_: int, *, fresh: bool = False) -> LapLog:
        """The race's log with its subscriber task scheduled (not awaited). ``fresh`` drops an
        older log of the race first; an ended log is not restarted unless ``fresh``."""
        entry = self._entry(season, round_, fresh=fresh)
        if (entry.task is None or entry.task.done()) and not entry.log.ended:
            entry.subscribed = asyncio.Event()
            entry.task = asyncio.create_task(
                self._run(entry.log, entry.subscribed), name=f"laplog-{season}-{round_}"
            )
        return entry.log

    async def ensure(self, season: int, round_: int) -> LapLog:
        """``start_soon``, then wait (at most ``SUBSCRIBE_TIMEOUT_S``) until it is subscribed."""
        log_ = self.start_soon(season, round_)
        entry = self._races[(season, round_)]
        if entry.task is not None and not entry.task.done():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(entry.subscribed.wait(), SUBSCRIBE_TIMEOUT_S)
        return log_

    def _entry(self, season: int, round_: int, *, fresh: bool) -> _Entry:
        key = (season, round_)
        now = self._clock()
        for k in [
            k for k, v in self._races.items() if k != key and now - v.last_used > self.idle_s
        ]:
            self._drop(k)
        if fresh and key in self._races:
            self._drop(key)
        entry = self._races.get(key)
        if entry is None:
            entry = self._races[key] = _Entry(LapLog(season, round_), now)
        entry.last_used = now
        while len(self._races) > self.max_races:
            self._drop(
                min((k for k in self._races if k != key), key=lambda k: self._races[k].last_used)
            )
        return entry

    def _drop(self, key: tuple[int, int]) -> None:
        entry = self._races.pop(key, None)
        if entry is not None and entry.task is not None and not entry.task.done():
            entry.task.cancel()
            self._cancelled.add(entry.task)
            entry.task.add_done_callback(self._cancelled.discard)

    async def _run(self, log_: LapLog, subscribed: asyncio.Event) -> None:
        channel = K.events_channel(log_.season, log_.round)
        pubsub: Any = self.client.pubsub(ignore_subscribe_messages=True)
        try:
            await pubsub.subscribe(channel)
            subscribed.set()
            while not log_.ended:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=POLL_S)
                if msg is None or msg.get("type") != "message":
                    await asyncio.sleep(0.05)
                    continue
                data = msg.get("data")
                raw = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else data
                try:
                    parsed = json.loads(raw) if isinstance(raw, str) else None
                except ValueError:
                    parsed = None
                if isinstance(parsed, dict):
                    log_.observe(parsed)
                else:
                    log_.skipped += 1
        except (RedisError, OSError) as exc:
            log.warning(
                "lap log subscriber stopped",
                extra={"channel": channel, "exc_class": type(exc).__name__},
            )
        finally:
            subscribed.set()
            with anyio.CancelScope(shield=True):
                with contextlib.suppress(RedisError, OSError):
                    await pubsub.unsubscribe(channel)
                with contextlib.suppress(RedisError, OSError):
                    await pubsub.aclose()

    async def aclose(self) -> None:
        """Stop every subscriber and wait for them (API shutdown)."""
        for key in list(self._races):
            self._drop(key)
        pending = list(self._cancelled)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
