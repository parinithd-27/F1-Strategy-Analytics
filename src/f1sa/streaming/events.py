"""Race events on Kafka topic ``f1.race.events`` (#19): typed schema, JSON and the Spark schema.

One message per event, key = ``driver_id`` (``session_end`` uses the key ``session_end`` and is
sent once to every partition, so each partition's stream of a race ends with it).

=================  =========================================================================
event_id           uuid4 string
type               ``lap`` | ``pit_in`` | ``pit_out`` | ``session_end``
season, round      the race
driver_id          Jolpica driverId (null for ``session_end``)
lap                ``lap``: the lap completed; ``pit_in``: the in-lap (Jolpica pit ``lap``);
                   ``pit_out``: the out-lap (in-lap + 1); ``session_end``: the last lap or null
lap_time_s         ``lap`` only, null when Jolpica has no time
position           ``lap`` only: the Jolpica timing-line position at the end of the lap
compound           ``lap`` only: SOFT | MEDIUM | HARD | INTERMEDIATE | WET, or null
tyre_age           ``lap`` only: tyre age in laps at the end of the lap, or null
produced_at_ms     wall clock (epoch ms) when the producer sent the event
race_time_s        race clock (s since lights out): the lap's line crossing, the stop for a pit
                   event. Used for pacing and for the stream's watermark.
track_neutral      ``lap`` only: the batch per-lap flag (SC/VSC/red flag or neutralised)
track_red_flag     ``lap`` only: the batch red-flag flag
race_laps          ``lap`` only: the race distance in laps
=================  =========================================================================

The last three fields are **replayed race context** (Step 12 decision): the batch values come
from the Jolpica heuristic, which uses the whole race's green median, and from the winner's lap
count. A live deployment would take them from the race-control feed and the scheduled distance.
They are the only values in the stream that are not causal (docs/architecture.md, "Streaming").

The module imports no Spark (``spark_schema`` imports it lazily), so the hot path stays light.
"""

from __future__ import annotations

import json
import math
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, fields, replace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

EVENT_TYPES: tuple[str, ...] = ("lap", "pit_in", "pit_out", "session_end")
COMPOUNDS: tuple[str, ...] = ("SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET")
SESSION_END_KEY = "session_end"
INFRA_HEADER = ("test", b"infra")
"""Header of the Step 2 infrastructure test message, which must never reach the race logic."""
CONTEXT_FIELDS: tuple[str, ...] = ("track_neutral", "track_red_flag", "race_laps")
"""Replayed race context: the only non-causal values in the stream (see the module docstring)."""


class EventError(ValueError):
    """A message that does not satisfy the event schema."""


@dataclass(frozen=True)
class RaceEvent:
    event_id: str
    type: str
    season: int
    round: int
    race_time_s: float
    driver_id: str | None = None
    lap: int | None = None
    lap_time_s: float | None = None
    position: int | None = None
    compound: str | None = None
    tyre_age: int | None = None
    produced_at_ms: int | None = None
    track_neutral: bool | None = None
    track_red_flag: bool | None = None
    race_laps: int | None = None

    def __post_init__(self) -> None:
        validate(self)

    @property
    def race(self) -> tuple[int, int]:
        return (self.season, self.round)

    def stamped(self, produced_at_ms: int) -> RaceEvent:
        """A copy with ``produced_at_ms`` set (the producer stamps it right before sending)."""
        return replace(self, produced_at_ms=produced_at_ms)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), allow_nan=False, separators=(",", ":"), sort_keys=True)


FIELD_NAMES: tuple[str, ...] = tuple(f.name for f in fields(RaceEvent))


def new_event_id() -> str:
    return str(uuid.uuid4())


def _int(name: str, v: Any, lo: int, *, nullable: bool = True) -> None:
    if v is None:
        if nullable:
            return
        raise EventError(f"{name} is required")
    if isinstance(v, bool) or not isinstance(v, int) or v < lo:
        raise EventError(f"{name} must be an int >= {lo}, got {v!r}")


def _float(name: str, v: Any, lo: float, *, nullable: bool = True) -> None:
    if v is None:
        if nullable:
            return
        raise EventError(f"{name} is required")
    if isinstance(v, bool) or not isinstance(v, int | float):
        raise EventError(f"{name} must be a number, got {v!r}")
    if not math.isfinite(v) or v < lo:
        raise EventError(f"{name} must be finite and >= {lo}, got {v!r}")


def _bool(name: str, v: Any) -> None:
    if v is not None and not isinstance(v, bool):
        raise EventError(f"{name} must be a bool or null, got {v!r}")


def validate(e: RaceEvent) -> None:
    """Raise ``EventError`` unless every field of ``e`` is valid for its type."""
    try:
        parsed = uuid.UUID(e.event_id) if isinstance(e.event_id, str) else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.version != 4 or str(parsed) != e.event_id:
        raise EventError(f"event_id must be a canonical uuid4 string, got {e.event_id!r}")
    if e.type not in EVENT_TYPES:
        raise EventError(f"type must be one of {EVENT_TYPES}, got {e.type!r}")
    _int("season", e.season, 1950, nullable=False)
    _int("round", e.round, 1, nullable=False)
    _float("race_time_s", e.race_time_s, 0.0, nullable=False)
    _int("lap", e.lap, 1)
    _float("lap_time_s", e.lap_time_s, 0.0)
    _int("position", e.position, 1)
    _int("tyre_age", e.tyre_age, 0)
    _int("produced_at_ms", e.produced_at_ms, 0)
    _int("race_laps", e.race_laps, 1)
    _bool("track_neutral", e.track_neutral)
    _bool("track_red_flag", e.track_red_flag)
    if e.compound is not None and e.compound not in COMPOUNDS:
        raise EventError(f"compound must be one of {COMPOUNDS} or null, got {e.compound!r}")
    if e.type == "session_end":
        if e.driver_id is not None:
            raise EventError("session_end has no driver_id")
    else:
        if not isinstance(e.driver_id, str) or not e.driver_id:
            raise EventError(f"{e.type} needs a driver_id, got {e.driver_id!r}")
        if e.lap is None:
            raise EventError(f"{e.type} needs a lap")
    if e.type != "lap":
        lap_only = {
            "lap_time_s": e.lap_time_s,
            "position": e.position,
            "compound": e.compound,
            "tyre_age": e.tyre_age,
            **{c: getattr(e, c) for c in CONTEXT_FIELDS},
        }
        set_fields = sorted(k for k, v in lap_only.items() if v is not None)
        if set_fields:
            raise EventError(f"{e.type} must not carry lap fields: {set_fields}")


def from_dict(data: Mapping[str, Any]) -> RaceEvent:
    unknown = sorted(set(data) - set(FIELD_NAMES))
    if unknown:
        raise EventError(f"unknown event fields: {unknown}")
    missing = sorted({"event_id", "type", "season", "round", "race_time_s"} - set(data))
    if missing:
        raise EventError(f"missing event fields: {missing}")
    values = dict(data)
    # JSON has one number type: an integral float for race_time_s / lap_time_s is fine.
    for name in ("race_time_s", "lap_time_s"):
        if isinstance(values.get(name), int) and not isinstance(values.get(name), bool):
            values[name] = float(values[name])
    try:
        return RaceEvent(**values)
    except TypeError as exc:
        raise EventError(str(exc)) from exc


def from_json(text: str | bytes) -> RaceEvent:
    try:
        data = json.loads(text)
    except (ValueError, UnicodeDecodeError) as exc:
        raise EventError(f"not JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise EventError(f"an event must be a JSON object, got {type(data).__name__}")
    return from_dict(data)


def header_items(headers: Iterable[Any] | None) -> list[tuple[str, bytes | None]]:
    """Kafka headers as ``(key, value)`` pairs (confluent-kafka tuples or Spark Rows)."""
    out: list[tuple[str, bytes | None]] = []
    for h in headers or ():
        if isinstance(h, tuple):
            key, value = h
        else:
            key, value = h["key"], h["value"]
        out.append((str(key), None if value is None else bytes(value)))
    return out


def is_infra_test(headers: Iterable[Any] | None) -> bool:
    """True for the Step 2 infrastructure message (header ``test=infra``)."""
    return INFRA_HEADER in header_items(headers)


def spark_schema() -> StructType:
    """The Spark ``StructType`` of the event JSON (for ``from_json``), in field order."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    types = {
        "event_id": StringType(),
        "type": StringType(),
        "season": IntegerType(),
        "round": IntegerType(),
        "race_time_s": DoubleType(),
        "driver_id": StringType(),
        "lap": IntegerType(),
        "lap_time_s": DoubleType(),
        "position": IntegerType(),
        "compound": StringType(),
        "tyre_age": IntegerType(),
        "produced_at_ms": LongType(),
        "track_neutral": BooleanType(),
        "track_red_flag": BooleanType(),
        "race_laps": IntegerType(),
    }
    if tuple(types) != FIELD_NAMES:
        raise AssertionError("spark_schema is out of step with RaceEvent")
    return StructType([StructField(n, t, nullable=True) for n, t in types.items()])


# ============================================================================== published alerts
ALERT_REASONS: tuple[str, ...] = ("opportunity", "attempt")
ALERT_TRIGGERS: dict[str, tuple[str, ...]] = {
    "opportunity": ("new_candidate", "probability_crossed"),
    "attempt": ("pit_in",),
}
"""Why an ``undercut_alert`` was published (docs/architecture.md, "Alert policy"):

* ``opportunity`` / ``new_candidate``: the pair is a rule-eligible candidate at decision lap L-1
  and was not one at L-2;
* ``opportunity`` / ``probability_crossed``: the pair was a candidate at L-2 below the threshold
  and its calibrated P at L-1 is at or above it;
* ``attempt`` / ``pit_in``: the attacker of a candidate pair at L-1 pits on lap L (P is the one at
  L-1)."""
_ALERT_CATEGORICAL = frozenset(
    {
        "pit_loss_source",
        "attacker_expected_outlap_pace_source",
        "defender_expected_outlap_pace_source",
    }
)


@dataclass(frozen=True)
class UndercutAlert:
    """The payload of an ``undercut_alert`` on ``race:{s}:{r}:events`` (before the timing fields
    ``trigger_event_id``, ``produced_at_ms``, ``batch_received_at_ms`` and ``redis_written_at_ms``
    the sink adds)."""

    reason: str
    trigger: str
    season: int
    round: int
    attacker_id: str
    defender_id: str
    decision_lap: int
    gap_s: float
    undercut_prob: float
    threshold: float
    inputs: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.reason not in ALERT_REASONS:
            raise EventError(f"reason must be one of {ALERT_REASONS}, got {self.reason!r}")
        if self.trigger not in ALERT_TRIGGERS[self.reason]:
            raise EventError(f"trigger {self.trigger!r} is not valid for reason {self.reason!r}")
        _int("season", self.season, 1950, nullable=False)
        _int("round", self.round, 1, nullable=False)
        _int("decision_lap", self.decision_lap, 1, nullable=False)
        _float("gap_s", self.gap_s, 0.0, nullable=False)
        for name in ("undercut_prob", "threshold"):
            v = getattr(self, name)
            _float(name, v, 0.0, nullable=False)
            if v > 1.0:
                raise EventError(f"{name} must be <= 1, got {v!r}")
        if not self.attacker_id or not self.defender_id:
            raise EventError("an alert needs an attacker and a defender")

    @property
    def attempt_lap(self) -> int:
        return self.decision_lap + 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "trigger": self.trigger,
            "season": self.season,
            "round": self.round,
            "attacker_id": self.attacker_id,
            "defender_id": self.defender_id,
            "decision_lap": self.decision_lap,
            "attempt_lap": self.attempt_lap,
            "gap_s": self.gap_s,
            "undercut_prob": self.undercut_prob,
            "threshold": self.threshold,
            "inputs": dict(self.inputs),
        }


ALERT_TIMING_FIELDS: tuple[str, ...] = (
    "trigger_event_id",
    "produced_at_ms",
    "batch_received_at_ms",
    "redis_written_at_ms",
)


def alert_spark_schema() -> StructType:
    """The Spark ``StructType`` of a published ``undercut_alert`` payload (``UndercutAlert`` plus
    the sink's timing fields); numeric inputs are doubles, the three ``*_source`` inputs strings."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    from f1sa.streaming.live_features import INPUT_NAMES

    inputs = StructType(
        [
            StructField(n, StringType() if n in _ALERT_CATEGORICAL else DoubleType())
            for n in INPUT_NAMES
        ]
    )
    return StructType(
        [
            StructField("reason", StringType()),
            StructField("trigger", StringType()),
            StructField("season", IntegerType()),
            StructField("round", IntegerType()),
            StructField("attacker_id", StringType()),
            StructField("defender_id", StringType()),
            StructField("decision_lap", IntegerType()),
            StructField("attempt_lap", IntegerType()),
            StructField("gap_s", DoubleType()),
            StructField("undercut_prob", DoubleType()),
            StructField("threshold", DoubleType()),
            StructField("inputs", inputs),
            StructField("trigger_event_id", StringType()),
            StructField("produced_at_ms", LongType()),
            StructField("batch_received_at_ms", LongType()),
            StructField("redis_written_at_ms", LongType()),
        ]
    )
