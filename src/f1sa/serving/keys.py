"""Redis key and channel names: the exact contract in CLAUDE.md (#21-#24).

* HASH    ``race:{season}:{round}:driver:{driverId}`` with the 9 fields in ``DRIVER_FIELDS``
* ZSET    ``race:{season}:{round}:order`` (member = driverId, score = position)
* PUBSUB  ``race:{season}:{round}:events`` (message types in ``EVENT_TYPES``)

Every function is pure and validates its inputs (season 1950-2100, round 1-30, driverId
``[a-z0-9_]+``, the Jolpica ``driverId`` form). Invalid input raises ``InvalidKeyError``, so a
malformed key never reaches Redis.
"""

from __future__ import annotations

import re

SEASON_MIN, SEASON_MAX = 1950, 2100
ROUND_MIN, ROUND_MAX = 1, 30
DRIVER_ID_RE = re.compile(r"[a-z0-9_]+")

DRIVER_FIELDS: tuple[str, ...] = (
    "position",
    "gap_ahead_s",
    "gap_leader_s",
    "tyre_compound",
    "tyre_age_laps",
    "last_lap_time_s",
    "undercut_flag",
    "undercut_prob",
    "updated_at",
)
"""The 9 hash fields, in contract order."""

EVENT_TYPES: tuple[str, ...] = ("lap", "pit_in", "pit_out", "undercut_alert", "session_end")
"""The Pub/Sub message types."""


class InvalidKeyError(ValueError):
    """A season, round or driverId outside the key contract."""


def _int_in(name: str, value: object, lo: int, hi: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidKeyError(f"{name} must be an int, got {value!r}")
    if not lo <= value <= hi:
        raise InvalidKeyError(f"{name} must be in {lo}..{hi}, got {value}")
    return value


def validate_season(season: object) -> int:
    return _int_in("season", season, SEASON_MIN, SEASON_MAX)


def validate_round(round_: object) -> int:
    return _int_in("round", round_, ROUND_MIN, ROUND_MAX)


def validate_driver_id(driver_id: object) -> str:
    if not isinstance(driver_id, str) or DRIVER_ID_RE.fullmatch(driver_id) is None:
        raise InvalidKeyError(f"driverId must match [a-z0-9_]+, got {driver_id!r}")
    return driver_id


def race_prefix(season: int, round_: int) -> str:
    """``race:{season}:{round}``, validated."""
    return f"race:{validate_season(season)}:{validate_round(round_)}"


def driver_key(season: int, round_: int, driver_id: str) -> str:
    """``race:{season}:{round}:driver:{driverId}``"""
    return f"{race_prefix(season, round_)}:driver:{validate_driver_id(driver_id)}"


def order_key(season: int, round_: int) -> str:
    """``race:{season}:{round}:order``"""
    return f"{race_prefix(season, round_)}:order"


def events_channel(season: int, round_: int) -> str:
    """``race:{season}:{round}:events`` (a Pub/Sub channel, not a key)."""
    return f"{race_prefix(season, round_)}:events"


def race_pattern(season: int, round_: int) -> str:
    """``race:{season}:{round}:*``: the SCAN MATCH pattern for every key of one race. The trailing
    colon keeps round 5 from matching round 50; validated ints contain no glob characters."""
    return f"{race_prefix(season, round_)}:*"
