"""Heuristic real-time undercut detector (#20). Pure Python: no Spark dependency.

At the end of a lap, ``detect`` returns every pair (attacker, defender) that satisfies the
shared candidate rule (``f1sa.features.undercut_rule``), the same rule that defines a batch
undercut attempt. A detected pair is an *opportunity*: whether the attacker actually pits on the
next lap is only known later. The batch label adds "A pits on lap L and D does not" and the
outcome window on top of the same rule (tested in ``tests/unit/test_detector.py``).

``snapshot_from_state`` builds the rule's ``LapSnapshot`` from a per-driver state mapping (the
fields of the Redis driver hash plus the timing fields the rule needs), validating every value.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from f1sa.features.undercut_rule import (
    CandidatePair,
    DriverLapState,
    LapSnapshot,
    RuleParams,
    candidate_pairs,
)

__all__ = ["UndercutCandidate", "detect", "snapshot_from_state"]


@dataclass(frozen=True)
class UndercutCandidate:
    """An eligible undercut opportunity at the end of ``decision_lap``."""

    season: int
    round: int
    attacker_id: str
    defender_id: str
    decision_lap: int
    gap_s: float
    tyre_age_delta_laps: int | None
    """Defender's tyre age minus the attacker's (positive: the defender's tyres are older)."""
    attacker_compound: str | None
    defender_compound: str | None
    last_lap_delta_s: float | None
    """Attacker's last lap minus the defender's (negative: the attacker is faster)."""

    @property
    def attempt_lap(self) -> int:
        return self.decision_lap + 1


def _candidate(pair: CandidatePair, cars: Mapping[str, DriverLapState]) -> UndercutCandidate:
    a, d = cars[pair.attacker_id], cars[pair.defender_id]
    age = (
        d.tyre_age_laps - a.tyre_age_laps
        if a.tyre_age_laps is not None and d.tyre_age_laps is not None
        else None
    )
    pace = (
        round(a.last_lap_time_s - d.last_lap_time_s, 3)
        if a.last_lap_time_s is not None and d.last_lap_time_s is not None
        else None
    )
    if pair.gap_s is None:  # eligible pairs always have a gap; guard the invariant
        raise ValueError(f"eligible pair without a gap: {pair}")
    return UndercutCandidate(
        season=pair.season,
        round=pair.round,
        attacker_id=pair.attacker_id,
        defender_id=pair.defender_id,
        decision_lap=pair.decision_lap,
        gap_s=pair.gap_s,
        tyre_age_delta_laps=age,
        attacker_compound=a.tyre_compound,
        defender_compound=d.tyre_compound,
        last_lap_delta_s=pace,
    )


def detect(snapshot: LapSnapshot, params: RuleParams | None = None) -> list[UndercutCandidate]:
    """Eligible undercut candidates at the end of ``snapshot.lap``, ordered by position."""
    rule = params or RuleParams()
    cars = {c.driver_id: c for c in snapshot.drivers}
    return [_candidate(p, cars) for p in candidate_pairs(snapshot, rule) if p.eligible]


def _opt_float(name: str, value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise ValueError(f"{name}: expected a number, got {value!r}")
    try:
        out = float(value)
    except ValueError as exc:
        raise ValueError(f"{name}: expected a number, got {value!r}") from exc
    if not math.isfinite(out):
        raise ValueError(f"{name}: expected a finite number, got {value!r}")
    return out


def _opt_int(name: str, value: Any) -> int | None:
    f = _opt_float(name, value)
    if f is None:
        return None
    if not f.is_integer():
        raise ValueError(f"{name}: expected an integer, got {value!r}")
    return int(f)


def _bool(name: str, value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false", "1", "0"):
        return value.lower() in ("true", "1")
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise ValueError(f"{name}: expected a boolean, got {value!r}")


def _opt_str(name: str, value: Any) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(f"{name}: expected a string, got {value!r}")
    return value


def snapshot_from_state(
    season: int,
    round_: int,
    lap: int,
    state: Mapping[str, Mapping[str, Any]],
    *,
    track_green: bool = True,
    restart_lap: bool = False,
    leader_lap_time_s: float | None = None,
) -> LapSnapshot:
    """A ``LapSnapshot`` from ``{driver_id: fields}``.

    Fields (all optional except ``position``): ``position``, ``gap_ahead_s``, ``gap_leader_s``,
    ``tyre_compound``, ``tyre_age_laps``, ``last_lap_time_s`` (as in the Redis driver hash, where
    values may arrive as strings), plus ``crossing_s``, ``prev_crossing_s``, ``team_key``,
    ``on_in_lap``, ``on_out_lap`` and ``running``. Raises ``ValueError`` on a malformed value.
    """
    if lap < 1:
        raise ValueError(f"lap must be >= 1, got {lap}")
    drivers: list[DriverLapState] = []
    for driver_id in sorted(state):
        f = state[driver_id]
        if not isinstance(driver_id, str) or not driver_id:
            raise ValueError(f"driver id must be a non-empty string, got {driver_id!r}")
        position = _opt_int(f"{driver_id}.position", f.get("position"))
        if position is not None and position < 1:
            raise ValueError(f"{driver_id}.position must be >= 1, got {position}")
        drivers.append(
            DriverLapState(
                driver_id=driver_id,
                position=position,
                gap_ahead_s=_opt_float(f"{driver_id}.gap_ahead_s", f.get("gap_ahead_s")),
                gap_leader_s=_opt_float(f"{driver_id}.gap_leader_s", f.get("gap_leader_s")),
                crossing_s=_opt_float(f"{driver_id}.crossing_s", f.get("crossing_s")),
                prev_crossing_s=_opt_float(
                    f"{driver_id}.prev_crossing_s", f.get("prev_crossing_s")
                ),
                team_key=_opt_str(f"{driver_id}.team_key", f.get("team_key")),
                on_in_lap=_bool(f"{driver_id}.on_in_lap", f.get("on_in_lap"), False),
                on_out_lap=_bool(f"{driver_id}.on_out_lap", f.get("on_out_lap"), False),
                running=_bool(f"{driver_id}.running", f.get("running"), True),
                tyre_compound=_opt_str(f"{driver_id}.tyre_compound", f.get("tyre_compound")),
                tyre_age_laps=_opt_int(f"{driver_id}.tyre_age_laps", f.get("tyre_age_laps")),
                last_lap_time_s=_opt_float(
                    f"{driver_id}.last_lap_time_s", f.get("last_lap_time_s")
                ),
            )
        )
    return LapSnapshot(
        season=season,
        round=round_,
        lap=lap,
        drivers=tuple(drivers),
        track_green=track_green,
        restart_lap=restart_lap,
        leader_lap_time_s=_opt_float("leader_lap_time_s", leader_lap_time_s),
    )
