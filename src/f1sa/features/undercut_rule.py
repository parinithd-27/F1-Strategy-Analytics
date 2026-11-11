"""The undercut candidate rule, shared by the batch label (``features/undercut.py``) and the
real-time detector (``streaming/detector.py``). Pure Python: no Spark or pandas import, so the
streaming side can use it and the two cannot drift apart.

The definition is in ``docs/label_definition.md`` ("Undercut attempts"). At the **end of lap
L-1** (the decision lap), attacker A and defender D form a candidate pair when:

* A is directly behind D on the timing line (``A.position == D.position + 1``) and
  ``0 < gap <= gap_threshold_s``, where gap = A's gap to the car ahead (cum_A - cum_D);
* the attempt lap L = L-1 + 1 is at least ``min_attempt_lap`` (5);
* A and D are not teammates (same team history key);
* no other car crossed the line between D and A (a lapped car, or one lapping them);
* neither A nor D is lapped (gap to the leader >= the leader's lap time at L-1);
* neither is on an in-lap or out-lap at L-1;
* lap L-1 is green and is not the first green lap after a neutralised run (restart lap).

A pair with a gap above the threshold is not a candidate and is not returned. A pair that fails
any other condition is returned with ``eligible = False`` and every failed condition in
``reasons``, so the batch job can count exclusions. The detector keeps eligible pairs only.
Only information from laps <= L-1 is used.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass

GAP_GRID: tuple[float, ...] = (2.5, 3.5, 5.0)
"""Gap thresholds G (s) emitted by the batch build (sensitivity grid)."""
WINDOW_GRID: tuple[int, ...] = (1, 2, 3)
"""Defender response windows k (laps) emitted by the batch build."""
DEFAULT_GAP_S = 3.5
DEFAULT_WINDOW_LAPS = 2
MIN_ATTEMPT_LAP = 5
"""An attempt with stop lap L < 5 is ``early_lap``. Stops on laps 2-4 are mostly lap-1 damage,
red-flag entries or early wet stops, not strategy, and at L = 4 the 3-lap decision lookback
(``gap_ahead_change_3l``, L-1 vs L-4) and the degradation slopes are always null. Raised from 4 to 5
in Step 8b from counts only (no labelled attempt at L = 4), before any model result; the label
definition is frozen after this change. The batch label and the detector both use this one value."""

# Rule-level reasons (subset of the batch exclusion reasons, see features/undercut.py).
MISSING_DATA = "missing_data"
INCONSISTENT_TIMING = "inconsistent_timing"
EARLY_LAP = "early_lap"
TEAMMATES = "teammates"
TRAFFIC_BETWEEN = "traffic_between"
LAPPED = "lapped"
IN_PIT = "in_pit"
NEUTRALISED = "neutralised"
RULE_REASONS: tuple[str, ...] = (
    MISSING_DATA,
    INCONSISTENT_TIMING,
    EARLY_LAP,
    TEAMMATES,
    TRAFFIC_BETWEEN,
    LAPPED,
    IN_PIT,
    NEUTRALISED,
)


@dataclass(frozen=True)
class RuleParams:
    """Tunable parameters of the candidate rule."""

    gap_threshold_s: float = DEFAULT_GAP_S
    min_attempt_lap: int = MIN_ATTEMPT_LAP

    def __post_init__(self) -> None:
        if not (math.isfinite(self.gap_threshold_s) and self.gap_threshold_s > 0):
            raise ValueError(
                f"gap_threshold_s must be a positive number, got {self.gap_threshold_s}"
            )
        if self.min_attempt_lap < 2:
            raise ValueError(f"min_attempt_lap must be >= 2, got {self.min_attempt_lap}")


@dataclass(frozen=True)
class DriverLapState:
    """One car at the end of the decision lap (L-1).

    ``crossing_s`` / ``prev_crossing_s`` are the car's line-crossing times (cumulative race time)
    at the end of laps L-1 and L-2; a lapped car's L-2 crossing can fall between two cars that
    are on lap L-1. ``gap_ahead_s`` is the gap to the car one position ahead, ``gap_leader_s`` the
    gap to P1 at the same lap number.
    """

    driver_id: str
    position: int | None
    gap_ahead_s: float | None
    gap_leader_s: float | None = None
    crossing_s: float | None = None
    prev_crossing_s: float | None = None
    team_key: str | None = None
    on_in_lap: bool = False
    on_out_lap: bool = False
    running: bool = True
    tyre_compound: str | None = None
    tyre_age_laps: int | None = None
    last_lap_time_s: float | None = None


@dataclass(frozen=True)
class LapSnapshot:
    """The race at the end of ``lap`` (the decision lap L-1)."""

    season: int
    round: int
    lap: int
    drivers: tuple[DriverLapState, ...]
    track_green: bool = True
    restart_lap: bool = False
    leader_lap_time_s: float | None = None


@dataclass(frozen=True)
class CandidatePair:
    season: int
    round: int
    attacker_id: str
    defender_id: str
    decision_lap: int
    gap_s: float | None
    reasons: tuple[str, ...]

    @property
    def attempt_lap(self) -> int:
        """The lap L on which the attacker would pit."""
        return self.decision_lap + 1

    @property
    def eligible(self) -> bool:
        return not self.reasons


def _num(x: float | None) -> float | None:
    """``x`` as a float when it is a finite number, else None."""
    return float(x) if x is not None and math.isfinite(x) else None


def _gap(a: DriverLapState, d: DriverLapState) -> float | None:
    gap = _num(a.gap_ahead_s)
    if gap is not None:
        return gap
    ca, cd = _num(a.crossing_s), _num(d.crossing_s)
    return round(ca - cd, 3) if ca is not None and cd is not None else None


def _between(lo: float, hi: float, others: Iterable[DriverLapState]) -> bool:
    for x in others:
        for t in (_num(x.crossing_s), _num(x.prev_crossing_s)):
            if t is not None and lo < t < hi:
                return True
    return False


def _lapped(car: DriverLapState, leader_lap_time_s: float | None) -> bool:
    gap, lap_time = _num(car.gap_leader_s), _num(leader_lap_time_s)
    return gap is not None and lap_time is not None and gap >= lap_time


def evaluate_pair(
    snapshot: LapSnapshot, attacker: DriverLapState, defender: DriverLapState, params: RuleParams
) -> CandidatePair | None:
    """The candidate pair (A behind D), or None when the gap is known and above the threshold."""
    gap = _gap(attacker, defender)
    reasons: list[str] = []
    if gap is None or attacker.position is None or defender.position is None:
        reasons.append(MISSING_DATA)
    elif gap <= 0:
        reasons.append(INCONSISTENT_TIMING)
    elif gap > params.gap_threshold_s:
        return None
    if snapshot.lap + 1 < params.min_attempt_lap:
        reasons.append(EARLY_LAP)
    if attacker.team_key is not None and attacker.team_key == defender.team_key:
        reasons.append(TEAMMATES)
    lo, hi = _num(defender.crossing_s), _num(attacker.crossing_s)
    if lo is not None and hi is not None:
        others = [
            x
            for x in snapshot.drivers
            if x.running and x.driver_id not in (attacker.driver_id, defender.driver_id)
        ]
        if _between(lo, hi, others):
            reasons.append(TRAFFIC_BETWEEN)
    if _lapped(attacker, snapshot.leader_lap_time_s) or _lapped(
        defender, snapshot.leader_lap_time_s
    ):
        reasons.append(LAPPED)
    if attacker.on_in_lap or attacker.on_out_lap or defender.on_in_lap or defender.on_out_lap:
        reasons.append(IN_PIT)
    if not snapshot.track_green or snapshot.restart_lap:
        reasons.append(NEUTRALISED)
    return CandidatePair(
        season=snapshot.season,
        round=snapshot.round,
        attacker_id=attacker.driver_id,
        defender_id=defender.driver_id,
        decision_lap=snapshot.lap,
        gap_s=gap,
        reasons=tuple(reasons),
    )


def candidate_pairs(snapshot: LapSnapshot, params: RuleParams) -> list[CandidatePair]:
    """Every candidate pair at the end of ``snapshot.lap``, ordered by the attacker's position.

    Cars that are not running or have no position are ignored. If two running cars share a
    position, the snapshot is inconsistent and a ``ValueError`` is raised.
    """
    by_pos: dict[int, DriverLapState] = {}
    for car in snapshot.drivers:
        if not car.running or car.position is None:
            continue
        if car.position in by_pos:
            raise ValueError(
                f"{snapshot.season} R{snapshot.round} lap {snapshot.lap}: position {car.position} "
                f"held by {by_pos[car.position].driver_id} and {car.driver_id}"
            )
        by_pos[car.position] = car
    out: list[CandidatePair] = []
    for pos in sorted(by_pos):
        defender = by_pos.get(pos - 1)
        if defender is None:
            continue
        pair = evaluate_pair(snapshot, by_pos[pos], defender, params)
        if pair is not None:
            out.append(pair)
    return out
