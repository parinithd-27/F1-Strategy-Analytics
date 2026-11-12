"""Decision-time features computed live from the event stream (Step 12, feature parity).

The batch pipeline computes the 21 ``real`` model inputs with Spark windows and a few pure-Python
helpers (``features/undercut.py``, ``etl/df_jobs.py``, ``model/dataset.with_derived_inputs``).
This module reproduces each of them **from the events received up to the end of the decision lap
L-1**, with no Spark import, so the stream's hot path stays light. Every port is tested equal to
its batch original (``tests/unit/test_live_features.py``, ``test_stream_parity.py``).

How each input is obtained (docs/architecture.md, "Streaming"):

* live from the events: ``gap_s``, ``gap_ahead_change_1l/_3l``, ``attacker_position``,
  ``pace_delta_last3_s``, the three degradation slopes, ``rejoin_traffic_cars``,
  ``rejoin_nearest_ahead_s`` and both ``*_stint_laps``;
* prior tables of the target race (earlier races only, loaded once per race): ``pit_loss_prior_s``,
  ``pit_loss_source`` and the six out-lap / stop priors;
* replayed race context: ``laps_remaining`` (``race_laps``), and the green-lap filter of the pace
  and slope inputs (``track_neutral``).

Numerics follow Spark exactly:

* ``F.round(x, d)`` is ``BigDecimal(Double.toString(x)).setScale(d, HALF_UP)`` (``spark_round``);
* ``regr_slope(y, x)`` is ``ck / m2`` with Spark's update order of the covariance and
  ``VariancePop`` buffers, and null when ``m2 == 0`` (``regr_slope``, read from Spark 4.2.1's plan);
* window sums add in lap order, starting from the first value.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from f1sa.features.undercut_rule import CandidatePair

TRAFFIC_WINDOW_S = 3.0
"""``features.undercut.TRAFFIC_WINDOW_S`` (tested equal)."""
RESTART_BUFFER_LAPS = 1
"""``features.undercut.RESTART_BUFFER_LAPS`` (tested equal)."""
RED_FLAG_RESTART_LAPS = 3
"""``features.undercut.RED_FLAG_RESTART_LAPS`` (tested equal)."""

INPUT_NAMES: tuple[str, ...] = (
    "gap_s",
    "gap_ahead_change_1l",
    "gap_ahead_change_3l",
    "attacker_position",
    "pace_delta_last3_s",
    "attacker_deg_slope_s_per_lap",
    "defender_deg_slope_s_per_lap",
    "deg_slope_delta_s_per_lap",
    "laps_remaining",
    "pit_loss_prior_s",
    "pit_loss_source",
    "rejoin_traffic_cars",
    "rejoin_nearest_ahead_s",
    "attacker_expected_outlap_pace_residual_s",
    "attacker_expected_outlap_pace_source",
    "defender_expected_outlap_pace_residual_s",
    "defender_expected_outlap_pace_source",
    "attacker_expected_stop_delta_s",
    "defender_expected_stop_delta_s",
    "attacker_stint_laps",
    "defender_stint_laps",
)
"""The 21 inputs of feature set ``real`` in registry order (tested equal to ``scorer.json``)."""

LIVE_INPUTS: frozenset[str] = frozenset(
    {
        "gap_s",
        "gap_ahead_change_1l",
        "gap_ahead_change_3l",
        "attacker_position",
        "rejoin_traffic_cars",
        "rejoin_nearest_ahead_s",
        "attacker_stint_laps",
        "defender_stint_laps",
    }
)
PRIOR_INPUTS: frozenset[str] = frozenset(
    {
        "pit_loss_prior_s",
        "pit_loss_source",
        "attacker_expected_outlap_pace_residual_s",
        "attacker_expected_outlap_pace_source",
        "defender_expected_outlap_pace_residual_s",
        "defender_expected_outlap_pace_source",
        "attacker_expected_stop_delta_s",
        "defender_expected_stop_delta_s",
    }
)
CONTEXT_INPUTS: frozenset[str] = frozenset(
    {
        "laps_remaining",
        "pace_delta_last3_s",
        "attacker_deg_slope_s_per_lap",
        "defender_deg_slope_s_per_lap",
        "deg_slope_delta_s_per_lap",
    }
)
"""Inputs that depend on replayed race context (``race_laps``, or ``track_neutral`` through the
green-lap filter). Together with the two sets above they cover the 21 inputs exactly."""
assert set(INPUT_NAMES) == LIVE_INPUTS | PRIOR_INPUTS | CONTEXT_INPUTS
assert not (LIVE_INPUTS & PRIOR_INPUTS or LIVE_INPUTS & CONTEXT_INPUTS)
assert not PRIOR_INPUTS & CONTEXT_INPUTS


# ============================================================================== Spark numerics
def spark_round(x: float | None, digits: int) -> float | None:
    """Spark's ``round(x, digits)`` for a double (HALF_UP on the shortest decimal form)."""
    if x is None:
        return None
    if not math.isfinite(x):
        return x
    q = Decimal(repr(float(x))).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP)
    return float(q)


def regr_slope(points: Iterable[tuple[float | None, float | None]]) -> float | None:
    """Spark's ``regr_slope(y, x)`` over ``(y, x)`` in order; rows with a null are skipped."""
    n = 0.0
    x_avg = y_avg = ck = 0.0
    m2 = 0.0
    for y, x in points:
        if y is None or x is None:
            continue
        n = n + 1.0
        dx = x - x_avg
        dy = y - y_avg
        new_y_avg = y_avg + dy / n
        ck = ck + dx * (y - new_y_avg)
        m2 = m2 + dx * (dx - dx / n)
        x_avg = x_avg + dx / n
        y_avg = new_y_avg
    if m2 == 0.0:
        return None
    return ck / m2


def _diff(a: float | None, b: float | None, digits: int) -> float | None:
    return spark_round(a - b, digits) if a is not None and b is not None else None


# ============================================================================== timing
@dataclass(frozen=True)
class LapInput:
    """One received ``lap`` event (what the stream knows about a car's lap)."""

    driver_id: str
    lap: int
    position: int | None
    lap_time_s: float | None
    race_time_s: float
    compound: str | None = None
    tyre_age: int | None = None
    track_neutral: bool = False
    track_red_flag: bool = False


@dataclass(frozen=True)
class TimedLap:
    """A received lap with the timing ``df_jobs.timing_gaps`` derives (state at the line)."""

    lap: LapInput
    cumulative_time_s: float | None
    gap_leader_s: float | None
    gap_ahead_s: float | None
    ahead_driver_id: str | None
    gap_ahead_change_1l: float | None
    gap_ahead_change_3l: float | None


def compute_timing(laps: Iterable[LapInput]) -> dict[tuple[str, int], TimedLap]:
    """Port of ``df_jobs.timing_gaps`` over the laps received so far (exact)."""
    by_driver: dict[str, list[LapInput]] = {}
    for r in laps:
        by_driver.setdefault(r.driver_id, []).append(r)
    cum: dict[tuple[str, int], float | None] = {}
    for driver, rows in by_driver.items():
        rows.sort(key=lambda r: r.lap)
        total: float | None = None
        unknown = False
        for r in rows:
            if r.lap_time_s is None:
                unknown = True
            else:
                total = r.lap_time_s if total is None else total + r.lap_time_s
            cum[(driver, r.lap)] = None if unknown or total is None else spark_round(total, 3)
    by_lap: dict[int, list[LapInput]] = {}
    for rows in by_driver.values():
        for r in rows:
            by_lap.setdefault(r.lap, []).append(r)
    gap_leader: dict[tuple[str, int], float | None] = {}
    ahead: dict[tuple[str, int], tuple[str | None, float | None]] = {}
    for lap_no, rows in by_lap.items():
        leaders = [
            cum[(r.driver_id, lap_no)]
            for r in rows
            if r.position == 1 and cum[(r.driver_id, lap_no)] is not None
        ]
        leader = max((c for c in leaders if c is not None), default=None)
        ordered = sorted(rows, key=lambda r: (r.position is not None, r.position or 0))
        prev: LapInput | None = None
        for r in ordered:
            c = cum[(r.driver_id, lap_no)]
            gap_leader[(r.driver_id, lap_no)] = (
                spark_round(c - leader, 3) if c is not None and leader is not None else None
            )
            if prev is None:
                ahead[(r.driver_id, lap_no)] = (None, None)
            else:
                pc = cum[(prev.driver_id, lap_no)]
                ahead[(r.driver_id, lap_no)] = (
                    prev.driver_id,
                    spark_round(c - pc, 3) if c is not None and pc is not None else None,
                )
            prev = r
    out: dict[tuple[str, int], TimedLap] = {}
    for driver, rows in by_driver.items():
        for i, r in enumerate(rows):
            key = (driver, r.lap)
            ahead_id, gap = ahead[key]
            changes: dict[int, float | None] = {}
            for k in (1, 3):
                change = None
                if i >= k and rows[i - k].lap == r.lap - k:
                    prev_ahead, prev_gap = ahead[(driver, rows[i - k].lap)]
                    if prev_ahead is not None and ahead_id is not None and prev_ahead == ahead_id:
                        change = _diff(gap, prev_gap, 3)
                changes[k] = change
            out[key] = TimedLap(
                lap=r,
                cumulative_time_s=cum[key],
                gap_leader_s=gap_leader[key],
                gap_ahead_s=gap,
                ahead_driver_id=ahead_id,
                gap_ahead_change_1l=changes[1],
                gap_ahead_change_3l=changes[3],
            )
    return out


# ============================================================================== track status
def is_restart(
    neutral: Mapping[int, bool], lap: int, red_flag: frozenset[int] = frozenset()
) -> bool:
    """Port of ``features.undercut._is_restart`` (the 1-lap / 3-lap red-flag restart rule)."""
    if neutral.get(lap, False):
        return False
    if any(neutral.get(lap - b, False) for b in range(1, RESTART_BUFFER_LAPS + 1)):
        return True
    end = next(
        (lap - b for b in range(1, RED_FLAG_RESTART_LAPS + 1) if neutral.get(lap - b, False)),
        None,
    )
    if end is None:
        return False
    j = end
    while neutral.get(j, False):
        if j in red_flag:
            return True
        j -= 1
    return False


# ============================================================================== pace and slope
def is_green(t: TimedLap, in_laps: frozenset[int], out_laps: frozenset[int]) -> bool:
    """The green racing lap of ``undercut.lap_decision_features`` (lap >= 2, not an in- or
    out-lap, not neutralised, time known)."""
    r = t.lap
    return (
        r.lap >= 2
        and r.lap not in in_laps
        and r.lap not in out_laps
        and not r.track_neutral
        and r.lap_time_s is not None
    )


def pace_last3(green_times: Sequence[float]) -> float | None:
    """Mean of the last (up to) 3 green lap times of the stint, rounded to 3 (Spark order)."""
    last = list(green_times)[-3:]
    if not last:
        return None
    total = 0.0
    for t in last:
        total = total + t
    return spark_round(total / len(last), 3)


def deg_slope(points: Sequence[tuple[float, int | None]]) -> float | None:
    """Least-squares slope of green lap time on tyre age (>= 3 laps with a tyre age), round 4."""
    n = sum(1 for _, age in points if age is not None)
    if n < 3:
        return None
    return spark_round(regr_slope((y, None if age is None else float(age)) for y, age in points), 4)


@dataclass(frozen=True)
class StintView:
    """One car's stint up to the decision lap: the start lap and the green laps so far."""

    start_lap: int
    green: tuple[TimedLap, ...]

    @property
    def pace_last3_s(self) -> float | None:
        return pace_last3([g.lap.lap_time_s for g in self.green if g.lap.lap_time_s is not None])

    @property
    def deg_slope_s_per_lap(self) -> float | None:
        return deg_slope(
            [(g.lap.lap_time_s, g.lap.tyre_age) for g in self.green if g.lap.lap_time_s is not None]
        )


def stint_start(lap: int, in_laps: Iterable[int]) -> int:
    """The first lap of the stint containing ``lap``: 1 + the last in-lap before it (or 1)."""
    return 1 + max((p for p in in_laps if p < lap), default=0)


def stint_view(
    timing: Mapping[tuple[str, int], TimedLap],
    driver: str,
    lap: int,
    in_laps: frozenset[int],
    out_laps: frozenset[int],
) -> StintView:
    start = stint_start(lap, in_laps)
    rows = [timing[(driver, n)] for n in range(start, lap + 1) if (driver, n) in timing]
    return StintView(start, tuple(t for t in rows if is_green(t, in_laps, out_laps)))


# ============================================================================== rejoin traffic
def rejoin_traffic(
    timing: Mapping[tuple[str, int], TimedLap],
    drivers_at_lap: Iterable[str],
    attacker: str,
    lap: int,
    pit_loss_prior_s: float | None,
) -> tuple[int | None, float | None]:
    """Port of ``features.undercut._rejoin_traffic`` (cars just ahead of A's rejoin point)."""
    a = timing.get((attacker, lap))
    loss = pit_loss_prior_s
    if a is None or a.cumulative_time_s is None or loss is None:
        return None, None
    a_cum = a.cumulative_time_s
    margins = []
    for x in sorted(drivers_at_lap):
        if x == attacker:
            continue
        row = timing[(x, lap)]
        prev = timing.get((x, lap - 1))
        gaps = [
            t - a_cum
            for t in (row.cumulative_time_s, prev.cumulative_time_s if prev else None)
            if t is not None and t - a_cum > 0
        ]
        if not gaps:
            continue
        g = min(gaps)
        if loss - TRAFFIC_WINDOW_S < g <= loss:
            margins.append(round(loss - g, 3))
    return len(margins), (min(margins) if margins else None)


# ============================================================================== priors
@dataclass(frozen=True)
class DriverPrior:
    """One driver's decision-time out-lap priors (``outlap_features``, max per driver)."""

    pace_residual_s: float | None = None
    pace_source: str | None = None
    stop_delta_s: float | None = None


@dataclass(frozen=True)
class RacePriors:
    """Everything the stream knows about a race before it starts (loaded once per race).

    ``pit_loss_*`` and the driver priors come from feature rows built from earlier races only;
    ``team_keys`` is the lineage key of each entrant's constructor (the entry list).
    """

    season: int
    round: int
    pit_loss_prior_s: float | None = None
    pit_loss_source: str | None = None
    team_keys: Mapping[str, str | None] = field(default_factory=dict)
    drivers: Mapping[str, DriverPrior] = field(default_factory=dict)

    def driver(self, driver_id: str) -> DriverPrior:
        return self.drivers.get(driver_id, DriverPrior())

    def to_json(self) -> dict[str, Any]:
        return {
            "season": self.season,
            "round": self.round,
            "pit_loss_prior_s": self.pit_loss_prior_s,
            "pit_loss_source": self.pit_loss_source,
            "team_keys": dict(self.team_keys),
            "drivers": {
                d: [p.pace_residual_s, p.pace_source, p.stop_delta_s]
                for d, p in sorted(self.drivers.items())
            },
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> RacePriors:
        return cls(
            season=int(data["season"]),
            round=int(data["round"]),
            pit_loss_prior_s=data.get("pit_loss_prior_s"),
            pit_loss_source=data.get("pit_loss_source"),
            team_keys=dict(data.get("team_keys", {})),
            drivers={
                d: DriverPrior(v[0], v[1], v[2]) for d, v in dict(data.get("drivers", {})).items()
            },
        )


# ============================================================================== the 21 inputs
def decision_inputs(
    timing: Mapping[tuple[str, int], TimedLap],
    drivers_at_lap: Iterable[str],
    pair: CandidatePair,
    priors: RacePriors,
    race_laps: int | None,
    in_laps: Mapping[str, frozenset[int]],
    out_laps: Mapping[str, frozenset[int]],
) -> dict[str, Any]:
    """The 21 ``real`` inputs of ``pair`` at its decision lap, exactly as the batch derives them."""
    lap = pair.decision_lap
    a_id, d_id = pair.attacker_id, pair.defender_id
    a = timing[(a_id, lap)]
    none: frozenset[int] = frozenset()
    a_stint = stint_view(timing, a_id, lap, in_laps.get(a_id, none), out_laps.get(a_id, none))
    d_stint = stint_view(timing, d_id, lap, in_laps.get(d_id, none), out_laps.get(d_id, none))
    a_slope, d_slope = a_stint.deg_slope_s_per_lap, d_stint.deg_slope_s_per_lap
    traffic, nearest = rejoin_traffic(timing, drivers_at_lap, a_id, lap, priors.pit_loss_prior_s)
    pa, pd = priors.driver(a_id), priors.driver(d_id)
    out: dict[str, Any] = {
        "gap_s": pair.gap_s,
        "gap_ahead_change_1l": a.gap_ahead_change_1l,
        "gap_ahead_change_3l": a.gap_ahead_change_3l,
        "attacker_position": a.lap.position,
        "pace_delta_last3_s": _diff(a_stint.pace_last3_s, d_stint.pace_last3_s, 3),
        "attacker_deg_slope_s_per_lap": a_slope,
        "defender_deg_slope_s_per_lap": d_slope,
        "deg_slope_delta_s_per_lap": _diff(a_slope, d_slope, 4),
        "laps_remaining": race_laps - lap if race_laps is not None else None,
        "pit_loss_prior_s": priors.pit_loss_prior_s,
        "pit_loss_source": priors.pit_loss_source,
        "rejoin_traffic_cars": traffic,
        "rejoin_nearest_ahead_s": nearest,
        "attacker_expected_outlap_pace_residual_s": pa.pace_residual_s,
        "attacker_expected_outlap_pace_source": pa.pace_source or "none",
        "defender_expected_outlap_pace_residual_s": pd.pace_residual_s,
        "defender_expected_outlap_pace_source": pd.pace_source or "none",
        "attacker_expected_stop_delta_s": pa.stop_delta_s,
        "defender_expected_stop_delta_s": pd.stop_delta_s,
        "attacker_stint_laps": lap - a_stint.start_lap + 1,
        "defender_stint_laps": lap - d_stint.start_lap + 1,
    }
    assert tuple(out) == INPUT_NAMES
    return out
