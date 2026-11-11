"""Undercut attempts, labels and decision-time features (#13). Definition:
``docs/label_definition.md``, section "Undercut attempts".

* **Candidates** come from the shared rule in ``features.undercut_rule``, which the real-time
  detector (``streaming.detector``) also uses. It is applied per race in pure Python
  (``evaluate_race``), distributed with the RDD API (``keyBy`` -> ``cogroup`` -> ``flatMap``).
* **Attempt**: a candidate at the end of lap L-1 whose attacker A pits on lap L (the Jolpica pit
  ``lap`` is the in-lap). The defender D's first stop in [L+1, L+k] gives the label lap (D's
  out-lap); without one the outcome is ``defender_stayed_out``.
* **Label** = 1 when A is ahead of D on the timing line at the end of the label lap.
* ``detector_eligible`` is True when the pair passed the shared rule at L-1, i.e. the real-time
  detector would have flagged it before the stop.
* **Exclusions** are evaluated over the window L-1 .. label lap (or L-1 .. L+k). The first reason
  in ``EXCLUSION_PRIORITY`` sets ``outcome_category = excluded_<reason>``; all reasons are kept in
  ``exclusion_reasons``.
* **Grid**: every (G, k) in ``GAP_GRID`` x ``WINDOW_GRID`` is emitted in one run.
* **Decision-time features** (``DECISION_FEATURES``) use only laps <= L-1 of the race and
  strictly earlier races. ``oracle_*`` columns use the stop itself and later laps (ORACLE
  ablation only); ``outcome_*`` columns describe what happened and are never features.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from f1sa.etl.clean import conform, table_checksum
from f1sa.features import schemas as FS
from f1sa.features.lineage import history_key_col
from f1sa.features.undercut_rule import (
    DEFAULT_GAP_S,
    DEFAULT_WINDOW_LAPS,
    GAP_GRID,
    MIN_ATTEMPT_LAP,
    RULE_REASONS,
    WINDOW_GRID,
    CandidatePair,
    DriverLapState,
    LapSnapshot,
    RuleParams,
    candidate_pairs,
)

RESTART_BUFFER_LAPS = 1
"""Green laps after a neutralised run that still count as neutralised (the restart lap)."""
RED_FLAG_RESTART_LAPS = 3
"""Green laps after a neutralised run that contains a red-flag lap that still count as
neutralised. After a red flag the field restarts bunched on free new tyres, and the longest
decision-feature lookback is 3 laps (``gap_ahead_change_3l`` compares L-1 with L-4). With 3
restart laps, an eligible decision lap L-1 is at least 4 laps after the run, so no decision
feature reaches back into the stoppage or the restart."""
SLOW_STOP_MARGIN_S = 3.0
"""A stop this much slower than the race's median green stop is flagged ``*_slow_stop``."""
DRIVE_THROUGH_MARGIN_S = 2.0
"""A stop this much faster than the race's median green stop is a suspected drive-through."""
INCIDENT_LAP_RATIO = 1.07
"""A green racing lap slower than this multiple of the race's green median is an incident."""
TRAFFIC_WINDOW_S = 3.0
"""Cars up to this far ahead of A's expected rejoin point count as rejoin traffic."""
WET_COMPOUNDS = frozenset({"INTERMEDIATE", "WET"})

EXCLUSION_PRIORITY: tuple[str, ...] = (
    "missing_data",
    "inconsistent_timing",
    "early_lap",
    "teammates",
    "traffic_between",
    "lapped",
    "in_pit",
    "neutralised",
    "same_lap_stop",
    "retired",
    "long_stop",
    "penalty_suspected",
    "attacker_restop",
    "incident",
    "wet",
    "race_end",
)
OUTCOME_CATEGORIES: tuple[str, ...] = (
    "success",
    "fail",
    "defender_stayed_out",
    *(f"excluded_{r}" for r in EXCLUSION_PRIORITY),
)
assert set(RULE_REASONS) <= set(EXCLUSION_PRIORITY)

DECISION_FEATURES: tuple[str, ...] = (
    "gap_s",
    "gap_ahead_change_1l",
    "gap_ahead_change_3l",
    "attacker_position",
    "attacker_tyre_age_laps",
    "defender_tyre_age_laps",
    "tyre_age_delta_laps",
    "attacker_compound",
    "defender_compound",
    "compound_is_synthetic",
    "attacker_pace_last3_s",
    "defender_pace_last3_s",
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
)
"""The only columns a model or the detector may use (all known at the end of lap L-1)."""
LEGACY_FEATURES: tuple[str, ...] = ("attacker_expected_outlap_delta_s_legacy",)
"""Decision-time safe but collinear with pit_loss; kept for the S16 comparison only."""
ORACLE_COLUMNS: tuple[str, ...] = (
    "oracle_attacker_realised_outlap_delta_s",
    "oracle_attacker_outlap_pace_residual_s",
    "oracle_attacker_stop_delta_s",
)
OUTCOME_COLUMNS: tuple[str, ...] = (
    "outcome_defender_pit_lap",
    "outcome_label_lap",
    "outcome_attacker_position",
    "outcome_defender_position",
    "outcome_gap_s",
    "outcome_attacker_stop_s",
    "outcome_defender_stop_s",
    "outcome_attacker_slow_stop",
    "outcome_defender_slow_stop",
    "outcome_attacker_double_stack",
    "outcome_defender_double_stack",
)

_RACE = ["season", "round"]
_DRIVER = ["season", "round", "driver_id"]


# ============================================================================== pure Python
@dataclass(frozen=True)
class LapRow:
    """One clean lap of one car (the subset of ``laps_enriched`` the evaluation needs)."""

    driver_id: str
    lap: int
    position: int | None
    lap_time_s: float | None
    cumulative_time_s: float | None
    gap_ahead_s: float | None
    gap_leader_s: float | None
    is_pit_in_lap: bool = False
    is_pit_out_lap: bool = False
    track_neutral: bool = False
    track_red_flag: bool = False
    compound: str | None = None
    tyre_age_laps: int | None = None
    team_key: str | None = None


@dataclass(frozen=True)
class StopRow:
    driver_id: str
    lap: int
    duration_s: float | None
    is_long_stop: bool = False
    time_of_day: str | None = None


@dataclass
class RaceData:
    season: int
    round: int
    laps: list[LapRow]
    stops: list[StopRow]
    race_laps: int | None = None
    pit_loss_prior_s: float | None = None
    _by_key: dict[tuple[str, int], LapRow] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        self.laps = sorted(self.laps, key=lambda r: (r.lap, r.driver_id))
        self.stops = sorted(self.stops, key=lambda s: (s.lap, s.driver_id, s.time_of_day or ""))
        for r in self.laps:
            key = (r.driver_id, r.lap)
            if key in self._by_key:
                raise ValueError(f"{self.season} R{self.round}: duplicate lap row {key}")
            self._by_key[key] = r

    def row(self, driver_id: str, lap: int) -> LapRow | None:
        return self._by_key.get((driver_id, lap))

    def rows_at(self, lap: int) -> list[LapRow]:
        return [r for r in self.laps if r.lap == lap]


def _neutral_laps(race: RaceData) -> dict[int, bool]:
    out: dict[int, bool] = {}
    for r in race.laps:
        out[r.lap] = out.get(r.lap, False) or r.track_neutral
    return out


def defender_response_lap(lap: int, k: int, d_pits: Iterable[int]) -> int | None:
    """D's first stop in L+1 .. L+k (the response window), or None."""
    return next((p for p in sorted(d_pits) if lap + 1 <= p <= lap + k), None)


def attempt_window(lap: int, k: int, d_pits: Iterable[int]) -> range:
    """Laps checked for track status: L-1 through D's out-lap (D's stop + 1), or L+k when D
    does not respond within the window."""
    d_pit = defender_response_lap(lap, k, d_pits)
    end = d_pit + 1 if d_pit is not None else lap + k
    return range(lap - 1, end + 1)


def _red_flag_laps(race: RaceData) -> frozenset[int]:
    return frozenset(r.lap for r in race.laps if r.track_red_flag)


def _is_restart(
    neutral: Mapping[int, bool], lap: int, red_flag: frozenset[int] = frozenset()
) -> bool:
    """A green lap within ``RESTART_BUFFER_LAPS`` after a neutralised lap, or within
    ``RED_FLAG_RESTART_LAPS`` after a neutralised run that contains a red-flag lap."""
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


def snapshot_at(
    race: RaceData,
    lap: int,
    neutral: Mapping[int, bool] | None = None,
    red_flag: frozenset[int] | None = None,
) -> LapSnapshot:
    """The rule's view of the race at the end of ``lap`` (uses laps ``lap`` and ``lap - 1``)."""
    flags = neutral if neutral is not None else _neutral_laps(race)
    red = red_flag if red_flag is not None else _red_flag_laps(race)
    rows = race.rows_at(lap)
    leader = next((r for r in rows if r.position == 1), None)
    drivers = []
    for r in rows:
        prev = race.row(r.driver_id, lap - 1)
        drivers.append(
            DriverLapState(
                driver_id=r.driver_id,
                position=r.position,
                gap_ahead_s=r.gap_ahead_s,
                gap_leader_s=r.gap_leader_s,
                crossing_s=r.cumulative_time_s,
                prev_crossing_s=prev.cumulative_time_s if prev else None,
                team_key=r.team_key,
                on_in_lap=r.is_pit_in_lap,
                on_out_lap=r.is_pit_out_lap,
                running=True,
                tyre_compound=r.compound,
                tyre_age_laps=r.tyre_age_laps,
                last_lap_time_s=r.lap_time_s,
            )
        )
    return LapSnapshot(
        season=race.season,
        round=race.round,
        lap=lap,
        drivers=tuple(drivers),
        track_green=not flags.get(lap, False),
        restart_lap=_is_restart(flags, lap, red),
        leader_lap_time_s=leader.lap_time_s if leader else None,
    )


def _stop_summary(stops: Sequence[StopRow]) -> dict[tuple[str, int], StopRow]:
    """One stop per (driver, lap): durations summed, long if any stop was long."""
    grouped: dict[tuple[str, int], list[StopRow]] = defaultdict(list)
    for s in stops:
        grouped[(s.driver_id, s.lap)].append(s)
    out = {}
    for key, group in grouped.items():
        durations = [s.duration_s for s in group]
        total = (
            round(sum(d for d in durations if d is not None), 3)
            if all(d is not None for d in durations)
            else None
        )
        times = [s.time_of_day for s in group if s.time_of_day]
        out[key] = StopRow(
            key[0], key[1], total, any(s.is_long_stop for s in group), min(times) if times else None
        )
    return out


def _green_medians(
    race: RaceData, neutral: Mapping[int, bool]
) -> tuple[float | None, float | None]:
    """(median green racing lap, median green non-long stop duration) of the race."""
    laps = [
        r.lap_time_s
        for r in race.laps
        if r.lap >= 2
        and r.lap_time_s is not None
        and not (r.is_pit_in_lap or r.is_pit_out_lap or neutral.get(r.lap, False))
    ]
    stops = [
        s.duration_s
        for s in race.stops
        if s.duration_s is not None
        and not s.is_long_stop
        and not neutral.get(s.lap, False)
        and not neutral.get(s.lap + 1, False)
    ]
    return (median(laps) if laps else None, median(stops) if stops else None)


def _rejoin_traffic(race: RaceData, attacker: str, lap: int) -> tuple[int | None, float | None]:
    """Cars ahead of A's expected rejoin point at the end of ``lap`` (laps <= ``lap`` only).

    A car's on-track gap behind A is the smallest positive (its crossing of ``lap`` or ``lap - 1``
    minus A's crossing of ``lap``). Cars with a gap in (pit_loss - TRAFFIC_WINDOW_S, pit_loss]
    will be just ahead of A when A rejoins.
    """
    a = race.row(attacker, lap)
    loss = race.pit_loss_prior_s
    if a is None or a.cumulative_time_s is None or loss is None:
        return None, None
    margins = []
    for x in race.rows_at(lap):
        if x.driver_id == attacker:
            continue
        prev = race.row(x.driver_id, lap - 1)
        gaps = [
            t - a.cumulative_time_s
            for t in (x.cumulative_time_s, prev.cumulative_time_s if prev else None)
            if t is not None and t - a.cumulative_time_s > 0
        ]
        if not gaps:
            continue
        g = min(gaps)
        if loss - TRAFFIC_WINDOW_S < g <= loss:
            margins.append(round(loss - g, 3))
    return len(margins), (min(margins) if margins else None)


@dataclass(frozen=True)
class _RaceContext:
    race: RaceData
    neutral: Mapping[int, bool]
    red_flag: frozenset[int]
    stops: Mapping[tuple[str, int], StopRow]
    pit_laps: Mapping[str, frozenset[int]]
    green_lap_median: float | None
    green_stop_median: float | None
    leaders: Mapping[int, LapRow]


def _context(race: RaceData) -> _RaceContext:
    neutral = _neutral_laps(race)
    stops = _stop_summary(race.stops)
    pit: dict[str, set[int]] = defaultdict(set)
    for driver, lap in stops:
        pit[driver].add(lap)
    lap_med, stop_med = _green_medians(race, neutral)
    leaders = {r.lap: r for r in race.laps if r.position == 1}
    return _RaceContext(
        race,
        neutral,
        _red_flag_laps(race),
        stops,
        {d: frozenset(v) for d, v in pit.items()},
        lap_med,
        stop_med,
        leaders,
    )


def _lapped_in_window(ctx: _RaceContext, driver: str, lap: int) -> bool:
    """Exact: the car's crossing of ``lap`` comes after the leader's crossing of ``lap + 1``."""
    row = ctx.race.row(driver, lap)
    nxt = ctx.leaders.get(lap + 1)
    if row is None or row.cumulative_time_s is None or nxt is None:
        return False
    return nxt.cumulative_time_s is not None and row.cumulative_time_s > nxt.cumulative_time_s


def _double_stack(ctx: _RaceContext, driver: str, lap: int) -> bool:
    """A teammate stopped on the same lap before ``driver`` (or the order is unknown)."""
    me = ctx.stops.get((driver, lap))
    row = ctx.race.row(driver, lap)
    if me is None or row is None or row.team_key is None:
        return False
    for (other, other_lap), stop in ctx.stops.items():
        if other == driver or other_lap != lap:
            continue
        mate = ctx.race.row(other, lap)
        if mate is None or mate.team_key != row.team_key:
            continue
        if me.time_of_day is None or stop.time_of_day is None or stop.time_of_day < me.time_of_day:
            return True
    return False


def _ordered(reasons: Iterable[str]) -> list[str]:
    seen = set(reasons)
    return [r for r in EXCLUSION_PRIORITY if r in seen]


def _evaluate(ctx: _RaceContext, pair: CandidatePair, k: int) -> dict[str, Any]:
    race, a_id, d_id, lap = ctx.race, pair.attacker_id, pair.defender_id, pair.attempt_lap
    reasons = set(pair.reasons)
    d_pits = ctx.pit_laps.get(d_id, frozenset())
    a_pits = ctx.pit_laps.get(a_id, frozenset())
    if lap in d_pits:
        reasons.add("same_lap_stop")
    d_pit = defender_response_lap(lap, k, d_pits)
    window = attempt_window(lap, k, d_pits)
    end = window[-1]
    if race.race_laps is None or race.race_laps - lap < k + 2:
        reasons.add("race_end")
    if any(race.row(x, j) is None for x in (a_id, d_id) for j in range(lap, end + 1)):
        reasons.add("retired")
    if any(ctx.neutral.get(j, False) or _is_restart(ctx.neutral, j, ctx.red_flag) for j in window):
        reasons.add("neutralised")
    if any(_lapped_in_window(ctx, x, j) for x in (a_id, d_id) for j in range(lap, end + 1)):
        reasons.add("lapped")
    a_stop = ctx.stops.get((a_id, lap))
    d_stop = ctx.stops.get((d_id, d_pit)) if d_pit is not None else None
    stops = [s for s in (a_stop, d_stop) if s is not None]
    if any(s.is_long_stop for s in stops):
        reasons.add("long_stop")
    med = ctx.green_stop_median

    def _dur_flag(s: StopRow | None, slow: bool) -> bool:
        if s is None or s.duration_s is None or med is None or s.is_long_stop:
            return False
        if slow:
            return s.duration_s > med + SLOW_STOP_MARGIN_S
        return s.duration_s < med - DRIVE_THROUGH_MARGIN_S

    if any(_dur_flag(s, slow=False) for s in stops):
        reasons.add("penalty_suspected")
    if any(p in a_pits for p in range(lap + 1, end + 1)):
        reasons.add("attacker_restop")
    for x in (a_id, d_id):
        for j in window:
            r = race.row(x, j)
            if r is None:
                continue
            if (
                ctx.green_lap_median is not None
                and r.lap_time_s is not None
                and j >= 2
                and not (r.is_pit_in_lap or r.is_pit_out_lap or ctx.neutral.get(j, False))
                and r.lap_time_s > INCIDENT_LAP_RATIO * ctx.green_lap_median
            ):
                reasons.add("incident")
            if r.compound is not None and r.compound.upper() in WET_COMPOUNDS:
                reasons.add("wet")
    ordered = _ordered(reasons)
    label_lap = d_pit + 1 if d_pit is not None else None
    a_end = race.row(a_id, label_lap) if label_lap is not None else None
    d_end = race.row(d_id, label_lap) if label_lap is not None else None
    a_pos = a_end.position if a_end else None
    d_pos = d_end.position if d_end else None
    if label_lap is not None and not ordered and (a_pos is None or d_pos is None):
        ordered = ["missing_data"]
    outcome_gap = (
        round(d_end.cumulative_time_s - a_end.cumulative_time_s, 3)
        if a_end
        and d_end
        and a_end.cumulative_time_s is not None
        and d_end.cumulative_time_s is not None
        else None
    )
    label: int | None = None
    if ordered:
        category = f"excluded_{ordered[0]}"
    elif d_pit is None:
        category = "defender_stayed_out"
    else:
        assert a_pos is not None and d_pos is not None
        label = int(a_pos < d_pos)
        category = "success" if label else "fail"
    traffic, nearest = _rejoin_traffic(race, a_id, pair.decision_lap)
    return {
        "season": race.season,
        "round": race.round,
        "attacker_id": a_id,
        "defender_id": d_id,
        "lap": lap,
        "decision_lap": pair.decision_lap,
        "response_window_laps": k,
        "detector_eligible": pair.eligible,
        "gap_s": pair.gap_s,
        "outcome_category": category,
        "label": label,
        "exclusion_reasons": ordered,
        "rejoin_traffic_cars": traffic,
        "rejoin_nearest_ahead_s": nearest,
        "outcome_defender_pit_lap": d_pit,
        "outcome_label_lap": label_lap,
        "outcome_attacker_position": a_pos,
        "outcome_defender_position": d_pos,
        "outcome_gap_s": outcome_gap,
        "outcome_attacker_stop_s": a_stop.duration_s if a_stop else None,
        "outcome_defender_stop_s": d_stop.duration_s if d_stop else None,
        "outcome_attacker_slow_stop": _dur_flag(a_stop, slow=True),
        "outcome_defender_slow_stop": _dur_flag(d_stop, slow=True),
        "outcome_attacker_double_stack": _double_stack(ctx, a_id, lap),
        "outcome_defender_double_stack": (
            _double_stack(ctx, d_id, d_pit) if d_pit is not None else False
        ),
    }


def evaluate_race(
    race: RaceData,
    gap_grid: Sequence[float] = GAP_GRID,
    window_grid: Sequence[int] = WINDOW_GRID,
    min_attempt_lap: int = MIN_ATTEMPT_LAP,
) -> list[dict[str, Any]]:
    """Every undercut attempt of one race for every (G, k), as plain dicts (sorted).

    An attempt is a candidate pair at the end of lap L-1 (``undercut_rule.candidate_pairs``)
    whose attacker has a pit stop on lap L. Lap-1 stops never qualify (there is no lap 0).
    """
    if not gap_grid or not window_grid:
        raise ValueError("gap_grid and window_grid must not be empty")
    if any(k < 1 for k in window_grid):
        raise ValueError(f"response windows must be >= 1, got {list(window_grid)}")
    ctx = _context(race)
    snapshots: dict[int, LapSnapshot] = {}
    out: list[dict[str, Any]] = []
    attempts = sorted({(lap, driver) for driver, laps in ctx.pit_laps.items() for lap in laps})
    for lap, attacker in attempts:
        decision = lap - 1
        if decision < 1 or race.row(attacker, decision) is None:
            continue
        if decision not in snapshots:
            snapshots[decision] = snapshot_at(race, decision, ctx.neutral, ctx.red_flag)
        for g in sorted(set(gap_grid)):
            params = RuleParams(gap_threshold_s=g, min_attempt_lap=min_attempt_lap)
            pair = next(
                (
                    p
                    for p in candidate_pairs(snapshots[decision], params)
                    if p.attacker_id == attacker
                ),
                None,
            )
            if pair is None:
                continue
            for k in sorted(set(window_grid)):
                row = _evaluate(ctx, pair, k)
                row["gap_threshold_s"] = float(g)
                row["is_default_params"] = g == DEFAULT_GAP_S and k == DEFAULT_WINDOW_LAPS
                out.append(row)
    return sorted(
        out,
        key=lambda r: (r["lap"], r["attacker_id"], r["gap_threshold_s"], r["response_window_laps"]),
    )


# ============================================================================== Spark
def _opt_bool(x: Any) -> bool:
    return bool(x) if x is not None else False


def _race_from_rows(key: tuple[int, int], laps: Iterable[Any], stops: Iterable[Any]) -> RaceData:
    lap_list = list(laps)
    race_laps = next((r["race_laps"] for r in lap_list if r["race_laps"] is not None), None)
    loss = next(
        (r["pit_loss_prior_s"] for r in lap_list if r["pit_loss_prior_s"] is not None), None
    )
    return RaceData(
        season=key[0],
        round=key[1],
        laps=[
            LapRow(
                driver_id=r["driver_id"],
                lap=r["lap"],
                position=r["position"],
                lap_time_s=r["lap_time_s"],
                cumulative_time_s=r["cumulative_time_s"],
                gap_ahead_s=r["gap_ahead_s"],
                gap_leader_s=r["gap_leader_s"],
                is_pit_in_lap=_opt_bool(r["is_pit_in_lap"]),
                is_pit_out_lap=_opt_bool(r["is_pit_out_lap"]),
                track_neutral=_opt_bool(r["track_neutral"]),
                track_red_flag=_opt_bool(r["track_red_flag"]),
                compound=r["compound"],
                tyre_age_laps=r["tyre_age_laps"],
                team_key=r["team_key"],
            )
            for r in lap_list
        ],
        stops=[
            StopRow(
                driver_id=s["driver_id"],
                lap=s["lap"],
                duration_s=s["duration_s"],
                is_long_stop=_opt_bool(s["is_long_stop"]),
                time_of_day=s["time_of_day"],
            )
            for s in stops
            if s["lap"] is not None
        ],
        race_laps=race_laps,
        pit_loss_prior_s=loss,
    )


def _track_neutral() -> Column:
    flags = ["is_neutralised_lap", "is_sc_lap", "is_vsc_lap", "is_red_flag_lap"]
    out = F.lit(False)
    for c in flags:
        out = out | F.coalesce(F.col(c), F.lit(False))
    return out


def race_lap_rows(laps_enriched: DataFrame, pit_loss: DataFrame) -> DataFrame:
    """The per-lap input of ``evaluate_race`` (with the race's prior pit loss)."""
    loss = pit_loss.select(*_RACE, "pit_loss_prior_s")
    return laps_enriched.select(
        *_DRIVER,
        "lap",
        "position",
        "lap_time_s",
        "cumulative_time_s",
        "gap_ahead_s",
        "gap_leader_s",
        "is_pit_in_lap",
        "is_pit_out_lap",
        _track_neutral().alias("track_neutral"),
        F.coalesce(F.col("is_red_flag_lap"), F.lit(False)).alias("track_red_flag"),
        "compound",
        "tyre_age_laps",
        history_key_col("constructor_id").alias("team_key"),
        "race_laps",
    ).join(loss, _RACE, "left")


def attempt_rows(
    spark: SparkSession,
    laps_enriched: DataFrame,
    pitstops: DataFrame,
    pit_loss: DataFrame,
    gap_grid: Sequence[float] = GAP_GRID,
    window_grid: Sequence[int] = WINDOW_GRID,
) -> DataFrame:
    """Attempt identity, outcome and the Python-side features (RDD: keyBy, cogroup, flatMap)."""
    gaps, windows = tuple(gap_grid), tuple(window_grid)
    laps_rdd = race_lap_rows(laps_enriched, pit_loss).rdd.keyBy(lambda r: (r["season"], r["round"]))
    stops_rdd = pitstops.select(
        *_DRIVER, "lap", "duration_s", "is_long_stop", "time_of_day"
    ).rdd.keyBy(lambda r: (r["season"], r["round"]))

    def per_race(
        kv: tuple[tuple[int, int], tuple[Iterable[Any], Iterable[Any]]],
    ) -> list[tuple[Any, ...]]:
        key, (laps, stops) = kv
        laps = list(laps)
        if not laps:
            return []
        rows = evaluate_race(_race_from_rows(key, laps, stops), gaps, windows)
        return [tuple(r.get(f.name) for f in FS.UNDERCUT_CORE.fields) for r in rows]

    rdd = laps_rdd.cogroup(stops_rdd).flatMap(per_race)
    return spark.createDataFrame(rdd, FS.UNDERCUT_CORE)


def lap_decision_features(laps_enriched: DataFrame, stints: DataFrame) -> DataFrame:
    """Per (race, driver, lap): pace and degradation over the current stint up to that lap.

    Green racing laps only (lap >= 2, not an in- or out-lap, not neutralised, time known).
    ``pace_last3_s`` is the mean of the last (up to) 3 green laps; ``deg_slope_s_per_lap`` the
    least-squares slope of lap time on tyre age over the stint's green laps (>= 3 laps).
    """
    green = (
        (F.col("lap") >= 2)
        & ~F.coalesce(F.col("is_pit_in_lap"), F.lit(False))
        & ~F.coalesce(F.col("is_pit_out_lap"), F.lit(False))
        & ~_track_neutral()
        & F.col("lap_time_s").isNotNull()
    )
    w = (
        Window.partitionBy(*_DRIVER, "stint")
        .orderBy("lap")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    times = F.collect_list(F.when(green, F.col("lap_time_s"))).over(w)
    slope = F.regr_slope(
        F.when(green, F.col("lap_time_s")), F.when(green, F.col("tyre_age_laps").cast("double"))
    ).over(w)
    n_slope = F.count(F.when(green & F.col("tyre_age_laps").isNotNull(), F.lit(1))).over(w)
    synthetic = stints.groupBy(*_DRIVER, "stint").agg(
        F.max(F.coalesce(F.col("is_synthetic"), F.lit(False))).alias("is_synthetic")
    )
    df = (
        laps_enriched.withColumn("_times", times)
        .withColumn("_slope", slope)
        .withColumn("_n", n_slope)
        .join(synthetic, [*_DRIVER, "stint"], "left")
    )
    last3 = F.slice(F.col("_times"), F.greatest(F.size("_times") - 2, F.lit(1)), 3)
    mean3 = F.aggregate(last3, F.lit(0.0), lambda acc, x: acc + x) / F.size(last3)
    return df.select(
        *_DRIVER,
        "lap",
        "position",
        "gap_ahead_change_1l",
        "gap_ahead_change_3l",
        "tyre_age_laps",
        "compound",
        "laps_remaining",
        F.coalesce(F.col("is_synthetic"), F.lit(False)).alias("is_synthetic"),
        F.when(F.size(last3) > 0, F.round(mean3, 3)).alias("pace_last3_s"),
        F.when(F.col("_n") >= 3, F.round("_slope", 4)).alias("deg_slope_s_per_lap"),
    )


def _driver_priors(outlap: DataFrame) -> DataFrame:
    """Per (race, driver): the decision-time out-lap priors (identical on every stint row)."""
    return outlap.groupBy(*_DRIVER).agg(
        F.max("expected_outlap_pace_residual_s").alias("pace_residual"),
        F.max("expected_outlap_pace_source").alias("pace_source"),
        F.max("expected_stop_delta_s").alias("stop_delta"),
        F.max("expected_outlap_delta_s").alias("legacy_delta"),
    )


def undercut_attempts(
    spark: SparkSession,
    laps_enriched: DataFrame,
    pitstops: DataFrame,
    stints: DataFrame,
    pit_loss: DataFrame,
    outlap: DataFrame,
    gap_grid: Sequence[float] = GAP_GRID,
    window_grid: Sequence[int] = WINDOW_GRID,
) -> DataFrame:
    """The ``undercut_attempts`` table for the races in ``laps_enriched``."""
    core = attempt_rows(spark, laps_enriched, pitstops, pit_loss, gap_grid, window_grid)
    lapf = lap_decision_features(laps_enriched, stints)

    def side(prefix: str) -> DataFrame:
        return lapf.select(
            *_RACE,
            F.col("driver_id").alias(f"{prefix}_id"),
            F.col("lap").alias("decision_lap"),
            *[F.col(c).alias(f"{prefix}_{c}") for c in lapf.columns if c not in (*_DRIVER, "lap")],
        )

    priors = _driver_priors(outlap)

    def prior_side(prefix: str) -> DataFrame:
        return priors.select(
            *_RACE,
            F.col("driver_id").alias(f"{prefix}_id"),
            *[F.col(c).alias(f"{prefix}_prior_{c}") for c in priors.columns if c not in _DRIVER],
        )

    oracle = outlap.where(F.col("outlap").isNotNull()).select(
        *_RACE,
        F.col("driver_id").alias("attacker_id"),
        (F.col("outlap") - 1).alias("lap"),
        F.col("realised_outlap_delta_s").alias("oracle_attacker_realised_outlap_delta_s"),
        F.col("outlap_pace_residual_s").alias("oracle_attacker_outlap_pace_residual_s"),
        F.col("stop_delta_s").alias("oracle_attacker_stop_delta_s"),
    )
    loss = pit_loss.select(*_RACE, "pit_loss_prior_s", "pit_loss_source")
    df = (
        core.join(side("attacker"), [*_RACE, "attacker_id", "decision_lap"], "left")
        .join(side("defender"), [*_RACE, "defender_id", "decision_lap"], "left")
        .join(prior_side("attacker"), [*_RACE, "attacker_id"], "left")
        .join(prior_side("defender"), [*_RACE, "defender_id"], "left")
        .join(oracle, [*_RACE, "attacker_id", "lap"], "left")
        .join(loss, _RACE, "left")
    )

    def diff(a: str, b: str, digits: int = 3) -> Column:
        return F.round(F.col(a) - F.col(b), digits)

    df = df.select(
        "*",
        F.col("attacker_gap_ahead_change_1l").alias("gap_ahead_change_1l"),
        F.col("attacker_gap_ahead_change_3l").alias("gap_ahead_change_3l"),
        (F.col("defender_tyre_age_laps") - F.col("attacker_tyre_age_laps"))
        .cast("int")
        .alias("tyre_age_delta_laps"),
        (
            F.coalesce(F.col("attacker_is_synthetic"), F.lit(False))
            | F.coalesce(F.col("defender_is_synthetic"), F.lit(False))
        ).alias("compound_is_synthetic"),
        diff("attacker_pace_last3_s", "defender_pace_last3_s").alias("pace_delta_last3_s"),
        diff("attacker_deg_slope_s_per_lap", "defender_deg_slope_s_per_lap", 4).alias(
            "deg_slope_delta_s_per_lap"
        ),
        F.col("attacker_laps_remaining").alias("laps_remaining"),
        F.col("attacker_prior_pace_residual").alias("attacker_expected_outlap_pace_residual_s"),
        F.coalesce(F.col("attacker_prior_pace_source"), F.lit("none")).alias(
            "attacker_expected_outlap_pace_source"
        ),
        F.col("defender_prior_pace_residual").alias("defender_expected_outlap_pace_residual_s"),
        F.coalesce(F.col("defender_prior_pace_source"), F.lit("none")).alias(
            "defender_expected_outlap_pace_source"
        ),
        F.col("attacker_prior_stop_delta").alias("attacker_expected_stop_delta_s"),
        F.col("defender_prior_stop_delta").alias("defender_expected_stop_delta_s"),
        F.col("attacker_prior_legacy_delta").alias("attacker_expected_outlap_delta_s_legacy"),
    )
    return conform(df, FS.UNDERCUT_ATTEMPTS, "undercut_attempts")


# ============================================================================== counts CSV
COUNTS_HEADER = "# SMOKE TEST ONLY: 2 sample races, SYNTHETIC tyre compounds; not findings"
COUNT_COLUMNS: tuple[str, ...] = (
    "source",
    "smoke_test_only",
    "season",
    "gap_threshold_s",
    "response_window_laps",
    "candidates",
    "labelled",
    "success",
    "fail",
    "success_rate",
    "success_rate_excl_slow_stop",
    *(f"n_{c}" for c in OUTCOME_CATEGORIES),
    "n_excluded_neutralised_sc_vsc_red_flag",
    "n_excluded_neutralised_sustained_slow",
)
"""The last two columns split ``n_excluded_neutralised`` for reporting only
(``features.neutral_split``); they are empty when the split was not computed."""


LABEL_COLUMNS: tuple[str, ...] = (
    "season",
    "round",
    "lap",
    "attacker_id",
    "defender_id",
    "gap_threshold_s",
    "response_window_laps",
    "outcome_category",
    "label",
    "exclusion_reasons",
    "detector_eligible",
)
"""What decides labelling: the attempt key, its category, label, reasons and rule eligibility."""


def labels_checksum(attempts: DataFrame) -> str:
    """Order-independent checksum of ``LABEL_COLUMNS`` (feature columns are ignored)."""
    return table_checksum(attempts.select(*LABEL_COLUMNS))


def _rate(num: int, den: int) -> str:
    return f"{num / den:.4f}" if den else ""


def undercut_counts(
    attempts: DataFrame,
    source: str,
    neutral_kinds: Mapping[tuple[int, int, int, str, str, float, int], str] | None = None,
) -> list[dict[str, Any]]:
    """Per (season, G, k): attempt counts, success rates and one count per outcome category.

    ``neutral_kinds`` (``features.neutral_split.neutralised_kinds``) only fills the two
    reporting columns that split ``n_excluded_neutralised``; no other value depends on it.
    """
    rows = attempts.select(
        "season",
        "round",
        "lap",
        "attacker_id",
        "defender_id",
        "gap_threshold_s",
        "response_window_laps",
        "outcome_category",
        "outcome_attacker_slow_stop",
        "outcome_defender_slow_stop",
    ).collect()
    groups: dict[tuple[int, float, int], list[Any]] = defaultdict(list)
    for r in rows:
        groups[(r["season"], r["gap_threshold_s"], r["response_window_laps"])].append(r)
    out = []
    for (season, g, k), group in sorted(groups.items()):
        cats = [r["outcome_category"] for r in group]
        success, fail = cats.count("success"), cats.count("fail")
        clean = [
            r
            for r in group
            if r["outcome_category"] in ("success", "fail")
            and not (r["outcome_attacker_slow_stop"] or r["outcome_defender_slow_stop"])
        ]
        clean_success = sum(1 for r in clean if r["outcome_category"] == "success")
        row: dict[str, Any] = {
            "source": source,
            "smoke_test_only": "true" if source == "sample" else "false",
            "season": season,
            "gap_threshold_s": f"{g:.1f}",
            "response_window_laps": k,
            "candidates": len(group),
            "labelled": success + fail,
            "success": success,
            "fail": fail,
            "success_rate": _rate(success, success + fail),
            "success_rate_excl_slow_stop": _rate(clean_success, len(clean)),
        }
        for c in OUTCOME_CATEGORIES:
            row[f"n_{c}"] = cats.count(c)
        if neutral_kinds is not None:
            kinds = [
                neutral_kinds.get(
                    (
                        r["season"],
                        r["round"],
                        r["lap"],
                        r["attacker_id"],
                        r["defender_id"],
                        float(r["gap_threshold_s"]),
                        r["response_window_laps"],
                    )
                )
                for r in group
                if r["outcome_category"] == "excluded_neutralised"
            ]
            if None in kinds:
                raise ValueError(f"{season} G={g} k={k}: a neutralised attempt has no kind")
            row["n_excluded_neutralised_sc_vsc_red_flag"] = kinds.count("sc_vsc_red_flag")
            row["n_excluded_neutralised_sustained_slow"] = kinds.count("sustained_slow")
        out.append(row)
    return out


def write_counts_csv(rows: Sequence[Mapping[str, Any]], path: Path, *, smoke_test: bool) -> None:
    """Write the counts with a fixed column order, ``\\n`` line endings and (for the sample) the
    smoke-test header line, so reruns are byte-identical."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        if smoke_test:
            fh.write(COUNTS_HEADER + "\n")
        writer = csv.DictWriter(fh, fieldnames=list(COUNT_COLUMNS), lineterminator="\n")
        writer.writeheader()
        for r in rows:
            writer.writerow({c: r.get(c, "") for c in COUNT_COLUMNS})
    tmp.replace(path)
