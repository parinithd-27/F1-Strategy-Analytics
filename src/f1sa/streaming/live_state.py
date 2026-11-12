"""Per-race live state of the stream job (driver side, no Spark).

``RaceState`` keeps every lap and pit event received for one race and answers three questions:

* **What is the timing now?** ``timing()`` recomputes cumulative times and gaps from all received
  laps with the exact port of ``df_jobs.timing_gaps`` (about 1,300 rows per race, milliseconds).
* **Which decisions can be evaluated?** The batch evaluates the end of lap L-1 with the whole
  race; the stream may use only what has arrived. A pair with attacker A at decision lap L-1 is
  evaluated once the **watermark** (the smallest race-time high-water mark over the topic's
  partitions; Kafka keeps order within a partition) has passed ``T_A(L-1) + max(pit_loss, 0)``.
  By then every crossing the rule and the rejoin-traffic feature read has arrived: cars between
  D and A crossed before A, and the rejoin window ends ``pit_loss`` seconds after A. Two cases
  wait longer, until the car arrives or ``RETIRE_TIMEOUT_LAPS`` x the leader's lap time has passed
  (the car is then treated as retired, which the batch cannot see either):

  * the car at A's position - 1 at L-1 has not arrived (only possible with lapped cars);
  * a car with an L-2 crossing inside (D, A) or inside the rejoin window has not completed L-1
    yet (a lapped car; the batch reads its L-2 crossing through its L-1 row).

  At ``session_end`` (seen on every partition) everything left is evaluated with complete data.
* **What is the live order?** ``live_order()`` ranks cars by laps completed, then line-crossing
  time, so positions are unique at any moment (the Redis order sorted set needs that).
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from f1sa.features.undercut_rule import (
    CandidatePair,
    DriverLapState,
    LapSnapshot,
    RuleParams,
    candidate_pairs,
)
from f1sa.streaming.events import RaceEvent
from f1sa.streaming.live_features import (
    TRAFFIC_WINDOW_S,
    LapInput,
    RacePriors,
    TimedLap,
    compute_timing,
    is_restart,
)

RETIRE_TIMEOUT_LAPS = 2.0
"""A car awaited by a decision is treated as retired after this many leader lap times."""
DEFAULT_LAP_S = 150.0
"""Leader lap time assumed for the timeout when it is unknown."""


@dataclass(frozen=True)
class Decision:
    """A rule-eligible undercut opportunity, scored at its decision lap."""

    season: int
    round: int
    decision_lap: int
    attacker_id: str
    defender_id: str
    gap_s: float
    inputs: Mapping[str, Any]
    probability: float

    @property
    def attempt_lap(self) -> int:
        return self.decision_lap + 1


@dataclass(frozen=True)
class EligibleRecord:
    """An attacker's rule-eligible pair at one decision lap (what the alert policy remembers)."""

    defender_id: str
    probability: float
    gap_s: float
    inputs: Mapping[str, Any]


@dataclass
class Counters:
    applied: int = 0
    duplicates: int = 0
    after_end: int = 0
    inconsistent_snapshots: int = 0

    def to_json(self) -> dict[str, int]:
        return dict(vars(self))


@dataclass
class RaceState:
    season: int
    round: int
    n_partitions: int
    priors: RacePriors
    laps: dict[tuple[str, int], LapInput] = field(default_factory=dict)
    in_laps: dict[str, set[int]] = field(default_factory=dict)
    out_laps: dict[str, set[int]] = field(default_factory=dict)
    lap_neutral: dict[int, bool] = field(default_factory=dict)
    red_flag_laps: set[int] = field(default_factory=set)
    race_laps: int | None = None
    hwm: dict[int, float] = field(default_factory=dict)
    ended_partitions: set[int] = field(default_factory=set)
    seen: set[str] = field(default_factory=set)
    pending: set[tuple[int, str]] = field(default_factory=set)
    evaluated: set[tuple[int, str]] = field(default_factory=set)
    flags: dict[str, tuple[int, bool, float | None]] = field(default_factory=dict)
    """Per attacker: (latest evaluated decision lap, eligible, calibrated P or None)."""
    eligible: dict[tuple[int, str], EligibleRecord] = field(default_factory=dict)
    """(decision lap, attacker) -> the eligible pair there (the alert policy's memory)."""
    attempts_alerted: set[tuple[str, int]] = field(default_factory=set)
    """(attacker, attempt lap) pairs whose ``attempt`` alert was published."""
    ended: bool = False
    counters: Counters = field(default_factory=Counters)
    _timing: dict[tuple[str, int], TimedLap] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.n_partitions < 1:
            raise ValueError(f"n_partitions must be >= 1, got {self.n_partitions}")
        if (self.priors.season, self.priors.round) != (self.season, self.round):
            raise ValueError("priors belong to another race")

    # ------------------------------------------------------------------ ingest
    def apply(self, event: RaceEvent, partition: int) -> bool:
        """Apply one event received on ``partition``. False if it was ignored (duplicate,
        after the race ended, or another race)."""
        if event.race != (self.season, self.round):
            raise ValueError(f"event of {event.race} applied to {(self.season, self.round)}")
        if not 0 <= partition < self.n_partitions:
            raise ValueError(f"partition {partition} outside 0..{self.n_partitions - 1}")
        if self.ended:
            self.counters.after_end += 1
            return False
        if event.event_id in self.seen:
            self.counters.duplicates += 1
            return False
        self.seen.add(event.event_id)
        self.counters.applied += 1
        self.hwm[partition] = max(self.hwm.get(partition, -math.inf), event.race_time_s)
        if event.type == "session_end":
            self.ended_partitions.add(partition)
            return True
        driver = event.driver_id
        assert driver is not None and event.lap is not None  # validated by RaceEvent
        if event.type == "pit_in":
            self.in_laps.setdefault(driver, set()).add(event.lap)
        elif event.type == "pit_out":
            self.out_laps.setdefault(driver, set()).add(event.lap)
        else:
            row = LapInput(
                driver_id=driver,
                lap=event.lap,
                position=event.position,
                lap_time_s=event.lap_time_s,
                race_time_s=event.race_time_s,
                compound=event.compound,
                tyre_age=event.tyre_age,
                track_neutral=bool(event.track_neutral),
                track_red_flag=bool(event.track_red_flag),
            )
            self.laps[(driver, event.lap)] = row
            self.lap_neutral[event.lap] = (
                self.lap_neutral.get(event.lap, False) or row.track_neutral
            )
            if row.track_red_flag:
                self.red_flag_laps.add(event.lap)
            if event.race_laps is not None:
                self.race_laps = event.race_laps
            if (event.lap, driver) not in self.evaluated:
                self.pending.add((event.lap, driver))
        self._timing = None
        return True

    @property
    def session_complete(self) -> bool:
        """``session_end`` has been received on every partition."""
        return len(self.ended_partitions) == self.n_partitions

    @property
    def watermark(self) -> float:
        """Race time up to which every partition has delivered its events."""
        marks = [
            math.inf if p in self.ended_partitions else self.hwm.get(p, -math.inf)
            for p in range(self.n_partitions)
        ]
        return min(marks)

    # ------------------------------------------------------------------ timing
    def timing(self) -> dict[tuple[str, int], TimedLap]:
        if self._timing is None:
            self._timing = compute_timing(self.laps.values())
        return self._timing

    def drivers_at(self, lap: int) -> list[str]:
        return sorted(d for (d, n) in self.laps if n == lap)

    def frozen_pits(self) -> tuple[dict[str, frozenset[int]], dict[str, frozenset[int]]]:
        return (
            {d: frozenset(v) for d, v in self.in_laps.items()},
            {d: frozenset(v) for d, v in self.out_laps.items()},
        )

    def _leader_lap_time(self, lap: int) -> float | None:
        for d in self.drivers_at(lap):
            r = self.laps[(d, lap)]
            if r.position == 1:
                return r.lap_time_s
        return None

    # ------------------------------------------------------------------ readiness
    def _ready(self, lap: int, attacker: str, wm: float) -> bool:
        if wm == math.inf:
            return True
        timing = self.timing()
        a = self.laps[(attacker, lap)]
        loss = self.priors.pit_loss_prior_s
        if wm < a.race_time_s + max(loss or 0.0, 0.0):
            return False
        timeout = RETIRE_TIMEOUT_LAPS * (self._leader_lap_time(lap) or DEFAULT_LAP_S)
        here = self.drivers_at(lap)
        if a.position is not None and a.position > 1:
            ahead = [d for d in here if self.laps[(d, lap)].position == a.position - 1]
            if not ahead and wm < a.race_time_s + timeout:
                return False
        a_cum = timing[(attacker, lap)].cumulative_time_s
        if a_cum is None:
            return True
        d_cum = next(
            (
                timing[(d, lap)].cumulative_time_s
                for d in here
                if a.position is not None and self.laps[(d, lap)].position == a.position - 1
            ),
            None,
        )
        present = set(here)
        for x in {d for (d, n) in self.laps if n == lap - 1} - present:
            p = timing[(x, lap - 1)].cumulative_time_s
            if p is None:
                continue
            between = d_cum is not None and d_cum < p < a_cum
            rejoin = loss is not None and loss - TRAFFIC_WINDOW_S < p - a_cum <= loss
            if (between or rejoin) and wm < p + timeout:
                return False
        return True

    def ready(self, final: bool = False) -> list[tuple[int, str]]:
        """Decisions (decision lap, attacker) that can be evaluated now, in race order."""
        wm = math.inf if final else self.watermark
        out: list[tuple[int, str]] = []
        chosen: set[tuple[int, str]] = set()
        for key in sorted(self.pending):
            prev = (key[0] - 1, key[1])
            # one attacker's laps strictly in order, so "the previous lap" is always known
            if prev in self.pending and prev not in chosen:
                continue
            if self._ready(key[0], key[1], wm):
                out.append(key)
                chosen.add(key)
        return out

    # ------------------------------------------------------------------ the rule
    def snapshot(self, lap: int) -> LapSnapshot:
        """Port of ``features.undercut.snapshot_at`` over the received laps."""
        timing = self.timing()
        drivers = []
        leader_time = None
        for d in self.drivers_at(lap):
            t = timing[(d, lap)]
            prev = timing.get((d, lap - 1))
            if t.lap.position == 1:
                leader_time = t.lap.lap_time_s
            drivers.append(
                DriverLapState(
                    driver_id=d,
                    position=t.lap.position,
                    gap_ahead_s=t.gap_ahead_s,
                    gap_leader_s=t.gap_leader_s,
                    crossing_s=t.cumulative_time_s,
                    prev_crossing_s=prev.cumulative_time_s if prev else None,
                    team_key=self.priors.team_keys.get(d),
                    on_in_lap=lap in self.in_laps.get(d, set()),
                    on_out_lap=lap in self.out_laps.get(d, set()),
                    running=True,
                    tyre_compound=t.lap.compound,
                    tyre_age_laps=t.lap.tyre_age,
                    last_lap_time_s=t.lap.lap_time_s,
                )
            )
        return LapSnapshot(
            season=self.season,
            round=self.round,
            lap=lap,
            drivers=tuple(drivers),
            track_green=not self.lap_neutral.get(lap, False),
            restart_lap=is_restart(self.lap_neutral, lap, frozenset(self.red_flag_laps)),
            leader_lap_time_s=leader_time,
        )

    def evaluate(self, lap: int, attacker: str, params: RuleParams) -> CandidatePair | None:
        """The rule's pair with ``attacker`` at decision lap ``lap`` (None: not a candidate).
        Marks the decision evaluated. An inconsistent snapshot (two cars on one position) is
        counted and yields no pair, as the batch would fail on it."""
        self.pending.discard((lap, attacker))
        self.evaluated.add((lap, attacker))
        try:
            pairs = candidate_pairs(self.snapshot(lap), params)
        except ValueError:
            self.counters.inconsistent_snapshots += 1
            return None
        return next((p for p in pairs if p.attacker_id == attacker), None)

    def set_flag(self, attacker: str, lap: int, eligible: bool, prob: float | None) -> None:
        current = self.flags.get(attacker)
        if current is None or lap >= current[0]:
            self.flags[attacker] = (lap, eligible, prob)

    # ------------------------------------------------------------------ Redis view
    def latest_laps(self) -> dict[str, LapInput]:
        out: dict[str, LapInput] = {}
        for (d, n), r in self.laps.items():
            if d not in out or n > out[d].lap:
                out[d] = r
        return out

    def live_order(self) -> dict[str, int]:
        """Unique positions 1..n: laps completed (desc), line-crossing time, Jolpica position."""
        latest = self.latest_laps()
        ranked = sorted(
            latest.values(),
            key=lambda r: (-r.lap, r.race_time_s, r.position or 10**6, r.driver_id),
        )
        return {r.driver_id: i for i, r in enumerate(ranked, start=1)}

    # ------------------------------------------------------------------ (de)serialisation
    def to_json(self) -> dict[str, Any]:
        return {
            "season": self.season,
            "round": self.round,
            "n_partitions": self.n_partitions,
            "priors": self.priors.to_json(),
            "laps": [
                [
                    r.driver_id,
                    r.lap,
                    r.position,
                    r.lap_time_s,
                    r.race_time_s,
                    r.compound,
                    r.tyre_age,
                    r.track_neutral,
                    r.track_red_flag,
                ]
                for _, r in sorted(self.laps.items())
            ],
            "in_laps": {d: sorted(v) for d, v in sorted(self.in_laps.items())},
            "out_laps": {d: sorted(v) for d, v in sorted(self.out_laps.items())},
            "race_laps": self.race_laps,
            "hwm": {str(p): t for p, t in sorted(self.hwm.items())},
            "ended_partitions": sorted(self.ended_partitions),
            "seen": sorted(self.seen),
            "pending": sorted([list(k) for k in self.pending]),
            "evaluated": sorted([list(k) for k in self.evaluated]),
            "flags": {d: list(v) for d, v in sorted(self.flags.items())},
            "eligible": [
                [n, a, e.defender_id, e.probability, e.gap_s, dict(e.inputs)]
                for (n, a), e in sorted(self.eligible.items())
            ],
            "attempts_alerted": sorted([list(k) for k in self.attempts_alerted]),
            "ended": self.ended,
            "counters": self.counters.to_json(),
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> RaceState:
        state = cls(
            season=int(data["season"]),
            round=int(data["round"]),
            n_partitions=int(data["n_partitions"]),
            priors=RacePriors.from_json(data["priors"]),
        )
        for row in data["laps"]:
            r = LapInput(*row)
            state.laps[(r.driver_id, r.lap)] = r
            state.lap_neutral[r.lap] = state.lap_neutral.get(r.lap, False) or r.track_neutral
            if r.track_red_flag:
                state.red_flag_laps.add(r.lap)
        state.in_laps = {d: set(v) for d, v in data["in_laps"].items()}
        state.out_laps = {d: set(v) for d, v in data["out_laps"].items()}
        state.race_laps = data["race_laps"]
        state.hwm = {int(p): float(t) for p, t in data["hwm"].items()}
        state.ended_partitions = set(data["ended_partitions"])
        state.seen = set(data["seen"])
        state.pending = {(int(n), str(d)) for n, d in data["pending"]}
        state.evaluated = {(int(n), str(d)) for n, d in data["evaluated"]}
        state.flags = {d: (int(v[0]), bool(v[1]), v[2]) for d, v in data["flags"].items()}
        state.eligible = {
            (int(n), str(a)): EligibleRecord(str(d), float(p), float(g), dict(i))
            for n, a, d, p, g, i in data["eligible"]
        }
        state.attempts_alerted = {(str(a), int(n)) for a, n in data["attempts_alerted"]}
        state.ended = bool(data["ended"])
        state.counters = Counters(**data["counters"])
        return state
