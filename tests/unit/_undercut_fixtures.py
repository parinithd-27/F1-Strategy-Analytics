"""Hand-built undercut scenarios: a tiny race simulator that writes clean-zone rows.

Every car has a base lap time, a lap-1 time, pit laps and a fresh-tyre gain. A pit stop on lap p
adds ``IN_LOSS`` to lap p (the in-lap) and ``OUT_LOSS`` to lap p+1 (the out-lap); after the
out-lap the car is ``fresh_gain`` seconds per lap faster. Neutralised laps add ``SC_SLOW`` to
every car. Positions at each lap come from the cumulative times, as on a real timing line.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from _feature_fixtures import frame, lap, pitstop, race, result
from pyspark.sql import DataFrame, SparkSession

from f1sa.etl.io import FEATURE_INPUTS

IN_LOSS = 5.0
OUT_LOSS = 17.0
SC_SLOW = 30.0
N_LAPS = 14


@dataclass
class Car:
    driver: str
    team: str
    base: float
    lap1: float
    pit_laps: list[int] = field(default_factory=list)
    fresh_gain: float = 0.0
    retire_after: int | None = None
    stop_s: float = 22.5
    base_by_lap: dict[int, float] = field(default_factory=dict)


@dataclass
class Scenario:
    season: int
    round: int
    cars: list[Car]
    neutral_laps: set[int] = field(default_factory=set)
    n_laps: int = N_LAPS
    circuit: str = ""

    def lap_time(self, car: Car, n: int) -> float:
        t = car.lap1 if n == 1 else car.base_by_lap.get(n, car.base)
        if n in car.pit_laps:
            t += IN_LOSS
        elif n - 1 in car.pit_laps:
            t += OUT_LOSS
        elif any(p + 1 < n for p in car.pit_laps):
            t -= car.fresh_gain
        if n in self.neutral_laps:
            t += SC_SLOW
        return round(t, 3)

    def last_lap(self, car: Car) -> int:
        return car.retire_after if car.retire_after is not None else self.n_laps

    def rows(self) -> dict[str, list[dict[str, Any]]]:
        laps: list[dict[str, Any]] = []
        cum: dict[str, float] = {c.driver: 0.0 for c in self.cars}
        for n in range(1, self.n_laps + 1):
            running = [c for c in self.cars if n <= self.last_lap(c)]
            times = {c.driver: self.lap_time(c, n) for c in running}
            for c in running:
                cum[c.driver] = round(cum[c.driver] + times[c.driver], 3)
            order = sorted(running, key=lambda c: cum[c.driver])
            for pos, c in enumerate(order, start=1):
                laps.append(
                    lap(
                        self.season,
                        self.round,
                        c.driver,
                        n,
                        pos,
                        times[c.driver],
                        pit_in=n in c.pit_laps,
                        pit_out=n - 1 in c.pit_laps,
                        neutralised=n in self.neutral_laps,
                    )
                )
        finish = sorted(self.cars, key=lambda c: (-self.last_lap(c), cum[c.driver]))
        results = []
        for pos, c in enumerate(finish, start=1):
            r = result(self.season, self.round, c.driver, c.team, pos, self.last_lap(c))
            if c.retire_after is not None:
                r.update(status="Retired", finished=False, classified=False)
            results.append(r)
        stops, stints = [], []
        for c in self.cars:
            starts = [1, *[p + 1 for p in sorted(c.pit_laps) if p < self.last_lap(c)]]
            for i, p in enumerate(sorted(c.pit_laps), start=1):
                stops.append(pitstop(self.season, self.round, c.driver, i, p, c.stop_s))
            for i, s in enumerate(starts, start=1):
                end = starts[i] - 1 if i < len(starts) else self.last_lap(c)
                stints.append(
                    {
                        "season": self.season,
                        "round": self.round,
                        "driver_id": c.driver,
                        "stint": i,
                        "start_lap": s,
                        "end_lap": end,
                        "laps": end - s + 1,
                        "compound": "MEDIUM" if i == 1 else "HARD",
                        "tyre_age_at_start": 0,
                        "is_synthetic": True,
                        "source": "synthetic",
                    }
                )
        drivers = [
            {
                "season": self.season,
                "round": self.round,
                "driver_id": c.driver,
                "code": c.driver[:3].upper(),
                "constructor_id": c.team,
            }
            for c in self.cars
        ]
        constructors = [
            {"season": self.season, "round": self.round, "constructor_id": t, "name": t}
            for t in sorted({c.team for c in self.cars})
        ]
        return {
            "laps": laps,
            "results": results,
            "pitstops": stops,
            "stints": stints,
            "drivers": drivers,
            "constructors": constructors,
            "races": [race(self.season, self.round, self.circuit or f"c{self.round}")],
        }


def clean_tables(spark: SparkSession, scenarios: list[Scenario]) -> dict[str, DataFrame]:
    """Every clean table ``compute_features`` reads (telemetry tables empty)."""
    merged: dict[str, list[dict[str, Any]]] = {t: [] for t in FEATURE_INPUTS}
    for sc in scenarios:
        for table, rows in sc.rows().items():
            merged[table].extend(rows)
    return {t: frame(spark, t, rows) for t, rows in merged.items()}


def standard_cars(**overrides: Any) -> list[Car]:
    """lead (P1, far ahead), d (defender), a (attacker 1.5 s behind d), back (3 s behind a).

    ``a`` pits on lap 6, ``d`` on lap 7. Keyword overrides replace fields of ``a`` / ``d``
    (``a_<field>`` / ``d_<field>``).
    """
    a = Car("a", "team_a", 90.0, 98.5, pit_laps=[6], fresh_gain=2.5)
    d = Car("d", "team_d", 90.0, 97.0, pit_laps=[7], fresh_gain=2.5)
    for key, value in overrides.items():
        target, name = key.split("_", 1)
        setattr({"a": a, "d": d}[target], name, value)
    return [
        Car("lead", "team_l", 88.0, 94.0, pit_laps=[9]),
        d,
        a,
        Car("back", "team_b", 90.0, 101.5, pit_laps=[10]),
    ]


def race_data(sc: Scenario, pit_loss: float | None = None) -> Any:
    """The scenario as ``features.undercut.RaceData`` (gaps computed like ``timing_gaps``)."""
    from f1sa.features.undercut import LapRow, RaceData, StopRow

    rows = sc.rows()
    teams = {c.driver: c.team for c in sc.cars}
    compounds = {
        (s["driver_id"], n): s["compound"]
        for s in rows["stints"]
        for n in range(s["start_lap"], s["end_lap"] + 1)
    }
    cum: dict[str, float] = {}
    by_lap: dict[int, list[dict[str, Any]]] = {}
    for r in sorted(rows["laps"], key=lambda r: (r["lap"], r["position"])):
        by_lap.setdefault(r["lap"], []).append(r)
    out = []
    for n, laps in sorted(by_lap.items()):
        for r in laps:
            cum[r["driver_id"]] = round(cum.get(r["driver_id"], 0.0) + r["lap_time_s"], 3)
        leader = cum[laps[0]["driver_id"]]
        for i, r in enumerate(laps):
            ahead = cum[laps[i - 1]["driver_id"]] if i else None
            c = cum[r["driver_id"]]
            out.append(
                LapRow(
                    driver_id=r["driver_id"],
                    lap=n,
                    position=r["position"],
                    lap_time_s=r["lap_time_s"],
                    cumulative_time_s=c,
                    gap_ahead_s=round(c - ahead, 3) if ahead is not None else None,
                    gap_leader_s=round(c - leader, 3),
                    is_pit_in_lap=r["is_pit_in_lap"],
                    is_pit_out_lap=r["is_pit_out_lap"],
                    track_neutral=r["is_neutralised_lap"],
                    compound=compounds.get((r["driver_id"], n)),
                    team_key=teams[r["driver_id"]],
                )
            )
    stops = [
        StopRow(s["driver_id"], s["lap"], s["duration_s"], s["is_long_stop"])
        for s in rows["pitstops"]
    ]
    return RaceData(sc.season, sc.round, out, stops, race_laps=sc.n_laps, pit_loss_prior_s=pit_loss)
