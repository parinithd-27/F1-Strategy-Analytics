"""Shared helpers for the what-if tests: invented races, a tiny lake on disk, simple tyre models
and lap records. All data is generated here (no real timing data)."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from f1sa.sim.race_sim import CarState, DecisionState, ThirdCar
from f1sa.sim.tyre_model import COMPOUNDS, UNKNOWN, DegFit, RaceTyreModel
from f1sa.sim.whatif import LapRecord, LiveDriver

Truth = Callable[[int, int, str], float]
"""``(age, lap, compound) -> seconds over the driver's base lap``."""


def linear_truth(age: int, lap: int, compound: str) -> float:
    offset = {"SOFT": 0.0, "MEDIUM": 0.4, "HARD": 0.8}.get(compound, 0.5)
    return offset + 0.06 * age - 0.04 * lap


@dataclass(frozen=True)
class RaceSpec:
    season: int
    round: int
    circuit: str = "circ"
    drivers: int = 6
    laps: int = 40
    stop_lap: int = 18
    compounds: tuple[str, str] = ("MEDIUM", "HARD")
    noise_sd: float = 0.15
    scale: float = 1.0
    """Multiplies every lap time (the x100 leakage tests)."""
    synthetic: bool = True


def race_rows(
    spec: RaceSpec, truth: Truth = linear_truth, seed: int = 0
) -> dict[str, list[dict[str, Any]]]:
    """Clean ``laps`` and ``stints`` rows of one invented race: each driver stops once, on
    ``stop_lap + driver index`` (in-lap), and has one neutralised lap (lap 30)."""
    rng = np.random.default_rng(seed + spec.season * 100 + spec.round)
    laps: list[dict[str, Any]] = []
    stints: list[dict[str, Any]] = []
    for i in range(spec.drivers):
        d = f"drv{i}"
        stop = spec.stop_lap + i
        base = 90.0 + 0.2 * i
        stints.append(
            dict(
                driver_id=d,
                stint=1,
                start_lap=1,
                end_lap=stop,
                compound=spec.compounds[0],
                tyre_age_at_start=2 if i % 2 else 0,
                is_synthetic=spec.synthetic,
            )
        )
        stints.append(
            dict(
                driver_id=d,
                stint=2,
                start_lap=stop + 1,
                end_lap=spec.laps,
                compound=spec.compounds[1],
                tyre_age_at_start=0,
                is_synthetic=spec.synthetic,
            )
        )
        for lap in range(1, spec.laps + 1):
            if lap <= stop:
                c, age = spec.compounds[0], (2 if i % 2 else 0) + lap
            else:
                c, age = spec.compounds[1], lap - stop
            t = base + truth(age, lap, c) + rng.normal(0.0, spec.noise_sd)
            if lap == 1:
                t += 6.0
            if lap == stop:
                t += 5.0
            if lap == stop + 1:
                t += 17.0
            neutral = lap == 30
            if neutral:
                t += 25.0
            laps.append(
                dict(
                    driver_id=d,
                    lap=lap,
                    lap_time_s=t * spec.scale,
                    is_pit_in_lap=lap == stop,
                    is_pit_out_lap=lap == stop + 1,
                    is_neutralised_lap=neutral,
                    is_red_flag_lap=False,
                )
            )
    return {"laps": laps, "stints": stints}


class DictReader:
    """A ``LakeReader`` over in-memory rows: ``{table: {(season, round): rows}}``."""

    def __init__(
        self, tables: Mapping[str, Mapping[tuple[int, int], Sequence[Mapping[str, Any]]]]
    ) -> None:
        self.tables = tables
        self.reads: list[tuple[str, int, int]] = []

    def partitions(self, table: str, *, required: bool = True) -> list[tuple[int, int]]:
        return sorted(self.tables.get(table, {}))

    def read_rows(
        self, table: str, season: int, round_: int, columns: Sequence[str]
    ) -> list[dict[str, Any]]:
        self.reads.append((table, season, round_))
        rows = self.tables.get(table, {}).get((season, round_), [])
        return [{c: r.get(c) for c in columns} for r in rows]


def lake_readers(
    specs: Iterable[RaceSpec], *, ti: bool = False, truth: Truth = linear_truth
) -> tuple[DictReader, DictReader]:
    """``(clean, features)`` readers for the races (``stints_tracinginsights`` when ``ti``)."""
    clean: dict[str, dict[tuple[int, int], list[dict[str, Any]]]] = {
        "races": {},
        "laps": {},
        "stints": {},
        "stints_tracinginsights": {},
    }
    features: dict[str, dict[tuple[int, int], list[dict[str, Any]]]] = {
        "pit_loss": {},
        "outlap_features": {},
    }
    for s in specs:
        key = (s.season, s.round)
        rows = race_rows(s, truth)
        clean["races"][key] = [{"circuit_id": s.circuit}]
        clean["laps"][key] = rows["laps"]
        if ti:
            clean["stints_tracinginsights"][key] = rows["stints"]
            clean["stints"][key] = [
                dict(r, compound=None, is_synthetic=False) for r in rows["stints"]
            ]
        else:
            clean["stints"][key] = rows["stints"]
        features["pit_loss"][key] = [{"pit_loss_prior_s": 21.5, "pit_loss_source": "circuit_prior"}]
        features["outlap_features"][key] = [
            {
                "driver_id": f"drv{i}",
                "stop_delta_s": [0.3, -0.2, 0.1, 9.0, -0.4, 0.0][i % 6],
                "outlap_pace_residual_s": [0.5, -0.5, 0.2, -0.1, 30.0, 0.0][i % 6],
                "is_long_stop": False,
                "outlap_neutralised": False,
                "expected_outlap_pace_residual_s": 0.1 * i,
                "expected_outlap_pace_source": "team",
                "expected_stop_delta_s": -0.05 * i,
            }
            for i in range(s.drivers)
        ]
    return DictReader(clean), DictReader(features)


def write_parquet_lake(root: Path, clean: DictReader, features: DictReader) -> Path:
    """The readers' rows as a Spark-layout lake (``{root}/{zone}/{table}/season=S/round=R``)."""
    for zone, reader in (("clean", clean), ("features", features)):
        for table, parts in reader.tables.items():
            for (season, round_), rows in parts.items():
                if not rows:
                    continue
                d = root / zone / table / f"season={season}" / f"round={round_}"
                d.mkdir(parents=True, exist_ok=True)
                pq.write_table(pa.Table.from_pylist(list(rows)), d / "part-00000.parquet")
    return root


# ------------------------------------------------------------------ simulator inputs
def simple_tyres(
    beta: float = 0.05,
    phi: float = -0.04,
    *,
    offsets: Mapping[str, float] | None = None,
    sd: float = 0.6,
    stop_sd: float = 0.5,
    synthetic: bool = False,
) -> RaceTyreModel:
    off = offsets or {"SOFT": 0.0, "MEDIUM": 0.4, "HARD": 0.8}
    fits = {
        c: DegFit(
            source="compound",
            form="linear",
            alpha=off.get(c, 0.5),
            beta=beta,
            phi=phi,
            n_laps=500,
            n_stints=20,
            n_races=5,
            residual_sd_s=sd,
        )
        for c in COMPOUNDS
    }
    fits[UNKNOWN] = DegFit(
        source="global", form="linear", alpha=0.5, beta=beta, phi=phi, residual_sd_s=sd
    )
    return RaceTyreModel(
        season=2024,
        round=5,
        circuit_id="circ",
        fits=fits,
        stop_delta_sd_s=stop_sd,
        outlap_residual_sd_s=0.0,
        stop_history_n=50,
        is_synthetic=synthetic,
    )


def car(
    driver: str,
    t: float,
    *,
    compound: str = "MEDIUM",
    age: int = 20,
    level: float = 90.0,
    n: int = 5,
    sd: float = 0.4,
    loss: float | None = 22.0,
) -> CarState:
    return CarState(driver, t, compound, age, level, n, sd, loss)


def decision_state(
    gap: float = 1.5,
    *,
    current_lap: int = 20,
    race_laps: int | None = 60,
    attacker: CarState | None = None,
    defender: CarState | None = None,
    traffic: Sequence[ThirdCar] = (),
    tyres: RaceTyreModel | None = None,
) -> DecisionState:
    d = defender or car("def", 2000.0)
    a = attacker or car("att", d.t_cross_s + gap)
    return DecisionState(
        2024, 5, current_lap, race_laps, a, d, tyres or simple_tyres(), tuple(traffic)
    )


def lap_records(
    drivers: Mapping[str, float],
    *,
    laps: int = 20,
    base: float = 90.0,
    compound: str | None = "MEDIUM",
    pit_in: Mapping[str, int] | None = None,
    slow: Mapping[str, Iterable[int]] | None = None,
) -> list[LapRecord]:
    """``drivers``: driver -> offset of its race clock (seconds behind a car at offset 0).
    Every car laps in ``base`` seconds (+5 s on an in-lap, +20 s on the out-lap, +30 s on
    ``slow`` laps); the tyre age equals the lap number until a stop."""
    out: list[LapRecord] = []
    pit_in = pit_in or {}
    slow = {k: set(v) for k, v in (slow or {}).items()}
    for d, offset in drivers.items():
        t = offset
        stop = pit_in.get(d)
        for lap in range(1, laps + 1):
            lt = base + (8.0 if lap == 1 else 0.0)
            if stop is not None and lap == stop:
                lt += 5.0
            if stop is not None and lap == stop + 1:
                lt += 20.0
            if lap in slow.get(d, set()):
                lt += 30.0
            t += lt
            age = lap if stop is None or lap <= stop else lap - stop
            out.append(
                LapRecord(
                    driver_id=d,
                    lap=lap,
                    race_time_s=round(t, 3),
                    lap_time_s=lt,
                    position=None,
                    compound=compound,
                    tyre_age=age,
                    pit_in=stop is not None and lap == stop,
                    pit_out=stop is not None and lap == stop + 1,
                )
            )
    return out


def live(
    position: int,
    *,
    gap_ahead: float | None = 1.5,
    compound: str = "MEDIUM",
    age: int | None = 20,
    last: float | None = 90.0,
    prob: float | None = 0.4,
    flag: bool = True,
) -> LiveDriver:
    return LiveDriver(position, gap_ahead, None, compound, age, last, flag, prob)
