"""What-if strategy comparison at the current lap of a replayed race (#26, ``docs/sim_spec.md``).

``build_state`` turns what is known at the end of lap ``C`` into a ``race_sim.DecisionState``:

* the Redis driver hashes (``serving`` contract, read by ``api.state.read_race_state``):
  positions, gaps, compound, tyre age, last lap, and the stream's ``undercut_prob``;
* the lap history of the race channel (``LapRecord``, recorded by ``api.lap_log``); ``C`` is the
  attacker's latest lap there, and every record after ``C`` is dropped;
* the pre-race priors (``features/pit_loss``, ``features/outlap_features``: the stream's own
  sources, reduced as ``streaming.priors`` does) and the race's tyre model entry;
* in full mode, the real compound of the stint in progress (``clean/stints_tracinginsights``: only
  ``start_lap``, ``compound`` and ``tyre_age_at_start`` of stints that started by lap ``C``).

The backtest (``sim/consistency.py``) builds the same ``LapRecord`` list from ``laps_enriched`` up
to lap L-1, so the live state and the backtest state come from one code path.
"""

from __future__ import annotations

import math
import statistics
import threading
import time
from collections import OrderedDict, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from f1sa.sim.race_sim import (
    N_DEFAULT,
    NEXT_COMPOUND,
    SEED_DEFAULT,
    CarState,
    DecisionState,
    SimError,
    SimResult,
    ThirdCar,
    run_all,
)
from f1sa.sim.tyre_model import (
    COMPOUNDS,
    INCIDENT_RATIO,
    UNKNOWN,
    RaceTyreModel,
    TyreModelArtefact,
    TyreModelError,
)
from f1sa.streaming.live_features import DriverPrior, RacePriors

RECENT_WINDOW_LAPS = 10
"""Non-pit laps (lap >= 2, up to C) the causal green filter looks at."""
RECENT_PACE_LAPS = 5
NOISE_SHRINK_N0 = 3
PRIOR_NOISE_SD_S = 1.0
"""Noise prior when the tyre model has no fit (source ``none``)."""
RETIRED_AFTER_LAPS = 2.0
"""A third car whose latest crossing is older than this many typical laps counts as retired."""
CACHE_RACES = 8


class WhatIfError(RuntimeError):
    """Base of the errors the API maps to status codes."""


class UnknownRace(WhatIfError):
    pass


class UnknownDriver(WhatIfError):
    pass


class NotAdjacent(WhatIfError):
    pass


class NoDecisionState(WhatIfError):
    """The state needed at lap C is missing (no pace or no gap)."""


class SimUnavailable(WhatIfError):
    """The tyre model file is missing, unreadable or does not match the lake."""


# ------------------------------------------------------------------ inputs
@dataclass(frozen=True)
class LapRecord:
    """One completed lap of one car, as the race channel carries it."""

    driver_id: str
    lap: int
    race_time_s: float
    lap_time_s: float | None = None
    position: int | None = None
    compound: str | None = None
    tyre_age: int | None = None
    pit_in: bool = False
    """This lap is an in-lap (a ``pit_in`` event for it)."""
    pit_out: bool = False
    """This lap is an out-lap (a ``pit_out`` event for it)."""


@dataclass(frozen=True)
class LiveDriver:
    """The fields of one driver hash the what-if uses (``DriverStateOut``)."""

    position: int
    gap_ahead_s: float | None
    gap_leader_s: float | None
    tyre_compound: str
    tyre_age_laps: int | None
    last_lap_time_s: float | None
    undercut_flag: bool
    undercut_prob: float | None


@dataclass(frozen=True)
class StintStart:
    """A TracingInsights stint as far as it is known when it starts (never its end)."""

    driver_id: str
    start_lap: int
    compound: str | None
    tyre_age_at_start: int | None


@dataclass(frozen=True)
class CarInputs:
    driver_id: str
    position: int
    compound: str
    compound_source: str
    tyre_age: int
    new_compound: str
    pace_laps: int
    noise_source: str
    noise_sd_s: float
    pace_level_s: float
    stop_loss_s: float | None
    stop_delta_s: float | None
    outlap_residual_s: float | None
    tyre_fit_source: str
    tyre_fit_form: str


@dataclass(frozen=True)
class StateInputs:
    current_lap: int | None
    decision_lap: int | None
    race_laps: int | None
    gap_s: float
    gap_source: str
    history: str
    traffic_cars: int
    attacker: CarInputs
    defender: CarInputs
    pit_loss_prior_s: float | None
    pit_loss_source: str | None
    stop_noise_sd_s: float
    synthetic: bool
    warnings: tuple[str, ...] = ()


# ------------------------------------------------------------------ priors (no Spark)
def race_priors_from_rows(
    season: int,
    round_: int,
    pit_loss_rows: Sequence[Mapping[str, Any]],
    outlap_rows: Iterable[Mapping[str, Any]],
) -> RacePriors:
    """``streaming.priors.priors_from_frames`` without Spark (``team_keys`` is not needed here):
    the race's one ``pit_loss`` row and, per driver, the max of each out-lap prior (nulls
    ignored, as Spark's ``max``)."""
    if len(pit_loss_rows) > 1:
        raise WhatIfError(f"{season} R{round_}: {len(pit_loss_rows)} pit_loss rows, expected <= 1")
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for r in outlap_rows:
        if isinstance(r.get("driver_id"), str):
            grouped[r["driver_id"]].append(r)

    def mx(rows: list[Mapping[str, Any]], col: str) -> Any:
        values = [r[col] for r in rows if r.get(col) is not None]
        return max(values) if values else None

    loss = pit_loss_rows[0] if pit_loss_rows else {}
    return RacePriors(
        season=season,
        round=round_,
        pit_loss_prior_s=loss.get("pit_loss_prior_s"),
        pit_loss_source=loss.get("pit_loss_source"),
        drivers={
            d: DriverPrior(
                mx(rows, "expected_outlap_pace_residual_s"),
                mx(rows, "expected_outlap_pace_source"),
                mx(rows, "expected_stop_delta_s"),
            )
            for d, rows in grouped.items()
        },
    )


# ------------------------------------------------------------------ state
def _compound(value: Any) -> str | None:
    if isinstance(value, str) and value.upper() in COMPOUNDS:
        return value.upper()
    return None


def _usable_time(v: Any) -> bool:
    return isinstance(v, float | int) and not isinstance(v, bool) and math.isfinite(v) and v > 0


def recent_laps(records: Sequence[LapRecord]) -> list[LapRecord]:
    """The causal green filter: of the last ``RECENT_WINDOW_LAPS`` non-pit laps (lap >= 2, time
    known), those within ``INCIDENT_RATIO`` x their fastest; the last ``RECENT_PACE_LAPS`` kept."""
    window = [
        r
        for r in sorted(records, key=lambda r: r.lap)
        if r.lap >= 2 and not r.pit_in and not r.pit_out and _usable_time(r.lap_time_s)
    ][-RECENT_WINDOW_LAPS:]
    if not window:
        return []
    fastest = min(float(r.lap_time_s) for r in window if r.lap_time_s is not None)
    kept = [
        r for r in window if r.lap_time_s is not None and r.lap_time_s <= INCIDENT_RATIO * fastest
    ]
    return kept[-RECENT_PACE_LAPS:]


def _tyre_at(
    driver: str,
    lap: int,
    by_lap: Mapping[int, LapRecord],
    stints: Sequence[StintStart] | None,
    live: LiveDriver | None,
    current_lap: int | None,
) -> tuple[str, int | None, str]:
    """``(compound, age, source)`` at the end of ``lap`` with knowledge up to ``current_lap``."""
    if stints is not None:
        started = [s for s in stints if s.driver_id == driver and s.start_lap <= lap]
        if started:
            s = max(started, key=lambda s: s.start_lap)
            age0 = s.tyre_age_at_start if s.tyre_age_at_start is not None else 0
            return (
                _compound(s.compound) or UNKNOWN,
                age0 + lap - s.start_lap + 1,
                "tracinginsights_stint",
            )
    rec = by_lap.get(lap)
    if rec is not None and (rec.compound is not None or rec.tyre_age is not None):
        return _compound(rec.compound) or UNKNOWN, rec.tyre_age, "race_channel"
    if live is not None and lap == current_lap:
        return _compound(live.tyre_compound) or UNKNOWN, live.tyre_age_laps, "redis"
    return UNKNOWN, None, "none"


def _car(
    driver: str,
    live: LiveDriver,
    records: Sequence[LapRecord],
    current_lap: int | None,
    tyres: RaceTyreModel,
    priors: RacePriors,
    stints: Sequence[StintStart] | None,
    warnings: list[str],
    role: str,
) -> tuple[CarInputs, float, int, float]:
    """``(inputs, pace level, pace n, noise sd)`` of A or D."""
    by_lap = {r.lap: r for r in records}
    lap_c = current_lap if current_lap is not None else 0
    compound, age, csource = _tyre_at(driver, lap_c, by_lap, stints, live, current_lap)
    if age is None:
        age = live.tyre_age_laps if live.tyre_age_laps is not None else 0
        if live.tyre_age_laps is None:
            warnings.append(f"{role}_tyre_age_unknown")
    if compound == UNKNOWN and csource != "tracinginsights_stint" and _compound(live.tyre_compound):
        compound, csource = str(_compound(live.tyre_compound)), "redis"
    fit = tyres.fit_for(compound)
    s0 = fit.residual_sd_s if fit.residual_sd_s is not None else PRIOR_NOISE_SD_S
    residuals: list[float] = []
    for r in recent_laps(records):
        c, a, _ = _tyre_at(driver, r.lap, by_lap, stints, None, None)
        if a is None or r.lap_time_s is None:
            continue
        residuals.append(float(r.lap_time_s) - float(tyres.fit_for(c).predict(a, r.lap)))
    if residuals:
        n = len(residuals)
        level = statistics.fmean(residuals)
        var = statistics.variance(residuals) if n >= 2 else 0.0
        sd = math.sqrt((n * var + NOISE_SHRINK_N0 * s0 * s0) / (n + NOISE_SHRINK_N0))
        nsource = "driver_recent"
    elif _usable_time(live.last_lap_time_s):
        assert live.last_lap_time_s is not None
        n, sd, nsource = 1, s0, "prior"
        level = float(live.last_lap_time_s) - float(fit.predict(age, lap_c))
    else:
        raise NoDecisionState(f"no lap time known for {driver}")
    prior = priors.driver(driver)
    loss: float | None = None
    if priors.pit_loss_prior_s is not None:
        loss = float(priors.pit_loss_prior_s)
        for part, name in (
            (prior.stop_delta_s, "stop_delta"),
            (prior.pace_residual_s, "outlap_residual"),
        ):
            if part is None:
                warnings.append(f"{role}_{name}_missing")
            else:
                loss += float(part)
    inputs = CarInputs(
        driver_id=driver,
        position=live.position,
        compound=compound,
        compound_source=csource,
        tyre_age=int(age),
        new_compound=NEXT_COMPOUND[compound],
        pace_laps=len(residuals),
        noise_source=nsource,
        noise_sd_s=sd,
        pace_level_s=level,
        stop_loss_s=loss,
        stop_delta_s=prior.stop_delta_s,
        outlap_residual_s=prior.pace_residual_s,
        tyre_fit_source=fit.source,
        tyre_fit_form=fit.form,
    )
    return inputs, level, n, sd


def _third_cars(by_driver: Mapping[str, Sequence[LapRecord]], exclude: set[str]) -> list[ThirdCar]:
    latest: dict[str, LapRecord] = {}
    paces: dict[str, float] = {}
    for d, recs in by_driver.items():
        if d in exclude or not recs:
            continue
        last = max(recs, key=lambda r: r.lap)
        kept = recent_laps(recs)
        if kept:
            pace = statistics.median(float(r.lap_time_s) for r in kept if r.lap_time_s is not None)
        elif _usable_time(last.lap_time_s):
            pace = float(last.lap_time_s)  # type: ignore[arg-type]
        else:
            continue
        latest[d], paces[d] = last, pace
    if not latest:
        return []
    now = max(r.race_time_s for r in latest.values())
    typical = statistics.median(paces.values())
    return [
        ThirdCar(d, latest[d].race_time_s, paces[d])
        for d in sorted(latest)
        if latest[d].race_time_s >= now - RETIRED_AFTER_LAPS * typical
    ]


def build_state(
    season: int,
    round_: int,
    attacker: str,
    defender: str,
    drivers: Mapping[str, LiveDriver],
    records: Sequence[LapRecord],
    priors: RacePriors,
    tyres: RaceTyreModel,
    *,
    race_laps: int | None,
    stints: Sequence[StintStart] | None = None,
) -> tuple[DecisionState, StateInputs]:
    """The decision state at the end of the attacker's latest recorded lap ``C``."""
    for d in (attacker, defender):
        if d not in drivers:
            raise UnknownDriver(f"driver {d!r} has no live state in {season} R{round_}")
    a_live, d_live = drivers[attacker], drivers[defender]
    if d_live.position != a_live.position - 1:
        raise NotAdjacent(
            f"{defender} (P{d_live.position}) is not directly ahead of "
            f"{attacker} (P{a_live.position})"
        )
    a_laps = [r.lap for r in records if r.driver_id == attacker]
    current = max(a_laps) if a_laps else None
    known = [r for r in records if current is not None and r.lap <= current]
    if stints is not None:  # only stints that had started by lap C are known at decision time
        stints = [s for s in stints if current is not None and s.start_lap <= current]
    by_driver: dict[str, list[LapRecord]] = defaultdict(list)
    for r in known:
        by_driver[r.driver_id].append(r)
    warnings: list[str] = []
    a_in, a_level, a_n, a_sd = _car(
        attacker, a_live, by_driver[attacker], current, tyres, priors, stints, warnings, "attacker"
    )
    d_in, d_level, d_n, d_sd = _car(
        defender, d_live, by_driver[defender], current, tyres, priors, stints, warnings, "defender"
    )

    def crossing(driver: str) -> float | None:
        return next((r.race_time_s for r in by_driver[driver] if r.lap == current), None)

    t_a, t_d = crossing(attacker), crossing(defender)
    if t_a is not None and t_d is not None and t_a > t_d:
        gap_source = "race_channel"
    elif a_live.gap_ahead_s is not None and a_live.gap_ahead_s > 0:
        gap_source = "redis"
        t_d = t_d if t_d is not None else (t_a - a_live.gap_ahead_s if t_a is not None else 0.0)
        t_a = t_d + a_live.gap_ahead_s
    else:
        raise NoDecisionState(f"no gap from {attacker} to {defender} at lap {current}")
    for rec_driver, role in ((attacker, "attacker"), (defender, "defender")):
        recs = by_driver[rec_driver]
        if any(r.lap == current and (r.pit_in or r.pit_out) for r in recs):
            warnings.append(f"{role}_on_pit_lap")
    traffic = _third_cars(by_driver, {attacker, defender}) if current is not None else []
    state = DecisionState(
        season=season,
        round=round_,
        current_lap=current if current is not None else 0,
        race_laps=race_laps if current is not None else None,
        attacker=CarState(
            attacker, t_a, a_in.compound, a_in.tyre_age, a_level, a_n, a_sd, a_in.stop_loss_s
        ),
        defender=CarState(
            defender, t_d, d_in.compound, d_in.tyre_age, d_level, d_n, d_sd, d_in.stop_loss_s
        ),
        tyres=tyres,
        traffic=tuple(traffic),
    )
    if current is None:
        warnings.append("no_lap_history")
    inputs = StateInputs(
        current_lap=current,
        decision_lap=current + 1 if current is not None else None,
        race_laps=race_laps,
        gap_s=t_a - t_d,
        gap_source=gap_source,
        history="race_channel" if current is not None else "none",
        traffic_cars=len(traffic),
        attacker=a_in,
        defender=d_in,
        pit_loss_prior_s=priors.pit_loss_prior_s,
        pit_loss_source=priors.pit_loss_source,
        stop_noise_sd_s=tyres.stop_noise_sd_s,
        synthetic=tyres.is_synthetic,
        warnings=tuple(warnings),
    )
    return state, inputs


# ------------------------------------------------------------------ service
STINT_START_COLUMNS = (
    "driver_id",
    "start_lap",
    "compound",
    "tyre_age_at_start",
    "compound_conflict",
)
"""What is read of ``clean/stints_tracinginsights``: never ``end_lap`` or ``laps``."""


def stint_starts(rows: Iterable[Mapping[str, Any]]) -> tuple[StintStart, ...]:
    """TracingInsights stint rows as ``StintStart`` (a stint whose lap labels conflict gets no
    compound)."""
    return tuple(
        StintStart(
            driver_id=str(r["driver_id"]),
            start_lap=int(r["start_lap"]),
            compound=None if r.get("compound_conflict") else r.get("compound"),
            tyre_age_at_start=r.get("tyre_age_at_start"),
        )
        for r in rows
        if isinstance(r.get("driver_id"), str) and isinstance(r.get("start_lap"), int)
    )


@dataclass(frozen=True)
class RaceContext:
    priors: RacePriors
    tyres: RaceTyreModel
    stints: tuple[StintStart, ...] | None


@dataclass(frozen=True)
class WhatIfResult:
    season: int
    round: int
    attacker: str
    defender: str
    sim: SimResult
    inputs: StateInputs
    model_probability: float | None
    model_flag: bool
    runtime_ms: float = 0.0


@dataclass
class WhatIfService:
    """Per race the priors, tyre entry and (full mode) stint starts, loaded once and cached.

    ``readers`` returns the read-only ``(clean, features)`` lake readers (``api.catalog``). The
    tyre model file is chosen per race from the catalog's ``synthetic`` flag:
    ``{sim_dir}/tyre_model_sample.json`` for a synthetic (sample) race, ``..._full.json``
    otherwise."""

    readers: Callable[[], tuple[Any, Any]]
    sim_dir: Path
    cache_races: int = CACHE_RACES
    _artefacts: dict[str, TyreModelArtefact] = field(default_factory=dict, init=False, repr=False)
    _cache: OrderedDict[tuple[int, int], RaceContext] = field(
        default_factory=OrderedDict, init=False, repr=False
    )
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def artefact(self, source: str) -> TyreModelArtefact:
        """``tyre_model_{source}.json`` (``sample`` or ``full``), loaded once."""
        if source not in self._artefacts:
            path = self.sim_dir / f"tyre_model_{source}.json"
            try:
                art = TyreModelArtefact.load(path)
            except TyreModelError as exc:
                raise SimUnavailable(
                    f"tyre model not available ({path.name}; build it with "
                    f"make sim-tyre-model-{source})"
                ) from exc
            if art.source != source:
                raise SimUnavailable(f"{path.name} holds a {art.source} model")
            self._artefacts[source] = art
        return self._artefacts[source]

    def context(self, season: int, round_: int, *, synthetic: bool) -> RaceContext:
        with self._lock:
            key = (season, round_)
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        source = "sample" if synthetic else "full"
        art = self.artefact(source)
        tyres = art.race(season, round_)
        if tyres is None:
            raise SimUnavailable(
                f"the tyre model has no entry for {season} R{round_} "
                f"(rebuild it: make sim-tyre-model-{source})"
            )
        if tyres.is_synthetic != synthetic:
            raise SimUnavailable("the tyre model does not match the lake (synthetic vs real data)")
        clean, features = self.readers()
        priors = race_priors_from_rows(
            season,
            round_,
            features.read_rows("pit_loss", season, round_, ["pit_loss_prior_s", "pit_loss_source"]),
            features.read_rows(
                "outlap_features",
                season,
                round_,
                [
                    "driver_id",
                    "expected_outlap_pace_residual_s",
                    "expected_outlap_pace_source",
                    "expected_stop_delta_s",
                ],
            ),
        )
        stints: tuple[StintStart, ...] | None = None
        if art.source == "full":
            stints = stint_starts(
                clean.read_rows("stints_tracinginsights", season, round_, list(STINT_START_COLUMNS))
            )
        ctx = RaceContext(priors=priors, tyres=tyres, stints=stints)
        with self._lock:
            self._cache[key] = ctx
            while len(self._cache) > self.cache_races:
                self._cache.popitem(last=False)
        return ctx

    def run(
        self,
        season: int,
        round_: int,
        attacker: str,
        defender: str,
        drivers: Mapping[str, LiveDriver],
        records: Sequence[LapRecord],
        *,
        race_laps: int | None,
        synthetic: bool,
        n: int = N_DEFAULT,
        seed: int = SEED_DEFAULT,
    ) -> WhatIfResult:
        t0 = time.perf_counter()
        ctx = self.context(season, round_, synthetic=synthetic)
        state, inputs = build_state(
            season,
            round_,
            attacker,
            defender,
            drivers,
            records,
            ctx.priors,
            ctx.tyres,
            race_laps=race_laps,
            stints=ctx.stints,
        )
        try:
            sim = run_all(state, n=n, seed=seed)
        except SimError as exc:
            raise NoDecisionState(str(exc)) from exc
        a = drivers[attacker]
        return WhatIfResult(
            season=season,
            round=round_,
            attacker=attacker,
            defender=defender,
            sim=sim,
            inputs=inputs,
            model_probability=a.undercut_prob,
            model_flag=a.undercut_flag,
            runtime_ms=(time.perf_counter() - t0) * 1000.0,
        )
