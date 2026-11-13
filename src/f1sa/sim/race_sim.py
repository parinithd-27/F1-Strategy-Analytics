"""Seeded Monte Carlo of an undercut decision (#26, ``docs/sim_spec.md``). Pure Python + NumPy.

The state is taken at the end of lap ``C`` (the attacker's last completed lap); ``L = C + 1`` is
the label's attempt lap. For each strategy and defender response the attacker A and the defender D
are simulated lap by lap up to the evaluation lap (the later of the two out-laps):

    lap time = m_i + f_c(age, lap) + u_i + e_il,   u_i ~ N(0, s_i^2 / n_i),  e_il ~ N(0, s_i^2)

plus the stop loss on the out-lap (``loss_i + eta``, ``eta ~ N(0, stop_noise_sd^2)``) and the
traffic hold at the rejoin (held to 1.0 s behind a third car). All strategies and responses use
the same standard-normal draws (common random numbers), so one seed gives identical output and
the strategies are compared on the same draws. Nothing reads the future: the state holds only
what was known at the end of lap ``C``.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

import numpy as np

from f1sa.sim.tyre_model import COMPOUNDS, UNKNOWN, RaceTyreModel

Strategy = Literal["pit_now", "pit_next_lap", "stay_out"]
Response = Literal["primary", "sensitivity"]
STRATEGIES: tuple[Strategy, ...] = ("pit_now", "pit_next_lap", "stay_out")
RESPONSES: tuple[Response, ...] = ("primary", "sensitivity")

N_DEFAULT = 500
N_MIN = 100
N_MAX = 5000
SEED_DEFAULT = 42
SEED_MAX = 2**32 - 1
RESPONSE_K = 2
"""The sensitivity response: D covers k laps after A (the deployed model's k)."""
TRAFFIC_WINDOW_S = 1.0
INTERVAL = (10.0, 90.0)
MAX_TRAFFIC_ITERATIONS = 64

NEXT_COMPOUND: dict[str, str] = {
    "SOFT": "MEDIUM",
    "MEDIUM": "HARD",
    "HARD": "MEDIUM",
    "INTERMEDIATE": "INTERMEDIATE",
    "WET": "WET",
    UNKNOWN: UNKNOWN,
}


class SimError(ValueError):
    """The simulation cannot run on this input."""


@dataclass(frozen=True)
class CarState:
    """A or D at the end of lap ``C`` (everything known then)."""

    driver_id: str
    t_cross_s: float
    """Race clock of the car's lap-C line crossing."""
    compound: str
    tyre_age: int
    """Tyre age at the end of lap C (``df_jobs.tyre_age`` convention)."""
    pace_level_s: float
    """m_i: the mean of the recent residuals ``lap_time - f_c(age, lap)``."""
    pace_n: int
    noise_sd_s: float
    stop_loss_s: float | None
    """Mean loss of a stop: pit-loss prior + stop delta + out-lap residual; None if no prior."""

    def __post_init__(self) -> None:
        if self.compound not in (*COMPOUNDS, UNKNOWN):
            raise SimError(f"unknown compound {self.compound!r}")
        if self.pace_n < 1 or self.noise_sd_s < 0 or self.tyre_age < 0:
            raise SimError(f"invalid car state for {self.driver_id}")
        for v in (self.t_cross_s, self.pace_level_s, self.noise_sd_s):
            if not math.isfinite(v):
                raise SimError(f"non-finite car state for {self.driver_id}")


@dataclass(frozen=True)
class ThirdCar:
    """Another car, projected without a stop: crossings ``t_cross_s + k * pace_s``."""

    driver_id: str
    t_cross_s: float
    pace_s: float

    def __post_init__(self) -> None:
        if not (math.isfinite(self.t_cross_s) and math.isfinite(self.pace_s) and self.pace_s > 0):
            raise SimError(f"invalid third car {self.driver_id}")


@dataclass(frozen=True)
class DecisionState:
    season: int
    round: int
    current_lap: int
    race_laps: int | None
    attacker: CarState
    defender: CarState
    tyres: RaceTyreModel
    traffic: tuple[ThirdCar, ...] = ()

    @property
    def gap_s(self) -> float:
        """A's gap to D at the line at the end of lap C (positive: A behind)."""
        return self.attacker.t_cross_s - self.defender.t_cross_s


@dataclass(frozen=True)
class StrategyResult:
    strategy: Strategy
    response: Response
    attacker_pit_lap: int
    defender_pit_lap: int
    evaluation_lap: int
    feasible: bool
    reason: str | None
    p_attacker_ahead: float | None
    expected_gap_s: float | None
    interval80_s: tuple[float, float] | None
    mc_se: float | None
    attacker_new_compound: str
    defender_new_compound: str


@dataclass(frozen=True)
class SimResult:
    n: int
    seed: int
    results: tuple[StrategyResult, ...] = field(default_factory=tuple)

    def get(self, strategy: Strategy, response: Response = "primary") -> StrategyResult:
        for r in self.results:
            if r.strategy == strategy and r.response == response:
                return r
        raise KeyError((strategy, response))


def timeline(
    strategy: Strategy, response: Response, current_lap: int, k: int = RESPONSE_K
) -> tuple[int, int, int]:
    """``(A's in-lap, D's in-lap, evaluation lap)``; the evaluation lap is the later out-lap."""
    if k < 1:
        raise SimError(f"k must be >= 1, got {k}")
    lap = current_lap + 1
    cover = 1 if response == "primary" else k
    if strategy == "pit_now":
        a, d = lap, lap + cover
    elif strategy == "pit_next_lap":
        a, d = lap + 1, lap + 1 + cover
    elif strategy == "stay_out":
        d = lap + cover
        a = d + 1
    else:
        raise SimError(f"unknown strategy {strategy!r}")
    return a, d, max(a, d) + 1


@dataclass(frozen=True)
class _Draws:
    u_a: np.ndarray
    u_d: np.ndarray
    eta_a: np.ndarray
    eta_d: np.ndarray
    e_a: np.ndarray
    e_d: np.ndarray


def _horizon(current_lap: int, k: int) -> int:
    return (
        max(timeline(s, r, current_lap, k)[2] for s in STRATEGIES for r in RESPONSES) - current_lap
    )


def _draws(n: int, seed: int, horizon: int) -> _Draws:
    """One generator, a fixed draw order: the common random numbers of every strategy."""
    rng = np.random.Generator(np.random.PCG64(np.random.SeedSequence(seed)))
    return _Draws(
        u_a=rng.standard_normal(n),
        u_d=rng.standard_normal(n),
        eta_a=rng.standard_normal(n),
        eta_d=rng.standard_normal(n),
        e_a=rng.standard_normal((n, horizon)),
        e_d=rng.standard_normal((n, horizon)),
    )


def hold_in_traffic(
    t: np.ndarray, traffic: Sequence[ThirdCar], window_s: float = TRAFFIC_WINDOW_S
) -> np.ndarray:
    """Crossing times after the traffic hold: a car 0-``window_s`` behind a projected crossing of
    a third car is held to exactly ``window_s`` behind it, repeated to a fixed point. The map is
    non-decreasing in ``t``, so a later arrival never gives an earlier crossing."""
    out = np.array(t, dtype=float, copy=True)
    if not traffic:
        return out
    for _ in range(MAX_TRAFFIC_ITERATIONS):
        changed = False
        for z in traffic:
            k = np.floor((out - z.t_cross_s) / z.pace_s)
            tc = z.t_cross_s + k * z.pace_s
            behind = out - tc
            mask = (k >= 0) & (behind > 0.0) & (behind < window_s)
            if mask.any():
                out = np.where(mask, tc + window_s, out)
                changed = True
        if not changed:
            return out
    raise SimError("traffic hold did not converge")


def _crossing(
    car: CarState,
    tyres: RaceTyreModel,
    current_lap: int,
    pit_lap: int,
    eval_lap: int,
    u: np.ndarray,
    eta: np.ndarray,
    e: np.ndarray,
    stop_noise_sd: float,
    loss_shift_s: float,
    traffic: Sequence[ThirdCar],
) -> np.ndarray:
    """The car's crossing time at ``eval_lap`` for every draw."""
    if car.stop_loss_s is None:
        raise SimError("no stop loss")
    level = car.pace_level_s + u * car.noise_sd_s / math.sqrt(car.pace_n)
    old_fit = tyres.fit_for(car.compound)
    new_compound = NEXT_COMPOUND[car.compound]
    new_fit = tyres.fit_for(new_compound)
    t = np.full(u.shape, car.t_cross_s, dtype=float)
    for j, lap in enumerate(range(current_lap + 1, eval_lap + 1)):
        if lap <= pit_lap:  # the in-lap is still on the old tyres
            mean = old_fit.predict(car.tyre_age + lap - current_lap, lap)
        else:
            mean = new_fit.predict(lap - pit_lap, lap)  # the out-lap is age 1
        t = t + level + mean + car.noise_sd_s * e[:, j]
        if lap == pit_lap + 1:
            t = t + car.stop_loss_s + loss_shift_s + stop_noise_sd * eta
            t = hold_in_traffic(t, traffic)
    return t


def simulate(
    state: DecisionState,
    strategy: Strategy,
    response: Response,
    *,
    n: int = N_DEFAULT,
    seed: int = SEED_DEFAULT,
    k: int = RESPONSE_K,
    attacker_loss_shift_s: float = 0.0,
    pit_loss_shift_s: float = 0.0,
    draws: _Draws | None = None,
) -> StrategyResult:
    """One strategy and response. The shifts move A's loss only, or both cars' loss."""
    _check_n_seed(n, seed)
    c = state.current_lap
    a_pit, d_pit, eval_lap = timeline(strategy, response, c, k)
    base = {
        "strategy": strategy,
        "response": response,
        "attacker_pit_lap": a_pit,
        "defender_pit_lap": d_pit,
        "evaluation_lap": eval_lap,
        "attacker_new_compound": NEXT_COMPOUND[state.attacker.compound],
        "defender_new_compound": NEXT_COMPOUND[state.defender.compound],
    }
    reason = None
    if state.race_laps is not None and eval_lap > state.race_laps:
        reason = "race_end"
    elif state.attacker.stop_loss_s is None or state.defender.stop_loss_s is None:
        reason = "no_pit_loss_prior"
    if reason is not None:
        return StrategyResult(
            **base,  # type: ignore[arg-type]
            feasible=False,
            reason=reason,
            p_attacker_ahead=None,
            expected_gap_s=None,
            interval80_s=None,
            mc_se=None,
        )
    d = draws or _draws(n, seed, _horizon(c, k))
    noise = state.tyres.stop_noise_sd_s
    t_a = _crossing(
        state.attacker,
        state.tyres,
        c,
        a_pit,
        eval_lap,
        d.u_a,
        d.eta_a,
        d.e_a,
        noise,
        attacker_loss_shift_s + pit_loss_shift_s,
        state.traffic,
    )
    t_d = _crossing(
        state.defender,
        state.tyres,
        c,
        d_pit,
        eval_lap,
        d.u_d,
        d.eta_d,
        d.e_d,
        noise,
        pit_loss_shift_s,
        state.traffic,
    )
    gap = t_d - t_a
    p = float(np.mean(gap > 0.0))
    lo, hi = (float(v) for v in np.percentile(gap, INTERVAL))
    return StrategyResult(
        **base,  # type: ignore[arg-type]
        feasible=True,
        reason=None,
        p_attacker_ahead=p,
        expected_gap_s=float(np.mean(gap)),
        interval80_s=(lo, hi),
        mc_se=math.sqrt(p * (1.0 - p) / n),
    )


def run_all(
    state: DecisionState,
    *,
    n: int = N_DEFAULT,
    seed: int = SEED_DEFAULT,
    k: int = RESPONSE_K,
    attacker_loss_shift_s: float = 0.0,
    pit_loss_shift_s: float = 0.0,
) -> SimResult:
    """Every strategy under both responses, on one set of draws."""
    _check_n_seed(n, seed)
    draws = _draws(n, seed, _horizon(state.current_lap, k))
    return SimResult(
        n=n,
        seed=seed,
        results=tuple(
            simulate(
                state,
                s,
                r,
                n=n,
                seed=seed,
                k=k,
                attacker_loss_shift_s=attacker_loss_shift_s,
                pit_loss_shift_s=pit_loss_shift_s,
                draws=draws,
            )
            for r in RESPONSES
            for s in STRATEGIES
        ),
    )


def _check_n_seed(n: int, seed: int) -> None:
    if isinstance(n, bool) or not isinstance(n, int) or not N_MIN <= n <= N_MAX:
        raise SimError(f"n must be an int in [{N_MIN}, {N_MAX}], got {n!r}")
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= SEED_MAX:
        raise SimError(f"seed must be an int in [0, {SEED_MAX}], got {seed!r}")
