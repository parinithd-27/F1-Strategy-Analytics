"""Tyre degradation per circuit x compound for the what-if simulator (#26, ``docs/sim_spec.md``).

For every target race the model is fitted on green laps of **strictly earlier** races only
(``race_seq = season*100 + round < target``), the expanding window of the pit-loss priors:

* target ``y = lap_time - the driver's green median lap of that race`` (the
  ``driver_green_pace.sql`` filter: lap > 1, no pit in/out lap, not neutralised or red flag,
  time known, >= 5 green laps; laps above 1.07 x the median are trimmed);
* ``phi``, the per-lap fuel trend, is one value per target race from a joint fit over all earlier
  green laps (an intercept and an age slope per compound, one shared ``lap`` term), because within
  one stint age and lap move together and cannot be separated;
* form ``y - phi*lap = alpha + beta*age`` (linear) or ``+ gamma*age^2`` (quadratic), chosen by
  leave-one-race-out RMSE over the earlier races (quadratic only when strictly lower; it needs at
  least 2 races at the level);
* levels in fallback order ``circuit_compound -> compound -> global -> none`` (each needs >= 30
  laps from >= 2 stints), with the level kept as the source flag.

Fits use per-race sufficient statistics (X'X, X'y, y'y), so leave-one-race-out is exact and cheap.
Compounds are the real TracingInsights ones in full mode (``clean/stints_tracinginsights``) and the
synthetic sample ones (``clean/stints``, flagged) in sample mode. The per-race stop-noise SDs come
from ``features/outlap_features`` of earlier races (``stop_delta_s``, ``outlap_pace_residual_s``).

``python -m f1sa.sim.tyre_model --source sample|lake [--allow-test-season]`` writes
``models/sim/tyre_model_{sample,full}.json`` atomically (the only place it writes). Reads go through
the API's read-only lake readers (``api.catalog.sim_readers``): no Spark, file:// or hdfs://.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import statistics
import subprocess
import sys
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import numpy as np

log = logging.getLogger(__name__)

ARTEFACT_VERSION = 1
DRY_COMPOUNDS: tuple[str, ...] = ("SOFT", "MEDIUM", "HARD")
COMPOUNDS: tuple[str, ...] = (*DRY_COMPOUNDS, "INTERMEDIATE", "WET")
UNKNOWN = "UNKNOWN"
MODEL_KEYS: tuple[str, ...] = (*COMPOUNDS, UNKNOWN)

MIN_GREEN_LAPS = 5
"""Green laps a driver needs for a race median (``etl/sql_jobs.MIN_GREEN_LAPS``)."""
INCIDENT_RATIO = 1.07
"""Laps slower than this x the reference are trimmed (``undercut.INCIDENT_LAP_RATIO``)."""
MIN_FIT_LAPS = 30
MIN_FIT_STINTS = 2
MIN_QUADRATIC_RACES = 2
MIN_SD_VALUES = 2
MAD_TO_SD = 1.4826

Source = Literal["circuit_compound", "compound", "global", "none"]
Form = Literal["linear", "quadratic", "none"]
ArtefactSource = Literal["sample", "full"]

# Columns of the detrended design [1, age, age^2] (y - phi * lap); the linear one drops age^2.
_QUAD: tuple[int, ...] = (0, 1, 2)
_LIN: tuple[int, ...] = (0, 1)


class TyreModelError(RuntimeError):
    """The tyre model cannot be fitted, written or loaded."""


def race_seq(season: int, round_: int) -> int:
    return int(season) * 100 + int(round_)


# ------------------------------------------------------------------ fitted model
@dataclass(frozen=True)
class DegFit:
    """``f(age, lap) = alpha + beta*age + gamma*age^2 + phi*lap`` (seconds vs the race median)."""

    source: Source
    form: Form
    alpha: float = 0.0
    beta: float = 0.0
    gamma: float = 0.0
    phi: float = 0.0
    n_laps: int = 0
    n_stints: int = 0
    n_races: int = 0
    rmse_linear_loro: float | None = None
    rmse_quadratic_loro: float | None = None
    residual_sd_s: float | None = None

    def predict(self, age: Any, lap: Any) -> Any:
        """Works on floats and numpy arrays alike."""
        return self.alpha + self.beta * age + self.gamma * age * age + self.phi * lap


NO_FIT = DegFit(source="none", form="none")


@dataclass(frozen=True)
class RaceTyreModel:
    """Everything the simulator needs about tyres and stop noise for one target race."""

    season: int
    round: int
    circuit_id: str | None
    fits: Mapping[str, DegFit]
    stop_delta_sd_s: float | None
    outlap_residual_sd_s: float | None
    stop_history_n: int
    is_synthetic: bool
    fuel_s_per_lap: float | None = None
    """The shared per-lap trend (phi) of every fit; None if not identified (phi = 0 used)."""

    def fit_for(self, compound: str | None) -> DegFit:
        key = compound if compound in COMPOUNDS else UNKNOWN
        return self.fits.get(key, NO_FIT)

    @property
    def stop_noise_sd_s(self) -> float:
        """sqrt(sd(stop_delta)^2 + sd(outlap residual)^2); unknown parts count as 0."""
        a = self.stop_delta_sd_s or 0.0
        b = self.outlap_residual_sd_s or 0.0
        return math.sqrt(a * a + b * b)


@dataclass(frozen=True)
class TyreModelArtefact:
    source: ArtefactSource
    lake_scheme: str
    synthetic: bool
    seasons: tuple[int, ...]
    created_utc: str
    git_sha: str | None
    git_dirty: bool | None
    races: Mapping[tuple[int, int], RaceTyreModel]
    version: int = ARTEFACT_VERSION

    def race(self, season: int, round_: int) -> RaceTyreModel | None:
        return self.races.get((season, round_))

    def to_json(self) -> str:
        body: dict[str, Any] = {
            "version": self.version,
            "source": self.source,
            "lake_scheme": self.lake_scheme,
            "synthetic": self.synthetic,
            "seasons": list(self.seasons),
            "created_utc": self.created_utc,
            "git_sha": self.git_sha,
            "git_dirty": self.git_dirty,
            "races": [
                {
                    "season": m.season,
                    "round": m.round,
                    "circuit_id": m.circuit_id,
                    "stop_delta_sd_s": m.stop_delta_sd_s,
                    "outlap_residual_sd_s": m.outlap_residual_sd_s,
                    "stop_history_n": m.stop_history_n,
                    "fuel_s_per_lap": m.fuel_s_per_lap,
                    "is_synthetic": m.is_synthetic,
                    "fits": {k: asdict(m.fits[k]) for k in MODEL_KEYS if k in m.fits},
                }
                for _, m in sorted(self.races.items())
            ],
        }
        return json.dumps(body, indent=1, sort_keys=True, allow_nan=False)

    @classmethod
    def from_json(cls, text: str) -> TyreModelArtefact:
        try:
            body = json.loads(text)
            if body.get("version") != ARTEFACT_VERSION:
                raise TyreModelError(f"unsupported tyre model version {body.get('version')!r}")
            races: dict[tuple[int, int], RaceTyreModel] = {}
            for r in body["races"]:
                fits = {k: DegFit(**v) for k, v in r["fits"].items()}
                bad = set(fits) - set(MODEL_KEYS)
                if bad:
                    raise TyreModelError(f"unknown compound keys {sorted(bad)}")
                m = RaceTyreModel(
                    season=int(r["season"]),
                    round=int(r["round"]),
                    circuit_id=r["circuit_id"],
                    fits=fits,
                    stop_delta_sd_s=r["stop_delta_sd_s"],
                    outlap_residual_sd_s=r["outlap_residual_sd_s"],
                    stop_history_n=int(r["stop_history_n"]),
                    fuel_s_per_lap=r["fuel_s_per_lap"],
                    is_synthetic=bool(r["is_synthetic"]),
                )
                races[(m.season, m.round)] = m
            source = body["source"]
            if source not in ("sample", "full"):
                raise TyreModelError(f"unknown artefact source {source!r}")
            return cls(
                source=source,
                lake_scheme=str(body["lake_scheme"]),
                synthetic=bool(body["synthetic"]),
                seasons=tuple(int(s) for s in body["seasons"]),
                created_utc=str(body["created_utc"]),
                git_sha=body["git_sha"],
                git_dirty=body["git_dirty"],
                races=races,
            )
        except TyreModelError:
            raise
        except (ValueError, KeyError, TypeError) as exc:
            raise TyreModelError(f"malformed tyre model file: {type(exc).__name__}: {exc}") from exc

    @classmethod
    def load(cls, path: Path) -> TyreModelArtefact:
        try:
            text = path.read_text()
        except OSError as exc:
            raise TyreModelError(f"tyre model file not readable: {path}") from exc
        return cls.from_json(text)


# ------------------------------------------------------------------ fitting
@dataclass(frozen=True)
class GreenLap:
    race_seq: int
    circuit_id: str | None
    stint_key: tuple[int, str, int]
    compound: str | None
    age: int
    lap: int
    y: float


@dataclass
class _Stats:
    """Sufficient statistics of the design ``[1, age, age^2, lap]`` for one (race, level key)."""

    xtx: np.ndarray = field(default_factory=lambda: np.zeros((4, 4)))
    xty: np.ndarray = field(default_factory=lambda: np.zeros(4))
    yty: float = 0.0
    n: int = 0
    stints: set[tuple[int, str, int]] = field(default_factory=set)

    def add(self, laps: Sequence[GreenLap]) -> None:
        if not laps:
            return
        a = np.array([g.age for g in laps], dtype=float)
        lap = np.array([g.lap for g in laps], dtype=float)
        y = np.array([g.y for g in laps], dtype=float)
        x = np.column_stack([np.ones_like(a), a, a * a, lap])
        self.xtx += x.T @ x
        self.xty += x.T @ y
        self.yty += float(y @ y)
        self.n += len(laps)
        self.stints.update(g.stint_key for g in laps)

    def detrended(self, phi: float) -> _Stats3:
        """The statistics of ``[1, age, age^2]`` against ``y - phi * lap``."""
        return _Stats3(
            xtx=self.xtx[:3, :3].copy(),
            xty=self.xty[:3] - phi * self.xtx[:3, 3],
            yty=self.yty - 2.0 * phi * float(self.xty[3]) + phi * phi * float(self.xtx[3, 3]),
            n=self.n,
        )


@dataclass
class _Stats3:
    xtx: np.ndarray
    xty: np.ndarray
    yty: float
    n: int


_FUEL_COLS: int = 2 * len(MODEL_KEYS) + 1


@dataclass
class _FuelStats:
    """Statistics of the joint fuel design: per compound key an intercept and an age slope,
    plus one shared ``lap`` column (the last)."""

    xtx: np.ndarray = field(default_factory=lambda: np.zeros((_FUEL_COLS, _FUEL_COLS)))
    xty: np.ndarray = field(default_factory=lambda: np.zeros(_FUEL_COLS))

    def add(self, laps: Sequence[GreenLap]) -> None:
        if not laps:
            return
        x = np.zeros((len(laps), _FUEL_COLS))
        for i, g in enumerate(laps):
            k = MODEL_KEYS.index(g.compound if g.compound in COMPOUNDS else UNKNOWN)
            x[i, 2 * k] = 1.0
            x[i, 2 * k + 1] = g.age
            x[i, -1] = g.lap
        y = np.array([g.y for g in laps], dtype=float)
        self.xtx += x.T @ x
        self.xty += x.T @ y


def fuel_trend(per_race: Sequence[_FuelStats]) -> float | None:
    """The shared per-lap trend (fuel burn-off) of the joint fit over the earlier races: each
    compound has its own intercept and age slope, so the lap term is identified across stints
    that start on different laps. ``None`` when it is not identified."""
    if not per_race:
        return None
    xtx = sum((r.xtx for r in per_race), np.zeros((_FUEL_COLS, _FUEL_COLS)))
    xty = sum((r.xty for r in per_race), np.zeros(_FUEL_COLS))
    used = tuple(i for i in range(_FUEL_COLS) if xtx[i, i] > 0.0)
    if not used or used[-1] != _FUEL_COLS - 1:
        return None
    coef = _solve(xtx, xty, used)
    return None if coef is None else float(coef[-1])


def _solve(xtx: np.ndarray, xty: np.ndarray, idx: tuple[int, ...]) -> np.ndarray | None:
    a = xtx[np.ix_(idx, idx)]
    b = xty[list(idx)]
    if np.linalg.matrix_rank(a) < len(idx):
        return None
    try:
        return np.linalg.solve(a, b)
    except np.linalg.LinAlgError:
        return None


def _sse(st: _Stats3, beta: np.ndarray, idx: tuple[int, ...]) -> float:
    a = st.xtx[np.ix_(idx, idx)]
    b = st.xty[list(idx)]
    return max(float(st.yty - 2.0 * beta @ b + beta @ a @ beta), 0.0)


def _loro_rmse(per_race: Sequence[_Stats3], total: _Stats3, idx: tuple[int, ...]) -> float | None:
    """Leave-one-race-out RMSE (pooled SSE / pooled n over the folds that can be fitted)."""
    sse, n = 0.0, 0
    for r in per_race:
        if total.n - r.n < MIN_FIT_LAPS:
            continue
        beta = _solve(total.xtx - r.xtx, total.xty - r.xty, idx)
        if beta is None:
            continue
        sse += _sse(r, beta, idx)
        n += r.n
    return math.sqrt(sse / n) if n else None


def fit_level(per_race: Sequence[_Stats], source: Source, phi: float = 0.0) -> DegFit | None:
    """The fit of one level from its earlier races (``y - phi * lap`` on ``[1, age(, age^2)]``),
    or ``None`` if the level is too thin."""
    stints: set[tuple[int, str, int]] = set()
    for st in per_race:
        stints |= st.stints
    races = [r.detrended(phi) for r in per_race if r.n > 0]
    total = _Stats3(np.zeros((3, 3)), np.zeros(3), 0.0, 0)
    for r in races:
        total = _Stats3(total.xtx + r.xtx, total.xty + r.xty, total.yty + r.yty, total.n + r.n)
    if total.n < MIN_FIT_LAPS or len(stints) < MIN_FIT_STINTS:
        return None
    lin = _solve(total.xtx, total.xty, _LIN)
    if lin is None:
        return None
    rmse_lin = rmse_quad = None
    quad = None
    if len(races) >= MIN_QUADRATIC_RACES:
        rmse_lin = _loro_rmse(races, total, _LIN)
        rmse_quad = _loro_rmse(races, total, _QUAD)
        quad = _solve(total.xtx, total.xty, _QUAD)
    use_quad = (
        quad is not None and rmse_lin is not None and rmse_quad is not None and rmse_quad < rmse_lin
    )
    idx: tuple[int, ...]
    if use_quad:
        assert quad is not None
        alpha, beta, gamma = (float(v) for v in quad)
        idx, coef = _QUAD, quad
    else:
        alpha, beta = (float(v) for v in lin)
        gamma = 0.0
        idx, coef = _LIN, lin
    dof = total.n - len(idx)
    sd = math.sqrt(_sse(total, coef, idx) / dof) if dof > 0 else None
    return DegFit(
        source=source,
        form="quadratic" if use_quad else "linear",
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        phi=phi,
        n_laps=total.n,
        n_stints=len(stints),
        n_races=len(races),
        rmse_linear_loro=rmse_lin,
        rmse_quadratic_loro=rmse_quad,
        residual_sd_s=sd,
    )


def _normalise_compound(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    c = value.strip().upper()
    return c if c in COMPOUNDS else None


def green_laps(
    season: int,
    round_: int,
    circuit_id: str | None,
    laps: Iterable[Mapping[str, Any]],
    stints: Iterable[Mapping[str, Any]],
) -> list[GreenLap]:
    """The fit rows of one race (clean ``laps`` + a stints table)."""
    by_driver: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for r in laps:
        d = r.get("driver_id")
        if (
            isinstance(d, str)
            and isinstance(r.get("lap"), int)
            and r["lap"] > 1
            and not r.get("is_pit_in_lap")
            and not r.get("is_pit_out_lap")
            and not r.get("is_neutralised_lap")
            and not r.get("is_red_flag_lap")
            and isinstance(r.get("lap_time_s"), float | int)
            and math.isfinite(r["lap_time_s"])
        ):
            by_driver[d].append(r)
    stints_by_driver: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for s in stints:
        if isinstance(s.get("driver_id"), str) and isinstance(s.get("start_lap"), int):
            stints_by_driver[s["driver_id"]].append(s)
    seq = race_seq(season, round_)
    out: list[GreenLap] = []
    for d, rows in sorted(by_driver.items()):
        if len(rows) < MIN_GREEN_LAPS:
            continue
        median = statistics.median(float(r["lap_time_s"]) for r in rows)
        for r in sorted(rows, key=lambda r: r["lap"]):
            t = float(r["lap_time_s"])
            if t > INCIDENT_RATIO * median:
                continue
            lap = int(r["lap"])
            stint = next(
                (
                    s
                    for s in stints_by_driver.get(d, [])
                    if s["start_lap"] <= lap <= (s.get("end_lap") or s["start_lap"])
                ),
                None,
            )
            if stint is None:
                continue
            age0 = stint.get("tyre_age_at_start")
            age = (int(age0) if isinstance(age0, int) else 0) + lap - int(stint["start_lap"]) + 1
            out.append(
                GreenLap(
                    race_seq=seq,
                    circuit_id=circuit_id,
                    stint_key=(seq, d, int(stint.get("stint") or 0)),
                    compound=_normalise_compound(stint.get("compound")),
                    age=age,
                    lap=lap,
                    y=t - median,
                )
            )
    return out


@dataclass(frozen=True)
class RaceInputs:
    """What the fit reads of one race."""

    season: int
    round: int
    circuit_id: str | None
    laps: list[GreenLap]
    stop_deltas: list[float]
    outlap_residuals: list[float]
    is_synthetic: bool


def robust_sd(values: Sequence[float]) -> float | None:
    """1.4826 x the median absolute deviation (a normal-consistent SD that ignores the heavy
    tails of the stop history: unsafe releases, damage, penalties); ``None`` below 2 values."""
    if len(values) < MIN_SD_VALUES:
        return None
    med = statistics.median(values)
    return MAD_TO_SD * statistics.median(abs(v - med) for v in values)


def fit_races(races: Sequence[RaceInputs]) -> dict[tuple[int, int], RaceTyreModel]:
    """One ``RaceTyreModel`` per race, each from strictly earlier races only."""
    ordered = sorted(races, key=lambda r: race_seq(r.season, r.round))
    # per race: stats per level key
    stats: list[dict[tuple[str, ...], _Stats]] = []
    fuel: list[_FuelStats] = []
    for r in ordered:
        per: dict[tuple[str, ...], _Stats] = defaultdict(_Stats)
        grouped: dict[tuple[str, ...], list[GreenLap]] = defaultdict(list)
        for g in r.laps:
            grouped[("global",)].append(g)
            if g.compound is not None:
                grouped[("compound", g.compound)].append(g)
                if g.circuit_id is not None:
                    grouped[("circuit_compound", g.circuit_id, g.compound)].append(g)
        for key, rows in grouped.items():
            per[key].add(rows)
        stats.append(dict(per))
        fs = _FuelStats()
        fs.add(r.laps)
        fuel.append(fs)
    out: dict[tuple[int, int], RaceTyreModel] = {}
    for i, target in enumerate(ordered):
        earlier = stats[:i]
        phi = fuel_trend(fuel[:i])

        def level(
            key: tuple[str, ...],
            source: Source,
            earlier: list[dict[tuple[str, ...], _Stats]] = earlier,
            phi: float = phi or 0.0,
        ) -> DegFit | None:
            return fit_level([e[key] for e in earlier if key in e], source, phi)

        global_fit = level(("global",), "global")
        fits: dict[str, DegFit] = {UNKNOWN: global_fit or NO_FIT}
        for c in COMPOUNDS:
            fit = None
            if target.circuit_id is not None:
                fit = level(("circuit_compound", target.circuit_id, c), "circuit_compound")
            fit = fit or level(("compound", c), "compound") or global_fit or NO_FIT
            fits[c] = fit
        deltas = [v for r in ordered[:i] for v in r.stop_deltas]
        resid = [v for r in ordered[:i] for v in r.outlap_residuals]
        out[(target.season, target.round)] = RaceTyreModel(
            season=target.season,
            round=target.round,
            circuit_id=target.circuit_id,
            fits=fits,
            stop_delta_sd_s=robust_sd(deltas),
            outlap_residual_sd_s=robust_sd(resid),
            stop_history_n=min(len(deltas), len(resid)),
            fuel_s_per_lap=phi,
            is_synthetic=target.is_synthetic,
        )
    return out


# ------------------------------------------------------------------ reading the lake
LAP_COLUMNS = (
    "driver_id",
    "lap",
    "lap_time_s",
    "is_pit_in_lap",
    "is_pit_out_lap",
    "is_neutralised_lap",
    "is_red_flag_lap",
)
STINT_COLUMNS = (
    "driver_id",
    "stint",
    "start_lap",
    "end_lap",
    "compound",
    "tyre_age_at_start",
    "is_synthetic",
)
OUTLAP_COLUMNS = ("stop_delta_s", "outlap_pace_residual_s", "is_long_stop", "outlap_neutralised")


def _finite(values: Iterable[Any]) -> list[float]:
    return [float(v) for v in values if isinstance(v, float | int) and math.isfinite(v)]


def read_race_inputs(
    clean: Any, features: Any, season: int, round_: int, *, stints_table: str
) -> RaceInputs:
    """One race through the read-only lake readers (``api.catalog.sim_readers``)."""
    races = clean.read_rows("races", season, round_, ["circuit_id"])
    circuit = races[0].get("circuit_id") if races else None
    laps = clean.read_rows("laps", season, round_, list(LAP_COLUMNS))
    ti = stints_table == "stints_tracinginsights"
    cols = [*STINT_COLUMNS, "compound_conflict"] if ti else list(STINT_COLUMNS)
    stints = clean.read_rows(stints_table, season, round_, cols)
    for s in stints:  # a stint whose lap labels disagree has no reliable compound
        if s.get("compound_conflict"):
            s["compound"] = None
    if not stints and stints_table != "stints":
        stints = clean.read_rows("stints", season, round_, list(STINT_COLUMNS))
        for s in stints:  # inferred stints in full mode: no real compound
            s["compound"] = None
    outlap = features.read_rows("outlap_features", season, round_, list(OUTLAP_COLUMNS))
    kept = [r for r in outlap if not r.get("is_long_stop") and not r.get("outlap_neutralised")]
    return RaceInputs(
        season=season,
        round=round_,
        circuit_id=circuit if isinstance(circuit, str) else None,
        laps=green_laps(season, round_, circuit, laps, stints),
        stop_deltas=_finite(r.get("stop_delta_s") for r in kept),
        outlap_residuals=_finite(r.get("outlap_pace_residual_s") for r in kept),
        is_synthetic=any(s.get("is_synthetic") is True for s in stints),
    )


def _git() -> tuple[str | None, bool | None]:
    sha = os.environ.get("F1SA_GIT_SHA", "").strip() or None
    dirty = {"true": True, "false": False}.get(os.environ.get("F1SA_GIT_DIRTY", "").strip().lower())
    if sha is None:
        try:
            sha = (
                subprocess.run(
                    ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
                ).stdout.strip()
                or None
            )
            dirty = bool(
                subprocess.run(
                    ["git", "status", "--porcelain"], capture_output=True, text=True, check=True
                ).stdout.strip()
            )
        except (OSError, subprocess.CalledProcessError):
            return None, None
    return sha, dirty


def build_artefact(
    clean: Any,
    features: Any,
    *,
    source: ArtefactSource,
    lake_scheme: str,
    now: datetime | None = None,
) -> TyreModelArtefact:
    """Fit every race the clean zone lists (``laps`` with a ``races`` row)."""
    targets = sorted(set(clean.partitions("laps")) & set(clean.partitions("races")))
    if not targets:
        raise TyreModelError("the clean zone lists no race")
    stints_table = "stints_tracinginsights" if source == "full" else "stints"
    inputs = [
        read_race_inputs(clean, features, s, r, stints_table=stints_table) for s, r in targets
    ]
    races = fit_races(inputs)
    sha, dirty = _git()
    return TyreModelArtefact(
        source=source,
        lake_scheme=lake_scheme,
        synthetic=any(r.is_synthetic for r in inputs),
        seasons=tuple(sorted({s for s, _ in targets})),
        created_utc=(now or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        git_sha=sha,
        git_dirty=dirty,
        races=races,
    )


def default_output(source: ArtefactSource, root: Path | None = None) -> Path:
    return (root or Path.cwd()) / "models" / "sim" / f"tyre_model_{source}.json"


def check_output_path(path: Path, *, root: Path | None = None) -> Path:
    """The resolved ``path`` if it is a ``.json`` file directly in ``<root>/models/sim``."""
    allowed = ((root or Path.cwd()) / "models" / "sim").resolve()
    target = path.resolve()
    if target.parent != allowed or target.suffix != ".json":
        raise TyreModelError(f"refusing to write {path}: only {allowed}/*.json is allowed")
    return target


def write_artefact(artefact: TyreModelArtefact, path: Path, *, root: Path | None = None) -> Path:
    """Atomic write; refuses any path outside ``<root>/models/sim``."""
    target = check_output_path(path, root=root)
    allowed = target.parent
    allowed.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=allowed, prefix=".tyre_model.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(artefact.to_json())
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, target)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return target


def main(argv: Sequence[str] | None = None) -> int:
    from f1sa.api.catalog import CatalogError, sim_readers
    from f1sa.config import get_settings

    parser = argparse.ArgumentParser(prog="python -m f1sa.sim.tyre_model", description=__doc__)
    parser.add_argument("--source", choices=("sample", "lake"), required=True)
    parser.add_argument("--allow-test-season", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    settings = get_settings()
    source: ArtefactSource = "sample" if args.source == "sample" else "full"
    scheme = settings.data_lake_uri.split("://", 1)[0]
    if source == "sample" and scheme != "file":
        print("--source sample needs a file:// DATA_LAKE_URI", file=sys.stderr)
        return 2
    try:
        out = check_output_path(args.out or default_output(source))
        readers = sim_readers(settings, allow_test_season=args.allow_test_season)
        artefact = build_artefact(
            readers.clean, readers.features, source=source, lake_scheme=scheme
        )
        if source == "sample" and not artefact.synthetic:
            raise TyreModelError("sample source but no synthetic stints: is this the sample lake?")
        path = write_artefact(artefact, out)
    except (CatalogError, TyreModelError) as exc:
        print(f"tyre model failed: {exc}", file=sys.stderr)
        return 1
    counts: dict[str, int] = defaultdict(int)
    for m in artefact.races.values():
        for f in m.fits.values():
            counts[f"{f.source}/{f.form}"] += 1
    print(
        f"wrote {path} ({len(artefact.races)} races, seasons {list(artefact.seasons)}, "
        f"synthetic={artefact.synthetic}); fits: {dict(sorted(counts.items()))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
