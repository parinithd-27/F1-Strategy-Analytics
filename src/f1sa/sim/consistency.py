"""What-if consistency checks and backtest (#32, #40; pre-registered in ``docs/sim_spec.md``).

1. monotonicity: P(pit_now) never increases when the attacker's stop loss increases;
2. determinism: the same seed gives byte-identical results;
3. ordering: on a synthetic grid, P(pit_now) falls with the gap and pit_now vs pit_next_lap
   follows the closing rate;
4. backtest: simulator P(pit_now) and the model's P against the observed labels (Brier,
   calibration in 10 and 5 bins, ECE) and their agreement, per season subset, with 2025 as a
   separate held-out row.

Stop rule: if |mean(P_sim - P_model)| > 0.20 on 2021-2024 the run stops (exit 3) before 2025 is
read and writes nothing to ``results/``. Everything here is pure Python + NumPy; the Spark read
of the backtest cases is ``sim/backtest.py``. ``experiments/whatif_consistency.py`` is the entry.
"""

from __future__ import annotations

import csv
import itertools
import json
import math
import os
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np

from f1sa.sim.race_sim import (
    N_DEFAULT,
    SEED_DEFAULT,
    CarState,
    DecisionState,
    StrategyResult,
    run_all,
    simulate,
)
from f1sa.sim.tyre_model import COMPOUNDS, UNKNOWN, RaceTyreModel

LOSS_SHIFTS_S: tuple[float, ...] = (-2.0, -1.0, 0.0, 1.0, 2.0, 4.0)
GRID_GAPS_S: tuple[float, ...] = (0.5, 1.0, 2.0, 3.5, 5.0)
GRID_RATES_S: tuple[float, ...] = (-0.5, 0.0, 0.5)
STOP_THRESHOLD = 0.20
BINS: tuple[int, ...] = (10, 5)
CSV_COLUMNS = ("check", "subset", "response", "metric", "value", "n", "pass", "post_hoc")
POST_HOC_NOTE = (
    "post hoc (added 2026-11-13 after the development run, docs/sim_spec.md): sign of "
    "E[gap | pit_next_lap] - E[gap | pit_now]; information only, never a pass/fail criterion"
)
TRAIN, VALIDATION, TEST = (2021, 2022, 2023), (2024,), (2025,)
SUBSETS: dict[str, tuple[int, ...]] = {
    "2021-2023": TRAIN,
    "2024": VALIDATION,
    "2021-2024": TRAIN + VALIDATION,
}
HELD_OUT = ("2025", TEST)


@dataclass(frozen=True)
class Case:
    """One labelled attempt with its decision state at the end of lap L-1."""

    season: int
    round: int
    attacker: str
    defender: str
    lap: int
    label: int
    model_p: float
    state: DecisionState
    closing_s_per_lap: float | None = None
    """Minus the attacker's 1-lap gap change at L-1 (positive: A is closing)."""


@dataclass(frozen=True)
class Row:
    check: str
    subset: str
    response: str
    metric: str
    value: float | None
    n: int
    passed: bool | None = None
    post_hoc: bool = False
    """Supplementary rows added after seeing development data (never pass/fail)."""

    def __post_init__(self) -> None:
        if self.post_hoc and self.passed is not None:
            raise ValueError("a post-hoc row is never a pass/fail criterion")

    def csv(self) -> dict[str, Any]:
        v = "" if self.value is None else repr(float(self.value))
        p = "" if self.passed is None else str(self.passed).lower()
        return {
            "check": self.check,
            "subset": self.subset,
            "response": self.response,
            "metric": self.metric,
            "value": v,
            "n": self.n,
            "pass": p,
            "post_hoc": str(self.post_hoc).lower(),
        }


# ------------------------------------------------------------------ metrics
def brier(p: Sequence[float], y: Sequence[int]) -> float:
    a, b = np.asarray(p, float), np.asarray(y, float)
    return float(np.mean((a - b) ** 2))


def calibration_bins(
    p: Sequence[float], y: Sequence[int], bins: int
) -> list[tuple[int, int, float | None, float | None]]:
    """``(bin, n, mean p, observed rate)`` of equal-width bins on [0, 1] (p = 1 in the last)."""
    a, b = np.asarray(p, float), np.asarray(y, float)
    idx = np.minimum((a * bins).astype(int), bins - 1)
    out = []
    for i in range(bins):
        m = idx == i
        n = int(m.sum())
        out.append((i, n, float(a[m].mean()) if n else None, float(b[m].mean()) if n else None))
    return out


def ece(p: Sequence[float], y: Sequence[int], bins: int) -> float:
    total = len(p)
    return float(
        sum(
            n / total * abs(mp - oy)
            for _, n, mp, oy in calibration_bins(p, y, bins)
            if n and mp is not None and oy is not None
        )
    )


def _ranks(x: np.ndarray) -> np.ndarray:
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty(len(x), float)
    xs = x[order]
    i = 0
    while i < len(x):
        j = i
        while j + 1 < len(x) and xs[j + 1] == xs[i]:
            j += 1
        ranks[order[i : j + 1]] = (i + j) / 2.0
        i = j + 1
    return ranks


def spearman(a: Sequence[float], b: Sequence[float]) -> float | None:
    x, y = np.asarray(a, float), np.asarray(b, float)
    if len(x) < 3:
        return None
    rx, ry = _ranks(x), _ranks(y)
    if rx.std() == 0 or ry.std() == 0:
        return None
    return float(np.corrcoef(rx, ry)[0, 1])


# ------------------------------------------------------------------ checks
def _p(r: StrategyResult) -> float | None:
    return r.p_attacker_ahead if r.feasible else None


def monotonicity(
    cases: Sequence[Case], *, n: int = N_DEFAULT, seed: int = SEED_DEFAULT
) -> tuple[list[Row], dict[str, list[float]]]:
    """Check 1 (pass/fail on A's loss) plus the information-only shift of both cars' loss."""
    rows: list[Row] = []
    curves: dict[str, list[float]] = {}
    for kind in ("attacker_loss", "both_loss"):
        per_shift: list[list[float]] = [[] for _ in LOSS_SHIFTS_S]
        violations, max_increase, used = 0, 0.0, 0
        for c in cases:
            ps = [
                _p(
                    simulate(
                        c.state,
                        "pit_now",
                        "primary",
                        n=n,
                        seed=seed,
                        attacker_loss_shift_s=d if kind == "attacker_loss" else 0.0,
                        pit_loss_shift_s=d if kind == "both_loss" else 0.0,
                    )
                )
                for d in LOSS_SHIFTS_S
            ]
            if any(p is None for p in ps):
                continue
            used += 1
            vals = [float(p) for p in ps if p is not None]
            for i, v in enumerate(vals):
                per_shift[i].append(v)
            steps = [b - a for a, b in itertools.pairwise(vals)]
            if any(s > 0 for s in steps):
                violations += 1
                max_increase = max(max_increase, max(steps))
        curves[kind] = [float(np.mean(v)) if v else math.nan for v in per_shift]
        strict = kind == "attacker_loss"
        rows.append(
            Row(
                "monotonicity",
                kind,
                "primary",
                "cases_with_an_increase",
                float(violations),
                used,
                (violations == 0) if strict else None,
            )
        )
        rows.append(Row("monotonicity", kind, "primary", "max_increase", max_increase, used, None))
        for d, m in zip(LOSS_SHIFTS_S, curves[kind], strict=True):
            rows.append(
                Row("monotonicity", kind, "primary", f"mean_p_at_shift_{d:+.0f}s", m, used, None)
            )
    return rows, curves


def determinism(
    cases: Sequence[Case], *, n: int = N_DEFAULT, seed: int = SEED_DEFAULT
) -> list[Row]:
    same, spread, used = 0, 0.0, 0
    for c in cases:
        a = run_all(c.state, n=n, seed=seed)
        b = run_all(c.state, n=n, seed=seed)
        other = run_all(c.state, n=n, seed=seed + 1)
        used += 1
        if json.dumps(asdict(a), sort_keys=True) == json.dumps(asdict(b), sort_keys=True):
            same += 1
        for x, y in zip(a.results, other.results, strict=True):
            if x.feasible and y.feasible:
                spread = max(spread, abs((x.p_attacker_ahead or 0.0) - (y.p_attacker_ahead or 0.0)))
    return [
        Row(
            "determinism",
            "backtest_states",
            "all",
            "identical_with_same_seed",
            float(same),
            used,
            same == used,
        ),
        Row("determinism", "backtest_states", "all", "max_abs_dp_seed_plus_1", spread, used, None),
    ]


def grid_tyres(tyres: RaceTyreModel) -> RaceTyreModel:
    """Every compound uses the race's global fit (equal tyres for the synthetic grid)."""
    g = tyres.fit_for(UNKNOWN)
    return replace(tyres, fits={k: g for k in (*COMPOUNDS, UNKNOWN)})


def grid_state(tyres: RaceTyreModel, gap: float, rate: float) -> DecisionState:
    """Equal cars on MEDIUM, age 20, lap 20; A faster by ``rate`` s/lap; 22 s stop loss."""
    sd = tyres.fit_for(UNKNOWN).residual_sd_s or 1.0
    d = CarState("defender", 2000.0, "MEDIUM", 20, 0.0, 5, sd, 22.0)
    a = CarState("attacker", 2000.0 + gap, "MEDIUM", 20, -rate, 5, sd, 22.0)
    return DecisionState(0, 0, 20, None, a, d, grid_tyres(tyres))


def ordering(
    tyres: RaceTyreModel, *, n: int = N_DEFAULT, seed: int = SEED_DEFAULT
) -> tuple[list[Row], dict[tuple[float, float], tuple[float, float]]]:
    """Check 3 on the synthetic grid: ``{(gap, rate): (P_now, P_next)}`` and the rows."""
    grid: dict[tuple[float, float], tuple[float, float]] = {}
    egap: dict[tuple[float, float], float] = {}
    for rate in GRID_RATES_S:
        for gap in GRID_GAPS_S:
            st = grid_state(tyres, gap, rate)
            now_r = simulate(st, "pit_now", "primary", n=n, seed=seed)
            next_r = simulate(st, "pit_next_lap", "primary", n=n, seed=seed)
            p_now, p_next = now_r.p_attacker_ahead, next_r.p_attacker_ahead
            assert p_now is not None and p_next is not None
            assert now_r.expected_gap_s is not None and next_r.expected_gap_s is not None
            grid[(gap, rate)] = (p_now, p_next)
            egap[(gap, rate)] = next_r.expected_gap_s - now_r.expected_gap_s
    rows: list[Row] = []

    def se(p: float) -> float:
        return math.sqrt(p * (1 - p) / n)

    for rate in GRID_RATES_S:
        nows = [grid[(g, rate)][0] for g in GRID_GAPS_S]
        mono = all(b <= a for a, b in itertools.pairwise(nows))
        rows.append(
            Row(
                "ordering",
                f"rate_{rate:+.1f}",
                "primary",
                "p_now_non_increasing_in_gap",
                None,
                len(nows),
                mono,
            )
        )
        small, large = grid[(GRID_GAPS_S[0], rate)], grid[(GRID_GAPS_S[-1], rate)]
        rows.append(
            Row(
                "ordering",
                f"rate_{rate:+.1f}",
                "primary",
                "large_gap_below_small_gap",
                None,
                2,
                large[0] < small[0] and large[1] < small[1],
            )
        )
        for gap in GRID_GAPS_S:
            p_now, p_next = grid[(gap, rate)]
            tol = se(max(p_now, p_next))
            if rate > 0:
                ok: bool | None = p_next >= p_now - tol
            elif rate < 0:
                ok = p_now >= p_next - tol
            else:
                ok = None
            rows.append(
                Row(
                    "ordering",
                    f"rate_{rate:+.1f}_gap_{gap:.1f}",
                    "primary",
                    "p_next_minus_p_now",
                    p_next - p_now,
                    n,
                    ok,
                )
            )
    rows += ordering_post_hoc(egap, n=n)
    return rows, grid


def ordering_post_hoc(egap: dict[tuple[float, float], float], *, n: int) -> list[Row]:
    """Post-hoc supplementary rows (see ``POST_HOC_NOTE``): per grid cell the expected-gap
    difference pit_next_lap - pit_now and its sign (+1: waiting a lap helps the attacker on
    average), and whether that sign follows the closing rate. Never pass/fail."""
    rows: list[Row] = []
    for (gap, rate), d in sorted(egap.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        sub = f"rate_{rate:+.1f}_gap_{gap:.1f}"
        sign = float(np.sign(d))
        rows.append(
            Row("ordering_posthoc", sub, "primary", "egap_next_minus_now_s", d, n, None, True)
        )
        rows.append(
            Row("ordering_posthoc", sub, "primary", "egap_sign_next_minus_now", sign, n, None, True)
        )
        if rate != 0:
            follows = sign == float(np.sign(rate))
            rows.append(
                Row(
                    "ordering_posthoc",
                    sub,
                    "primary",
                    "egap_sign_follows_rate",
                    float(follows),
                    n,
                    None,
                    True,
                )
            )
    return rows


def ordering_on_states(
    cases: Sequence[Case], *, n: int = N_DEFAULT, seed: int = SEED_DEFAULT
) -> list[Row]:
    """Information only: share of real states where sign(P_next - P_now) = sign(closing rate)."""
    agree = used = 0
    for c in cases:
        if c.closing_s_per_lap is None or c.closing_s_per_lap == 0:
            continue
        now = _p(simulate(c.state, "pit_now", "primary", n=n, seed=seed))
        nxt = _p(simulate(c.state, "pit_next_lap", "primary", n=n, seed=seed))
        if now is None or nxt is None or now == nxt:
            continue
        used += 1
        agree += (nxt > now) == (c.closing_s_per_lap > 0)
    return [
        Row(
            "ordering",
            "backtest_states",
            "primary",
            "sign_agrees_with_closing_rate",
            agree / used if used else None,
            used,
            None,
        )
    ]


@dataclass(frozen=True)
class Scored:
    case: Case
    p_sim: dict[str, float | None]


def score(cases: Sequence[Case], *, n: int = N_DEFAULT, seed: int = SEED_DEFAULT) -> list[Scored]:
    out = []
    for c in cases:
        res = run_all(c.state, n=n, seed=seed)
        out.append(Scored(c, {r: _p(res.get("pit_now", r)) for r in ("primary", "sensitivity")}))
    return out


def predicted(s: Scored, which: str) -> float | None:
    """The simulator's P(pit_now) for a response (``primary`` / ``sensitivity``) or ``model``."""
    return s.case.model_p if which == "model" else s.p_sim[which]


def agreement(scored: Sequence[Scored], response: str = "primary") -> tuple[float | None, int]:
    d = [q - s.case.model_p for s in scored if (q := predicted(s, response)) is not None]
    return (float(np.mean(d)) if d else None), len(d)


def _metric_rows(scored: Sequence[Scored], subset: str, which: str) -> list[Row]:
    name = "model" if which == "model" else "simulator"
    pairs = [(q, s.case.label) for s in scored if (q := predicted(s, which)) is not None]
    if not pairs:
        return [Row("backtest", subset, which, f"{name}_brier", None, 0)]
    p, y = [a for a, _ in pairs], [b for _, b in pairs]
    n = len(p)
    rows = [
        Row("backtest", subset, which, f"{name}_brier", brier(p, y), n),
        Row("backtest", subset, which, f"{name}_mean_p", float(np.mean(p)), n),
        Row("backtest", subset, which, "observed_rate", float(np.mean(y)), n),
    ]
    for bins in BINS:
        rows.append(Row("backtest", subset, which, f"{name}_ece_{bins}bins", ece(p, y, bins), n))
        for i, nb, mp, oy in calibration_bins(p, y, bins):
            rows.append(Row("backtest", subset, which, f"{name}_bin{bins}_{i:02d}_mean_p", mp, nb))
            rows.append(
                Row("backtest", subset, which, f"{name}_bin{bins}_{i:02d}_observed", oy, nb)
            )
    return rows


def backtest(scored: Sequence[Scored], subset: str) -> list[Row]:
    """Brier, calibration and ECE of the simulator (both responses) and the model, and their
    agreement, on one subset."""
    rows: list[Row] = []
    for which in ("primary", "sensitivity", "model"):
        rows += _metric_rows(scored, subset, which)
    for response in ("primary", "sensitivity"):
        pairs = [(q, s.case.model_p) for s in scored if (q := predicted(s, response)) is not None]
        diffs = [a - b for a, b in pairs]
        n = len(diffs)
        rows += [
            Row(
                "agreement",
                subset,
                response,
                "mean_sim_minus_model",
                float(np.mean(diffs)) if n else None,
                n,
            ),
            Row(
                "agreement",
                subset,
                response,
                "mean_abs_sim_minus_model",
                float(np.mean(np.abs(diffs))) if n else None,
                n,
            ),
            Row(
                "agreement",
                subset,
                response,
                "spearman_sim_model",
                spearman([a for a, _ in pairs], [b for _, b in pairs]),
                n,
            ),
        ]
    infeasible = float(sum(s.p_sim["primary"] is None for s in scored))
    rows.append(Row("backtest", subset, "all", "infeasible_cases", infeasible, len(scored)))
    return rows


def in_seasons(scored: Iterable[Scored], seasons: Sequence[int]) -> list[Scored]:
    return [s for s in scored if s.case.season in seasons]


def stop_check(scored: Sequence[Scored]) -> tuple[float | None, bool]:
    """``(mean P_sim - P_model on the primary response, stop?)``."""
    mean, _n = agreement(scored, "primary")
    return mean, mean is not None and abs(mean) > STOP_THRESHOLD


# ------------------------------------------------------------------ outputs
def write_csv(rows: Sequence[Row], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(CSV_COLUMNS), lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow(r.csv())
    os.replace(tmp, path)


COLORS = ("#2a78d6", "#eb6834", "#1baf7a")
"""Categorical slots 1-3 of the reference palette (light), each paired with its own marker."""
SURFACE, INK, MUTED = "#fcfcfb", "#0b0b0b", "#52514e"


def write_png(
    path: Path,
    curves: dict[str, list[float]],
    grid: dict[tuple[float, float], tuple[float, float]],
    dev: Sequence[Scored],
    held_out: Sequence[Scored],
    *,
    title: str,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.size": 9,
            "axes.edgecolor": MUTED,
            "axes.labelcolor": INK,
            "text.color": INK,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
        }
    )
    fig, ax = plt.subplots(2, 2, figsize=(11, 8.5), facecolor=SURFACE)
    for a in ax.flat:
        a.set_facecolor(SURFACE)
        a.grid(True, color="#e4e3df", linewidth=0.6)
        a.spines[["top", "right"]].set_visible(False)
    a = ax[0, 0]
    for (kind, label), c, m in zip(
        (
            ("attacker_loss", "attacker's loss shifted (pass/fail)"),
            ("both_loss", "both cars' loss shifted (info)"),
        ),
        COLORS,
        ("o", "s"),
        strict=False,
    ):
        a.plot(
            LOSS_SHIFTS_S,
            curves.get(kind, []),
            color=c,
            marker=m,
            markersize=6,
            linewidth=2,
            label=label,
        )
    a.set(
        title="1. Monotonicity: mean P(pit_now) vs stop-loss shift",
        xlabel="loss shift (s)",
        ylabel="mean P(attacker ahead)",
        ylim=(0, 1),
    )
    a.legend(frameon=False)
    a = ax[0, 1]
    for rate, c, m in zip(GRID_RATES_S, COLORS, ("v", "o", "^"), strict=True):
        a.plot(
            GRID_GAPS_S,
            [grid[(g, rate)][0] for g in GRID_GAPS_S],
            color=c,
            marker=m,
            markersize=6,
            linewidth=2,
            label=f"pit_now, closing {rate:+.1f} s/lap",
        )
        a.plot(
            GRID_GAPS_S,
            [grid[(g, rate)][1] for g in GRID_GAPS_S],
            color=c,
            marker=m,
            markersize=6,
            linewidth=2,
            linestyle="--",
            label=f"pit_next_lap, closing {rate:+.1f} s/lap",
        )
    a.set(
        title="3. Ordering on the synthetic grid",
        xlabel="gap at decision (s)",
        ylabel="P(attacker ahead)",
        ylim=(0, 1),
    )
    a.legend(frameon=False, fontsize=7)
    a = ax[1, 0]
    a.plot([0, 1], [0, 1], color=MUTED, linewidth=1, linestyle=":", label="perfect calibration")
    for (name, sc, which), c, m in zip(
        (
            ("simulator <=2024", dev, "primary"),
            ("model <=2024", dev, "model"),
            ("simulator 2025 (held out)", held_out, "primary"),
        ),
        COLORS,
        ("o", "s", "D"),
        strict=True,
    ):
        pairs = [(q, s.case.label) for s in sc if (q := predicted(s, which)) is not None]
        if not pairs:
            continue
        bins = [
            b for b in calibration_bins([p for p, _ in pairs], [y for _, y in pairs], 10) if b[1]
        ]
        a.plot(
            [b[2] for b in bins],
            [b[3] for b in bins],
            color=c,
            marker=m,
            markersize=7,
            linewidth=2,
            label=f"{name} (n={len(pairs)})",
        )
    a.set(
        title="4. Reliability (10 bins): P(pit_now) vs observed success",
        xlabel="mean predicted P",
        ylabel="observed success rate",
        xlim=(0, 1),
        ylim=(0, 1),
    )
    a.legend(frameon=False, fontsize=7)
    a = ax[1, 1]
    a.plot([0, 1], [0, 1], color=MUTED, linewidth=1, linestyle=":")
    for (name, sc), c, m in zip(
        (("<=2024", dev), ("2025 (held out)", held_out)), COLORS, ("o", "D"), strict=False
    ):
        pts = [(s.case.model_p, s.p_sim["primary"]) for s in sc if s.p_sim["primary"] is not None]
        if pts:
            a.scatter(
                [x for x, _ in pts],
                [y for _, y in pts],
                s=22,
                color=c,
                marker=m,
                edgecolors=SURFACE,
                linewidths=0.8,
                label=f"{name} (n={len(pts)})",
            )
    a.set(
        title="4. Agreement: simulator vs model P",
        xlabel="model P (calibrated LR)",
        ylabel="simulator P(pit_now)",
        xlim=(0, 1),
        ylim=(0, 1),
    )
    a.legend(frameon=False)
    fig.suptitle(title, color=INK)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.stem + ".tmp.png")
    fig.savefig(tmp, dpi=130, facecolor=SURFACE)
    plt.close(fig)
    os.replace(tmp, path)


# ------------------------------------------------------------------ entry point
RECORD_DIR = Path("results")
RECORD_CSV, RECORD_PNG = "whatif_consistency.csv", "whatif_consistency.png"
RUN_ROOT = Path("data/whatif")
EXIT_OK, EXIT_CHECK_FAILED, EXIT_REFUSED, EXIT_STOP = 0, 1, 2, 3


def run_id(now: Any = None) -> str:
    from datetime import UTC, datetime

    t = now or datetime.now(UTC)
    return t.strftime("%Y%m%dT%H%M%SZ")


def output_dir(*, record: bool, source: str, now: Any = None) -> Path:
    """``results/`` only for a record run; otherwise ``data/whatif/<source>_<run id>/``."""
    return RECORD_DIR if record else RUN_ROOT / f"{source}_{run_id(now)}"


def check_record_args(
    *, source: str, record: bool, allow_test_season: bool, out: Path
) -> str | None:
    """Why the arguments are refused, or ``None``."""
    if record and source != "full":
        return "--record is for --source full only (the sample is a smoke test)"
    if allow_test_season and not record:
        return "--allow-test-season needs --record: 2025 is read only by the recorded run"
    if record:
        for name in (RECORD_CSV, RECORD_PNG):
            if (out / name).exists():
                return f"{out / name} exists and is never overwritten"
    return None


def evaluate(
    dev: Sequence[Case],
    tyres_for_grid: RaceTyreModel,
    *,
    n: int,
    seed: int,
) -> tuple[
    list[Row], dict[str, list[float]], dict[tuple[float, float], tuple[float, float]], list[Scored]
]:
    rows: list[Row] = []
    mono, curves = monotonicity(dev, n=n, seed=seed)
    rows += mono
    rows += determinism(dev, n=n, seed=seed)
    order, grid = ordering(tyres_for_grid, n=n, seed=seed)
    rows += order
    rows += ordering_on_states(dev, n=n, seed=seed)
    scored = score(dev, n=n, seed=seed)
    for name, seasons in SUBSETS.items():
        rows += backtest(in_seasons(scored, seasons), name)
    return rows, curves, grid, scored


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    from f1sa.config import get_settings
    from f1sa.etl.spark import build_spark
    from f1sa.sim.backtest import load_cases
    from f1sa.sim.tyre_model import TyreModelArtefact, TyreModelError
    from f1sa.streaming.stream_job import load_model

    p = argparse.ArgumentParser(prog="experiments/whatif_consistency.py", description=__doc__)
    p.add_argument("--source", choices=("sample", "full"), required=True)
    p.add_argument("--record", action="store_true", help="write results/whatif_consistency.*")
    p.add_argument("--allow-test-season", action="store_true", help="add the 2025 held-out row")
    p.add_argument("--model-dir", type=Path, default=Path("models/undercut/deploy_real_G3.5_k2"))
    p.add_argument("--sim-dir", type=Path, default=Path("models/sim"))
    p.add_argument("--n", type=int, default=N_DEFAULT)
    p.add_argument("--seed", type=int, default=SEED_DEFAULT)
    args = p.parse_args(list(argv) if argv is not None else None)

    out = output_dir(record=args.record, source=args.source)
    why = check_record_args(
        source=args.source, record=args.record, allow_test_season=args.allow_test_season, out=out
    )
    if why:
        print(f"refused: {why}")
        return EXIT_REFUSED
    try:
        artefact = TyreModelArtefact.load(args.sim_dir / f"tyre_model_{args.source}.json")
    except TyreModelError as exc:
        print(f"refused: {exc} (make sim-tyre-model-{args.source})")
        return EXIT_REFUSED
    if args.allow_test_season and not any(s == TEST[0] for s, _ in artefact.races):
        print(
            "refused: the tyre model has no 2025 entries (make sim-tyre-model-full TEST_SEASON=1)"
        )
        return EXIT_REFUSED
    scorer, _params, meta = load_model(args.model_dir)
    g, k = float(meta["g"]), int(meta["k"])
    spark = build_spark("f1sa-whatif-consistency")
    lake = get_settings().data_lake_uri
    dev_seasons = tuple(
        s for s in sorted({s for s, _ in artefact.races}) if s in TRAIN + VALIDATION
    )
    dev = load_cases(spark, lake, dev_seasons, artefact, scorer.probability, g=g, k=k)
    if not dev:
        print("no labelled attempts in the development seasons")
        return EXIT_REFUSED
    last = max(k_ for k_ in artefact.races if k_[0] in dev_seasons)
    rows, curves, grid, scored = evaluate(dev, artefact.races[last], n=args.n, seed=args.seed)
    mean_diff, stop = stop_check(in_seasons(scored, TRAIN + VALIDATION))
    print(
        f"{len(dev)} labelled attempts {dev_seasons[0]}-{dev_seasons[-1]}: "
        f"mean P_sim - P_model = {mean_diff}"
    )
    title = f"What-if consistency ({args.source}; n={args.n}, seed={args.seed}; docs/sim_spec.md)"
    if stop and args.source == "full":
        run = output_dir(record=False, source=args.source)
        write_csv(rows, run / RECORD_CSV)
        write_png(run / RECORD_PNG, curves, grid, scored, [], title=title + " - STOPPED")
        print(
            f"STOP: |mean P_sim - P_model| = {abs(mean_diff or 0):.3f} > {STOP_THRESHOLD}; "
            f"2025 not read; wrote {run}"
        )
        return EXIT_STOP
    held: list[Scored] = []
    if args.allow_test_season:
        cases = load_cases(spark, lake, TEST, artefact, scorer.probability, g=g, k=k)
        held = score(cases, n=args.n, seed=args.seed)
        rows += backtest(held, HELD_OUT[0])
    write_csv(rows, out / RECORD_CSV)
    write_png(out / RECORD_PNG, curves, grid, scored, held, title=title)
    failed = [r for r in rows if r.passed is False]
    print(
        f"wrote {out / RECORD_CSV} and {out / RECORD_PNG}: "
        f"{sum(r.passed is True for r in rows)} checks passed, {len(failed)} failed"
    )
    egap = {r.subset: r.value for r in rows if r.metric == "egap_next_minus_now_s"}
    for r in failed:
        print(f"  FAILED {r.check} {r.subset} {r.metric} value={r.value}")
        if r.subset in egap:
            print(f"    post hoc: E[gap] next - now = {egap[r.subset]:+.3f} s")
    print(f"post_hoc=true rows: {POST_HOC_NOTE}")
    return EXIT_CHECK_FAILED if failed else EXIT_OK
