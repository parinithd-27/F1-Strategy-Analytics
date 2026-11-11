"""Scoring and evaluation (#17, #29): every model and baseline on a split, with bootstrap CIs.

* ``score_split`` scores one split with LR, GBT (raw and calibrated) and both baselines.
* ``evaluate_test`` is the **only** function that opens the sealed test split, and it can do so
  once per dataset. Calling it is the single test-season evaluation of a run.
* Bootstrap CIs (percentile, default 95%, ``BOOTSTRAP_N`` resamples, fixed seed), paired across
  models:

  * ``race_cluster``: resample whole races ((season, round)) with replacement and keep every
    attempt of each drawn race. Attempts in one race share conditions (circuit, weather, safety
    cars), so they are not independent; this is the headline interval.
  * ``attempt``: resample attempts independently, for comparison.

  A resample on which a metric is undefined (for example one class) is skipped for that metric;
  ``n_valid`` reports how many resamples were used.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np
import numpy.typing as npt
from pyspark.sql import DataFrame

from f1sa.model import features as MF
from f1sa.model import metrics as M
from f1sa.model.baselines import MajorityBaseline, RuleBaseline
from f1sa.model.calibrate import Calibrator
from f1sa.model.dataset import KEY, RACE_ID, ModelDataset, _issue_test_token
from f1sa.model.train import TrainedModel, predict

BOOTSTRAP_N = 1000
CI_LEVEL = 0.95
BOOTSTRAP_KINDS = ("race_cluster", "attempt")
MODEL_NAMES = ("lr", "lr_calibrated", "gbt", "gbt_calibrated", "majority", "rule")
IntArray = npt.NDArray[np.int64]


@dataclass
class ScoredSplit:
    split: str
    rows: Any  # pandas: KEY, race_id, label, baseline columns
    scores: dict[str, M.FloatArray]

    @property
    def labels(self) -> M.FloatArray:
        return np.asarray(self.rows["label"], dtype=np.float64)

    @property
    def groups(self) -> npt.NDArray[np.str_]:
        return np.asarray(self.rows[RACE_ID], dtype=str)


def score_split(
    split: str,
    df: DataFrame,
    models: Mapping[str, TrainedModel],
    calibrators: Mapping[str, Calibrator],
    majority: MajorityBaseline,
    rule: RuleBaseline,
) -> ScoredSplit:
    pdf: Any = df.select(*KEY, RACE_ID, "label", *MF.BASELINE_COLUMNS).toPandas()
    rows = pdf.sort_values(list(KEY)).reset_index(drop=True)
    scores: dict[str, M.FloatArray] = {}
    for kind, tm in models.items():
        pred = predict(tm.model, df)
        if not (pred[list(KEY)].to_numpy() == rows[list(KEY)].to_numpy()).all():
            raise RuntimeError(f"{kind}: predictions are not aligned with the split rows")
        raw = np.asarray(pred["score"], dtype=np.float64)
        scores[kind] = raw
        scores[f"{kind}_calibrated"] = calibrators[kind].apply(raw)
    scores["majority"] = majority.predict_proba(len(rows))
    scores["rule"] = rule.predict_proba(rows)
    return ScoredSplit(split, rows, scores)


def resample_indices(groups: npt.ArrayLike, rng: np.random.Generator, kind: str) -> IntArray:
    """Row indices of one bootstrap resample (``race_cluster`` draws whole groups)."""
    g = np.asarray(groups)
    n = g.size
    if kind == "attempt":
        out: IntArray = rng.integers(0, n, n).astype(np.int64)
        return out
    if kind == "race_cluster":
        uniq, inverse = np.unique(g, return_inverse=True)
        members = [np.nonzero(inverse == i)[0] for i in range(uniq.size)]
        draw = rng.integers(0, uniq.size, uniq.size)
        res: IntArray = np.concatenate([members[d] for d in draw]).astype(np.int64)
        return res
    raise ValueError(f"unknown bootstrap kind {kind!r}")


def bootstrap(
    y: npt.ArrayLike,
    scores: Mapping[str, npt.ArrayLike],
    groups: npt.ArrayLike,
    *,
    kind: str,
    n: int = BOOTSTRAP_N,
    seed: int = 42,
    level: float = CI_LEVEL,
    metric_fns: Mapping[str, Callable[[Any, Any], float]] | None = None,
) -> dict[tuple[str, str], tuple[float, float, int]]:
    """{(model, metric): (low, high, n_valid)}; the same resamples for every model (paired)."""
    if n < 1:
        raise ValueError("n must be >= 1")
    names = tuple(metric_fns) if metric_fns is not None else M.METRICS
    ya = np.asarray(y, dtype=np.float64)
    if ya.size == 0:
        return {(m, k): (M.NAN, M.NAN, 0) for m in scores for k in names}
    rng = np.random.default_rng(seed)
    samples: dict[tuple[str, str], list[float]] = {(m, k): [] for m in scores for k in names}
    arrays = {m: np.asarray(p, dtype=np.float64) for m, p in scores.items()}
    for _ in range(n):
        idx = resample_indices(groups, rng, kind)
        yb = ya[idx]
        for m, p in arrays.items():
            pb = p[idx]
            if metric_fns is None:
                rep = M.report(yb, pb)
                for k in names:
                    samples[(m, k)].append(rep[k])
            else:
                for k, fn in metric_fns.items():
                    samples[(m, k)].append(fn(yb, pb))
    lo_q, hi_q = (1 - level) / 2, 1 - (1 - level) / 2
    out = {}
    for key, vals in samples.items():
        v = np.asarray(vals, dtype=np.float64)
        v = v[np.isfinite(v)]
        if v.size:
            out[key] = (float(np.quantile(v, lo_q)), float(np.quantile(v, hi_q)), int(v.size))
        else:
            out[key] = (M.NAN, M.NAN, 0)
    return out


def metric_rows(
    scored: ScoredSplit,
    *,
    n_boot: int = 0,
    seed: int = 42,
) -> list[dict[str, Any]]:
    """One row per (model, metric[, bootstrap kind]) with the point value and optional CI."""
    y, groups = scored.labels, scored.groups
    reason = M.undefined_reason(y) or ""
    n_races = int(np.unique(groups).size) if groups.size else 0
    cis = {
        kind: bootstrap(y, scored.scores, groups, kind=kind, n=n_boot, seed=seed)
        for kind in (BOOTSTRAP_KINDS if n_boot else ())
    }
    rows = []
    for model in MODEL_NAMES:
        if model not in scored.scores:
            continue
        point = M.report(y, scored.scores[model]) if y.size else dict.fromkeys(M.METRICS, M.NAN)
        for metric in M.METRICS:
            base = {
                "model": model,
                "split": scored.split,
                "metric": metric,
                "value": point[metric],
                "n_attempts": int(y.size),
                "n_races": n_races,
                "note": reason if metric in ("pr_auc", "roc_auc") and reason else "",
            }
            if not cis:
                rows.append(
                    {**base, "ci_kind": "", "ci_low": M.NAN, "ci_high": M.NAN, "n_valid": 0}
                )
            for kind, ci in cis.items():
                lo, hi, nv = ci[(model, metric)]
                rows.append({**base, "ci_kind": kind, "ci_low": lo, "ci_high": hi, "n_valid": nv})
    return rows


SUPPLEMENTARY_ECE_BINS = 5
ECE_BIAS_NOTE = (
    "bootstrap ECE intervals are biased upward (resampling duplicates rows into bins), so the "
    "interval can lie above the point value"
)


def supplementary_ece_rows(
    scored: ScoredSplit, *, n_boot: int, seed: int = 42, n_bins: int = SUPPLEMENTARY_ECE_BINS
) -> list[dict[str, Any]]:
    """``ece_5bin`` rows (supplementary; the pre-specified primary ECE keeps 10 bins), with the
    same paired resamples as ``metric_rows`` (same seed and kinds)."""
    y, groups = scored.labels, scored.groups
    name = f"ece_{n_bins}bin"
    fns = {name: lambda yy, pp: M.ece(yy, pp, n_bins=n_bins)}
    n_races = int(np.unique(groups).size) if groups.size else 0
    cis = {
        kind: bootstrap(y, scored.scores, groups, kind=kind, n=n_boot, seed=seed, metric_fns=fns)
        for kind in BOOTSTRAP_KINDS
    }
    note = f"supplementary ({n_bins} bins; primary ECE uses 10); {ECE_BIAS_NOTE}"
    rows = []
    for model in MODEL_NAMES:
        if model not in scored.scores:
            continue
        value = M.ece(y, scored.scores[model], n_bins=n_bins) if y.size else M.NAN
        for kind, ci in cis.items():
            lo, hi, nv = ci[(model, name)]
            rows.append(
                {
                    "model": model,
                    "split": scored.split,
                    "metric": name,
                    "value": value,
                    "ci_kind": kind,
                    "ci_low": lo,
                    "ci_high": hi,
                    "n_valid": nv,
                    "n_attempts": int(y.size),
                    "n_races": n_races,
                    "note": note,
                }
            )
    return rows


def evaluate_test(
    ds: ModelDataset,
    models: Mapping[str, TrainedModel],
    calibrators: Mapping[str, Calibrator],
    majority: MajorityBaseline,
    rule: RuleBaseline,
    *,
    n_boot: int = BOOTSTRAP_N,
    seed: int = 42,
) -> tuple[ScoredSplit, list[dict[str, Any]]]:
    """The single test-season evaluation: opens the sealed split, scores it, bootstraps CIs."""
    test_df = ds.test.unseal(_issue_test_token())
    scored = score_split("test", test_df, models, calibrators, majority, rule)
    return scored, metric_rows(scored, n_boot=n_boot, seed=seed)
