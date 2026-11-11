"""Classification metrics in numpy, shared by tuning, evaluation, the bootstrap and explanations.

* ``pr_auc``: average precision, ``sum_n (R_n - R_{n-1}) P_n`` over distinct score thresholds
  (the step-wise area; it does not interpolate like Spark's ``areaUnderPR``).
* ``roc_auc``: the Mann-Whitney statistic with average ranks for ties (equals Spark's
  ``areaUnderROC``).
* ``brier``: mean squared error of the probability.
* ``ece``: expected calibration error over ``n_bins`` equal-width bins on [0, 1]
  (``sum_b |B|/n * |mean(p in B) - mean(y in B)|``; the last bin includes 1.0).
* ``f1_positive`` / ``macro_f1`` at a probability threshold (default 0.5).

A metric that is undefined for the data (for example a ranking metric when only one class is
present, or no rows at all) is ``nan``; ``undefined_reason`` says why.
"""

from __future__ import annotations

import math

import numpy as np
import numpy.typing as npt

FloatArray = npt.NDArray[np.float64]
METRICS = ("f1_positive", "macro_f1", "pr_auc", "roc_auc", "brier", "ece")
NAN = float("nan")


def _arrays(y: npt.ArrayLike, p: npt.ArrayLike) -> tuple[FloatArray, FloatArray]:
    ya = np.asarray(y, dtype=np.float64).ravel()
    pa = np.asarray(p, dtype=np.float64).ravel()
    if ya.shape != pa.shape:
        raise ValueError(f"labels and scores differ in length: {ya.shape} vs {pa.shape}")
    if ya.size and not np.isin(ya, (0.0, 1.0)).all():
        raise ValueError("labels must be 0 or 1")
    if not np.isfinite(pa).all():
        raise ValueError("scores must be finite")
    return ya, pa


def undefined_reason(y: npt.ArrayLike) -> str | None:
    """Why ranking metrics are undefined for labels ``y``, or None."""
    ya = np.asarray(y, dtype=np.float64).ravel()
    if ya.size == 0:
        return "no rows"
    if ya.min() == ya.max():
        return f"only one class present (label={int(ya[0])})"
    return None


def pr_auc(y: npt.ArrayLike, p: npt.ArrayLike) -> float:
    ya, pa = _arrays(y, p)
    if undefined_reason(ya):
        return NAN
    order = np.argsort(-pa, kind="mergesort")
    ys, ps = ya[order], pa[order]
    tp = np.cumsum(ys)
    fp = np.cumsum(1.0 - ys)
    last = np.r_[np.nonzero(np.diff(ps))[0], ys.size - 1]  # last index of each distinct score
    tp, fp = tp[last], fp[last]
    precision = tp / (tp + fp)
    recall = tp / ya.sum()
    prev = np.r_[0.0, recall[:-1]]
    return float(np.sum((recall - prev) * precision))


def roc_auc(y: npt.ArrayLike, p: npt.ArrayLike) -> float:
    ya, pa = _arrays(y, p)
    if undefined_reason(ya):
        return NAN
    order = np.argsort(pa, kind="mergesort")
    ranks = np.empty(pa.size, dtype=np.float64)
    sorted_p = pa[order]
    i = 0
    while i < pa.size:  # average ranks over ties
        j = i
        while j + 1 < pa.size and sorted_p[j + 1] == sorted_p[i]:
            j += 1
        ranks[order[i : j + 1]] = (i + j) / 2.0 + 1.0
        i = j + 1
    n_pos = ya.sum()
    n_neg = ya.size - n_pos
    return float((ranks[ya == 1.0].sum() - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg))


def brier(y: npt.ArrayLike, p: npt.ArrayLike) -> float:
    ya, pa = _arrays(y, p)
    return float(np.mean((pa - ya) ** 2)) if ya.size else NAN


def calibration_bins(
    y: npt.ArrayLike, p: npt.ArrayLike, n_bins: int = 10
) -> list[tuple[float, float, float, int]]:
    """Per equal-width bin: (centre, mean predicted, observed rate, count); empty bins omitted."""
    if n_bins < 1:
        raise ValueError("n_bins must be >= 1")
    ya, pa = _arrays(y, p)
    idx = np.minimum((np.clip(pa, 0.0, 1.0) * n_bins).astype(int), n_bins - 1)
    out = []
    for b in range(n_bins):
        m = idx == b
        if m.any():
            out.append(((b + 0.5) / n_bins, float(pa[m].mean()), float(ya[m].mean()), int(m.sum())))
    return out


def ece(y: npt.ArrayLike, p: npt.ArrayLike, n_bins: int = 10) -> float:
    ya, _ = _arrays(y, p)
    if not ya.size:
        return NAN
    bins = calibration_bins(y, p, n_bins)
    return float(sum(n / ya.size * abs(mp - obs) for _, mp, obs, n in bins))


def confusion(
    y: npt.ArrayLike, p: npt.ArrayLike, threshold: float = 0.5
) -> tuple[int, int, int, int]:
    """(tn, fp, fn, tp) at ``p >= threshold``."""
    ya, pa = _arrays(y, p)
    pred = pa >= threshold
    pos = ya == 1.0
    return (
        int(np.sum(~pred & ~pos)),
        int(np.sum(pred & ~pos)),
        int(np.sum(~pred & pos)),
        int(np.sum(pred & pos)),
    )


def _f1(tp: int, fp: int, fn: int) -> float:
    denom = 2 * tp + fp + fn
    return 2 * tp / denom if denom else NAN


def f1_positive(y: npt.ArrayLike, p: npt.ArrayLike, threshold: float = 0.5) -> float:
    _, fp, fn, tp = confusion(y, p, threshold)
    return _f1(tp, fp, fn)


def macro_f1(y: npt.ArrayLike, p: npt.ArrayLike, threshold: float = 0.5) -> float:
    tn, fp, fn, tp = confusion(y, p, threshold)
    pos, neg = _f1(tp, fp, fn), _f1(tn, fn, fp)
    vals = [v for v in (pos, neg) if not math.isnan(v)]
    return float(np.mean(vals)) if vals else NAN


def report(y: npt.ArrayLike, p: npt.ArrayLike, threshold: float = 0.5) -> dict[str, float]:
    """Every metric in ``METRICS``."""
    return {
        "f1_positive": f1_positive(y, p, threshold),
        "macro_f1": macro_f1(y, p, threshold),
        "pr_auc": pr_auc(y, p),
        "roc_auc": roc_auc(y, p),
        "brier": brier(y, p),
        "ece": ece(y, p),
    }
