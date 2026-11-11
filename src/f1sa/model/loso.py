"""Leave-one-season-out stability check for the development run (never includes the test season).

Each fold trains on three of the seasons 2021-2024 and evaluates on the fourth. Because tuning on
the held-out season would leak, every fold uses the **first grid point** of each model
(``TrainConfig.grid(kind)[0]``) instead of the tuned parameters, and the scores are raw (no
calibration: the only calibration season would be the held-out one). Class weights come from the
fold's training rows only. The majority and rule baselines are fitted on the fold's training rows.

The folds are not chronological (2021 is evaluated with a model trained on later seasons), so the
results describe stability across seasons, not deployment performance.

A held-out season with fewer than ``MIN_CLASS_COUNT`` positives or negatives reports the
classification and ranking metrics (``CLASS_METRICS``) as undefined (NaN plus a note) rather
than as a number such as 0; Brier and ECE stay defined whenever the season has rows.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.model import metrics as M
from f1sa.model.baselines import MajorityBaseline, RuleBaseline
from f1sa.model.calibrate import Calibrator
from f1sa.model.dataset import WEIGHT, ModelDataset, SplitSpec, class_weights
from f1sa.model.evaluate import metric_rows, score_split
from f1sa.model.train import KINDS, TrainConfig, TrainedModel, make_pipeline

LOSO_SEASONS: tuple[int, ...] = (2021, 2022, 2023, 2024)
MIN_CLASS_COUNT = 5
CLASS_METRICS = ("f1_positive", "macro_f1", "pr_auc", "roc_auc")
LOSO_MODELS = ("lr", "gbt", "majority", "rule")
LOSO_COLUMNS = (
    "fold",
    "train_seasons",
    "held_out_season",
    "model",
    "metric",
    "value",
    "n_attempts",
    "n_positive",
    "n_races",
    "params",
    "note",
)


class LosoError(RuntimeError):
    """The folds cannot be built (a test season in the input, or fewer than two seasons)."""


def loso_folds(
    seasons: Iterable[int], test_seasons: Sequence[int] = SplitSpec().test
) -> list[tuple[tuple[int, ...], int]]:
    """``[(training seasons, held-out season), ...]``; refuses any test season."""
    wanted = sorted(set(int(s) for s in seasons))
    leaked = sorted(set(wanted) & set(test_seasons))
    if leaked:
        raise LosoError(f"leave-one-season-out must never include the test season(s) {leaked}")
    if len(wanted) < 2:
        raise LosoError(f"leave-one-season-out needs at least two seasons, got {wanted}")
    return [(tuple(s for s in wanted if s != held), held) for held in wanted]


def undefined_for_counts(n_positive: int, n_negative: int, minimum: int = MIN_CLASS_COUNT) -> str:
    """Why the class metrics of a held-out season are undefined, or '' when they are defined."""
    if n_positive < minimum or n_negative < minimum:
        return (
            f"undefined: held-out season has {n_positive} positives and {n_negative} negatives "
            f"(fewer than {minimum} of a class)"
        )
    return ""


def fold_rows(
    scored_rows: Iterable[dict[str, Any]], n_positive: int, n_negative: int
) -> list[dict[str, Any]]:
    """``metric_rows`` output with the class metrics set to NaN when the season has too few of a
    class, and the calibrated duplicates dropped (LOSO scores are raw)."""
    reason = undefined_for_counts(n_positive, n_negative)
    out = []
    for r in scored_rows:
        if r["model"] not in LOSO_MODELS:
            continue
        row = dict(r)
        if reason and row["metric"] in CLASS_METRICS:
            row["value"] = M.NAN
            row["note"] = reason
        out.append(row)
    return out


def _fold_frames(
    rows: DataFrame, train_seasons: Sequence[int], held: int
) -> tuple[DataFrame, DataFrame, dict[int, float]]:
    train = rows.where(F.col("season").isin(*train_seasons))
    weights = class_weights(train)
    w = F.when(F.col("label") == 1, F.lit(weights[1])).otherwise(F.lit(weights[0]))
    train = train.withColumn(WEIGHT, w)
    test = rows.where(F.col("season") == held).withColumn(WEIGHT, F.lit(1.0))
    return train, test, weights


def run_loso(
    spark: SparkSession,
    ds: ModelDataset,
    config: TrainConfig | None = None,
    seasons: Sequence[int] = LOSO_SEASONS,
) -> list[dict[str, Any]]:
    """Every fold's metric rows (see the module docstring). ``ds`` is the development dataset;
    its sealed test split is never opened (only ``ds.train`` and ``ds.validation`` are used)."""
    cfg = config or TrainConfig()
    folds = loso_folds(seasons, ds.split.test or SplitSpec().test)
    rows_all = ds.train.drop(WEIGHT).unionByName(ds.validation.drop(WEIGHT)).cache()
    out: list[dict[str, Any]] = []
    for train_seasons, held in folds:
        train, test, _weights = _fold_frames(rows_all, train_seasons, held)
        train_labels = [int(r[0]) for r in train.select("label").collect()]
        test_labels = [int(r[0]) for r in test.select("label").collect()]
        n_pos, n_neg = sum(test_labels), len(test_labels) - sum(test_labels)
        base = {
            "fold": f"hold_out_{held}",
            "train_seasons": "+".join(str(s) for s in train_seasons),
            "held_out_season": held,
            "n_positive": n_pos,
        }
        if len(set(train_labels)) < 2:
            note = f"undefined: training seasons {train_seasons} have only one class"
            out += [
                {
                    **base,
                    "model": model,
                    "metric": metric,
                    "value": M.NAN,
                    "n_attempts": len(test_labels),
                    "n_races": 0,
                    "params": "",
                    "note": note,
                }
                for model in LOSO_MODELS
                for metric in M.METRICS
            ]
            continue
        models = {}
        for kind in KINDS:
            params = dict(cfg.grid(kind)[0])
            fitted = make_pipeline(kind, ds.columns, params, cfg.seed).fit(train)
            models[kind] = TrainedModel(
                kind, fitted, params, tuning_status="fixed first grid point"
            )
        majority = MajorityBaseline.fit(train_labels)
        rule = RuleBaseline(fallback_rate=majority.positive_rate)
        identity = {k: Calibrator("identity", reason="LOSO: raw scores") for k in KINDS}
        scored = score_split(f"loso_{held}", test, models, identity, majority, rule)
        for r in fold_rows(metric_rows(scored), n_pos, n_neg):
            params_text = str(models[r["model"]].params) if r["model"] in models else ""
            out.append({**base, **r, "params": params_text})
    rows_all.unpersist()
    return out


def summary_lines(
    rows: Sequence[dict[str, Any]], metrics: Sequence[str] = ("pr_auc", "brier")
) -> list[str]:
    """Human-readable per-fold lines for the given metrics (NaN printed as 'undefined')."""
    lines = []
    for fold in sorted({r["fold"] for r in rows}):
        parts = []
        for model in LOSO_MODELS:
            for metric in metrics:
                vals = [
                    r["value"]
                    for r in rows
                    if (r["fold"], r["model"], r["metric"]) == (fold, model, metric)
                ]
                if vals:
                    v = float(vals[0])
                    parts.append(f"{model}.{metric}={'undefined' if math.isnan(v) else f'{v:.3f}'}")
        sample = next(r for r in rows if r["fold"] == fold)
        lines.append(
            f"  {fold} (train {sample['train_seasons']}, n={sample['n_attempts']}, "
            f"positives={sample['n_positive']}): " + " ".join(parts)
        )
    return lines
