"""Explanations of the undercut models (#15).

* ``gbt_importances``: ``GBTClassifier.featureImportances``, summed per registry feature (a numeric
  feature's value and ``_missing`` columns, or a categorical feature's one-hot columns).
* ``lr_coefficients``: LogisticRegression coefficients on the standardised features (the pipeline
  scales before fitting), so magnitudes are comparable; the sign is kept, one row per column.
* ``permutation_importance``: on the **validation** season, the drop in PR-AUC when one registry
  feature's columns are shuffled together (same permutation for all its columns), ``repeats``
  times with a seeded generator. All permuted copies are stacked and scored in one Spark job.
  Undefined PR-AUC (one class) gives nan with a note.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import Any

import numpy as np
import pandas as pd
from pyspark.ml.classification import GBTClassificationModel, LogisticRegressionModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.model import features as MF
from f1sa.model import metrics as M
from f1sa.model.dataset import LABEL, WEIGHT
from f1sa.model.train import TrainedModel

PERMUTATION_REPEATS = 5


def _stage(tm: TrainedModel) -> Any:
    return tm.model.stages[-1]


def gbt_importances(tm: TrainedModel, columns: Sequence[str]) -> list[dict[str, Any]]:
    model = _stage(tm)
    if not isinstance(model, GBTClassificationModel):
        raise TypeError("gbt_importances needs a GBT pipeline")
    imp = model.featureImportances.toArray()
    grouped: dict[str, float] = defaultdict(float)
    for col, v in zip(columns, imp, strict=True):
        grouped[MF.base_feature_of(col)] += float(v)
    return [
        {
            "model": "gbt",
            "method": "gbt_importance",
            "feature": f,
            "value": v,
            "std": 0.0,
            "note": "",
        }
        for f, v in sorted(grouped.items(), key=lambda kv: (-kv[1], kv[0]))
    ]


def lr_coefficients(tm: TrainedModel, columns: Sequence[str]) -> list[dict[str, Any]]:
    model = _stage(tm)
    if not isinstance(model, LogisticRegressionModel):
        raise TypeError("lr_coefficients needs an LR pipeline")
    coefs = model.coefficients.toArray()
    rows = [
        {
            "model": "lr",
            "method": "lr_std_coefficient",
            "feature": col.removeprefix("f_"),
            "value": float(c),
            "std": 0.0,
            "note": "coefficient on the standardised column (log-odds per 1 sd)",
        }
        for col, c in zip(columns, coefs, strict=True)
    ]
    return sorted(rows, key=lambda r: (-abs(r["value"]), r["feature"]))


def permutation_importance(
    spark: SparkSession,
    tm: TrainedModel,
    validation: DataFrame,
    columns: Sequence[str],
    *,
    repeats: int = PERMUTATION_REPEATS,
    seed: int = 42,
    transform: Callable[[Any], Any] | None = None,
) -> list[dict[str, Any]]:
    """Mean and sd of the validation PR-AUC drop per registry feature.

    ``transform`` (e.g. ``Calibrator.apply``) maps the raw scores before PR-AUC; the pipeline
    calls this without it (raw scores), so its outputs are unchanged."""
    if repeats < 1:
        raise ValueError("repeats must be >= 1")
    base: Any = validation.select("label", LABEL, WEIGHT, *columns).toPandas()
    y = np.asarray(base["label"], dtype=np.float64)
    reason = M.undefined_reason(y)
    groups: dict[str, list[str]] = defaultdict(list)
    for col in columns:
        groups[MF.base_feature_of(col)].append(col)
    if reason is not None:
        return [
            {
                "model": tm.kind,
                "method": "permutation_pr_auc_drop",
                "feature": f,
                "value": M.NAN,
                "std": M.NAN,
                "note": f"validation PR-AUC undefined ({reason})",
            }
            for f in sorted(groups)
        ]
    rng = np.random.default_rng(seed)
    frames = [base.assign(_perm=-1)]
    perm_ids: dict[int, str] = {}
    pid = 0
    for feature in sorted(groups):
        for _ in range(repeats):
            order = rng.permutation(len(base))
            copy = base.copy()
            for col in groups[feature]:
                copy[col] = base[col].to_numpy()[order]
            frames.append(copy.assign(_perm=pid))
            perm_ids[pid] = feature
            pid += 1
    stacked = pd.concat(frames, ignore_index=True)
    sdf = spark.createDataFrame(stacked)
    scored: Any = (
        tm.model.transform(sdf)
        .select("_perm", "label", vector_to_array(F.col("probability"))[1].alias("score"))
        .toPandas()
    )
    if transform is not None:
        scored["score"] = np.asarray(transform(scored["score"].to_numpy()), dtype=np.float64)
    by = {k: g for k, g in scored.groupby("_perm")}
    ref = M.pr_auc(by[-1]["label"], by[-1]["score"])
    drops: dict[str, list[float]] = defaultdict(list)
    for p, feature in perm_ids.items():
        g = by[p]
        drops[feature].append(ref - M.pr_auc(g["label"], g["score"]))
    rows = [
        {
            "model": tm.kind,
            "method": "permutation_pr_auc_drop",
            "feature": f,
            "value": float(np.mean(v)),
            "std": float(np.std(v, ddof=1)) if len(v) > 1 else 0.0,
            "note": f"validation PR-AUC {ref:.4f}, {repeats} repeats",
        }
        for f, v in drops.items()
    ]
    return sorted(rows, key=lambda r: (-r["value"], r["feature"]))
