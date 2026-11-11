"""MLlib undercut-success models (#14): LogisticRegression and GBTClassifier.

* Pipelines: ``VectorAssembler -> StandardScaler -> LogisticRegression`` and
  ``VectorAssembler -> GBTClassifier``, both with ``weightCol`` (training-only class weights).
* Tuning: a small grid; every candidate is fitted on the **training seasons** and scored on the
  **validation season** by PR-AUC (``metrics.pr_auc``). There is no CrossValidator across time.
  The chosen candidate is the model fitted on train only; validation stays out-of-sample for
  calibration. When validation has fewer than two rows or one class, PR-AUC is undefined: the
  first grid point is used and ``tuning_status`` says so.
* ``check_feature_policy``: in ``full`` mode a feature set whose provenance can be synthetic
  (``model.features.SYNTHETIC_FLAGS``) is refused unless every row's flag is false.
"""

from __future__ import annotations

import json
import math
import platform
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pyspark
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from f1sa.model import features as MF
from f1sa.model import metrics as M
from f1sa.model.dataset import KEY, LABEL, RACE_ID, WEIGHT, ModelDataset

LR_GRID: tuple[dict[str, float], ...] = tuple(
    {"regParam": r, "elasticNetParam": e} for r in (0.0, 0.01, 0.1) for e in (0.0, 0.5)
)
GBT_GRID: tuple[dict[str, float], ...] = tuple(
    {"maxDepth": d, "maxIter": i, "stepSize": s}
    for d in (2, 3, 5)
    for i in (20, 50)
    for s in (0.05, 0.1)
)
SEED = 42
KINDS = ("lr", "gbt")


class TrainingError(RuntimeError):
    """The models cannot be trained (for example one class in the training split)."""


class SyntheticFeatureError(RuntimeError):
    """A full-mode run selected a feature whose values are synthetic."""


@dataclass(frozen=True)
class TrainConfig:
    lr_grid: tuple[dict[str, float], ...] = LR_GRID
    gbt_grid: tuple[dict[str, float], ...] = GBT_GRID
    seed: int = SEED

    def grid(self, kind: str) -> tuple[dict[str, float], ...]:
        if kind not in KINDS:
            raise ValueError(f"unknown model kind {kind!r}")
        g = self.lr_grid if kind == "lr" else self.gbt_grid
        if not g:
            raise ValueError(f"empty {kind} grid")
        return g


@dataclass
class TrainedModel:
    kind: str
    model: PipelineModel
    params: dict[str, float]
    grid_scores: list[dict[str, Any]] = field(default_factory=list)
    tuning_status: str = "tuned on validation PR-AUC"
    validation_pr_auc: float = float("nan")


def check_feature_policy(df: DataFrame, feature_set: str, source: str) -> None:
    """Refuse synthetic-valued features in ``full`` mode (static provenance + row flags)."""
    if feature_set not in MF.FEATURE_SETS:
        raise ValueError(f"unknown feature set {feature_set!r}")
    if source != "full":
        return
    flags = MF.required_flags(feature_set)
    missing = [f for f in flags if f not in df.columns]
    if missing:
        raise SyntheticFeatureError(
            f"feature set {feature_set!r} needs provenance flags {missing} to prove its values "
            "are not synthetic, and they are absent"
        )
    bad = [f for f in flags if not df.where(F.coalesce(F.col(f), F.lit(True))).isEmpty()]
    if bad:
        cols = [
            s.name
            for s in MF.specs_for(feature_set)
            if any(f in MF.SYNTHETIC_FLAGS.get(s.provenance, ()) for f in bad)
        ]
        raise SyntheticFeatureError(
            f"--source full refuses synthetic features: rows flagged {bad} feed {cols}. "
            "Use feature set 'real' until FastF1 data replaces the synthetic values."
        )


def make_pipeline(
    kind: str, columns: Sequence[str], params: Mapping[str, float], seed: int = SEED
) -> Pipeline:
    assembler = VectorAssembler(
        inputCols=list(columns), outputCol="features_raw", handleInvalid="error"
    )
    if kind == "lr":
        scaler = StandardScaler(
            inputCol="features_raw", outputCol="features", withMean=True, withStd=True
        )
        lr = LogisticRegression(
            featuresCol="features",
            labelCol=LABEL,
            weightCol=WEIGHT,
            maxIter=200,
            regParam=float(params["regParam"]),
            elasticNetParam=float(params["elasticNetParam"]),
        )
        return Pipeline(stages=[assembler, scaler, lr])
    if kind == "gbt":
        gbt = GBTClassifier(
            featuresCol="features_raw",
            labelCol=LABEL,
            weightCol=WEIGHT,
            seed=seed,
            maxDepth=int(params["maxDepth"]),
            maxIter=int(params["maxIter"]),
            stepSize=float(params["stepSize"]),
        )
        return Pipeline(stages=[assembler, gbt])
    raise ValueError(f"unknown model kind {kind!r}")


def predict(model: PipelineModel, df: DataFrame) -> Any:
    """pandas frame: KEY, race_id, label and ``score`` = P(success), sorted by KEY."""
    out = model.transform(df).select(
        *KEY,
        RACE_ID,
        "label",
        vector_to_array(F.col("probability"))[1].alias("score"),
    )
    pdf: Any = out.toPandas()
    return pdf.sort_values(list(KEY)).reset_index(drop=True)


def _classes(df: DataFrame) -> set[int]:
    return {int(r[0]) for r in df.select("label").distinct().collect()}


def tune(kind: str, ds: ModelDataset, config: TrainConfig | None = None) -> TrainedModel:
    """Fit every grid point on train, pick the best validation PR-AUC (ties: the first)."""
    cfg = config or TrainConfig()
    grid = cfg.grid(kind)
    if ds.counts["train"]["n"] < 2 or len(_classes(ds.train)) < 2:
        raise TrainingError(
            f"training seasons {ds.split.train} need both classes and >= 2 rows; "
            f"got {ds.counts['train']}"
        )
    val = ds.validation
    reason = M.undefined_reason(
        np.asarray([r[0] for r in val.select("label").collect()], dtype=np.float64)
    )
    if reason is not None:
        model = make_pipeline(kind, ds.columns, grid[0], cfg.seed).fit(ds.train)
        return TrainedModel(
            kind,
            model,
            dict(grid[0]),
            [{"params": dict(grid[0]), "validation_pr_auc": None}],
            tuning_status=f"skipped: validation PR-AUC undefined ({reason}); first grid point used",
        )
    best: TrainedModel | None = None
    scores: list[dict[str, Any]] = []
    for params in grid:
        model = make_pipeline(kind, ds.columns, params, cfg.seed).fit(ds.train)
        pred = predict(model, val)
        ap = M.pr_auc(pred["label"], pred["score"])
        scores.append({"params": dict(params), "validation_pr_auc": ap})
        if best is None or (not math.isnan(ap) and ap > best.validation_pr_auc):
            best = TrainedModel(kind, model, dict(params), validation_pr_auc=ap)
    assert best is not None
    best.grid_scores = scores
    return best


def _git_commit() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return out.stdout.strip() or None


def versions() -> dict[str, str | None]:
    return {
        "pyspark": pyspark.__version__,
        "python": platform.python_version(),
        "numpy": np.__version__,
        "git_commit": _git_commit(),
    }


def check_models_dir(models_dir: Path) -> None:
    """Models may only be written under ``./models`` (relative to the working directory)."""
    allowed = (Path.cwd() / "models").resolve()
    target = models_dir.resolve()
    if target != allowed and allowed not in target.parents:
        raise TrainingError(f"models dir {models_dir} is outside {allowed}")


def save_models(
    models: Mapping[str, TrainedModel], models_dir: Path, metadata: Mapping[str, Any]
) -> Path:
    """Save each PipelineModel and ``metadata.json`` under ``models_dir``; returns the JSON path."""
    check_models_dir(models_dir)
    models_dir.mkdir(parents=True, exist_ok=True)
    for kind, tm in models.items():
        tm.model.write().overwrite().save(str((models_dir / kind).resolve()))
    path = models_dir / "metadata.json"
    path.write_text(json.dumps(metadata, indent=2, sort_keys=True, default=str) + "\n")
    return path
