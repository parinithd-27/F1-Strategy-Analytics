"""Probability calibration (#18, #39), fitted on validation-season scores only.

* **isotonic**: ``pyspark.ml.regression.IsotonicRegression`` on (raw score, label). Applied as
  Spark does: linear interpolation between boundaries, clamped at the ends.
* **platt**: ``LogisticRegression`` on ``logit(clip(score))`` with the slope constrained to be
  >= 0, so the mapping is monotonic non-decreasing.
* **identity**: used when the validation scores cannot support a fit (fewer than 2 rows or one
  class); the reason is recorded.

``choose`` picks by validation Brier score only. Below ``PLATT_BELOW_N`` labelled validation rows,
Platt is chosen regardless: isotonic regression has one free step per distinct score and overfits
small samples. The test season is never an input here.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import numpy.typing as npt
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Matrices
from pyspark.ml.regression import IsotonicRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

from f1sa.model import metrics as M
from f1sa.model.metrics import FloatArray

PLATT_BELOW_N = 300
EPS = 1e-6
_SCHEMA = StructType(
    [
        StructField("season", IntegerType()),
        StructField("label", DoubleType()),
        StructField("score", DoubleType()),
    ]
)


@dataclass(frozen=True)
class Calibrator:
    method: str
    params: dict[str, Any] = field(default_factory=dict)
    reason: str = ""

    def apply(self, scores: npt.ArrayLike) -> FloatArray:
        s = np.asarray(scores, dtype=np.float64)
        if self.method == "identity":
            return s.copy()
        if self.method == "isotonic":
            xs = np.asarray(self.params["boundaries"], dtype=np.float64)
            ys = np.asarray(self.params["predictions"], dtype=np.float64)
            out: FloatArray = np.interp(s, xs, ys)
            return out
        if self.method == "platt":
            z = float(self.params["a"]) * _logit(s) + float(self.params["b"])
            res: FloatArray = 1.0 / (1.0 + np.exp(-z))
            return res
        raise ValueError(f"unknown calibration method {self.method!r}")

    def to_json(self) -> dict[str, Any]:
        return {"method": self.method, "params": self.params, "reason": self.reason}

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Calibrator:
        return cls(str(data["method"]), dict(data.get("params", {})), str(data.get("reason", "")))


def _logit(s: FloatArray) -> FloatArray:
    c = np.clip(s, EPS, 1.0 - EPS)
    out: FloatArray = np.log(c / (1.0 - c))
    return out


def _frame(spark: SparkSession, seasons: Any, labels: Any, scores: Any) -> Any:
    rows = [(int(s), float(y), float(p)) for s, y, p in zip(seasons, labels, scores, strict=True)]
    return spark.createDataFrame(rows, _SCHEMA)


def fit_isotonic(spark: SparkSession, seasons: Any, labels: Any, scores: Any) -> Calibrator:
    df = _frame(spark, seasons, labels, scores)
    model = IsotonicRegression(featuresCol="score", labelCol="label", isotonic=True).fit(df)
    return Calibrator(
        "isotonic",
        {
            "boundaries": [float(x) for x in model.boundaries.toArray()],
            "predictions": [float(x) for x in model.predictions.toArray()],
        },
    )


def fit_platt(spark: SparkSession, seasons: Any, labels: Any, scores: Any) -> Calibrator:
    df = _frame(spark, seasons, labels, scores).withColumn(
        "logit",
        F.log(
            F.greatest(F.least(F.col("score"), F.lit(1.0 - EPS)), F.lit(EPS))
            / (1.0 - F.greatest(F.least(F.col("score"), F.lit(1.0 - EPS)), F.lit(EPS)))
        ),
    )
    vec = VectorAssembler(inputCols=["logit"], outputCol="features").transform(df)
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        regParam=0.0,
        maxIter=200,
        lowerBoundsOnCoefficients=Matrices.dense(1, 1, [0.0]),
    ).fit(vec)
    return Calibrator("platt", {"a": float(lr.coefficients[0]), "b": float(lr.intercept)})


def choose(
    spark: SparkSession,
    seasons: Any,
    labels: Any,
    scores: Any,
    platt_below_n: int = PLATT_BELOW_N,
) -> tuple[Calibrator, dict[str, float]]:
    """The calibrator to use and the validation Brier score of raw / isotonic / platt."""
    y = np.asarray(labels, dtype=np.float64)
    p = np.asarray(scores, dtype=np.float64)
    raw = M.brier(y, p)
    reason = M.undefined_reason(y) if y.size >= 2 else f"only {y.size} validation row(s)"
    if reason is not None:
        return Calibrator("identity", reason=f"identity: {reason}"), {"raw": raw}
    iso = fit_isotonic(spark, seasons, y, p)
    platt = fit_platt(spark, seasons, y, p)
    briers = {"raw": raw, "isotonic": M.brier(y, iso.apply(p)), "platt": M.brier(y, platt.apply(p))}
    if y.size < platt_below_n:
        why = (
            f"platt: validation has {y.size} labelled attempts (< {platt_below_n}); isotonic has "
            "one free step per distinct score and overfits small samples"
        )
        return Calibrator(platt.method, platt.params, why), briers
    best = iso if briers["isotonic"] < briers["platt"] else platt
    other = "platt" if best is iso else "isotonic"
    why = (
        f"{best.method}: lower validation Brier ({briers[best.method]:.4f} vs "
        f"{briers[other]:.4f} for {other})"
    )
    if math.isclose(briers["isotonic"], briers["platt"]):
        best, why = platt, "platt: validation Brier tie; the smoother mapping is preferred"
    return Calibrator(best.method, best.params, why), briers
