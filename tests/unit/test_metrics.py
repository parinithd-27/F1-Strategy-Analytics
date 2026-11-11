"""Numpy metrics: hand-computed cases, ties, undefined cases, Spark agreement; baselines."""

from __future__ import annotations

import math

import numpy as np
import pytest
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession

from f1sa.model import metrics as M
from f1sa.model.baselines import MajorityBaseline, RuleBaseline


def test_pr_auc_hand_computed_and_ties() -> None:
    # sorted by score: 1, 0, 1, 0 -> precision at the hits 1/1 and 2/3
    assert M.pr_auc([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.1]) == pytest.approx(0.5 * 1 + 0.5 * 2 / 3)
    assert M.pr_auc([1, 1, 0], [0.9, 0.8, 0.1]) == 1.0
    # a tie between a positive and a negative counts as one threshold
    assert M.pr_auc([1, 0], [0.5, 0.5]) == pytest.approx(0.5)


def test_roc_auc_hand_computed_and_ties() -> None:
    assert M.roc_auc([0, 0, 1, 1], [0.1, 0.4, 0.35, 0.8]) == pytest.approx(0.75)
    assert M.roc_auc([0, 1], [0.5, 0.5]) == pytest.approx(0.5)


def test_undefined_metrics_are_nan_with_reason() -> None:
    assert math.isnan(M.pr_auc([1, 1], [0.2, 0.9])) and math.isnan(M.roc_auc([0, 0], [0.1, 0.2]))
    assert M.undefined_reason([1, 1]) == "only one class present (label=1)"
    assert M.undefined_reason([]) == "no rows" and M.undefined_reason([0, 1]) is None
    assert math.isnan(M.brier([], [])) and math.isnan(M.ece([], []))
    rep = M.report([], [])
    assert all(math.isnan(v) for v in rep.values())


def test_brier_ece_f1_and_confusion() -> None:
    y, p = [1, 0, 1, 0], [0.8, 0.2, 0.4, 0.6]
    assert M.brier(y, p) == pytest.approx((0.04 + 0.04 + 0.36 + 0.36) / 4)
    assert M.confusion(y, p) == (1, 1, 1, 1)
    assert M.f1_positive(y, p) == pytest.approx(0.5) and M.macro_f1(y, p) == pytest.approx(0.5)
    # ECE: bins 0.2 (y=0), 0.4 (y=1), 0.6 (y=0), 0.8 (y=1): |0.2-0|+|0.4-1|+|0.6-0|+|0.8-1| over 4
    assert M.ece(y, p) == pytest.approx((0.2 + 0.6 + 0.6 + 0.2) / 4)
    assert M.ece([1, 1], [1.0, 1.0]) == 0.0  # 1.0 falls in the last bin
    assert math.isnan(M.f1_positive([0, 0], [0.1, 0.2]))  # no positives, none predicted
    assert M.macro_f1([0, 0], [0.1, 0.2]) == 1.0


def test_calibration_bins_and_validation() -> None:
    bins = M.calibration_bins([0, 1, 1], [0.05, 0.55, 0.58])
    assert bins == [(0.05, 0.05, 0.0, 1), (0.55, pytest.approx(0.565), 1.0, 2)]
    with pytest.raises(ValueError, match="0 or 1"):
        M.brier([2], [0.5])
    with pytest.raises(ValueError, match="differ in length"):
        M.brier([1, 0], [0.5])
    with pytest.raises(ValueError, match="finite"):
        M.brier([1], [float("nan")])
    with pytest.raises(ValueError, match="n_bins"):
        M.calibration_bins([1], [0.5], n_bins=0)


def test_roc_auc_matches_spark(spark: SparkSession) -> None:
    rng = np.random.default_rng(3)
    y = rng.integers(0, 2, 200).astype(float)
    p = np.round(np.clip(y * 0.3 + rng.normal(0.35, 0.2, 200), 0, 1), 2)  # rounding makes ties
    df = spark.createDataFrame(
        [(float(a), float(b)) for a, b in zip(y, p, strict=True)], "label double, s double"
    )
    spark_auc = BinaryClassificationEvaluator(
        rawPredictionCol="s", metricName="areaUnderROC"
    ).evaluate(df)
    assert M.roc_auc(y, p) == pytest.approx(spark_auc, abs=1e-9)


# ------------------------------------------------------------------ baselines
def test_majority_baseline() -> None:
    b = MajorityBaseline.fit([0, 0, 1, 0])
    assert b.positive_rate == 0.25 and b.majority == 0
    assert (b.predict_proba(3) == 0.25).all()
    assert MajorityBaseline.fit([1, 1, 0]).majority == 1
    with pytest.raises(ValueError, match="empty"):
        MajorityBaseline.fit([])


def test_rule_baseline_gain_and_fallback() -> None:
    rows = {
        "gap_s": [1.0, 3.0, 1.0],
        "defender_deg_slope_s_per_lap": [0.1, 0.1, float("nan")],
        "defender_stint_laps": [20, 20, 20],
        "attacker_expected_outlap_pace_residual_s": [0.5, float("nan"), 0.0],
    }
    rule = RuleBaseline(fallback_rate=0.3)
    gain = rule.gain_s(rows)
    assert gain[0] == pytest.approx(1.5) and gain[1] == pytest.approx(2.0)  # residual nan -> 0
    p = rule.predict_proba(rows)
    assert p[0] > 0.5 > p[1]  # gap 1.0 < gain 1.5; gap 3.0 > gain 2.0
    assert p[2] == 0.3  # missing slope -> fallback
    assert rule.fallback_mask(rows).tolist() == [False, False, True]
    negative = RuleBaseline(0.3).gain_s(
        {**rows, "defender_deg_slope_s_per_lap": [-0.2, -0.2, -0.2]}
    )
    assert gain.shape == negative.shape and negative[1] == 0.0  # negative slope gives no gain
    with pytest.raises(ValueError):
        RuleBaseline(0.3, k_expected=0)
