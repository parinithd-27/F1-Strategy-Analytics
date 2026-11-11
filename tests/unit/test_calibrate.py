"""Calibration: isotonic and Platt are monotonic, the chooser rule, identity fallback, JSON."""

from __future__ import annotations

import numpy as np
import pytest
from pyspark.ml.regression import IsotonicRegression
from pyspark.sql import SparkSession

from f1sa.model import calibrate as C
from f1sa.model import metrics as M


def _data(n: int = 400, seed: int = 0) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    raw = rng.uniform(0, 1, n)
    y = (rng.uniform(0, 1, n) < raw**2).astype(float)  # raw scores are over-confident
    return np.full(n, 2024), y, raw


@pytest.mark.parametrize("method", ["isotonic", "platt"])
def test_calibration_is_monotonic(spark: SparkSession, method: str) -> None:
    seasons, y, raw = _data()
    cal = (C.fit_isotonic if method == "isotonic" else C.fit_platt)(spark, seasons, y, raw)
    grid = np.linspace(0, 1, 501)
    out = cal.apply(grid)
    assert np.all(np.diff(out) >= -1e-12) and out.min() >= 0 and out.max() <= 1
    assert M.brier(y, cal.apply(raw)) < M.brier(y, raw)


def test_platt_slope_is_never_negative(spark: SparkSession) -> None:
    seasons, y, raw = _data()
    cal = C.fit_platt(spark, seasons, 1 - y, raw)  # anti-correlated scores
    assert cal.params["a"] >= 0.0
    assert np.all(np.diff(cal.apply(np.linspace(0, 1, 101))) >= -1e-12)


def test_isotonic_apply_matches_spark(spark: SparkSession) -> None:
    seasons, y, raw = _data(200, seed=4)
    cal = C.fit_isotonic(spark, seasons, y, raw)
    df = spark.createDataFrame(
        [(float(s), float(t)) for s, t in zip(raw, y, strict=True)], "score double, label double"
    )
    model = IsotonicRegression(featuresCol="score", labelCol="label").fit(df)
    test = spark.createDataFrame([(float(x),) for x in np.linspace(-0.1, 1.1, 37)], "score double")
    spark_pred = [r["prediction"] for r in model.transform(test).collect()]
    assert np.allclose(cal.apply(np.linspace(-0.1, 1.1, 37)), spark_pred)


def test_choose_rules(spark: SparkSession) -> None:
    seasons, y, raw = _data()
    small, briers = C.choose(spark, seasons, y, raw)  # 400 >= 300: by Brier
    assert set(briers) == {"raw", "isotonic", "platt"}
    expected = "isotonic" if briers["isotonic"] < briers["platt"] else "platt"
    assert small.method == expected and "lower validation Brier" in small.reason
    few, _ = C.choose(spark, seasons[:100], y[:100], raw[:100])
    assert few.method == "platt" and "< 300" in few.reason
    forced, _ = C.choose(spark, seasons, y, raw, platt_below_n=10)
    assert forced.method == expected


def test_identity_when_validation_cannot_support_a_fit(spark: SparkSession) -> None:
    one, briers = C.choose(spark, [2024], [1.0], [0.7])
    assert one.method == "identity" and "1 validation row" in one.reason
    assert briers == {"raw": pytest.approx(0.09)}
    same, _ = C.choose(spark, [2024] * 3, [0.0, 0.0, 0.0], [0.1, 0.5, 0.2])
    assert same.method == "identity" and "one class" in same.reason
    assert np.array_equal(one.apply([0.2, 0.9]), [0.2, 0.9])


def test_json_round_trip_and_unknown_method(spark: SparkSession) -> None:
    seasons, y, raw = _data(100)
    for cal in (C.fit_platt(spark, seasons, y, raw), C.fit_isotonic(spark, seasons, y, raw)):
        back = C.Calibrator.from_json(cal.to_json())
        assert np.allclose(back.apply(raw), cal.apply(raw))
    with pytest.raises(ValueError, match="unknown calibration"):
        C.Calibrator("magic").apply([0.5])
