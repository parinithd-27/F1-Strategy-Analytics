"""The pure-Python calibrated-LR scorer equals the Spark pipeline (within 1e-9) and needs no Spark.

The LR is fitted on the GENERATED rehearsal table (train 2021-2023) and Platt-calibrated on its
2024 rows (enough rows for a real Platt fit, and null priors with ``*_source = 'none'``).
Missing values, unseen categories and constant (std 0) columns are compared with Spark's own
``dataset.feature_exprs`` + ``transform``.
"""

from __future__ import annotations

import json
import math
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from _model_fixtures import SIZE, SMALL, frame, rehearsal_rows
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from f1sa.model import deploy as D
from f1sa.model import features as MF
from f1sa.model import pipeline as P
from f1sa.model.calibrate import Calibrator, fit_platt
from f1sa.model.dataset import KEY, build_dataset, feature_exprs
from f1sa.model.lr_scorer import SCORER_FORMAT, InputSpec, LRScorer, ScorerError
from f1sa.model.train import predict, tune

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def fitted(spark: SparkSession) -> Iterator[dict[str, Any]]:
    df = frame(spark, rehearsal_rows())
    ds = build_dataset(df, split=P.DEV_SPLIT)
    lr = tune("lr", ds, SMALL)
    pred = predict(lr.model, ds.validation)
    cal = fit_platt(spark, pred["season"], pred["label"], pred["score"])
    doc = D.scorer_document(lr.model, ds.columns, cal)
    yield {"df": df, "ds": ds, "lr": lr, "pred": pred, "cal": cal, "doc": doc}


def _spark_side(spark: SparkSession, fitted: dict[str, Any], rows: list[dict[str, Any]]) -> Any:
    """Spark's assembled vector and P(success) for raw ``rows`` (same schema as the frame)."""
    schema = fitted["df"].schema
    sdf = spark.createDataFrame([tuple(r.get(f.name) for f in schema.fields) for r in rows], schema)
    assembled = sdf.select(*KEY, *feature_exprs("real"))
    out = (
        fitted["lr"]
        .model.transform(assembled)
        .select(
            vector_to_array("features_raw").alias("x"),
            vector_to_array("probability")[1].alias("p"),
        )
    )
    return out.collect()


def test_scorer_equals_spark_on_every_validation_row(fitted: dict[str, Any]) -> None:
    scorer = LRScorer.from_json(fitted["doc"])
    assert scorer.calibration_method == "platt"
    raw = D.raw_validation_rows(fitted["df"], D.DeployConfig())
    pred = fitted["pred"]
    assert len(raw) == len(pred) > 30
    spark_cal = fitted["cal"].apply(np.asarray(pred["score"]))
    diffs, raw_diffs = [], []
    for i, key in enumerate(pred[list(KEY)].itertuples(index=False, name=None)):
        row = raw[tuple(key)]
        raw_diffs.append(abs(scorer.raw_probability(row) - float(pred["score"][i])))
        diffs.append(abs(scorer.probability(row) - float(spark_cal[i])))
    assert max(raw_diffs) <= 1e-9 and max(diffs) <= 1e-9


def test_assembled_vector_equals_spark_exactly(spark: SparkSession, fitted: dict[str, Any]) -> None:
    scorer = LRScorer.from_json(fitted["doc"])
    rows = [r.asDict() for r in fitted["df"].where(F.col("season") == 2024).collect()]
    priors = ("pit_loss_prior_s", "attacker_expected_outlap_pace_residual_s")
    assert any(r[c] is None for r in rows for c in priors)  # the generator nulls some priors
    for r, s in zip(rows, _spark_side(spark, fitted, rows), strict=True):
        assert scorer.assemble(r) == list(s["x"])
        assert abs(scorer.raw_probability(r) - s["p"]) <= 1e-9


def test_missing_values_and_unseen_categories_match_spark(
    spark: SparkSession, fitted: dict[str, Any]
) -> None:
    scorer = LRScorer.from_json(fitted["doc"])
    base = fitted["df"].where(F.col("season") == 2024).first()
    assert base is not None
    numeric = [s.name for s in MF.specs_for("real") if s.kind == "numeric"]
    rows = [
        {**base.asDict(), **dict.fromkeys(numeric)},  # every numeric input missing
        {**base.asDict(), "gap_s": None, "pit_loss_source": None},
        {**base.asDict(), "pit_loss_source": "unseen_level", "attacker_stint_laps": None},
        {**base.asDict(), "attacker_expected_outlap_pace_source": None, "laps_remaining": 0},
    ]
    cols = list(fitted["doc"]["assembled_columns"])
    for r, s in zip(rows, _spark_side(spark, fitted, rows), strict=True):
        vec = scorer.assemble(r)
        assert vec == list(s["x"])
        assert abs(scorer.raw_probability(r) - s["p"]) <= 1e-9
    first = scorer.assemble(rows[0])
    for name in numeric:
        assert first[cols.index(f"f_{name}")] == 0.0
        assert first[cols.index(f"f_{name}_missing")] == 1.0
    third = scorer.assemble(rows[2])
    assert third[cols.index("f_pit_loss_source__unknown")] == 1.0
    assert sum(third[cols.index(f"f_pit_loss_source__{c}")] for c in ("circuit_prior", "none")) == 0


def test_constant_columns_score_zero_like_spark(fitted: dict[str, Any]) -> None:
    doc = fitted["doc"]
    assert 0.0 in doc["scaler_std"]  # e.g. a source level never seen in training
    scorer = LRScorer.from_json(doc)
    i = doc["scaler_std"].index(0.0)
    raw = D.raw_validation_rows(fitted["df"], D.DeployConfig())
    row = next(iter(raw.values()))
    vec = scorer.assemble(row)
    bumped = [*vec[:i], vec[i] + 1000.0, *vec[i + 1 :]]
    assert scorer.margin(bumped) == scorer.margin(vec)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), True])
def test_non_finite_or_boolean_numeric_raises(fitted: dict[str, Any], bad: Any) -> None:
    scorer = LRScorer.from_json(fitted["doc"])
    with pytest.raises(ScorerError):
        scorer.assemble({"gap_s": bad})


def test_absent_keys_are_missing(fitted: dict[str, Any]) -> None:
    scorer = LRScorer.from_json(fitted["doc"])
    vec = scorer.assemble({})
    assert len(vec) == len(fitted["doc"]["assembled_columns"])
    assert 0.0 < scorer.probability({}) < 1.0


def test_load_round_trip_and_calibration(tmp_path: Path, fitted: dict[str, Any]) -> None:
    path = tmp_path / "scorer.json"
    path.write_text(json.dumps(fitted["doc"]))
    scorer = LRScorer.load(path)
    assert scorer == LRScorer.from_json(fitted["doc"])
    cal: Calibrator = fitted["cal"]
    for p in (0.0, 1e-9, 0.2, 0.5, 0.93, 1.0):
        assert math.isclose(scorer.calibrate(p), float(cal.apply([p])[0]), abs_tol=1e-12)
    ident = LRScorer.from_json({**fitted["doc"], "calibrator": {"method": "identity"}})
    assert ident.calibrate(0.37) == 0.37


@pytest.mark.parametrize(
    "change",
    [
        {"format": "other"},
        {"coefficients": [0.0]},
        {"scaler_mean": []},
        {"calibrator": {"method": "isotonic", "params": {}}},
        {"calibrator": {"method": "platt", "params": {"a": 1.0}}},
        {"eps": 0.0},
        {"inputs": [{"name": "x", "kind": "numeric"}]},
    ],
)
def test_inconsistent_documents_are_rejected(
    fitted: dict[str, Any], change: dict[str, Any]
) -> None:
    with pytest.raises(ScorerError):
        LRScorer.from_json({**fitted["doc"], **change})


def test_input_spec_widths() -> None:
    assert InputSpec("a", "numeric").width == 2
    assert InputSpec("b", "categorical", ("x", "y")).width == 3
    assert SCORER_FORMAT.startswith("f1sa-lr-scorer")


def test_scorer_document_refuses_a_gbt_pipeline(
    spark: SparkSession, fitted: dict[str, Any]
) -> None:
    ds = fitted["ds"]
    gbt = tune("gbt", build_dataset(frame(spark, rehearsal_rows(**SIZE)), split=P.DEV_SPLIT), SMALL)
    with pytest.raises(D.DeployError):
        D.scorer_document(gbt.model, ds.columns, fitted["cal"])


def test_scorer_imports_no_spark() -> None:
    code = (
        "import sys, f1sa.model.lr_scorer; "
        "bad = sorted(m for m in sys.modules if m.split('.')[0] in ('pyspark', 'py4j', 'numpy')); "
        "print(bad); sys.exit(1 if bad else 0)"
    )
    res = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, cwd=REPO_ROOT, timeout=60
    )
    assert res.returncode == 0, res.stdout + res.stderr
