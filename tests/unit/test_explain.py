"""Explanations on a model fitted to the GENERATED table (known signal: gap_s matters)."""

from __future__ import annotations

from typing import Any

import pytest
from _model_fixtures import SMALL, frame, rehearsal_rows
from pyspark.sql import SparkSession

from f1sa.model import explain as X
from f1sa.model import features as MF
from f1sa.model.dataset import build_dataset
from f1sa.model.train import tune


@pytest.fixture(scope="module")
def fitted(spark: SparkSession) -> dict[str, Any]:
    ds = build_dataset(frame(spark, rehearsal_rows(races_per_season=12)))
    return {"ds": ds, "lr": tune("lr", ds, SMALL), "gbt": tune("gbt", ds, SMALL)}


def test_gbt_importances_are_grouped_and_sum_to_one(fitted: dict[str, Any]) -> None:
    rows = X.gbt_importances(fitted["gbt"], fitted["ds"].columns)
    assert {r["feature"] for r in rows} == {s.name for s in MF.specs_for("real")}
    assert sum(r["value"] for r in rows) == pytest.approx(1.0)
    assert rows[0]["value"] >= rows[-1]["value"]
    with pytest.raises(TypeError):
        X.gbt_importances(fitted["lr"], fitted["ds"].columns)


def test_lr_coefficients_are_standardised_and_signed(fitted: dict[str, Any]) -> None:
    rows = X.lr_coefficients(fitted["lr"], fitted["ds"].columns)
    assert len(rows) == len(fitted["ds"].columns)
    coef = {r["feature"]: r["value"] for r in rows}
    assert coef["gap_s"] < 0  # a bigger gap lowers the chance (the generator's -1.1 * gap)
    assert coef["pace_delta_last3_s"] < 0
    with pytest.raises(TypeError):
        X.lr_coefficients(fitted["gbt"], fitted["ds"].columns)


def test_permutation_importance_finds_the_signal(
    spark: SparkSession, fitted: dict[str, Any]
) -> None:
    rows = X.permutation_importance(
        spark, fitted["lr"], fitted["ds"].validation, fitted["ds"].columns
    )
    by = {r["feature"]: r for r in rows}
    assert set(by) == {s.name for s in MF.specs_for("real")}
    assert by["gap_s"]["value"] > 0.02  # shuffling the main driver of the label hurts PR-AUC
    assert (
        by["gap_s"]["value"] > by["rejoin_traffic_cars"]["value"]
    )  # noise feature in the generator
    assert all(r["std"] >= 0 for r in rows) and "5 repeats" in rows[0]["note"]
    again = X.permutation_importance(
        spark, fitted["lr"], fitted["ds"].validation, fitted["ds"].columns
    )
    assert again == rows  # seeded
    with pytest.raises(ValueError):
        X.permutation_importance(
            spark, fitted["lr"], fitted["ds"].validation, fitted["ds"].columns, repeats=0
        )


def test_permutation_importance_undefined_on_one_class(
    spark: SparkSession, fitted: dict[str, Any]
) -> None:
    one_class = fitted["ds"].validation.where("label = 1")
    rows = X.permutation_importance(spark, fitted["lr"], one_class, fitted["ds"].columns)
    assert all(r["value"] != r["value"] for r in rows)  # nan
    assert "undefined" in rows[0]["note"]
