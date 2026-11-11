"""Leave-one-season-out folds: never the test season, undefined metrics, fold-only class weights."""

from __future__ import annotations

import math

import pytest
from pyspark.sql import SparkSession

from f1sa.model import loso as L
from f1sa.model.dataset import WEIGHT


def test_folds_cover_2021_to_2024_and_never_2025() -> None:
    folds = L.loso_folds(L.LOSO_SEASONS)
    assert [held for _train, held in folds] == [2021, 2022, 2023, 2024]
    for train, held in folds:
        assert held not in train and len(train) == 3
        assert 2025 not in train and held != 2025


@pytest.mark.parametrize("seasons", [(2021, 2022, 2023, 2024, 2025), (2024, 2025)])
def test_folds_refuse_the_test_season(seasons: tuple[int, ...]) -> None:
    with pytest.raises(L.LosoError, match="never include the test season"):
        L.loso_folds(seasons)


def test_folds_need_two_seasons() -> None:
    with pytest.raises(L.LosoError, match="at least two seasons"):
        L.loso_folds([2023])


def test_undefined_for_counts() -> None:
    assert L.undefined_for_counts(5, 5) == ""
    assert "4 positives" in L.undefined_for_counts(4, 40)
    assert L.undefined_for_counts(0, 0).startswith("undefined")


def test_fold_rows_mark_class_metrics_undefined_not_zero() -> None:
    rows = [
        {"model": "lr", "metric": "f1_positive", "value": 0.0, "note": ""},
        {"model": "lr", "metric": "pr_auc", "value": 0.4, "note": ""},
        {"model": "lr", "metric": "brier", "value": 0.2, "note": ""},
        {"model": "lr_calibrated", "metric": "brier", "value": 0.2, "note": ""},
    ]
    out = L.fold_rows(rows, n_positive=2, n_negative=30)
    by = {(r["model"], r["metric"]): r for r in out}
    assert ("lr_calibrated", "brier") not in by
    assert math.isnan(by[("lr", "f1_positive")]["value"])
    assert (
        math.isnan(by[("lr", "pr_auc")]["value"]) and "2 positives" in by[("lr", "pr_auc")]["note"]
    )
    assert by[("lr", "brier")]["value"] == 0.2
    kept = L.fold_rows(rows, n_positive=10, n_negative=30)
    assert {r["value"] for r in kept if r["metric"] == "f1_positive"} == {0.0}


def test_fold_class_weights_come_from_training_seasons_only(spark: SparkSession) -> None:
    data = [(2021, 1), (2021, 0), (2021, 0), (2021, 0), (2022, 1), (2024, 1), (2024, 1)]
    df = spark.createDataFrame(data, "season int, label int")
    train, test, weights = L._fold_frames(df, (2021, 2022), 2024)
    assert weights == {0: 5 / 6, 1: 5 / 4}  # 2 positives, 3 negatives in 2021-2022 only
    assert {r["season"] for r in train.collect()} == {2021, 2022}
    assert {r[WEIGHT] for r in test.collect()} == {1.0}
    assert {r["season"] for r in test.collect()} == {2024}


def test_summary_lines_print_undefined() -> None:
    rows = [
        {
            "fold": "hold_out_2024",
            "model": "lr",
            "metric": "pr_auc",
            "value": float("nan"),
            "train_seasons": "2021+2022+2023",
            "n_attempts": 3,
            "n_positive": 1,
        },
    ]
    (line,) = L.summary_lines(rows)
    assert "lr.pr_auc=undefined" in line and "positives=1" in line
