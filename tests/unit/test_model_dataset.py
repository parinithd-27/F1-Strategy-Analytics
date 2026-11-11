"""Model dataset: (G, k) filter, labelled rows, split, class weights, sealed test split,
feature registry and the full-mode refusal of synthetic features."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from _model_fixtures import frame, rehearsal_rows
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import f1sa.model as model_pkg
from f1sa.model import dataset as D
from f1sa.model import features as MF
from f1sa.model.train import SyntheticFeatureError, check_feature_policy


@pytest.fixture(scope="module")
def rows() -> list[dict]:
    return rehearsal_rows()


def test_setting_filter_leaves_unique_attempt_keys(spark: SparkSession, rows: list[dict]) -> None:
    df = frame(spark, rows)
    assert df.groupBy(*D.KEY).count().where("count > 1").count() > 0  # G=3.5 and 5.0 share keys
    sel = D.select_setting(df, 3.5, 2)
    assert sel.groupBy(*D.KEY).count().where("count > 1").count() == 0
    assert {r[0] for r in sel.select("gap_threshold_s").distinct().collect()} == {3.5}


def test_unknown_setting_and_injected_duplicate_raise(
    spark: SparkSession, rows: list[dict]
) -> None:
    df = frame(spark, rows)
    with pytest.raises(D.DatasetError, match=r"no undercut attempts for G=2.5, k=2; available"):
        D.select_setting(df, 2.5, 2)
    dup = frame(spark, [*rows, dict(rows[0])])
    with pytest.raises(D.DatasetError, match="duplicate attempt keys"):
        D.build_dataset(dup)


def test_labelled_only_and_split_counts(spark: SparkSession, rows: list[dict]) -> None:
    ds = D.build_dataset(frame(spark, rows))
    labelled = [r for r in rows if r["gap_threshold_s"] == 3.5 and r["label"] is not None]
    n_train = sum(r["season"] <= 2023 for r in labelled)
    n_val = sum(r["season"] == 2024 for r in labelled)
    assert ds.counts["train"]["n"] == n_train and ds.counts["validation"]["n"] == n_val
    assert {r[0] for r in ds.train.select("season").distinct().collect()} == {2021, 2022, 2023}
    assert {r[0] for r in ds.validation.select("season").distinct().collect()} == {2024}
    cats = {r[0] for r in ds.train.select("label").distinct().collect()}
    assert cats == {0, 1}
    assert ds.test.seasons == (2025,) and ds.test.opened == 0


def test_class_weights_use_training_rows_only(spark: SparkSession, rows: list[dict]) -> None:
    base = D.build_dataset(frame(spark, rows))
    train = [
        r
        for r in rows
        if r["gap_threshold_s"] == 3.5 and r["label"] is not None and r["season"] <= 2023
    ]
    n, pos = len(train), sum(r["label"] for r in train)
    assert base.class_weights == pytest.approx({0: n / (2 * (n - pos)), 1: n / (2 * pos)})
    flipped = [
        {**r, "label": 1 - r["label"], "outcome_category": "fail" if r["label"] else "success"}
        if r["season"] >= 2024 and r["label"] is not None
        else r
        for r in rows
    ]
    assert D.build_dataset(frame(spark, flipped)).class_weights == base.class_weights
    w = {
        (r["label"], r["class_weight"])
        for r in base.train.select("label", "class_weight").collect()
    }
    assert w == {(0, base.class_weights[0]), (1, base.class_weights[1])}
    assert {r[0] for r in base.validation.select("class_weight").distinct().collect()} == {1.0}


def test_missing_values_use_constant_fill_and_indicators(
    spark: SparkSession, rows: list[dict]
) -> None:
    ds = D.build_dataset(frame(spark, rows))
    r = ds.train.where(F.col("pit_loss_prior_s").isNull()).first()
    assert r is not None
    assert r["f_pit_loss_prior_s"] == 0.0 and r["f_pit_loss_prior_s_missing"] == 1.0
    assert r["f_pit_loss_source__none"] == 1.0 and r["f_pit_loss_source__unknown"] == 0.0
    ok = ds.train.where(F.col("pit_loss_prior_s").isNotNull()).first()
    assert ok is not None and ok["f_pit_loss_prior_s_missing"] == 0.0
    assert ok["f_pit_loss_prior_s"] > 10.0  # the real value (about 21 s), not a fill
    assert tuple(c for c in ds.train.columns if c.startswith("f_")) == ds.columns


def test_split_spec_validation() -> None:
    with pytest.raises(D.DatasetError, match="overlap"):
        D.SplitSpec(train=(2021, 2024), validation=(2024,))
    with pytest.raises(D.DatasetError, match="chronological"):
        D.SplitSpec(train=(2024,), validation=(2023,), test=(2025,))
    with pytest.raises(D.DatasetError, match="empty"):
        D.SplitSpec(train=(), validation=(2024,))


def test_sealed_split_needs_the_token_and_opens_once(spark: SparkSession, rows: list[dict]) -> None:
    ds = D.build_dataset(frame(spark, rows))
    with pytest.raises(PermissionError):
        ds.test.unseal(object())  # type: ignore[arg-type]
    with pytest.raises(PermissionError):
        D._TestToken(object())
    df = ds.test.unseal(D._issue_test_token())
    assert {r[0] for r in df.select("season").distinct().collect()} == {2025}
    with pytest.raises(PermissionError, match="already opened"):
        ds.test.unseal(D._issue_test_token())


def test_only_evaluate_issues_test_tokens() -> None:
    root = Path(inspect.getfile(model_pkg)).parent
    users = sorted(p.name for p in root.glob("*.py") if "_issue_test_token(" in p.read_text())
    assert users == ["dataset.py", "evaluate.py"]  # defined in dataset, called only in evaluate


# ------------------------------------------------------------------ feature registry
def test_registry_covers_decision_features_without_leaks() -> None:
    MF.check_registry()
    for fs in MF.FEATURE_SETS:
        cols = MF.assembled_columns(fs)
        assert len(cols) == len(set(cols))
        assert not any(c.removeprefix("f_").startswith(("oracle_", "outcome_")) for c in cols)
        assert all(MF.base_feature_of(c) for c in cols)
    real = {s.provenance for s in MF.specs_for("real")}
    assert real == {MF.JOLPICA, MF.STINT_BOUNDARY}
    assert "attacker_compound" not in {s.name for s in MF.specs_for("real")}
    assert {"attacker_compound", "attacker_mean_rpm"} <= {
        s.name for s in MF.specs_for("with_synthetic")
    }
    with pytest.raises(ValueError, match="unknown feature set"):
        MF.specs_for("with_fastf1")
    with pytest.raises(ValueError, match="provenance"):
        MF.FeatureSpec("x", "nope")


# ------------------------------------------------------------------ full-mode refusal
def test_full_mode_refuses_synthetic_features(spark: SparkSession, rows: list[dict]) -> None:
    df = frame(spark, rows)
    with pytest.raises(SyntheticFeatureError, match="compound_is_synthetic"):
        check_feature_policy(df, "with_synthetic", "full")
    check_feature_policy(df, "with_synthetic", "sample")  # allowed in smoke mode
    check_feature_policy(df, "real", "full")  # nothing synthetic selected
    real_flags = frame(spark, rehearsal_rows(synthetic_flags=False))
    check_feature_policy(real_flags, "with_synthetic", "full")  # flags prove real values
    with pytest.raises(SyntheticFeatureError, match="absent"):
        check_feature_policy(df.drop("compound_is_synthetic"), "with_synthetic", "full")
    one_bad = real_flags.withColumn(
        "attacker_telemetry_is_synthetic",
        F.when(F.col("round") == 1, F.lit(None)).otherwise(F.lit(False)),
    )
    with pytest.raises(SyntheticFeatureError, match="attacker_telemetry_is_synthetic"):
        check_feature_policy(one_bad, "with_synthetic", "full")  # null counts as not proven


def test_load_attempts_on_the_sample_lake(spark: SparkSession, sample_features_root: str) -> None:
    df = D.load_attempts(spark, sample_features_root)
    assert df.count() == 303
    row = df.where(
        F.col("is_default_params")
        & (F.col("attacker_id") == "hamilton")
        & (F.col("season") == 2023)
    ).first()
    assert (
        row is not None and row["attacker_stint_laps"] == row["decision_lap"]
    )  # first stint from lap 1
    assert row["attacker_telemetry_is_synthetic"] is True and row["attacker_mean_rpm"] is not None
