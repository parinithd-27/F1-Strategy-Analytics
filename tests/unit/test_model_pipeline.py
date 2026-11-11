"""End to end on the GENERATED rehearsal table (2021-2025), plus the guards and sample smoke mode.

The rehearsal run trains, tunes, calibrates, explains and evaluates the 2025 test season of the
generated data once. A spy on ``Estimator.fit`` records the seasons of every DataFrame any fit
receives (pipelines, their stages, the calibrators), so a fit that reads test rows fails the test.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from _model_fixtures import SIZE, SMALL, frame, rehearsal_rows
from pyspark.ml.base import Estimator
from pyspark.sql import DataFrame, SparkSession

from conftest import SAMPLE_DIR, SampleLake
from f1sa.model import calibrate as C
from f1sa.model import evaluate as E
from f1sa.model import pipeline as P
from f1sa.model.dataset import build_dataset
from f1sa.model.train import predict, tune


@pytest.fixture(scope="module")
def rehearsal_run(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    base = tmp_path_factory.mktemp("rehearsal")
    seen: list[set[int]] = []
    original = Estimator.fit

    def spy(self: Any, dataset: DataFrame, params: Any = None) -> Any:
        if "season" in dataset.columns:
            seen.append({int(r[0]) for r in dataset.select("season").distinct().collect()})
        return original(self, dataset, params)

    mp = pytest.MonkeyPatch()
    mp.setattr(Estimator, "fit", spy)
    cwd = os.getcwd()
    os.chdir(base)
    try:
        cfg = P.RunConfig(source="rehearsal", n_boot=200, train=SMALL, rehearsal_kwargs=SIZE)
        lines: list[str] = []
        result = P.run(spark, cfg, out=lines.append)
    finally:
        os.chdir(cwd)
        mp.undo()
    yield {"result": result, "seen": seen, "base": base, "lines": lines}


# ------------------------------------------------------------------ the test season
def test_no_fit_tuning_or_calibration_reads_the_test_season(rehearsal_run: dict[str, Any]) -> None:
    seen = rehearsal_run["seen"]
    assert len(seen) >= 2 * (
        len(SMALL.lr_grid) + len(SMALL.gbt_grid)
    )  # pipelines + stages + calibrators
    assert all(2025 not in s for s in seen), seen
    assert {2021, 2022, 2023} in seen  # model fits see train only
    assert {2024} in seen  # calibrator fits see validation only
    assert not any({2021, 2024} <= s for s in seen)  # no fit mixes train and validation


def test_test_season_is_opened_exactly_once(rehearsal_run: dict[str, Any]) -> None:
    r = rehearsal_run["result"]
    assert r.dataset.test.opened == 1 and r.test is not None
    assert set(r.test.rows["season"]) == {2025}
    assert r.metadata["test_season_evaluated"] is True


def test_poisoned_test_season_changes_no_fitted_artefact(spark: SparkSession) -> None:
    clean = rehearsal_rows()
    poisoned = [
        {
            **r,
            "label": (1 - r["label"]) if r["label"] is not None else None,
            "outcome_category": {"success": "fail", "fail": "success"}.get(
                r["outcome_category"], r["outcome_category"]
            ),
            "gap_s": r["gap_s"] * 100,
            "pace_delta_last3_s": r["pace_delta_last3_s"] * 100,
            "defender_stint_laps": r["defender_stint_laps"] * 100,
        }
        if r["season"] == 2025
        else r
        for r in clean
    ]

    def artefacts(rows: list[dict[str, Any]]) -> dict[str, Any]:
        ds = build_dataset(frame(spark, rows))
        lr, gbt = tune("lr", ds, SMALL), tune("gbt", ds, SMALL)
        val = predict(lr.model, ds.validation)
        cal, _ = C.choose(spark, val["season"], val["label"], val["score"])
        return {
            "weights": ds.class_weights,
            "params": (lr.params, gbt.params),
            "lr_coef": np.round(lr.model.stages[-1].coefficients.toArray(), 10).tolist(),
            "gbt_imp": np.round(gbt.model.stages[-1].featureImportances.toArray(), 10).tolist(),
            "calibrator": cal.to_json(),
        }

    assert artefacts(clean) == artefacts(poisoned)


# ------------------------------------------------------------------ results
def test_model_beats_majority_on_test_pr_auc(rehearsal_run: dict[str, Any]) -> None:
    r = rehearsal_run["result"]
    point = {
        (row["model"], row["metric"]): row["value"]
        for row in r.eval_rows
        if row["split"] == "test" and row["ci_kind"] == "race_cluster"
    }
    best = f"{r.selected_model}_calibrated"
    assert point[(best, "pr_auc")] > point[("majority", "pr_auc")]
    assert point[(best, "roc_auc")] > 0.6
    assert point[(best, "brier")] < point[(r.selected_model, "brier")] + 0.02


def test_eval_rows_have_both_bootstrap_kinds(rehearsal_run: dict[str, Any]) -> None:
    rows = [row for row in rehearsal_run["result"].eval_rows if row["split"] == "test"]
    kinds = {row["ci_kind"] for row in rows}
    assert kinds == {"race_cluster", "attempt"}
    models = {row["model"] for row in rows}
    assert models == set(E.MODEL_NAMES)
    for row in rows:
        if row["n_valid"]:
            assert row["ci_low"] <= row["value"] + 1e-9 or row["metric"] == "ece"
            assert row["ci_low"] <= row["ci_high"]
    val = [row for row in rehearsal_run["result"].eval_rows if row["split"] == "validation"]
    assert val and all(row["ci_kind"] == "" for row in val)


def test_outputs_are_stamped_and_metadata_complete(rehearsal_run: dict[str, Any]) -> None:
    base, r = rehearsal_run["base"], rehearsal_run["result"]
    for name, rel in r.outputs.items():
        path = base / rel
        assert path.is_file() and path.stat().st_size > 0, name
        if path.suffix == ".csv":
            assert path.read_text().startswith("# REHEARSAL"), name
        if name != "metadata":
            assert "_rehearsal" in path.name, name
    meta = json.loads((base / r.outputs["metadata"]).read_text())
    for key in (
        "features",
        "feature_set",
        "g",
        "k",
        "versions",
        "seed",
        "source",
        "class_weights",
        "calibrators",
        "split",
        "counts",
        "selected_model",
        "assembled_columns",
    ):
        assert key in meta, key
    assert meta["source"] == "rehearsal" and meta["smoke_test"] is True
    assert meta["versions"]["pyspark"] == "4.2.1"
    assert (base / r.outputs["metadata"]).parent.name == "rehearsal_real_G3.5_k2"
    assert (base / "models/undercut/rehearsal_real_G3.5_k2/lr/metadata").is_dir()
    assert any("evaluated once" in line for line in rehearsal_run["lines"])


def test_metric_rows_are_deterministic(rehearsal_run: dict[str, Any]) -> None:
    r = rehearsal_run["result"]
    assert E.metric_rows(r.test, n_boot=50, seed=1) == E.metric_rows(r.test, n_boot=50, seed=1)


# ------------------------------------------------------------------ bootstrap
def test_race_cluster_bootstrap_resamples_whole_races() -> None:
    groups = np.array(["r1"] * 3 + ["r2"] * 2 + ["r3"] * 5)
    rng = np.random.default_rng(0)
    for _ in range(50):
        idx = E.resample_indices(groups, rng, "race_cluster")
        drawn = groups[idx]
        for race in np.unique(drawn):
            count = int((drawn == race).sum())
            size = int((groups == race).sum())
            assert count % size == 0  # every drawn race comes with all its attempts
            members = set(idx[drawn == race])
            assert members == set(np.nonzero(groups == race)[0])
    att = E.resample_indices(groups, np.random.default_rng(0), "attempt")
    assert att.size == groups.size
    with pytest.raises(ValueError):
        E.resample_indices(groups, rng, "nope")


def test_cluster_ci_is_wider_when_attempts_are_correlated() -> None:
    rng = np.random.default_rng(1)
    groups = np.repeat([f"r{i}" for i in range(20)], 15)
    effect = np.repeat(rng.normal(0, 1.5, 20), 15)
    y = (rng.uniform(size=300) < 1 / (1 + np.exp(-effect))).astype(float)
    p = np.full(300, y.mean())
    fns = {"rate": lambda a, b: float(np.mean(a))}
    race = E.bootstrap(y, {"m": p}, groups, kind="race_cluster", n=500, metric_fns=fns)
    att = E.bootstrap(y, {"m": p}, groups, kind="attempt", n=500, metric_fns=fns)
    width = lambda ci: ci[("m", "rate")][1] - ci[("m", "rate")][0]  # noqa: E731
    assert width(race) > 1.5 * width(att)
    empty = E.bootstrap([], {"m": []}, [], kind="attempt", n=5)
    assert all(v[2] == 0 for v in empty.values())


# ------------------------------------------------------------------ guards and sample smoke mode
def test_full_source_needs_the_test_season_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    assert P.main(["--source", "full"]) == 2
    assert "--allow-test-season" in capsys.readouterr().err
    with pytest.raises(P.RunError):
        P.RunConfig(source="full")
    with pytest.raises(P.RunError, match="unknown source"):
        P.RunConfig(source="lake")
    with pytest.raises(P.RunError, match="feature set"):
        P.RunConfig(source="sample", feature_set="with_fastf1")
    assert P.main(["--source", "rehearsal", "--results-dir", "/tmp"]) == 2
    assert P.main(["--source", "rehearsal", "--models-dir", "/tmp"]) == 2


class _NoStop:
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def __getattr__(self, name: str) -> Any:
        return getattr(self._spark, name)

    def stop(self) -> None:
        return None


def test_sample_smoke_mode(
    spark: SparkSession,
    sample_clean_lake: SampleLake,
    sample_features_root: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(sample_clean_lake.base)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{sample_clean_lake.lake}")
    monkeypatch.setenv("SAMPLE_DIR", str(SAMPLE_DIR))
    opened: list[int] = []
    monkeypatch.setattr(E, "evaluate_test", lambda *a, **k: opened.append(1))
    code = P.main(
        ["--source", "sample", "--bootstrap", "10"], spark_factory=lambda _m: _NoStop(spark)
    )  # type: ignore[arg-type,return-value]
    out = capsys.readouterr().out
    assert code == 0 and not opened
    assert P.SKIP_TEST_MESSAGE in out and "skipped: validation PR-AUC undefined" in out
    results = sample_clean_lake.base / "results"
    for name in ("model_eval", "calibration", "feature_importance"):
        text = (results / f"{name}_sample.csv").read_text()
        assert text.startswith("# SMOKE TEST"), name
    for name in ("reliability", "confusion_matrix", "feature_importance"):
        assert (results / f"{name}_sample.png").is_file()
    assert not (results / "model_eval.csv").exists()
    meta = json.loads(
        (sample_clean_lake.base / "models/undercut/sample_real_G3.5_k2/metadata.json").read_text()
    )
    assert meta["test_season_evaluated"] is False and meta["smoke_test"] is True
    assert meta["calibrators"]["lr"]["method"] == "identity"
