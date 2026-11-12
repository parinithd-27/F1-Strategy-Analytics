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
from pyspark.sql import DataFrame, DataFrameReader, SparkSession

from conftest import SAMPLE_DIR, SampleLake
from f1sa.model import calibrate as C
from f1sa.model import evaluate as E
from f1sa.model import metrics as M
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


# ------------------------------------------------------------------ development mode (--no-test)
@pytest.fixture(scope="module")
def dev_run(
    spark: SparkSession, sample_features_root: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    """``--source full --no-test`` on a copy of the sample feature lake whose 2025 partitions
    are unreadable junk: any read, listing for schema or fit of 2025 fails or is recorded."""
    import shutil

    base = tmp_path_factory.mktemp("dev")
    lake = base / "data" / "lake"
    shutil.copytree(Path(sample_features_root.removeprefix("file://")), lake / "features")
    for table in ("undercut_attempts", "stints", "telemetry_lap_features"):
        junk = lake / "features" / table / "season=2025" / "round=1"
        junk.mkdir(parents=True)
        (junk / "part-00000-junk.parquet").write_bytes(b"not parquet: the test season")
    paths: list[str] = []
    fits: list[set[int]] = []
    opened: list[int] = []
    original_parquet, original_fit = DataFrameReader.parquet, Estimator.fit

    def parquet_spy(self: Any, *p: str, **kw: Any) -> Any:
        paths.extend(str(x) for x in p)
        return original_parquet(self, *p, **kw)

    def fit_spy(self: Any, dataset: DataFrame, params: Any = None) -> Any:
        if "season" in dataset.columns:
            fits.append({int(r[0]) for r in dataset.select("season").distinct().collect()})
        return original_fit(self, dataset, params)

    mp = pytest.MonkeyPatch()
    mp.setattr(DataFrameReader, "parquet", parquet_spy)
    mp.setattr(Estimator, "fit", fit_spy)
    mp.setattr(E, "evaluate_test", lambda *a, **k: opened.append(1))
    mp.setattr(P, "evaluate_test", lambda *a, **k: opened.append(1))
    mp.setenv("DATA_LAKE_URI", f"file://{lake}")
    cwd = os.getcwd()
    os.chdir(base)
    lines: list[str] = []
    try:
        from f1sa.config import get_settings

        get_settings.cache_clear()
        cfg = P.RunConfig(source="full", no_test=True, n_boot=20, train=SMALL)
        result = P.run(spark, cfg, out=lines.append)
    finally:
        os.chdir(cwd)
        mp.undo()
    yield {
        "result": result,
        "paths": paths,
        "fits": fits,
        "opened": opened,
        "base": base,
        "lines": lines,
        "cfg": cfg,
    }


def test_no_test_never_reads_lists_or_fits_the_test_season(dev_run: dict[str, Any]) -> None:
    assert dev_run["paths"], "the loader spy recorded nothing"
    assert not [p for p in dev_run["paths"] if "2025" in p]
    assert all("/season=" in p for p in dev_run["paths"])  # explicit partition paths only
    assert dev_run["fits"] and all(2025 not in s for s in dev_run["fits"])
    assert dev_run["opened"] == []
    result = dev_run["result"]
    assert result.test is None and result.dataset.test.opened == 0
    assert result.dataset.split.test == ()
    assert P.DEV_SKIP_TEST_MESSAGE in dev_run["lines"]


def test_no_test_outputs_are_stamped_and_named_dev(dev_run: dict[str, Any]) -> None:
    results = dev_run["base"] / "results"
    csvs = ["eval", "calibration", "feature_importance", "class_balance", "loso"]
    for name in csvs:
        text = (results / f"model_dev_{name}.csv").read_text()
        assert text.startswith("# DEVELOPMENT"), name
        assert "noisy" in text.splitlines()[0]
    for name in ("reliability", "confusion_matrix", "feature_importance"):
        assert (results / f"model_dev_{name}.png").is_file()
    meta = json.loads((results / "model_dev_metadata.json").read_text())
    assert meta["stamp"].startswith("DEVELOPMENT") and meta["test_season_evaluated"] is False
    assert meta["split"]["test"] == [] and meta["feature_set"] == "real"
    assert not (dev_run["base"] / "models").exists()  # no model binaries in development mode
    assert not (results / "model_eval.csv").exists()
    assert dev_run["result"].dataset.feature_set == "real"


def test_no_test_validation_rows_have_both_bootstraps_and_baselines(
    dev_run: dict[str, Any],
) -> None:
    rows = dev_run["result"].eval_rows
    assert {r["split"] for r in rows} == {"validation"}
    assert {r["ci_kind"] for r in rows} == set(E.BOOTSTRAP_KINDS)
    assert {"majority", "rule", "lr_calibrated", "gbt_calibrated"} <= {r["model"] for r in rows}
    assert {"f1_positive", "macro_f1", "pr_auc", "roc_auc", "brier", "ece", "ece_5bin"} == {
        r["metric"] for r in rows
    }
    five = [r for r in rows if r["metric"] == "ece_5bin"]
    assert {r["ci_kind"] for r in five} == set(E.BOOTSTRAP_KINDS)
    assert all("supplementary" in r["note"] and "biased upward" in r["note"] for r in five)
    assert all("primary 10-bin" in r["note"] for r in rows if r["metric"] == "ece")
    rule_notes = {r["note"] for r in rows if r["model"] == "rule" and r["metric"] == "brier"}
    assert all("fall back to the training positive rate" in n for n in rule_notes)
    assert any(
        line.startswith("rule baseline: decision-time rule probability")
        for line in dev_run["lines"]
    )
    text = (dev_run["base"] / "results" / "model_dev_calibration.csv").read_text()
    assert "ece_5bin_raw,ece_5bin_calibrated" in text.splitlines()[1]


def test_supplementary_ece_uses_five_bins_and_the_same_resamples() -> None:
    import pandas as pd

    y = np.array([0, 1, 0, 1, 1, 0, 0, 1, 0, 0], dtype=float)
    p = np.linspace(0.05, 0.95, 10)
    rows = pd.DataFrame({"label": y, "race_id": [f"r{i // 2}" for i in range(10)]})
    scored = E.ScoredSplit("validation", rows, {"lr": p})
    out = E.supplementary_ece_rows(scored, n_boot=30, seed=1)
    assert {r["ci_kind"] for r in out} == set(E.BOOTSTRAP_KINDS)
    assert all(r["metric"] == "ece_5bin" for r in out)
    assert out[0]["value"] == pytest.approx(M.ece(y, p, n_bins=5))
    assert out[0]["value"] != pytest.approx(M.ece(y, p))  # differs from the 10-bin ECE here


def test_rule_fallback_note_and_annotation() -> None:
    import pandas as pd

    from f1sa.model.baselines import RuleBaseline

    rows = pd.DataFrame(
        {
            "label": [1.0, 0.0],
            "race_id": ["a", "b"],
            "gap_s": [1.0, 2.0],
            "defender_deg_slope_s_per_lap": [0.1, float("nan")],
            "defender_stint_laps": [20, 20],
            "attacker_expected_outlap_pace_residual_s": [0.0, 0.0],
        }
    )
    scored = E.ScoredSplit("validation", rows, {})
    rule = RuleBaseline(fallback_rate=0.25)
    note = P.rule_fallback_note(scored, rule)
    assert "on 1 of 2 rows; 1 fall back to the training positive rate 0.2500" in note
    metric_rows = [
        {"model": "rule", "metric": "brier", "value": 0.2, "note": ""},
        {"model": "lr", "metric": "ece", "value": 0.1, "note": "x"},
        {"model": "lr", "metric": "brier", "value": 0.3, "note": ""},
    ]
    out = P.annotate_rows(metric_rows, scored, rule)
    assert [r["value"] for r in out] == [0.2, 0.1, 0.3]
    assert out[0]["note"] == note and out[1]["note"].startswith("x; primary 10-bin ECE")
    assert out[2]["note"] == ""


def test_no_test_reports_class_balance_and_loso(dev_run: dict[str, Any]) -> None:
    lines = dev_run["lines"]
    assert any(line.startswith("class balance 2023 (train)") for line in lines)
    assert any(line.startswith("class balance 2024 (validation)") for line in lines)
    text = (dev_run["base"] / "results" / "model_dev_loso.csv").read_text()
    assert "hold_out_2025" not in text and "2025" not in text.split("\n", 1)[1]
    assert "hold_out_2021" in text and "hold_out_2024" in text


def test_output_names_are_distinct_across_modes_and_settings() -> None:
    final = P.RunConfig(source="full", allow_test_season=True)
    final_sens = P.RunConfig(source="full", allow_test_season=True, g=5.0, k=3)
    dev = P.RunConfig(source="full", no_test=True)
    sens = P.RunConfig(source="full", no_test=True, g=5.0, k=3)
    names = ("model_eval", "calibration", "feature_importance", "reliability", "loso")
    paths = [cfg.output(n, "csv") for cfg in (final, final_sens, dev, sens) for n in names]
    assert len(paths) == len(set(paths))
    assert final.output("model_eval", "csv").name == "model_eval_G3.5_k2.csv"
    assert final_sens.output("model_eval", "csv").name == "model_eval_G5.0_k3.csv"
    assert final.output("reliability", "png").name == "reliability_G3.5_k2.png"
    assert final_sens.output("confusion_matrix", "png").name == "confusion_matrix_G5.0_k3.png"
    assert final.output("feature_importance", "png").name == "feature_importance_G3.5_k2.png"
    # development names are unchanged by the rename
    assert dev.output("model_eval", "csv").name == "model_dev_eval.csv"
    assert sens.output("model_eval", "csv").name == "model_dev_eval_G5.0_k3.csv"
    assert P.RunConfig(source="sample").output("model_eval", "csv").name == "model_eval_sample.csv"
    assert final.stamp == "" and dev.stamp == sens.stamp == P.DEV_STAMP


def test_primary_and_sensitivity_test_runs_never_share_a_file() -> None:
    primary = P.RunConfig(source="full", allow_test_season=True)
    sensitivity = P.RunConfig(source="full", allow_test_season=True, g=5.0, k=3)
    a, b = set(primary.result_paths()), set(sensitivity.result_paths())
    assert len(a) == len(P.RESULT_FILES) + 1 and len(b) == len(a)
    assert not a & b


def test_a_test_run_refuses_to_overwrite_any_existing_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    primary = P.RunConfig(source="full", allow_test_season=True)
    sensitivity = P.RunConfig(source="full", allow_test_season=True, g=5.0, k=3)
    for path in sensitivity.result_paths():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("sensitivity result")
    P.check_no_previous_test_run(primary)  # the sensitivity files never block the primary
    with pytest.raises(P.RunError, match="never overwritten"):
        P.check_no_previous_test_run(sensitivity)
    primary.output("model_eval", "csv").write_text("first complete primary run")
    with pytest.raises(P.RunError, match=r"G=3\.5 k=2 already exist"):
        P.check_no_previous_test_run(primary)
    assert P.main(["--source", "full", "--allow-test-season"]) == 2
    assert "never overwritten" in capsys.readouterr().err
    assert all(p.read_text() == "sensitivity result" for p in sensitivity.result_paths())
    P.check_no_previous_test_run(P.RunConfig(source="full", no_test=True))  # dev runs may rerun


def test_no_test_guards(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    assert P.main(["--source", "full", "--no-test", "--allow-test-season"]) == 2
    assert "mutually exclusive" in capsys.readouterr().err
    with pytest.raises(P.RunError, match="mutually exclusive"):
        P.RunConfig(source="full", no_test=True, allow_test_season=True)
    for source in ("sample", "rehearsal"):
        assert P.main(["--source", source, "--no-test"]) == 2
        assert "development mode of --source full" in capsys.readouterr().err
    with pytest.raises(P.RunError, match="'real' feature set"):
        P.RunConfig(source="full", no_test=True, feature_set="with_synthetic")
    with pytest.raises(P.RunError, match="--no-test"):
        P.RunConfig(source="full")


def test_test_report_has_supplementary_5bin_ece(rehearsal_run: dict[str, Any]) -> None:
    rows = rehearsal_run["result"].eval_rows
    five = [r for r in rows if r["split"] == "test" and r["metric"] == "ece_5bin"]
    assert {r["ci_kind"] for r in five} == set(E.BOOTSTRAP_KINDS)
    assert {"lr_calibrated", "gbt_calibrated", "majority", "rule"} <= {r["model"] for r in five}
    assert all("supplementary (5 bins" in r["note"] and "biased upward" in r["note"] for r in five)
    ten = [r for r in rows if r["split"] == "test" and r["metric"] == "ece"]
    assert ten and all("primary 10-bin" in r["note"] for r in ten)
    test = rehearsal_run["result"].test
    (lr5,) = [r for r in five if r["model"] == "lr_calibrated" and r["ci_kind"] == "race_cluster"]
    assert lr5["value"] == pytest.approx(M.ece(test.labels, test.scores["lr_calibrated"], n_bins=5))


def test_decision_threshold_is_fixed_and_reported(rehearsal_run: dict[str, Any]) -> None:
    import csv

    assert M.DECISION_THRESHOLD == 0.5
    result = rehearsal_run["result"]
    test = result.test
    for r in result.eval_rows:
        if r["split"] == "test" and r["metric"] == "f1_positive" and r["ci_kind"] == "race_cluster":
            expected = M.f1_positive(test.labels, test.scores[r["model"]], 0.5)
            assert r["value"] == pytest.approx(expected) or (
                np.isnan(r["value"]) and np.isnan(expected)
            )
    path = rehearsal_run["base"] / "results" / "model_eval_rehearsal.csv"
    lines = [x for x in path.read_text().splitlines() if not x.startswith("#")]
    rows = list(csv.DictReader(lines))
    assert {
        r["decision_threshold"] for r in rows if r["metric"] in ("f1_positive", "macro_f1")
    } == {"0.500000"}
    assert {
        r["decision_threshold"] for r in rows if r["metric"] not in ("f1_positive", "macro_f1")
    } == {""}
    assert result.metadata["decision_threshold"] == 0.5


# ------------------------------------------------------------------ feature set real_ti (dev only)
def test_real_ti_guards_and_names(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(P.RunError, match="--allow-test-season"):
        P.RunConfig(source="full", feature_set="real_ti")
    with pytest.raises(P.RunError, match="no TracingInsights inputs"):
        P.RunConfig(source="rehearsal", feature_set="real_ti")
    test = P.RunConfig(source="full", allow_test_season=True, feature_set="real_ti")
    test_sens = P.RunConfig(
        source="full", allow_test_season=True, feature_set="real_ti", g=5.0, k=3
    )
    assert test.output("model_eval", "csv").name == "model_eval_real_ti_G3.5_k2.csv"
    assert test_sens.output("model_eval", "csv").name == "model_eval_real_ti_G5.0_k3.csv"
    assert test.output("missing_rates", "csv").name == "missing_rates_real_ti_G3.5_k2.csv"
    assert test.output("missing_rates", "csv") in test.result_paths()
    assert test.model_subdir / "metadata.json" in test.result_paths()
    assert test.model_subdir.name == "full_real_ti_G3.5_k2"
    dev = P.RunConfig(source="full", no_test=True, feature_set="real_ti")
    sens = P.RunConfig(source="full", no_test=True, feature_set="real_ti", g=5.0, k=3)
    assert dev.output("model_eval", "csv").name == "model_dev_eval_real_ti.csv"
    assert sens.output("model_eval", "csv").name == "model_dev_eval_real_ti_G5.0_k3.csv"
    assert dev.output("missing_rates", "csv").name == "model_dev_missing_rates_real_ti.csv"
    sample = P.RunConfig(source="sample", feature_set="real_ti")
    assert sample.output("model_eval", "csv").name == "model_eval_real_ti_sample.csv"
    real_dev = P.RunConfig(source="full", no_test=True)
    real_paths = set(real_dev.result_paths()) | set(
        P.RunConfig(source="full", no_test=True, g=5.0, k=3).result_paths()
    )
    assert not real_paths & (set(dev.result_paths()) | set(sens.result_paths()))
    assert {p.name for p in real_dev.result_paths()} == {
        "model_dev_eval.csv",
        "model_dev_calibration.csv",
        "model_dev_feature_importance.csv",
        "model_dev_feature_importance.png",
        "model_dev_reliability.png",
        "model_dev_confusion_matrix.png",
        "model_dev_class_balance.csv",
        "model_dev_loso.csv",
        "model_dev_metadata.json",
    }
    assert dev.output("missing_rates", "csv") in dev.result_paths()
    assert P.missing_rate_lines([]) == []


@pytest.fixture(scope="module")
def dev_ti_run(
    spark: SparkSession,
    sample_clean_lake: SampleLake,
    sample_features_root: str,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[dict[str, Any]]:
    """``--no-test --feature-set real_ti`` on a copy of the sample lake (features + clean laps).
    The sample has no TracingInsights tables, so every TI input is missing; every 2025
    partition (features and clean) is unreadable junk."""
    import shutil

    base = tmp_path_factory.mktemp("dev_ti")
    lake = base / "data" / "lake"
    shutil.copytree(Path(sample_features_root.removeprefix("file://")), lake / "features")
    shutil.copytree(sample_clean_lake.lake / "clean" / "laps", lake / "clean" / "laps")
    junk_dirs = [
        lake / "features" / t for t in ("undercut_attempts", "stints", "telemetry_lap_features")
    ]
    junk_dirs += [
        lake / "clean" / t for t in ("laps", "stints_tracinginsights", "telemetry_lap_real")
    ]
    for d in junk_dirs:
        junk = d / "season=2025" / "round=1"
        junk.mkdir(parents=True)
        (junk / "part-00000-junk.parquet").write_bytes(b"not parquet: the test season")
    paths: list[str] = []
    original_parquet = DataFrameReader.parquet

    def parquet_spy(self: Any, *p: str, **kw: Any) -> Any:
        paths.extend(str(x) for x in p)
        return original_parquet(self, *p, **kw)

    mp = pytest.MonkeyPatch()
    mp.setattr(DataFrameReader, "parquet", parquet_spy)
    mp.setattr(P, "evaluate_test", lambda *a, **k: pytest.fail("test season evaluated"))
    mp.setenv("DATA_LAKE_URI", f"file://{lake}")
    cwd = os.getcwd()
    os.chdir(base)
    lines: list[str] = []
    try:
        from f1sa.config import get_settings

        get_settings.cache_clear()
        cfg = P.RunConfig(
            source="full", no_test=True, feature_set="real_ti", n_boot=20, train=SMALL
        )
        result = P.run(spark, cfg, out=lines.append)
        real = P.run(
            spark,
            P.RunConfig(source="full", no_test=True, n_boot=20, train=SMALL),
            out=lambda _m: None,
        )
    finally:
        os.chdir(cwd)
        mp.undo()
    yield {"result": result, "real": real, "paths": paths, "base": base, "lines": lines}


def test_real_ti_dev_run_reads_2021_2024_only_and_drops_nothing(dev_ti_run: dict[str, Any]) -> None:
    paths = dev_ti_run["paths"]
    assert any("/clean/laps/season=" in p for p in paths)
    assert not [p for p in paths if "2025" in p]
    ds, real = dev_ti_run["result"].dataset, dev_ti_run["real"].dataset
    assert ds.feature_set == "real_ti" and len(ds.columns) == 98
    assert ds.counts == real.counts and ds.class_weights == real.class_weights


def test_real_ti_dev_run_reports_missing_rates(dev_ti_run: dict[str, Any]) -> None:
    results = dev_ti_run["base"] / "results"
    text = (results / "model_dev_missing_rates_real_ti.csv").read_text().splitlines()
    assert text[0].startswith("# DEVELOPMENT")
    assert text[1] == ",".join(P.MISSING_RATE_COLUMNS)
    meta = json.loads((results / "model_dev_metadata_real_ti.json").read_text())
    assert meta["feature_set"] == "real_ti" and meta["coverage_policy"] == P.COVERAGE_POLICY
    ti = [r for r in meta["missing_rates"] if r["provenance"] == "tracinginsights"]
    assert ti and all(r["missing_rate"] == 1.0 for r in ti)  # the sample has no TI tables
    assert any(x.startswith("missing (TracingInsights) 2024:") for x in dev_ti_run["lines"])
    assert (results / "model_dev_eval_real_ti.csv").is_file()
    assert (results / "model_dev_eval.csv").is_file()  # the real run kept its own names
    eval_text = (results / "model_dev_eval_real_ti.csv").read_text()
    assert ",real_ti," in eval_text


def test_real_tyre_guards_and_names() -> None:
    with pytest.raises(P.RunError, match="--allow-test-season"):
        P.RunConfig(source="full", feature_set="real_tyre")
    with pytest.raises(P.RunError, match="no TracingInsights inputs"):
        P.RunConfig(source="rehearsal", feature_set="real_tyre")
    test = P.RunConfig(source="full", allow_test_season=True, feature_set="real_tyre")
    assert test.output("model_eval", "csv").name == "model_eval_real_tyre_G3.5_k2.csv"
    assert test.output("missing_rates", "csv").name == "missing_rates_real_tyre_G3.5_k2.csv"
    assert test.model_subdir / "metadata.json" in test.result_paths()
    assert test.model_subdir.name == "full_real_tyre_G3.5_k2"
    tyre = P.RunConfig(source="full", no_test=True, feature_set="real_tyre")
    sens = P.RunConfig(source="full", no_test=True, feature_set="real_tyre", g=5.0, k=3)
    assert tyre.output("model_eval", "csv").name == "model_dev_eval_real_tyre.csv"
    assert sens.output("loso", "csv").name == "model_dev_loso_real_tyre_G5.0_k3.csv"
    assert tyre.output("missing_rates", "csv") in tyre.result_paths()
    others = [
        P.RunConfig(source="full", no_test=True, feature_set=fs, g=g, k=k)
        for fs in ("real", "real_ti")
        for g, k in ((3.5, 2), (5.0, 3))
    ]
    taken = {p for cfg in others for p in cfg.result_paths()}
    assert not taken & (set(tyre.result_paths()) | set(sens.result_paths()))


# ---------------------------------------------------- test season (docs/model_spec_v2.md)
SPEC_V2 = Path(__file__).resolve().parents[2] / "docs" / "model_spec_v2.md"
SPEC_V2_MARKER = "Test-season feature sets: "
SETTINGS = ((3.5, 2), (5.0, 3))


def test_test_season_feature_sets_are_exactly_those_in_model_spec_v2(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lines = [x for x in SPEC_V2.read_text().splitlines() if x.startswith(SPEC_V2_MARKER)]
    assert len(lines) == 1
    listed = tuple(x.strip(" `") for x in lines[0].removeprefix(SPEC_V2_MARKER).split(","))
    assert listed == P.MF.TEST_FEATURE_SETS == ("real", "real_tyre", "real_ti")
    for fs in P.MF.TEST_FEATURE_SETS:
        for g, k in SETTINGS:
            assert P.RunConfig(source="full", allow_test_season=True, feature_set=fs, g=g, k=k)
    others = sorted(set(P.MF.FEATURE_SETS) - set(P.MF.TEST_FEATURE_SETS))
    assert others == ["with_synthetic"]
    for fs in others:
        with pytest.raises(P.RunError, match="--allow-test-season accepts only"):
            P.RunConfig(source="full", allow_test_season=True, feature_set=fs)
    monkeypatch.chdir(tmp_path)
    argv = ["--source", "full", "--allow-test-season", "--feature-set", "with_synthetic"]
    assert P.main(argv) == 2
    assert "docs/model_spec_v2.md" in capsys.readouterr().err


def test_all_test_season_runs_write_disjoint_files() -> None:
    runs = [
        P.RunConfig(source="full", allow_test_season=True, feature_set=fs, g=g, k=k)
        for fs in P.MF.TEST_FEATURE_SETS
        for g, k in SETTINGS
    ]
    paths = [p for cfg in runs for p in cfg.result_paths()]
    assert len(runs) == 6 and len(paths) == len(set(paths))
    real = P.RunConfig(source="full", allow_test_season=True)
    assert {p.name for p in real.result_paths()} == {  # real's v1 names are unchanged
        "model_eval_G3.5_k2.csv",
        "calibration_G3.5_k2.csv",
        "feature_importance_G3.5_k2.csv",
        "feature_importance_G3.5_k2.png",
        "reliability_G3.5_k2.png",
        "confusion_matrix_G3.5_k2.png",
        "metadata.json",
    }


@pytest.mark.parametrize("feature_set", ["real_tyre", "real_ti"])
def test_test_run_overwrite_guard_covers_real_tyre_and_real_ti(
    feature_set: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    def cfg(fs: str, g: float = 3.5, k: int = 2) -> P.RunConfig:
        return P.RunConfig(source="full", allow_test_season=True, feature_set=fs, g=g, k=k)

    primary, sensitivity = cfg(feature_set), cfg(feature_set, 5.0, 3)
    assert all(f"_{feature_set}_G" in p.name for p in primary.result_paths()[:-1])
    for path in cfg("real").result_paths() + cfg("real", 5.0, 3).result_paths():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("real result")
    P.check_no_previous_test_run(primary)  # real's files never block the ablation runs
    P.check_no_previous_test_run(sensitivity)
    # each result file and the metadata blocks its own run, and only that run
    for blocker in primary.result_paths():
        blocker.parent.mkdir(parents=True, exist_ok=True)
        blocker.write_text("first complete ablation run")
        with pytest.raises(P.RunError, match=r"G=3\.5 k=2 already exist"):
            P.check_no_previous_test_run(primary)
        P.check_no_previous_test_run(sensitivity)
        blocker.unlink()
    other = "real_ti" if feature_set == "real_tyre" else "real_tyre"
    sensitivity.output("missing_rates", "csv").write_text("sensitivity missing rates")
    with pytest.raises(P.RunError, match=r"G=5 k=3 already exist.*never overwritten"):
        P.check_no_previous_test_run(sensitivity)
    P.check_no_previous_test_run(cfg(other, 5.0, 3))  # one ablation set never blocks the other
    argv = ["--source", "full", "--allow-test-season", "--feature-set", feature_set]
    assert P.main([*argv, "--g", "5.0", "--k", "3"]) == 2
    assert "never overwritten" in capsys.readouterr().err
    assert sensitivity.output("missing_rates", "csv").read_text() == "sensitivity missing rates"
    real_files = cfg("real").result_paths() + cfg("real", 5.0, 3).result_paths()
    assert all(p.read_text() == "real result" for p in real_files)
