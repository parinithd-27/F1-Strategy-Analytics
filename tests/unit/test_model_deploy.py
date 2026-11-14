"""Deployment model (Step 9c): saved without reading 2025, reloads exactly, matches the dev run.

The lake is a copy of the sample feature zone whose ``season=2025`` partitions are unreadable junk,
as in the development-mode test: any read, listing for schema or fit of 2025 fails or is recorded.
A development run (``pipeline.run`` in no-test mode) is made first, so the reproduction check has
its reference files; then the deployment is saved under spies on ``DataFrameReader.parquet``,
``Estimator.fit`` and ``evaluate_test``.
"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from _model_fixtures import SMALL
from pyspark.ml import PipelineModel
from pyspark.ml.base import Estimator
from pyspark.sql import DataFrame, DataFrameReader, SparkSession

from conftest import SAMPLE_DIR
from f1sa.model import deploy as D
from f1sa.model import evaluate as E
from f1sa.model import features as MF
from f1sa.model import metrics as M
from f1sa.model import pipeline as P
from f1sa.model.calibrate import Calibrator
from f1sa.model.lr_scorer import LRScorer
from f1sa.model.train import predict

GIT_ENV = {"F1SA_GIT_SHA": "0123456789abcdef", "F1SA_GIT_DIRTY": "false"}


@pytest.fixture(scope="module")
def deployed(
    spark: SparkSession, sample_features_root: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    base = tmp_path_factory.mktemp("deploy")
    lake = base / "data" / "lake"
    shutil.copytree(Path(sample_features_root.removeprefix("file://")), lake / "features")
    for table in D.FEATURE_TABLES:
        junk = lake / "features" / table / "season=2025" / "round=1"
        junk.mkdir(parents=True)
        (junk / "part-00000-junk.parquet").write_bytes(b"not parquet: the test season")
    mp = pytest.MonkeyPatch()
    mp.setenv("DATA_LAKE_URI", f"file://{lake}")
    for k, v in GIT_ENV.items():
        mp.setenv(k, v)
    cwd = os.getcwd()
    os.chdir(base)
    from f1sa.config import get_settings

    get_settings.cache_clear()
    paths: list[str] = []
    fits: list[set[int]] = []
    opened: list[int] = []
    lines: list[str] = []
    try:
        P.run(
            spark,
            P.RunConfig(source="full", no_test=True, n_boot=20, train=SMALL),
            out=lambda _m: None,
        )
        original_parquet, original_fit = DataFrameReader.parquet, Estimator.fit

        def parquet_spy(self: Any, *p: str, **kw: Any) -> Any:
            paths.extend(str(x) for x in p)
            return original_parquet(self, *p, **kw)

        def fit_spy(self: Any, dataset: DataFrame, params: Any = None) -> Any:
            if "season" in dataset.columns:
                fits.append({int(r[0]) for r in dataset.select("season").distinct().collect()})
            return original_fit(self, dataset, params)

        spy = pytest.MonkeyPatch()
        spy.setattr(DataFrameReader, "parquet", parquet_spy)
        spy.setattr(Estimator, "fit", fit_spy)
        spy.setattr(E, "evaluate_test", lambda *a, **k: opened.append(1))
        spy.setattr(P, "evaluate_test", lambda *a, **k: opened.append(1))
        try:
            result = D.save_deployment(spark, D.DeployConfig(train=SMALL), out=lines.append)
        finally:
            spy.undo()
        yield {
            "result": result,
            "paths": paths,
            "fits": fits,
            "opened": opened,
            "lines": lines,
            "base": base,
        }
    finally:
        os.chdir(cwd)
        mp.undo()
        get_settings.cache_clear()


def test_save_never_reads_lists_or_fits_the_test_season(deployed: dict[str, Any]) -> None:
    assert deployed["paths"], "the loader spy recorded nothing"
    assert not [p for p in deployed["paths"] if "2025" in p]
    assert all("/season=" in p for p in deployed["paths"])
    assert deployed["fits"] and all(2025 not in s for s in deployed["fits"])
    assert deployed["opened"] == []
    r = deployed["result"]
    assert r.dataset.split.test == () and r.dataset.test.opened == 0
    for kind in ("lr", "gbt"):
        assert r.metadata[kind]["partitions_read"]
        assert not [p for p in r.metadata[kind]["partitions_read"] if "2025" in p]


def test_layout_and_no_staging_left(deployed: dict[str, Any]) -> None:
    target = deployed["base"] / "models/undercut/deploy_real_G3.5_k2"
    assert deployed["result"].target == Path("models/undercut/deploy_real_G3.5_k2")
    for kind in ("lr", "gbt"):
        assert (target / kind / "pipeline" / "metadata").is_dir()
        assert (target / kind / "calibrator.json").is_file()
        assert (target / kind / "metadata.json").is_file()
    assert (target / "lr" / "scorer.json").is_file()
    assert not (target / "gbt" / "scorer.json").exists()
    assert (target / "reproduction.json").is_file()
    assert not list(target.parent.glob(".staging-*"))
    assert "reproduction: MATCH" in deployed["lines"]


def test_metadata_has_every_required_field(deployed: dict[str, Any]) -> None:
    target = deployed["base"] / "models/undercut/deploy_real_G3.5_k2"
    for kind in ("lr", "gbt"):
        meta = json.loads((target / kind / "metadata.json").read_text())
        missing = [k for k in D.REQUIRED_METADATA if k not in meta]
        assert not missing, (kind, missing)
        assert meta["model"] == kind and meta["model_spec"] == "model-spec-v1"
        assert meta["features"] == [s.name for s in MF.specs_for("real")]
        assert meta["assembled_columns"] == list(MF.assembled_columns("real"))
        assert meta["feature_set"] == "real" and meta["g"] == 3.5 and meta["k"] == 2
        assert meta["training_seasons"] == [2021, 2022, 2023]
        assert meta["calibration_seasons"] == [2024]
        assert meta["decision_threshold"] == M.DECISION_THRESHOLD == 0.5
        assert meta["seed"] == 42
        assert meta["git_sha"] == GIT_ENV["F1SA_GIT_SHA"] and meta["git_dirty"] is False
        assert meta["test_season_read"] is False
        assert "2025" in meta["test_season_statement"]
        assert {"pyspark", "python", "numpy", "java"} <= set(meta["versions"])
        assert meta["calibrator"] == json.loads((target / kind / "calibrator.json").read_text())
        assert set(meta["hyperparameters"]) == set(SMALL.grid(kind)[0])


def test_reloaded_model_predicts_exactly_like_the_in_memory_model(
    deployed: dict[str, Any],
) -> None:
    r = deployed["result"]
    target = deployed["base"] / "models/undercut/deploy_real_G3.5_k2"
    for kind in ("lr", "gbt"):
        loaded = PipelineModel.load((target / kind / "pipeline").as_uri())
        a = predict(r.models[kind].model, r.dataset.validation)
        b = predict(loaded, r.dataset.validation)
        assert np.array_equal(a["score"].to_numpy(), b["score"].to_numpy())
        cal = Calibrator.from_json(json.loads((target / kind / "calibrator.json").read_text()))
        assert cal == r.calibrators[kind]
    assert all(r.reproduction["round_trip_exact"].values())


def test_reproduction_matches_the_development_run(deployed: dict[str, Any]) -> None:
    rep = deployed["result"].reproduction
    assert rep["match"] is True
    checks = {(row["check"], row["model"]) for row in rep["development_run"]}
    for kind in ("lr", "gbt"):
        for name in (
            "hyperparameters",
            "validation_pr_auc_raw",
            "calibrator",
            "brier_calibrated (6 dp)",
            "pr_auc_calibrated (6 dp)",
        ):
            assert (name, kind) in checks
    assert all(row["match"] for row in rep["development_run"])
    assert rep["scorer_vs_spark"]["max_abs_diff"] <= D.SCORER_TOLERANCE


def test_saved_scorer_equals_spark_on_validation(deployed: dict[str, Any]) -> None:
    r = deployed["result"]
    target = deployed["base"] / "models/undercut/deploy_real_G3.5_k2"
    scorer = LRScorer.load(target / "lr" / "scorer.json")
    assert scorer.assembled_columns == r.dataset.columns
    assert r.validation.labels.size >= 1


def test_existing_folder_is_refused_without_force_and_replaced_with_it(
    spark: SparkSession, deployed: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    base = deployed["base"]
    monkeypatch.chdir(base)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{base}/data/lake")
    for k, v in GIT_ENV.items():
        monkeypatch.setenv(k, v)
    from f1sa.config import get_settings

    get_settings.cache_clear()
    target = base / "models/undercut/deploy_real_G3.5_k2"
    before = json.loads((target / "lr" / "metadata.json").read_text())
    with pytest.raises(D.DeployError, match="--force"):
        D.save_deployment(spark, D.DeployConfig(train=SMALL), out=lambda _m: None)
    assert D.main(["--source", "full"], spark_factory=lambda _m: spark, train=SMALL) == 2
    (target / "marker").write_text("old")
    rc = D.main(["--source", "full", "--force"], spark_factory=lambda _m: spark, train=SMALL)
    assert rc == 0
    assert not (target / "marker").exists()
    after = json.loads((target / "lr" / "metadata.json").read_text())
    assert after["calibrator"] == before["calibrator"]
    assert after["hyperparameters"] == before["hyperparameters"]
    get_settings.cache_clear()


def test_mismatch_with_the_development_run_stops_and_keeps_staging(
    spark: SparkSession, deployed: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    base = deployed["base"]
    monkeypatch.chdir(base)
    monkeypatch.setenv("DATA_LAKE_URI", f"file://{base}/data/lake")
    for k, v in GIT_ENV.items():
        monkeypatch.setenv(k, v)
    from f1sa.config import get_settings

    get_settings.cache_clear()
    tampered = base / "results" / "tampered"
    tampered.mkdir()
    for f in (base / "results").glob("model_dev_*"):
        shutil.copy(f, tampered / f.name)
    meta_path = tampered / "model_dev_metadata.json"
    meta = json.loads(meta_path.read_text())
    meta["models"]["lr"]["params"]["regParam"] = 123.0
    meta_path.write_text(json.dumps(meta))
    cfg = D.DeployConfig(
        train=SMALL, results_dir=Path("results/tampered"), models_dir=Path("models/t")
    )
    with pytest.raises(D.ReproductionError) as info:
        D.save_deployment(spark, cfg, out=lambda _m: None)
    assert any("hyperparameters lr" in m for m in info.value.mismatches)
    assert info.value.staging.is_dir() and (info.value.staging / "reproduction.json").is_file()
    assert not (base / cfg.target).exists()
    rep = json.loads((info.value.staging / "reproduction.json").read_text())
    assert rep["match"] is False
    get_settings.cache_clear()


def test_missing_development_results_are_a_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    cfg = D.DeployConfig(results_dir=Path("results"))
    rows, bad = D.compare_with_dev(cfg, {}, {}, None)  # type: ignore[arg-type]
    assert rows == [] and bad and "model-dev-full" in bad[0]


def test_setting_folders_are_distinct_and_never_the_test_run_folder() -> None:
    primary = D.DeployConfig().target
    sensitivity = D.DeployConfig(g=5.0, k=3).target
    assert primary.name == "deploy_real_G3.5_k2" and sensitivity.name == "deploy_real_G5.0_k3"
    assert primary != sensitivity
    for g, k in ((3.5, 2), (5.0, 3)):
        test_dir = P.RunConfig(source="full", allow_test_season=True, g=g, k=k).model_subdir
        assert test_dir not in (primary, sensitivity)
        assert D.DeployConfig(g=g, k=k).target.parent == test_dir.parent
    assert D.DeployConfig().run_config().no_test is True
    assert D.DeployConfig().run_config().allow_test_season is False


@pytest.mark.parametrize(
    "argv",
    [
        ["--source", "rehearsal"],
        ["--source", "full", "--allow-test-season"],
        ["--source", "full", "--no-test"],
        [],
    ],
)
def test_cli_rejects_other_sources_and_test_season_flags(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as info:
        D.parse_args(argv)
    assert info.value.code == 2


def test_only_the_real_feature_set() -> None:
    with pytest.raises(D.DeployError):
        D.DeployConfig(feature_set="with_synthetic")


def test_git_info_from_env_and_refusal_without_sha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert D.git_info({"F1SA_GIT_SHA": "abc", "F1SA_GIT_DIRTY": "true"}) == D.GitInfo("abc", True)
    assert D.git_info({"F1SA_GIT_SHA": "abc"}).dirty is None
    monkeypatch.chdir(tmp_path)  # not a git repository
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))
    with pytest.raises(D.DeployError, match="git sha"):
        D.git_info({})


def test_cli_refuses_without_git_sha(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))
    for k in GIT_ENV:
        monkeypatch.delenv(k, raising=False)

    def no_spark(_m: str) -> SparkSession:
        raise AssertionError("spark must not start")

    assert D.main(["--source", "full"], spark_factory=no_spark) == 2


def test_cli_refuses_models_dir_outside_models(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    for k, v in GIT_ENV.items():
        monkeypatch.setenv(k, v)

    def no_spark(_m: str) -> SparkSession:
        raise AssertionError("spark must not start")

    assert D.main(["--source", "full", "--models-dir", "/tmp/x"], spark_factory=no_spark) == 2


# ------------------------------------------------------------------ sample source (Step 17)
@pytest.fixture(scope="module")
def deployed_sample(
    spark: SparkSession, sample_features_root: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Any]]:
    """``--source sample`` on a copy of the sample feature zone, with no development run."""
    base = tmp_path_factory.mktemp("deploy_sample")
    lake = base / "data" / "lake"
    shutil.copytree(Path(sample_features_root.removeprefix("file://")), lake / "features")
    mp = pytest.MonkeyPatch()
    mp.setenv("DATA_LAKE_URI", f"file://{lake}")
    mp.setenv("SAMPLE_DIR", str(SAMPLE_DIR))
    for k, v in GIT_ENV.items():
        mp.setenv(k, v)
    cwd = os.getcwd()
    os.chdir(base)
    from f1sa.config import get_settings

    get_settings.cache_clear()
    lines: list[str] = []
    opened: list[int] = []
    spy = pytest.MonkeyPatch()
    spy.setattr(E, "evaluate_test", lambda *a, **k: opened.append(1))
    spy.setattr(P, "evaluate_test", lambda *a, **k: opened.append(1))
    try:
        cfg = D.DeployConfig(source="sample", train=SMALL)
        result = D.save_deployment(spark, cfg, out=lines.append)
        yield {"result": result, "lines": lines, "base": base, "opened": opened}
    finally:
        spy.undo()
        os.chdir(cwd)
        mp.undo()
        get_settings.cache_clear()


def test_sample_source_writes_only_the_sample_folder(deployed_sample: dict[str, Any]) -> None:
    base = deployed_sample["base"]
    assert deployed_sample["result"].target == Path("models/undercut/sample_deploy_real_G3.5_k2")
    target = base / "models/undercut/sample_deploy_real_G3.5_k2"
    assert (target / "lr" / "scorer.json").is_file()
    assert (target / "reproduction.json").is_file()
    assert not (base / "models/undercut/deploy_real_G3.5_k2").exists()
    assert not (base / "results").exists(), "the sample deployment must not write results/"
    assert not list(target.parent.glob(".staging-*"))
    assert deployed_sample["opened"] == []


def test_sample_metadata_is_stamped_and_has_no_test_season(
    deployed_sample: dict[str, Any],
) -> None:
    target = deployed_sample["base"] / "models/undercut/sample_deploy_real_G3.5_k2"
    for kind in ("lr", "gbt"):
        meta = json.loads((target / kind / "metadata.json").read_text())
        assert meta["source"] == "sample" and meta["smoke_test"] is True
        assert meta["stamp"].startswith("SMOKE TEST")
        assert meta["test_season_read"] is False
        assert meta["test_season_statement"] == D.SAMPLE_TEST_SEASON_STATEMENT
        assert meta["training_seasons"] == [2021, 2022, 2023]
        assert meta["calibration_seasons"] == [2024]
        for field_name in D.REQUIRED_METADATA:
            assert field_name in meta, field_name


def test_sample_reproduction_skips_only_the_development_comparison(
    deployed_sample: dict[str, Any],
) -> None:
    report = deployed_sample["result"].reproduction
    assert report["development_run"] == D.SAMPLE_DEV_SKIPPED
    assert all(report["round_trip_exact"].values())
    assert report["scorer_vs_spark"]["max_abs_diff"] <= D.SCORER_TOLERANCE
    assert report["match"] is True
    assert "reproduction: MATCH" in deployed_sample["lines"]
    assert any(D.SAMPLE_DEV_SKIPPED in line for line in deployed_sample["lines"])


def test_sample_model_loads_with_the_stream_jobs_loader(deployed_sample: dict[str, Any]) -> None:
    from f1sa.streaming.stream_job import load_model

    target = deployed_sample["base"] / "models/undercut/sample_deploy_real_G3.5_k2"
    scorer, params, meta = load_model(target)
    assert params.gap_threshold_s == 3.5 and meta["smoke_test"] is True
    assert [s.name for s in scorer.inputs] == meta["features"]


def test_sample_and_full_targets_differ_and_sources_are_checked() -> None:
    full, sample = D.DeployConfig().target, D.DeployConfig(source="sample").target
    assert full.name == "deploy_real_G3.5_k2" and sample.name == "sample_deploy_real_G3.5_k2"
    assert full.parent == sample.parent
    rc = D.DeployConfig(source="sample").run_config()
    assert rc.source == "sample" and rc.no_test is False and rc.allow_test_season is False
    with pytest.raises(D.DeployError, match="unknown source"):
        D.DeployConfig(source="rehearsal")
    assert D.parse_args(["--source", "sample"]).source == "sample"
