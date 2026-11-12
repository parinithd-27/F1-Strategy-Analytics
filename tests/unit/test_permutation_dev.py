"""DEVELOPMENT permutation importance for both models (``model.permutation_dev``)."""

from __future__ import annotations

import math
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _model_fixtures import SMALL, frame, rehearsal_rows
from pyspark.sql import DataFrameReader, SparkSession

from conftest import SampleLake
from f1sa.model import dataset as D
from f1sa.model import permutation_dev as PD
from f1sa.model import pipeline as P
from f1sa.model.explain import permutation_importance
from f1sa.model.train import TrainedModel, tune


def test_transform_monotone_is_identical_and_reversal_is_not(spark: SparkSession) -> None:
    ds = D.build_dataset(frame(spark, rehearsal_rows()), split=D.SplitSpec(test=()))
    tm = tune("lr", ds, SMALL)
    raw = permutation_importance(spark, tm, ds.validation, ds.columns, repeats=2)
    platt_like = permutation_importance(
        spark, tm, ds.validation, ds.columns, repeats=2, transform=lambda s: 0.2 + 0.5 * s
    )
    flipped = permutation_importance(
        spark, tm, ds.validation, ds.columns, repeats=2, transform=lambda s: 1.0 - s
    )
    assert [(r["feature"], r["value"]) for r in raw] == [
        (r["feature"], r["value"]) for r in platt_like
    ]
    assert {r["feature"]: r["value"] for r in raw} != {r["feature"]: r["value"] for r in flipped}


def _tm(kind: str, params: dict[str, float], pr_auc: float) -> TrainedModel:
    return TrainedModel(kind=kind, model=None, params=params, validation_pr_auc=pr_auc)  # type: ignore[arg-type]


def test_check_reproduction() -> None:
    meta = {
        "counts": {"train": {"n": 4}},
        "models": {
            "lr": {"params": {"regParam": 0.1}, "validation_pr_auc": 0.5},
            "gbt": {"params": {"maxDepth": 2}, "validation_pr_auc": float("nan")},
        },
    }
    ok = {"lr": _tm("lr", {"regParam": 0.1}, 0.5), "gbt": _tm("gbt", {"maxDepth": 2}, math.nan)}
    PD.check_reproduction(meta, {"train": {"n": 4}}, ok)
    with pytest.raises(PD.ReproductionError, match="counts"):
        PD.check_reproduction(meta, {"train": {"n": 5}}, ok)
    with pytest.raises(PD.ReproductionError, match="lr params"):
        PD.check_reproduction(
            meta, {"train": {"n": 4}}, {**ok, "lr": _tm("lr", {"regParam": 0.0}, 0.5)}
        )
    with pytest.raises(PD.ReproductionError, match="validation PR-AUC"):
        PD.check_reproduction(
            meta, {"train": {"n": 4}}, {**ok, "lr": _tm("lr", {"regParam": 0.1}, 0.5001)}
        )


def test_check_selected_and_reader(tmp_path: Path) -> None:
    f = tmp_path / "fi.csv"
    f.write_text(
        "# DEVELOPMENT\nmodel,method,feature,value,std,note\n"
        "gbt,gbt_importance,gap_s,0.4,,\n"
        "gbt,permutation_pr_auc_drop,gap_s,0.012345,0.001,\n"
        "gbt,permutation_pr_auc_drop,laps_remaining,,,undefined\n"
    )
    ref = PD.read_selected_permutation(f)
    assert ref["gap_s"] == 0.012345 and math.isnan(ref["laps_remaining"]) and len(ref) == 2
    rows = [
        {"model": "gbt", "scores": "raw", "feature": "gap_s", "value": 0.0123451},
        {"model": "gbt", "scores": "raw", "feature": "laps_remaining", "value": math.nan},
        {"model": "gbt", "scores": "calibrated", "feature": "gap_s", "value": 9.0},
        {"model": "lr", "scores": "raw", "feature": "gap_s", "value": 9.0},
    ]
    PD.check_selected(rows, "gbt", ref)
    with pytest.raises(PD.ReproductionError, match="gap_s"):
        PD.check_selected([{**rows[0], "value": 0.02}, rows[1]], "gbt", ref)


def test_guards(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, spark: SparkSession) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(P.RunError, match="development mode only"):
        PD.run_permutation(spark, P.RunConfig(source="sample"))
    with pytest.raises(P.RunError, match="run the development model first"):
        PD.run_permutation(spark, P.RunConfig(source="full", no_test=True, feature_set="real_ti"))
    assert PD.main(["--feature-set", "with_synthetic"]) == 2


@pytest.fixture(scope="module")
def dev_lake(
    spark: SparkSession,
    sample_clean_lake: SampleLake,
    sample_features_root: str,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[dict[str, Any]]:
    """A ``real_ti`` development run on a copy of the sample lake, then the permutation run."""
    base = tmp_path_factory.mktemp("perm_dev")
    lake = base / "data" / "lake"
    shutil.copytree(Path(sample_features_root.removeprefix("file://")), lake / "features")
    shutil.copytree(sample_clean_lake.lake / "clean" / "laps", lake / "clean" / "laps")
    paths: list[str] = []
    original = DataFrameReader.parquet

    def spy(self: Any, *p: str, **kw: Any) -> Any:
        paths.extend(str(x) for x in p)
        return original(self, *p, **kw)

    mp = pytest.MonkeyPatch()
    mp.setenv("DATA_LAKE_URI", f"file://{lake}")
    cwd = os.getcwd()
    os.chdir(base)
    try:
        from f1sa.config import get_settings

        get_settings.cache_clear()
        cfg = P.RunConfig(
            source="full", no_test=True, feature_set="real_ti", n_boot=10, train=SMALL
        )
        P.run(spark, cfg, out=lambda _m: None)
        files = [base / p for p in cfg.result_paths()]
        fixed = [p.read_bytes() for p in files]
        mp.setattr(DataFrameReader, "parquet", spy)
        lines: list[str] = []
        target = PD.run_permutation(spark, cfg, out=lines.append)
        yield {
            "target": base / target,
            "lines": lines,
            "paths": paths,
            "files": files,
            "fixed": fixed,
        }
    finally:
        os.chdir(cwd)
        mp.undo()


def test_permutation_run_reproduces_and_writes_both_models(dev_lake: dict[str, Any]) -> None:
    text = dev_lake["target"].read_text().splitlines()
    assert dev_lake["target"].name == "model_dev_permutation_real_ti.csv"
    assert text[0].startswith("# DEVELOPMENT") and text[1] == ",".join(PD.COLUMNS)
    body = text[2:]
    combos = {tuple(x.split(",")[:2]) for x in body}
    assert combos == {("lr", "calibrated"), ("lr", "raw"), ("gbt", "calibrated"), ("gbt", "raw")}
    assert any("reproduction MATCH" in x for x in dev_lake["lines"])
    assert dev_lake["paths"] and not [p for p in dev_lake["paths"] if "2025" in p]
    # the development run's own files are untouched
    assert [p.read_bytes() for p in dev_lake["files"]] == dev_lake["fixed"]
