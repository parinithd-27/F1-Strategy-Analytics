"""``f1sa.model.oracle_dev`` (``make oracle-dev-full``): the ORACLE development variant.

ORACLE (realised out-lap delta) is not usable at decision time and is never evaluated on 2025.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from f1sa.model import features as MF
from f1sa.model import oracle_dev as O
from f1sa.model.dataset import ModelDataset, SealedSplit, SplitSpec

NOW = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)


def _ds(spark: SparkSession) -> tuple[ModelDataset, Any]:
    key = ("season", "round", "attacker_id", "defender_id", "lap")
    train = spark.createDataFrame(
        [(2021, 1, "a", "d", 10, 1, 0.5), (2022, 2, "b", "e", 12, 0, 0.1)],
        [*key, "label", "f_gap"],
    )
    validation = spark.createDataFrame([(2024, 3, "c", "f", 7, 1, 0.3)], [*key, "label", "f_gap"])
    frame = spark.createDataFrame(
        [
            (2021, 1, "a", "d", 10, 3.5, 2, 1.25),
            (2021, 1, "a", "d", 10, 5.0, 3, 9.0),
            (2022, 2, "b", "e", 12, 3.5, 2, None),
            (2024, 3, "c", "f", 7, 3.5, 2, -0.75),
        ],
        [*key, "gap_threshold_s", "response_window_laps", O.ORACLE_COLUMN],
    )
    ds = ModelDataset(
        train=train,
        validation=validation,
        test=SealedSplit(train.limit(0), ()),
        feature_set="real",
        columns=("f_gap",),
        g=3.5,
        k=2,
        split=SplitSpec(train=(2021, 2022, 2023), validation=(2024,), test=()),
        class_weights={0: 1.0, 1: 1.0},
    )
    return ds, frame


def test_with_oracle_appends_the_column_and_its_indicator(spark: SparkSession) -> None:
    ds, frame = _ds(spark)
    out = O.with_oracle(ds, frame, 3.5, 2)
    assert out.columns == ("f_gap", *O.ASSEMBLED)
    value, missing = O.ASSEMBLED
    rows = {r["attacker_id"]: (r[value], r[missing]) for r in out.train.collect()}
    assert rows == {"a": (1.25, 0.0), "b": (0.0, 1.0)}  # this setting only; null -> 0.0 + flag
    assert [(r[value], r[missing]) for r in out.validation.collect()] == [(-0.75, 0.0)]
    assert ds.columns == ("f_gap",)  # the input dataset is not changed


def test_with_oracle_needs_the_column(spark: SparkSession) -> None:
    ds, frame = _ds(spark)
    with pytest.raises(O.OracleError, match="no oracle_attacker_realised_outlap_delta_s"):
        O.with_oracle(ds, frame.drop(O.ORACLE_COLUMN), 3.5, 2)


def test_only_development_seasons() -> None:
    O.check_seasons([2021, 2022, 2023, 2024])
    with pytest.raises(O.OracleError, match=r"\[2025\]"):
        O.check_seasons([2024, 2025])
    assert 2025 not in O.DEV_SEASONS


def test_oracle_is_not_a_model_feature_set() -> None:
    assert "oracle" not in " ".join(MF.FEATURE_SETS)
    assert all(
        not c.startswith("f_oracle") for fs in MF.FEATURE_SETS for c in MF.assembled_columns(fs)
    )
    MF.check_registry()


def test_no_test_season_option() -> None:
    with pytest.raises(SystemExit):
        O.parse_args(["--source", "full", "--allow-test-season"])


def test_output_names_and_stamp() -> None:
    assert O.output_paths(Path("results"), 3.5, 2) == {
        "eval": Path("results/model_dev_eval_oracle.csv"),
        "loso": Path("results/model_dev_loso_oracle.csv"),
    }
    assert O.output_paths(Path("r"), 5.0, 3)["loso"] == Path("r/model_dev_loso_oracle_G5.0_k3.csv")
    assert "not usable at decision time" in O.STAMP and "never loaded" in O.STAMP


def _main(*argv: str, setting: Any) -> tuple[int, list[str]]:
    out: list[str] = []
    rc = O.main(
        ["--source", "full", *argv],
        spark_factory=lambda m: None,
        setting=setting,
        out=out.append,
        now=lambda: NOW,
    )
    return rc, out


def test_main_runs_both_settings_into_data_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    seen: list[tuple[float, int, Path]] = []

    def fake(spark: Any, g: float, k: int, results_dir: Path, out: Any) -> list[dict[str, Any]]:
        seen.append((g, k, results_dir))
        return [{"model": "lr_calibrated", "ci_kind": "race_cluster", "metric": "pr_auc",
                 "value": 0.5, "ci_low": 0.4, "ci_high": 0.6}]  # fmt: skip

    rc, out = _main(setting=fake)
    assert rc == O.EXIT_OK
    run = Path("data/oracle/full_20261114T120000Z")
    assert seen == [(3.5, 2, run), (5.0, 3, run)]
    assert out[0] == O.STAMP
    assert any("not usable at decision time" in line for line in out[1:])


def test_record_writes_results_and_refuses_existing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    seen: list[Path] = []
    rc, _ = _main("--record", setting=lambda s, g, k, d, o: seen.append(d) or [])
    assert rc == O.EXIT_OK and seen == [Path("results"), Path("results")]
    (tmp_path / "results").mkdir()
    (tmp_path / "results/model_dev_loso_oracle_G5.0_k3.csv").write_text("x")
    rc, out = _main("--record", setting=lambda *a: pytest.fail("ran despite existing outputs"))
    assert rc == O.EXIT_REFUSED and "never overwritten" in out[0]


def test_summary_lines_label_every_value() -> None:
    rows = [
        {"model": "lr_calibrated", "ci_kind": "race_cluster", "metric": "brier", "value": 0.16,
         "ci_low": 0.1, "ci_high": 0.2},
        {"model": "lr", "ci_kind": "race_cluster", "metric": "brier", "value": 0.2,
         "ci_low": 0.1, "ci_high": 0.3},
    ]  # fmt: skip
    lines = O.summary_lines(rows, 5.0, 3)
    assert lines == [
        "  G=5 k=3 lr_calibrated brier = 0.160 [0.100, 0.200] "
        "(ORACLE: realised out-lap delta, not usable at decision time)"
    ]
