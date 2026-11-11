"""Full-data report: counts, constructors, neutralisation, provenance, labelled splits."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from f1sa.etl import full_report as R
from f1sa.features.build import BuildError
from f1sa.features.undercut import OUTCOME_CATEGORIES
from f1sa.model import features as MF
from f1sa.model.dataset import SplitSpec


def _results(spark: SparkSession, rows: list[tuple[int, int, str, str]]) -> DataFrame:
    return spark.createDataFrame(
        rows, "season int, round int, driver_id string, constructor_id string"
    )


def _laps(spark: SparkSession, rows: list[tuple[int, int, str, int, bool, bool]]) -> DataFrame:
    return spark.createDataFrame(
        rows,
        "season int, round int, driver_id string, lap int, "
        "is_neutralised_lap boolean, is_red_flag_lap boolean",
    )


def _pits(spark: SparkSession, rows: list[tuple[int, int, str]]) -> DataFrame:
    return spark.createDataFrame(rows, "season int, round int, driver_id string")


def test_season_summary_counts_races_and_rows(spark: SparkSession) -> None:
    results = _results(spark, [(2021, 1, "a", "x"), (2021, 2, "a", "x"), (2022, 1, "a", "x")])
    laps = _laps(spark, [(2021, 1, "a", 1, False, False), (2021, 1, "a", 2, False, False)])
    assert R.season_summary({"results": results, "laps": laps}) == [
        {"season": 2021, "races": 2, "results": 2, "laps": 2},
        {"season": 2022, "races": 1, "results": 1, "laps": 0},
    ]


def test_race_summary_handles_missing_laps_and_pits(spark: SparkSession) -> None:
    results = _results(spark, [(2021, 1, "a", "x"), (2021, 1, "b", "x"), (2021, 2, "a", "x")])
    laps = _laps(spark, [(2021, 1, "a", 1, False, False), (2021, 1, "a", 2, False, False)])
    rows = R.race_summary(results, laps, _pits(spark, [(2021, 1, "a")]))
    assert rows == [
        {
            "season": 2021,
            "round": 1,
            "drivers": 2,
            "drivers_with_laps": 1,
            "race_laps": 2,
            "lap_rows": 2,
            "pit_stops": 1,
        },
        {
            "season": 2021,
            "round": 2,
            "drivers": 1,
            "drivers_with_laps": 0,
            "race_laps": 0,
            "lap_rows": 0,
            "pit_stops": 0,
        },
    ]


def test_unexpected_seasons_bounds_are_inclusive() -> None:
    seasons = [{"season": s, "races": n} for s, n in [(1, 21), (2, 22), (3, 24), (4, 25)]]
    assert R.unexpected_seasons(seasons) == [
        {"season": 1, "races": 21},
        {"season": 4, "races": 25},
    ]


def test_unexpected_races_lists_every_reason() -> None:
    good = {
        "season": 2021,
        "round": 1,
        "drivers": 20,
        "drivers_with_laps": 20,
        "race_laps": 50,
        "lap_rows": 1000,
        "pit_stops": 30,
    }
    bad = {**good, "round": 2, "drivers": 19, "drivers_with_laps": 18, "pit_stops": 0}
    out = R.unexpected_races([good, bad])
    assert [r["round"] for r in out] == [2]
    assert out[0]["reasons"] == [
        "19 drivers in results",
        "18 of 19 drivers have laps",
        "no pit stops",
    ]


@pytest.mark.parametrize(
    ("constructor", "status"),
    [("alfa", "lineage"), ("sauber", "stable"), ("kick_sauber", "unknown")],
)
def test_constructor_status(constructor: str, status: str) -> None:
    assert R.constructor_status(constructor) == status


def test_constructors_by_season_flags_unknown(spark: SparkSession) -> None:
    results = _results(
        spark, [(2024, 1, "a", "racing_bulls"), (2024, 1, "b", "rb"), (2024, 2, "a", "rb")]
    )
    assert R.constructors_by_season(results) == [
        {"season": 2024, "constructor_id": "racing_bulls", "status": "unknown"},
        {"season": 2024, "constructor_id": "rb", "status": "stable"},
    ]


def test_neutralisation_counts_race_laps_not_driver_laps(spark: SparkSession) -> None:
    laps = _laps(
        spark,
        [
            (2021, 1, "a", 1, False, False),
            (2021, 1, "b", 1, False, False),
            (2021, 1, "a", 2, True, False),
            (2021, 1, "b", 2, True, False),
            (2021, 1, "a", 3, True, True),
            (2021, 1, "a", 4, False, False),
        ],
    )
    assert R.neutralisation_by_race(laps) == [
        {
            "season": 2021,
            "round": 1,
            "race_laps": 4,
            "neutralised_laps": 2,
            "red_flag_laps": 1,
            "neutralised_share": 0.5,
        }
    ]


def _shares(values: list[float]) -> list[dict[str, Any]]:
    return [{"season": 2021, "round": i, "neutralised_share": v} for i, v in enumerate(values, 1)]


def test_outliers_use_tukey_fences_and_list_zero_shares() -> None:
    out = R.neutralisation_outliers(_shares([0.0, 0.10, 0.10, 0.11, 0.12, 0.12, 0.9]))
    assert [r["round"] for r in out["high"]] == [7]
    assert [r["round"] for r in out["low"]] == [1]
    assert [r["round"] for r in out["zero"]] == [1]


@pytest.mark.parametrize("values", [[], [0.2]])
def test_outliers_with_too_few_races(values: list[float]) -> None:
    out = R.neutralisation_outliers(_shares(values))
    assert out["high"] == out["low"] == out["zero"] == []


def _provenance_inputs(spark: SparkSession, **override: Any) -> list[DataFrame]:
    stint = override.get("stint", ("jolpica_inferred", False, None))
    lap = override.get("lap", (None, 3, True))
    tel = override.get("telemetry", [])
    attempt = override.get("attempt", (None, None, False))
    return [
        spark.createDataFrame([stint], "source string, is_synthetic boolean, compound string"),
        spark.createDataFrame(
            [lap], "compound string, tyre_age_laps int, tyre_age_assumed_new boolean"
        ),
        spark.createDataFrame(tel, "x int"),
        spark.createDataFrame(tel, "x int"),
        spark.createDataFrame(
            [attempt],
            "attacker_compound string, defender_compound string, compound_is_synthetic boolean",
        ),
    ]


def test_provenance_passes_on_jolpica_only_data(spark: SparkSession) -> None:
    checks = R.check_provenance(*_provenance_inputs(spark))
    assert len(checks) == 9
    assert all(c.passed for c in checks)


@pytest.mark.parametrize(
    ("override", "failing"),
    [
        ({"stint": ("synthetic", False, None)}, {"stints_all_inferred"}),
        ({"stint": ("jolpica_inferred", True, None)}, {"stints_not_synthetic"}),
        ({"stint": ("jolpica_inferred", None, None)}, {"stints_not_synthetic"}),
        ({"stint": ("jolpica_inferred", False, "SOFT")}, {"stint_compound_null"}),
        ({"lap": ("SOFT", 3, True)}, {"lap_compound_null"}),
        ({"lap": (None, 3, False)}, {"tyre_age_flagged_assumed_new"}),
        (
            {"telemetry": [(1,)]},
            {"clean_telemetry_absent", "feature_telemetry_absent"},
        ),
        ({"attempt": ("SOFT", None, False)}, {"attempt_compound_null"}),
        ({"attempt": (None, None, True)}, {"attempts_not_synthetic"}),
        ({"attempt": (None, None, None)}, {"attempts_not_synthetic"}),
    ],
)
def test_each_provenance_failure_is_caught(
    spark: SparkSession, override: dict[str, Any], failing: set[str]
) -> None:
    checks = R.check_provenance(*_provenance_inputs(spark, **override))
    assert {c.name for c in checks if not c.passed} == failing


def test_lap_without_tyre_age_needs_no_flag(spark: SparkSession) -> None:
    checks = R.check_provenance(*_provenance_inputs(spark, lap=(None, None, None)))
    assert all(c.passed for c in checks)


def test_real_feature_set_has_no_synthetic_capable_input() -> None:
    assert R.real_feature_set_violations("real") == []
    assert R.real_feature_set_violations("with_synthetic")
    columns = MF.assembled_columns("real")
    assert not [c for c in columns if "compound" in c or "tyre_age" in c or "speed" in c]


def _count_row(season: int, g: str, k: int, labelled: int) -> dict[str, str]:
    row = {
        "season": str(season),
        "gap_threshold_s": g,
        "response_window_laps": str(k),
        "candidates": str(labelled * 2),
        "labelled": str(labelled),
    }
    return {**row, **{f"n_{c}": "0" for c in OUTCOME_CATEGORIES}}


COUNTS = [
    _count_row(2021, "3.5", 2, 40),
    _count_row(2022, "3.5", 2, 60),
    _count_row(2023, "3.5", 2, 70),
    _count_row(2024, "3.5", 2, 50),
    _count_row(2025, "3.5", 2, 49),
    _count_row(2021, "5.0", 2, 999),
    _count_row(2021, "3.5", 3, 999),
]


def test_rows_at_selects_one_setting() -> None:
    assert len(R.rows_at(COUNTS, 3.5, 2)) == 5
    assert R.rows_at(COUNTS, 2.5, 1) == []


def test_split_counts_sum_the_chronological_seasons() -> None:
    assert R.split_counts(COUNTS, 3.5, 2) == {"train": 170, "validation": 50, "test": 49}
    custom = SplitSpec(train=(2021,), validation=(2022,), test=())
    assert R.split_counts(COUNTS, 3.5, 2, custom) == {"train": 40, "validation": 60, "test": 0}


def test_seasons_below_flags_low_and_missing_seasons() -> None:
    seasons = [2021, 2022, 2023, 2024, 2025, 2026]
    assert R.seasons_below(COUNTS, 3.5, 2, seasons) == [2021, 2025, 2026]


def test_read_counts_csv_never_returns_success_rates(tmp_path: Path) -> None:
    path = tmp_path / "c.csv"
    row = {**_count_row(2021, "3.5", 2, 7), "success": "3", "fail": "4", "success_rate": "0.4286"}
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    (out,) = R.read_counts_csv(path)
    assert out["labelled"] == "7"
    assert not {"success", "fail", "success_rate"} & set(out)


def test_read_counts_csv_missing_file(tmp_path: Path) -> None:
    with pytest.raises(R.ReportError, match="features-full"):
        R.read_counts_csv(tmp_path / "absent.csv")


def test_read_text_keeps_the_whole_file(spark: SparkSession, tmp_path: Path) -> None:
    path = tmp_path / "r.json"
    path.write_text('{\n "a": 1\n}\n', encoding="utf-8")
    assert json.loads(R.read_text(spark, f"file://{path}")) == {"a": 1}


def test_write_csv_is_ordered_and_atomic(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "o.csv"
    R.write_csv([{"b": 2, "a": 1}], ("a", "b"), path)
    assert path.read_bytes() == b"a,b\n1,2\n"
    assert list(path.parent.iterdir()) == [path]


def _report(**override: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "quality": {"hard_failures": 0},
        "constructors": [{"status": "stable"}],
        "provenance": [{"count": 0}],
        "real_feature_set": {"passed": True},
    }
    return {**base, **override}


@pytest.mark.parametrize(
    "override",
    [
        {"quality": {"hard_failures": 1}},
        {"constructors": [{"status": "unknown"}]},
        {"provenance": [{"count": 2}]},
        {"real_feature_set": {"passed": False}},
    ],
)
def test_report_ok_fails_on_each_failure(override: dict[str, Any]) -> None:
    assert R.report_ok(_report())
    assert not R.report_ok(_report(**override))


def test_main_refuses_results_dir_outside_results(capsys: pytest.CaptureFixture[str]) -> None:
    assert R.main(["--results-dir", "/tmp"]) == 2
    assert "outside" in capsys.readouterr().err


def test_run_report_refuses_results_dir_outside_results(spark: SparkSession) -> None:
    with pytest.raises(BuildError, match="outside"):
        R.run_report(spark, object(), results_dir=Path("/tmp"))  # type: ignore[arg-type]
