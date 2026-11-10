"""Tests for lake/quality.py on small crafted clean tables."""

from __future__ import annotations

import json
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from f1sa.lake import quality as q
from f1sa.lake import schemas as S


def df(spark: SparkSession, table: str, rows: list[dict[str, Any]]) -> DataFrame:
    schema = S.CLEAN_SCHEMAS[table]
    return spark.createDataFrame(
        [tuple(r.get(f.name) for f in schema.fields) for r in rows], schema
    )


def lap(driver: str, n: int, pos: int) -> dict[str, Any]:
    return {"season": 2023, "round": 5, "driver_id": driver, "lap": n, "position": pos}


def good_tables(spark: SparkSession) -> dict[str, DataFrame]:
    laps = [lap("a", n, 1) for n in (1, 2, 3)] + [lap("b", n, 2) for n in (1, 2, 3)]
    tables = {
        "laps": df(spark, "laps", laps),
        "results": df(
            spark,
            "results",
            [
                {"season": 2023, "round": 5, "driver_id": "a", "laps": 3},
                {"season": 2023, "round": 5, "driver_id": "b", "laps": 3},
            ],
        ),
        "pitstops": df(
            spark,
            "pitstops",
            [
                {"season": 2023, "round": 5, "driver_id": "a", "stop": 1, "lap": 2},
            ],
        ),
        "stints": df(
            spark,
            "stints",
            [
                {
                    "season": 2023,
                    "round": 5,
                    "driver_id": "a",
                    "stint": 1,
                    "start_lap": 1,
                    "end_lap": 2,
                    "laps": 2,
                },
                {
                    "season": 2023,
                    "round": 5,
                    "driver_id": "a",
                    "stint": 2,
                    "start_lap": 3,
                    "end_lap": 3,
                    "laps": 1,
                },
                {
                    "season": 2023,
                    "round": 5,
                    "driver_id": "b",
                    "stint": 1,
                    "start_lap": 1,
                    "end_lap": 3,
                    "laps": 3,
                },
            ],
        ),
    }
    return tables


def names(report: q.QualityReport) -> set[str]:
    return {c.name for c in report.hard_failures + report.soft_issues}


def test_clean_tables_pass(spark: SparkSession) -> None:
    report = q.run_checks(good_tables(spark), expected_races=[(2023, 5)])
    assert report.ok and names(report) == set()
    out = report.to_dict()
    assert out["status"] == "PASS" and out["hard_failures"] == 0
    json.dumps(out)  # serialisable
    assert report.summary_lines()[0].startswith("quality: PASS")


def test_hard_null_and_duplicate_keys(spark: SparkSession) -> None:
    t = good_tables(spark)
    t["laps"] = t["laps"].unionByName(
        df(spark, "laps", [lap("a", 3, 1), {**lap("c", 1, 3), "driver_id": None}])
    )
    report = q.run_checks(t, expected_races=[(2023, 5)])
    assert not report.ok
    hard = {c.name: c for c in report.hard_failures}
    assert hard["duplicate_keys:laps"].count == 1 and hard["null_keys:laps"].count == 1
    assert hard["duplicate_keys:laps"].examples[0]["driver_id"] == "a"


def test_hard_missing_race_and_pit_lap_out_of_range(spark: SparkSession) -> None:
    t = good_tables(spark)
    t["pitstops"] = df(
        spark,
        "pitstops",
        [
            {"season": 2023, "round": 5, "driver_id": "a", "stop": 1, "lap": 9},  # > last lap + 1
            {"season": 2023, "round": 5, "driver_id": "b", "stop": 1, "lap": 4},  # last lap + 1: ok
        ],
    )
    report = q.run_checks(t, expected_races=[(2023, 5), (2023, 6)])
    hard = {c.name: c for c in report.hard_failures}
    assert set(hard) == {"races_have_results_and_laps", "pit_lap_in_range"}
    assert hard["pit_lap_in_range"].count == 1
    missing = next(c for c in report.soft_issues if c.name == "pit_lap_exists_in_laps")
    assert [e["driver_id"] for e in missing.examples] == ["a"]  # b's last-lap+1 stop is allowed


def test_soft_checks(spark: SparkSession) -> None:
    t = good_tables(spark)
    t["laps"] = df(
        spark,
        "laps",
        [lap("a", 1, 1), lap("a", 3, 1), lap("b", 1, 1), lap("b", 2, 3), lap("b", 3, 2)],
    )
    t["results"] = df(
        spark,
        "results",
        [
            {"season": 2023, "round": 5, "driver_id": "a", "laps": 5},
            {"season": 2023, "round": 5, "driver_id": "b", "laps": 3},
        ],
    )
    report = q.run_checks(t, expected_races=[(2023, 5)], unparseable_times=2)
    assert report.ok  # soft only
    soft = {c.name: c for c in report.soft_issues}
    assert set(soft) == {
        "laps_contiguous_per_driver",  # a has no lap 2
        "positions_1_to_n_per_lap",  # lap 1 has two P1s; lap 2 has only a P3
        "pit_lap_exists_in_laps",  # a's stop on lap 2 has no lap 2
        "results_laps_match_lap_table",  # a: results say 5, laps end at 3
        "time_strings_parse",
    }  # stints still tile 1..3 for both drivers
    assert soft["positions_1_to_n_per_lap"].count == 2
    assert soft["time_strings_parse"].count == 2


def test_stint_tiling(spark: SparkSession) -> None:
    t = good_tables(spark)
    t["stints"] = df(
        spark,
        "stints",
        [
            {
                "season": 2023,
                "round": 5,
                "driver_id": "a",
                "stint": 1,
                "start_lap": 1,
                "end_lap": 2,
                "laps": 2,
            },
            {
                "season": 2023,
                "round": 5,
                "driver_id": "b",
                "stint": 1,
                "start_lap": 2,
                "end_lap": 3,
                "laps": 2,
            },
        ],
    )
    report = q.run_checks(t, expected_races=[(2023, 5)])
    tiling = next(c for c in report.soft_issues if c.name == "stints_tile_laps")
    assert tiling.count == 2  # a ends early, b starts late


def test_unmapped_fastf1_is_soft(spark: SparkSession) -> None:
    unmapped = spark.createDataFrame(
        [(2023, 5, "ZZZ", "99")], "season int, round int, Driver string, DriverNumber string"
    )
    report = q.run_checks(good_tables(spark), expected_races=[(2023, 5)], unmapped_fastf1=unmapped)
    assert report.ok and names(report) == {"fastf1_drivers_mapped"}


@pytest.mark.parametrize("count,passed", [(0, True), (3, False)])
def test_check_result_passed(count: int, passed: bool) -> None:
    assert q.CheckResult("x", "soft", count).passed is passed
