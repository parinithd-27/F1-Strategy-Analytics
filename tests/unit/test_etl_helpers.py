"""Tests for the clean job's building blocks (parsers, flags, stints, guards, checksum)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from _fakes import FIXTURES
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.etl import clean
from f1sa.etl.spark import RUNTIME_CONF, configure_session
from f1sa.ingest import synthetic
from f1sa.ingest.jolpica_client import parse_laps, parse_pitstops
from f1sa.lake import schemas as S


@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        ("31.234", 31.234),
        ("1:31.234", 91.234),
        ("1:27:38.241", 5258.241),
        (" 22.688 ", 22.688),
        ("+34.388", None),
        ("1:61.0", None),
        ("1:60:00.0", None),
        ("abc", None),
        ("", None),
        (None, None),
    ],
)
def test_duration_seconds(spark: SparkSession, text: str | None, seconds: float | None) -> None:
    row = (
        spark.createDataFrame([(text,)], "t string")
        .select(clean.duration_seconds(F.col("t")))
        .first()
    )
    assert row is not None
    assert row[0] == (None if seconds is None else pytest.approx(seconds))


def test_ns_to_seconds(spark: SparkSession) -> None:
    row = (
        spark.createDataFrame([(3_601_500_000_000,)], "t long")
        .select(clean.ns_to_seconds(F.col("t")))
        .first()
    )
    assert row is not None and row[0] == pytest.approx(3601.5)


def test_configure_session_sets_runtime_conf(spark: SparkSession) -> None:
    configure_session(spark)
    for key, value in RUNTIME_CONF.items():
        assert spark.conf.get(key) == value


def test_spark_uri_and_output_guard(tmp_path: Path) -> None:
    assert (
        clean.spark_uri("file://./data/lake") == f"file://{(tmp_path / 'data' / 'lake').resolve()}"
    )
    assert clean.spark_uri("hdfs://namenode:8020/f1/") == "hdfs://namenode:8020/f1"
    with pytest.raises(clean.CleanError):
        clean.spark_uri("s3://bucket/x")
    clean.check_output_root(f"file://{tmp_path}/data/lake/clean")
    clean.check_output_root("hdfs://namenode:8020/f1/clean")  # no local guard for HDFS
    for bad in (f"file://{tmp_path}/data/sample/clean", f"file://{tmp_path.parent}/x"):
        with pytest.raises(clean.CleanError, match="refusing"):
            clean.check_output_root(bad)


def test_conform_and_select_races(spark: SparkSession) -> None:
    d = spark.createDataFrame(
        [(2023, 5, "1"), (2024, 5, "2"), (2024, 6, "3")], "season int, round int, x string"
    )
    out = clean.conform(d.withColumnRenamed("x", "y"), S._s(("season", S.INT), ("y", S.INT)), "t")
    assert out.schema.fieldNames() == ["season", "y"] and out.schema["y"].dataType == S.INT
    with pytest.raises(clean.CleanError, match="missing columns"):
        clean.conform(d, S._s(("nope", S.INT)), "t")
    assert clean.select_races(d, [2024], None).count() == 2
    assert clean.select_races(d, None, {(2024, 6), (2023, 5)}).count() == 2
    assert clean.select_races(d, None, None).count() == 3


def test_table_checksum_is_order_and_column_independent(spark: SparkSession) -> None:
    rows = [(1, "a", 1.5), (2, "b", None), (3, "c", 2.0)]
    a = spark.createDataFrame(rows, "k int, v string, f double")
    b = spark.createDataFrame(rows[::-1], "k int, v string, f double").select("f", "k", "v")
    c = spark.createDataFrame(rows[:2], "k int, v string, f double")
    assert clean.table_checksum(a) == clean.table_checksum(b)
    assert clean.table_checksum(a) != clean.table_checksum(c)
    assert clean.table_checksum(a.limit(0)) == clean.table_checksum(c.limit(0))


def _laps_df(spark: SparkSession, rows: list[tuple[Any, ...]]) -> DataFrame:
    return spark.createDataFrame(
        rows,
        "season int, round int, driver_id string, lap int, position int, lap_time_s double, "
        "is_pit_in_lap boolean, is_pit_out_lap boolean",
    )


def test_heuristic_flags_neutralised_red_and_lap_one(spark: SparkSession) -> None:
    rows = []
    for lap in range(1, 11):
        for i, d in enumerate("abcd"):
            t = 90.0 + i * 0.1
            if lap == 1:
                t = 150.0  # slow standing start: excluded, never flagged
            if lap in (5, 6):
                t = 130.0  # safety car (> 1.25x)
            if lap == 8:
                t = 400.0  # red-flag lap including the stoppage (> 3x)
            pit = d == "a" and lap == 3
            rows.append((2024, 5, d, lap, i + 1, 200.0 if pit else t, pit, False))
    flags = {r["lap"]: r for r in clean.heuristic_track_flags(_laps_df(spark, rows)).collect()}
    assert [lap for lap, r in sorted(flags.items()) if r["is_neutralised_lap"]] == [5, 6, 8]
    assert [lap for lap, r in sorted(flags.items()) if r["is_red_flag_lap"]] == [8]
    assert flags[3]["is_neutralised_lap"] is False  # one slow pit lap does not move the median
    assert flags[1]["is_neutralised_lap"] is False and flags[1]["is_red_flag_lap"] is False
    assert all(
        r["is_sc_lap"] is None and r["track_status_source"] == "heuristic" for r in flags.values()
    )


def test_spark_stint_inference_matches_python_generator(spark: SparkSession) -> None:
    pages = [json.loads((FIXTURES / f"laps_2023_5_p{i}.json").read_text()) for i in range(3)]
    pit_pages = [json.loads((FIXTURES / "pitstops_2023_5.json").read_text())]
    expected = synthetic.infer_stints(parse_laps(pages), parse_pitstops(pit_pages))
    laps = spark.createDataFrame(
        parse_laps(pages)[["season", "round", "driver_id", "lap"]].astype(object)
    )
    pits = spark.createDataFrame(
        parse_pitstops(pit_pages)[["season", "round", "driver_id", "lap"]].astype(object)
    )
    got = clean.infer_stints(
        laps.select(
            *(F.col(c).cast("int" if c != "driver_id" else "string").alias(c) for c in laps.columns)
        ),
        pits.select(
            *(F.col(c).cast("int" if c != "driver_id" else "string").alias(c) for c in pits.columns)
        ),
    )
    got_rows = sorted(
        (r["driver_id"], r["stint"], r["start_lap"], r["end_lap"]) for r in got.collect()
    )
    exp_rows = sorted(
        zip(
            expected["driver_id"],
            expected["stint"],
            expected["start_lap"],
            expected["end_lap"],
            strict=True,
        )
    )
    assert got_rows == [(d, int(s), int(a), int(b)) for d, s, a, b in exp_rows]
    assert {r["source"] for r in got.collect()} == {"jolpica_inferred"}


def test_best_source_per_race(spark: SparkSession) -> None:
    a = spark.createDataFrame(
        [(2023, 5, "x", "synthetic"), (2024, 5, "y", "synthetic")],
        "season int, round int, v string, source string",
    )
    b = spark.createDataFrame(
        [(2023, 5, "z", "fastf1")], "season int, round int, v string, source string"
    )
    best = {(r["season"], r["v"]) for r in clean._best_source_per_race(a, b).collect()}
    assert best == {(2023, "z"), (2024, "y")}  # FastF1 wins where present, synthetic elsewhere


def test_read_missing_inputs_give_empty_frames(spark: SparkSession, tmp_path: Path) -> None:
    root = f"file://{tmp_path}/raw"
    pages = clean.read_json_pages(spark, root, "laps", None)
    assert pages.count() == 0 and pages.schema == S.RAW_JSON_SCHEMAS["laps"]
    ff1 = clean.read_parquet_table(
        spark, f"{root}/fastf1/laps/season=*/round=*/laps.parquet", S.RAW_FASTF1_LAPS, "x"
    )
    assert ff1.count() == 0 and ff1.schema == S.RAW_FASTF1_LAPS


def test_write_text_roundtrip(spark: SparkSession, tmp_path: Path) -> None:
    target = tmp_path / "data" / "lake" / "clean" / "_quality" / "r.json"
    clean.write_text(spark, f"file://{target}", '{"ok": true}\n')
    assert json.loads(target.read_text()) == {"ok": True}
