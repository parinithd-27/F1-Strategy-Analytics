"""TracingInsights per-lap telemetry aggregates: equality with the clean FastF1 path, boundary
de-duplication and the coverage flag."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from f1sa.etl import clean
from f1sa.features import telemetry_agg, telemetry_agg_ti
from f1sa.features.telemetry_agg_ti import aggregate_race, session_samples
from f1sa.ingest import tracinginsights_parse as P

RACE = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "tracinginsights"
    / "Testland Grand Prix"
    / "Race"
)
AGG = (
    "mean_speed_kph",
    "max_speed_kph",
    "pct_full_throttle",
    "brake_fraction",
    "gear_changes",
    "mean_rpm",
)


def _laps() -> pd.DataFrame:
    return P.parse_laps((RACE / "session_laptimes.json").read_bytes()).frame


def _telemetry() -> pd.DataFrame:
    frames = [
        P.parse_telemetry(f.read_bytes(), f.parent.name, P.telemetry_lap_number(f.name)).frame
        for f in sorted(RACE.glob("*/*_tel.json"))
    ]
    return pd.concat(frames, ignore_index=True)


def _fastf1_path(spark: SparkSession, samples: pd.DataFrame, laps: pd.DataFrame) -> pd.DataFrame:
    """The same continuous sample stream through ``etl.clean``'s FastF1 functions."""
    car_schema = T.StructType(
        [
            T.StructField("season", T.IntegerType()),
            T.StructField("round", T.IntegerType()),
            T.StructField("driver_id", T.StringType()),
            T.StructField("SessionTime", T.LongType()),
            T.StructField("Driver", T.StringType()),
            T.StructField("Date", T.LongType()),
            T.StructField("Speed", T.DoubleType()),
            T.StructField("Throttle", T.DoubleType()),
            T.StructField("Brake", T.BooleanType()),
            T.StructField("nGear", T.IntegerType()),
            T.StructField("RPM", T.DoubleType()),
        ]
    )
    car = [
        (
            2024,
            1,
            r.driver_code,
            round(r.t * 1e9),
            r.driver_code,
            0,
            float(r.speed_kph),
            float(r.throttle_pct),
            bool(r.brake),
            int(r.n_gear),
            float(r.rpm),
        )
        for r in samples.itertuples()
    ]
    lap_schema = T.StructType(
        [
            T.StructField("season", T.IntegerType()),
            T.StructField("round", T.IntegerType()),
            T.StructField("driver_id", T.StringType()),
            T.StructField("LapNumber", T.DoubleType()),
            T.StructField("LapStartTime", T.LongType()),
            T.StructField("Time", T.LongType()),
        ]
    )
    windows = [
        (
            2024,
            1,
            r.driver_code,
            float(r.lap),
            round(r.lap_start_session_s * 1e9),
            round(r.lap_end_session_s * 1e9),
        )
        for r in laps.itertuples()
    ]
    ff1 = clean.fastf1_samples(
        spark.createDataFrame(car, car_schema), spark.createDataFrame(windows, lap_schema)
    )
    return clean.fastf1_telemetry_lap(ff1).toPandas()


def test_equal_to_the_clean_fastf1_path(spark: SparkSession) -> None:
    laps, tel = _laps(), _telemetry()
    ti = aggregate_race(spark, 2024, 1, tel, laps).frame
    stream, _ = session_samples(tel, laps)  # one continuous stream, as FastF1 car data is
    ff1 = _fastf1_path(spark, stream, laps)
    got = ti[ti.n_samples > 0].astype({"lap": "int64"})
    got = got.set_index(["driver_code", "lap"])[list(AGG)].sort_index()
    want = ff1.rename(columns={"driver_id": "driver_code"}).astype({"lap": "int64"})
    want = want.set_index(["driver_code", "lap"])
    want = want[list(AGG)].sort_index()
    assert len(got) == 10 and got.index.equals(want.index)
    pd.testing.assert_frame_equal(got.astype(float), want.astype(float), check_exact=True)


def test_exact_values_and_boundary_rule(spark: SparkSession) -> None:
    result = aggregate_race(spark, 2024, 1, _telemetry(), _laps())
    f = result.frame.set_index(["driver_code", "lap"])
    assert result.n_duplicate_samples == 8  # 4 shared boundaries for each of AAA and BBB
    assert result.n_samples_outside_window == 2  # the last sample of each final lap
    # AAA lap 1: samples at 0..75 s (the 90 s sample is lap 2's first): 6 samples
    row = f.loc[("AAA", 1)]
    assert row["n_samples"] == 6
    assert row["max_speed_kph"] == 215.0 and row["mean_speed_kph"] == 212.5
    # throttle 100, 97.9, 98.0, 0, 100, 99.5 -> 4 of 6 at >= 98
    assert row["pct_full_throttle"] == round(4 / 6, 4)
    assert row["brake_fraction"] == round(1 / 6, 4)
    assert row["gear_changes"] == 3  # 7,7,8,3,3,7
    # the final lap's 90 s sample lies at lap_end (half-open window): 6 samples
    assert f.loc[("AAA", 5), "n_samples"] == 6


# The fixture samples every 15 s (real data: ~0.13 s), so its in-window samples span at most
# 75 of 90 s and its gaps are 15 s: the rules are tested with fixture-sized thresholds.
FIXTURE_GAP_S = 20.0
FIXTURE_SPAN = 0.8


def test_default_gap_threshold_flags_the_coarse_fixture(spark: SparkSession) -> None:
    f = aggregate_race(spark, 2024, 1, _telemetry(), _laps()).frame
    assert set(f[f.n_samples > 0]["telemetry_coverage"]) == {"partial"}


def test_coverage_flag(spark: SparkSession) -> None:
    f = aggregate_race(
        spark, 2024, 1, _telemetry(), _laps(), max_gap_s=FIXTURE_GAP_S, min_span_share=FIXTURE_SPAN
    ).frame.set_index(["driver_code", "lap"])
    assert f.loc[("AAA", 4), "telemetry_coverage"] == "partial"  # 24 s gap
    assert (f.xs("CCC")["telemetry_coverage"] == "none").all()
    assert (f.xs("CCC")["n_samples"] == 0).all() and f.xs("CCC")["mean_speed_kph"].isna().all()
    others = f.drop(index=[("AAA", 4)]).drop(index="CCC", level=0)
    assert (others["telemetry_coverage"] == "full").all()
    assert len(f) == 15  # every lap window gets a row


def test_span_rule_marks_short_coverage_partial(spark: SparkSession) -> None:
    laps = _laps()
    tel = _telemetry()
    # BBB lap 3: keep samples 15..45 s only -> span 30 of 90 s; gaps to the edges too
    keep = ~(
        (tel.driver_code == "BBB") & (tel.lap == 3) & ~tel.time_in_lap_s.isin([15.0, 30.0, 45.0])
    )
    tel = tel[keep]
    tel = tel[~((tel.driver_code == "BBB") & (tel.lap == 2) & (tel.time_in_lap_s == 90.0))]
    f = aggregate_race(
        spark, 2024, 1, tel, laps, max_gap_s=100.0, min_span_share=FIXTURE_SPAN
    ).frame
    f = f.set_index(["driver_code", "lap"])
    assert f.loc[("BBB", 3), "telemetry_coverage"] == "partial"  # span rule alone
    assert f.loc[("BBB", 4), "telemetry_coverage"] == "full"
    assert telemetry_agg_ti.MIN_SPAN_SHARE == 0.90 and telemetry_agg_ti.MAX_GAP_S == 2.0


def test_without_dedup_the_boundary_sample_would_count_twice() -> None:
    laps, tel = _laps(), _telemetry()
    deduped, n_dup = session_samples(tel, laps)
    assert n_dup == 8 and len(deduped) == len(tel) - 8
    assert not deduped.duplicated(["driver_code", "t"]).any()
    # the kept boundary copy is the later lap's time-0 sample, whatever the input order
    reversed_, _ = session_samples(tel.iloc[::-1].reset_index(drop=True), laps)
    pd.testing.assert_frame_equal(deduped, reversed_)
    at_1090 = deduped[(deduped.driver_code == "AAA") & (deduped.t == 1090.0)]
    assert list(at_1090["lap"]) == [2] and list(at_1090["time_in_lap_s"]) == [0.0]


def test_samples_of_a_lap_without_start_time_are_dropped() -> None:
    laps = _laps()
    laps.loc[(laps.driver_code == "AAA") & (laps.lap == 2), "lap_start_session_s"] = pd.NA
    tel = _telemetry()
    samples, _ = session_samples(tel, laps)
    assert not ((samples.driver_code == "AAA") & (samples.lap == 2)).any()


def test_definitions_are_reused_not_reimplemented() -> None:
    source = inspect.getsource(telemetry_agg_ti)
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "f1sa.features.telemetry_agg"
        for alias in node.names
    }
    assert {"aggregate_samples", "assign_to_laps"} <= imported
    numbers = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)}
    assert telemetry_agg.FULL_THROTTLE_PCT not in numbers and 98 not in numbers
    assert "groupBy" in source  # only for the coverage flag, not for the aggregates
    assert "F.avg" not in source and 'F.max("speed' not in source


@pytest.mark.parametrize("column", ["speed_kph", "throttle_pct", "brake", "n_gear", "rpm"])
def test_null_channel_values_are_ignored_like_fastf1(spark: SparkSession, column: str) -> None:
    laps, tel = _laps(), _telemetry()
    tel.loc[tel.index[0], column] = pd.NA
    out = aggregate_race(spark, 2024, 1, tel, laps).frame
    assert len(out) == 15


def test_integer_columns_stay_integer_with_no_sample_laps(
    spark: SparkSession, tmp_path: Path
) -> None:
    """A lap with no samples has null gear_changes; Parquet must still store it as INT32 so the
    clean job can read it with RAW_TELEMETRY_LAP_AGG (regression: 2021 R8 was written as DOUBLE)."""
    from f1sa.lake.schemas_tracinginsights import RAW_TELEMETRY_LAP_AGG

    f = aggregate_race(spark, 2024, 1, _telemetry(), _laps()).frame
    assert f.loc[f.driver_code == "CCC", "gear_changes"].isna().all()
    assert str(f["gear_changes"].dtype) == "Int32"
    assert str(f["n_samples"].dtype) == "Int32"
    assert str(f["lap"].dtype) == "Int32"  # IntegerType in Spark, INT32 as before the fix
    path = tmp_path / "agg.parquet"
    f.to_parquet(path, index=False, engine="pyarrow")
    back = spark.read.schema(RAW_TELEMETRY_LAP_AGG).parquet(str(path)).toPandas()
    assert len(back) == len(f)
    assert back["gear_changes"].isna().sum() == f["gear_changes"].isna().sum()
    pd.testing.assert_series_equal(
        back["gear_changes"].dropna().astype(int).reset_index(drop=True),
        f["gear_changes"].dropna().astype(int).reset_index(drop=True),
    )


def test_with_nullable_ints_casts_only_integer_fields() -> None:
    schema = T.StructType(
        [
            T.StructField("a", T.IntegerType()),
            T.StructField("b", T.LongType()),
            T.StructField("c", T.DoubleType()),
            T.StructField("d", T.StringType()),
        ]
    )
    pdf = pd.DataFrame({"a": [1.0, None], "b": [2.0, 3.0], "c": [0.5, None], "d": ["x", None]})
    out = telemetry_agg_ti._with_nullable_ints(pdf, schema)
    assert [str(t) for t in out.dtypes] == ["Int32", "Int64", "float64", "object"]
    assert out["a"].tolist()[0] == 1 and pd.isna(out["a"].tolist()[1])
    assert str(pdf["a"].dtype) == "float64"  # input untouched
