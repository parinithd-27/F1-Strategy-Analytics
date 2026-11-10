"""DataFrame jobs: hand-built 3-driver, 5-lap race with known answers, plus the real sample.

The fixture race (2024 R1, start 12:00:00 UTC):

=====  ====================================  ==========================  =========================
lap    A (leader)                            B (pits on lap 3)           C (lapped)
=====  ====================================  ==========================  =========================
1      90  cum 90   P1                       91  cum 91   P2             92  cum 92   P3
2      90  cum 180  P1                       91  cum 182  P2             92  cum 184  P3
3      90  cum 270  P1                       111 cum 293  P3 (pit in)    92  cum 276  P2
4      90  cum 360  P1                       95  cum 388  P2 (out lap)   190 cum 466  P3
5      90  cum 450  P1 (flag)                91  cum 479  P2             (lapped: 466 > 450)
=====  ====================================  ==========================  =========================

C's gap to the leader at lap 4 is 466 - 360 = 106 s, compared with the leader's time at lap 4. That
is more than one lap time, which is correct for a lapped car.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from _feature_fixtures import frame, lap, pitstop, race, result
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from conftest import SampleLake
from f1sa.etl import df_jobs
from f1sa.etl.io import load_clean
from f1sa.features import schemas as FS

LAP_TIMES = {
    "A": [90.0, 90.0, 90.0, 90.0, 90.0],
    "B": [91.0, 91.0, 111.0, 95.0, 91.0],
    "C": [92.0, 92.0, 92.0, 190.0],
}
POSITIONS = {
    "A": [1, 1, 1, 1, 1],
    "B": [2, 2, 3, 2, 2],
    "C": [3, 3, 2, 3],
}
# (driver, lap) -> (gap_leader_s, gap_ahead_s, ahead_driver_id)
EXPECTED_GAPS: dict[tuple[str, int], tuple[float, float | None, str | None]] = {
    **{("A", n): (0.0, None, None) for n in range(1, 6)},
    ("B", 1): (1.0, 1.0, "A"),
    ("B", 2): (2.0, 2.0, "A"),
    ("B", 3): (23.0, 17.0, "C"),
    ("B", 4): (28.0, 28.0, "A"),
    ("B", 5): (29.0, 29.0, "A"),
    ("C", 1): (2.0, 1.0, "B"),
    ("C", 2): (4.0, 2.0, "B"),
    ("C", 3): (6.0, 6.0, "A"),
    ("C", 4): (106.0, 78.0, "B"),
}
# (driver, lap) -> (ahead 1l, ahead 3l, leader 1l, leader 3l); null when the car ahead changed
EXPECTED_CHANGES: dict[tuple[str, int], tuple[float | None, ...]] = {
    ("A", 1): (None, None, None, None),
    ("A", 2): (None, None, 0.0, None),
    ("A", 3): (None, None, 0.0, None),
    ("A", 4): (None, None, 0.0, 0.0),
    ("A", 5): (None, None, 0.0, 0.0),
    ("B", 1): (None, None, None, None),
    ("B", 2): (1.0, None, 1.0, None),
    ("B", 3): (None, None, 21.0, None),  # car ahead A -> C
    ("B", 4): (None, 27.0, 5.0, 27.0),  # ahead C -> A over 1 lap; A both at lap 1 and lap 4
    ("B", 5): (1.0, 27.0, 1.0, 27.0),
    ("C", 1): (None, None, None, None),
    ("C", 2): (1.0, None, 2.0, None),
    ("C", 3): (None, None, 2.0, None),  # ahead B -> A
    ("C", 4): (None, 77.0, 100.0, 104.0),
}
EXPECTED_TYRE_AGE = {
    **{("A", n): n for n in range(1, 6)},
    ("B", 1): 3,
    ("B", 2): 4,
    ("B", 3): 5,  # stint 1 started on 2-lap-old tyres
    ("B", 4): 1,  # new tyres after the stop on lap 3
    ("B", 5): 2,
    **{("C", n): n for n in range(1, 5)},
}


def _laps(spark: SparkSession, times: dict[str, list[float | None]] | None = None) -> DataFrame:
    rows = []
    for driver, lap_times in (times or LAP_TIMES).items():
        for i, t in enumerate(lap_times, start=1):
            rows.append(
                lap(
                    2024,
                    1,
                    driver,
                    i,
                    POSITIONS[driver][i - 1],
                    t,
                    pit_in=(driver, i) == ("B", 3),
                    pit_out=(driver, i) == ("B", 4),
                )
            )
    return frame(spark, "laps", rows)


def _tables(spark: SparkSession, start: datetime | None = None) -> dict[str, DataFrame]:
    teams = {"A": "t1", "B": "t2", "C": "t3"}
    return {
        "laps": _laps(spark),
        "results": frame(
            spark,
            "results",
            [result(2024, 1, d, teams[d], i + 1, len(LAP_TIMES[d])) for i, d in enumerate("ABC")],
        ),
        "pitstops": frame(spark, "pitstops", [pitstop(2024, 1, "B", 1, 3)]),
        "drivers": frame(
            spark,
            "drivers",
            [{"season": 2024, "round": 1, "driver_id": d, "code": d * 3} for d in "ABC"],
        ),
        "constructors": frame(
            spark,
            "constructors",
            [
                {"season": 2024, "round": 1, "constructor_id": t, "name": t.upper()}
                for t in teams.values()
            ],
        ),
        "stints": frame(
            spark,
            "stints",
            [
                _stint("A", 1, 1, 5, "SOFT", 0),
                _stint("B", 1, 1, 3, "MEDIUM", 2),
                _stint("B", 2, 4, 5, "HARD", 0),
                _stint("C", 1, 1, 4, None, None),
            ],
        ),
        "races": frame(spark, "races", [race(2024, 1, "testring", start)]),
    }


def _stint(
    driver: str, n: int, start: int, end: int, compound: str | None, age: int | None
) -> dict[str, Any]:
    return {
        "season": 2024,
        "round": 1,
        "driver_id": driver,
        "stint": n,
        "start_lap": start,
        "end_lap": end,
        "laps": end - start + 1,
        "compound": compound,
        "tyre_age_at_start": age,
        "is_synthetic": False,
        "source": "jolpica_inferred" if compound is None else "synthetic",
    }


def _enriched(spark: SparkSession, start: datetime | None = None) -> dict[tuple[str, int], Any]:
    t = _tables(spark, start)
    df = df_jobs.enrich_laps(
        t["laps"],
        t["results"],
        t["pitstops"],
        t["drivers"],
        t["constructors"],
        t["stints"],
        t["races"],
    ).withColumn("end_txt", F.date_format("lap_end_utc", "yyyy-MM-dd HH:mm:ss.SSS"))
    df = df.withColumn("start_txt", F.date_format("lap_start_utc", "yyyy-MM-dd HH:mm:ss.SSS"))
    return {(r["driver_id"], r["lap"]): r for r in df.collect()}


def test_gaps_known_answers_with_lapped_car_and_pit_lap(spark: SparkSession) -> None:
    rows = _enriched(spark)
    assert set(rows) == set(EXPECTED_GAPS)
    for key, (leader, ahead, ahead_id) in EXPECTED_GAPS.items():
        r = rows[key]
        assert r["gap_leader_s"] == leader, key
        assert r["gap_ahead_s"] == ahead, key
        assert r["ahead_driver_id"] == ahead_id, key
    assert rows[("C", 4)]["gap_leader_s"] > max(LAP_TIMES["A"])  # lapped: more than one lap


def test_gap_changes_known_answers(spark: SparkSession) -> None:
    rows = _enriched(spark)
    for key, expected in EXPECTED_CHANGES.items():
        r = rows[key]
        got = (
            r["gap_ahead_change_1l"],
            r["gap_ahead_change_3l"],
            r["gap_leader_change_1l"],
            r["gap_leader_change_3l"],
        )
        assert got == expected, key


def test_tyre_age_pit_and_laps_remaining(spark: SparkSession) -> None:
    rows = _enriched(spark)
    for key, age in EXPECTED_TYRE_AGE.items():
        assert rows[key]["tyre_age_laps"] == age, key
    assert rows[("B", 3)]["stint"] == 1 and rows[("B", 4)]["stint"] == 2
    assert rows[("B", 4)]["compound"] == "HARD"
    assert rows[("C", 2)]["tyre_age_assumed_new"] is True
    assert rows[("B", 2)]["tyre_age_assumed_new"] is False
    b3 = rows[("B", 3)]
    assert (b3["pit_stops_in_lap"], b3["pit_duration_s"], b3["is_pit_in_lap"]) == (1, 22.5, True)
    assert rows[("B", 2)]["pit_stops_in_lap"] == 0
    assert all(rows[(d, n)]["laps_remaining"] == 5 - n for d, n in rows)
    assert rows[("C", 4)]["race_laps"] == 5  # the winner's distance, not C's
    assert rows[("A", 1)]["constructor_name"] == "T1" and rows[("C", 1)]["driver_code"] == "CCC"
    assert rows[("C", 1)]["final_position"] == 3 and rows[("C", 1)]["finished"] is True


def test_lap_timestamps_from_cumulative_time_plus_offset(spark: SparkSession) -> None:
    assert df_jobs.START_OFFSET_S == 180.0
    rows = _enriched(spark)
    # 12:00:00 + 180 s offset + cumulative
    assert rows[("A", 1)]["start_txt"] == "2024-01-01 12:03:00.000"
    assert rows[("A", 1)]["end_txt"] == "2024-01-01 12:04:30.000"
    assert rows[("C", 4)]["end_txt"] == "2024-01-01 12:10:46.000"  # 180 + 466
    assert rows[("C", 4)]["start_txt"] == "2024-01-01 12:07:36.000"  # minus the 190 s lap
    assert all(r["lap_time_estimated"] for r in rows.values())


def test_unknown_start_time_gives_null_timestamps(spark: SparkSession) -> None:
    rows = _enriched(spark, start=datetime(2024, 1, 1, 0, 0, tzinfo=UTC))
    assert all(r["lap_end_utc"] is None and r["lap_start_utc"] is None for r in rows.values())


def test_missing_lap_time_nulls_cumulative_and_gaps_from_that_lap(spark: SparkSession) -> None:
    times: dict[str, list[float | None]] = {
        "A": [90.0, 90.0, 90.0, 90.0, 90.0],
        "B": [91.0, None, 111.0, 95.0, 91.0],
        "C": [92.0, 92.0, 92.0, 190.0],
    }
    rows = {
        (r["driver_id"], r["lap"]): r for r in df_jobs.timing_gaps(_laps(spark, times)).collect()
    }
    assert rows[("B", 1)]["cumulative_time_s"] == 91.0
    assert all(rows[("B", n)]["cumulative_time_s"] is None for n in range(2, 6))
    assert all(rows[("B", n)]["gap_leader_s"] is None for n in range(2, 6))
    # C is behind B on lap 2: its gap ahead depends on B's unknown time
    assert rows[("C", 2)]["gap_ahead_s"] is None and rows[("C", 2)]["gap_leader_s"] == 4.0


def test_pit_per_lap_two_stops_in_one_lap_do_not_duplicate(spark: SparkSession) -> None:
    stops = frame(
        spark,
        "pitstops",
        [pitstop(2024, 1, "B", 1, 3, 20.0), pitstop(2024, 1, "B", 2, 3, 70.0)],
    )
    per_lap = df_jobs.pit_per_lap(stops).collect()
    assert len(per_lap) == 1
    assert (per_lap[0]["pit_stops_in_lap"], per_lap[0]["pit_duration_s"]) == (2, 90.0)
    assert per_lap[0]["is_long_stop"] is True
    t = _tables(spark)
    out = df_jobs.enrich_laps(
        t["laps"], t["results"], stops, t["drivers"], t["constructors"], t["stints"], t["races"]
    )
    assert out.count() == t["laps"].count()


def test_race_distance_falls_back_to_max_lap(spark: SparkSession) -> None:
    t = _tables(spark)
    no_winner = t["results"].where(F.col("position") != 1)
    (row,) = df_jobs.race_distance(no_winner, t["laps"]).collect()
    assert row["race_laps"] == 5
    (row,) = df_jobs.race_distance(t["results"], t["laps"]).collect()
    assert row["race_laps"] == 5


def test_tyre_age_lap_without_stint_is_null(spark: SparkSession) -> None:
    t = _tables(spark)
    only_a = t["stints"].where(F.col("driver_id") == "A")
    rows = {(r["driver_id"], r["lap"]): r for r in df_jobs.tyre_age(t["laps"], only_a).collect()}
    assert rows[("B", 1)]["tyre_age_laps"] is None and rows[("B", 1)]["stint"] is None
    assert rows[("B", 1)]["tyre_age_assumed_new"] is None
    assert rows[("A", 5)]["tyre_age_laps"] == 5


def test_lap_timestamps_needs_cumulative(spark: SparkSession) -> None:
    t = _tables(spark)
    with_cum = df_jobs.timing_gaps(t["laps"])
    out = df_jobs.lap_timestamps(with_cum, t["races"])
    assert out.count() == t["laps"].count()
    assert "lap_end_utc" in out.columns and "_start" not in out.columns


def test_sample_enriched_matches_clean_gaps(
    spark: SparkSession, sample_clean_lake: SampleLake
) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root)
    out = df_jobs.enrich_laps(
        c["laps"],
        c["results"],
        c["pitstops"],
        c["drivers"],
        c["constructors"],
        c["stints"],
        c["races"],
    )
    assert [(f.name, f.dataType) for f in out.schema] == [
        (f.name, f.dataType) for f in FS.LAPS_ENRICHED
    ]
    assert out.count() == c["laps"].count() == 2168
    j = out.alias("e").join(c["laps"].alias("c"), ["season", "round", "driver_id", "lap"])
    diff = j.where(
        ~F.col("e.gap_leader_s").eqNullSafe(F.col("c.gap_to_leader_s"))
        | ~F.col("e.gap_ahead_s").eqNullSafe(F.col("c.gap_ahead_s"))
        | ~F.col("e.cumulative_time_s").eqNullSafe(F.col("c.cumulative_time_s"))
    )
    assert diff.count() == 0
    assert out.where(F.col("tyre_age_laps").isNull() | F.col("compound").isNull()).count() == 0
    assert out.where(F.col("laps_remaining") < 0).count() == 0
