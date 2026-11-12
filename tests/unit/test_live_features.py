"""Live feature ports equal their batch originals (Spark numerics, timing, restart, rejoin)."""

from __future__ import annotations

import random
import subprocess
import sys
from typing import Any

import pytest
from _feature_fixtures import frame
from _undercut_fixtures import Car, Scenario, race_data, standard_cars
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from f1sa.etl.df_jobs import timing_gaps
from f1sa.features import undercut as U
from f1sa.model import features as MF
from f1sa.streaming import live_features as LF


def test_constants_and_input_order_equal_the_batch() -> None:
    assert LF.TRAFFIC_WINDOW_S == U.TRAFFIC_WINDOW_S
    assert LF.RESTART_BUFFER_LAPS == U.RESTART_BUFFER_LAPS
    assert LF.RED_FLAG_RESTART_LAPS == U.RED_FLAG_RESTART_LAPS
    assert tuple(s.name for s in MF.specs_for("real")) == LF.INPUT_NAMES


# ------------------------------------------------------------------ Spark numerics
def test_spark_round_equals_spark(spark: SparkSession) -> None:
    rnd = random.Random(3)
    vals = [rnd.uniform(-300, 300) for _ in range(3000)]
    vals += [round(rnd.uniform(60, 120), 3) - round(rnd.uniform(60, 120), 3) for _ in range(3000)]
    vals += [0.0005, 0.0015, 2.675, -2.675, 0.125, -0.125, 89.9995, 0.1 + 0.2, 1e-7, -0.0]
    df = spark.createDataFrame([(v,) for v in vals], "x double")
    rows = df.select("x", F.round("x", 3).alias("r3"), F.round("x", 4).alias("r4")).collect()
    for r in rows:
        assert LF.spark_round(r["x"], 3) == r["r3"], r["x"]
        assert LF.spark_round(r["x"], 4) == r["r4"], r["x"]
    assert LF.spark_round(None, 3) is None
    assert LF.spark_round(2.0005, 3) == 2.001  # HALF_UP, where Python's round gives 2.0


def test_regr_slope_equals_spark_bit_for_bit(spark: SparkSession) -> None:
    rnd = random.Random(11)
    data = []
    for g in range(400):
        for i in range(rnd.randint(1, 25)):
            y = round(rnd.uniform(80, 100), 3) if rnd.random() > 0.1 else None
            x = float(rnd.randint(0, 40)) if rnd.random() > 0.05 else None
            data.append((g, i, y, x))
    df = spark.createDataFrame(data, "g int, i int, y double, x double")
    w = Window.partitionBy("g").orderBy("i").rowsBetween(Window.unboundedPreceding, 0)
    got: dict[int, list[Any]] = {}
    for r in sorted(
        df.withColumn("s", F.regr_slope("y", "x").over(w)).collect(), key=lambda r: (r.g, r.i)
    ):
        got.setdefault(r.g, []).append(r)
    for rows in got.values():
        pts: list[tuple[float | None, float | None]] = []
        for r in rows:
            pts.append((r.y, r.x))
            assert LF.regr_slope(pts) == r.s


def test_regr_slope_edge_cases() -> None:
    assert LF.regr_slope([]) is None
    assert LF.regr_slope([(1.0, 2.0)]) is None  # m2 == 0
    assert LF.regr_slope([(1.0, 2.0), (3.0, 2.0)]) is None  # constant x
    assert LF.regr_slope([(1.0, 0.0), (None, 1.0), (3.0, 2.0)]) == 1.0


def test_pace_last3_and_deg_slope() -> None:
    assert LF.pace_last3([]) is None
    assert LF.pace_last3([90.0]) == 90.0
    assert LF.pace_last3([99.0, 90.0, 91.0, 92.0]) == 91.0
    assert LF.deg_slope([(90.0, 1), (90.1, 2)]) is None  # fewer than 3 laps
    assert LF.deg_slope([(90.0, 1), (90.1, None), (90.2, 3)]) is None  # 2 laps with an age
    assert LF.deg_slope([(90.0, 1), (90.1, 2), (90.2, 3)]) == 0.1


def test_stint_start() -> None:
    assert LF.stint_start(5, []) == 1
    assert LF.stint_start(5, [4]) == 5
    assert LF.stint_start(4, [4]) == 1  # the in-lap belongs to the old stint
    assert LF.stint_start(20, [4, 12, 30]) == 13


# ------------------------------------------------------------------ timing
def _laps(rows: list[tuple[str, int, int | None, float | None]]) -> list[LF.LapInput]:
    return [LF.LapInput(d, n, p, t, 0.0) for d, n, p, t in rows]


EDGE = [  # driver, lap, position, lap time: c lapped at lap 3, b with an unknown lap 2
    ("a", 1, 1, 90.0), ("b", 1, 2, 91.0), ("c", 1, 3, 120.0),
    ("a", 2, 1, 90.0), ("b", 2, 2, None), ("c", 2, 3, 120.0),
    ("a", 3, 1, 90.0), ("b", 3, 2, 90.5), ("c", 3, 3, 125.0),
    ("a", 4, 1, 90.0), ("b", 4, 2, 90.5),
    ("a", 5, 1, 89.123), ("b", 5, 2, 90.456), ("c", 4, 3, 99.0),
]  # fmt: skip


def _batch_timing(
    spark: SparkSession, rows: list[tuple[str, int, int | None, float | None]]
) -> dict[tuple[str, int], Any]:
    laps = frame(
        spark,
        "laps",
        [
            {"season": 2024, "round": 1, "driver_id": d, "lap": n, "position": p, "lap_time_s": t}
            for d, n, p, t in rows
        ],
    )
    out = timing_gaps(laps.drop("cumulative_time_s", "gap_ahead_s")).collect()
    return {(r["driver_id"], r["lap"]): r for r in out}


def _assert_timing_equal(
    batch: dict[tuple[str, int], Any], live: dict[tuple[str, int], LF.TimedLap]
) -> None:
    assert set(batch) == set(live)
    for key, b in batch.items():
        t = live[key]
        assert (
            t.cumulative_time_s,
            t.gap_leader_s,
            t.gap_ahead_s,
            t.ahead_driver_id,
            t.gap_ahead_change_1l,
            t.gap_ahead_change_3l,
        ) == (
            b["cumulative_time_s"],
            b["gap_leader_s"],
            b["gap_ahead_s"],
            b["ahead_driver_id"],
            b["gap_ahead_change_1l"],
            b["gap_ahead_change_3l"],
        ), key


def test_compute_timing_equals_timing_gaps_on_edge_cases(spark: SparkSession) -> None:
    _assert_timing_equal(_batch_timing(spark, EDGE), LF.compute_timing(_laps(EDGE)))


def test_compute_timing_equals_timing_gaps_on_seeded_races(spark: SparkSession) -> None:
    rnd = random.Random(5)
    rows: list[tuple[str, int, int | None, float | None]] = []
    cars = [f"d{i}" for i in range(12)]
    cum = dict.fromkeys(cars, 0.0)
    for n in range(1, 25):
        times = {c: round(rnd.uniform(88, 95), 3) for c in cars}
        for c in cars:
            cum[c] += times[c]
        for pos, c in enumerate(sorted(cars, key=lambda c: cum[c]), start=1):
            rows.append((c, n, pos, None if rnd.random() < 0.01 else times[c]))
    _assert_timing_equal(_batch_timing(spark, rows), LF.compute_timing(_laps(rows)))


# ------------------------------------------------------------------ restart and rejoin ports
def test_is_restart_equals_the_batch_on_random_flag_patterns() -> None:
    rnd = random.Random(7)
    for _ in range(500):
        neutral = {n: rnd.random() < 0.25 for n in range(1, 30)}
        red = frozenset(n for n, v in neutral.items() if v and rnd.random() < 0.3)
        for lap in range(1, 30):
            assert LF.is_restart(neutral, lap, red) == U._is_restart(neutral, lap, red)


@pytest.mark.parametrize("loss", [20.0, 22.0, 25.0, None])
def test_rejoin_traffic_equals_the_batch(loss: float | None) -> None:
    sc = Scenario(
        2023, 9, [*standard_cars(), Car("x", "team_x", 119.25, 100.0), Car("y", "t_y", 90.5, 110.0)]
    )
    race = race_data(sc, pit_loss=loss)
    timing = {
        (r.driver_id, r.lap): LF.TimedLap(
            LF.LapInput(r.driver_id, r.lap, r.position, r.lap_time_s, 0.0),
            r.cumulative_time_s,
            r.gap_leader_s,
            r.gap_ahead_s,
            None,
            None,
            None,
        )
        for r in race.laps
    }
    for lap in range(1, sc.n_laps + 1):
        here = [r.driver_id for r in race.rows_at(lap)]
        for attacker in here:
            assert LF.rejoin_traffic(timing, here, attacker, lap, loss) == U._rejoin_traffic(
                race, attacker, lap
            ), (lap, attacker)


# ------------------------------------------------------------------ priors JSON
def test_race_priors_json_round_trip() -> None:
    p = LF.RacePriors(
        2024,
        5,
        21.5,
        "global_prior",
        {"a": "rb", "b": None},
        {"a": LF.DriverPrior(0.3, "team", -0.2), "b": LF.DriverPrior()},
    )
    assert LF.RacePriors.from_json(p.to_json()) == p
    assert p.driver("zzz") == LF.DriverPrior()


# ------------------------------------------------------------------ hot path stays light
def test_hot_path_modules_import_no_spark_features_or_etl() -> None:
    code = (
        "import sys\n"
        "import f1sa.streaming.processor, f1sa.streaming.latency, f1sa.streaming.redis_sink\n"
        "bad = [m for m in sys.modules if m.startswith(('pyspark', 'py4j', 'f1sa.etl', 'numpy', "
        "'pandas', 'f1sa.features.telemetry')) or (m.startswith('f1sa.features.') and "
        "m != 'f1sa.features.undercut_rule')]\n"
        "print(sorted(bad))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "[]"
