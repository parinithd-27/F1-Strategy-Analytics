"""Spark SQL views: leakage-safe prior history, fallback chains, exclusions, race summary.

Leakage fixture (8-lap races, green laps 90 s, every stop on lap 4). For a stop with pit loss L and
out-lap delta D the fixture sets in-lap = 90 + (L - D) and out-lap = 90 + D, so the pit loss is
in + out - 2 x 90 = L.

===  ===========  =======  ===========================================  ==================
id   race         circuit  stops (driver/team: pit loss, out-lap delta)  no stop
===  ===========  =======  ===========================================  ==================
A    2023 R1      x        d1/t1: 20, 5   d2/t2: 22, 6
B    2023 R2      y        d1/t1: 30, 8   d2/t2: 34, 10
C    2024 R1      x        d1/t1: 24, 7   d2/t2: 26, 9                  d3/t3 (new team)
D    2024 R2      z        d1/t1: 40, 12                                d2/t2, d4/t4 (new)
===  ===========  =======  ===========================================  ==================
"""

from __future__ import annotations

import statistics
from typing import Any

import pytest
from _feature_fixtures import frame, lap, pitstop, race, result
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from conftest import SampleLake
from f1sa.etl.clean import CleanError
from f1sa.etl.io import load_clean
from f1sa.etl.sql_jobs import SQL_VIEWS, load_sql, run_sql_views
from f1sa.features.pitloss import (
    pit_loss_features,
    race_summary_features,
    team_outlap_features,
)

RACES = {"A": (2023, 1, "x"), "B": (2023, 2, "y"), "C": (2024, 1, "x"), "D": (2024, 2, "z")}
ENTRIES: dict[str, list[tuple[str, str, tuple[float, float] | None]]] = {
    "A": [("d1", "t1", (20, 5)), ("d2", "t2", (22, 6))],
    "B": [("d1", "t1", (30, 8)), ("d2", "t2", (34, 10))],
    "C": [("d1", "t1", (24, 7)), ("d2", "t2", (26, 9)), ("d3", "t3", None)],
    "D": [("d1", "t1", (40, 12)), ("d2", "t2", None), ("d4", "t4", None)],
}
# race -> (pit_loss_source, pit_loss_prior_s, circuit_prior_s, global_prior_s, g_races, g_stops)
EXPECTED_PIT_LOSS = {
    "A": ("none", None, None, None, 0, 0),
    "B": ("global_prior", 21.0, None, 21.0, 1, 2),
    "C": ("circuit_prior", 21.0, 21.0, 26.0, 2, 4),
    "D": ("global_prior", 25.0, None, 25.0, 3, 6),
}
# (race, team) -> (outlap_source, outlap_delta_prior_s)
EXPECTED_OUTLAP = {
    ("A", "t1"): ("none", None),
    ("A", "t2"): ("none", None),
    ("B", "t1"): ("team_prior", 5.0),
    ("B", "t2"): ("team_prior", 6.0),
    ("C", "t1"): ("team_prior", 6.5),
    ("C", "t2"): ("team_prior", 8.0),
    ("C", "t3"): ("circuit_prior", 5.5),  # new team: circuit x from race A only
    ("D", "t1"): ("team_prior", 7.0),
    ("D", "t2"): ("team_prior", 9.0),
    ("D", "t4"): ("global_prior", 7.5),  # new team at a new circuit
}


def _seq(key: str) -> int:
    season, round_, _ = RACES[key]
    return season * 100 + round_


def leakage_tables(spark: SparkSession, scale_from: str | None = None) -> dict[str, DataFrame]:
    """The fixture. Races at or after ``scale_from`` get every stop's loss and delta x100."""
    laps: list[dict[str, Any]] = []
    stops, results, races = [], [], []
    for key, (season, round_, circuit) in RACES.items():
        races.append(race(season, round_, circuit))
        factor = 100.0 if scale_from is not None and _seq(key) >= _seq(scale_from) else 1.0
        for pos, (driver, team, stop) in enumerate(ENTRIES[key], start=1):
            results.append(result(season, round_, driver, team, pos, 8))
            for n in range(1, 9):
                t = 95.0 if n == 1 else 90.0
                if stop is not None and n == 4:
                    t = 90.0 + (stop[0] - stop[1]) * factor
                if stop is not None and n == 5:
                    t = 90.0 + stop[1] * factor
                is_stop = stop is not None
                laps.append(
                    lap(
                        season,
                        round_,
                        driver,
                        n,
                        pos,
                        t,
                        pit_in=is_stop and n == 4,
                        pit_out=is_stop and n == 5,
                    )
                )
            if stop is not None:
                stops.append(pitstop(season, round_, driver, 1, 4))
    return {
        "races": frame(spark, "races", races),
        "results": frame(spark, "results", results),
        "laps": frame(spark, "laps", laps),
        "pitstops": frame(spark, "pitstops", stops),
    }


def _key_of(season: int, round_: int) -> str:
    return next(k for k, v in RACES.items() if v[:2] == (season, round_))


def _pit_loss(views: dict[str, DataFrame]) -> dict[str, tuple[Any, ...]]:
    return {
        _key_of(r["season"], r["round"]): (
            r["pit_loss_source"],
            r["pit_loss_prior_s"],
            r["circuit_prior_s"],
            r["global_prior_s"],
            r["global_prior_races"],
            r["global_prior_stops"],
        )
        for r in pit_loss_features(views).collect()
    }


def _outlap(views: dict[str, DataFrame]) -> dict[tuple[str, str], tuple[Any, ...]]:
    return {
        (_key_of(r["season"], r["round"]), r["constructor_id"]): (
            r["outlap_source"],
            r["outlap_delta_prior_s"],
        )
        for r in team_outlap_features(views).collect()
    }


def test_prior_values_and_fallback_chain_known_answers(spark: SparkSession) -> None:
    views = run_sql_views(spark, leakage_tables(spark))
    stops = {
        (r["season"], r["round"], r["driver_id"]): r["pit_loss_s"]
        for r in views["pit_loss_stops"].collect()
    }
    assert stops[(2023, 1, "d1")] == 20.0 and stops[(2024, 2, "d1")] == 40.0
    assert _pit_loss(views) == EXPECTED_PIT_LOSS
    assert _outlap(views) == EXPECTED_OUTLAP
    missing = {
        r["season"]: r["pit_loss_missing"]
        for r in pit_loss_features(views).collect()
        if r["round"] == 1
    }
    assert missing == {2023: True, 2024: False}


@pytest.mark.parametrize("target", ["A", "B", "C", "D"])
def test_leakage_history_uses_only_earlier_races(spark: SparkSession, target: str) -> None:
    """Scaling the data of the target race and every later race by x100 must not change the
    target's prior values, or those of any earlier race."""
    base_loss, base_out = EXPECTED_PIT_LOSS, EXPECTED_OUTLAP
    views = run_sql_views(spark, leakage_tables(spark, scale_from=target))
    loss, out = _pit_loss(views), _outlap(views)
    upto = [k for k in RACES if _seq(k) <= _seq(target)]
    for k in upto:
        assert loss[k] == base_loss[k], k
        for (race_key, team), value in base_out.items():
            if race_key == k:
                assert out[(race_key, team)] == value, (race_key, team)
    later = [k for k in RACES if _seq(k) > _seq(target) and base_loss[k][1] is not None]
    for k in later:  # sanity check: the scaling does reach later races
        assert loss[k] != base_loss[k], k


def test_pit_cycle_exclusions(spark: SparkSession) -> None:
    """Only the clean, unneutralised stop with a reference pace is used."""
    laps: list[dict[str, Any]] = []
    cases = {
        "ok": (4, 22.0, 12),
        "long_stop": (4, 75.0, 12),
        "last_lap": (12, 22.0, 12),
        "lap_one": (1, 22.0, 12),
        "neutral_out": (6, 22.0, 12),  # lap 7 is neutralised for the whole field
        "few_green": (2, 22.0, 4),  # retires after 4 laps: no reference pace
    }
    stops = []
    for pos, (driver, (stop_lap, duration, n_laps)) in enumerate(cases.items(), start=1):
        for n in range(1, n_laps + 1):
            laps.append(
                lap(
                    2024,
                    1,
                    driver,
                    n,
                    pos,
                    110.0 if n in (stop_lap, stop_lap + 1) else 90.0,
                    pit_in=n == stop_lap,
                    pit_out=n == stop_lap + 1,
                    neutralised=n == 7,
                )
            )
        stops.append(pitstop(2024, 1, driver, 1, stop_lap, duration))
    tables = {
        "races": frame(spark, "races", [race(2024, 1, "x")]),
        "results": frame(
            spark, "results", [result(2024, 1, d, "t", i, 12) for i, d in enumerate(cases, 1)]
        ),
        "laps": frame(spark, "laps", laps),
        "pitstops": frame(spark, "pitstops", stops),
    }
    views = run_sql_views(spark, tables)
    used = [(r["driver_id"], r["pit_loss_s"]) for r in views["pit_loss_stops"].collect()]
    assert used == [("ok", 40.0)]
    assert [r["driver_id"] for r in views["outlap_deltas"].collect()] == ["ok"]


def test_sample_fallback_path_two_circuits(
    spark: SparkSession, sample_clean_lake: SampleLake
) -> None:
    """The two sample races are at different circuits. 2023 R5 (miami) has no history at all.
    2024 R5 (shanghai) has no circuit history, so it falls back to the global prior, which comes
    from 2023 R5 only. Values are compared within 1e-3 because Spark's ROUND rounds half up and
    Python's round does not."""
    c = load_clean(spark, sample_clean_lake.clean_root)
    views = run_sql_views(spark, c)
    loss = {r["season"]: r for r in pit_loss_features(views).collect()}
    assert loss[2023]["pit_loss_source"] == "none" and loss[2023]["pit_loss_missing"] is True
    assert loss[2023]["pit_loss_prior_s"] is None and loss[2023]["global_prior_stops"] == 0
    s24 = loss[2024]
    assert s24["pit_loss_source"] == "global_prior" and s24["circuit_prior_s"] is None
    stops_2023 = [r["pit_loss_s"] for r in views["pit_loss_stops"].where("season = 2023").collect()]
    assert s24["global_prior_stops"] == len(stops_2023) > 0
    assert s24["global_prior_races"] == 1
    assert s24["pit_loss_prior_s"] == pytest.approx(statistics.median(stops_2023), abs=1e-3)

    out = team_outlap_features(views).collect()
    assert {r["outlap_source"] for r in out if r["season"] == 2023} == {"none"}
    by_team = {r["constructor_id"]: r for r in out if r["season"] == 2024}
    assert {r["outlap_source"] for r in by_team.values()} <= {"team_prior", "global_prior"}
    deltas_2023 = views["outlap_deltas"].where("season = 2023")
    rb = [r["outlap_delta_s"] for r in deltas_2023.where("constructor_id = 'red_bull'").collect()]
    assert by_team["red_bull"]["outlap_source"] == "team_prior"
    assert by_team["red_bull"]["team_prior_s"] == pytest.approx(statistics.median(rb), abs=1e-3)
    for r in by_team.values():
        if r["outlap_source"] == "global_prior":  # renamed team (e.g. alphatauri -> rb)
            assert r["team_prior_n"] == 0 and r["circuit_prior_n"] == 0
            all_2023 = [x["outlap_delta_s"] for x in deltas_2023.collect()]
            assert r["outlap_delta_prior_s"] == pytest.approx(statistics.median(all_2023), abs=1e-3)


def test_sample_race_summary(spark: SparkSession, sample_clean_lake: SampleLake) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root)
    rows = {r["season"]: r for r in race_summary_features(run_sql_views(spark, c)).collect()}
    assert (rows[2023]["race_laps"], rows[2024]["race_laps"]) == (57, 56)
    assert (rows[2023]["pit_stops"], rows[2024]["pit_stops"]) == (20, 40)
    assert rows[2023]["entrants"] == rows[2024]["entrants"] == 20
    assert rows[2024]["long_stops"] == 1 and rows[2024]["stops_per_driver"] == 2.0
    assert rows[2023]["neutralised_laps"] == 0 and rows[2024]["neutralised_laps"] > 0
    fastest = c["laps"].where("season = 2023").agg(F.min("lap_time_s")).first()
    assert fastest is not None and rows[2023]["fastest_lap_s"] == fastest[0]
    assert rows[2023]["winner_driver_id"] == "max_verstappen"


def test_load_sql_and_input_errors(spark: SparkSession) -> None:
    for name in SQL_VIEWS:
        assert "SELECT" in load_sql(name)
    with pytest.raises(CleanError, match="unknown SQL view"):
        load_sql("drop_everything")
    tables = leakage_tables(spark)
    with pytest.raises(CleanError, match="need clean tables"):
        run_sql_views(spark, {k: v for k, v in tables.items() if k != "pitstops"})
    tables["races"] = frame(spark, "races", [race(2024, 100, "x")])
    with pytest.raises(CleanError, match="race_seq"):
        run_sql_views(spark, tables)


def test_wrappers_need_built_views() -> None:
    with pytest.raises(CleanError, match="not built"):
        pit_loss_features({})
