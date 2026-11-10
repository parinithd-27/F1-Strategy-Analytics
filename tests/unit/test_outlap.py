"""Out-lap pace: clean-lap rule, realised delta (ORACLE), leakage-safe expected delta.

Leakage fixture (8-lap races, green laps 90 s, every stop on lap 4, stints 1-4 and 5-8):
out-lap 5 = 90 + delta, and the stint-2 clean median is 90, so realised = delta.

===  =========  =======  =================================================================
id   race       circuit  entries (driver/team: out-lap delta, or "-" = no stop)
===  =========  =======  =================================================================
A    2023 R1    x        d1/alphatauri: 10   d2/ferrari: 20
B    2023 R2    y        d1/alphatauri: 12   d2/ferrari: 22
C    2024 R1    x        d1/rb: 14   d3/rb: 16 (new driver)   d2/haas: 30 (changed team)
D    2024 R2    z        d1/rb: 18   d4/williams: -  (no history at any level but global)
===  =========  =======  =================================================================
"""

from __future__ import annotations

from typing import Any

import pytest
from _feature_fixtures import frame, lap, pitstop, race, result
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from conftest import SampleLake
from f1sa.etl.io import load_clean
from f1sa.features import outlap as O
from f1sa.features import schemas as FS

RACES = {"A": (2023, 1, "x"), "B": (2023, 2, "y"), "C": (2024, 1, "x"), "D": (2024, 2, "z")}
ENTRIES: dict[str, list[tuple[str, str, float | None]]] = {
    "A": [("d1", "alphatauri", 10.0), ("d2", "ferrari", 20.0)],
    "B": [("d1", "alphatauri", 12.0), ("d2", "ferrari", 22.0)],
    "C": [("d1", "rb", 14.0), ("d3", "rb", 16.0), ("d2", "haas", 30.0)],
    "D": [("d1", "rb", 18.0), ("d4", "williams", None)],
}
# (race, driver) -> (expected_outlap_delta_s, expected_outlap_source)
EXPECTED: dict[tuple[str, str], tuple[float | None, str]] = {
    ("A", "d1"): (None, "none"),
    ("A", "d2"): (None, "none"),
    ("B", "d1"): (10.0, "driver_team"),
    ("B", "d2"): (20.0, "driver_team"),
    ("C", "d1"): (11.0, "driver_team"),  # alphatauri history continues as rb (lineage)
    ("C", "d3"): (11.0, "team"),  # new driver at rb: team history
    ("C", "d2"): (15.0, "circuit"),  # haas has no history: circuit x from race A
    ("D", "d1"): (12.0, "driver_team"),
    ("D", "d4"): (17.714, "global"),  # 124 / 7 over A, B, C
}


def _stint(season: int, round_: int, driver: str, n: int, start: int, end: int) -> dict[str, Any]:
    return {
        "season": season,
        "round": round_,
        "driver_id": driver,
        "stint": n,
        "start_lap": start,
        "end_lap": end,
        "laps": end - start + 1,
        "is_synthetic": False,
        "source": "jolpica_inferred",
    }


def _seq(key: str) -> int:
    season, round_, _ = RACES[key]
    return season * 100 + round_


def leakage_tables(spark: SparkSession, scale_from: str | None = None) -> dict[str, DataFrame]:
    laps, stints, stops, results, races = [], [], [], [], []
    for key, (season, round_, circuit) in RACES.items():
        races.append(race(season, round_, circuit))
        factor = 100.0 if scale_from is not None and _seq(key) >= _seq(scale_from) else 1.0
        for pos, (driver, team, delta) in enumerate(ENTRIES[key], start=1):
            results.append(result(season, round_, driver, team, pos, 8))
            stopped = delta is not None
            for n in range(1, 9):
                t = 95.0 if n == 1 else 90.0
                if stopped and n == 4:
                    t = 100.0
                if stopped and n == 5:
                    t = 90.0 + (delta or 0.0) * factor
                laps.append(
                    lap(
                        season,
                        round_,
                        driver,
                        n,
                        pos,
                        t,
                        pit_in=stopped and n == 4,
                        pit_out=stopped and n == 5,
                    )
                )
            if stopped:
                stops.append(pitstop(season, round_, driver, 1, 4))
                stints += [
                    _stint(season, round_, driver, 1, 1, 4),
                    _stint(season, round_, driver, 2, 5, 8),
                ]
            else:
                stints.append(_stint(season, round_, driver, 1, 1, 8))
    return {
        "laps": frame(spark, "laps", laps),
        "stints": frame(spark, "stints", stints),
        "pitstops": frame(spark, "pitstops", stops),
        "results": frame(spark, "results", results),
        "races": frame(spark, "races", races),
    }


def _features(t: dict[str, DataFrame]) -> DataFrame:
    return O.outlap_features(t["laps"], t["stints"], t["pitstops"], t["results"], t["races"])


def _key_of(season: int, round_: int) -> str:
    return next(k for k, v in RACES.items() if v[:2] == (season, round_))


def _expected(df: DataFrame) -> dict[tuple[str, str], tuple[Any, Any]]:
    return {
        (_key_of(r["season"], r["round"]), r["driver_id"]): (
            r["expected_outlap_delta_s"],
            r["expected_outlap_source"],
        )
        for r in df.collect()
    }


# ------------------------------------------------------------------ clean laps
def test_clean_lap_rule(spark: SparkSession) -> None:
    rows = []
    for n in range(2, 13):  # driver b: 11 candidate laps at 100 s (lap 1 excluded)
        rows.append(lap(2024, 1, "b", n, 2, 100.0))
    rows.append(lap(2024, 1, "b", 1, 2, 104.0))
    times = {1: 120.0, 3: 120.0, 4: 125.0, 6: 100.0, 7: 100.0, 8: 108.0, 9: 106.0}
    for n in range(1, 13):
        row = lap(2024, 1, "a", n, 1, times.get(n, 100.0), pit_in=n == 3, pit_out=n == 4)
        row["is_sc_lap"] = n == 6
        row["is_red_flag_lap"] = n == 7
        rows.append(row)
    out = O.clean_laps(frame(spark, "laps", rows))
    a = {r["lap"]: r for r in out.where(F.col("driver_id") == "a").collect()}
    assert a[2]["race_candidate_median_s"] == 100.0
    clean = sorted(n for n, r in a.items() if r["is_clean_lap"])
    # excluded: 1 first lap, 3 in-lap, 4 out-lap, 6 SC, 7 red flag, 8 at 108 % (> 107 %)
    assert clean == [2, 5, 9, 10, 11, 12]
    assert a[9]["lap_time_s"] == 106.0  # 106 % is kept
    assert out.where(F.col("driver_id") == "b").where(~F.col("is_clean_lap")).count() == 1


def test_clean_lap_neutralised_flag_and_unknown_time(spark: SparkSession) -> None:
    rows = [lap(2024, 1, "a", n, 1, 90.0) for n in range(1, 6)]
    rows[2] = lap(2024, 1, "a", 3, 1, 90.0, neutralised=True)
    rows[3] = lap(2024, 1, "a", 4, 1, None)
    out = {r["lap"]: r["is_clean_lap"] for r in O.clean_laps(frame(spark, "laps", rows)).collect()}
    assert out == {1: False, 2: True, 3: False, 4: False, 5: True}


# ------------------------------------------------------------------ realised
def test_realised_same_stint_previous_stint_and_none(spark: SparkSession) -> None:
    times = {
        1: 95.0,
        2: 90.0,
        3: 92.0,
        4: 100.0,
        5: 110.0,
        6: 91.0,
        7: 91.0,
        8: 101.0,
        9: 112.0,
        10: 93.0,
    }
    rows = []
    for n, t in times.items():
        row = lap(2024, 1, "a", n, 1, t, pit_in=n in (4, 8), pit_out=n in (5, 9))
        row["is_sc_lap"] = n in (6, 7)  # stint 2 has no clean lap
        rows.append(row)
    rows += [
        lap(2024, 1, "x", 1, 2, 95.0),
        lap(2024, 1, "x", 2, 2, 120.0, pit_in=True),
        lap(2024, 1, "x", 3, 2, 115.0, pit_out=True, neutralised=True),
    ]
    stints = frame(
        spark,
        "stints",
        [
            _stint(2024, 1, "a", 1, 1, 4),
            _stint(2024, 1, "a", 2, 5, 8),
            _stint(2024, 1, "a", 3, 9, 10),
            _stint(2024, 1, "x", 1, 1, 2),
            _stint(2024, 1, "x", 2, 3, 3),
        ],
    )
    stops = frame(
        spark,
        "pitstops",
        [
            pitstop(2024, 1, "a", 1, 4),
            pitstop(2024, 1, "a", 2, 8, 75.0),
            pitstop(2024, 1, "x", 1, 2),
        ],
    )
    got = {
        (r["driver_id"], r["stint"]): r
        for r in O.realised_outlaps(frame(spark, "laps", rows), stints, stops).collect()
    }
    s1 = got[("a", 1)]
    assert (s1["outlap"], s1["realised_outlap_delta_s"], s1["reference_source"]) == (
        None,
        None,
        None,
    )
    s2 = got[("a", 2)]  # stint 2 has no clean lap: previous stint median (90, 92) = 91
    assert (s2["outlap"], s2["reference_clean_lap_s"], s2["reference_source"]) == (
        5,
        91.0,
        "previous_stint",
    )
    assert s2["realised_outlap_delta_s"] == 19.0 and s2["is_long_stop"] is False
    s3 = got[("a", 3)]  # stint 3: same-stint median (93)
    assert (s3["reference_source"], s3["realised_outlap_delta_s"]) == ("same_stint", 19.0)
    assert s3["is_long_stop"] is True  # the 75 s stop on lap 8
    x2 = got[("x", 2)]
    assert (x2["reference_source"], x2["realised_outlap_delta_s"]) == ("none", None)
    assert x2["outlap_neutralised"] is True


# ------------------------------------------------------------------ expected / leakage
def test_expected_known_answers_and_fallback_levels(spark: SparkSession) -> None:
    df = _features(leakage_tables(spark))
    assert [(f.name, f.dataType) for f in df.schema] == [
        (f.name, f.dataType) for f in FS.OUTLAP_FEATURES
    ]
    got = _expected(df)
    assert got == EXPECTED
    rows = df.collect()
    assert {r["team_history_key"] for r in rows if r["constructor_id"] == "alphatauri"} == {"rb"}
    missing = {
        r["expected_outlap_missing"] for r in rows if _key_of(r["season"], r["round"]) == "A"
    }
    assert missing == {True}
    realised = {
        (_key_of(r["season"], r["round"]), r["driver_id"]): r["realised_outlap_delta_s"]
        for r in rows
        if r["stint"] == 2
    }
    assert realised[("C", "d2")] == 30.0 and realised[("A", "d1")] == 10.0
    # every stint row of a driver carries the same expected value
    assert (
        len(
            {
                r["expected_outlap_delta_s"]
                for r in rows
                if (r["season"], r["round"], r["driver_id"]) == (2024, 1, "d1")
            }
        )
        == 1
    )


@pytest.mark.parametrize("target", ["A", "B", "C", "D"])
def test_leakage_expected_uses_only_earlier_races(spark: SparkSession, target: str) -> None:
    """Scaling the out-lap deltas of the target race and every later race by x100 must not change
    the target's expected_outlap_delta_s or its source (or those of any earlier race)."""
    got = _expected(_features(leakage_tables(spark, scale_from=target)))
    for (race_key, driver), value in EXPECTED.items():
        if _seq(race_key) <= _seq(target):
            assert got[(race_key, driver)] == value, (race_key, driver)
    later = [k for k in EXPECTED if _seq(k[0]) > _seq(target) and EXPECTED[k][0] is not None]
    if later:  # sanity check: the scaling reaches some later history (C/d2 only uses race A)
        assert any(got[k] != EXPECTED[k] for k in later), later


def test_history_excludes_long_stops_and_neutralised_outlaps(spark: SparkSession) -> None:
    laps, stints, stops, results = [], [], [], []
    deltas = {"ok": (10.0, 22.0, False), "long": (50.0, 75.0, False), "sc": (70.0, 22.0, True)}
    for pos, (driver, (delta, duration, neutral)) in enumerate(deltas.items(), start=1):
        results.append(result(2023, 1, driver, "ferrari", pos, 8))
        for n in range(1, 9):
            t = 90.0 + delta if n == 5 else (100.0 if n == 4 else 90.0)
            laps.append(
                lap(
                    2023,
                    1,
                    driver,
                    n,
                    pos,
                    t,
                    pit_in=n == 4,
                    pit_out=n == 5,
                    neutralised=neutral and n == 5,
                )
            )
        stops.append(pitstop(2023, 1, driver, 1, 4, duration))
        stints += [_stint(2023, 1, driver, 1, 1, 4), _stint(2023, 1, driver, 2, 5, 8)]
    results.append(result(2023, 2, "new", "ferrari", 1, 8))
    laps += [lap(2023, 2, "new", n, 1, 90.0) for n in range(1, 9)]
    stints.append(_stint(2023, 2, "new", 1, 1, 8))
    t = {
        "laps": frame(spark, "laps", laps),
        "stints": frame(spark, "stints", stints),
        "pitstops": frame(spark, "pitstops", stops),
        "results": frame(spark, "results", results),
        "races": frame(spark, "races", [race(2023, 1, "x"), race(2023, 2, "y")]),
    }
    (row,) = _features(t).where(F.col("round") == 2).collect()
    assert (row["expected_outlap_delta_s"], row["expected_outlap_source"]) == (10.0, "team")
    assert row["team_prior_n"] == 1


def test_sample_outlap_features(spark: SparkSession, sample_clean_lake: SampleLake) -> None:
    c = load_clean(spark, sample_clean_lake.clean_root)
    df = O.outlap_features(c["laps"], c["stints"], c["pitstops"], c["results"], c["races"])
    rows = df.collect()
    assert len(rows) == 100  # one per clean stint
    assert {r["expected_outlap_source"] for r in rows if r["season"] == 2023} == {"none"}
    s24 = {r["expected_outlap_source"] for r in rows if r["season"] == 2024}
    assert s24 <= {"driver_team", "team"}
    tsunoda = [r for r in rows if r["season"] == 2024 and r["driver_id"] == "tsunoda"]
    assert tsunoda and tsunoda[0]["team_history_key"] == "rb"
    assert tsunoda[0]["expected_outlap_source"] == "driver_team"  # alphatauri 2023 -> rb 2024
    with_out = [r for r in rows if r["stint"] > 1]
    assert len(with_out) == 60 and all(r["outlap"] is not None for r in with_out)
    # one prior race: every 2024 driver_team level has n <= 2 stops, so it is shrunk to the team
    pace = {r["expected_outlap_pace_source"] for r in rows if r["season"] == 2024}
    assert pace <= {"driver_team_shrunk", "team"} and "driver_team_shrunk" in pace
    assert {r["expected_outlap_pace_source"] for r in rows if r["season"] == 2023} == {"none"}
    eligible = [r for r in with_out if r["outlap_pace_residual_s"] is not None]
    assert eligible and all(r["stop_delta_s"] is not None for r in eligible)


# ------------------------------------------------------------------ decomposition and shrinkage
def _race_tables(
    spark: SparkSession,
    entries: dict[tuple[int, int], list[tuple[str, str, float, float]]],
    scale_from: int | None = None,
) -> dict[str, DataFrame]:
    """8-lap races; every stop on lap 4 with the given duration; out-lap 5 = 90 + delta, so the
    realised delta is ``delta``. ``scale_from`` (race_seq) multiplies deltas and durations x100
    for that race and every later one."""
    laps, stints, stops, results, races = [], [], [], [], []
    for (season, round_), rows in sorted(entries.items()):
        races.append(race(season, round_, f"c{round_}"))
        f = 100.0 if scale_from is not None and season * 100 + round_ >= scale_from else 1.0
        for pos, (driver, team, delta, duration) in enumerate(rows, start=1):
            results.append(result(season, round_, driver, team, pos, 8))
            for n in range(1, 9):
                t = {1: 95.0, 4: 100.0, 5: 90.0 + delta * f}.get(n, 90.0)
                laps.append(lap(season, round_, driver, n, pos, t, pit_in=n == 4, pit_out=n == 5))
            stops.append(pitstop(season, round_, driver, 1, 4, duration * f))
            stints += [
                _stint(season, round_, driver, 1, 1, 4),
                _stint(season, round_, driver, 2, 5, 8),
            ]
    return {
        "laps": frame(spark, "laps", laps),
        "stints": frame(spark, "stints", stints),
        "pitstops": frame(spark, "pitstops", stops),
        "results": frame(spark, "results", results),
        "races": frame(spark, "races", races),
    }


def test_decomposition_residual_and_stop_delta(spark: SparkSession) -> None:
    t = _race_tables(
        spark,
        {
            (2023, 1): [
                ("a", "ferrari", 15.0, 22.0),
                ("b", "haas", 16.0, 22.5),
                ("c", "rb", 20.0, 25.0),
            ]
        },
    )
    rows = {r["driver_id"]: r for r in _features(t).where(F.col("stint") == 2).collect()}
    assert {r["race_median_outlap_delta_s"] for r in rows.values()} == {16.0}
    assert {r["race_median_stop_s"] for r in rows.values()} == {22.5}
    assert {d: r["stop_delta_s"] for d, r in rows.items()} == {"a": -0.5, "b": 0.0, "c": 2.5}
    assert {d: r["outlap_pace_residual_s"] for d, r in rows.items()} == {
        "a": -0.5,
        "b": 0.0,
        "c": 1.5,
    }
    assert rows["c"]["stop_duration_s"] == 25.0


def test_decomposition_skips_long_and_neutralised_stops(spark: SparkSession) -> None:
    t = _race_tables(
        spark,
        {
            (2023, 1): [
                ("a", "ferrari", 15.0, 22.0),
                ("b", "haas", 60.0, 75.0),
                ("c", "rb", 17.0, 23.0),
            ]
        },
    )
    rows = {r["driver_id"]: r for r in _features(t).where(F.col("stint") == 2).collect()}
    assert rows["b"]["is_long_stop"] is True
    assert rows["b"]["outlap_pace_residual_s"] is None and rows["b"]["stop_delta_s"] is None
    assert rows["a"]["race_median_outlap_delta_s"] == 16.0  # median of a and c only
    assert rows["a"]["race_median_stop_s"] == 22.5


def _shrink_entries(
    last_extra: bool = True,
) -> dict[tuple[int, int], list[tuple[str, str, float, float]]]:
    """Seven races. d1/d2 at ferrari, d3 at haas: deltas 18/16/13, durations 23/23/22, so the
    race medians are 16 s and 23 s, residuals +2/0/-2 and stop deltas 0/0/-1. Race 7 adds a new
    ferrari driver d4 and a new team (williams) driver d5."""
    out = {}
    for r in range(1, 8):
        rows = [
            ("d1", "ferrari", 18.0, 23.0),
            ("d2", "ferrari", 16.0, 23.0),
            ("d3", "haas", 13.0, 22.0),
        ]
        if r == 7 and last_extra:
            rows += [("d4", "ferrari", 16.0, 23.0), ("d5", "williams", 16.0, 23.0)]
        out[(2023, r)] = rows
    return out


def _shrink_expected(n: int) -> dict[str, tuple[float | None, str]]:
    """Known answers for race n+1 (n prior races), from the shrinkage formulas."""
    k = O.SHRINK_KAPPA
    if n == 0:
        return {"d1": (None, "none")}
    g = 0.0  # residuals +2, 0, -2 per race
    team = (2 * n * 1.0 + k * g) / (2 * n + k)
    dt = (n * 2.0 + k * team) / (n + k)
    src = "driver_team" if n >= O.DRIVER_TEAM_MIN_N else "driver_team_shrunk"
    return {"d1": (round(dt, 3), src)}


@pytest.mark.parametrize("n", [0, 1, 4, 5, 6])
def test_shrinkage_values_and_sources(spark: SparkSession, n: int) -> None:
    df = _features(_race_tables(spark, _shrink_entries()))
    rows = {
        r["driver_id"]: r
        for r in df.where((F.col("round") == n + 1) & (F.col("stint") == 2)).collect()
    }
    value, source = _shrink_expected(n)["d1"]
    d1 = rows["d1"]
    assert (d1["expected_outlap_pace_residual_s"], d1["expected_outlap_pace_source"]) == (
        value,
        source,
    )
    assert d1["pace_driver_team_n"] == n and d1["pace_team_n"] == 2 * n
    assert d1["expected_outlap_pace_missing"] is (n == 0)
    if n == 0:
        assert d1["expected_stop_delta_s"] is None and d1["expected_stop_source"] == "none"
    else:  # ferrari stop deltas are 0; global mean is -1/3
        k = O.SHRINK_KAPPA
        stop = (0.0 + k * (-1.0 / 3.0)) / (2 * n + k)
        assert d1["expected_stop_delta_s"] == round(stop, 3)
        assert d1["expected_stop_source"] == "team"


def test_shrinkage_new_driver_uses_team_and_new_team_uses_global(spark: SparkSession) -> None:
    df = _features(_race_tables(spark, _shrink_entries()))
    rows = {
        r["driver_id"]: r for r in df.where((F.col("round") == 7) & (F.col("stint") == 2)).collect()
    }
    k, n = O.SHRINK_KAPPA, 6
    team = (2 * n * 1.0) / (2 * n + k)
    assert (
        rows["d4"]["expected_outlap_pace_residual_s"],
        rows["d4"]["expected_outlap_pace_source"],
    ) == (
        round(team, 3),
        "team",
    )
    assert (
        rows["d5"]["expected_outlap_pace_residual_s"],
        rows["d5"]["expected_outlap_pace_source"],
    ) == (
        0.0,
        "global",
    )
    assert rows["d5"]["expected_stop_source"] == "global"
    assert rows["d5"]["expected_stop_delta_s"] == round(-1.0 / 3.0, 3)


@pytest.mark.parametrize("target_round", [2, 5])
def test_leakage_decomposed_priors_use_only_earlier_races(
    spark: SparkSession, target_round: int
) -> None:
    """Scaling the target race and every later race x100 must not change the target's priors."""
    cols = [
        "expected_outlap_pace_residual_s",
        "expected_outlap_pace_source",
        "expected_stop_delta_s",
        "expected_stop_source",
    ]

    def priors(scale: int | None) -> dict[tuple[int, str], tuple[Any, ...]]:
        df = _features(_race_tables(spark, _shrink_entries(), scale_from=scale))
        return {
            (r["round"], r["driver_id"]): tuple(r[c] for c in cols)
            for r in df.where(F.col("stint") == 2).collect()
        }

    base, scaled = priors(None), priors(2023 * 100 + target_round)
    for key, value in base.items():
        if key[0] <= target_round:
            assert scaled[key] == value, key
    assert any(scaled[k] != v for k, v in base.items() if k[0] > target_round + 1)
