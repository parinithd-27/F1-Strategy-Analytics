"""Undercut attempts: hand-built scenarios, grid, leakage, detector agreement and edge cases.

Scenarios (``_undercut_fixtures``, 14-lap races, 2023 rounds 1-8): ``a`` runs 1.5 s behind ``d``,
pits on lap 6 (in-lap 6, out-lap 7); ``d`` pits on lap 7, so the label lap is 8 (d's out-lap).

=======  ==============================================  ==========================
round    change from the standard race                   default-params outcome
=======  ==============================================  ==========================
1        none (a gains 2.5 s/lap on fresh tyres)         success (a ahead by 1.0 s)
2        a gains only 0.5 s/lap                          fail (a 1.0 s behind)
3        d stops on lap 12 (outside L+1..L+3)            defender_stayed_out
4        SC on lap 7                                     excluded_neutralised
5        a retires after lap 7                           excluded_retired
6        lapped car x crosses lap 4 between d and a      excluded_traffic_between
7        d stops on lap 8 (L+2)                          k=1 stayed out, k=2 success
8        a starts 3.0 s behind d                         not a candidate at G=2.5
=======  ==============================================  ==========================
"""

from __future__ import annotations

import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest
from _feature_fixtures import frame
from _undercut_fixtures import Car, Scenario, clean_tables, race_data, standard_cars
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.features import schemas as FS
from f1sa.features import undercut as U
from f1sa.features.build import compute_features
from f1sa.features.undercut_rule import GAP_GRID, RULE_REASONS, WINDOW_GRID, RuleParams
from f1sa.streaming.detector import detect, snapshot_from_state

LAPPED = Car("x", "team_x", 119.25, 100.0)
SCENARIOS: dict[str, Scenario] = {
    "success": Scenario(2023, 1, standard_cars()),
    "fail": Scenario(2023, 2, standard_cars(a_fresh_gain=0.5)),
    "stayed_out": Scenario(2023, 3, standard_cars(d_pit_laps=[12])),
    "sc_in_window": Scenario(2023, 4, standard_cars(), neutral_laps={7}),
    "attacker_retires": Scenario(2023, 5, standard_cars(a_retire_after=7)),
    "lapped_between": Scenario(2023, 6, [*standard_cars(), LAPPED]),
    "late_cover": Scenario(2023, 7, standard_cars(d_pit_laps=[8])),
    "gap_3s": Scenario(2023, 8, standard_cars(a_lap1=100.0)),
}
DEFAULT_OUTCOME = {
    "success": ("success", 1),
    "fail": ("fail", 0),
    "stayed_out": ("defender_stayed_out", None),
    "sc_in_window": ("excluded_neutralised", None),
    "attacker_retires": ("excluded_retired", None),
    "lapped_between": ("excluded_traffic_between", None),
}


def _attempts(
    spark: SparkSession, scenarios: list[Scenario]
) -> tuple[DataFrame, dict[str, DataFrame]]:
    clean = clean_tables(spark, scenarios)
    targets = {(s.season, s.round) for s in scenarios}
    tables = compute_features(spark, clean, targets)
    return tables["undercut_attempts"], tables


@pytest.fixture(scope="module")
def built(spark: SparkSession) -> dict[str, Any]:
    df, tables = _attempts(spark, list(SCENARIOS.values()))
    rows = [r.asDict() for r in df.collect()]
    enriched = [r.asDict() for r in tables["laps_enriched"].collect()]
    stops = [r.asDict() for r in tables["laps_enriched"].where("is_pit_in_lap").collect()]
    return {"df": df, "rows": rows, "enriched": enriched, "stops": stops}


def _rows_for(built: dict[str, Any], name: str, **match: Any) -> list[dict[str, Any]]:
    sc = SCENARIOS[name]
    return [
        r
        for r in built["rows"]
        if (r["season"], r["round"]) == (sc.season, sc.round)
        and r["attacker_id"] == "a"
        and all(r[k] == v for k, v in match.items())
    ]


# ------------------------------------------------------------------ the six scenarios
@pytest.mark.parametrize("name", list(DEFAULT_OUTCOME))
def test_scenario_outcome_at_default_params(built: dict[str, Any], name: str) -> None:
    (row,) = _rows_for(built, name, is_default_params=True)
    assert (row["outcome_category"], row["label"]) == DEFAULT_OUTCOME[name]
    assert (row["defender_id"], row["lap"], row["decision_lap"]) == ("d", 6, 5)
    assert row["gap_threshold_s"] == 3.5 and row["response_window_laps"] == 2
    assert row["gap_s"] == 1.5


def test_success_and_fail_outcome_details(built: dict[str, Any]) -> None:
    (ok,) = _rows_for(built, "success", is_default_params=True)
    assert (ok["outcome_defender_pit_lap"], ok["outcome_label_lap"]) == (7, 8)
    assert ok["outcome_gap_s"] == 1.0  # a ahead of d by 1.0 s at the end of lap 8
    assert ok["outcome_attacker_position"] < ok["outcome_defender_position"]
    assert ok["exclusion_reasons"] == [] and ok["detector_eligible"] is True
    (bad,) = _rows_for(built, "fail", is_default_params=True)
    assert bad["outcome_gap_s"] == -1.0


def test_lapped_car_between_is_rule_level(built: dict[str, Any]) -> None:
    (row,) = _rows_for(built, "lapped_between", is_default_params=True)
    assert row["exclusion_reasons"] == ["traffic_between"]
    assert row["detector_eligible"] is False


def test_sc_window_is_not_rule_level(built: dict[str, Any]) -> None:
    """The SC is on lap 7, after the decision: the detector flags it, the label excludes it."""
    (row,) = _rows_for(built, "sc_in_window", is_default_params=True)
    assert row["detector_eligible"] is True and row["exclusion_reasons"] == ["neutralised"]


# ------------------------------------------------------------------ grid
def test_grid_emits_every_combination(built: dict[str, Any]) -> None:
    rows = _rows_for(built, "success")
    combos = {(r["gap_threshold_s"], r["response_window_laps"]) for r in rows}
    assert combos == {(g, k) for g in GAP_GRID for k in WINDOW_GRID}
    assert [r["is_default_params"] for r in rows].count(True) == 1


def test_grid_gap_threshold_is_monotone(built: dict[str, Any]) -> None:
    keys: dict[tuple[float, int], set[tuple[Any, ...]]] = defaultdict(set)
    for r in built["rows"]:
        keys[(r["gap_threshold_s"], r["response_window_laps"])].add(
            (r["season"], r["round"], r["attacker_id"], r["lap"])
        )
    for k in WINDOW_GRID:
        assert keys[(2.5, k)] <= keys[(3.5, k)] <= keys[(5.0, k)]
    gap3 = _rows_for(built, "gap_3s")
    assert {r["gap_threshold_s"] for r in gap3} == {3.5, 5.0}  # gap 3.0 s > 2.5


def test_grid_response_window(built: dict[str, Any]) -> None:
    by_k = {
        r["response_window_laps"]: r for r in _rows_for(built, "late_cover", gap_threshold_s=3.5)
    }
    assert by_k[1]["outcome_category"] == "defender_stayed_out" and by_k[1]["label"] is None
    assert (by_k[2]["outcome_category"], by_k[2]["outcome_label_lap"]) == ("success", 9)
    assert by_k[3]["outcome_category"] == "success"
    for r in _rows_for(built, "stayed_out"):
        assert r["outcome_category"] == "defender_stayed_out"


def test_table_schema_and_column_groups(built: dict[str, Any]) -> None:
    df = built["df"]
    assert [(f.name, f.dataType) for f in df.schema] == [
        (f.name, f.dataType) for f in FS.UNDERCUT_ATTEMPTS
    ]
    cols = set(df.columns)
    groups = [set(U.DECISION_FEATURES), set(U.LEGACY_FEATURES), set(U.ORACLE_COLUMNS)]
    assert all(g <= cols for g in groups) and set(U.OUTCOME_COLUMNS) <= cols
    assert not set(U.DECISION_FEATURES) & (set(U.ORACLE_COLUMNS) | set(U.OUTCOME_COLUMNS))
    assert not any(c.startswith(("oracle_", "outcome_")) for c in U.DECISION_FEATURES)
    for r in built["rows"]:
        assert r["outcome_category"] in U.OUTCOME_CATEGORIES
        assert (r["label"] is not None) == (r["outcome_category"] in ("success", "fail"))


def test_decision_features_values(built: dict[str, Any]) -> None:
    (r,) = _rows_for(built, "success", is_default_params=True)
    assert r["attacker_position"] == 3 and r["laps_remaining"] == 9  # 14 - 5
    assert r["attacker_pace_last3_s"] == 90.0 and r["pace_delta_last3_s"] == 0.0
    assert r["attacker_tyre_age_laps"] == 5 and r["tyre_age_delta_laps"] == 0
    assert r["attacker_compound"] == "MEDIUM" and r["compound_is_synthetic"] is True
    assert r["gap_ahead_change_1l"] == 0.0


# ------------------------------------------------------------------ detector agreement
def _snapshots(enriched: list[dict[str, Any]]) -> dict[tuple[int, int, int], Any]:
    by: dict[tuple[int, int, int], dict[str, dict[str, Any]]] = defaultdict(dict)
    for r in enriched:
        by[(r["season"], r["round"], r["lap"])][r["driver_id"]] = r
    flagged = {
        key for key, cars in by.items() if any(c["is_neutralised_lap"] for c in cars.values())
    }
    out = {}
    for (season, round_, lap), cars in by.items():
        prev = by.get((season, round_, lap - 1), {})
        state = {
            d: {
                "position": c["position"],
                "gap_ahead_s": c["gap_ahead_s"],
                "gap_leader_s": c["gap_leader_s"],
                "crossing_s": c["cumulative_time_s"],
                "prev_crossing_s": prev[d]["cumulative_time_s"] if d in prev else None,
                "team_key": c["constructor_id"],
                "on_in_lap": c["is_pit_in_lap"],
                "on_out_lap": c["is_pit_out_lap"],
                "tyre_compound": c["compound"],
                "tyre_age_laps": c["tyre_age_laps"],
                "last_lap_time_s": c["lap_time_s"],
            }
            for d, c in cars.items()
        }
        leader = next(c for c in cars.values() if c["position"] == 1)
        green = (season, round_, lap) not in flagged
        out[(season, round_, lap)] = snapshot_from_state(
            season,
            round_,
            lap,
            state,
            track_green=green,
            restart_lap=green and (season, round_, lap - 1) in flagged,
            leader_lap_time_s=leader["lap_time_s"],
        )
    return out


def test_detector_and_batch_rule_agree(built: dict[str, Any]) -> None:
    """For every scenario, lap and G: the pairs the detector flags at the end of L-1 whose
    attacker then pits on L are exactly the batch attempts with ``detector_eligible``."""
    snaps = _snapshots(built["enriched"])
    pits = {(r["season"], r["round"], r["driver_id"], r["lap"]) for r in built["stops"]}
    for g in GAP_GRID:
        detected = {
            (c.season, c.round, c.attacker_id, c.defender_id, c.attempt_lap)
            for snap in snaps.values()
            for c in detect(snap, RuleParams(gap_threshold_s=g))
            if (c.season, c.round, c.attacker_id, c.attempt_lap) in pits
        }
        batch = {
            (r["season"], r["round"], r["attacker_id"], r["defender_id"], r["lap"])
            for r in built["rows"]
            if r["gap_threshold_s"] == g and r["detector_eligible"]
        }
        assert detected == batch, g
        assert batch  # the comparison is not vacuous


# ------------------------------------------------------------------ leakage
LEAK_RACES = [(2023, 1, "c1"), (2023, 2, "c2"), (2024, 1, "c1"), (2024, 2, "c2")]
TARGET = (2024, 1)
ATTEMPT_LAP = 6


def _leak_tables(spark: SparkSession, scaled: bool) -> dict[str, DataFrame]:
    """Four standard races. Scaled: the target race from lap L on, and every later race, x100
    (lap times and stop durations)."""
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for season, round_, circuit in LEAK_RACES:
        sc = Scenario(season, round_, standard_cars(), circuit=circuit)
        seq, target = season * 100 + round_, TARGET[0] * 100 + TARGET[1]
        for table, items in sc.rows().items():
            for r in items:
                r = dict(r)
                if (
                    scaled
                    and table == "laps"
                    and (seq > target or (seq == target and r["lap"] >= ATTEMPT_LAP))
                ):
                    r["lap_time_s"] = r["lap_time_s"] * 100.0
                if (
                    scaled
                    and table == "pitstops"
                    and (seq > target or (seq == target and r["lap"] >= ATTEMPT_LAP))
                ):
                    r["duration_s"] = r["duration_s"] * 100.0
                    r["is_long_stop"] = r["duration_s"] > 60.0
                rows[table].append(r)
    return {t: frame(spark, t, items) for t, items in rows.items()} | {
        t: frame(spark, t, []) for t in ("telemetry_lap", "telemetry_raw")
    }


def _features_by_race(spark: SparkSession, scaled: bool) -> dict[tuple[Any, ...], dict[str, Any]]:
    clean = _leak_tables(spark, scaled)
    targets = {(s, r) for s, r, _ in LEAK_RACES}
    df = compute_features(spark, clean, targets)["undercut_attempts"]
    return {
        (
            r["season"],
            r["round"],
            r["attacker_id"],
            r["lap"],
            r["gap_threshold_s"],
            r["response_window_laps"],
        ): {c: r[c] for c in U.DECISION_FEATURES}
        for r in df.collect()
    }


def test_leakage_decision_features_ignore_later_data(spark: SparkSession) -> None:
    base, scaled = _features_by_race(spark, False), _features_by_race(spark, True)
    target = {k: v for k, v in base.items() if k[:2] == TARGET}
    assert target, "the target race must have attempts"
    for key, features in target.items():
        assert key in scaled, f"attempt {key} disappeared after scaling later data"
        assert scaled[key] == features, key
    # the history is real: the target uses priors from the two 2023 races
    sample = next(iter(target.values()))
    assert sample["pit_loss_prior_s"] is not None
    assert sample["attacker_expected_outlap_pace_source"] != "none"
    # sanity: scaling reaches the later race's features
    later = [k for k in base if k[:2] == (2024, 2)]
    assert later and any(base[k] != scaled.get(k) for k in later)


# ------------------------------------------------------------------ pure-Python edge cases
def _default(rows: list[dict[str, Any]], attacker: str = "a") -> dict[str, Any]:
    (r,) = [x for x in rows if x["is_default_params"] and x["attacker_id"] == attacker]
    return r


def _eval(sc: Scenario, pit_loss: float | None = 21.0) -> list[dict[str, Any]]:
    return U.evaluate_race(race_data(sc, pit_loss))


def test_teammates_are_excluded() -> None:
    cars = standard_cars(a_team="team_d")
    r = _default(_eval(Scenario(2023, 1, cars)))
    assert r["outcome_category"] == "excluded_teammates" and not r["detector_eligible"]


def test_early_lap_stop() -> None:
    r = _default(_eval(Scenario(2023, 1, standard_cars(a_pit_laps=[3], d_pit_laps=[4]))))
    assert r["outcome_category"] == "excluded_early_lap" and r["lap"] == 3


def test_lap_one_stop_is_never_a_candidate() -> None:
    rows = _eval(Scenario(2023, 1, standard_cars(a_pit_laps=[1], d_pit_laps=[2])))
    assert not [r for r in rows if r["attacker_id"] == "a"]


def test_same_lap_stop_is_recorded() -> None:
    r = _default(_eval(Scenario(2023, 1, standard_cars(d_pit_laps=[6]))))
    assert r["outcome_category"] == "excluded_same_lap_stop"


def test_race_end_window() -> None:
    cars = standard_cars(a_pit_laps=[11], d_pit_laps=[12])
    for c in cars:
        if c.driver in ("lead", "back"):
            c.pit_laps = []  # keep d directly ahead of a at the end of lap 10
    r = _default(_eval(Scenario(2023, 1, cars)))  # 14 - 11 = 3 < k + 2 = 4
    assert r["defender_id"] == "d"
    assert r["outcome_category"] == "excluded_race_end"


def test_attacker_restop() -> None:
    r = _default(_eval(Scenario(2023, 1, standard_cars(a_pit_laps=[6, 8], d_pit_laps=[8]))))
    assert "attacker_restop" in r["exclusion_reasons"]


def test_long_stop() -> None:
    r = _default(_eval(Scenario(2023, 1, standard_cars(a_stop_s=75.0))))
    assert r["outcome_category"] == "excluded_long_stop"


def test_slow_stop_is_flagged_not_excluded() -> None:
    r = _default(_eval(Scenario(2023, 1, standard_cars(a_stop_s=27.0))))
    assert r["outcome_attacker_slow_stop"] is True
    assert r["outcome_category"] in ("success", "fail")


def test_suspected_drive_through() -> None:
    r = _default(_eval(Scenario(2023, 1, standard_cars(a_stop_s=19.0))))
    assert r["outcome_category"] == "excluded_penalty_suspected"


def test_incident_lap_in_window() -> None:
    r = _default(_eval(Scenario(2023, 1, standard_cars(d_base_by_lap={6: 97.0}))))
    assert "incident" in r["exclusion_reasons"]


def test_wet_tyres() -> None:
    sc = Scenario(2023, 1, standard_cars())
    data = race_data(sc)
    wet = [
        type(r)(**{**r.__dict__, "compound": "INTERMEDIATE"})
        if r.driver_id == "d" and r.lap == 8
        else r
        for r in data.laps
    ]
    rows = U.evaluate_race(
        U.RaceData(data.season, data.round, wet, data.stops, data.race_laps, None)
    )
    assert _default(rows)["outcome_category"] == "excluded_wet"


def test_neutralised_decision_lap_and_restart_lap() -> None:
    at_decision = _default(_eval(Scenario(2023, 1, standard_cars(), neutral_laps={5})))
    assert at_decision["outcome_category"] == "excluded_neutralised"
    assert not at_decision["detector_eligible"]
    restart = _default(_eval(Scenario(2023, 1, standard_cars(), neutral_laps={4})))
    assert restart["outcome_category"] == "excluded_neutralised"  # lap 5 is the restart lap
    assert not restart["detector_eligible"]


def test_defender_lapped_at_decision() -> None:
    cars = standard_cars()
    slow_d = Car("d", "team_d", 100.0, 97.0, pit_laps=[7], fresh_gain=2.5)
    slow_a = Car("a", "team_a", 100.0, 98.5, pit_laps=[6], fresh_gain=2.5)
    lead = Car("lead", "team_l", 80.0, 94.0, pit_laps=[])
    sc = Scenario(2023, 1, [lead, slow_d, slow_a, cars[3]], n_laps=14)
    for r in _eval(sc):
        if r["attacker_id"] == "a" and r["is_default_params"]:
            assert "lapped" in r["exclusion_reasons"]
            break
    else:
        pytest.fail("no attempt for a")


def test_rejoin_traffic_counts_cars_ahead_of_the_rejoin_point() -> None:
    # back is 3 s behind a; with a pit loss of 4 s, a rejoins 1 s behind back
    r = _default(_eval(Scenario(2023, 1, standard_cars()), pit_loss=4.0))
    assert (r["rejoin_traffic_cars"], r["rejoin_nearest_ahead_s"]) == (1, 1.0)
    none = _default(_eval(Scenario(2023, 1, standard_cars()), pit_loss=None))
    assert none["rejoin_traffic_cars"] is None


def test_double_stack_flags() -> None:
    cars = standard_cars()
    mate = Car("mate", "team_a", 90.0, 97.8, pit_laps=[6])
    rows = _eval(Scenario(2023, 1, [*cars, mate]))
    r = next(x for x in rows if x["attacker_id"] == "a" and x["is_default_params"])
    assert r["outcome_attacker_double_stack"] is True


def test_evaluate_race_rejects_bad_grid_and_duplicates() -> None:
    data = race_data(Scenario(2023, 1, standard_cars()))
    with pytest.raises(ValueError, match="must not be empty"):
        U.evaluate_race(data, gap_grid=())
    with pytest.raises(ValueError, match=">= 1"):
        U.evaluate_race(data, window_grid=(0,))
    with pytest.raises(ValueError, match="duplicate lap row"):
        U.RaceData(2023, 1, [data.laps[0], data.laps[0]], [])


def test_exclusion_priority_covers_rule_reasons() -> None:
    assert set(RULE_REASONS) <= set(U.EXCLUSION_PRIORITY)
    assert U.OUTCOME_CATEGORIES[:3] == ("success", "fail", "defender_stayed_out")


def test_snapshot_at_matches_detector_snapshot() -> None:
    data = race_data(Scenario(2023, 1, standard_cars()))
    snap = U.snapshot_at(data, 5)
    assert snap.track_green and not snap.restart_lap
    assert {c.driver_id for c in snap.drivers} == {"lead", "d", "a", "back"}
    (cand,) = [c for c in detect(snap) if c.attacker_id == "a"]
    assert (cand.defender_id, cand.gap_s) == ("d", 1.5)


# ------------------------------------------------------------------ counts CSV
def test_counts_and_csv_are_deterministic(
    spark: SparkSession, built: dict[str, Any], tmp_path: Path
) -> None:
    rows = U.undercut_counts(built["df"], "sample")
    assert {(r["gap_threshold_s"], r["response_window_laps"]) for r in rows} == {
        (f"{g:.1f}", k) for g in GAP_GRID for k in WINDOW_GRID
    }
    default = next(
        r for r in rows if r["gap_threshold_s"] == "3.5" and r["response_window_laps"] == 2
    )
    assert default["success"] == 2 and default["fail"] == 2  # success + late_cover, fail + gap_3s
    assert default["labelled"] == 4 and default["success_rate"] == "0.5000"
    assert sum(default[f"n_{c}"] for c in U.OUTCOME_CATEGORIES) == default["candidates"]
    one, two = tmp_path / "a.csv", tmp_path / "b.csv"
    U.write_counts_csv(rows, one, smoke_test=True)
    U.write_counts_csv(U.undercut_counts(built["df"], "sample"), two, smoke_test=True)
    assert one.read_bytes() == two.read_bytes()
    lines = one.read_text().splitlines()
    assert lines[0].startswith("# SMOKE TEST ONLY") and lines[1].split(",")[:2] == [
        "source",
        "smoke_test_only",
    ]
    assert all(line.split(",")[1] == "true" for line in lines[2:])


def test_detector_module_does_not_import_spark() -> None:
    code = (
        "import sys, f1sa.streaming.detector, f1sa.features.undercut_rule; "
        "bad = [m for m in sys.modules if m.split('.')[0] in ('pyspark', 'pandas', 'py4j')]; "
        "assert not bad, bad"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_attempts_stay_one_row_per_key(built: dict[str, Any]) -> None:
    keys = [tuple(r[k] for k in FS.FEATURE_KEYS["undercut_attempts"]) for r in built["rows"]]
    assert len(keys) == len(set(keys))
    assert built["df"].where(F.col("attacker_id") == F.col("defender_id")).count() == 0
