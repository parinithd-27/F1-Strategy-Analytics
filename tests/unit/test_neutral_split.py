"""Reporting-only split of excluded_neutralised: the timing rule, and labels unchanged by it."""

from __future__ import annotations

from typing import Any

import pytest
from _feature_fixtures import frame, lap
from _undercut_fixtures import Scenario, clean_tables, standard_cars
from pyspark.sql import SparkSession

from f1sa.etl import clean as C
from f1sa.features import neutral_split as N
from f1sa.features import undercut as U
from f1sa.features.build import compute_features


def _run(start: int, ratios: list[float | None], **kw: Any) -> list[N.TrackLap]:
    return [N.TrackLap(start + i, True, ratio=r, **kw) for i, r in enumerate(ratios)]


def test_long_slow_run_is_sustained_and_safety_car_pace_is_not() -> None:
    """2022 R4 shape: SC pace on laps 2-4, then 15 wet laps at 1.1-1.2x."""
    laps = _run(2, [1.80, 1.68, 1.63] + [1.18, 1.14, 1.12] * 5)
    kinds = N.classify_laps(laps)
    assert [kinds[n] for n in (2, 3, 4)] == [N.SC_LIKE] * 3
    assert {kinds[n] for n in range(5, 20)} == {N.SUSTAINED}


def test_long_safety_car_is_not_sustained() -> None:
    """2024 R5 shape: 11 SC laps, median 1.31x, several below 1.35x: all safety car."""
    laps = _run(21, [1.47, 1.40, 1.31, 1.29, 1.30, 1.38, 1.36, 1.31, 1.25, 1.12, 1.45])
    assert set(N.classify_laps(laps).values()) == {N.SC_LIKE}


def test_run_median_threshold_is_strict() -> None:
    at = N.classify_laps(_run(2, [N.SUSTAINED_MAX_RUN_MEDIAN] * 9))
    below = N.classify_laps(_run(2, [N.SUSTAINED_MAX_RUN_MEDIAN - 0.01] * 9))
    assert set(at.values()) == {N.SC_LIKE} and set(below.values()) == {N.SUSTAINED}


def test_short_run_is_safety_car_even_at_wet_pace() -> None:
    kinds = N.classify_laps(_run(10, [1.15] * (N.SUSTAINED_MIN_LAPS - 1)))
    assert set(kinds.values()) == {N.SC_LIKE}


def test_run_with_a_red_flag_is_safety_car() -> None:
    laps = _run(10, [1.15] * 10)
    laps[4] = N.TrackLap(14, True, red_flag=True, ratio=None)
    assert set(N.classify_laps(laps).values()) == {N.SC_LIKE}


@pytest.mark.parametrize(
    "override",
    [{"ratio": None}, {"heuristic": False}, {"ratio": N.SUSTAINED_MAX_RATIO}],
)
def test_unknown_ratio_exact_flags_and_threshold_are_safety_car(override: dict[str, Any]) -> None:
    laps = _run(10, [1.15] * 10)
    laps[5] = N.TrackLap(15, True, **{"ratio": 1.15, **override})
    kinds = N.classify_laps(laps)
    assert kinds[15] == N.SC_LIKE
    assert kinds[14] == N.SUSTAINED


def test_green_laps_get_no_kind_and_split_runs() -> None:
    laps = [*_run(2, [1.2] * 8), N.TrackLap(10, False, ratio=1.0), *_run(11, [1.2] * 3)]
    kinds = N.classify_laps(laps)
    assert 10 not in kinds
    assert {kinds[n] for n in range(2, 10)} == {N.SUSTAINED}
    assert {kinds[n] for n in range(11, 14)} == {N.SC_LIKE}


def test_window_kind_mixed_sustained_restart_and_none() -> None:
    neutral = {n: n in range(2, 10) or n == 20 for n in range(1, 30)}
    kinds = {n: N.SUSTAINED for n in range(2, 10)} | {20: N.SC_LIKE}
    assert N.window_kind(kinds, neutral, frozenset(), range(8, 12)) == N.SUSTAINED
    assert N.window_kind(kinds, neutral, frozenset(), range(10, 11)) == N.SUSTAINED  # restart
    assert N.window_kind(kinds, neutral, frozenset(), range(9, 21)) == N.SC_LIKE
    assert N.window_kind(kinds, neutral, frozenset(), range(12, 19)) is None


def test_red_flag_restart_period_is_safety_car() -> None:
    neutral = {n: n == 5 for n in range(1, 20)}
    kinds = {5: N.SC_LIKE}
    for j in (6, 7, 8):
        assert N.window_kind(kinds, neutral, frozenset({5}), [j]) == N.SC_LIKE
    assert N.window_kind(kinds, neutral, frozenset({5}), [9]) is None
    assert N.window_kind(kinds, neutral, frozenset(), [7]) is None  # no red flag: 1-lap restart


def test_classify_attempts_uses_the_attempt_window() -> None:
    track = {(2023, 1): [N.TrackLap(n, n >= 10, ratio=1.2) for n in range(1, 25)]}
    base = {
        "season": 2023,
        "round": 1,
        "attacker_id": "a",
        "defender_id": "d",
        "gap_threshold_s": 3.5,
        "outcome_category": "excluded_neutralised",
    }
    attempts = [
        {**base, "lap": 9, "response_window_laps": 2},  # window 8..11
        {**base, "lap": 5, "response_window_laps": 2, "outcome_category": "success"},
    ]
    out = N.classify_attempts(attempts, track, {(2023, 1, "d"): frozenset({10})})
    assert out == {(2023, 1, 9, "a", "d", 3.5, 2): N.SUSTAINED}


def test_classify_attempts_without_any_flag_falls_back_to_safety_car() -> None:
    attempt = {
        "season": 2023,
        "round": 1,
        "lap": 6,
        "attacker_id": "a",
        "defender_id": "d",
        "gap_threshold_s": 3.5,
        "response_window_laps": 2,
        "outcome_category": "excluded_neutralised",
    }
    assert N.classify_attempts([attempt], {}, {}) == {(2023, 1, 6, "a", "d", 3.5, 2): N.SC_LIKE}


def test_attempt_window_and_defender_response_lap() -> None:
    assert U.defender_response_lap(6, 2, {7, 12}) == 7
    assert U.defender_response_lap(6, 2, {6, 9}) is None
    assert list(U.attempt_window(6, 2, {7})) == [5, 6, 7, 8]  # through D's out-lap
    assert list(U.attempt_window(6, 2, set())) == [5, 6, 7, 8]  # L+k
    assert list(U.attempt_window(6, 3, {9})) == [5, 6, 7, 8, 9, 10]


def test_track_ratios_exclude_lap_one_and_pit_laps(spark: SparkSession) -> None:
    rows = [
        lap(2023, 1, d, n, i + 1, 90.0 if n > 1 else 150.0, pit_in=(n == 3 and d == "a"))
        for n in range(1, 5)
        for i, d in enumerate("abc")
    ]
    out = {r["lap"]: r for r in C.track_ratios(frame(spark, "laps", rows)).collect()}
    assert out[1]["ratio"] is None and out[1]["field_ratio"] == pytest.approx(150 / 90)
    assert out[2]["ratio"] == pytest.approx(1.0) and out[2]["slow_share"] == 0.0
    assert out[3]["ratio"] == pytest.approx(1.0)  # the pit lap is left out of ratio


LONG_SLOW = Scenario(2023, 9, standard_cars(), neutral_laps=set(range(3, 13)))
SCENARIOS = [
    Scenario(2023, 1, standard_cars()),
    Scenario(2023, 4, standard_cars(), neutral_laps={7}),
    Scenario(2023, 5, standard_cars(), red_flag_laps={2}),
    LONG_SLOW,
]


@pytest.fixture(scope="module")
def built(spark: SparkSession) -> dict[str, Any]:
    clean = clean_tables(spark, SCENARIOS)
    tables = compute_features(spark, clean, {(s.season, s.round) for s in SCENARIOS})
    attempts = tables["undercut_attempts"].cache()
    return {"attempts": attempts, "clean": clean}


def test_split_leaves_labels_and_every_other_count_unchanged(built: dict[str, Any]) -> None:
    attempts, clean = built["attempts"], built["clean"]
    before = U.labels_checksum(attempts)
    plain = U.undercut_counts(attempts, "full")
    kinds = N.neutralised_kinds(attempts, clean["laps"], clean["pitstops"])
    split = U.undercut_counts(attempts, "full", kinds)
    assert U.labels_checksum(attempts) == before
    extra = {"n_excluded_neutralised_sc_vsc_red_flag", "n_excluded_neutralised_sustained_slow"}
    assert len(plain) == len(split)
    for p, s in zip(plain, split, strict=True):
        assert {k: v for k, v in s.items() if k not in extra} == p
        assert not extra & set(p)
        assert sum(s[c] for c in extra) == s["n_excluded_neutralised"]
    assert sum(s["n_excluded_neutralised"] for s in split) > 0


def test_split_kinds_on_the_scenarios(built: dict[str, Any]) -> None:
    attempts, clean = built["attempts"], built["clean"]
    kinds = N.neutralised_kinds(attempts, clean["laps"], clean["pitstops"])
    by_round: dict[int, set[str]] = {}
    for (_s, rnd, *_rest), kind in kinds.items():
        by_round.setdefault(rnd, set()).add(kind)
    assert by_round[4] == {N.SC_LIKE}  # one SC lap
    assert by_round[5] == {N.SC_LIKE}  # red-flag restart period
    assert N.SUSTAINED in by_round[9]  # 10 slow laps below 1.35x


def test_labels_checksum_ignores_features_but_not_labels(built: dict[str, Any]) -> None:
    from pyspark.sql import functions as F

    attempts = built["attempts"]
    base = U.labels_checksum(attempts)
    assert U.labels_checksum(attempts.withColumn("gap_s", F.lit(0.0))) == base
    flipped = attempts.withColumn("label", F.when(F.col("label") == 1, 0).otherwise(F.col("label")))
    assert U.labels_checksum(flipped) != base
