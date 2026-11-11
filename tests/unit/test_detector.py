"""The shared candidate rule (``features.undercut_rule``) and the real-time detector."""

from __future__ import annotations

from typing import Any

import pytest

from f1sa.features import undercut_rule as R
from f1sa.streaming.detector import UndercutCandidate, detect, snapshot_from_state


def _car(driver: str, pos: int, cross: float, **kw: Any) -> R.DriverLapState:
    """A car at the end of lap 9; gap_ahead_s is filled in by ``_snap``."""
    return R.DriverLapState(
        driver_id=driver,
        position=pos,
        gap_ahead_s=kw.pop("gap_ahead_s", None),
        gap_leader_s=kw.pop("gap_leader_s", cross - 810.0),
        crossing_s=cross,
        prev_crossing_s=kw.pop("prev_crossing_s", cross - 90.0),
        team_key=kw.pop("team_key", driver),
        **kw,
    )


def _snap(*cars: R.DriverLapState, lap: int = 9, **kw: Any) -> R.LapSnapshot:
    ordered = sorted(cars, key=lambda c: c.position or 0)
    filled = []
    for i, c in enumerate(ordered):
        if c.gap_ahead_s is None and i > 0 and c.crossing_s is not None:
            prev = ordered[i - 1].crossing_s
            gap = round(c.crossing_s - prev, 3) if prev is not None else None
            c = R.DriverLapState(**{**c.__dict__, "gap_ahead_s": gap})
        filled.append(c)
    kw.setdefault("leader_lap_time_s", 90.0)
    return R.LapSnapshot(2024, 5, lap, tuple(filled), **kw)


BASE = (_car("p1", 1, 810.0), _car("d", 2, 820.0), _car("a", 3, 821.5), _car("b", 4, 830.0))


def _pair(snap: R.LapSnapshot, attacker: str = "a", g: float = 3.5) -> R.CandidatePair | None:
    pairs = R.candidate_pairs(snap, R.RuleParams(gap_threshold_s=g))
    return next((p for p in pairs if p.attacker_id == attacker), None)


def test_eligible_pair_and_attempt_lap() -> None:
    p = _pair(_snap(*BASE))
    assert p is not None and p.eligible and p.reasons == ()
    assert (p.defender_id, p.gap_s, p.decision_lap, p.attempt_lap) == ("d", 1.5, 9, 10)


def test_gap_threshold_drops_pairs() -> None:
    assert _pair(_snap(*BASE), g=1.0) is None
    assert _pair(_snap(*BASE), g=1.5) is not None  # gap <= G is inclusive
    pairs = R.candidate_pairs(_snap(*BASE), R.RuleParams(gap_threshold_s=5.0))
    assert [p.attacker_id for p in pairs] == ["a"]  # d is 10 s behind p1, b 8.5 s behind a


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"a": {"team_key": "d"}}, R.TEAMMATES),
        ({"a": {"on_out_lap": True}}, R.IN_PIT),
        ({"d": {"on_in_lap": True}}, R.IN_PIT),
        ({"a": {"gap_leader_s": 95.0}}, R.LAPPED),
        ({"d": {"gap_leader_s": 90.0}}, R.LAPPED),
        ({"a": {"gap_ahead_s": -0.2}}, R.INCONSISTENT_TIMING),
        ({"a": {"gap_ahead_s": 0.0}}, R.INCONSISTENT_TIMING),
    ],
)
def test_rule_reasons(change: dict[str, dict[str, Any]], reason: str) -> None:
    cars = [R.DriverLapState(**{**c.__dict__, **change.get(c.driver_id, {})}) for c in BASE]
    p = _pair(_snap(*cars))
    assert p is not None and reason in p.reasons and not p.eligible


def test_missing_gap_and_crossings() -> None:
    a = R.DriverLapState("a", 3, None, crossing_s=None)
    p = _pair(_snap(BASE[0], BASE[1], a))
    assert p is not None and p.reasons == (R.MISSING_DATA,)
    # without gap_ahead_s the gap falls back to the crossing difference
    a2 = R.DriverLapState("a", 3, None, crossing_s=822.0, team_key="a")
    snap = R.LapSnapshot(2024, 5, 9, (BASE[0], BASE[1], a2), leader_lap_time_s=90.0)
    p2 = _pair(snap)
    assert p2 is not None and p2.gap_s == 2.0 and p2.eligible


def test_traffic_between_uses_previous_lap_crossing() -> None:
    lapped = _car("x", 5, 920.0, prev_crossing_s=821.0, gap_leader_s=110.0)
    p = _pair(_snap(*BASE, lapped))
    assert p is not None and p.reasons == (R.TRAFFIC_BETWEEN,)
    gone = R.DriverLapState(**{**lapped.__dict__, "running": False})
    p2 = _pair(_snap(*BASE, gone))
    assert p2 is not None and p2.eligible  # a retired car is not traffic


def test_neutralised_restart_and_early_lap() -> None:
    assert _pair(_snap(*BASE, track_green=False)).reasons == (R.NEUTRALISED,)  # type: ignore[union-attr]
    assert _pair(_snap(*BASE, restart_lap=True)).reasons == (R.NEUTRALISED,)  # type: ignore[union-attr]
    early = _pair(_snap(*BASE, lap=2))
    assert early is not None and R.EARLY_LAP in early.reasons
    assert R.RuleParams().min_attempt_lap == R.MIN_ATTEMPT_LAP == 5
    at4 = _pair(_snap(*BASE, lap=3))  # decision lap 3: attempt lap 4
    assert at4 is not None and at4.reasons == (R.EARLY_LAP,)
    at5 = _pair(_snap(*BASE, lap=4))  # decision lap 4: attempt lap 5
    assert at5 is not None and at5.eligible


def test_several_reasons_are_all_reported() -> None:
    cars = [R.DriverLapState(**{**c.__dict__, "team_key": "x"}) for c in BASE]
    p = _pair(_snap(*cars, track_green=False))
    assert p is not None and p.reasons == (R.TEAMMATES, R.NEUTRALISED)


def test_duplicate_positions_raise() -> None:
    dup = _car("z", 3, 825.0)
    with pytest.raises(ValueError, match="position 3"):
        R.candidate_pairs(_snap(*BASE, dup), R.RuleParams())


def test_non_running_and_unpositioned_cars_are_ignored() -> None:
    d_out = R.DriverLapState(**{**BASE[1].__dict__, "running": False})
    assert _pair(_snap(BASE[0], d_out, BASE[2], BASE[3])) is None  # nobody at P2
    nopos = R.DriverLapState("q", None, None)
    assert _pair(_snap(*BASE, nopos)) is not None


def test_rule_params_validation() -> None:
    with pytest.raises(ValueError, match="positive"):
        R.RuleParams(gap_threshold_s=0.0)
    with pytest.raises(ValueError, match="positive"):
        R.RuleParams(gap_threshold_s=float("nan"))
    with pytest.raises(ValueError, match=">= 2"):
        R.RuleParams(min_attempt_lap=1)
    assert R.DEFAULT_GAP_S in R.GAP_GRID and R.DEFAULT_WINDOW_LAPS in R.WINDOW_GRID


# ------------------------------------------------------------------ detector
STATE: dict[str, dict[str, Any]] = {
    "p1": {"position": 1, "crossing_s": 810.0, "gap_leader_s": 0.0, "last_lap_time_s": 90.0},
    "d": {
        "position": "2",
        "gap_ahead_s": "10.0",
        "gap_leader_s": 10.0,
        "crossing_s": 820.0,
        "tyre_compound": "MEDIUM",
        "tyre_age_laps": "14",
        "last_lap_time_s": 91.2,
        "team_key": "ferrari",
    },
    "a": {
        "position": 3,
        "gap_ahead_s": 1.5,
        "gap_leader_s": 11.5,
        "crossing_s": 821.5,
        "tyre_compound": "HARD",
        "tyre_age_laps": 9,
        "last_lap_time_s": "90.7",
        "team_key": "mclaren",
        "on_in_lap": "false",
    },
}


def test_detect_returns_eligible_candidates_with_context() -> None:
    snap = snapshot_from_state(2024, 5, 9, STATE, leader_lap_time_s=90.0)
    (c,) = detect(snap)
    assert c == UndercutCandidate(
        season=2024,
        round=5,
        attacker_id="a",
        defender_id="d",
        decision_lap=9,
        gap_s=1.5,
        tyre_age_delta_laps=5,
        attacker_compound="HARD",
        defender_compound="MEDIUM",
        last_lap_delta_s=-0.5,
    )
    assert c.attempt_lap == 10


def test_detect_drops_ineligible_and_respects_params() -> None:
    sc = snapshot_from_state(2024, 5, 9, STATE, track_green=False)
    assert detect(sc) == []
    snap = snapshot_from_state(2024, 5, 9, STATE)
    assert detect(snap, R.RuleParams(gap_threshold_s=1.0)) == []
    teammates = {**STATE, "a": {**STATE["a"], "team_key": "ferrari"}}
    assert detect(snapshot_from_state(2024, 5, 9, teammates)) == []


def test_detect_handles_missing_context_fields() -> None:
    minimal = {
        "d": {"position": 1, "crossing_s": 100.0},
        "a": {"position": 2, "gap_ahead_s": 1.0, "crossing_s": 101.0},
    }
    (c,) = detect(snapshot_from_state(2024, 5, 9, minimal))
    assert c.tyre_age_delta_laps is None and c.last_lap_delta_s is None


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("position", "first", "expected a number"),
        ("position", 2.5, "expected an integer"),
        ("position", 0, ">= 1"),
        ("gap_ahead_s", float("inf"), "finite"),
        ("gap_ahead_s", [1.0], "expected a number"),
        ("on_in_lap", "maybe", "expected a boolean"),
        ("running", True, None),
        ("team_key", 5, "expected a string"),
        ("tyre_age_laps", True, "expected a number"),
    ],
)
def test_snapshot_from_state_validation(field: str, value: Any, match: str | None) -> None:
    state = {**STATE, "a": {**STATE["a"], field: value}}
    if match is None:
        snapshot_from_state(2024, 5, 9, state)
        return
    with pytest.raises(ValueError, match=match):
        snapshot_from_state(2024, 5, 9, state)


def test_snapshot_from_state_lap_and_driver_ids() -> None:
    with pytest.raises(ValueError, match="lap must be >= 1"):
        snapshot_from_state(2024, 5, 0, STATE)
    with pytest.raises(ValueError, match="driver id"):
        snapshot_from_state(2024, 5, 9, {"": {"position": 1}})
    snap = snapshot_from_state(2024, 5, 9, STATE)
    assert [c.driver_id for c in snap.drivers] == ["a", "d", "p1"]  # sorted, deterministic
    assert {c.driver_id: c.running for c in snap.drivers} == {"a": True, "d": True, "p1": True}
