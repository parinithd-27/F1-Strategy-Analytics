"""Alert policy: opportunity on a new candidate or an upward threshold crossing, attempt on pit."""

from __future__ import annotations

import pytest

from f1sa.streaming import alerts as AL
from f1sa.streaming.events import UndercutAlert
from f1sa.streaming.live_features import RacePriors
from f1sa.streaming.live_state import Decision, EligibleRecord, RaceState

S, R = 2024, 5


def state() -> RaceState:
    return RaceState(S, R, 1, RacePriors(S, R))


def dec(lap: int, p: float, a: str = "a", d: str = "d") -> Decision:
    return Decision(S, R, lap, a, d, 1.2, {"gap_s": 1.2}, p)


def kinds(alerts: list[UndercutAlert]) -> list[tuple[str, str, int]]:
    return [(x.reason, x.trigger, x.decision_lap) for x in alerts]


@pytest.mark.parametrize(
    "prev, defender, p, out",
    [
        (None, "d", 0.1, "new_candidate"),
        (EligibleRecord("x", 0.9, 1.0, {}), "d", 0.1, "new_candidate"),  # new defender
        (EligibleRecord("d", 0.4, 1.0, {}), "d", 0.5, "probability_crossed"),
        (EligibleRecord("d", 0.4, 1.0, {}), "d", 0.49, None),
        (EligibleRecord("d", 0.6, 1.0, {}), "d", 0.7, None),  # already above
        (EligibleRecord("d", 0.6, 1.0, {}), "d", 0.3, None),  # downward
    ],
)
def test_opportunity_trigger(
    prev: EligibleRecord | None, defender: str, p: float, out: str | None
) -> None:
    assert AL.opportunity_trigger(prev, defender, p, 0.5) == out


def test_a_run_of_laps_alerts_only_on_changes() -> None:
    st = state()
    got = []
    for lap, p in [(10, 0.2), (11, 0.3), (12, 0.6), (13, 0.7), (14, 0.4), (15, 0.55)]:
        got += AL.on_decision(st, dec(lap, p))
    assert kinds(got) == [
        ("opportunity", "new_candidate", 10),
        ("opportunity", "probability_crossed", 12),
        ("opportunity", "probability_crossed", 15),
    ]
    # a gap of one lap makes the pair new again
    assert kinds(AL.on_decision(st, dec(17, 0.9))) == [("opportunity", "new_candidate", 17)]


def test_attempt_when_the_pit_arrives_after_the_decision() -> None:
    st = state()
    AL.on_decision(st, dec(10, 0.3))
    st.in_laps["a"] = {11}
    alert = AL.on_pit_in(st, "a", 11)
    assert alert is not None and (alert.reason, alert.trigger, alert.attempt_lap) == (
        "attempt",
        "pit_in",
        11,
    )
    assert alert.undercut_prob == 0.3 and alert.defender_id == "d"
    assert AL.on_pit_in(st, "a", 11) is None  # once per attacker and lap


def test_attempt_when_the_decision_arrives_after_the_pit() -> None:
    st = state()
    st.in_laps["a"] = {11}
    assert AL.on_pit_in(st, "a", 11) is None  # decision not evaluated yet
    assert kinds(AL.on_decision(st, dec(10, 0.3))) == [
        ("opportunity", "new_candidate", 10),
        ("attempt", "pit_in", 10),
    ]
    assert AL.on_pit_in(st, "a", 11) is None


def test_no_attempt_without_a_candidate_at_l_minus_1() -> None:
    st = state()
    AL.on_decision(st, dec(8, 0.3))
    st.in_laps["a"] = {11}
    assert AL.on_pit_in(st, "a", 11) is None


def test_threshold_is_configurable() -> None:
    st = state()
    AL.on_decision(st, dec(10, 0.2), threshold=0.25)
    out = AL.on_decision(st, dec(11, 0.3), threshold=0.25)
    assert kinds(out) == [("opportunity", "probability_crossed", 11)] and out[0].threshold == 0.25
