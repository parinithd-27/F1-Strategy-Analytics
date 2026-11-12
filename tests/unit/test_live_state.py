"""RaceState: ingest, watermark, when a decision may be evaluated, live order, (de)serialisation."""

from __future__ import annotations

import math
from typing import Any

import pytest

from f1sa.features.undercut_rule import RuleParams
from f1sa.streaming.events import RaceEvent, new_event_id
from f1sa.streaming.live_features import RacePriors
from f1sa.streaming.live_state import DEFAULT_LAP_S, RETIRE_TIMEOUT_LAPS, RaceState

S, R = 2024, 5


def state(loss: float | None = 20.0, n: int = 2) -> RaceState:
    return RaceState(
        S, R, n, RacePriors(S, R, pit_loss_prior_s=loss, pit_loss_source="global_prior")
    )


def lap_ev(
    driver: str, lap: int, pos: int, t: float, lap_time: float = 90.0, **kw: Any
) -> RaceEvent:
    return RaceEvent(
        new_event_id(),
        "lap",
        S,
        R,
        t,
        driver_id=driver,
        lap=lap,
        lap_time_s=lap_time,
        position=pos,
        **kw,
    )


def end_ev() -> RaceEvent:
    return RaceEvent(new_event_id(), "session_end", S, R, 10_000.0)


def test_apply_ignores_duplicates_and_events_after_the_end() -> None:
    st = state()
    ev = lap_ev("a", 1, 1, 90.0)
    assert st.apply(ev, 0) and not st.apply(ev, 0)
    assert st.counters.duplicates == 1 and st.counters.applied == 1
    st.ended = True
    assert not st.apply(lap_ev("b", 1, 2, 91.0), 1)
    assert st.counters.after_end == 1


def test_apply_refuses_other_races_and_bad_partitions() -> None:
    st = state()
    other = RaceEvent(new_event_id(), "lap", S, 6, 1.0, driver_id="a", lap=1)
    with pytest.raises(ValueError, match="applied to"):
        st.apply(other, 0)
    with pytest.raises(ValueError, match="partition"):
        st.apply(lap_ev("a", 1, 1, 90.0), 2)


def test_watermark_is_the_slowest_partition_and_session_end_closes_one() -> None:
    st = state(n=2)
    assert st.watermark == -math.inf
    st.apply(lap_ev("a", 1, 1, 90.0), 0)
    assert st.watermark == -math.inf  # partition 1 has delivered nothing yet
    st.apply(lap_ev("b", 1, 2, 91.0), 1)
    st.apply(lap_ev("a", 2, 1, 180.0), 0)
    assert st.watermark == 91.0
    st.apply(end_ev(), 1)
    assert st.watermark == 180.0 and not st.session_complete
    st.apply(end_ev(), 0)
    assert st.watermark == math.inf and st.session_complete


def test_pit_events_set_the_in_and_out_laps() -> None:
    st = state()
    st.apply(RaceEvent(new_event_id(), "pit_in", S, R, 100.0, driver_id="a", lap=6), 0)
    st.apply(RaceEvent(new_event_id(), "pit_out", S, R, 120.0, driver_id="a", lap=7), 0)
    assert st.in_laps == {"a": {6}} and st.out_laps == {"a": {7}}
    ins, outs = st.frozen_pits()
    assert ins == {"a": frozenset({6})} and outs == {"a": frozenset({7})}


def test_context_flags_and_race_laps_are_kept() -> None:
    st = state()
    st.apply(lap_ev("a", 3, 1, 270.0, track_neutral=True, track_red_flag=True, race_laps=50), 0)
    st.apply(lap_ev("b", 3, 2, 271.0, track_neutral=False), 1)
    assert st.lap_neutral == {3: True} and st.red_flag_laps == {3} and st.race_laps == 50


def test_a_decision_waits_for_the_rejoin_horizon() -> None:
    st = state(loss=20.0)
    st.apply(lap_ev("a", 5, 1, 450.0), 0)
    st.apply(lap_ev("b", 5, 2, 451.0), 1)
    assert st.ready() == []  # watermark 450 < a + loss = 470
    st.apply(lap_ev("a", 6, 1, 540.0), 0)  # partition 0 far ahead
    st.apply(lap_ev("c", 5, 3, 470.0), 1)  # partition 1 at 470 = a + 20
    assert (5, "a") in st.ready()
    assert (5, "b") not in st.ready()  # b + 20 = 471 > watermark 470
    assert st.ready(final=True) == sorted(st.pending)


def test_no_prior_pit_loss_means_no_horizon() -> None:
    st = state(loss=None, n=1)
    st.apply(lap_ev("a", 5, 1, 450.0), 0)
    assert st.ready() == [(5, "a")]


def test_a_missing_car_ahead_delays_until_the_timeout() -> None:
    st = state(loss=0.0, n=1)
    st.apply(lap_ev("lead", 5, 1, 450.0, lap_time=90.0), 0)
    st.apply(lap_ev("a", 5, 3, 452.0), 0)  # position 2 has not arrived
    assert (5, "a") not in st.ready()
    st.apply(lap_ev("lead", 6, 1, 452.0 + RETIRE_TIMEOUT_LAPS * 90.0, lap_time=90.0), 0)
    assert (5, "a") in st.ready()


def test_a_pending_car_outside_the_window_does_not_block() -> None:
    st = state(loss=0.0, n=1)
    for d, pos, t in (("d", 1, 90.0), ("a", 2, 91.5), ("x", 3, 0.0)):
        st.apply(lap_ev(d, 4, pos, t or 1.0, lap_time=t or 1.0), 0)
    # lap 5: d and a cross; x's lap-4 crossing (cum 1.0) is not between them, so nothing waits
    st.apply(lap_ev("d", 5, 1, 180.0), 0)
    st.apply(lap_ev("a", 5, 2, 181.5), 0)
    assert (5, "a") in st.ready()


def test_pending_car_inside_the_window_waits_then_times_out() -> None:
    st = state(loss=0.0, n=1)
    st.apply(lap_ev("d", 4, 1, 360.0, lap_time=360.0), 0)
    st.apply(lap_ev("x", 4, 3, 450.5, lap_time=450.5), 0)  # x crosses lap 4 at 450.5 ...
    st.apply(lap_ev("a", 4, 2, 361.5, lap_time=361.5), 0)
    st.apply(lap_ev("d", 5, 1, 450.0, lap_time=90.0), 0)
    st.apply(lap_ev("a", 5, 2, 451.5, lap_time=90.0), 0)  # ... between d and a at lap 5
    assert (5, "a") not in st.ready()
    st.apply(lap_ev("x", 5, 3, 541.0, lap_time=90.5), 0)  # x arrives: no longer pending
    assert (5, "a") in st.ready()


def test_pending_car_timeout_uses_the_default_lap_without_a_leader_time() -> None:
    st = state(loss=0.0, n=1)
    st.apply(lap_ev("d", 4, 1, 360.0, lap_time=360.0), 0)
    st.apply(lap_ev("x", 4, 3, 450.5, lap_time=450.5), 0)
    st.apply(lap_ev("a", 4, 2, 361.5, lap_time=361.5), 0)
    st.apply(RaceEvent(new_event_id(), "lap", S, R, 450.0, driver_id="d", lap=5, position=1), 0)
    st.apply(lap_ev("a", 5, 2, 451.5), 0)
    assert (5, "a") not in st.ready()
    st.apply(lap_ev("d", 6, 1, 450.5 + RETIRE_TIMEOUT_LAPS * DEFAULT_LAP_S), 0)
    assert (5, "a") in st.ready()


def test_evaluate_marks_the_decision_and_counts_inconsistent_snapshots() -> None:
    st = state(n=1)
    st.apply(lap_ev("a", 5, 1, 450.0), 0)
    st.apply(lap_ev("b", 5, 1, 451.0), 0)  # two cars on P1
    assert st.evaluate(5, "b", RuleParams()) is None
    assert st.counters.inconsistent_snapshots == 1
    assert (5, "b") in st.evaluated and (5, "b") not in st.pending


def test_set_flag_keeps_the_latest_decision_lap() -> None:
    st = state()
    st.set_flag("a", 10, True, 0.4)
    st.set_flag("a", 9, False, None)
    assert st.flags["a"] == (10, True, 0.4)
    st.set_flag("a", 11, False, None)
    assert st.flags["a"] == (11, False, None)


def test_live_order_is_unique_and_complete() -> None:
    st = state(n=1)
    st.apply(lap_ev("a", 2, 1, 180.0), 0)
    st.apply(lap_ev("b", 2, 2, 181.0), 0)
    st.apply(lap_ev("c", 1, 3, 95.0), 0)  # retired or lapped: fewer laps
    st.apply(lap_ev("b", 3, 1, 270.0), 0)  # b completes lap 3 first
    assert st.live_order() == {"b": 1, "a": 2, "c": 3}


def test_json_round_trip_restores_everything() -> None:
    st = state()
    st.apply(
        lap_ev("a", 5, 1, 450.0, compound="SOFT", tyre_age=4, track_neutral=True, race_laps=50), 0
    )
    st.apply(RaceEvent(new_event_id(), "pit_in", S, R, 451.0, driver_id="a", lap=5), 1)
    st.apply(end_ev(), 1)
    st.evaluate(5, "a", RuleParams())
    st.set_flag("a", 5, True, 0.25)
    back = RaceState.from_json(st.to_json())
    assert back.to_json() == st.to_json()
    assert back.timing() == st.timing() and back.watermark == st.watermark


def test_priors_must_belong_to_the_race() -> None:
    with pytest.raises(ValueError, match="another race"):
        RaceState(S, R, 1, RacePriors(S, 6))
    with pytest.raises(ValueError, match="n_partitions"):
        RaceState(S, R, 0, RacePriors(S, R))


def test_an_attacker_is_evaluated_lap_by_lap_in_order() -> None:
    st = state(loss=0.0, n=1)
    st.apply(lap_ev("lead", 4, 1, 360.0, lap_time=360.0), 0)
    st.apply(lap_ev("x", 4, 3, 450.5, lap_time=450.5), 0)  # x will block a at lap 5
    st.apply(lap_ev("a", 4, 2, 361.5, lap_time=361.5), 0)
    st.apply(lap_ev("lead", 5, 1, 450.0, lap_time=90.0), 0)
    st.apply(lap_ev("a", 5, 2, 451.5, lap_time=90.0), 0)
    st.apply(lap_ev("lead", 6, 1, 540.0, lap_time=90.0), 0)
    st.apply(lap_ev("a", 6, 2, 541.5, lap_time=90.0), 0)
    st.apply(lap_ev("lead", 7, 1, 630.0, lap_time=90.0), 0)
    ready = st.ready()
    assert (5, "a") not in ready and (6, "a") not in ready  # lap 6 waits for lap 5
    assert (6, "lead") in ready


def test_alert_memory_survives_a_round_trip() -> None:
    from f1sa.streaming.live_state import EligibleRecord

    st = state()
    st.eligible[(9, "a")] = EligibleRecord("d", 0.4, 1.2, {"gap_s": 1.2, "pit_loss_source": None})
    st.attempts_alerted.add(("a", 10))
    back = RaceState.from_json(st.to_json())
    assert back.eligible == st.eligible and back.attempts_alerted == st.attempts_alerted
