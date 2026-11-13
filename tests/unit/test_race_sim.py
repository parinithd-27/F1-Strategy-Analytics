"""Monte Carlo core (``sim/race_sim.py``): timelines, determinism, monotonicity, traffic, edges."""

from __future__ import annotations

import itertools
import json
import time
from dataclasses import asdict, replace

import numpy as np
import pytest
from _sim_fixtures import car, decision_state, simple_tyres

from f1sa.sim import race_sim as R


def _dump(result: R.SimResult) -> str:
    return json.dumps(asdict(result), sort_keys=True)


# ------------------------------------------------------------------ timelines
@pytest.mark.parametrize(
    ("strategy", "response", "expected"),
    [
        ("pit_now", "primary", (21, 22, 23)),
        ("pit_now", "sensitivity", (21, 23, 24)),
        ("pit_next_lap", "primary", (22, 23, 24)),
        ("pit_next_lap", "sensitivity", (22, 24, 25)),
        ("stay_out", "primary", (23, 22, 24)),
        ("stay_out", "sensitivity", (24, 23, 25)),
    ],
)
def test_timelines(
    strategy: R.Strategy, response: R.Response, expected: tuple[int, int, int]
) -> None:
    assert R.timeline(strategy, response, 20) == expected


def test_timeline_rejects_bad_input() -> None:
    with pytest.raises(R.SimError):
        R.timeline("pit_now", "primary", 20, k=0)
    with pytest.raises(R.SimError):
        R.timeline("box", "primary", 20)  # type: ignore[arg-type]


# ------------------------------------------------------------------ results
def test_run_all_returns_every_strategy_and_response() -> None:
    res = R.run_all(decision_state())
    assert [(r.strategy, r.response) for r in res.results] == [
        (s, rsp) for rsp in R.RESPONSES for s in R.STRATEGIES
    ]
    for r in res.results:
        assert r.feasible and r.p_attacker_ahead is not None and 0.0 <= r.p_attacker_ahead <= 1.0
        assert (
            r.interval80_s is not None
            and r.interval80_s[0] <= r.expected_gap_s <= r.interval80_s[1]
        )  # type: ignore[operator]
        assert r.mc_se == pytest.approx(
            (r.p_attacker_ahead * (1 - r.p_attacker_ahead) / 500) ** 0.5
        )
    assert res.get("pit_now").attacker_new_compound == "HARD"
    with pytest.raises(KeyError):
        res.get("box")  # type: ignore[arg-type]


def test_same_seed_same_bytes_and_other_seed_differs() -> None:
    st = decision_state()
    a, b = R.run_all(st, seed=7), R.run_all(st, seed=7)
    assert _dump(a) == _dump(b)
    c = R.run_all(st, seed=8)
    assert _dump(a) != _dump(c)
    for x, y in zip(a.results, c.results, strict=True):
        assert abs((x.p_attacker_ahead or 0) - (y.p_attacker_ahead or 0)) < 0.1


def test_single_strategy_equals_run_all_common_random_numbers() -> None:
    st = decision_state()
    allr = R.run_all(st, seed=3)
    one = R.simulate(st, "pit_next_lap", "sensitivity", seed=3)
    assert one == allr.get("pit_next_lap", "sensitivity")


def test_no_noise_is_the_deterministic_calculation() -> None:
    """With zero noise the gap is exact: equal cars, MEDIUM->HARD, linear tyres."""
    tyres = simple_tyres(beta=0.05, phi=0.0, offsets={"MEDIUM": 0.0, "HARD": 0.0}, stop_sd=0.0)
    d = car("def", 1000.0, sd=0.0, age=20)
    a = car("att", 1001.0, sd=0.0, age=20)
    st = decision_state(attacker=a, defender=d, tyres=tyres)
    r = R.simulate(st, "pit_now", "primary")
    # lap 21: A in-lap age 21 vs D age 21 -> equal; lap 22: A out-lap age 1 vs D in-lap age 22
    # lap 23: A age 2 vs D out-lap age 1. Both pay 22 s. Gain = 0.05*(22-1) + 0.05*(1-2) = 1.0
    assert r.expected_gap_s == pytest.approx(-1.0 + 1.0, abs=1e-9)
    assert r.interval80_s == pytest.approx((0.0, 0.0), abs=1e-9)


# ------------------------------------------------------------------ monotonicity
@pytest.mark.parametrize("strategy", R.STRATEGIES)
def test_more_attacker_loss_never_helps_pathwise(strategy: R.Strategy) -> None:
    st = decision_state(traffic=[R.ThirdCar("z", 1960.0, 90.0), R.ThirdCar("y", 2010.0, 91.0)])
    ps = [
        R.simulate(st, strategy, "primary", attacker_loss_shift_s=d).p_attacker_ahead
        for d in (-2.0, -1.0, 0.0, 1.0, 2.0, 4.0)
    ]
    assert all(x >= y for x, y in itertools.pairwise(ps)), ps  # type: ignore[operator]


def test_larger_gap_never_helps() -> None:
    ps = [
        R.simulate(decision_state(g), "pit_now", "primary").p_attacker_ahead
        for g in (0.5, 1, 2, 3.5, 5)
    ]
    assert all(x >= y for x, y in itertools.pairwise(ps)), ps  # type: ignore[operator]
    assert ps[0] > ps[-1]  # type: ignore[operator]


def test_traffic_hold_is_monotone_and_holds_exactly_one_window() -> None:
    z = [R.ThirdCar("z", 100.0, 90.0)]
    t = np.linspace(185.0, 195.0, 2001)
    held = R.hold_in_traffic(t, z)
    assert np.all(np.diff(held) >= 0.0)
    # crossings of z at 190.0: arrivals in (190, 191) are held to 191
    assert held[(t > 190.0) & (t < 191.0)] == pytest.approx(191.0)
    assert np.array_equal(held[(t <= 190.0) | (t >= 191.0)], t[(t <= 190.0) | (t >= 191.0)])
    assert np.array_equal(R.hold_in_traffic(t, []), t)


def test_traffic_hold_chains_through_a_train_of_cars() -> None:
    train = [R.ThirdCar("a", 100.0, 1000.0), R.ThirdCar("b", 100.6, 1000.0)]
    assert R.hold_in_traffic(np.array([100.3]), train)[0] == pytest.approx(101.6)


def test_traffic_costs_the_rejoining_car() -> None:
    free = decision_state(1.5)
    # a car that crosses just before A's out-lap crossing of the no-traffic draws
    base = R.simulate(free, "pit_now", "primary")
    t_out = free.attacker.t_cross_s + 2 * 90.0 + 22.0
    jam = replace(free, traffic=(R.ThirdCar("z", t_out - 0.5 - 1000.0, 1000.0),))  # A only
    slowed = R.simulate(jam, "pit_now", "primary")
    assert slowed.expected_gap_s < base.expected_gap_s  # type: ignore[operator]


# ------------------------------------------------------------------ edges
def test_race_end_makes_a_strategy_infeasible() -> None:
    res = R.run_all(decision_state(current_lap=20, race_laps=23))
    assert res.get("pit_now").feasible
    for r in res.results:
        if r.evaluation_lap > 23:
            assert not r.feasible and r.reason == "race_end" and r.p_attacker_ahead is None
    assert not res.get("stay_out", "sensitivity").feasible


def test_no_race_distance_means_no_race_end_check() -> None:
    assert all(r.feasible for r in R.run_all(decision_state(race_laps=None)).results)


def test_missing_pit_loss_prior_is_infeasible() -> None:
    st = decision_state(attacker=car("att", 2001.5, loss=None))
    assert {r.reason for r in R.run_all(st).results} == {"no_pit_loss_prior"}


@pytest.mark.parametrize(
    ("n", "seed"), [(99, 1), (5001, 1), (True, 1), (500, -1), (500, 2**32), (500.0, 1)]
)
def test_n_and_seed_bounds(n: object, seed: object) -> None:
    with pytest.raises(R.SimError):
        R.run_all(decision_state(), n=n, seed=seed)  # type: ignore[arg-type]


def test_invalid_states_are_refused() -> None:
    with pytest.raises(R.SimError):
        car("x", 1.0, compound="SUPERSOFT")
    with pytest.raises(R.SimError):
        car("x", float("nan"))
    with pytest.raises(R.SimError):
        car("x", 1.0, n=0)
    with pytest.raises(R.SimError):
        R.ThirdCar("z", 1.0, 0.0)


def test_unknown_compound_uses_the_global_fit_and_stays_unknown() -> None:
    st = decision_state(attacker=car("att", 2001.5, compound="UNKNOWN"))
    assert R.simulate(st, "pit_now", "primary").attacker_new_compound == "UNKNOWN"


def test_five_hundred_draws_well_under_a_second() -> None:
    st = decision_state(
        traffic=[R.ThirdCar(f"z{i}", 1900.0 + 7 * i, 90.0 + i / 10) for i in range(18)]
    )
    t0 = time.perf_counter()
    R.run_all(st, n=500)
    assert time.perf_counter() - t0 < 1.0
