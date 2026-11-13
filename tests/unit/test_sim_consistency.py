"""What-if consistency checks, metrics, backtest cases and the record gating
(``sim/consistency.py``, ``sim/backtest.py``)."""

from __future__ import annotations

import csv
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from _sim_fixtures import car, decision_state, lap_records, simple_tyres

from f1sa.sim import backtest as B
from f1sa.sim import consistency as C
from f1sa.sim.race_sim import ThirdCar


def _case(
    gap: float = 1.5, label: int = 1, model_p: float = 0.4, season: int = 2022, **kw: Any
) -> C.Case:
    return C.Case(
        season,
        3,
        "att",
        "def",
        21,
        label,
        model_p,
        decision_state(gap, **kw),
        closing_s_per_lap=0.2,
    )


# ------------------------------------------------------------------ metrics
def test_brier_bins_ece_spearman() -> None:
    p, y = [0.1, 0.4, 0.95, 1.0], [0, 1, 1, 1]
    assert C.brier(p, y) == pytest.approx((0.01 + 0.36 + 0.0025 + 0.0) / 4)
    bins = C.calibration_bins(p, y, 5)
    assert [b[1] for b in bins] == [1, 0, 1, 0, 2]  # p = 1.0 lands in the last bin
    assert bins[4][2] == pytest.approx(0.975) and bins[1][2] is None
    assert C.ece(p, y, 5) == pytest.approx((0.1 + 0.6 + 2 * 0.025) / 4)
    assert C.spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert C.spearman([1, 2, 2, 3], [3, 2, 2, 1]) == pytest.approx(-1.0)
    assert C.spearman([1, 2], [1, 2]) is None and C.spearman([1, 1, 1], [1, 2, 3]) is None


# ------------------------------------------------------------------ checks
def test_monotonicity_passes_and_reports_curves() -> None:
    cases = [_case(g, traffic=[ThirdCar("z", 1990.0, 90.0)]) for g in (0.5, 1.5, 3.0)]
    rows, curves = C.monotonicity(cases, n=200)
    strict = next(
        r for r in rows if r.subset == "attacker_loss" and r.metric == "cases_with_an_increase"
    )
    assert strict.passed is True and strict.n == 3 and strict.value == 0
    info = next(r for r in rows if r.subset == "both_loss" and r.metric == "cases_with_an_increase")
    assert info.passed is None
    assert curves["attacker_loss"][0] >= curves["attacker_loss"][-1]
    assert len(curves["both_loss"]) == len(C.LOSS_SHIFTS_S)


def test_monotonicity_skips_infeasible_cases() -> None:
    rows, _ = C.monotonicity([_case(race_laps=21)], n=200)
    assert all(r.n == 0 for r in rows)


def test_determinism_rows() -> None:
    rows = C.determinism([_case()], n=200)
    assert rows[0].passed is True and rows[0].value == 1.0
    assert rows[1].value is not None and rows[1].value < 0.2


def test_ordering_grid_and_rows() -> None:
    rows, grid = C.ordering(simple_tyres(), n=500)
    assert len(grid) == len(C.GRID_GAPS_S) * len(C.GRID_RATES_S)
    mono = [r for r in rows if r.metric == "p_now_non_increasing_in_gap"]
    assert len(mono) == 3 and all(r.passed for r in mono)
    zero = [r for r in rows if r.check == "ordering" and r.subset.startswith("rate_+0.0_gap")]
    assert zero and all(r.passed is None for r in zero)
    closing = [r for r in rows if r.check == "ordering" and r.subset.startswith("rate_+0.5_gap")]
    assert all(r.passed for r in closing)


def test_grid_state_uses_the_global_fit_for_every_compound() -> None:
    t = simple_tyres(offsets={"SOFT": -1.0, "MEDIUM": 0.0, "HARD": 1.0})
    st = C.grid_state(t, 2.0, 0.5)
    g = st.tyres.fit_for("UNKNOWN")
    assert all(st.tyres.fit_for(c) == g for c in ("SOFT", "MEDIUM", "HARD"))
    assert st.gap_s == pytest.approx(2.0) and st.attacker.pace_level_s == -0.5


def test_ordering_on_states_counts_only_signed_cases() -> None:
    c = _case()
    rows = C.ordering_on_states([c, C.Case(**{**c.__dict__, "closing_s_per_lap": None})], n=200)
    assert rows[0].n <= 1 and rows[0].passed is None


def test_backtest_rows_and_stop_rule() -> None:
    cases = [_case(0.5, 1, 0.6), _case(4.0, 0, 0.2), _case(1.0, 1, 0.5, race_laps=21)]
    scored = C.score(cases, n=200)
    assert scored[2].p_sim["primary"] is None  # infeasible: race end
    rows = C.backtest(scored, "x")
    by = {(r.check, r.response, r.metric): r for r in rows}
    assert by[("backtest", "primary", "simulator_brier")].n == 2
    assert by[("backtest", "model", "model_brier")].n == 3
    assert by[("backtest", "all", "infeasible_cases")].value == 1.0
    mean, n = C.agreement(scored)
    assert n == 2 and by[("agreement", "primary", "mean_sim_minus_model")].value == pytest.approx(
        mean
    )
    far = [C.Scored(c, {"primary": c.model_p + 0.3, "sensitivity": None}) for c in cases]
    assert C.stop_check(far) == (pytest.approx(0.3), True)
    near = [C.Scored(c, {"primary": c.model_p - 0.1, "sensitivity": None}) for c in cases]
    assert C.stop_check(near)[1] is False
    assert C.stop_check([])[1] is False
    assert C.backtest([], "empty")[0].value is None


def test_in_seasons() -> None:
    s = C.score([_case(season=2021), _case(season=2024)], n=200)
    assert [x.case.season for x in C.in_seasons(s, (2024,))] == [2024]


# ------------------------------------------------------------------ outputs and gating
def test_csv_and_png(tmp_path: Path) -> None:
    rows = [C.Row("a", "b", "c", "m", 0.5, 3, True), C.Row("a", "b", "c", "m2", None, 0)]
    C.write_csv(rows, tmp_path / "x" / "out.csv")
    got = list(csv.DictReader((tmp_path / "x" / "out.csv").open()))
    assert got[0] == {
        "check": "a",
        "subset": "b",
        "response": "c",
        "metric": "m",
        "value": "0.5",
        "n": "3",
        "pass": "true",
        "post_hoc": "false",
    }
    assert got[1]["value"] == "" and got[1]["pass"] == ""
    scored = C.score([_case(), _case(3.0, 0)], n=200)
    _, curves = C.monotonicity([_case()], n=200)
    _, grid = C.ordering(simple_tyres(), n=200)
    C.write_png(tmp_path / "x" / "out.png", curves, grid, scored, scored[:1], title="t")
    assert (tmp_path / "x" / "out.png").read_bytes()[:4] == b"\x89PNG"
    assert sorted(p.name for p in (tmp_path / "x").iterdir()) == ["out.csv", "out.png"]


def test_output_dir_and_record_gating(tmp_path: Path) -> None:
    now = datetime(2026, 11, 13, 12, 0, 0, tzinfo=UTC)
    assert C.output_dir(record=False, source="full", now=now) == Path(
        "data/whatif/full_20261113T120000Z"
    )
    assert C.output_dir(record=True, source="full") == Path("results")
    chk = C.check_record_args
    assert chk(source="full", record=False, allow_test_season=False, out=tmp_path) is None
    assert "full only" in (
        chk(source="sample", record=True, allow_test_season=False, out=tmp_path) or ""
    )
    assert "needs --record" in (
        chk(source="full", record=False, allow_test_season=True, out=tmp_path) or ""
    )
    assert chk(source="full", record=True, allow_test_season=True, out=tmp_path) is None
    (tmp_path / C.RECORD_PNG).write_bytes(b"x")
    assert "never overwritten" in (
        chk(source="full", record=True, allow_test_season=True, out=tmp_path) or ""
    )


def test_main_refuses_before_reading_anything(tmp_path: Path) -> None:
    assert C.main(["--source", "sample", "--record"]) == C.EXIT_REFUSED
    assert C.main(["--source", "full", "--allow-test-season"]) == C.EXIT_REFUSED
    assert (
        C.main(["--source", "full", "--sim-dir", str(tmp_path)]) == C.EXIT_REFUSED
    )  # no model file


# ------------------------------------------------------------------ backtest cases
def _race_rows(offsets: dict[str, float], laps: int = 20) -> list[dict[str, Any]]:
    pos = {d: i + 1 for i, d in enumerate(sorted(offsets, key=offsets.__getitem__))}
    out = []
    for r in lap_records(offsets, laps=laps):
        out.append(
            dict(
                driver_id=r.driver_id,
                lap=r.lap,
                lap_time_s=r.lap_time_s,
                position=pos[r.driver_id],
                cumulative_time_s=r.race_time_s,
                gap_leader_s=offsets[r.driver_id] - min(offsets.values()),
                gap_ahead_s=None,
                compound="MEDIUM",
                tyre_age_laps=r.tyre_age,
                is_pit_in_lap=False,
                is_pit_out_lap=False,
                race_laps=57,
            )
        )
    return out


ATTEMPT = {
    "season": 2024,
    "round": 5,
    "lap": 16,
    "decision_lap": 15,
    "attacker_id": "b",
    "defender_id": "a",
    "label": 1,
    "gap_ahead_change_1l": -0.3,
}


def test_case_from_rows_uses_only_laps_up_to_l_minus_1() -> None:
    rows = _race_rows({"a": 0.0, "b": 1.2, "c": 5.0}, laps=25)
    race = B.RaceRows(
        rows, [{"pit_loss_prior_s": 21.0, "pit_loss_source": "global_prior"}], [], None
    )
    case = B.case_from_rows(ATTEMPT, race, simple_tyres(), 0.55)
    assert case.state.current_lap == 15 and case.state.race_laps == 57
    assert case.state.gap_s == pytest.approx(1.2) and case.label == 1 and case.model_p == 0.55
    assert case.closing_s_per_lap == pytest.approx(0.3)
    later = [
        dict(r, lap_time_s=r["lap_time_s"] * 100, cumulative_time_s=r["cumulative_time_s"] * 100)
        if r["lap"] > 15
        else r
        for r in rows
    ]
    other = B.case_from_rows(
        ATTEMPT, B.RaceRows(later, race.pit_loss, [], None), simple_tyres(), 0.55
    )
    assert other.state == case.state


def test_case_from_rows_checks_the_decision_lap() -> None:
    race = B.RaceRows(_race_rows({"a": 0.0, "b": 1.2}), [], [], None)
    with pytest.raises(B.BacktestError, match="L-1"):
        B.case_from_rows({**ATTEMPT, "decision_lap": 14}, race, simple_tyres(), 0.5)


def test_build_cases_scores_the_21_inputs_and_needs_tyres() -> None:
    from f1sa.sim.tyre_model import TyreModelArtefact
    from f1sa.streaming.live_features import INPUT_NAMES

    race = B.RaceRows(_race_rows({"a": 0.0, "b": 1.2}), [{"pit_loss_prior_s": 21.0}], [], [])
    seen: list[dict[str, Any]] = []

    def prob(row: Any) -> float:
        seen.append(dict(row))
        return 0.25

    art = TyreModelArtefact(
        "full", "hdfs", False, (2024,), "t", None, None, {(2024, 5): simple_tyres()}
    )
    cases = B.build_cases([ATTEMPT], {(2024, 5): race}, art, prob)
    assert cases[0].model_p == 0.25 and list(seen[0]) == list(INPUT_NAMES)
    empty = TyreModelArtefact("full", "hdfs", False, (2024,), "t", None, None, {})
    with pytest.raises(B.BacktestError, match="no entry"):
        B.build_cases([ATTEMPT], {(2024, 5): race}, empty, prob)


def test_constants_are_the_pre_registered_ones() -> None:
    assert C.STOP_THRESHOLD == 0.20 and C.LOSS_SHIFTS_S == (-2.0, -1.0, 0.0, 1.0, 2.0, 4.0)
    assert C.GRID_GAPS_S == (0.5, 1.0, 2.0, 3.5, 5.0) and C.GRID_RATES_S == (-0.5, 0.0, 0.5)
    assert C.BINS == (10, 5) and C.HELD_OUT == ("2025", (2025,))
    assert math.isclose(sum(1 for _ in C.SUBSETS), 3)
    assert car("x", 1.0).compound == "MEDIUM"


def test_post_hoc_expected_gap_rows_are_labelled_and_never_pass_fail(tmp_path: Path) -> None:
    rows, _ = C.ordering(simple_tyres(), n=200)
    post = [r for r in rows if r.post_hoc]
    cells = len(C.GRID_GAPS_S) * len(C.GRID_RATES_S)
    assert {r.check for r in post} == {"ordering_posthoc"} and all(r.passed is None for r in post)
    diffs = [r for r in post if r.metric == "egap_next_minus_now_s"]
    signs = [r for r in post if r.metric == "egap_sign_next_minus_now"]
    assert len(diffs) == len(signs) == cells
    assert all(
        s.value == math.copysign(1.0, d.value) for d, s in zip(diffs, signs, strict=True) if d.value
    )  # type: ignore[arg-type]
    follows = [r for r in post if r.metric == "egap_sign_follows_rate"]
    assert len(follows) == 2 * len(C.GRID_GAPS_S)  # rate 0 has no expected sign
    assert all(r.value == 1.0 for r in follows)  # equal cars: the mean follows the closing rate
    assert not any(r.post_hoc for r in rows if r.check == "ordering")  # the rule is unchanged
    with pytest.raises(ValueError, match="never a pass/fail"):
        C.Row("x", "y", "z", "m", 1.0, 1, True, True)
    C.write_csv(rows, tmp_path / "o.csv")
    got = list(csv.DictReader((tmp_path / "o.csv").open()))
    assert list(got[0]) == list(C.CSV_COLUMNS) and C.CSV_COLUMNS[-1] == "post_hoc"
    assert {r["post_hoc"] for r in got if r["check"] == "ordering_posthoc"} == {"true"}
    assert {r["post_hoc"] for r in got if r["check"] == "ordering"} == {"false"}
