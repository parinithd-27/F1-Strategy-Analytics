"""Decision state and service of the what-if (``sim/whatif.py``)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from _sim_fixtures import RaceSpec, lake_readers, lap_records, live, simple_tyres
from pyspark.sql import SparkSession

from f1sa.api.catalog import sim_readers
from f1sa.config import Settings
from f1sa.sim import tyre_model as T
from f1sa.sim import whatif as W
from f1sa.streaming.live_features import DriverPrior, RacePriors

PRIORS = RacePriors(
    2024,
    5,
    pit_loss_prior_s=21.0,
    pit_loss_source="circuit_prior",
    drivers={"att": DriverPrior(0.5, "team", -0.2), "def": DriverPrior(None, None, 0.1)},
)
DRIVERS = {
    "lead": live(3, gap_ahead=None),
    "def": live(4),
    "att": live(5, gap_ahead=1.5),
    "back": live(6),
}
OFFSETS = {"lead": -4.0, "def": 0.0, "att": 1.5, "back": 3.0}


def _state(records: list[W.LapRecord] | None = None, **kw: Any) -> tuple[Any, W.StateInputs]:
    recs = lap_records(OFFSETS) if records is None else records
    args: dict[str, Any] = dict(race_laps=60)
    args.update(kw)
    drivers = args.pop("drivers", DRIVERS)
    priors = args.pop("priors", PRIORS)
    return W.build_state(2024, 5, "att", "def", drivers, recs, priors, simple_tyres(), **args)


# ------------------------------------------------------------------ priors port
def test_priors_port_equals_spark(
    spark: SparkSession, sample_clean_lake: Any, sample_features_root: str
) -> None:
    from f1sa.streaming import priors as PRI

    settings = sample_clean_lake.settings()
    readers = sim_readers(settings)
    for race in ((2023, 5), (2024, 5)):
        spark_p = PRI.load_priors(spark, settings.data_lake_uri, *race)
        port = W.race_priors_from_rows(
            *race,
            readers.features.read_rows("pit_loss", *race, ["pit_loss_prior_s", "pit_loss_source"]),
            readers.features.read_rows(
                "outlap_features",
                *race,
                [
                    "driver_id",
                    "expected_outlap_pace_residual_s",
                    "expected_outlap_pace_source",
                    "expected_stop_delta_s",
                ],
            ),
        )
        assert (port.pit_loss_prior_s, port.pit_loss_source) == (
            spark_p.pit_loss_prior_s,
            spark_p.pit_loss_source,
        )
        assert dict(port.drivers) == dict(spark_p.drivers)
        assert len(port.drivers) > 0


def test_priors_port_max_per_driver_and_nulls() -> None:
    rows = [
        {
            "driver_id": "a",
            "expected_outlap_pace_residual_s": 0.1,
            "expected_outlap_pace_source": "team",
            "expected_stop_delta_s": None,
        },
        {
            "driver_id": "a",
            "expected_outlap_pace_residual_s": 0.3,
            "expected_outlap_pace_source": "driver_team",
            "expected_stop_delta_s": -0.2,
        },
        {
            "driver_id": "b",
            "expected_outlap_pace_residual_s": None,
            "expected_outlap_pace_source": None,
            "expected_stop_delta_s": None,
        },
        {"driver_id": None},
    ]
    p = W.race_priors_from_rows(2024, 5, [], rows)
    assert p.pit_loss_prior_s is None and p.pit_loss_source is None
    assert p.drivers == {"a": DriverPrior(0.3, "team", -0.2), "b": DriverPrior(None, None, None)}
    with pytest.raises(W.WhatIfError, match="2 pit_loss rows"):
        W.race_priors_from_rows(2024, 5, [{}, {}], [])


# ------------------------------------------------------------------ state
def test_state_from_the_race_channel() -> None:
    state, inputs = _state()
    assert (
        state.current_lap == 20 and inputs.decision_lap == 21 and inputs.history == "race_channel"
    )
    assert state.gap_s == pytest.approx(1.5) and inputs.gap_source == "race_channel"
    assert inputs.attacker.pace_laps == 5 and inputs.attacker.noise_source == "driver_recent"
    # lap time 90 at age a, lap l: residual = 90 - (0.4 + 0.05 a - 0.04 l) with a = l
    assert state.attacker.pace_level_s == pytest.approx(90.0 - 0.4 - 0.01 * 18.0)
    assert inputs.attacker.compound == "MEDIUM" and inputs.attacker.new_compound == "HARD"
    assert inputs.attacker.compound_source == "race_channel" and state.attacker.tyre_age == 20
    assert inputs.attacker.stop_loss_s == pytest.approx(21.0 - 0.2 + 0.5)
    assert inputs.defender.stop_loss_s == pytest.approx(21.1)
    assert "defender_outlap_residual_missing" in inputs.warnings
    assert {z.driver_id for z in state.traffic} == {"lead", "back"} and inputs.traffic_cars == 2
    assert inputs.race_laps == 60 and inputs.synthetic is False


def test_laps_after_the_current_lap_change_nothing() -> None:
    """The attacker completed lap 20; everything after it (x100) must not matter."""
    recs = lap_records(OFFSETS, laps=20)
    later = [r for r in lap_records(OFFSETS, laps=25) if r.lap > 20 and r.driver_id != "att"]
    scaled = [
        replace(r, lap_time_s=(r.lap_time_s or 0) * 100, race_time_s=r.race_time_s * 100)
        for r in later
    ]
    stints = [W.StintStart("att", 1, "MEDIUM", 0), W.StintStart("att", 21, "SOFT", 0)]
    base = _state(recs, stints=stints)
    other = _state(recs + scaled, stints=[*stints, W.StintStart("def", 22, "SOFT", 0)])
    assert base == other


def test_slow_and_pit_laps_are_left_out_of_the_pace() -> None:
    clean, _ = _state()
    sc, inputs = _state(lap_records(OFFSETS, slow={"att": [19]}))
    assert inputs.attacker.pace_laps == 5
    assert sc.attacker.pace_level_s == pytest.approx(clean.attacker.pace_level_s, abs=0.05)
    pit, pin = _state(lap_records(OFFSETS, pit_in={"att": 12}))
    assert pin.attacker.tyre_age == 8 and pin.attacker.pace_laps == 5
    assert pit.attacker.pace_level_s == pytest.approx(90.0 - 0.4 - 0.05 * 6 + 0.04 * 18)


def test_pit_lap_warning() -> None:
    _, inputs = _state(lap_records(OFFSETS, pit_in={"def": 20}))
    assert "defender_on_pit_lap" in inputs.warnings


def test_without_history_it_falls_back_to_redis() -> None:
    state, inputs = _state([])
    assert inputs.history == "none" and inputs.current_lap is None and inputs.decision_lap is None
    assert inputs.gap_source == "redis" and state.gap_s == pytest.approx(1.5)
    assert inputs.attacker.noise_source == "prior" and state.attacker.pace_n == 1
    assert inputs.attacker.compound_source == "redis" and state.traffic == ()
    assert state.race_laps is None and "no_lap_history" in inputs.warnings


def test_missing_gap_or_pace_is_no_state() -> None:
    no_gap = dict(DRIVERS, att=live(5, gap_ahead=None))
    with pytest.raises(W.NoDecisionState, match="no gap"):
        _state([], drivers=no_gap)
    no_time = dict(DRIVERS, att=live(5, last=None))
    with pytest.raises(W.NoDecisionState, match="no lap time"):
        _state([], drivers=no_time)


def test_unknown_driver_and_not_adjacent() -> None:
    with pytest.raises(W.UnknownDriver):
        W.build_state(2024, 5, "att", "ghost", DRIVERS, [], PRIORS, simple_tyres(), race_laps=60)
    with pytest.raises(W.NotAdjacent, match="not directly ahead"):
        W.build_state(2024, 5, "back", "def", DRIVERS, [], PRIORS, simple_tyres(), race_laps=60)
    with pytest.raises(W.NotAdjacent):
        W.build_state(2024, 5, "def", "att", DRIVERS, [], PRIORS, simple_tyres(), race_laps=60)


def test_tracinginsights_stints_give_the_real_compound() -> None:
    stints = [
        W.StintStart("att", 1, "SOFT", 3),
        W.StintStart("att", 15, "HARD", 0),
        W.StintStart("att", 21, "MEDIUM", 0),  # starts after lap C: unknown at decision time
    ]
    state, inputs = _state(stints=stints)
    assert (
        inputs.attacker.compound == "HARD"
        and inputs.attacker.compound_source == "tracinginsights_stint"
    )
    assert state.attacker.tyre_age == 20 - 15 + 1 and inputs.attacker.new_compound == "MEDIUM"
    assert inputs.defender.compound_source == "race_channel"  # no TI stint for def: channel


def test_no_pit_loss_prior_gives_no_stop_loss() -> None:
    state, inputs = _state(priors=RacePriors(2024, 5))
    assert state.attacker.stop_loss_s is None and inputs.pit_loss_source is None


def test_retired_third_cars_are_not_traffic() -> None:
    recs = [r for r in lap_records(OFFSETS) if not (r.driver_id == "back" and r.lap > 10)]
    state, _ = _state(recs)
    assert {z.driver_id for z in state.traffic} == {"lead"}


def test_recent_laps_filter() -> None:
    recs = lap_records({"a": 0.0}, laps=14, slow={"a": [13]})
    kept = W.recent_laps(recs)
    assert [r.lap for r in kept] == [9, 10, 11, 12, 14]
    assert W.recent_laps([]) == []


# ------------------------------------------------------------------ service
def _service(
    tmp_path: Path, specs: list[RaceSpec], *, ti: bool = False
) -> tuple[W.WhatIfService, Any]:
    clean, features = lake_readers(specs, ti=ti)
    source = "full" if ti else "sample"
    art = T.build_artefact(clean, features, source=source, lake_scheme="file")  # type: ignore[arg-type]
    T.write_artefact(art, T.default_output(source, tmp_path), root=tmp_path)  # type: ignore[arg-type]
    return W.WhatIfService(
        readers=lambda: (clean, features), sim_dir=tmp_path / "models" / "sim"
    ), clean


def _run(svc: W.WhatIfService, *, synthetic: bool = True, **kw: Any) -> W.WhatIfResult:
    drivers = {"drv1": live(4, gap_ahead=None), "drv2": live(5, gap_ahead=1.2, prob=0.37)}
    recs = lap_records({"drv1": 0.0, "drv2": 1.2})
    return svc.run(2023, 2, "drv2", "drv1", drivers, recs, race_laps=50, synthetic=synthetic, **kw)


def test_service_runs_and_caches_the_race(tmp_path: Path) -> None:
    svc, clean = _service(tmp_path, [RaceSpec(2023, 1), RaceSpec(2023, 2)])
    res = _run(svc)
    assert res.model_probability == 0.37 and res.model_flag is True
    assert len(res.sim.results) == 6 and res.inputs.synthetic and res.runtime_ms > 0
    assert res.inputs.pit_loss_prior_s == 21.5
    reads = len(clean.reads)
    again = _run(svc)
    assert len(clean.reads) == reads  # priors loaded once per race
    assert again.sim == res.sim
    assert _run(svc, seed=1).sim != res.sim


def test_service_reads_tracinginsights_stints_in_full_mode(tmp_path: Path) -> None:
    specs = [RaceSpec(2023, 1, synthetic=False), RaceSpec(2023, 2, synthetic=False)]
    svc, clean = _service(tmp_path, specs, ti=True)
    res = _run(svc, synthetic=False)
    assert ("stints_tracinginsights", 2023, 2) in clean.reads
    assert res.inputs.attacker.compound_source == "tracinginsights_stint"
    assert not res.inputs.synthetic


def test_service_errors(tmp_path: Path) -> None:
    svc, _ = _service(tmp_path, [RaceSpec(2023, 1), RaceSpec(2023, 2)])
    with pytest.raises(W.SimUnavailable, match="sim-tyre-model-full"):
        _run(svc, synthetic=False)  # no tyre_model_full.json
    with pytest.raises(W.SimUnavailable, match="no entry"):
        svc.context(2023, 9, synthetic=True)
    (tmp_path / "models" / "sim" / "tyre_model_full.json").write_text(
        (tmp_path / "models" / "sim" / "tyre_model_sample.json").read_text()
    )
    fresh = W.WhatIfService(readers=svc.readers, sim_dir=svc.sim_dir)
    with pytest.raises(W.SimUnavailable, match="holds a sample model"):
        fresh.context(2023, 2, synthetic=False)


def test_service_refuses_a_lake_mismatch(tmp_path: Path) -> None:
    svc, _ = _service(
        tmp_path, [RaceSpec(2023, 1, synthetic=False), RaceSpec(2023, 2, synthetic=False)]
    )
    with pytest.raises(W.SimUnavailable, match="does not match"):
        svc.context(2023, 2, synthetic=True)


def test_service_cache_is_bounded(tmp_path: Path) -> None:
    svc, _ = _service(tmp_path, [RaceSpec(2023, r) for r in range(1, 5)])
    svc.cache_races = 2
    for r in (1, 2, 3, 4):
        svc.context(2023, r, synthetic=True)
    assert list(svc._cache) == [(2023, 3), (2023, 4)]


def test_sample_lake_settings_are_readable(tmp_path: Path) -> None:
    readers = sim_readers(Settings(data_lake_uri=f"file://{tmp_path}"))
    assert readers.features.partitions("pit_loss", required=False) == []


def test_stint_starts_drop_conflicting_compounds_and_bad_rows() -> None:
    rows = [
        {"driver_id": "a", "start_lap": 1, "compound": "SOFT", "tyre_age_at_start": 2},
        {"driver_id": "a", "start_lap": 20, "compound": "HARD", "compound_conflict": True},
        {"driver_id": None, "start_lap": 3},
        {"driver_id": "b", "start_lap": None},
    ]
    assert W.stint_starts(rows) == (
        W.StintStart("a", 1, "SOFT", 2),
        W.StintStart("a", 20, None, None),
    )
