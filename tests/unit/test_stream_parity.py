"""Batch vs stream parity (Step 12, the key test).

A race is replayed through ``StreamProcessor`` in simulated Kafka micro-batches (no broker; the
``foreachBatch`` function is called directly) and compared with the batch pipeline:

* at **every** decision lap the stream's rule-eligible pairs equal the batch's;
* every labelled attempt (G=3.5, k=2) is flagged at its decision lap, its 21 inputs equal the
  batch's ``undercut_attempts`` + ``with_derived_inputs`` (|diff| <= 1e-9, or both null), and the
  pure-Python calibrated P equals Spark's ``PipelineModel.transform`` + ``Calibrator`` (1e-9).

Races: the Step 8 hand-built scenarios (lapped car between, SC in the window, retirement, ...)
and both real sample races (2023 R5, 2024 R5), each at micro-batch sizes 1, 7 and 50 over 3
partitions, plus a variant where one partition lags 3 batches behind. The only inputs that are not
causal are the declared replayed context (``CONTEXT_INPUTS``), and nothing else is.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import fakeredis
import pytest
from _model_fixtures import SMALL, frame, rehearsal_rows
from _undercut_fixtures import Car, Scenario, clean_tables, standard_cars
from pyspark.sql import DataFrame, SparkSession

from f1sa.etl.clean import read_clean
from f1sa.features.build import compute_features, read_feature
from f1sa.features.undercut_rule import RuleParams
from f1sa.model import deploy as D
from f1sa.model import pipeline as P
from f1sa.model.calibrate import Calibrator, fit_platt
from f1sa.model.dataset import build_dataset, with_derived_inputs
from f1sa.model.lr_scorer import LRScorer
from f1sa.model.train import predict, tune
from f1sa.serving.redis_store import RaceStore
from f1sa.streaming import events as E
from f1sa.streaming import live_features as LF
from f1sa.streaming import parity as PT
from f1sa.streaming.priors import load_priors, priors_from_frames
from f1sa.streaming.processor import StreamProcessor
from f1sa.streaming.producer import build_events, load_race_inputs, race_inputs
from f1sa.streaming.redis_sink import RedisSink

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = REPO_ROOT / "models" / "undercut" / "deploy_real_G3.5_k2"
G, K = 3.5, 2
N_PARTITIONS = 3
PARAMS = RuleParams(gap_threshold_s=G)
VARIANTS: list[tuple[int, int | None, bool]] = [
    (1, None, False),
    (7, None, False),
    (50, None, True),
    (50, 0, True),
]
"""(micro-batch size, lagging partition, through a Spark DataFrame / process_batch)."""

ALERT_COUNTS: dict[tuple[tuple[int, int | None, bool], tuple[int, int], str], PT.AlertCheck] = {}
"""Alert checks per (variant, race), so the variants can be compared with each other."""

LAPPED = Car("x", "team_x", 119.25, 100.0)
SCENARIOS: dict[str, Scenario] = {
    "success": Scenario(2023, 1, standard_cars()),
    "fail": Scenario(2023, 2, standard_cars(a_fresh_gain=0.5)),
    "stayed_out": Scenario(2023, 3, standard_cars(d_pit_laps=[12])),
    "sc_in_window": Scenario(2023, 4, standard_cars(), neutral_laps={7}),
    "attacker_retires": Scenario(2023, 5, standard_cars(a_retire_after=7)),
    "lapped_between": Scenario(2023, 6, [*standard_cars(), LAPPED]),
    "red_flag": Scenario(2023, 7, standard_cars(), red_flag_laps={2}),
}


# ------------------------------------------------------------------ model (always available)
@pytest.fixture(scope="module")
def model(spark: SparkSession) -> dict[str, Any]:
    """A calibrated LR on the GENERATED rehearsal table, exported like the deployment model."""
    ds = build_dataset(frame(spark, rehearsal_rows()), split=P.DEV_SPLIT)
    lr = tune("lr", ds, SMALL)
    pred = predict(lr.model, ds.validation)
    cal = fit_platt(spark, pred["season"], pred["label"], pred["score"])
    doc = D.scorer_document(lr.model, ds.columns, cal)
    return {"pipeline": lr.model, "calibrator": cal, "scorer": LRScorer.from_json(doc)}


def _processor(
    scorer: LRScorer, priors: Mapping[tuple[int, int], LF.RacePriors]
) -> StreamProcessor:
    store = RaceStore(fakeredis.FakeRedis(decode_responses=True))
    return StreamProcessor(
        scorer=scorer,
        params=PARAMS,
        sink=RedisSink(store),
        priors_loader=lambda s, r: priors[(s, r)],
        n_partitions=N_PARTITIONS,
        keep_decisions=True,
    )


def _check(
    spark: SparkSession,
    model: Mapping[str, Any],
    tables: Mapping[str, DataFrame],
    inputs: Mapping[tuple[int, int], Any],
    priors: Mapping[tuple[int, int], LF.RacePriors],
    variant: tuple[int, int | None, bool],
    scorer: LRScorer | None = None,
) -> list[PT.RaceParity]:
    size, lag, via_df = variant
    proc = _processor(scorer or model["scorer"], priors)
    convert: Callable[[list[dict[str, Any]]], Any] | None = (
        PT.spark_frame(spark) if via_df else None
    )
    batch_id = 0
    for race in sorted(inputs):
        batches = PT.kafka_batches(
            build_events(inputs[race], N_PARTITIONS), N_PARTITIONS, size, lag_partition=lag
        )
        batch_id += PT.replay_into(proc, batches, as_dataframe=convert, first_batch_id=batch_id)
        assert proc.ended(*race), f"{race} did not end"
    attempts = with_derived_inputs(
        tables["undercut_attempts"], tables["stints"], tables["telemetry_lap_features"]
    )
    labelled = PT.labelled_rows(attempts, G, K)
    sel = attempts.where(f"gap_threshold_s = {G} AND response_window_laps = {K}").where(
        "outcome_category IN ('success', 'fail')"
    )
    probs = PT.batch_probabilities(model["pipeline"], model["calibrator"], sel)
    races = PT.batch_race_data(tables["laps_enriched"], tables["pit_loss"])
    reports = []
    attempts_flagged = PT.eligible_attempts(tables["undercut_attempts"], G)
    for race in sorted(inputs):
        pairs = PT.batch_eligible(races[race], PARAMS)
        alert_check = PT.check_alerts(
            race, proc.alerts, proc.decisions, pairs, attempts_flagged.get(race, set()), labelled
        )
        assert alert_check.ok, alert_check.problems
        ALERT_COUNTS[(variant, race, str(model.get("tag", "rehearsal")))] = alert_check
        rep = PT.compare_race(race, pairs, proc.decisions, labelled, probs)
        assert rep.ok, json.dumps(
            {
                "summary": rep.summary(),
                "candidates": rep.candidate_mismatches[:5],
                "missing": rep.missing_in_stream,
                "inputs": rep.input_mismatches[:10],
                "probs": rep.prob_mismatches[:5],
            },
            default=str,
        )
        reports.append(rep)
    return reports


# ------------------------------------------------------------------ hand-built scenarios
@pytest.fixture(scope="module")
def scenario_tables(spark: SparkSession) -> dict[str, Any]:
    scenarios = list(SCENARIOS.values())
    clean = clean_tables(spark, scenarios)
    targets = {(s.season, s.round) for s in scenarios}
    tables = compute_features(spark, clean, targets)
    inputs = {t: race_inputs(clean, *t) for t in sorted(targets)}
    priors = {
        t: priors_from_frames(tables["pit_loss"], tables["outlap_features"], clean["results"], *t)
        for t in sorted(targets)
    }
    return {"tables": tables, "inputs": inputs, "priors": priors}


@pytest.mark.parametrize("variant", VARIANTS, ids=lambda v: f"b{v[0]}-lag{v[1]}-df{v[2]}")
def test_scenarios_match_batch(
    spark: SparkSession,
    model: dict[str, Any],
    scenario_tables: dict[str, Any],
    variant: tuple[int, int | None, bool],
) -> None:
    reports = _check(
        spark,
        model,
        scenario_tables["tables"],
        scenario_tables["inputs"],
        scenario_tables["priors"],
        variant,
    )
    by_race = {(r.season, r.round): r for r in reports}
    # success and fail are labelled; the lapped car makes round 6 ineligible at L-1 = 5
    assert by_race[(2023, 1)].labelled == 1 and by_race[(2023, 2)].labelled == 1
    assert sum(r.batch_pairs for r in reports) > 0


# ------------------------------------------------------------------ the real sample races
@pytest.fixture(scope="module")
def sample_tables(
    spark: SparkSession, sample_clean_lake: Any, sample_features_root: str
) -> dict[str, Any]:
    lake_uri = sample_clean_lake.settings().data_lake_uri
    tables = {
        t: read_feature(spark, sample_features_root, t)
        for t in (
            "laps_enriched",
            "pit_loss",
            "outlap_features",
            "undercut_attempts",
            "stints",
            "telemetry_lap_features",
        )
    }
    races = sorted(
        {
            (int(r["season"]), int(r["round"]))
            for r in tables["laps_enriched"].select("season", "round").distinct().collect()
        }
    )
    inputs = {t: load_race_inputs(spark, lake_uri, *t) for t in races}
    priors = {t: load_priors(spark, lake_uri, *t) for t in races}
    results = read_clean(spark, sample_clean_lake.clean_root, "results")
    return {
        "tables": tables,
        "inputs": inputs,
        "priors": priors,
        "results": results,
        "lake": lake_uri,
    }


@pytest.mark.parametrize("variant", VARIANTS, ids=lambda v: f"b{v[0]}-lag{v[1]}-df{v[2]}")
def test_sample_races_match_batch(
    spark: SparkSession,
    model: dict[str, Any],
    sample_tables: dict[str, Any],
    variant: tuple[int, int | None, bool],
) -> None:
    reports = _check(
        spark,
        model,
        sample_tables["tables"],
        sample_tables["inputs"],
        sample_tables["priors"],
        variant,
    )
    assert {(r.season, r.round) for r in reports} == {(2023, 5), (2024, 5)}
    r24 = ALERT_COUNTS[(variant, (2024, 5), "rehearsal")]
    assert r24.attempt >= r24.labelled >= 1 and r24.new_candidate > 0
    assert r24.opportunity + r24.attempt < sum(r.batch_pairs for r in reports if r.round == 5)
    assert sum(r.labelled for r in reports) >= 1
    assert sum(r.batch_pairs for r in reports) > 0


def test_loaded_priors_equal_the_frame_priors(sample_tables: dict[str, Any]) -> None:
    t = sample_tables["tables"]
    for race, loaded in sample_tables["priors"].items():
        assert loaded == priors_from_frames(
            t["pit_loss"], t["outlap_features"], sample_tables["results"], *race
        )


@pytest.fixture
def deploy_model(spark: SparkSession) -> Iterator[dict[str, Any]]:
    if not (DEPLOY_DIR / "lr" / "scorer.json").is_file():
        pytest.skip("deployment model not saved (make model-save-full); gitignored")
    from pyspark.ml import PipelineModel

    cal = Calibrator.from_json(json.loads((DEPLOY_DIR / "lr" / "calibrator.json").read_text()))
    yield {
        "pipeline": PipelineModel.load(str(DEPLOY_DIR / "lr" / "pipeline")),
        "calibrator": cal,
        "scorer": LRScorer.load(DEPLOY_DIR / "lr" / "scorer.json"),
        "tag": "deploy",
    }


def test_sample_races_match_batch_with_the_deployed_model(
    spark: SparkSession, deploy_model: dict[str, Any], sample_tables: dict[str, Any]
) -> None:
    _check(
        spark,
        deploy_model,
        sample_tables["tables"],
        sample_tables["inputs"],
        sample_tables["priors"],
        (7, 1, False),
    )


# ------------------------------------------------------------------ declared non-causal inputs
def test_only_the_declared_context_is_non_causal() -> None:
    assert set(LF.INPUT_NAMES) == LF.LIVE_INPUTS | LF.PRIOR_INPUTS | LF.CONTEXT_INPUTS
    assert {
        "laps_remaining",
        "pace_delta_last3_s",
        "attacker_deg_slope_s_per_lap",
        "defender_deg_slope_s_per_lap",
        "deg_slope_delta_s_per_lap",
    } == LF.CONTEXT_INPUTS
    assert E.CONTEXT_FIELDS == ("track_neutral", "track_red_flag", "race_laps")


# ------------------------------------------------------------------ parity helpers
def test_kafka_batches_keep_partition_order_and_lag_one_partition() -> None:
    from _stream_fixtures import scenario_inputs

    out = build_events(scenario_inputs(SCENARIOS["success"]), N_PARTITIONS)
    plain = PT.kafka_batches(out, N_PARTITIONS, 5)
    lagged = PT.kafka_batches(out, N_PARTITIONS, 5, lag_partition=0, lag_batches=2)
    flat = [r for b in lagged for r in b]
    assert sorted(r["value"] for r in flat) == sorted(r["value"] for b in plain for r in b)
    for p in range(N_PARTITIONS):
        offs = [r["offset"] for r in flat if r["partition"] == p]
        assert offs == sorted(offs)
    first_p0 = next(i for i, b in enumerate(lagged) if any(r["partition"] == 0 for r in b))
    assert first_p0 >= 2
    with pytest.raises(ValueError):
        PT.kafka_batches(out, N_PARTITIONS, 0)


def test_parse_seasons_refuses_the_test_season() -> None:
    assert PT.parse_seasons("2021-2024") == (2021, 2022, 2023, 2024)
    assert PT.parse_seasons("2023,2021") == (2023, 2021)
    for bad in ("2024-2025", "2025"):
        with pytest.raises(ValueError, match="2025"):
            PT.parse_seasons(bad)


def test_compare_race_reports_each_kind_of_mismatch() -> None:
    from f1sa.streaming.live_state import Decision

    inputs = dict.fromkeys(LF.INPUT_NAMES)
    d = Decision(2024, 5, 9, "a", "b", 1.0, {**inputs, "gap_s": 1.0}, 0.4)
    labelled = [{"season": 2024, "round": 5, "lap": 10, "decision_lap": 9, "attacker_id": "a",
                 "defender_id": "b", **inputs, "gap_s": 1.5}]  # fmt: skip
    rep = PT.compare_race(
        (2024, 5), {9: {("a", "b")}, 10: {("c", "d")}}, [d], labelled, {(2024, 5, "a", 10): 0.5}
    )
    assert rep.candidate_mismatches == [(10, [("c", "d")], [])]
    assert rep.input_mismatches == [(9, "a", "gap_s", 1.5, 1.0)]
    assert rep.prob_mismatches == [(9, "a", 0.5, 0.4)] and not rep.ok
    missing = PT.compare_race((2024, 5), {}, [], labelled, {})
    assert missing.missing_in_stream == [(9, "a", "b")]


@pytest.fixture
def saved_model_dir(tmp_path: Path, model: dict[str, Any]) -> Path:
    from f1sa.model import features as MF

    lr = tmp_path / "deploy" / "lr"
    model["pipeline"].write().save(str(lr / "pipeline"))
    (lr / "calibrator.json").write_text(json.dumps(model["calibrator"].to_json()))
    doc = {
        "format": "f1sa-lr-scorer-v1",
        "inputs": [
            {"name": s.name, "kind": s.kind, "categories": list(s.categories)}
            for s in model["scorer"].inputs
        ],
        "assembled_columns": list(model["scorer"].assembled_columns),
        "scaler_mean": list(model["scorer"].mean),
        "scaler_std": list(model["scorer"].std),
        "coefficients": list(model["scorer"].coefficients),
        "intercept": model["scorer"].intercept,
        "calibrator": model["calibrator"].to_json(),
        "eps": model["scorer"].eps,
    }
    (lr / "scorer.json").write_text(json.dumps(doc))
    meta = {
        "feature_set": "real",
        "g": G,
        "k": K,
        "model": "lr",
        "features": [s.name for s in MF.specs_for("real")],
    }
    (lr / "metadata.json").write_text(json.dumps(meta))
    return tmp_path / "deploy"


def test_full_parity_main_on_the_sample_lake(
    sample_tables: dict[str, Any],
    saved_model_dir: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATA_LAKE_URI", sample_tables["lake"])
    out = tmp_path / "parity.csv"
    rc = PT.main(["--seasons", "2023-2024", "--model-dir", str(saved_model_dir), "--out", str(out)])
    assert rc == 0
    lines = out.read_text().splitlines()
    assert lines[0].split(",") == list(PT.SUMMARY_COLUMNS) and len(lines) == 3
    assert all(line.endswith(",True") for line in lines[1:])


def test_null_store_writes_nothing() -> None:
    store = PT.NullStore()
    assert store.write_batch(2024, 5, None) == 0 and store.end_session(2024, 5) == 0


def test_alerts_do_not_depend_on_the_micro_batching() -> None:
    by_race: dict[tuple[tuple[int, int], str], set[tuple[int, int, int, int]]] = {}
    for (_variant, race, tag), c in ALERT_COUNTS.items():
        by_race.setdefault((race, tag), set()).add(
            (c.new_candidate, c.probability_crossed, c.attempt, c.labelled)
        )
    if not by_race:
        pytest.skip("needs the parity tests of this module to run first")
    assert all(len(v) == 1 for v in by_race.values()), by_race
