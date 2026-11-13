"""Backtest cases for the what-if consistency run (``sim/consistency.py``): the labelled
undercut attempts at the deployed setting, each with its decision state at the end of lap L-1.

The state is built by ``sim.whatif.build_state``, the code path of the live API, from the batch
equivalent of what the stream would have published by then:

* ``LapRecord``\\ s from ``features/laps_enriched`` with lap <= L-1 (race time = cumulative time,
  the producer's line-crossing clock; compound and tyre age as on the lap events);
* each driver's "hash" = its ``laps_enriched`` row at its latest lap <= L-1, the attacker's
  ``undercut_prob`` = the deployed calibrated LR on the attempt's 21 inputs (the stream's score,
  equal within 1e-9 by the Step 12 parity check);
* the priors from ``features/pit_loss`` / ``features/outlap_features`` (``race_priors_from_rows``)
  and, in full mode, the TracingInsights stint starts.

Only the requested seasons' partitions are read (``model.dataset.read_feature_seasons``).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from f1sa.sim.consistency import Case
from f1sa.sim.tyre_model import RaceTyreModel, TyreModelArtefact
from f1sa.sim.whatif import (
    STINT_START_COLUMNS,
    LapRecord,
    LiveDriver,
    build_state,
    race_priors_from_rows,
    stint_starts,
)
from f1sa.streaming.live_features import INPUT_NAMES

LAP_COLUMNS = (
    "driver_id",
    "lap",
    "lap_time_s",
    "position",
    "cumulative_time_s",
    "gap_leader_s",
    "gap_ahead_s",
    "compound",
    "tyre_age_laps",
    "is_pit_in_lap",
    "is_pit_out_lap",
    "race_laps",
)
PIT_LOSS_COLUMNS = ("pit_loss_prior_s", "pit_loss_source")
OUTLAP_COLUMNS = (
    "driver_id",
    "expected_outlap_pace_residual_s",
    "expected_outlap_pace_source",
    "expected_stop_delta_s",
)


class BacktestError(RuntimeError):
    pass


@dataclass(frozen=True)
class RaceRows:
    laps: Sequence[Mapping[str, Any]]
    pit_loss: Sequence[Mapping[str, Any]]
    outlap: Sequence[Mapping[str, Any]]
    ti_stints: Sequence[Mapping[str, Any]] | None


def _compound(v: Any) -> str:
    return v if v in ("SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET") else "UNKNOWN"


def case_from_rows(
    attempt: Mapping[str, Any], race: RaceRows, tyres: RaceTyreModel, model_p: float
) -> Case:
    """One backtest case (pure; the attempt row carries season, round, ids, lap, label)."""
    season, round_ = int(attempt["season"]), int(attempt["round"])
    lap = int(attempt["lap"])
    current = lap - 1
    if attempt.get("decision_lap") is not None and int(attempt["decision_lap"]) != current:
        raise BacktestError(f"{season} R{round_} lap {lap}: decision_lap is not L-1")
    known = [r for r in race.laps if r.get("lap") is not None and r["lap"] <= current]
    records = [
        LapRecord(
            driver_id=r["driver_id"],
            lap=int(r["lap"]),
            race_time_s=float(r["cumulative_time_s"]),
            lap_time_s=r.get("lap_time_s"),
            position=r.get("position"),
            compound=r.get("compound"),
            tyre_age=r.get("tyre_age_laps"),
            pit_in=bool(r.get("is_pit_in_lap")),
            pit_out=bool(r.get("is_pit_out_lap")),
        )
        for r in known
        if r.get("cumulative_time_s") is not None
    ]
    latest: dict[str, Mapping[str, Any]] = {}
    for r in known:
        if r.get("position") is None:
            continue
        if r["driver_id"] not in latest or r["lap"] > latest[r["driver_id"]]["lap"]:
            latest[r["driver_id"]] = r
    attacker, defender = str(attempt["attacker_id"]), str(attempt["defender_id"])
    drivers = {
        d: LiveDriver(
            position=int(r["position"]),
            gap_ahead_s=r.get("gap_ahead_s"),
            gap_leader_s=r.get("gap_leader_s"),
            tyre_compound=_compound(r.get("compound")),
            tyre_age_laps=r.get("tyre_age_laps"),
            last_lap_time_s=r.get("lap_time_s"),
            undercut_flag=d == attacker,
            undercut_prob=model_p if d == attacker else None,
        )
        for d, r in latest.items()
    }
    race_laps = next(
        (int(r["race_laps"]) for r in race.laps if r.get("race_laps") is not None), None
    )
    priors = race_priors_from_rows(season, round_, list(race.pit_loss), race.outlap)
    stints = None if race.ti_stints is None else stint_starts(race.ti_stints)
    state, _ = build_state(
        season,
        round_,
        attacker,
        defender,
        drivers,
        records,
        priors,
        tyres,
        race_laps=race_laps,
        stints=stints,
    )
    change = attempt.get("gap_ahead_change_1l")
    return Case(
        season=season,
        round=round_,
        attacker=attacker,
        defender=defender,
        lap=lap,
        label=int(attempt["label"]),
        model_p=float(model_p),
        state=state,
        closing_s_per_lap=None if change is None else -float(change),
    )


def build_cases(
    attempts: Iterable[Mapping[str, Any]],
    races: Mapping[tuple[int, int], RaceRows],
    artefact: TyreModelArtefact,
    probability: Callable[[Mapping[str, Any]], float],
) -> list[Case]:
    out: list[Case] = []
    for a in sorted(attempts, key=lambda a: (a["season"], a["round"], a["lap"], a["attacker_id"])):
        key = (int(a["season"]), int(a["round"]))
        tyres = artefact.race(*key)
        if tyres is None:
            raise BacktestError(f"the tyre model has no entry for {key[0]} R{key[1]}")
        out.append(
            case_from_rows(a, races[key], tyres, probability({n: a.get(n) for n in INPUT_NAMES}))
        )
    return out


def load_cases(
    spark: Any,
    lake_uri: str,
    seasons: Sequence[int],
    artefact: TyreModelArtefact,
    probability: Callable[[Mapping[str, Any]], float],
    *,
    g: float,
    k: int,
) -> list[Case]:
    """The labelled attempts of ``seasons`` at (G, k) as backtest cases (Spark reads only)."""
    from pyspark.sql import functions as F

    from f1sa.etl.clean import spark_uri
    from f1sa.model.dataset import (
        LABELLED,
        read_clean_seasons,
        read_feature_seasons,
        with_derived_inputs,
    )

    lake = spark_uri(lake_uri)
    features, clean = f"{lake}/features", f"{lake}/clean"
    ft = {
        t: read_feature_seasons(spark, features, t, seasons)
        for t in (
            "undercut_attempts",
            "stints",
            "telemetry_lap_features",
            "laps_enriched",
            "pit_loss",
            "outlap_features",
        )
    }
    attempts = with_derived_inputs(
        ft["undercut_attempts"], ft["stints"], ft["telemetry_lap_features"]
    )
    rows = [
        r.asDict()
        for r in attempts.where(
            (F.col("gap_threshold_s") == F.lit(float(g)))
            & (F.col("response_window_laps") == F.lit(int(k)))
            & F.col("outcome_category").isin(*LABELLED)
        )
        .select(
            "season",
            "round",
            "lap",
            "decision_lap",
            "attacker_id",
            "defender_id",
            "label",
            *INPUT_NAMES,
        )
        .collect()
    ]
    keys = sorted({(int(r["season"]), int(r["round"])) for r in rows})
    wanted = set(keys)

    def by_race(df: Any, cols: Sequence[str]) -> dict[tuple[int, int], list[dict[str, Any]]]:
        out: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
        for r in df.select("season", "round", *cols).collect():
            key = (int(r["season"]), int(r["round"]))
            if key in wanted:
                out[key].append(r.asDict())
        return out

    laps = by_race(ft["laps_enriched"], LAP_COLUMNS)
    pit = by_race(ft["pit_loss"], PIT_LOSS_COLUMNS)
    outlap = by_race(ft["outlap_features"], OUTLAP_COLUMNS)
    ti = None
    if artefact.source == "full":
        ti = by_race(
            read_clean_seasons(spark, clean, "stints_tracinginsights", seasons), STINT_START_COLUMNS
        )
    races = {
        key: RaceRows(
            laps=laps.get(key, []),
            pit_loss=pit.get(key, []),
            outlap=outlap.get(key, []),
            ti_stints=None if ti is None else ti.get(key, []),
        )
        for key in keys
    }
    return build_cases(rows, races, artefact, probability)
