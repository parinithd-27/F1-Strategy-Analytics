"""Shared helpers for the streaming tests: a valid scorer document and a processor on fakeredis."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import fakeredis

from f1sa.features.undercut_rule import RuleParams
from f1sa.model import features as MF
from f1sa.model.lr_scorer import SCORER_FORMAT, LRScorer
from f1sa.serving.redis_store import Batch, RaceStore
from f1sa.streaming.live_features import RacePriors
from f1sa.streaming.processor import StreamProcessor
from f1sa.streaming.redis_sink import RedisSink


def scorer_doc(intercept: float = -0.5, gap_coef: float = -0.3) -> dict[str, Any]:
    """A ``scorer.json`` for feature set ``real`` (only ``gap_s`` has a weight), Platt."""
    cols = MF.assembled_columns("real")
    coef = [gap_coef if c == "f_gap_s" else 0.0 for c in cols]
    return {
        "format": SCORER_FORMAT,
        "inputs": [
            {"name": s.name, "kind": s.kind, "categories": list(s.categories)}
            for s in MF.specs_for("real")
        ],
        "assembled_columns": list(cols),
        "scaler_mean": [0.0] * len(cols),
        "scaler_std": [1.0] * len(cols),
        "coefficients": coef,
        "intercept": intercept,
        "calibrator": {"method": "platt", "params": {"a": 1.2, "b": -0.1}, "reason": "test"},
        "eps": 1e-6,
    }


def scorer() -> LRScorer:
    return LRScorer.from_json(scorer_doc())


class CountingStore(RaceStore):
    """``RaceStore`` on fakeredis that records every ``write_batch`` and ``end_session``."""

    def __init__(self) -> None:
        super().__init__(fakeredis.FakeRedis(decode_responses=True))
        self.batches: list[tuple[int, int, Batch]] = []
        self.ends: list[tuple[int, int]] = []

    def write_batch(self, season: int, round_: int, batch: Batch) -> int:
        self.batches.append((season, round_, batch))
        return super().write_batch(season, round_, batch)

    def end_session(self, season: int, round_: int, event_ts: Any = None) -> int:
        self.ends.append((season, round_))
        return super().end_session(season, round_, event_ts)


def processor(
    store: RaceStore,
    priors: Mapping[tuple[int, int], RacePriors] | None = None,
    *,
    n_partitions: int = 1,
    state_dir: Path | None = None,
    **kw: Any,
) -> StreamProcessor:
    def load(season: int, round_: int) -> RacePriors:
        if priors is not None:
            return priors[(season, round_)]
        return RacePriors(season, round_, None, None, {}, {})

    return StreamProcessor(
        scorer=scorer(),
        params=RuleParams(),
        sink=RedisSink(store),
        priors_loader=load,
        n_partitions=n_partitions,
        state_dir=state_dir,
        **kw,
    )


def row(value: bytes, partition: int = 0, offset: int = 0, headers: Any = None) -> dict[str, Any]:
    return {"value": value, "partition": partition, "offset": offset, "headers": headers}


def scenario_inputs(sc: Any) -> Any:
    """``producer.RaceInputs`` of an ``_undercut_fixtures.Scenario`` without Spark (tyre age as
    ``df_jobs.tyre_age``: age at start + laps into the stint + 1; distance = the winner's laps)."""
    from f1sa.streaming.producer import LapRecord, RaceInputs, StopRecord

    rows = sc.rows()
    stints = rows["stints"]
    laps = []
    for r in rows["laps"]:
        st = next(
            s
            for s in stints
            if s["driver_id"] == r["driver_id"] and s["start_lap"] <= r["lap"] <= s["end_lap"]
        )
        laps.append(
            LapRecord(
                r["driver_id"],
                r["lap"],
                r["position"],
                r["lap_time_s"],
                st["compound"],
                st["tyre_age_at_start"] + r["lap"] - st["start_lap"] + 1,
                bool(r.get("is_neutralised_lap")) or bool(r.get("is_red_flag_lap")),
                bool(r.get("is_red_flag_lap")),
            )
        )
    stops = [StopRecord(s["driver_id"], s["lap"], s["duration_s"]) for s in rows["pitstops"]]
    winner = next(r for r in rows["results"] if r["position"] == 1)
    return RaceInputs(sc.season, sc.round, tuple(laps), tuple(stops), winner["laps"])


def scenario_doc_dir(path: Path, *, g: float = 3.5, features: list[str] | None = None) -> Path:
    """A deployment-model folder (``lr/metadata.json`` + ``lr/scorer.json``) for the job tests."""
    import json

    doc = scorer_doc()
    lr = path / "lr"
    lr.mkdir(parents=True)
    (lr / "scorer.json").write_text(json.dumps(doc), encoding="utf-8")
    meta = {
        "feature_set": "real",
        "g": g,
        "k": 2,
        "model": "lr",
        "features": features if features is not None else [s["name"] for s in doc["inputs"]],
    }
    (lr / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    return path
