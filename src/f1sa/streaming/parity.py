"""Batch vs stream feature parity (Step 12): the shared comparison of the unit test and
``experiments/stream_parity.py``.

A race is replayed through the stream's ``StreamProcessor`` in simulated Kafka micro-batches (no
broker) and compared with the batch pipeline:

1. **candidate sets**: at every decision lap, the stream's rule-eligible pairs equal the batch's
   (``features.undercut.snapshot_at`` over the whole race + the shared ``candidate_pairs``);
2. **features**: for every labelled attempt at one (G, k) (``undercut_attempts`` +
   ``dataset.with_derived_inputs``), the stream flagged the pair at its decision lap and the 21
   inputs agree
   (numeric |diff| <= tol or both null; categoricals equal);
3. **probability**: the batch probability (Spark ``PipelineModel.transform`` + ``Calibrator``)
   equals the stream's pure-Python calibrated LR within tol.

Spark-side module (imports ``features.undercut`` and ``model``); the stream itself never imports it.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from f1sa.features import undercut as U
from f1sa.features.undercut_rule import RuleParams, candidate_pairs
from f1sa.streaming.live_features import INPUT_NAMES
from f1sa.streaming.live_state import Decision
from f1sa.streaming.processor import StreamProcessor
from f1sa.streaming.producer import Outgoing, sim_partition

TOLERANCE = 1e-9
CATEGORICAL_INPUTS = frozenset(
    {
        "pit_loss_source",
        "attacker_expected_outlap_pace_source",
        "defender_expected_outlap_pace_source",
    }
)
KAFKA_SOURCE_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
        StructField(
            "headers",
            ArrayType(
                StructType([StructField("key", StringType()), StructField("value", BinaryType())])
            ),
        ),
    ]
)
"""The columns of Spark's Kafka source (``includeHeaders=true``)."""

Pair = tuple[str, str]
RaceKey = tuple[int, int]


# ============================================================================== simulated Kafka
def kafka_batches(
    events: Sequence[Outgoing],
    n_partitions: int,
    batch_size: int,
    *,
    lag_partition: int | None = None,
    lag_batches: int = 3,
    topic: str = "f1.race.events",
) -> list[list[dict[str, Any]]]:
    """``events`` as Kafka source rows split into micro-batches of about ``batch_size`` rows.

    Order is kept within each partition (as Kafka does). With ``lag_partition``, that
    partition's rows reach the job ``lag_batches`` batches late, so the other partitions run ahead.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")
    offsets: dict[int, int] = defaultdict(int)
    rows = []
    for out in events:
        p = out.partition if out.partition is not None else sim_partition(out.key, n_partitions)
        rows.append(
            {
                "key": out.key.encode(),
                "value": out.event.to_json().encode(),
                "topic": topic,
                "partition": p,
                "offset": offsets[p],
                "timestamp": None,
                "timestampType": 0,
                "headers": None,
            }
        )
        offsets[p] += 1
    chunks = [rows[i : i + batch_size] for i in range(0, len(rows), batch_size)]
    if lag_partition is None:
        return chunks
    out_batches: list[list[dict[str, Any]]] = [[] for _ in range(len(chunks) + lag_batches)]
    for i, chunk in enumerate(chunks):
        for r in chunk:
            j = i + lag_batches if r["partition"] == lag_partition else i
            out_batches[j].append(r)
    return [b for b in out_batches if b]


def replay_into(
    processor: StreamProcessor,
    batches: Iterable[list[dict[str, Any]]],
    *,
    as_dataframe: Callable[[list[dict[str, Any]]], Any] | None = None,
    first_batch_id: int = 0,
) -> int:
    """Feed ``batches`` to the processor; through ``process_batch(df, id)`` when
    ``as_dataframe`` is given (the real ``foreachBatch`` path), else ``process_rows``."""
    n = 0
    for i, rows in enumerate(batches, start=first_batch_id):
        if as_dataframe is not None:
            processor.process_batch(as_dataframe(rows), i)
        else:
            processor.process_rows(rows, i)
        n += 1
    return n


def spark_frame(spark: Any) -> Callable[[list[dict[str, Any]]], DataFrame]:
    """A ``rows -> DataFrame`` converter with the Kafka source schema."""
    names = [f.name for f in KAFKA_SOURCE_SCHEMA.fields]

    def convert(rows: list[dict[str, Any]]) -> DataFrame:
        df: DataFrame = spark.createDataFrame(
            [tuple(r[n] for n in names) for r in rows], KAFKA_SOURCE_SCHEMA
        )
        return df.select("value", "headers", "partition", "offset")

    return convert


# ============================================================================== batch side
def batch_race_data(laps_enriched: DataFrame, pit_loss: DataFrame) -> dict[RaceKey, U.RaceData]:
    """``features.undercut.RaceData`` of every race in ``laps_enriched`` (the batch's own view)."""
    grouped: dict[RaceKey, list[Any]] = defaultdict(list)
    for r in U.race_lap_rows(laps_enriched, pit_loss).collect():
        grouped[(int(r["season"]), int(r["round"]))].append(r)
    return {k: U._race_from_rows(k, rows, []) for k, rows in grouped.items()}


def batch_eligible(race: U.RaceData, params: RuleParams) -> dict[int, set[Pair]]:
    """The batch's rule-eligible pairs at every lap of ``race``."""
    neutral, red = U._neutral_laps(race), U._red_flag_laps(race)
    out: dict[int, set[Pair]] = {}
    for lap in sorted({r.lap for r in race.laps}):
        snap = U.snapshot_at(race, lap, neutral, red)
        out[lap] = {
            (p.attacker_id, p.defender_id) for p in candidate_pairs(snap, params) if p.eligible
        }
    return out


def labelled_rows(attempts: DataFrame, g: float, k: int) -> list[dict[str, Any]]:
    """Labelled attempts (``success`` / ``fail``) at (G, k) with the 21 inputs, as dicts.
    ``attempts`` must already carry the derived inputs (``dataset.with_derived_inputs``)."""
    sel = attempts.where(
        (F.col("gap_threshold_s") == F.lit(float(g)))
        & (F.col("response_window_laps") == F.lit(int(k)))
        & F.col("outcome_category").isin("success", "fail")
    ).select("season", "round", "lap", "decision_lap", "attacker_id", "defender_id", *INPUT_NAMES)
    return [r.asDict() for r in sel.collect()]


# ============================================================================== comparison
@dataclass
class RaceParity:
    season: int
    round: int
    decision_laps: int = 0
    batch_pairs: int = 0
    stream_pairs: int = 0
    candidate_mismatches: list[tuple[int, list[Pair], list[Pair]]] = field(default_factory=list)
    labelled: int = 0
    missing_in_stream: list[tuple[int, str, str]] = field(default_factory=list)
    input_mismatches: list[tuple[int, str, str, Any, Any]] = field(default_factory=list)
    max_abs_diff: dict[str, float] = field(default_factory=dict)
    max_abs_dp: float = 0.0
    prob_mismatches: list[tuple[int, str, float, float]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (
            self.candidate_mismatches
            or self.missing_in_stream
            or self.input_mismatches
            or self.prob_mismatches
        )

    def summary(self) -> dict[str, Any]:
        return {
            "season": self.season,
            "round": self.round,
            "decision_laps": self.decision_laps,
            "batch_eligible_pairs": self.batch_pairs,
            "stream_eligible_pairs": self.stream_pairs,
            "candidate_mismatch_laps": len(self.candidate_mismatches),
            "labelled_attempts": self.labelled,
            "labelled_missing_in_stream": len(self.missing_in_stream),
            "input_mismatches": len(self.input_mismatches),
            "max_abs_input_diff": max(self.max_abs_diff.values(), default=0.0),
            "max_abs_probability_diff": self.max_abs_dp,
            "ok": self.ok,
        }


def stream_eligible(decisions: Iterable[Decision]) -> dict[RaceKey, dict[int, set[Pair]]]:
    out: dict[RaceKey, dict[int, set[Pair]]] = defaultdict(lambda: defaultdict(set))
    for d in decisions:
        out[(d.season, d.round)][d.decision_lap].add((d.attacker_id, d.defender_id))
    return out


def _numeric_diff(a: Any, b: Any) -> float | None:
    """|a - b| for two numbers, 0.0 for two nulls, None when exactly one is null."""
    if a is None and b is None:
        return 0.0
    if a is None or b is None:
        return None
    return abs(float(a) - float(b))


def compare_race(
    race: RaceKey,
    batch_pairs: Mapping[int, set[Pair]],
    decisions: Sequence[Decision],
    labelled: Sequence[Mapping[str, Any]],
    batch_probs: Mapping[tuple[int, int, str, int], float],
    tol: float = TOLERANCE,
) -> RaceParity:
    rep = RaceParity(*race)
    mine = [d for d in decisions if (d.season, d.round) == race]
    stream_pairs = stream_eligible(mine).get(race, {})
    laps = sorted(set(batch_pairs) | set(stream_pairs))
    rep.decision_laps = len(laps)
    for lap in laps:
        b, s = set(batch_pairs.get(lap, set())), set(stream_pairs.get(lap, set()))
        rep.batch_pairs += len(b)
        rep.stream_pairs += len(s)
        if b != s:
            rep.candidate_mismatches.append((lap, sorted(b - s), sorted(s - b)))
    by_key = {(d.decision_lap, d.attacker_id, d.defender_id): d for d in mine}
    for row in labelled:
        if (int(row["season"]), int(row["round"])) != race:
            continue
        rep.labelled += 1
        key = (int(row["decision_lap"]), row["attacker_id"], row["defender_id"])
        d = by_key.get(key)
        if d is None:
            rep.missing_in_stream.append(key)
            continue
        for name in INPUT_NAMES:
            b_val, s_val = row[name], d.inputs[name]
            if name in CATEGORICAL_INPUTS:
                if b_val != s_val:
                    rep.input_mismatches.append((*key[:2], name, b_val, s_val))
                continue
            diff = _numeric_diff(b_val, s_val)
            if diff is None or diff > tol:
                rep.input_mismatches.append((*key[:2], name, b_val, s_val))
            if diff is not None:
                rep.max_abs_diff[name] = max(rep.max_abs_diff.get(name, 0.0), diff)
        bp = batch_probs.get((race[0], race[1], row["attacker_id"], int(row["lap"])))
        if bp is None:
            rep.prob_mismatches.append((key[0], key[1], float("nan"), d.probability))
            continue
        dp = abs(bp - d.probability)
        rep.max_abs_dp = max(rep.max_abs_dp, dp)
        if dp > tol:
            rep.prob_mismatches.append((key[0], key[1], bp, d.probability))
    return rep


def batch_probabilities(
    model: Any, calibrator: Any, attempts: DataFrame
) -> dict[tuple[int, int, str, int], float]:
    """Calibrated P of every row of ``attempts`` with the Spark LR pipeline (the batch path)."""
    import numpy as np
    from pyspark.ml.functions import vector_to_array

    from f1sa.model.dataset import KEY, feature_exprs

    scored = (
        model.transform(attempts.select(*KEY, *feature_exprs("real")))
        .select(*KEY, vector_to_array(F.col("probability"))[1].alias("p"))
        .collect()
    )
    raw = np.array([r["p"] for r in scored], dtype=np.float64)
    cal = calibrator.apply(raw) if len(raw) else raw
    return {
        (int(r["season"]), int(r["round"]), r["attacker_id"], int(r["lap"])): float(p)
        for r, p in zip(scored, cal, strict=True)
    }


# ============================================================================== full-data run
def _constant(priors: Any) -> Callable[[int, int], Any]:
    def load(season: int, round_: int) -> Any:
        return priors

    return load


class NullStore:
    """A ``redis_sink.Store`` that writes nothing (the parity run never touches Redis)."""

    def write_batch(self, season: int, round_: int, batch: Any) -> int:
        return 0

    def end_session(self, season: int, round_: int, event_ts: Any = None) -> int:
        return 0


SUMMARY_COLUMNS: tuple[str, ...] = (
    "season",
    "round",
    "decision_laps",
    "batch_eligible_pairs",
    "stream_eligible_pairs",
    "candidate_mismatch_laps",
    "labelled_attempts",
    "labelled_missing_in_stream",
    "input_mismatches",
    "max_abs_input_diff",
    "max_abs_probability_diff",
    "ok",
)


def parse_seasons(text: str) -> tuple[int, ...]:
    """``2021-2024`` or ``2021,2023``; the sealed test season is refused."""
    from f1sa.streaming.processor import SEALED_SEASON

    if "-" in text:
        lo, hi = (int(x) for x in text.split("-"))
        seasons = tuple(range(lo, hi + 1))
    else:
        seasons = tuple(int(x) for x in text.split(","))
    if not seasons or SEALED_SEASON in seasons:
        raise ValueError(f"seasons must be non-empty and exclude {SEALED_SEASON}, got {text!r}")
    return seasons


def write_summary(reports: Sequence[RaceParity], path: Any) -> None:
    import csv
    import os
    from pathlib import Path

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(SUMMARY_COLUMNS), lineterminator="\n")
        w.writeheader()
        for r in reports:
            w.writerow(r.summary())
    os.replace(tmp, p)


def main(argv: Iterable[str] | None = None) -> int:
    """Parity over every race of the given seasons in the lake (never 2025), with the deployed
    model; writes ``results/stream_parity_full.csv``; exit 1 on any mismatch."""
    import argparse
    import json
    from pathlib import Path

    from pyspark.ml import PipelineModel

    from f1sa.config import get_settings
    from f1sa.etl.clean import spark_uri
    from f1sa.etl.spark import build_spark
    from f1sa.lake import schemas as S
    from f1sa.model.calibrate import Calibrator
    from f1sa.model.dataset import read_feature_seasons, read_seasons, with_derived_inputs
    from f1sa.streaming.priors import priors_from_frames
    from f1sa.streaming.producer import build_events, race_inputs
    from f1sa.streaming.redis_sink import RedisSink
    from f1sa.streaming.stream_job import load_model

    p = argparse.ArgumentParser(prog="f1sa-stream-parity")
    p.add_argument("--seasons", type=parse_seasons, default=parse_seasons("2021-2024"))
    p.add_argument("--model-dir", type=Path, default=Path("models/undercut/deploy_real_G3.5_k2"))
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--out", type=Path, default=Path("results/stream_parity_full.csv"))
    args = p.parse_args(list(argv) if argv is not None else None)

    scorer, params, meta = load_model(args.model_dir)
    g, k = float(meta["g"]), int(meta["k"])
    spark = build_spark("f1sa-stream-parity")
    lake = spark_uri(get_settings().data_lake_uri)
    features, clean_root = f"{lake}/features", f"{lake}/clean"
    ft = {
        t: read_feature_seasons(spark, features, t, args.seasons).cache()
        for t in (
            "laps_enriched",
            "pit_loss",
            "outlap_features",
            "undercut_attempts",
            "stints",
            "telemetry_lap_features",
        )
    }
    clean = {
        t: read_seasons(spark, clean_root, t, S.CLEAN_SCHEMAS[t], args.seasons).cache()
        for t in ("laps", "pitstops", "stints", "results")
    }
    attempts = with_derived_inputs(
        ft["undercut_attempts"], ft["stints"], ft["telemetry_lap_features"]
    ).cache()
    labelled = labelled_rows(attempts, g, k)
    sel = attempts.where(
        (F.col("gap_threshold_s") == F.lit(g))
        & (F.col("response_window_laps") == F.lit(k))
        & F.col("outcome_category").isin("success", "fail")
    )
    lr_dir = args.model_dir / "lr"
    cal = Calibrator.from_json(json.loads((lr_dir / "calibrator.json").read_text()))
    probs = batch_probabilities(PipelineModel.load(str(lr_dir / "pipeline")), cal, sel)
    races = batch_race_data(ft["laps_enriched"], ft["pit_loss"])
    reports = []
    for race in sorted(races):
        priors = priors_from_frames(ft["pit_loss"], ft["outlap_features"], clean["results"], *race)
        proc = StreamProcessor(
            scorer=scorer,
            params=params,
            sink=RedisSink(NullStore()),
            priors_loader=_constant(priors),
            n_partitions=args.partitions,
            keep_decisions=True,
        )
        events = build_events(race_inputs(clean, *race), args.partitions)
        replay_into(proc, kafka_batches(events, args.partitions, args.batch_size, lag_partition=0))
        rep = compare_race(
            race, batch_eligible(races[race], params), proc.decisions, labelled, probs
        )
        reports.append(rep)
        print(json.dumps(rep.summary()), flush=True)
    write_summary(reports, args.out)
    bad = [r for r in reports if not r.ok]
    total = sum(r.labelled for r in reports)
    print(
        f"{len(reports)} races, {total} labelled attempts, "
        f"{sum(r.batch_pairs for r in reports)} eligible pairs; mismatching races: {len(bad)}; "
        f"wrote {args.out}"
    )
    return 1 if bad or total != len(labelled) else 0


# ============================================================================== alert policy
AlertKey = tuple[str, str, int]
"""(attacker, defender, decision lap)."""


@dataclass
class AlertCheck:
    """The published alerts of one race against what the batch tables imply."""

    season: int
    round: int
    opportunity: int = 0
    new_candidate: int = 0
    probability_crossed: int = 0
    attempt: int = 0
    labelled: int = 0
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


def eligible_attempts(attempts: DataFrame, g: float) -> dict[RaceKey, set[AlertKey]]:
    """Batch attempts at gap threshold ``g`` that the detector would have flagged at L-1."""
    rows = (
        attempts.where((F.col("gap_threshold_s") == F.lit(float(g))) & F.col("detector_eligible"))
        .select("season", "round", "attacker_id", "defender_id", "decision_lap")
        .distinct()
        .collect()
    )
    out: dict[RaceKey, set[AlertKey]] = defaultdict(set)
    for r in rows:
        out[(int(r["season"]), int(r["round"]))].add(
            (r["attacker_id"], r["defender_id"], int(r["decision_lap"]))
        )
    return out


def check_alerts(
    race: RaceKey,
    alerts: Sequence[Any],
    decisions: Sequence[Decision],
    batch_pairs: Mapping[int, set[Pair]],
    batch_attempts: set[AlertKey],
    labelled: Sequence[Mapping[str, Any]],
    threshold: float = 0.5,
) -> AlertCheck:
    """The alert policy on one race:

    * ``new_candidate`` alerts = batch-eligible pairs at lap l that were not eligible at l-1;
    * ``probability_crossed`` alerts = consecutive eligible laps of a pair whose stream P goes
      from below ``threshold`` to at or above it (the batch has P only for attempts);
    * ``attempt`` alerts = batch attempts the detector flagged at L-1; every labelled attempt is
      among them;
    * at most one opportunity per pair and lap, one attempt per attacker and lap.
    """
    rep = AlertCheck(*race)
    mine = [a for a in alerts if (a.season, a.round) == race]
    opp = [a for a in mine if a.reason == "opportunity"]
    att = [a for a in mine if a.reason == "attempt"]
    rep.opportunity, rep.attempt = len(opp), len(att)
    keys = [(a.attacker_id, a.defender_id, a.decision_lap) for a in opp]
    if len(keys) != len(set(keys)):
        rep.problems.append("more than one opportunity alert for a pair on one lap")
    att_keys = [(a.attacker_id, a.defender_id, a.decision_lap) for a in att]
    if len({(k[0], k[2]) for k in att_keys}) != len(att_keys):
        rep.problems.append("more than one attempt alert for an attacker on one lap")
    new = {
        (a, d, lap)
        for lap, pairs in batch_pairs.items()
        for a, d in pairs
        if (a, d) not in batch_pairs.get(lap - 1, set())
    }
    got_new = {k for a, k in zip(opp, keys, strict=True) if a.trigger == "new_candidate"}
    rep.new_candidate = len(got_new)
    if got_new != new:
        rep.problems.append(
            f"new_candidate: missing {sorted(new - got_new)[:5]}, extra {sorted(got_new - new)[:5]}"
        )
    prob = {
        (d.attacker_id, d.defender_id, d.decision_lap): d.probability
        for d in decisions
        if (d.season, d.round) == race
    }
    crossed = {
        (a, d, lap)
        for (a, d, lap), p in prob.items()
        if (a, d, lap - 1) in prob and prob[(a, d, lap - 1)] < threshold <= p
    }
    got_crossed = {k for a, k in zip(opp, keys, strict=True) if a.trigger == "probability_crossed"}
    rep.probability_crossed = len(got_crossed)
    if got_crossed != crossed:
        rep.problems.append(
            f"probability_crossed: expected {sorted(crossed)[:5]}, got {sorted(got_crossed)[:5]}"
        )
    if set(att_keys) != batch_attempts:
        rep.problems.append(
            f"attempt: missing {sorted(batch_attempts - set(att_keys))[:5]}, "
            f"extra {sorted(set(att_keys) - batch_attempts)[:5]}"
        )
    for a in att:
        p = prob.get((a.attacker_id, a.defender_id, a.decision_lap))
        if p is None or p != a.undercut_prob:
            rep.problems.append(
                f"attempt alert P differs from P at L-1: {a.attacker_id} {a.decision_lap}"
            )
    for row in labelled:
        if (int(row["season"]), int(row["round"])) != race:
            continue
        rep.labelled += 1
        key = (row["attacker_id"], row["defender_id"], int(row["decision_lap"]))
        if key not in set(att_keys):
            rep.problems.append(f"labelled attempt without an attempt alert: {key}")
    return rep
