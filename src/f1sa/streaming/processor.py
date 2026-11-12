"""The ``foreachBatch`` logic of the stream job (#20), with no Spark import.

``StreamProcessor.process_batch(df, batch_id)`` is the function Structured Streaming calls for each
micro-batch (``df`` has the Kafka source columns ``value``, ``headers``, ``partition`` and
``offset``). Per batch it:

1. skips a ``batch_id`` it has already applied (Spark replays the last batch after a failure);
2. drops the infrastructure test message (header ``test=infra``) and counts malformed messages;
3. per race: loads the priors once (earlier races only), applies the events to the ``RaceState``,
   evaluates every decision that is ready with the **shared** rule (``features.undercut_rule``),
   scores the eligible pairs with the pure-Python calibrated LR (``model.lr_scorer``), applies the
   alert policy (``streaming.alerts``: what is published, not what is computed), and writes the
   race's batch through ``RedisSink`` (one pipeline);
4. on ``session_end`` from every partition: evaluates what is left, writes, calls
   ``end_session`` and writes **nothing** for that race afterwards (a late write would reset the
   safety TTL);
5. snapshots the driver-side state atomically (``<checkpoint>/driver_state/state.json``), so a
   restarted job resumes where the Spark checkpoint resumes.

This is driver-side state inside ``foreachBatch``; no Spark stateful operator is used.
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from f1sa.features.undercut_rule import RuleParams
from f1sa.model.lr_scorer import LRScorer
from f1sa.streaming.alerts import ALERT_THRESHOLD, on_decision, on_pit_in
from f1sa.streaming.events import EventError, RaceEvent, UndercutAlert, from_json, is_infra_test
from f1sa.streaming.live_features import INPUT_NAMES, RacePriors, decision_inputs
from f1sa.streaming.live_state import Decision, RaceState
from f1sa.streaming.redis_sink import RedisSink, now_ms

log = logging.getLogger(__name__)

SEALED_SEASON = 2025
"""The test season (sealed until Step 9f): refused unless explicitly allowed."""
STATE_FILE = "state.json"


class ProcessorError(RuntimeError):
    pass


class _Collectable(Protocol):
    def collect(self) -> list[Any]: ...


PriorsLoader = Callable[[int, int], RacePriors]


@dataclass
class ProcessorCounters:
    batches: int = 0
    skipped_batches: int = 0
    rows: int = 0
    infra_filtered: int = 0
    malformed: int = 0
    refused_test_season: int = 0
    after_end: int = 0

    def to_json(self) -> dict[str, int]:
        return dict(vars(self))


@dataclass
class StreamProcessor:
    scorer: LRScorer
    params: RuleParams
    sink: RedisSink
    priors_loader: PriorsLoader
    n_partitions: int
    allow_test_season: bool = False
    state_dir: Path | None = None
    batch_log: Path | None = None
    keep_decisions: bool = False
    alert_threshold: float = ALERT_THRESHOLD
    clock_ms: Callable[[], int] = now_ms
    races: dict[tuple[int, int], RaceState] = field(default_factory=dict)
    last_batch_id: int = -1
    counters: ProcessorCounters = field(default_factory=ProcessorCounters)
    decisions: list[Decision] = field(default_factory=list)
    """Every eligible decision (only with ``keep_decisions``; used by the parity check)."""
    alerts: list[UndercutAlert] = field(default_factory=list)
    """Every published alert (only with ``keep_decisions``)."""

    def __post_init__(self) -> None:
        names = tuple(s.name for s in self.scorer.inputs)
        if names != INPUT_NAMES:
            raise ProcessorError(f"scorer inputs {names} differ from the live inputs {INPUT_NAMES}")
        if self.state_dir is not None:
            self._restore()

    # ------------------------------------------------------------------ queries
    def ended(self, season: int, round_: int) -> bool:
        state = self.races.get((season, round_))
        return state is not None and state.ended

    # ------------------------------------------------------------------ entry points
    def process_batch(self, df: _Collectable, batch_id: int) -> None:
        """The ``foreachBatch`` function."""
        self.process_rows(df.collect(), batch_id)

    def process_rows(self, rows: Iterable[Any], batch_id: int) -> None:
        if batch_id <= self.last_batch_id:
            self.counters.skipped_batches += 1
            log.warning("batch %d already applied (last %d); skipped", batch_id, self.last_batch_id)
            return
        received = self.clock_ms()
        by_race: dict[tuple[int, int], list[tuple[int, RaceEvent]]] = defaultdict(list)
        ordered = sorted(rows, key=lambda r: (int(r["partition"]), int(r["offset"])))
        for row in ordered:
            self.counters.rows += 1
            if is_infra_test(row["headers"]):
                self.counters.infra_filtered += 1
                continue
            try:
                ev = from_json(bytes(row["value"]))
            except EventError as exc:
                self.counters.malformed += 1
                log.warning(
                    "malformed message at partition %s offset %s: %s",
                    row["partition"],
                    row["offset"],
                    exc,
                )
                continue
            by_race[ev.race].append((int(row["partition"]), ev))
        records = []
        for race, items in sorted(by_race.items()):
            record = self._process_race(race, items, received)
            if record is not None:
                records.append(record)
        self.last_batch_id = batch_id
        self.counters.batches += 1
        self._persist()
        self._log_batch(batch_id, received, records)

    # ------------------------------------------------------------------ per race
    def _state(self, race: tuple[int, int]) -> RaceState:
        state = self.races.get(race)
        if state is None:
            priors = self.priors_loader(*race)
            state = RaceState(race[0], race[1], self.n_partitions, priors)
            self.races[race] = state
        return state

    def _process_race(
        self, race: tuple[int, int], items: list[tuple[int, RaceEvent]], received: int
    ) -> dict[str, Any] | None:
        if race[0] == SEALED_SEASON and not self.allow_test_season:
            self.counters.refused_test_season += len(items)
            log.error("refusing %d events of sealed season %d", len(items), race[0])
            return None
        existing = self.races.get(race)
        if existing is not None and existing.ended:
            self.counters.after_end += len(items)
            existing.counters.after_end += len(items)
            return None
        state = self._state(race)
        applied = [ev for partition, ev in items if state.apply(ev, partition)]
        final = state.session_complete
        decisions = self._evaluate(state, final)
        alerts = [a for d in decisions for a in on_decision(state, d, self.alert_threshold)]
        for ev in applied:
            if ev.type == "pit_in":
                assert ev.driver_id is not None and ev.lap is not None
                alert = on_pit_in(state, ev.driver_id, ev.lap, self.alert_threshold)
                if alert is not None:
                    alerts.append(alert)
        if self.keep_decisions:
            self.alerts.extend(alerts)
        rec = self.sink.write(state, applied, alerts, received, session_end=final)
        return None if rec is None else rec.to_json()

    def _evaluate(self, state: RaceState, final: bool) -> list[Decision]:
        out = []
        in_laps, out_laps = state.frozen_pits()
        for lap, attacker in state.ready(final):
            pair = state.evaluate(lap, attacker, self.params)
            if pair is None or not pair.eligible:
                state.set_flag(attacker, lap, False, None)
                continue
            assert pair.gap_s is not None  # eligible pairs always have a gap
            inputs = decision_inputs(
                state.timing(),
                state.drivers_at(lap),
                pair,
                state.priors,
                state.race_laps,
                in_laps,
                out_laps,
            )
            prob = self.scorer.probability(inputs)
            d = Decision(
                state.season, state.round, lap, attacker, pair.defender_id, pair.gap_s, inputs, prob
            )
            state.set_flag(attacker, lap, True, prob)
            out.append(d)
        if self.keep_decisions:
            self.decisions.extend(out)
        return out

    # ------------------------------------------------------------------ state snapshots
    def _persist(self) -> None:
        if self.state_dir is None:
            return
        self.state_dir.mkdir(parents=True, exist_ok=True)
        doc = {
            "last_batch_id": self.last_batch_id,
            "n_partitions": self.n_partitions,
            "counters": self.counters.to_json(),
            "races": [s.to_json() for _, s in sorted(self.races.items())],
        }
        path = self.state_dir / STATE_FILE
        tmp = path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(doc, fh, allow_nan=False, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)

    def _restore(self) -> None:
        assert self.state_dir is not None
        path = self.state_dir / STATE_FILE
        if not path.exists():
            return
        doc = json.loads(path.read_text(encoding="utf-8"))
        if int(doc["n_partitions"]) != self.n_partitions:
            raise ProcessorError(
                f"saved state has {doc['n_partitions']} partitions, the topic has "
                f"{self.n_partitions}; use a new checkpoint"
            )
        self.last_batch_id = int(doc["last_batch_id"])
        self.counters = ProcessorCounters(**doc["counters"])
        for data in doc["races"]:
            s = RaceState.from_json(data)
            self.races[(s.season, s.round)] = s
        log.info(
            "restored driver state at batch %d (%d races)", self.last_batch_id, len(self.races)
        )

    def _log_batch(
        self, batch_id: int, received: int, records: Sequence[Mapping[str, Any]]
    ) -> None:
        if self.batch_log is None:
            return
        self.batch_log.parent.mkdir(parents=True, exist_ok=True)
        line = {
            "batch_id": batch_id,
            "batch_received_at_ms": received,
            "counters": self.counters.to_json(),
            "races": list(records),
        }
        with self.batch_log.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(line, allow_nan=False, separators=(",", ":")) + "\n")
