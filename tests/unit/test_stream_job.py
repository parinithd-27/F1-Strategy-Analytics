"""Stream job: the foreachBatch processor (filtering, idempotent replay, restart, session end,
the sealed season) and the job's configuration helpers (checkpoint, model, Kafka, listener)."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from _stream_fixtures import CountingStore, processor, row, scenario_doc_dir, scenario_inputs
from _undercut_fixtures import Scenario, standard_cars
from pyspark.sql import SparkSession

from f1sa.serving import keys as K
from f1sa.streaming import stream_job as J
from f1sa.streaming.events import RaceEvent, new_event_id
from f1sa.streaming.parity import kafka_batches, replay_into
from f1sa.streaming.processor import ProcessorError, StreamProcessor
from f1sa.streaming.producer import build_events

SC = Scenario(2023, 1, standard_cars())


def events(n_partitions: int = 1) -> list[Any]:
    return build_events(scenario_inputs(SC), n_partitions)


def run(proc: StreamProcessor, batches: list[list[dict[str, Any]]], first: int = 0) -> None:
    replay_into(proc, batches, first_batch_id=first)


# ------------------------------------------------------------------ processor
def test_a_full_replay_ends_the_race_and_flags_the_undercut() -> None:
    store = CountingStore()
    proc = processor(store, keep_decisions=True)
    run(proc, kafka_batches(events(), 1, 10))
    assert proc.ended(2023, 1) and store.ends == [(2023, 1)]
    assert {(d.decision_lap, d.attacker_id, d.defender_id) for d in proc.decisions} >= {
        (5, "a", "d")
    }
    assert all(0.0 < d.probability < 1.0 for d in proc.decisions)
    # a pits on lap 6 after being a candidate behind d at lap 5: one attempt alert, P at L-1
    (att,) = [x for x in proc.alerts if x.reason == "attempt"]
    assert (att.attacker_id, att.defender_id, att.decision_lap) == ("a", "d", 5)
    p5 = next(d.probability for d in proc.decisions if (d.decision_lap, d.attacker_id) == (5, "a"))
    assert att.undercut_prob == p5
    assert len(proc.alerts) < len(proc.decisions)
    assert {m for m, _ in store.get_order(2023, 1)} == {"lead", "d", "a", "back"}
    assert proc.counters.batches == len(kafka_batches(events(), 1, 10))


def test_infra_and_malformed_messages_are_dropped() -> None:
    store = CountingStore()
    proc = processor(store)
    ok = events()[0].event
    proc.process_rows(
        [
            row(b'{"type":"infra_test"}', 0, 0, [("test", b"infra")]),
            row(b"{not json", 0, 1),
            row(ok.to_json().encode(), 0, 2),
        ],
        0,
    )
    assert (proc.counters.infra_filtered, proc.counters.malformed, proc.counters.rows) == (1, 1, 3)
    assert proc.races[(2023, 1)].counters.applied == 1


def test_a_replayed_batch_id_is_skipped() -> None:
    store = CountingStore()
    proc = processor(store)
    batches = kafka_batches(events(), 1, 10)
    run(proc, batches[:2])
    writes = len(store.batches)
    proc.process_rows(batches[1], 1)  # Spark re-runs the last batch after a failure
    assert proc.counters.skipped_batches == 1 and len(store.batches) == writes


def test_nothing_is_written_for_a_race_after_its_session_end() -> None:
    store = CountingStore()
    proc = processor(store)
    run(proc, kafka_batches(events(), 1, 50))
    writes, ttl = len(store.batches), store.client.ttl(K.driver_key(2023, 1, "a"))
    late = RaceEvent(new_event_id(), "lap", 2023, 1, 1e5, driver_id="a", lap=99, lap_time_s=90.0)
    proc.process_rows([row(late.to_json().encode(), 0, 10_000)], 10_000)
    assert len(store.batches) == writes and proc.counters.after_end == 1
    assert store.client.ttl(K.driver_key(2023, 1, "a")) <= ttl


def test_restart_resumes_from_the_saved_state(tmp_path: Path) -> None:
    batches = kafka_batches(events(2), 2, 7)
    straight = processor(CountingStore(), n_partitions=2, keep_decisions=True)
    run(straight, batches)
    state_dir = tmp_path / "driver_state"
    first = processor(CountingStore(), n_partitions=2, state_dir=state_dir, keep_decisions=True)
    half = len(batches) // 2
    run(first, batches[:half])
    second = processor(CountingStore(), n_partitions=2, state_dir=state_dir, keep_decisions=True)
    assert second.last_batch_id == half - 1
    run(second, batches[half - 1 :], first=half - 1)  # Spark replays the last batch first
    assert second.counters.skipped_batches == 1
    resumed = [*first.decisions, *second.decisions]
    assert resumed == straight.decisions and second.ended(2023, 1)


def test_restore_refuses_another_partition_count(tmp_path: Path) -> None:
    state_dir = tmp_path / "s"
    run(
        processor(CountingStore(), n_partitions=2, state_dir=state_dir),
        kafka_batches(events(2), 2, 7)[:1],
    )
    with pytest.raises(ProcessorError, match="partitions"):
        processor(CountingStore(), n_partitions=3, state_dir=state_dir)


def test_the_sealed_season_is_refused_unless_allowed() -> None:
    called: list[tuple[int, int]] = []

    def load(s: int, r: int) -> Any:
        called.append((s, r))
        from f1sa.streaming.live_features import RacePriors

        return RacePriors(s, r)

    ev = RaceEvent(new_event_id(), "lap", 2025, 3, 90.0, driver_id="a", lap=1, lap_time_s=90.0)
    proc = processor(CountingStore())
    proc.priors_loader = load
    proc.process_rows([row(ev.to_json().encode())], 0)
    assert proc.counters.refused_test_season == 1 and called == [] and not proc.races
    allowed = processor(CountingStore(), allow_test_season=True)
    allowed.priors_loader = load
    allowed.process_rows([row(ev.to_json().encode())], 0)
    assert called == [(2025, 3)]


def test_scorer_inputs_must_match_the_live_inputs() -> None:
    from _stream_fixtures import scorer_doc

    from f1sa.model.lr_scorer import LRScorer

    doc = scorer_doc()
    doc["inputs"] = doc["inputs"][::-1]
    doc["assembled_columns"] = [c for s in doc["inputs"] for c in _cols(s)]
    with pytest.raises(ProcessorError, match="differ"):
        StreamProcessor(
            scorer=LRScorer.from_json(doc),
            params=processor(CountingStore()).params,
            sink=processor(CountingStore()).sink,
            priors_loader=lambda s, r: None,  # type: ignore[arg-type,return-value]
            n_partitions=1,
        )


def _cols(spec: dict[str, Any]) -> list[str]:
    n = spec["name"]
    if spec["kind"] == "numeric":
        return [f"f_{n}", f"f_{n}_missing"]
    return [f"f_{n}__{c.lower()}" for c in [*spec["categories"], "unknown"]]


def test_batch_log_lines(tmp_path: Path) -> None:
    log = tmp_path / "results" / "stream_batches.jsonl"
    proc = processor(CountingStore(), batch_log=log)
    run(proc, kafka_batches(events(), 1, 100))
    lines = [json.loads(x) for x in log.read_text().splitlines()]
    assert [x["batch_id"] for x in lines] == list(range(len(lines)))
    assert lines[-1]["races"][-1]["session_end"] is True
    assert all(r["write_ms"] >= 0 for x in lines for r in x["races"])


# ------------------------------------------------------------------ job configuration
@pytest.mark.parametrize("path", ["data/checkpoints/demo", "data/checkpoints/a/b"])
def test_checkpoint_inside_data_checkpoints(path: str) -> None:
    assert J.check_checkpoint(Path(path)) == Path(path)


@pytest.mark.parametrize("path", ["data/checkpoints", "data/lake/x", "/tmp/ck", "checkpoints"])
def test_checkpoint_elsewhere_is_refused(path: str) -> None:
    with pytest.raises(J.JobError, match="checkpoint"):
        J.check_checkpoint(Path(path))


def test_load_model_checks_the_metadata(tmp_path: Path) -> None:
    good = scenario_doc_dir(tmp_path / "good")
    scorer, params, meta = J.load_model(good)
    assert params.gap_threshold_s == 3.5 and meta["k"] == 2 and len(scorer.inputs) == 21
    bad = scenario_doc_dir(tmp_path / "bad", g=5.0)
    with pytest.raises(J.JobError, match="expected"):
        J.load_model(bad)
    other = scenario_doc_dir(tmp_path / "other", features=["gap_s"])
    with pytest.raises(J.JobError, match="differ"):
        J.load_model(other)
    with pytest.raises(J.JobError, match="missing"):
        J.load_model(tmp_path / "none")


def test_kafka_options() -> None:
    from f1sa.config import get_settings

    opts = J.kafka_options(get_settings(), 500)
    assert opts["startingOffsets"] == "latest" and opts["maxOffsetsPerTrigger"] == "500"
    assert opts["includeHeaders"] == "true" and opts["subscribe"] == "f1.race.events"
    with pytest.raises(J.JobError):
        J.kafka_options(get_settings(), 0)


def test_spark_conf_adds_the_pinned_connector_only_when_asked() -> None:
    on = J.spark_conf(packages=True)
    assert on["spark.jars.packages"] == "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1"
    assert on["spark.jars.ivy"].endswith("data/cache/ivy")
    assert "spark.jars.packages" not in J.spark_conf(packages=False)
    assert on["spark.hadoop.dfs.replication"] == "1"


def test_connector_detection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPARK_HOME", str(tmp_path))
    (tmp_path / "jars").mkdir()
    (tmp_path / "jars" / J.KAFKA_JAR).write_bytes(b"")
    assert J.connector_on_classpath() is True


def test_infra_filter_drops_only_the_infra_message(spark: SparkSession) -> None:
    schema = "value string, headers array<struct<key:string,value:binary>>"
    df = spark.createDataFrame(
        [
            ("keep-null", None),
            ("keep-other", [("test", bytearray(b"other"))]),
            ("drop", [("x", None), ("test", bytearray(b"infra"))]),
        ],
        schema,
    )
    assert sorted(r["value"] for r in df.where(J.infra_filter()).collect()) == [
        "keep-null",
        "keep-other",
    ]


def test_listener_appends_progress_json(tmp_path: Path) -> None:
    path, seen = tmp_path / "p.jsonl", threading.Event()
    listener = J.make_listener(path, seen)
    listener.onQueryProgress(SimpleNamespace(progress=SimpleNamespace(json='{"batchId": 0}')))
    listener.onQueryProgress(SimpleNamespace(progress=SimpleNamespace(json='{"batchId": 1}')))
    assert seen.is_set()
    assert [json.loads(x)["batchId"] for x in path.read_text().splitlines()] == [0, 1]


def test_parse_race() -> None:
    assert J.parse_race("2024:5") == (2024, 5)
    with pytest.raises(Exception, match="SEASON:ROUND"):
        J.parse_race("2024-5")


def test_main_refuses_a_checkpoint_outside_data() -> None:
    assert J.main(["--checkpoint", "/tmp/elsewhere"]) == 2


def test_shutdown_removes_the_listener_before_stopping() -> None:
    calls: list[str] = []

    class Q:
        isActive = True

        def stop(self) -> None:
            calls.append("stop")

    spark = SimpleNamespace(
        streams=SimpleNamespace(removeListener=lambda listener: calls.append("remove")),
        stop=lambda: calls.append("spark"),
    )
    J.shutdown(spark, Q(), object())
    assert calls == ["remove", "stop", "spark"]
