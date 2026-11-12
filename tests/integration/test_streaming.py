"""Kafka -> Structured Streaming -> Redis on the real `make up` infrastructure (#19, #20, #23, #24).

The sample race 2024 R5 is replayed at speed 60 through the real topic with the real stream job
(a subprocess on the host, Kafka connector from ``data/cache/ivy``, a tmp lake built from
``data/sample``, a tmp checkpoint). An infrastructure test message (header ``test=infra``) is
sent while the job runs. The race's keys ``race:2024:5:*`` are deleted before the run.

Asserted: every driver has a hash with the 9 fields; the order sorted set holds every driver;
after ``session_end`` every key has ``0 < TTL <= REDIS_TTL_AFTER_SESSION_S`` and the TTLs do not
go back up (nothing is written afterwards); the alert policy: an ``opportunity`` alert for every
pair that becomes a batch candidate, an ``attempt`` alert for every batch attempt the detector
flags (every labelled attempt included); the latency rows have all segments.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from f1sa.config import get_settings
from f1sa.features.build import read_feature
from f1sa.features.undercut_rule import RuleParams
from f1sa.serving import keys as K
from f1sa.serving.keys import DRIVER_FIELDS
from f1sa.serving.redis_store import RaceStore
from f1sa.streaming import latency as L
from f1sa.streaming import parity as PT
from f1sa.streaming import producer as PR
from f1sa.streaming.demo import check_services, wait_ready

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "models" / "undercut" / "deploy_real_G3.5_k2"
SEASON, ROUND = 2024, 5
TIMEOUT_S = 900


def _send_infra_message(bootstrap: str, topic: str) -> None:
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    p.produce(
        topic,
        value=json.dumps({"type": "infra_test", "id": str(uuid.uuid4())}).encode(),
        headers=[("test", b"infra")],
    )
    assert p.flush(10) == 0


def test_replay_of_the_sample_race_reaches_redis(
    spark: SparkSession,
    sample_clean_lake: Any,
    sample_features_root: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not (MODEL_DIR / "lr" / "scorer.json").is_file():
        pytest.skip("deployment model not saved (make model-save-full); it is gitignored")
    lake_uri = sample_clean_lake.settings().data_lake_uri
    monkeypatch.setenv("DATA_LAKE_URI", lake_uri)
    settings = get_settings()
    check_services(settings)
    store = RaceStore.from_settings(settings, socket_timeout=5)
    old = store.race_keys(SEASON, ROUND)
    if old:
        store.client.delete(*old)

    # batch reference: eligible pairs per lap, and the attempts the detector flags at L-1
    feats = {
        t: read_feature(spark, sample_features_root, t)
        for t in ("laps_enriched", "pit_loss", "undercut_attempts")
    }
    race = PT.batch_race_data(feats["laps_enriched"], feats["pit_loss"])[(SEASON, ROUND)]
    pairs = PT.batch_eligible(race, RuleParams())
    new_candidates = {
        (a, d, lap)
        for lap, ps in pairs.items()
        for a, d in ps
        if (a, d) not in pairs.get(lap - 1, set())
    }
    flagged = PT.eligible_attempts(feats["undercut_attempts"], 3.5).get((SEASON, ROUND), set())
    labelled = {
        (r["attacker_id"], r["defender_id"], int(r["decision_lap"]))
        for r in feats["undercut_attempts"]
        .where("gap_threshold_s = 3.5 AND response_window_laps = 2")
        .where("outcome_category IN ('success', 'fail')")
        .where(f"season = {SEASON} AND round = {ROUND}")
        .collect()
    }
    drivers = {r.driver_id for r in race.laps}

    pubsub = store.client.pubsub()  # type: ignore[no-untyped-call]
    L.subscribe_channel(pubsub, SEASON, ROUND)
    rows: list[dict[str, Any]] = []
    listener = threading.Thread(
        target=lambda: rows.extend(L.collect(pubsub, timeout_s=TIMEOUT_S)[0]), daemon=True
    )
    listener.start()

    ready = tmp_path / "ready"
    job = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "f1sa.streaming.stream_job",
            "--checkpoint",
            f"data/checkpoints/it_{int(time.time())}",
            "--model-dir",
            str(MODEL_DIR),
            "--results-dir",
            str(tmp_path / "results"),
            "--ready-file",
            str(ready),
            "--stop-after-session-end",
            f"{SEASON}:{ROUND}",
            "--timeout-s",
            str(TIMEOUT_S),
            "--ivy-dir",
            str(REPO_ROOT / "data" / "cache" / "ivy"),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        wait_ready(job, ready, 300)
        _send_infra_message(settings.kafka_bootstrap, settings.kafka_topic_events)
        rc = PR.main(
            ["--season", str(SEASON), "--round", str(ROUND), "--speed", "60"],
            spark_factory=lambda: spark,
        )
        assert rc == 0
        out, _ = job.communicate(timeout=TIMEOUT_S)
    finally:
        if job.poll() is None:
            job.kill()
    assert job.returncode == 0, out
    counters = json.loads(out.strip().splitlines()[-1])["processor"]
    assert counters["malformed"] == 0 and counters["refused_test_season"] == 0
    listener.join(timeout=30)

    # state for every driver, complete order
    for d in drivers:
        h = store.client.hgetall(K.driver_key(SEASON, ROUND, d))
        assert tuple(h) == DRIVER_FIELDS, d
    order = store.get_order(SEASON, ROUND)
    assert {m for m, _ in order} == drivers and sorted(p for _, p in order) == list(
        range(1, len(drivers) + 1)
    )

    # TTL after session_end, and nothing written afterwards
    keys = store.race_keys(SEASON, ROUND)
    first = {k: store.client.ttl(k) for k in keys}
    assert all(0 < t <= settings.redis_ttl_after_session_s for t in first.values()), first
    time.sleep(3)
    assert all(store.client.ttl(k) <= first[k] for k in keys)

    # alert policy: opportunity on a new candidate (or a threshold crossing), attempt on a pit
    types = [r["type"] for r in rows]
    assert types[-1] == "session_end"
    alerts = [r for r in rows if r["type"] == "undercut_alert"]
    key = lambda r: (r["attacker_id"], r["defender_id"], int(r["lap"]))  # noqa: E731
    got_new = {key(r) for r in alerts if r["trigger"] == "new_candidate"}
    got_attempts = {key(r) for r in alerts if r["reason"] == "attempt"}
    assert got_new == new_candidates and got_attempts == flagged and labelled <= got_attempts
    assert len(labelled) >= 1
    assert {r["reason"] for r in alerts} <= {"opportunity", "attempt"}
    assert len(alerts) < sum(len(v) for v in pairs.values())  # no longer one per pair and lap
    laps = [r for r in rows if r["type"] == "lap"]
    assert len(laps) == len(race.laps)
    assert all(r["end_to_end_ms"] is not None and r["end_to_end_ms"] >= 0 for r in laps)
    assert (tmp_path / "results" / "stream_progress.jsonl").is_file()
    assert (tmp_path / "results" / "stream_batches.jsonl").is_file()
