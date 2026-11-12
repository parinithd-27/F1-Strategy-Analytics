"""Structured Streaming undercut detector (#20): Kafka -> foreachBatch -> Redis.

``python -m f1sa.streaming.stream_job [--checkpoint data/checkpoints/stream_job]``

* ``readStream.format("kafka")`` on ``Settings.kafka_topic_events`` with ``startingOffsets=latest``,
  ``maxOffsetsPerTrigger`` and headers; the Step 2 infrastructure message (header ``test=infra``)
  is filtered out in Spark (and again in Python).
* ``trigger(processingTime="500 milliseconds")`` and ``foreachBatch`` with
  ``processor.StreamProcessor`` (driver-side state; no Spark stateful operator).
* Scoring: the pure-Python calibrated LR of ``models/undercut/deploy_real_G3.5_k2`` (no Spark ML
  per batch). The metadata must say feature set ``real``, G = 3.5, k = 2; the rule's gap threshold
  is taken from it.
* Checkpoints under ``data/checkpoints/<name>`` (``spark/`` for Spark, ``driver_state/`` for the
  processor); anywhere else is refused.
* ``results/stream_progress.jsonl``: one ``StreamingQueryProgress`` JSON per trigger;
  ``results/stream_batches.jsonl``: per micro-batch counts and the Redis write time. Both are
  started fresh by each run of the job.
* Host mode adds the Kafka connector with ``spark.jars.packages`` (exact pinned coordinate, ivy
  cache ``data/cache/ivy``); in the app image the connector is already in ``$SPARK_HOME/jars``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from f1sa.config import Settings, get_settings
from f1sa.features.undercut_rule import MIN_ATTEMPT_LAP, RuleParams
from f1sa.lake.paths import is_within
from f1sa.model.lr_scorer import LRScorer
from f1sa.streaming.alerts import ALERT_THRESHOLD
from f1sa.streaming.processor import StreamProcessor
from f1sa.streaming.redis_sink import RedisSink

log = logging.getLogger(__name__)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1"
KAFKA_JAR = "spark-sql-kafka-0-10_2.13-4.2.1.jar"
CHECKPOINT_ROOT = Path("data/checkpoints")
IVY_DIR = Path("data/cache/ivy")
DEFAULT_MODEL_DIR = Path("models/undercut/deploy_real_G3.5_k2")
DEFAULT_MAX_OFFSETS = 2000
TRIGGER = "500 milliseconds"
EXPECTED_MODEL = {"feature_set": "real", "g": 3.5, "k": 2, "model": "lr"}


class JobError(RuntimeError):
    pass


# ============================================================================== configuration
def check_checkpoint(path: Path) -> Path:
    """``path`` if it lies strictly inside ./data/checkpoints (gitignored), else ``JobError``."""
    if path.resolve() == CHECKPOINT_ROOT.resolve() or not is_within(path, CHECKPOINT_ROOT):
        raise JobError(f"checkpoint must be a directory inside ./{CHECKPOINT_ROOT}, got {path}")
    return path


def load_model(model_dir: Path) -> tuple[LRScorer, RuleParams, dict[str, Any]]:
    """The deployed calibrated LR scorer and the rule parameters of its setting."""
    meta_path, scorer_path = model_dir / "lr" / "metadata.json", model_dir / "lr" / "scorer.json"
    for p in (meta_path, scorer_path):
        if not p.is_file():
            raise JobError(f"deployment model file missing: {p} (run make model-save-full)")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    wrong = {k: meta.get(k) for k, v in EXPECTED_MODEL.items() if meta.get(k) != v}
    if wrong:
        raise JobError(f"{meta_path}: expected {EXPECTED_MODEL}, got {wrong}")
    scorer = LRScorer.load(scorer_path)
    names = [s.name for s in scorer.inputs]
    if names != list(meta.get("features", [])):
        raise JobError(f"{scorer_path}: inputs differ from the metadata features")
    params = RuleParams(gap_threshold_s=float(meta["g"]), min_attempt_lap=MIN_ATTEMPT_LAP)
    return scorer, params, meta


def kafka_options(settings: Settings, max_offsets: int) -> dict[str, str]:
    if max_offsets < 1:
        raise JobError(f"maxOffsetsPerTrigger must be >= 1, got {max_offsets}")
    return {
        "kafka.bootstrap.servers": settings.kafka_bootstrap,
        "subscribe": settings.kafka_topic_events,
        "startingOffsets": "latest",
        "maxOffsetsPerTrigger": str(max_offsets),
        "includeHeaders": "true",
        "failOnDataLoss": "true",
    }


def connector_on_classpath() -> bool:
    """True if the Kafka connector jar is already installed (the app image), else False."""
    import pyspark

    dirs = [Path(pyspark.__file__).parent / "jars"]
    if os.environ.get("SPARK_HOME"):
        dirs.append(Path(os.environ["SPARK_HOME"]) / "jars")
    return any((d / KAFKA_JAR).is_file() for d in dirs)


def spark_conf(
    *, packages: bool, master: str = "local[2]", ivy_dir: Path = IVY_DIR
) -> dict[str, str]:
    from f1sa.etl.spark import RUNTIME_CONF, hadoop_config

    conf = {
        "spark.master": master,
        "spark.app.name": "f1sa-stream-job",
        "spark.sql.shuffle.partitions": "4",
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
        **RUNTIME_CONF,
        **hadoop_config(),
    }
    if master.startswith("local"):
        conf["spark.driver.host"] = "127.0.0.1"
        conf["spark.driver.bindAddress"] = "127.0.0.1"
    if packages:
        conf["spark.jars.packages"] = KAFKA_PACKAGE
        conf["spark.jars.ivy"] = str(ivy_dir.resolve())
    return conf


def build_stream_spark(*, packages: bool, master: str = "local[2]", ivy_dir: Path = IVY_DIR) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    for k, v in spark_conf(packages=packages, master=master, ivy_dir=ivy_dir).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def infra_filter() -> Any:
    """Column: True for every message except the ``test=infra`` one (null headers kept)."""
    from pyspark.sql import functions as F

    infra = F.exists(
        "headers",
        lambda h: (h["key"] == F.lit("test")) & (h["value"].cast("string") == F.lit("infra")),
    )
    return ~F.coalesce(infra, F.lit(False))


def make_listener(path: Path, first_progress: threading.Event) -> Any:
    """A ``StreamingQueryListener`` appending each progress JSON to ``path``."""
    from pyspark.sql.streaming.listener import StreamingQueryListener

    class ProgressListener(StreamingQueryListener):
        def onQueryStarted(self, event: Any) -> None:
            return None

        def onQueryProgress(self, event: Any) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(json.loads(event.progress.json), separators=(",", ":")) + "\n")
            first_progress.set()

        def onQueryIdle(self, event: Any) -> None:
            first_progress.set()

        def onQueryTerminated(self, event: Any) -> None:
            return None

    return ProgressListener()


def shutdown(spark: Any, query: Any, listener: Any) -> None:
    """Stop cleanly: remove the Python listener first (otherwise the JVM calls its
    ``onQueryTerminated`` after the Python callback server has gone), then the query and Spark."""
    spark.streams.removeListener(listener)
    if query.isActive:
        query.stop()
    spark.stop()


# ============================================================================== CLI
def parse_race(text: str) -> tuple[int, int]:
    try:
        s, r = text.split(":")
        return int(s), int(r)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected SEASON:ROUND, got {text!r}") from exc


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="f1sa-stream", description=__doc__.split("\n\n")[0])
    p.add_argument("--checkpoint", type=Path, default=CHECKPOINT_ROOT / "stream_job")
    p.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    p.add_argument("--max-offsets-per-trigger", type=int, default=DEFAULT_MAX_OFFSETS)
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    p.add_argument("--stop-after-session-end", type=parse_race, default=None, metavar="S:R")
    p.add_argument("--ready-file", type=Path, default=None)
    p.add_argument("--timeout-s", type=float, default=None, help="stop and fail after this long")
    p.add_argument("--allow-test-season", action="store_true")
    p.add_argument("--master", default="local[2]")
    p.add_argument("--ivy-dir", type=Path, default=IVY_DIR, help="ivy cache (host mode)")
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    args = parse_args(argv)
    try:
        checkpoint = check_checkpoint(args.checkpoint)
        scorer, params, meta = load_model(args.model_dir)
        options = kafka_options(get_settings(), args.max_offsets_per_trigger)
    except JobError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    settings = get_settings()
    from f1sa.serving.redis_store import RaceStore
    from f1sa.streaming.priors import load_priors
    from f1sa.streaming.producer import topic_partitions

    n_partitions = topic_partitions(settings.kafka_bootstrap, settings.kafka_topic_events)
    progress_log = args.results_dir / "stream_progress.jsonl"
    batch_log = args.results_dir / "stream_batches.jsonl"
    for path in (progress_log, batch_log):  # one run per file
        path.unlink(missing_ok=True)
    spark = build_stream_spark(
        packages=not connector_on_classpath(), master=args.master, ivy_dir=args.ivy_dir
    )

    def priors(season: int, round_: int) -> Any:
        return load_priors(
            spark, settings.data_lake_uri, season, round_, allow_test_season=args.allow_test_season
        )

    processor = StreamProcessor(
        scorer=scorer,
        params=params,
        sink=RedisSink(RaceStore.from_settings(settings)),
        priors_loader=priors,
        n_partitions=n_partitions,
        allow_test_season=args.allow_test_season,
        state_dir=checkpoint / "driver_state",
        batch_log=batch_log,
        alert_threshold=float(meta.get("decision_threshold", ALERT_THRESHOLD)),
    )
    first_progress = threading.Event()
    listener = make_listener(progress_log, first_progress)
    spark.streams.addListener(listener)
    source = spark.readStream.format("kafka").options(**options).load()
    query = (
        source.where(infra_filter())
        .select("value", "headers", "partition", "offset")
        .writeStream.foreachBatch(processor.process_batch)
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", str((checkpoint / "spark").resolve()))
        .queryName("f1sa_undercut_detector")
        .start()
    )
    started = time.monotonic()
    if not first_progress.wait(timeout=180):
        print("error: the query reported no progress within 180 s", file=sys.stderr)
        shutdown(spark, query, listener)
        return 1
    if args.ready_file is not None:
        args.ready_file.parent.mkdir(parents=True, exist_ok=True)
        args.ready_file.write_text(f"{query.id}\n", encoding="utf-8")
    print(
        f"READY query={query.id} partitions={n_partitions} G={params.gap_threshold_s}", flush=True
    )
    rc = 0
    try:
        while query.isActive:
            query.awaitTermination(1.0)
            race = args.stop_after_session_end
            if race is not None and processor.ended(*race):
                print(f"session_end of {race[0]} R{race[1]} processed; stopping", flush=True)
                break
            if args.timeout_s is not None and time.monotonic() - started > args.timeout_s:
                print(f"error: timeout after {args.timeout_s} s", file=sys.stderr)
                rc = 1
                break
        if query.exception() is not None:
            print(f"error: query failed: {query.exception()}", file=sys.stderr)
            rc = 1
    finally:
        shutdown(spark, query, listener)
    print(json.dumps({"processor": processor.counters.to_json()}), flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
