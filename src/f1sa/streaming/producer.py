"""Kafka race replay (#19): one race from the clean zone, in race order, at a speed factor.

``python -m f1sa.streaming.producer --season 2024 --round 5 --speed 10``

* Reads only the race's ``season=S/round=R`` partitions of the clean tables under
  ``Settings.data_lake_uri`` (``file://`` for the sample, ``hdfs://`` inside the app container).
* Tyre compound and age come from ``etl.df_jobs.tyre_age`` and the race distance from
  ``df_jobs.race_distance``, the same definitions the batch features use; the per-lap track flags
  are the batch's ``track_neutral`` (SC / VSC / red flag / neutralised) and ``track_red_flag``.
  These three context values are replayed on each lap event (see ``streaming/events.py``).
* Events, ordered by race time: ``pit_in`` (in-lap L) just before ``lap`` L at the line crossing,
  ``pit_out`` (lap L+1) at the crossing plus the stop time (never after the next crossing), then
  one ``session_end`` per topic partition.
* Pacing: an event at race time t is sent at wall time ``start + (t - t_first) / speed``.
* Kafka: ``acks=all``, idempotent, key = ``driver_id``; any delivery error fails the run.
* Season 2025 (the sealed test season) is refused before anything is read unless
  ``--allow-test-season`` is given.

Exit codes: 0 ok, 1 delivery failure, 2 refused or bad arguments, 3 race data missing or invalid.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import zlib
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from f1sa.config import get_settings
from f1sa.streaming.events import (
    COMPOUNDS,
    SESSION_END_KEY,
    RaceEvent,
    new_event_id,
)
from f1sa.streaming.live_features import spark_round
from f1sa.streaming.processor import SEALED_SEASON
from f1sa.streaming.redis_sink import now_ms

log = logging.getLogger(__name__)

CLEAN_TABLES = ("laps", "pitstops", "stints", "results")
_KIND_ORDER = {"pit_in": 0, "lap": 1, "pit_out": 2}


class ProducerError(RuntimeError):
    """Race data that cannot be replayed (exit 3)."""


class RefusedError(RuntimeError):
    """A replay the producer refuses to run (exit 2)."""


# ============================================================================== race inputs
@dataclass(frozen=True)
class LapRecord:
    driver_id: str
    lap: int
    position: int | None
    lap_time_s: float | None
    compound: str | None
    tyre_age: int | None
    track_neutral: bool
    track_red_flag: bool


@dataclass(frozen=True)
class StopRecord:
    driver_id: str
    lap: int
    duration_s: float | None


@dataclass(frozen=True)
class RaceInputs:
    season: int
    round: int
    laps: tuple[LapRecord, ...]
    stops: tuple[StopRecord, ...]
    race_laps: int | None


def check_season(season: int, allow_test_season: bool) -> None:
    if season == SEALED_SEASON and not allow_test_season:
        raise RefusedError(
            f"season {season} is the sealed test season; pass --allow-test-season to replay it"
        )


def _compound(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).upper()
    return text if text in COMPOUNDS else None


def race_inputs(frames: Mapping[str, Any], season: int, round_: int) -> RaceInputs:
    """Collect one race from clean-zone DataFrames (``laps``, ``pitstops``, ``stints``,
    ``results``; other races are filtered out). Spark is imported here only."""
    from pyspark.sql import functions as F

    from f1sa.etl.df_jobs import race_distance, tyre_age

    def race(df: Any) -> Any:
        return df.where((F.col("season") == season) & (F.col("round") == round_))

    laps, stints = race(frames["laps"]), race(frames["stints"])
    results, stops = race(frames["results"]), race(frames["pitstops"])
    neutral = F.lit(False)
    for c in ("is_neutralised_lap", "is_sc_lap", "is_vsc_lap", "is_red_flag_lap"):
        neutral = neutral | F.coalesce(F.col(c), F.lit(False))
    rows = (
        tyre_age(laps, stints)
        .select(
            "driver_id",
            "lap",
            "position",
            "lap_time_s",
            "compound",
            "tyre_age_laps",
            neutral.alias("track_neutral"),
            F.coalesce(F.col("is_red_flag_lap"), F.lit(False)).alias("track_red_flag"),
        )
        .collect()
    )
    if not rows:
        raise ProducerError(f"{season} R{round_}: no laps in the clean zone")
    lap_records = tuple(
        LapRecord(
            driver_id=r["driver_id"],
            lap=int(r["lap"]),
            position=r["position"],
            lap_time_s=r["lap_time_s"],
            compound=_compound(r["compound"]),
            tyre_age=r["tyre_age_laps"],
            track_neutral=bool(r["track_neutral"]),
            track_red_flag=bool(r["track_red_flag"]),
        )
        for r in rows
    )
    keys = [(r.driver_id, r.lap) for r in lap_records]
    if len(keys) != len(set(keys)):
        raise ProducerError(
            f"{season} R{round_}: duplicate (driver, lap) rows (overlapping stints?)"
        )
    distance = race_distance(results, laps).collect()
    grouped: dict[tuple[str, int], list[float | None]] = defaultdict(list)
    for s in stops.select("driver_id", "lap", "duration_s").collect():
        if s["lap"] is not None:
            grouped[(s["driver_id"], int(s["lap"]))].append(s["duration_s"])
    stop_records = tuple(
        StopRecord(
            d,
            lap,
            spark_round(sum(x for x in ds if x is not None), 3)
            if all(x is not None for x in ds)
            else None,
        )
        for (d, lap), ds in sorted(grouped.items())
    )
    return RaceInputs(
        season=season,
        round=round_,
        laps=tuple(sorted(lap_records, key=lambda r: (r.lap, r.driver_id))),
        stops=stop_records,
        race_laps=int(distance[0]["race_laps"]) if distance and distance[0]["race_laps"] else None,
    )


def load_race_inputs(spark: Any, lake_uri: str, season: int, round_: int) -> RaceInputs:
    """``race_inputs`` from the race's clean partitions under ``lake_uri``."""
    from f1sa.etl.clean import spark_uri
    from f1sa.lake import schemas as S
    from f1sa.streaming.priors import PriorsError, read_race_partition

    clean = f"{spark_uri(lake_uri)}/clean"
    try:
        frames = {
            t: read_race_partition(spark, clean, t, S.CLEAN_SCHEMAS[t], season, round_)
            for t in CLEAN_TABLES
        }
    except PriorsError as exc:
        raise ProducerError(str(exc)) from exc
    return race_inputs(frames, season, round_)


# ============================================================================== events
@dataclass(frozen=True)
class Outgoing:
    """An event and its explicit partition (``session_end`` only; else Kafka's key partitioner)."""

    event: RaceEvent
    partition: int | None = None

    @property
    def key(self) -> str:
        return self.event.driver_id or SESSION_END_KEY


def _race_times(laps: Sequence[LapRecord]) -> dict[tuple[str, int], float]:
    """Line-crossing race time of every lap: the cumulative time (``timing_gaps`` rounding), or,
    after an unknown lap time, max(the car's previous time, the leader's time at that lap)."""
    by_driver: dict[str, list[LapRecord]] = defaultdict(list)
    for r in laps:
        by_driver[r.driver_id].append(r)
    cum: dict[tuple[str, int], float | None] = {}
    for d, rows in by_driver.items():
        total: float | None = None
        unknown = False
        for r in sorted(rows, key=lambda r: r.lap):
            if r.lap_time_s is None:
                unknown = True
            else:
                total = r.lap_time_s if total is None else total + r.lap_time_s
            cum[(d, r.lap)] = None if unknown or total is None else spark_round(total, 3)
    leader: dict[int, float] = {}
    for r in laps:
        c = cum[(r.driver_id, r.lap)]
        if c is not None and (r.lap not in leader or c < leader[r.lap]):
            leader[r.lap] = c
    out: dict[tuple[str, int], float] = {}
    for d, rows in by_driver.items():
        prev = 0.0
        for r in sorted(rows, key=lambda r: r.lap):
            c = cum[(d, r.lap)]
            t = c if c is not None else max(prev, leader.get(r.lap, prev))
            out[(d, r.lap)] = t
            prev = t
    return out


def build_events(inputs: RaceInputs, n_partitions: int) -> list[Outgoing]:
    """Every event of the race in send order (``produced_at_ms`` unset)."""
    if n_partitions < 1:
        raise ValueError(f"n_partitions must be >= 1, got {n_partitions}")
    times = _race_times(inputs.laps)
    s, r = inputs.season, inputs.round
    keyed: list[tuple[float, int, str, int, RaceEvent]] = []
    for lap in inputs.laps:
        t = times[(lap.driver_id, lap.lap)]
        ev = RaceEvent(
            event_id=new_event_id(),
            type="lap",
            season=s,
            round=r,
            race_time_s=t,
            driver_id=lap.driver_id,
            lap=lap.lap,
            lap_time_s=lap.lap_time_s,
            position=lap.position,
            compound=lap.compound,
            tyre_age=lap.tyre_age,
            track_neutral=lap.track_neutral,
            track_red_flag=lap.track_red_flag,
            race_laps=inputs.race_laps,
        )
        keyed.append((t, _KIND_ORDER["lap"], lap.driver_id, lap.lap, ev))
    last_time: dict[str, float] = {}
    for (d, _n), t in times.items():
        last_time[d] = max(last_time.get(d, 0.0), t)
    for stop in inputs.stops:
        t_in = times.get((stop.driver_id, stop.lap))
        if t_in is None:  # a stop on a lap the car never completed (retired in the pits)
            t_in = last_time.get(stop.driver_id, 0.0)
        t_out = t_in + (stop.duration_s or 0.0)
        nxt = times.get((stop.driver_id, stop.lap + 1))
        if nxt is not None:
            t_out = min(t_out, nxt)
        for kind, lap_no, t in (("pit_in", stop.lap, t_in), ("pit_out", stop.lap + 1, t_out)):
            ev = RaceEvent(
                event_id=new_event_id(),
                type=kind,
                season=s,
                round=r,
                race_time_s=t,
                driver_id=stop.driver_id,
                lap=lap_no,
            )
            keyed.append((t, _KIND_ORDER[kind], stop.driver_id, lap_no, ev))
    keyed.sort(key=lambda k: k[:4])
    out = [Outgoing(k[4]) for k in keyed]
    end_t = (keyed[-1][0] if keyed else 0.0) + 1.0
    last_lap = max((lap.lap for lap in inputs.laps), default=None)
    for p in range(n_partitions):
        ev = RaceEvent(
            event_id=new_event_id(),
            type="session_end",
            season=s,
            round=r,
            race_time_s=end_t,
            lap=last_lap,
        )
        out.append(Outgoing(ev, partition=p))
    return out


def sim_partition(key: str, n_partitions: int) -> int:
    """A stable stand-in for Kafka's key partitioner (tests and the parity replay only)."""
    return zlib.crc32(key.encode()) % n_partitions


# ============================================================================== sending
Send = Callable[[Outgoing], None]


def replay(
    events: Sequence[Outgoing],
    send: Send,
    speed: float,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    wall_ms: Callable[[], int] = now_ms,
) -> int:
    """Send ``events`` paced at ``speed`` race-seconds per wall-second; returns the count sent.
    Each event is stamped with ``produced_at_ms`` immediately before it is handed to ``send``."""
    if not (speed > 0 and speed < float("inf")):
        raise ValueError(f"speed must be a positive number, got {speed}")
    if not events:
        return 0
    t0, base = clock(), events[0].event.race_time_s
    for out in events:
        target = t0 + (out.event.race_time_s - base) / speed
        delay = target - clock()
        if delay > 0:
            sleep(delay)
        send(Outgoing(out.event.stamped(wall_ms()), out.partition))
    return len(events)


class KafkaProducerLike(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def producer_config(bootstrap: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "client.id": "f1sa-race-replay",
    }


def topic_partitions(bootstrap: str, topic: str, timeout_s: float = 10.0) -> int:
    """Number of partitions of ``topic`` (raises if it does not exist)."""
    from confluent_kafka.admin import AdminClient

    meta = AdminClient({"bootstrap.servers": bootstrap}).list_topics(topic, timeout=timeout_s)
    t = meta.topics.get(topic)
    if t is None or t.error is not None or not t.partitions:
        raise ProducerError(f"topic {topic!r} not available on {bootstrap}: {t and t.error}")
    return len(t.partitions)


class KafkaSender:
    """Sends ``Outgoing`` events; delivery failures are collected and fail ``close``."""

    def __init__(self, producer: KafkaProducerLike, topic: str) -> None:
        self.producer = producer
        self.topic = topic
        self.errors: list[str] = []
        self.delivered = 0

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.errors.append(str(err))
        else:
            self.delivered += 1

    def __call__(self, out: Outgoing) -> None:
        kwargs: dict[str, Any] = {
            "key": out.key.encode(),
            "value": out.event.to_json().encode(),
            "on_delivery": self._on_delivery,
        }
        if out.partition is not None:
            kwargs["partition"] = out.partition
        while True:
            try:
                self.producer.produce(self.topic, **kwargs)
                break
            except BufferError:  # local queue full: serve callbacks, then retry
                self.producer.poll(0.1)
        self.producer.poll(0)

    def close(self, timeout_s: float = 30.0) -> None:
        left = self.producer.flush(timeout_s)
        if left:
            self.errors.append(f"{left} messages still undelivered after {timeout_s} s")
        if self.errors:
            raise RuntimeError(f"Kafka delivery failed: {self.errors[:5]}")


# ============================================================================== CLI
def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="f1sa-replay", description=__doc__.split("\n\n")[0])
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--round", type=int, required=True)
    p.add_argument("--speed", type=float, default=10.0, help="race-seconds per wall-second")
    p.add_argument("--allow-test-season", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="build the events, send nothing")
    return p.parse_args(list(argv) if argv is not None else None)


def main(
    argv: Iterable[str] | None = None,
    *,
    make_producer: Callable[[dict[str, Any]], KafkaProducerLike] | None = None,
    partitions: Callable[[str, str], int] = topic_partitions,
    spark_factory: Callable[[], Any] | None = None,
) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    args = parse_args(argv)
    try:
        check_season(args.season, args.allow_test_season)
        if not args.speed > 0:
            raise RefusedError(f"--speed must be > 0, got {args.speed}")
    except RefusedError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    settings = get_settings()
    if spark_factory is None:
        from f1sa.etl.spark import build_spark

        def spark_factory() -> Any:
            return build_spark("f1sa-race-replay")

    spark = spark_factory()
    try:
        inputs = load_race_inputs(spark, settings.data_lake_uri, args.season, args.round)
    except ProducerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    n = 1 if args.dry_run else partitions(settings.kafka_bootstrap, settings.kafka_topic_events)
    events = build_events(inputs, n)
    span = events[-1].event.race_time_s - events[0].event.race_time_s
    print(
        f"{args.season} R{args.round}: {len(events)} events, {len(inputs.laps)} laps, "
        f"{len(inputs.stops)} stops, {span:.0f} race-s -> {span / args.speed:.0f} s "
        f"at x{args.speed:g}"
    )
    if args.dry_run:
        return 0
    if make_producer is None:
        from confluent_kafka import Producer

        def make_producer(conf: dict[str, Any]) -> KafkaProducerLike:
            producer: KafkaProducerLike = Producer(conf)  # type: ignore[assignment]
            return producer

    sender = KafkaSender(
        make_producer(producer_config(settings.kafka_bootstrap)), settings.kafka_topic_events
    )
    sent = replay(events, sender, args.speed)
    try:
        sender.close()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"sent {sent} events, {sender.delivered} delivered to {settings.kafka_topic_events}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
