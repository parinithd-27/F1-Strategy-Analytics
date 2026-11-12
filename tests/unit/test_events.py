"""Race event schema: validation, JSON round trip, Spark schema parity and the infra filter."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import fields, replace
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from f1sa.streaming import events as E


def lap(**kw: Any) -> E.RaceEvent:
    base: dict[str, Any] = {
        "event_id": E.new_event_id(),
        "type": "lap",
        "season": 2024,
        "round": 5,
        "race_time_s": 95.123,
        "driver_id": "max_verstappen",
        "lap": 1,
        "lap_time_s": 95.123,
        "position": 1,
        "compound": "MEDIUM",
        "tyre_age": 1,
        "produced_at_ms": 1_730_000_000_000,
        "track_neutral": False,
        "track_red_flag": False,
        "race_laps": 56,
    }
    return E.RaceEvent(**{**base, **kw})


def test_json_round_trip_is_exact() -> None:
    ev = lap()
    assert E.from_json(ev.to_json()) == ev
    assert E.from_json(ev.to_json().encode()) == ev
    assert json.loads(ev.to_json())["type"] == "lap"


@pytest.mark.parametrize("kind", ["pit_in", "pit_out"])
def test_pit_events_carry_no_lap_fields(kind: str) -> None:
    ev = E.RaceEvent(E.new_event_id(), kind, 2024, 5, 200.0, driver_id="a", lap=7)
    assert E.from_json(ev.to_json()) == ev
    with pytest.raises(E.EventError, match="must not carry lap fields"):
        E.RaceEvent(E.new_event_id(), kind, 2024, 5, 200.0, driver_id="a", lap=7, position=3)


def test_session_end_has_no_driver() -> None:
    ev = E.RaceEvent(E.new_event_id(), "session_end", 2024, 5, 5000.0, lap=56)
    assert E.from_json(ev.to_json()) == ev
    with pytest.raises(E.EventError, match="no driver_id"):
        E.RaceEvent(E.new_event_id(), "session_end", 2024, 5, 5000.0, driver_id="a")


@pytest.mark.parametrize(
    "change, match",
    [
        ({"event_id": "not-a-uuid"}, "uuid4"),
        ({"event_id": "12345678-1234-1234-1234-123456789abc"}, "uuid4"),  # version 1 style
        ({"type": "pit"}, "type"),
        ({"season": 1900}, "season"),
        ({"round": 0}, "round"),
        ({"lap": 0}, "lap"),
        ({"lap": None}, "needs a lap"),
        ({"driver_id": ""}, "driver_id"),
        ({"lap_time_s": float("nan")}, "lap_time_s"),
        ({"race_time_s": float("inf")}, "race_time_s"),
        ({"race_time_s": -1.0}, "race_time_s"),
        ({"position": True}, "position"),
        ({"compound": "medium"}, "compound"),
        ({"tyre_age": -1}, "tyre_age"),
        ({"track_neutral": 1}, "track_neutral"),
        ({"race_laps": 0}, "race_laps"),
    ],
)
def test_invalid_values_are_refused(change: dict[str, Any], match: str) -> None:
    with pytest.raises(E.EventError, match=match):
        lap(**change)


@pytest.mark.parametrize(
    "text, match",
    [
        ("{", "not JSON"),
        ("[1]", "JSON object"),
        ('{"type":"lap"}', "missing"),
        ('{"event_id":"x","type":"lap","season":2024,"round":5,"race_time_s":1,"x":1}', "unknown"),
    ],
)
def test_malformed_json_is_refused(text: str, match: str) -> None:
    with pytest.raises(E.EventError, match=match):
        E.from_json(text)


def test_integral_json_numbers_parse_as_floats() -> None:
    d = lap().to_dict()
    d["race_time_s"], d["lap_time_s"] = 95, 95
    ev = E.from_dict(d)
    assert isinstance(ev.race_time_s, float) and isinstance(ev.lap_time_s, float)


def test_stamped_sets_only_produced_at() -> None:
    ev = lap(produced_at_ms=None)
    assert ev.stamped(42) == replace(ev, produced_at_ms=42)


def test_spark_schema_matches_the_dataclass_and_parses_identically(spark: SparkSession) -> None:
    schema = E.spark_schema()
    assert (
        tuple(f.name for f in schema.fields)
        == E.FIELD_NAMES
        == tuple(f.name for f in fields(E.RaceEvent))
    )
    evs = [lap(), E.RaceEvent(E.new_event_id(), "session_end", 2024, 5, 5000.0)]
    df = spark.createDataFrame([(e.to_json(),) for e in evs], "value string")
    rows = df.select(F.from_json("value", schema).alias("e")).select("e.*").collect()
    assert [E.from_dict(r.asDict()) for r in rows] == evs


def test_infra_header_detection() -> None:
    assert E.is_infra_test([("test", b"infra")])
    assert E.is_infra_test([{"key": "test", "value": bytearray(b"infra")}])
    assert not E.is_infra_test(None)
    assert not E.is_infra_test([("test", b"other"), ("x", None)])


def test_module_imports_no_spark() -> None:
    code = (
        "import sys, f1sa.streaming.events; "
        "print(any(m.startswith('pyspark') for m in sys.modules))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


def alert(**kw: Any) -> E.UndercutAlert:
    base: dict[str, Any] = {
        "reason": "opportunity",
        "trigger": "new_candidate",
        "season": 2024,
        "round": 5,
        "attacker_id": "b",
        "defender_id": "a",
        "decision_lap": 9,
        "gap_s": 1.5,
        "undercut_prob": 0.42,
        "threshold": 0.5,
        "inputs": {"gap_s": 1.5, "pit_loss_source": "global_prior", "attacker_position": 4},
    }
    return E.UndercutAlert(**{**base, **kw})


@pytest.mark.parametrize(
    "change, match",
    [
        ({"reason": "spam"}, "reason"),
        ({"trigger": "pit_in"}, "not valid"),
        ({"reason": "attempt", "trigger": "new_candidate"}, "not valid"),
        ({"undercut_prob": 1.5}, "undercut_prob"),
        ({"gap_s": float("nan")}, "gap_s"),
        ({"defender_id": ""}, "defender"),
        ({"decision_lap": 0}, "decision_lap"),
    ],
)
def test_invalid_alerts_are_refused(change: dict[str, Any], match: str) -> None:
    with pytest.raises(E.EventError, match=match):
        alert(**change)


def test_alert_payload_parses_with_the_spark_schema(spark: SparkSession) -> None:
    from f1sa.streaming.redis_sink import alert_payload

    payload = alert_payload(alert(reason="attempt", trigger="pit_in"), None, 10, 12)
    schema = E.alert_spark_schema()
    df = spark.createDataFrame([(json.dumps(payload),)], "value string")
    (row,) = df.select(F.from_json("value", schema).alias("a")).select("a.*").collect()
    got = row.asDict(recursive=True)
    assert (got["reason"], got["trigger"], got["attempt_lap"]) == ("attempt", "pit_in", 10)
    assert (
        got["inputs"]["attacker_position"] == 4.0
        and got["inputs"]["pit_loss_source"] == "global_prior"
    )
    assert got["inputs"]["laps_remaining"] is None
    assert (got["batch_received_at_ms"], got["redis_written_at_ms"]) == (10, 12)
    assert set(payload) == {f.name for f in schema.fields}
