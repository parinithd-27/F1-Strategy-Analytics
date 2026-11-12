"""Parsers for the TracingInsights season repositories (FastF1-derived JSON, no login).

Files are column-oriented JSON: every field is a list of equal length. Missing values are
written as the **string** ``"None"`` (sometimes ``"nan"``/``"NaT"``/``""``), so numeric columns mix
float and str. Every parser here:

* checks the exact key set (``LAP_FIELDS`` / ``RCM_FIELDS`` / ``TELEMETRY_FIELDS``) and equal list
  lengths, raising :class:`TracingInsightsSchemaError` otherwise;
* converts the null spellings to real nulls explicitly and **counts** them per field
  (``ParseResult.none_strings``), so the clean zone can report them;
* raises :class:`TracingInsightsDataError` on an empty session instead of returning nothing.

Field meanings follow FastF1 (see ``docs/data_source_audit.md`` section 3). This module is pure
Python + pandas so both the host loader and the tests can use it without Spark.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

NULL_STRINGS = frozenset({"None", "nan", "NaN", "NaT", ""})
"""Spellings of a missing value in TracingInsights JSON."""

# (source key, output column, kind) -- kind: int | float | str | bool
LAP_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("drv", "driver_code", "str"),
    ("dNum", "driver_number", "str"),
    ("team", "team", "str"),
    ("lap", "lap", "int"),
    ("time", "lap_time_s", "float"),
    ("s1", "sector1_s", "float"),
    ("s2", "sector2_s", "float"),
    ("s3", "sector3_s", "float"),
    ("ms1", "mini_sectors1", "str"),
    ("ms2", "mini_sectors2", "str"),
    ("ms3", "mini_sectors3", "str"),
    ("compound", "compound", "str"),
    ("stint", "stint", "int"),
    ("life", "tyre_life", "int"),
    ("fresh", "fresh_tyre", "bool"),
    ("pos", "position", "int"),
    ("status", "track_status", "str"),
    ("pb", "is_personal_best", "bool"),
    ("lST", "lap_start_session_s", "float"),
    ("sesT", "lap_end_session_s", "float"),
    ("lSD", "lap_start_date", "str"),
    ("pin", "pit_in_session_s", "float"),
    ("pout", "pit_out_session_s", "float"),
    ("s1T", "sector1_session_s", "float"),
    ("s2T", "sector2_session_s", "float"),
    ("s3T", "sector3_session_s", "float"),
    ("vi1", "speed_i1_kph", "float"),
    ("vi2", "speed_i2_kph", "float"),
    ("vfl", "speed_fl_kph", "float"),
    ("vst", "speed_st_kph", "float"),
    ("del", "deleted", "bool"),
    ("delR", "deleted_reason", "str"),
    ("ff1G", "fastf1_generated", "bool"),
    ("iacc", "is_accurate", "bool"),
    ("wT", "weather_session_s", "float"),
    ("wAT", "air_temp_c", "float"),
    ("wH", "humidity_pct", "float"),
    ("wP", "pressure_mbar", "float"),
    ("wR", "rainfall", "bool"),
    ("wTT", "track_temp_c", "float"),
    ("wWD", "wind_direction_deg", "int"),
    ("wWS", "wind_speed_ms", "float"),
)
LAP_FIELDS = frozenset(src for src, _, _ in LAP_COLUMNS)

RCM_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("time", "time_utc", "str"),
    ("cat", "category", "str"),
    ("msg", "message", "str"),
    ("status", "status", "str"),
    ("flag", "flag", "str"),
    ("scope", "scope", "str"),
    ("sector", "sector", "int"),
    ("dNum", "driver_number", "str"),
    ("lap", "lap", "int"),
)
RCM_FIELDS = frozenset(src for src, _, _ in RCM_COLUMNS)

TELEMETRY_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("time", "time_in_lap_s", "float"),
    ("speed", "speed_kph", "float"),
    ("rpm", "rpm", "float"),
    ("gear", "n_gear", "int"),
    ("throttle", "throttle_pct", "float"),
    ("brake", "brake", "bool"),
    ("drs", "drs", "int"),
    ("distance", "distance_m", "float"),
    ("rel_distance", "rel_distance", "float"),
    ("x", "x", "float"),
    ("y", "y", "float"),
    ("z", "z", "float"),
    ("acc_x", "acc_x", "float"),
    ("acc_y", "acc_y", "float"),
    ("acc_z", "acc_z", "float"),
    ("DriverAhead", "driver_ahead", "str"),
    ("DistanceToDriverAhead", "distance_to_driver_ahead_m", "float"),
)
TELEMETRY_FIELDS = frozenset({*(src for src, _, _ in TELEMETRY_COLUMNS), "dataKey"})
SESSION_INFO_FIELDS = frozenset(
    {
        "t0_date",
        "session_start_time",
        "total_laps",
        "track_status",
        "session_status",
        "session_info",
    }
)
TRACK_STATUS_FIELDS = frozenset({"Time", "Status", "Message"})

_TEL_NAME = re.compile(r"^(\d+)_tel\.json$")
_PANDAS_DTYPES = {"int": "Int64", "float": "Float64", "str": "string", "bool": "boolean"}


class TracingInsightsSchemaError(ValueError):
    """A TracingInsights file does not have the documented structure."""


class TracingInsightsDataError(ValueError):
    """A TracingInsights file is structurally valid but holds no usable data."""


@dataclass
class ParseResult:
    """A typed frame plus null bookkeeping per output column."""

    frame: pd.DataFrame
    none_strings: dict[str, int] = field(default_factory=dict)
    """Values that arrived as a null *string* ("None", "nan", ...) and became null."""
    nulls: dict[str, int] = field(default_factory=dict)
    """All nulls after parsing (null strings + JSON nulls + float NaN)."""


def git_blob_sha1(data: bytes) -> str:
    """Git's object id for a blob with these bytes (``git hash-object``)."""
    return hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()


def load_json(data: bytes, what: str) -> Any:
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TracingInsightsSchemaError(f"{what}: not valid JSON ({exc})") from exc


def _is_null(value: Any) -> tuple[bool, bool]:
    """(is null, came as a null string)."""
    if value is None:
        return True, False
    if isinstance(value, str) and value.strip() in NULL_STRINGS:
        return True, True
    if isinstance(value, float) and math.isnan(value):
        return True, False
    return False, False


def _convert(value: Any, kind: str, where: str) -> Any:
    if kind == "str":
        return value if isinstance(value, str) else str(value)
    if kind == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value in ("True", "False"):
            return value == "True"
        if isinstance(value, int | float) and value in (0, 1):
            return bool(value)
        raise TracingInsightsSchemaError(f"{where}: expected a boolean, got {value!r}")
    if isinstance(value, bool):
        raise TracingInsightsSchemaError(f"{where}: expected a number, got {value!r}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise TracingInsightsSchemaError(f"{where}: expected a number, got {value!r}") from exc
    if math.isinf(number):
        raise TracingInsightsSchemaError(f"{where}: infinite value")
    if kind == "int":
        if not number.is_integer():
            raise TracingInsightsSchemaError(f"{where}: expected an integer, got {value!r}")
        return int(number)
    return number


def _columns(
    obj: Mapping[str, Any],
    spec: Sequence[tuple[str, str, str]],
    required: frozenset[str],
    what: str,
) -> ParseResult:
    if not isinstance(obj, Mapping):
        raise TracingInsightsSchemaError(f"{what}: expected a JSON object of columns")
    missing = sorted(required - set(obj))
    extra = sorted(set(obj) - required)
    if missing or extra:
        raise TracingInsightsSchemaError(f"{what}: missing keys {missing}, unexpected keys {extra}")
    lengths = {k: len(obj[k]) for k, _, _ in spec if isinstance(obj[k], list)}
    not_lists = sorted(k for k, _, _ in spec if not isinstance(obj[k], list))
    if not_lists:
        raise TracingInsightsSchemaError(f"{what}: keys {not_lists} are not lists")
    if len(set(lengths.values())) > 1:
        raise TracingInsightsSchemaError(f"{what}: columns have different lengths {lengths}")
    n = next(iter(lengths.values()), 0)
    data: dict[str, list[Any]] = {}
    none_strings: dict[str, int] = {}
    nulls: dict[str, int] = {}
    for src, out, kind in spec:
        values: list[Any] = []
        n_str = n_null = 0
        for i, raw in enumerate(obj[src]):
            null, as_string = _is_null(raw)
            if null:
                values.append(None)
                n_null += 1
                n_str += as_string
            else:
                values.append(_convert(raw, kind, f"{what} {src}[{i}]"))
        data[out] = values
        none_strings[out] = n_str
        nulls[out] = n_null
    frame = pd.DataFrame(
        {out: pd.array(data[out], dtype=_PANDAS_DTYPES[kind]) for _, out, kind in spec}
    )
    assert len(frame) == n
    return ParseResult(frame, none_strings, nulls)


def parse_laps(data: bytes, what: str = "session_laptimes.json") -> ParseResult:
    """All drivers' laps of one session (FastF1 ``Laps``). Raises on an empty session."""
    result = _columns(load_json(data, what), LAP_COLUMNS, LAP_FIELDS, what)
    if result.frame.empty:
        raise TracingInsightsDataError(f"{what}: session has no laps")
    for col in ("driver_code", "lap"):
        if result.nulls[col]:
            raise TracingInsightsSchemaError(f"{what}: {result.nulls[col]} rows without {col}")
    dup = result.frame.duplicated(["driver_code", "lap"])
    if dup.any():
        raise TracingInsightsSchemaError(f"{what}: {int(dup.sum())} duplicate (driver, lap) rows")
    return result


def parse_rcm(data: bytes, what: str = "rcm.json") -> ParseResult:
    """Race-control messages, with ``seq`` (file order) added. An empty list is valid."""
    result = _columns(load_json(data, what), RCM_COLUMNS, RCM_FIELDS, what)
    result.frame.insert(0, "seq", pd.array(range(len(result.frame)), dtype="Int64"))
    return result


@dataclass
class SessionInfo:
    t0_date: str | None
    session_start_s: float | None
    total_laps: int | None
    meeting_name: str | None
    track_status: ParseResult


def parse_session_info(data: bytes, what: str = "session_info.json") -> SessionInfo:
    """Session metadata and the track-status timeline (session seconds, status code, message)."""
    obj = load_json(data, what)
    if not isinstance(obj, Mapping):
        raise TracingInsightsSchemaError(f"{what}: expected a JSON object")
    missing = sorted(SESSION_INFO_FIELDS - set(obj))
    if missing:
        raise TracingInsightsSchemaError(f"{what}: missing keys {missing}")
    ts = obj["track_status"]
    if not isinstance(ts, Mapping) or set(ts) != TRACK_STATUS_FIELDS:
        raise TracingInsightsSchemaError(f"{what}: track_status must have keys Time/Status/Message")
    spec = (
        ("Time", "session_time_s", "float"),
        ("Status", "status", "str"),
        ("Message", "message", "str"),
    )
    track = _columns(ts, spec, TRACK_STATUS_FIELDS, f"{what} track_status")
    track.frame.insert(0, "seq", pd.array(range(len(track.frame)), dtype="Int64"))

    def scalar(key: str, kind: str) -> Any:
        null, _ = _is_null(obj[key])
        return None if null else _convert(obj[key], kind, f"{what} {key}")

    meeting = (
        obj["session_info"].get("Meeting", {}) if isinstance(obj["session_info"], Mapping) else {}
    )
    name = meeting.get("Name") if isinstance(meeting, Mapping) else None
    return SessionInfo(
        t0_date=scalar("t0_date", "str"),
        session_start_s=scalar("session_start_time", "float"),
        total_laps=scalar("total_laps", "int"),
        meeting_name=name if isinstance(name, str) else None,
        track_status=track,
    )


def telemetry_lap_number(file_name: str) -> int:
    """``12_tel.json`` -> 12."""
    match = _TEL_NAME.match(file_name)
    if match is None:
        raise TracingInsightsSchemaError(f"not a telemetry file name: {file_name!r}")
    return int(match.group(1))


def parse_telemetry(data: bytes, driver_code: str, lap: int, what: str = "") -> ParseResult:
    """One driver-lap of merged car + position data. Adds ``driver_code`` and ``lap``.

    ``time_in_lap_s`` counts from the lap start; the session time is ``lap_start_session_s +
    time_in_lap_s`` (see ``features.telemetry_agg_ti``). A file without samples is an error.
    """
    what = what or f"{driver_code}/{lap}_tel.json"
    obj = load_json(data, what)
    if not isinstance(obj, Mapping) or "tel" not in obj:
        raise TracingInsightsSchemaError(f"{what}: expected an object with key 'tel'")
    result = _columns(obj["tel"], TELEMETRY_COLUMNS, TELEMETRY_FIELDS, what)
    if result.frame.empty:
        raise TracingInsightsDataError(f"{what}: no samples")
    if result.nulls["time_in_lap_s"]:
        raise TracingInsightsSchemaError(f"{what}: samples without a time")
    result.frame.insert(0, "lap", pd.array([lap] * len(result.frame), dtype="Int64"))
    result.frame.insert(
        0, "driver_code", pd.array([driver_code] * len(result.frame), dtype="string")
    )
    return result


def merge_counts(target: dict[str, int], counts: Mapping[str, int]) -> dict[str, int]:
    """Add per-field counts into ``target`` (used to total null counts over many files)."""
    for key, value in counts.items():
        target[key] = target.get(key, 0) + value
    return target


Parser = Callable[[bytes, str], ParseResult]
