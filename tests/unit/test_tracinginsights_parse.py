"""TracingInsights parsers: key sets, explicit "None" handling with counts, typing, errors."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from f1sa.ingest import tracinginsights_parse as P

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "tracinginsights"
RACE = FIX / "Testland Grand Prix" / "Race"


def _laps_obj() -> dict[str, Any]:
    obj: dict[str, Any] = json.loads((RACE / "session_laptimes.json").read_bytes())
    return obj


def _dump(obj: Any) -> bytes:
    return json.dumps(obj).encode()


def test_fixtures_regenerate() -> None:
    spec = importlib.util.spec_from_file_location("make_fixtures", FIX / "make_fixtures.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    files: dict[Path, bytes] = module.files()
    assert len(files) == 14
    for path, data in files.items():
        assert path.read_bytes() == data, f"{path} differs from make_fixtures.py output"


def test_field_lists_match_the_documented_counts() -> None:
    assert len(P.LAP_FIELDS) == 42
    assert len(P.RCM_FIELDS) == 9
    assert len(P.TELEMETRY_FIELDS) == 18


def test_parse_laps_types_and_exact_none_counts() -> None:
    result = P.parse_laps((RACE / "session_laptimes.json").read_bytes())
    f = result.frame
    assert len(f) == 15
    assert str(f["lap_time_s"].dtype) == "Float64"
    assert str(f["lap"].dtype) == "Int64" and str(f["fresh_tyre"].dtype) == "boolean"
    expected = {
        "lap_time_s": 1,  # CCC lap 5
        "compound": 1,  # BBB lap 2
        "tyre_life": 1,  # BBB lap 2
        "sector1_s": 3,
        "sector1_session_s": 3,
        "speed_fl_kph": 3,
        "mini_sectors2": 15,
        "pit_in_session_s": 14,
        "pit_out_session_s": 14,
        "deleted_reason": 15,
    }
    assert {k: v for k, v in result.none_strings.items() if v} == expected
    assert result.nulls == result.none_strings  # every null arrived as a "None" string here
    bbb2 = f[(f.driver_code == "BBB") & (f.lap == 2)].iloc[0]
    assert pd.isna(bbb2["compound"]) and pd.isna(bbb2["tyre_life"])
    aaa = f[f.driver_code == "AAA"].set_index("lap")
    assert aaa.loc[3, "pit_in_session_s"] == 1265.0 and aaa.loc[4, "pit_out_session_s"] == 1275.0
    assert list(aaa["tyre_life"]) == [1, 2, 3, 3, 4]


@pytest.mark.parametrize("spelling", ["None", "nan", "NaN", "NaT", "", None, float("nan")])
def test_every_null_spelling_becomes_null(spelling: Any) -> None:
    obj = _laps_obj()
    obj["time"][0] = spelling
    obj["compound"][0] = spelling
    result = P.parse_laps(_dump(obj))
    assert pd.isna(result.frame.loc[0, "lap_time_s"]) and pd.isna(result.frame.loc[0, "compound"])
    as_string = isinstance(spelling, str)
    assert result.none_strings["lap_time_s"] == 1 + int(as_string)  # + CCC lap 5 ("None")
    assert result.nulls["lap_time_s"] == 2  # this row plus CCC lap 5


def test_null_string_and_json_null_are_counted_apart() -> None:
    obj = _laps_obj()
    obj["s2"][0] = None
    obj["s2"][1] = "None"
    result = P.parse_laps(_dump(obj))
    assert result.none_strings["sector2_s"] == 1
    assert result.nulls["sector2_s"] == 2


def test_mixed_float_and_string_column_is_typed() -> None:
    obj = _laps_obj()
    obj["s1"][1] = "30.5"  # a numeric string is accepted as a number
    f = P.parse_laps(_dump(obj)).frame
    assert f.loc[1, "sector1_s"] == 30.5


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("lap", "x", "expected a number"),
        ("lap", 1.5, "expected an integer"),
        ("fresh", "yes", "expected a boolean"),
        ("time", True, "expected a number"),
        ("time", float("inf"), "infinite"),
    ],
)
def test_bad_values_raise(key: str, value: Any, message: str) -> None:
    obj = _laps_obj()
    obj[key][0] = value
    with pytest.raises(P.TracingInsightsSchemaError, match=message):
        P.parse_laps(json.dumps(obj, allow_nan=True).encode())


def test_missing_extra_and_unequal_keys_raise() -> None:
    obj = _laps_obj()
    del obj["compound"]
    with pytest.raises(P.TracingInsightsSchemaError, match="missing keys \\['compound'\\]"):
        P.parse_laps(_dump(obj))
    obj = _laps_obj()
    obj["new"] = [1] * 15
    with pytest.raises(P.TracingInsightsSchemaError, match="unexpected keys \\['new'\\]"):
        P.parse_laps(_dump(obj))
    obj = _laps_obj()
    obj["time"] = obj["time"][:-1]
    with pytest.raises(P.TracingInsightsSchemaError, match="different lengths"):
        P.parse_laps(_dump(obj))
    obj = _laps_obj()
    obj["time"] = 5
    with pytest.raises(P.TracingInsightsSchemaError, match="not lists"):
        P.parse_laps(_dump(obj))
    with pytest.raises(P.TracingInsightsSchemaError, match="not valid JSON"):
        P.parse_laps(b"{nope")


def test_empty_session_raises() -> None:
    obj = {k: [] for k in _laps_obj()}
    with pytest.raises(P.TracingInsightsDataError, match="no laps"):
        P.parse_laps(_dump(obj))


def test_duplicate_driver_lap_and_missing_driver_raise() -> None:
    obj = _laps_obj()
    obj["lap"][1] = 1
    with pytest.raises(P.TracingInsightsSchemaError, match="duplicate"):
        P.parse_laps(_dump(obj))
    obj = _laps_obj()
    obj["drv"][0] = "None"
    with pytest.raises(P.TracingInsightsSchemaError, match="without driver_code"):
        P.parse_laps(_dump(obj))


def test_parse_rcm() -> None:
    result = P.parse_rcm((RACE / "rcm.json").read_bytes())
    f = result.frame
    assert list(f["seq"]) == [0, 1, 2, 3, 4]
    assert list(f["lap"]) == [2, 3, 4, 4, 5]
    assert f["sector"].isna().all() and result.none_strings["sector"] == 5
    assert result.none_strings["driver_number"] == 4 and f.loc[4, "driver_number"] == "3"
    assert P.parse_rcm(_dump({k: [] for k in P.RCM_FIELDS})).frame.empty  # no messages is valid


def test_parse_session_info() -> None:
    info = P.parse_session_info((RACE / "session_info.json").read_bytes())
    assert info.meeting_name == "Testland Grand Prix" and info.total_laps == 5
    assert info.session_start_s == 1000.0 and info.t0_date == "2024-03-01T14:00:00.000000"
    ts = info.track_status.frame
    assert list(ts["status"]) == ["1", "4", "1"] and list(ts["session_time_s"]) == [
        0.0,
        1150.0,
        1300.0,
    ]
    bad = json.loads((RACE / "session_info.json").read_bytes())
    del bad["track_status"]["Message"]
    with pytest.raises(P.TracingInsightsSchemaError, match="Time/Status/Message"):
        P.parse_session_info(_dump(bad))
    del bad["track_status"]
    with pytest.raises(P.TracingInsightsSchemaError, match="missing keys"):
        P.parse_session_info(_dump(bad))


def test_parse_telemetry() -> None:
    result = P.parse_telemetry((RACE / "AAA" / "4_tel.json").read_bytes(), "AAA", 4)
    f = result.frame
    assert len(f) == 8 and set(f["driver_code"]) == {"AAA"} and set(f["lap"]) == {4}
    assert str(f["brake"].dtype) == "boolean" and f["brake"].sum() == 1
    assert result.none_strings["driver_ahead"] == 8
    empty = {"tel": {**{k: [] for k in P.TELEMETRY_FIELDS if k != "dataKey"}, "dataKey": "x"}}
    with pytest.raises(P.TracingInsightsDataError, match="no samples"):
        P.parse_telemetry(_dump(empty), "AAA", 1)
    with pytest.raises(P.TracingInsightsSchemaError, match="key 'tel'"):
        P.parse_telemetry(b"{}", "AAA", 1)


def test_telemetry_lap_number() -> None:
    assert P.telemetry_lap_number("12_tel.json") == 12
    for bad in ("x_tel.json", "12.json", "12_tel.json.part"):
        with pytest.raises(P.TracingInsightsSchemaError):
            P.telemetry_lap_number(bad)


def test_git_blob_sha1_matches_git() -> None:
    assert P.git_blob_sha1(b"") == "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
    assert P.git_blob_sha1(b"hello\n") == "ce013625030ba8dba906f756967f9e9ca394464a"


def test_merge_counts() -> None:
    assert P.merge_counts({"a": 1}, {"a": 2, "b": 3}) == {"a": 3, "b": 3}
