"""Schemas of the TracingInsights raw inputs and the additive clean tables (Step 4C).

Kept apart from ``lake/schemas.py`` on purpose: ``etl.clean.CLEAN_TABLES`` (and everything built
on it in v1) is derived from that module, so new tables here can never change a v1 output.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

INT, LONG, DBL, STR, BOOL = IntegerType(), LongType(), DoubleType(), StringType(), BooleanType()


def _s(*fields: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


# ------------------------------------------------------------------ raw inputs (subset read)
RAW_LAPS = _s(
    ("season", INT),
    ("round", INT),
    ("driver_code", STR),
    ("driver_number", STR),
    ("lap", LONG),
    ("compound", STR),
    ("stint", LONG),
    ("tyre_life", LONG),
    ("fresh_tyre", BOOL),
    ("position", LONG),
    ("lap_start_session_s", DBL),
    ("lap_end_session_s", DBL),
)
RAW_RCM = _s(
    ("season", INT),
    ("round", INT),
    ("seq", LONG),
    ("time_utc", STR),
    ("category", STR),
    ("message", STR),
    ("status", STR),
    ("flag", STR),
    ("scope", STR),
    ("sector", LONG),
    ("driver_number", STR),
    ("lap", LONG),
)
RAW_TRACK_STATUS = _s(
    ("season", INT),
    ("round", INT),
    ("seq", LONG),
    ("session_time_s", DBL),
    ("status", STR),
    ("message", STR),
)
RAW_TELEMETRY_LAP_AGG = _s(
    ("season", INT),
    ("round", INT),
    ("driver_code", STR),
    ("lap", LONG),
    ("mean_speed_kph", DBL),
    ("max_speed_kph", DBL),
    ("pct_full_throttle", DBL),
    ("brake_fraction", DBL),
    ("gear_changes", INT),
    ("mean_rpm", DBL),
    ("n_samples", INT),
    ("telemetry_coverage", STR),
)

# ------------------------------------------------------------------ clean tables
STINTS_TRACINGINSIGHTS = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("driver_code", STR),
    ("stint", INT),
    ("start_lap", INT),
    ("end_lap", INT),
    ("laps", INT),
    ("compound", STR),
    ("compound_missing", BOOL),
    ("n_laps_compound_null", INT),
    ("compound_conflict", BOOL),
    ("tyre_age_at_start", INT),
    ("fresh_tyre", BOOL),
    ("is_synthetic", BOOL),
    ("source", STR),
)
RACE_CONTROL = _s(
    ("season", INT),
    ("round", INT),
    ("seq", INT),
    ("time_utc", TimestampType()),
    ("lap", INT),
    ("category", STR),
    ("message", STR),
    ("status", STR),
    ("flag", STR),
    ("scope", STR),
    ("sector", INT),
    ("driver_number", INT),
    ("driver_id", STR),
    ("is_sc_deployed", BOOL),
    ("is_vsc_deployed", BOOL),
    ("is_red_flag", BOOL),
    ("is_penalty", BOOL),
    ("source", STR),
)
RACE_CONTROL_TRACK_STATUS = _s(
    ("season", INT),
    ("round", INT),
    ("seq", INT),
    ("start_session_s", DBL),
    ("end_session_s", DBL),
    ("status", STR),
    ("message", STR),
    ("is_green", BOOL),
    ("is_yellow", BOOL),
    ("is_sc", BOOL),
    ("is_red_flag", BOOL),
    ("is_vsc", BOOL),
    ("is_vsc_ending", BOOL),
    ("lap_from", INT),
    ("lap_to", INT),
    ("source", STR),
)
TELEMETRY_LAP_REAL = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("driver_code", STR),
    ("lap", INT),
    ("mean_speed_kph", DBL),
    ("max_speed_kph", DBL),
    ("pct_full_throttle", DBL),
    ("brake_fraction", DBL),
    ("gear_changes", INT),
    ("mean_rpm", DBL),
    ("n_samples", INT),
    ("telemetry_coverage", STR),
    ("is_synthetic", BOOL),
    ("source", STR),
)
COMPOUND_COVERAGE = _s(
    ("season", INT),
    ("round", INT),
    ("n_lap_rows", INT),
    ("n_compound_known", INT),
    ("compound_share", DBL),
    ("n_compound_unknown_label", INT),
    ("n_compound_null", INT),
    ("n_tyre_life_known", INT),
    ("tyre_life_share", DBL),
    ("n_stint_null", INT),
    ("below_95pct", BOOL),
    ("n_drivers", INT),
    ("n_drivers_unmapped", INT),
    ("n_rcm_messages", INT),
    ("n_rcm_with_lap", INT),
    ("n_telemetry_laps", INT),
    ("n_telemetry_full", INT),
    ("n_telemetry_partial", INT),
    ("n_telemetry_none", INT),
)

CLEAN_SCHEMAS_TI: dict[str, StructType] = {
    "stints_tracinginsights": STINTS_TRACINGINSIGHTS,
    "race_control": RACE_CONTROL,
    "race_control_track_status": RACE_CONTROL_TRACK_STATUS,
    "telemetry_lap_real": TELEMETRY_LAP_REAL,
    "compound_coverage": COMPOUND_COVERAGE,
}
PRIMARY_KEYS_TI: dict[str, tuple[str, ...]] = {
    "stints_tracinginsights": ("season", "round", "driver_id", "stint"),
    "race_control": ("season", "round", "seq"),
    "race_control_track_status": ("season", "round", "seq"),
    "telemetry_lap_real": ("season", "round", "driver_id", "lap"),
    "compound_coverage": ("season", "round"),
}
DRY_COMPOUNDS = ("SOFT", "MEDIUM", "HARD")
KNOWN_COMPOUNDS = (*DRY_COMPOUNDS, "INTERMEDIATE", "WET")
COVERAGE_THRESHOLD = 0.95
SOURCE_VALUE = "tracinginsights"
