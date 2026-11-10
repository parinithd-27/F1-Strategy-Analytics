"""Explicit Spark schemas for the raw inputs and the clean zone.

Raw Jolpica JSON is read with these schemas (no inference). Raw Parquet (FastF1, synthetic)
is read as written and then *conformed* (cast and ordered) to the raw schema here, because
pandas may store integer/float widths differently; a missing column is an error.
FastF1 timestamps/durations are pandas nanosecond types, which Spark reads as ``long``
nanoseconds (``spark.sql.legacy.parquet.nanosAsLong=true``); the clean job converts them.

Clean tables all carry ``season`` and ``round`` (the partition columns). Every column is
documented in ``docs/data_dictionary.md``.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _s(*fields: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, dtype, nullable=True) for name, dtype in fields])


STR, INT, DBL, BOOL, LONG = StringType(), IntegerType(), DoubleType(), BooleanType(), LongType()

# ============================================================================ raw: Jolpica
_LOCATION = _s(("lat", STR), ("long", STR), ("locality", STR), ("country", STR))
_CIRCUIT = _s(("circuitId", STR), ("circuitName", STR), ("url", STR), ("Location", _LOCATION))
_SESSION = _s(("date", STR), ("time", STR))
_DRIVER = _s(
    ("driverId", STR),
    ("permanentNumber", STR),
    ("code", STR),
    ("url", STR),
    ("givenName", STR),
    ("familyName", STR),
    ("dateOfBirth", STR),
    ("nationality", STR),
)
_CONSTRUCTOR = _s(("constructorId", STR), ("url", STR), ("name", STR), ("nationality", STR))
_TIME = _s(("millis", STR), ("time", STR))
_FASTEST = _s(("rank", STR), ("lap", STR), ("Time", _s(("time", STR))))
_RESULT = _s(
    ("number", STR),
    ("position", STR),
    ("positionText", STR),
    ("points", STR),
    ("Driver", _DRIVER),
    ("Constructor", _CONSTRUCTOR),
    ("grid", STR),
    ("laps", STR),
    ("status", STR),
    ("Time", _TIME),
    ("FastestLap", _FASTEST),
)
_QUALI = _s(
    ("number", STR),
    ("position", STR),
    ("Driver", _DRIVER),
    ("Constructor", _CONSTRUCTOR),
    ("Q1", STR),
    ("Q2", STR),
    ("Q3", STR),
)
_PITSTOP = _s(("driverId", STR), ("lap", STR), ("stop", STR), ("time", STR), ("duration", STR))
_TIMING = _s(("driverId", STR), ("position", STR), ("time", STR))
_LAP = _s(("number", STR), ("Timings", ArrayType(_TIMING)))
_RACE = _s(
    ("season", STR),
    ("round", STR),
    ("url", STR),
    ("raceName", STR),
    ("Circuit", _CIRCUIT),
    ("date", STR),
    ("time", STR),
    ("Sprint", _SESSION),
    ("Results", ArrayType(_RESULT)),
    ("QualifyingResults", ArrayType(_QUALI)),
    ("PitStops", ArrayType(_PITSTOP)),
    ("Laps", ArrayType(_LAP)),
)


def _page(table: StructField) -> StructType:
    return _s(("MRData", StructType([StructField("total", STR), table])))


RAW_RACE_PAGE = _page(StructField("RaceTable", _s(("season", STR), ("Races", ArrayType(_RACE)))))
"""One Ergast page with a RaceTable (schedule, results, qualifying, pitstops, laps)."""
RAW_DRIVER_PAGE = _page(
    StructField("DriverTable", _s(("season", STR), ("Drivers", ArrayType(_DRIVER))))
)
RAW_CONSTRUCTOR_PAGE = _page(
    StructField("ConstructorTable", _s(("season", STR), ("Constructors", ArrayType(_CONSTRUCTOR))))
)
RAW_JSON_SCHEMAS: dict[str, StructType] = {
    "schedule": RAW_RACE_PAGE,
    "results": RAW_RACE_PAGE,
    "qualifying": RAW_RACE_PAGE,
    "pitstops": RAW_RACE_PAGE,
    "laps": RAW_RACE_PAGE,
    "drivers": RAW_DRIVER_PAGE,
    "constructors": RAW_CONSTRUCTOR_PAGE,
}

# ======================================================================= raw: FastF1 / synthetic
# Times are nanoseconds (pandas Timedelta / Timestamp read with nanosAsLong).
RAW_FASTF1_LAPS = _s(
    ("season", INT),
    ("round", INT),
    ("Driver", STR),
    ("DriverNumber", STR),
    ("LapNumber", DBL),
    ("LapTime", LONG),
    ("Compound", STR),
    ("TyreLife", DBL),
    ("Stint", DBL),
    ("PitInTime", LONG),
    ("PitOutTime", LONG),
    ("TrackStatus", STR),
    ("Position", DBL),
    ("Time", LONG),
    ("LapStartTime", LONG),
)
RAW_FASTF1_CAR = _s(
    ("season", INT),
    ("round", INT),
    ("Driver", STR),
    ("DriverNumber", STR),
    ("Date", LONG),
    ("SessionTime", LONG),
    ("Time", LONG),
    ("Speed", DBL),
    ("Throttle", DBL),
    ("Brake", BOOL),
    ("nGear", INT),
    ("RPM", DBL),
)
RAW_SYNTHETIC_STINTS = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("stint", INT),
    ("start_lap", INT),
    ("end_lap", INT),
    ("laps", INT),
    ("compound", STR),
    ("tyre_age_at_start", INT),
    ("is_synthetic", BOOL),
)
RAW_SYNTHETIC_TELEMETRY = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("lap", INT),
    ("mean_speed_kph", DBL),
    ("max_speed_kph", DBL),
    ("pct_full_throttle", DBL),
    ("brake_fraction", DBL),
    ("gear_changes", INT),
    ("mean_rpm", DBL),
    ("is_synthetic", BOOL),
)

# ============================================================================ clean zone
RACES = _s(
    ("season", INT),
    ("round", INT),
    ("race_name", STR),
    ("circuit_id", STR),
    ("circuit_name", STR),
    ("locality", STR),
    ("country", STR),
    ("lat", DBL),
    ("long", DBL),
    ("race_date", DateType()),
    ("race_start_utc", TimestampType()),
    ("has_sprint", BOOL),
    ("url", STR),
)
DRIVERS = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("code", STR),
    ("permanent_number", INT),
    ("car_number", INT),
    ("given_name", STR),
    ("family_name", STR),
    ("date_of_birth", DateType()),
    ("nationality", STR),
    ("constructor_id", STR),
)
CONSTRUCTORS = _s(
    ("season", INT),
    ("round", INT),
    ("constructor_id", STR),
    ("name", STR),
    ("nationality", STR),
)
RESULTS = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("constructor_id", STR),
    ("car_number", INT),
    ("grid", INT),
    ("position", INT),
    ("position_text", STR),
    ("points", DBL),
    ("laps", INT),
    ("status", STR),
    ("finished", BOOL),
    ("classified", BOOL),
    ("race_time_s", DBL),
    ("fastest_lap", INT),
    ("fastest_lap_rank", INT),
    ("fastest_lap_time_s", DBL),
)
QUALIFYING = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("constructor_id", STR),
    ("car_number", INT),
    ("position", INT),
    ("q1_s", DBL),
    ("q2_s", DBL),
    ("q3_s", DBL),
    ("best_s", DBL),
)
LAPS = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("lap", INT),
    ("position", INT),
    ("lap_time_s", DBL),
    ("cumulative_time_s", DBL),
    ("gap_to_leader_s", DBL),
    ("gap_ahead_s", DBL),
    ("is_pit_in_lap", BOOL),
    ("is_pit_out_lap", BOOL),
    ("is_sc_lap", BOOL),
    ("is_vsc_lap", BOOL),
    ("is_red_flag_lap", BOOL),
    ("is_neutralised_lap", BOOL),
    ("track_status_source", STR),
)
PITSTOPS = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("stop", INT),
    ("lap", INT),
    ("time_of_day", STR),
    ("duration_s", DBL),
    ("is_long_stop", BOOL),
)
STINTS = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("stint", INT),
    ("start_lap", INT),
    ("end_lap", INT),
    ("laps", INT),
    ("compound", STR),
    ("tyre_age_at_start", INT),
    ("is_synthetic", BOOL),
    ("source", STR),
)
TELEMETRY_LAP = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("lap", INT),
    ("mean_speed_kph", DBL),
    ("max_speed_kph", DBL),
    ("pct_full_throttle", DBL),
    ("brake_fraction", DBL),
    ("gear_changes", INT),
    ("mean_rpm", DBL),
    ("is_synthetic", BOOL),
    ("source", STR),
)
TELEMETRY_RAW = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("driver_code", STR),
    ("lap", INT),
    ("session_time_s", DBL),
    ("date", TimestampType()),
    ("speed_kph", DBL),
    ("throttle_pct", DBL),
    ("brake", BOOL),
    ("n_gear", INT),
    ("rpm", DBL),
)

CLEAN_SCHEMAS: dict[str, StructType] = {
    "races": RACES,
    "drivers": DRIVERS,
    "constructors": CONSTRUCTORS,
    "results": RESULTS,
    "qualifying": QUALIFYING,
    "laps": LAPS,
    "pitstops": PITSTOPS,
    "stints": STINTS,
    "telemetry_lap": TELEMETRY_LAP,
    "telemetry_raw": TELEMETRY_RAW,
}
PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "races": ("season", "round"),
    "drivers": ("season", "round", "driver_id"),
    "constructors": ("season", "round", "constructor_id"),
    "results": ("season", "round", "driver_id"),
    "qualifying": ("season", "round", "driver_id"),
    "laps": ("season", "round", "driver_id", "lap"),
    "pitstops": ("season", "round", "driver_id", "stop"),
    "stints": ("season", "round", "driver_id", "stint"),
    "telemetry_lap": ("season", "round", "driver_id", "lap"),
    "telemetry_raw": ("season", "round", "driver_id", "session_time_s"),
}
PARTITION_COLUMNS = ("season", "round")
