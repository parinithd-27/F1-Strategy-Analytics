"""Explicit schemas and primary keys of the feature tables (``{lake}/features/{table}``).

Every table carries ``season`` and ``round`` (the partition columns). A row about lap *L*
describes the state **at the end of lap L**; consumers that need "information available at the
end of lap L-1" (the undercut model, S8) must join lap L-1 rows. Columns are documented in
``docs/data_dictionary.md``.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _s(*fields: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, dtype, nullable=True) for name, dtype in fields])


STR, INT, DBL, BOOL, LONG, TS = (
    StringType(),
    IntegerType(),
    DoubleType(),
    BooleanType(),
    LongType(),
    TimestampType(),
)

STINT_BOUNDARIES = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("stint", INT),
    ("start_lap", INT),
    ("end_lap", INT),
    ("laps", INT),
)
"""Stints reconstructed from laps (RDD job and its DataFrame reference)."""

GAP_COLUMNS: tuple[tuple[str, DataType], ...] = (
    ("position", INT),
    ("ahead_driver_id", STR),
    ("cumulative_time_s", DBL),
    ("gap_leader_s", DBL),
    ("gap_ahead_s", DBL),
    ("gap_ahead_change_1l", DBL),
    ("gap_ahead_change_3l", DBL),
    ("gap_leader_change_1l", DBL),
    ("gap_leader_change_3l", DBL),
)

LAPS_ENRICHED = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("lap", INT),
    ("driver_code", STR),
    ("constructor_id", STR),
    ("constructor_name", STR),
    ("grid", INT),
    ("final_position", INT),
    ("status", STR),
    ("finished", BOOL),
    ("lap_time_s", DBL),
    *GAP_COLUMNS,
    ("is_pit_in_lap", BOOL),
    ("is_pit_out_lap", BOOL),
    ("pit_stops_in_lap", INT),
    ("pit_duration_s", DBL),
    ("is_long_stop", BOOL),
    ("stint", INT),
    ("compound", STR),
    ("tyre_age_laps", INT),
    ("tyre_age_assumed_new", BOOL),
    ("race_laps", INT),
    ("laps_remaining", INT),
    ("lap_start_utc", TS),
    ("lap_end_utc", TS),
    ("lap_time_estimated", BOOL),
    ("is_sc_lap", BOOL),
    ("is_vsc_lap", BOOL),
    ("is_red_flag_lap", BOOL),
    ("is_neutralised_lap", BOOL),
)

GAPS = _s(("season", INT), ("round", INT), ("driver_id", STR), ("lap", INT), *GAP_COLUMNS)

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
    ("compound_source", STR),
    ("matches_clean", BOOL),
)

LAP_TIME_DISTRIBUTION = _s(
    ("season", INT),
    ("round", INT),
    ("circuit_id", STR),
    ("n_laps", LONG),
    ("mean_s", DBL),
    ("std_s", DBL),
    ("min_s", DBL),
    ("max_s", DBL),
    ("bins_1s", MapType(INT, LONG)),
)

PIT_LOSS = _s(
    ("season", INT),
    ("round", INT),
    ("circuit_id", STR),
    ("race_seq", INT),
    ("pit_loss_prior_s", DBL),
    ("pit_loss_source", STR),
    ("pit_loss_missing", BOOL),
    ("circuit_prior_s", DBL),
    ("circuit_prior_stops", INT),
    ("circuit_prior_races", INT),
    ("global_prior_s", DBL),
    ("global_prior_stops", INT),
    ("global_prior_races", INT),
)

TEAM_OUTLAP_HISTORY = _s(
    ("season", INT),
    ("round", INT),
    ("constructor_id", STR),
    ("team_history_key", STR),
    ("circuit_id", STR),
    ("race_seq", INT),
    ("outlap_delta_prior_s", DBL),
    ("outlap_source", STR),
    ("outlap_missing", BOOL),
    ("team_prior_s", DBL),
    ("team_prior_n", INT),
    ("circuit_prior_s", DBL),
    ("circuit_prior_n", INT),
    ("global_prior_s", DBL),
    ("global_prior_n", INT),
)

RACE_SUMMARY = _s(
    ("season", INT),
    ("round", INT),
    ("circuit_id", STR),
    ("race_name", STR),
    ("race_laps", INT),
    ("entrants", INT),
    ("finishers", INT),
    ("classified", INT),
    ("pit_stops", INT),
    ("long_stops", INT),
    ("stops_per_driver", DBL),
    ("neutralised_laps", INT),
    ("green_median_lap_s", DBL),
    ("fastest_lap_s", DBL),
    ("fastest_lap_driver_id", STR),
    ("winner_driver_id", STR),
    ("winner_constructor_id", STR),
    ("winner_race_time_s", DBL),
)

TELEMETRY_LAP_FEATURES = _s(
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
    ("n_samples", INT),
    ("is_synthetic", BOOL),
    ("source", STR),
)

OUTLAP_FEATURES = _s(
    ("season", INT),
    ("round", INT),
    ("driver_id", STR),
    ("stint", INT),
    ("constructor_id", STR),
    ("team_history_key", STR),
    ("circuit_id", STR),
    ("race_seq", INT),
    ("outlap", INT),
    ("outlap_time_s", DBL),
    ("reference_clean_lap_s", DBL),
    ("reference_source", STR),
    ("realised_outlap_delta_s", DBL),
    ("outlap_neutralised", BOOL),
    ("is_long_stop", BOOL),
    ("expected_outlap_delta_s", DBL),
    ("expected_outlap_source", STR),
    ("expected_outlap_missing", BOOL),
    ("driver_team_prior_s", DBL),
    ("driver_team_prior_n", INT),
    ("team_prior_s", DBL),
    ("team_prior_n", INT),
    ("circuit_prior_s", DBL),
    ("circuit_prior_n", INT),
    ("global_prior_s", DBL),
    ("global_prior_n", INT),
    ("stop_duration_s", DBL),
    ("race_median_stop_s", DBL),
    ("race_median_outlap_delta_s", DBL),
    ("stop_delta_s", DBL),
    ("outlap_pace_residual_s", DBL),
    ("expected_outlap_pace_residual_s", DBL),
    ("expected_outlap_pace_source", STR),
    ("expected_outlap_pace_missing", BOOL),
    ("pace_driver_team_n", INT),
    ("pace_team_n", INT),
    ("pace_global_n", INT),
    ("expected_stop_delta_s", DBL),
    ("expected_stop_source", STR),
    ("stop_team_n", INT),
    ("stop_global_n", INT),
)
"""One row per (race, driver, stint). ``realised_*``, ``stop_delta_s`` and
``outlap_pace_residual_s`` are ORACLE-only (uses laps after the stop);
``expected_*`` comes from strictly earlier races and is decision-time safe
(docs/label_definition.md)."""

UNDERCUT_IDENTITY: tuple[tuple[str, DataType], ...] = (
    ("season", INT),
    ("round", INT),
    ("attacker_id", STR),
    ("defender_id", STR),
    ("lap", INT),
    ("decision_lap", INT),
    ("gap_threshold_s", DBL),
    ("response_window_laps", INT),
    ("is_default_params", BOOL),
    ("detector_eligible", BOOL),
    ("outcome_category", STR),
    ("label", INT),
    ("exclusion_reasons", ArrayType(STR)),
)
UNDERCUT_OUTCOME: tuple[tuple[str, DataType], ...] = (
    ("outcome_defender_pit_lap", INT),
    ("outcome_label_lap", INT),
    ("outcome_attacker_position", INT),
    ("outcome_defender_position", INT),
    ("outcome_gap_s", DBL),
    ("outcome_attacker_stop_s", DBL),
    ("outcome_defender_stop_s", DBL),
    ("outcome_attacker_slow_stop", BOOL),
    ("outcome_defender_slow_stop", BOOL),
    ("outcome_attacker_double_stack", BOOL),
    ("outcome_defender_double_stack", BOOL),
)
UNDERCUT_CORE = _s(
    *UNDERCUT_IDENTITY,
    ("gap_s", DBL),
    ("rejoin_traffic_cars", INT),
    ("rejoin_nearest_ahead_s", DBL),
    *UNDERCUT_OUTCOME,
)
"""Internal: the rows ``features.undercut.evaluate_race`` emits (before the Spark joins)."""

UNDERCUT_ATTEMPTS = _s(
    *UNDERCUT_IDENTITY,
    # decision-time features (features.undercut.DECISION_FEATURES): end of lap L-1 only
    ("gap_s", DBL),
    ("gap_ahead_change_1l", DBL),
    ("gap_ahead_change_3l", DBL),
    ("attacker_position", INT),
    ("attacker_tyre_age_laps", INT),
    ("defender_tyre_age_laps", INT),
    ("tyre_age_delta_laps", INT),
    ("attacker_compound", STR),
    ("defender_compound", STR),
    ("compound_is_synthetic", BOOL),
    ("attacker_pace_last3_s", DBL),
    ("defender_pace_last3_s", DBL),
    ("pace_delta_last3_s", DBL),
    ("attacker_deg_slope_s_per_lap", DBL),
    ("defender_deg_slope_s_per_lap", DBL),
    ("deg_slope_delta_s_per_lap", DBL),
    ("laps_remaining", INT),
    ("pit_loss_prior_s", DBL),
    ("pit_loss_source", STR),
    ("rejoin_traffic_cars", INT),
    ("rejoin_nearest_ahead_s", DBL),
    ("attacker_expected_outlap_pace_residual_s", DBL),
    ("attacker_expected_outlap_pace_source", STR),
    ("defender_expected_outlap_pace_residual_s", DBL),
    ("defender_expected_outlap_pace_source", STR),
    ("attacker_expected_stop_delta_s", DBL),
    ("defender_expected_stop_delta_s", DBL),
    # legacy (collinear with pit_loss), ORACLE and outcome columns: never model inputs
    ("attacker_expected_outlap_delta_s_legacy", DBL),
    ("oracle_attacker_realised_outlap_delta_s", DBL),
    ("oracle_attacker_outlap_pace_residual_s", DBL),
    ("oracle_attacker_stop_delta_s", DBL),
    *UNDERCUT_OUTCOME,
)
"""One row per (race, attacker, attempt lap L, G, k). See docs/label_definition.md."""

FEATURE_SCHEMAS: dict[str, StructType] = {
    "laps_enriched": LAPS_ENRICHED,
    "gaps": GAPS,
    "stints": STINTS,
    "lap_time_distribution": LAP_TIME_DISTRIBUTION,
    "pit_loss": PIT_LOSS,
    "team_outlap_history": TEAM_OUTLAP_HISTORY,
    "race_summary": RACE_SUMMARY,
    "telemetry_lap_features": TELEMETRY_LAP_FEATURES,
    "outlap_features": OUTLAP_FEATURES,
    "undercut_attempts": UNDERCUT_ATTEMPTS,
}
FEATURE_KEYS: dict[str, tuple[str, ...]] = {
    "laps_enriched": ("season", "round", "driver_id", "lap"),
    "gaps": ("season", "round", "driver_id", "lap"),
    "stints": ("season", "round", "driver_id", "stint"),
    "lap_time_distribution": ("season", "round", "circuit_id"),
    "pit_loss": ("season", "round"),
    "team_outlap_history": ("season", "round", "constructor_id"),
    "race_summary": ("season", "round"),
    "telemetry_lap_features": ("season", "round", "driver_id", "lap"),
    "outlap_features": ("season", "round", "driver_id", "stint"),
    "undercut_attempts": (
        "season",
        "round",
        "attacker_id",
        "lap",
        "gap_threshold_s",
        "response_window_laps",
    ),
}
FEATURE_TABLES = tuple(FEATURE_SCHEMAS)
