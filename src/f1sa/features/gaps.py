"""Gap features (#12: gap change). A thin typed wrapper over ``etl.df_jobs``."""

from __future__ import annotations

from pyspark.sql import DataFrame

from f1sa.etl.clean import conform
from f1sa.etl.df_jobs import timing_gaps
from f1sa.features import schemas as FS


def gap_features(laps: DataFrame) -> DataFrame:
    """Timing-line gaps and 1- and 3-lap gap changes per (race, driver, lap), from clean laps."""
    base = laps.drop("cumulative_time_s", "gap_to_leader_s", "gap_ahead_s")
    return conform(timing_gaps(base), FS.GAPS, "gaps")


def gaps_from_enriched(laps_enriched: DataFrame) -> DataFrame:
    """The gap columns of an already enriched laps table (no recomputation)."""
    return conform(laps_enriched, FS.GAPS, "gaps")
