"""DataFrame API jobs (commitments #8 and #10): joins, aggregations and window functions.

``enrich_laps`` joins the clean laps with results, pit stops, drivers, constructors, stints and
races. It adds the following:

* timing-line gaps, recomputed here from ``lap_time_s`` with windows:
  * ``gap_leader_s`` is cumulative time minus the cumulative time of the car in P1 **at the
    same lap number**. A lapped car is compared with the leader's time when the leader
    finished that same lap, not when the lapped car crossed the line. Its gap is therefore
    the true time behind and can be longer than one lap.
  * ``gap_ahead_s`` is the gap to the car one position ahead at the same lap number.
* ``gap_*_change_{1,3}l``: the change compared with k laps earlier. ``gap_ahead_change`` is
  null when the car ahead is not the same driver.
* tyre age, the stint and the compound, from the clean stints table;
* laps remaining, counted against the winner's race distance;
* estimated lap timestamps. Jolpica has no per-lap timestamps, so
  ``lap_end_utc = race_start_utc + START_OFFSET_S + cumulative_time_s``. This approximation and
  its limits are described in ``docs/architecture.md``.

All values describe the state at the **end** of lap L.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window, WindowSpec
from pyspark.sql import functions as F

from f1sa.etl.clean import conform
from f1sa.features import schemas as FS

START_OFFSET_S = 180.0
"""Seconds from the scheduled race start (``races.race_start_utc``) to lights out. The scheduled
time is the start of the formation lap, which usually takes about 2.5 to 4 minutes."""

_RACE = ["season", "round"]
_DRIVER = ["season", "round", "driver_id"]


def pit_per_lap(pitstops: DataFrame) -> DataFrame:
    """One row per (driver, lap) that has a stop, so two stops in one lap cannot duplicate lap
    rows. Columns: stop count, total pit-lane duration and whether any stop was long."""
    return pitstops.groupBy(*_DRIVER, "lap").agg(
        F.count("*").cast("int").alias("pit_stops_in_lap"),
        F.round(F.sum("duration_s"), 3).alias("pit_duration_s"),
        F.max(F.coalesce(F.col("is_long_stop"), F.lit(False))).alias("is_long_stop"),
    )


def _lag_exact(col: str, k: int, w: WindowSpec) -> Column:
    """``col`` from k laps earlier, or null if that row is not exactly lap - k."""
    return F.when(F.lag("lap", k).over(w) == F.col("lap") - k, F.lag(col, k).over(w))


def timing_gaps(laps: DataFrame) -> DataFrame:
    """Add cumulative time, leader/ahead gaps and their 1- and 3-lap changes to ``laps``.

    ``laps`` needs season, round, driver_id, lap, position and lap_time_s. The cumulative time
    becomes null at the first lap with an unknown time and stays null for every later lap, and
    any gap that depends on it is null too.
    """
    by_driver = Window.partitionBy(*_DRIVER).orderBy("lap")
    upto = by_driver.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    cum = F.when(
        F.sum(F.col("lap_time_s").isNull().cast("int")).over(upto) == 0,
        F.round(F.sum("lap_time_s").over(upto), 3),
    )
    df = laps.withColumn("cumulative_time_s", cum)

    same_lap = Window.partitionBy(*_RACE, "lap")
    by_pos = same_lap.orderBy("position")
    leader = F.max(F.when(F.col("position") == 1, F.col("cumulative_time_s"))).over(same_lap)
    df = (
        df.withColumn("gap_leader_s", F.round(F.col("cumulative_time_s") - leader, 3))
        .withColumn("ahead_driver_id", F.lag("driver_id").over(by_pos))
        .withColumn(
            "gap_ahead_s",
            F.round(F.col("cumulative_time_s") - F.lag("cumulative_time_s").over(by_pos), 3),
        )
    )
    for k in (1, 3):
        same_ahead = _lag_exact("ahead_driver_id", k, by_driver) == F.col("ahead_driver_id")
        df = df.withColumn(
            f"gap_ahead_change_{k}l",
            F.when(
                same_ahead,
                F.round(F.col("gap_ahead_s") - _lag_exact("gap_ahead_s", k, by_driver), 3),
            ),
        ).withColumn(
            f"gap_leader_change_{k}l",
            F.round(F.col("gap_leader_s") - _lag_exact("gap_leader_s", k, by_driver), 3),
        )
    return df


def lap_timestamps(laps: DataFrame, races: DataFrame) -> DataFrame:
    """Add the estimated ``lap_start_utc`` and ``lap_end_utc`` (needs ``cumulative_time_s``).

    Both are null when the race has no start time (Jolpica gives none, and the clean zone stores
    00:00:00 UTC) or when the cumulative time is unknown.
    """
    start = races.select(
        *_RACE,
        F.when(
            F.date_format("race_start_utc", "HH:mm:ss") != "00:00:00", F.col("race_start_utc")
        ).alias("_start"),
    )
    j = laps.join(start, _RACE, "left")
    base = F.unix_micros(F.col("_start")).cast("double") / 1e6 + F.lit(START_OFFSET_S)
    end_s = base + F.col("cumulative_time_s")
    lap_end = F.timestamp_micros(F.round(end_s * 1e6).cast("long"))
    lap_start = F.timestamp_micros(F.round((end_s - F.col("lap_time_s")) * 1e6).cast("long"))
    return (
        j.withColumn("lap_end_utc", lap_end)
        .withColumn("lap_start_utc", lap_start)
        .withColumn("lap_time_estimated", F.lit(True))
        .drop("_start")
    )


def tyre_age(laps: DataFrame, stints: DataFrame) -> DataFrame:
    """Attach the stint, the compound and ``tyre_age_laps`` (age at the end of the lap).

    ``tyre_age_laps = tyre_age_at_start + (lap - start_lap) + 1``. If ``tyre_age_at_start`` is
    unknown (stints inferred from Jolpica), the tyre is assumed new and ``tyre_age_assumed_new``
    is set. A lap that no stint covers gets nulls.
    """
    s = stints.select(
        *[F.col(c).alias(f"_s_{c}") for c in _DRIVER],
        F.col("stint"),
        F.col("start_lap").alias("_start_lap"),
        F.col("end_lap").alias("_end_lap"),
        F.col("compound"),
        F.col("tyre_age_at_start").alias("_age0"),
    )
    cond = (
        (F.col("season") == F.col("_s_season"))
        & (F.col("round") == F.col("_s_round"))
        & (F.col("driver_id") == F.col("_s_driver_id"))
        & F.col("lap").between(F.col("_start_lap"), F.col("_end_lap"))
    )
    j = laps.join(s, cond, "left")
    covered = F.col("_start_lap").isNotNull()
    age = F.coalesce(F.col("_age0"), F.lit(0)) + F.col("lap") - F.col("_start_lap") + 1
    return (
        j.withColumn("tyre_age_laps", F.when(covered, age.cast("int")))
        .withColumn("tyre_age_assumed_new", F.when(covered, F.col("_age0").isNull()))
        .drop(*[f"_s_{c}" for c in _DRIVER], "_start_lap", "_end_lap", "_age0")
    )


def race_distance(results: DataFrame, laps: DataFrame) -> DataFrame:
    """(season, round, race_laps): the winner's lap count, or the race's max lap without one."""
    winner = results.where(F.col("position") == 1).groupBy(*_RACE).agg(F.max("laps").alias("_w"))
    longest = laps.groupBy(*_RACE).agg(F.max("lap").alias("_m"))
    return longest.join(winner, _RACE, "left").select(
        *_RACE, F.coalesce("_w", "_m").cast("int").alias("race_laps")
    )


def enrich_laps(
    laps: DataFrame,
    results: DataFrame,
    pitstops: DataFrame,
    drivers: DataFrame,
    constructors: DataFrame,
    stints: DataFrame,
    races: DataFrame,
) -> DataFrame:
    """Laps enriched with entry, result, pit, gap, tyre, distance and timestamp features.

    Every joined side is reduced to at most one row per join key first, so the output has exactly
    one row per input lap.
    """
    base = laps.drop("cumulative_time_s", "gap_to_leader_s", "gap_ahead_s")
    res = results.select(
        *_DRIVER,
        "constructor_id",
        "grid",
        F.col("position").alias("final_position"),
        "status",
        "finished",
    ).dropDuplicates(_DRIVER)
    drv = drivers.select(*_DRIVER, F.col("code").alias("driver_code")).dropDuplicates(_DRIVER)
    con = constructors.select(
        *_RACE, "constructor_id", F.col("name").alias("constructor_name")
    ).dropDuplicates([*_RACE, "constructor_id"])

    df = timing_gaps(base)
    df = (
        df.join(res, _DRIVER, "left")
        .join(drv, _DRIVER, "left")
        .join(con, [*_RACE, "constructor_id"], "left")
        .join(pit_per_lap(pitstops), [*_DRIVER, "lap"], "left")
        .withColumn("pit_stops_in_lap", F.coalesce("pit_stops_in_lap", F.lit(0)))
        .withColumn("is_long_stop", F.coalesce("is_long_stop", F.lit(False)))
    )
    df = tyre_age(df, stints)
    df = df.join(race_distance(results, laps), _RACE, "left").withColumn(
        "laps_remaining", (F.col("race_laps") - F.col("lap")).cast("int")
    )
    df = lap_timestamps(df, races)
    return conform(df, FS.LAPS_ENRICHED, "laps_enriched")
