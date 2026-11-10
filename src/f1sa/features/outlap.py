"""Out-lap pace features (#12): clean laps, realised out-lap delta (ORACLE), expected delta.

The definitions and parameters are in ``docs/label_definition.md``, section "Out-lap pace features".

* **Clean lap**: all of the following hold:

  * ``lap >= FIRST_RACING_LAP`` (2), so the standing-start lap is excluded;
  * not an in-lap and not an out-lap;
  * not neutralised, safety car, virtual safety car or red flag (``is_neutralised_lap``,
    ``is_sc_lap``, ``is_vsc_lap``, ``is_red_flag_lap``);
  * the lap time is known;
  * ``lap_time_s <= CLEAN_LAP_MAX_RATIO x`` the race's median candidate lap. A candidate lap is
    one that passes every other rule. This last check catches slow laps the neutralisation
    heuristic misses.

* **realised_outlap_delta_s** (ORACLE only, **not** known at decision time): the out-lap time
  minus the driver's median clean lap in the stint that the out-lap starts. If that stint has no
  clean lap, the previous stint's median is used. It needs laps after the stop.

* **expected_outlap_delta_s** (decision-time safe): the mean realised delta from **strictly
  earlier** races. The window is ``rangeBetween(unboundedPreceding, -1)`` over
  ``race_seq = season*100 + round``. Levels, in order:

  1. the same driver and the same team (team history key, via ``features.lineage``);
  2. the team;
  3. the circuit;
  4. global;
  5. null, with ``expected_outlap_source = 'none'``.

  History rows exclude neutralised out-laps, long stops and null deltas.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from f1sa.etl.clean import conform
from f1sa.etl.df_jobs import tyre_age
from f1sa.features import schemas as FS
from f1sa.features.lineage import with_history_key

CLEAN_LAP_MAX_RATIO = 1.07
"""A clean lap is at most this multiple of the race's median candidate lap (the 107% rule)."""
FIRST_RACING_LAP = 2
"""Laps before this one (the standing-start lap) are never clean."""

_RACE = ["season", "round"]
_DRIVER = ["season", "round", "driver_id"]
LEVELS = ("driver_team", "team", "circuit", "global")


def _flag(col: str) -> Column:
    return F.coalesce(F.col(col), F.lit(False))


def clean_laps(laps: DataFrame) -> DataFrame:
    """``laps`` plus ``race_candidate_median_s`` and ``is_clean_lap`` (see the module docstring)."""
    candidate = (
        (F.col("lap") >= FIRST_RACING_LAP)
        & ~_flag("is_pit_in_lap")
        & ~_flag("is_pit_out_lap")
        & ~_flag("is_neutralised_lap")
        & ~_flag("is_sc_lap")
        & ~_flag("is_vsc_lap")
        & ~_flag("is_red_flag_lap")
        & F.col("lap_time_s").isNotNull()
    )
    df = laps.withColumn("_candidate", candidate)
    medians = (
        df.where("_candidate")
        .groupBy(*_RACE)
        .agg(F.median("lap_time_s").alias("race_candidate_median_s"))
    )
    df = df.join(medians, _RACE, "left")
    clean = F.col("_candidate") & (
        F.col("lap_time_s") <= F.lit(CLEAN_LAP_MAX_RATIO) * F.col("race_candidate_median_s")
    )
    return df.withColumn("is_clean_lap", F.coalesce(clean, F.lit(False))).drop("_candidate")


def realised_outlaps(laps: DataFrame, stints: DataFrame, pitstops: DataFrame) -> DataFrame:
    """One row per (race, driver, stint). For stint k > 1, the out-lap is the stint's first lap.

    Columns: outlap, outlap_time_s, reference_clean_lap_s, reference_source (``same_stint``,
    ``previous_stint`` or ``none``; null for a stint without an out-lap), realised_outlap_delta_s,
    outlap_neutralised and is_long_stop (a clean-zone long stop, > 60 s, on the in-lap). History
    rows exclude both.
    """
    flagged = tyre_age(clean_laps(laps), stints)
    ref = (
        flagged.where(F.col("is_clean_lap") & F.col("stint").isNotNull())
        .groupBy(*_DRIVER, "stint")
        .agg(F.median("lap_time_s").alias("_ref"))
    )
    prev = ref.select(
        *_DRIVER, (F.col("stint") + 1).alias("stint"), F.col("_ref").alias("_prev_ref")
    )
    out_lap = flagged.select(
        *_DRIVER,
        F.col("lap").alias("_start_lap"),
        F.col("lap_time_s").alias("outlap_time_s"),
        (_flag("is_neutralised_lap") | _flag("is_red_flag_lap")).alias("outlap_neutralised"),
    )
    long_stop = pitstops.groupBy(*_DRIVER, (F.col("lap") + 1).alias("_start_lap")).agg(
        F.max(F.coalesce(F.col("is_long_stop"), F.lit(False))).alias("_long")
    )
    s = stints.select(*_DRIVER, "stint", F.col("start_lap").alias("_start_lap")).dropDuplicates(
        [*_DRIVER, "stint"]
    )
    j = (
        s.join(ref, [*_DRIVER, "stint"], "left")
        .join(prev, [*_DRIVER, "stint"], "left")
        .join(out_lap, [*_DRIVER, "_start_lap"], "left")
        .join(long_stop, [*_DRIVER, "_start_lap"], "left")
    )
    has_out = F.col("stint") > 1
    reference = F.coalesce("_ref", "_prev_ref")
    source = (
        F.when(F.col("_ref").isNotNull(), "same_stint")
        .when(F.col("_prev_ref").isNotNull(), "previous_stint")
        .otherwise("none")
    )
    return j.select(
        *_DRIVER,
        "stint",
        F.when(has_out, F.col("_start_lap")).alias("outlap"),
        F.when(has_out, F.col("outlap_time_s")).alias("outlap_time_s"),
        F.when(has_out, F.round(reference, 3)).alias("reference_clean_lap_s"),
        F.when(has_out, source).alias("reference_source"),
        F.when(has_out, F.round(F.col("outlap_time_s") - reference, 3)).alias(
            "realised_outlap_delta_s"
        ),
        F.when(has_out, F.coalesce("outlap_neutralised", F.lit(False))).alias("outlap_neutralised"),
        F.when(has_out, F.coalesce("_long", F.lit(False))).alias("is_long_stop"),
    )


def _entries(results: DataFrame, races: DataFrame) -> DataFrame:
    """(race, driver) entries with constructor, team history key, circuit and race_seq."""
    e = results.select(*_DRIVER, "constructor_id").dropDuplicates(_DRIVER)
    e = with_history_key(e)
    r = races.select(*_RACE, "circuit_id")
    return e.join(r, _RACE, "left").withColumn(
        "race_seq", (F.col("season") * 100 + F.col("round")).cast("int")
    )


def expected_outlap(entries: DataFrame, realised: DataFrame) -> DataFrame:
    """Per (race, driver): the prior mean out-lap delta with its fallback level and support."""
    history = (
        realised.where(
            F.col("realised_outlap_delta_s").isNotNull()
            & ~F.col("outlap_neutralised")
            & ~F.col("is_long_stop")
        )
        .join(entries, _DRIVER)
        .select(
            "driver_id",
            "team_history_key",
            "circuit_id",
            "race_seq",
            F.col("realised_outlap_delta_s").alias("v"),
            F.lit(False).alias("_anchor"),
            "season",
            "round",
        )
    )
    anchors = entries.select(
        "driver_id",
        "team_history_key",
        "circuit_id",
        "race_seq",
        F.lit(None).cast("double").alias("v"),
        F.lit(True).alias("_anchor"),
        "season",
        "round",
    )
    rows = anchors.unionByName(history)
    order = Window.orderBy("race_seq").rangeBetween(Window.unboundedPreceding, -1)
    windows = {
        "driver_team": Window.partitionBy("driver_id", "team_history_key")
        .orderBy("race_seq")
        .rangeBetween(Window.unboundedPreceding, -1),
        "team": Window.partitionBy("team_history_key")
        .orderBy("race_seq")
        .rangeBetween(Window.unboundedPreceding, -1),
        "circuit": Window.partitionBy("circuit_id")
        .orderBy("race_seq")
        .rangeBetween(Window.unboundedPreceding, -1),
        "global": order,
    }
    for level, w in windows.items():
        rows = rows.withColumn(f"{level}_prior_s", F.round(F.avg("v").over(w), 3)).withColumn(
            f"{level}_prior_n", F.count("v").over(w).cast("int")
        )
    expected = F.coalesce(*[F.col(f"{lvl}_prior_s") for lvl in LEVELS])
    source = F.lit("none")
    for level in reversed(LEVELS):
        source = F.when(F.col(f"{level}_prior_s").isNotNull(), level).otherwise(source)
    return rows.where("_anchor").select(
        *_DRIVER,
        expected.alias("expected_outlap_delta_s"),
        source.alias("expected_outlap_source"),
        expected.isNull().alias("expected_outlap_missing"),
        *[F.col(f"{lvl}_prior_{x}") for lvl in LEVELS for x in ("s", "n")],
    )


def outlap_features(
    laps: DataFrame,
    stints: DataFrame,
    pitstops: DataFrame,
    results: DataFrame,
    races: DataFrame,
) -> DataFrame:
    """The ``outlap_features`` table: one row per (race, driver, stint).

    Pass every clean race. History comes from strictly earlier races, so filtering to target races
    afterwards gives the same values as a full build.
    """
    realised = realised_outlaps(laps, stints, pitstops)
    entries = _entries(results, races)
    expected = expected_outlap(entries, realised)
    df = (
        realised.join(entries, _DRIVER, "left")
        .join(expected, _DRIVER, "left")
        .withColumn("expected_outlap_source", F.coalesce("expected_outlap_source", F.lit("none")))
        .withColumn("expected_outlap_missing", F.coalesce("expected_outlap_missing", F.lit(True)))
    )
    return conform(df, FS.OUTLAP_FEATURES, "outlap_features")
