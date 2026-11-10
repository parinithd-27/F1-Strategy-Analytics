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

  **Legacy.** About 74% of a stop's time loss lands on the out-lap (the timing line sits before
  the pit boxes), so this value is mostly stop time and is collinear with ``pit_loss``. It is
  kept for the S16 comparison but is not a default model feature.

* **Decomposition** (Step 8). For a history-eligible stop (realised delta known, out-lap not
  neutralised, stop not long, stop duration known):

  * ``race_median_outlap_delta_s`` / ``race_median_stop_s``: the medians over that race's
    eligible stops (the circuit's stop share on the out-lap, and its typical lane time);
  * ``stop_delta_s`` (ORACLE) = stop duration - race median stop (pit-crew execution);
  * ``outlap_pace_residual_s`` (ORACLE) = realised delta - race median delta - stop_delta_s
    (warm-up and tyre effect; it assumes stationary-time deviations land 1:1 on the out-lap).

* **expected_outlap_pace_residual_s** / **expected_stop_delta_s** (decision-time safe): the same
  strictly-earlier windows, with shrinkage (``SHRINK_KAPPA`` pseudo-observations):
  ``team = (n_t*mean_t + k*global) / (n_t + k)`` and
  ``driver_team = (n_dt*mean_dt + k*team) / (n_dt + k)``. The pace source is ``driver_team``
  when ``n_dt >= DRIVER_TEAM_MIN_N``, ``driver_team_shrunk`` for 1..N-1, then ``team``,
  ``global`` or ``none``. The stop delta is a team property (levels ``team``, ``global``,
  ``none``).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window, WindowSpec
from pyspark.sql import functions as F

from f1sa.etl.clean import conform
from f1sa.etl.df_jobs import tyre_age
from f1sa.features import schemas as FS
from f1sa.features.lineage import with_history_key

CLEAN_LAP_MAX_RATIO = 1.07
"""A clean lap is at most this multiple of the race's median candidate lap (the 107% rule)."""
FIRST_RACING_LAP = 2
"""Laps before this one (the standing-start lap) are never clean."""

SHRINK_KAPPA = 5.0
"""Shrinkage strength in pseudo-observations: a level with n observations gets weight n/(n+k)."""
DRIVER_TEAM_MIN_N = 5
"""Prior stops needed before the driver_team pace level is reported as ``driver_team`` (below
it, the estimate is shrunk towards the team and reported as ``driver_team_shrunk``)."""

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
    outlap_neutralised, is_long_stop (a clean-zone long stop, > 60 s, on the in-lap) and
    stop_duration_s (the in-lap's total pit-lane time). History rows exclude neutralised out-laps
    and long stops.
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
        F.max(F.coalesce(F.col("is_long_stop"), F.lit(False))).alias("_long"),
        F.round(F.sum("duration_s"), 3).alias("_dur"),
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
        F.when(has_out, F.col("_dur")).alias("stop_duration_s"),
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


def decompose_outlaps(realised: DataFrame) -> DataFrame:
    """``realised`` plus the race medians, ``stop_delta_s`` and ``outlap_pace_residual_s``.

    Only history-eligible stops (see the module docstring) get a residual; ``stop_delta_s`` is
    set for every stop that is not long and has a duration.
    """
    eligible = (
        F.col("realised_outlap_delta_s").isNotNull()
        & ~F.coalesce(F.col("outlap_neutralised"), F.lit(True))
        & ~F.coalesce(F.col("is_long_stop"), F.lit(True))
        & F.col("stop_duration_s").isNotNull()
    )
    df = realised.withColumn("_eligible", eligible)
    medians = (
        df.where("_eligible")
        .groupBy(*_RACE)
        .agg(
            F.round(F.median("realised_outlap_delta_s"), 3).alias("race_median_outlap_delta_s"),
            F.round(F.median("stop_duration_s"), 3).alias("race_median_stop_s"),
        )
    )
    df = df.join(medians, _RACE, "left")
    stop_ok = ~F.coalesce(F.col("is_long_stop"), F.lit(True)) & F.col("stop_duration_s").isNotNull()
    stop_delta = F.round(F.col("stop_duration_s") - F.col("race_median_stop_s"), 3)
    residual = F.round(
        F.col("realised_outlap_delta_s")
        - F.col("race_median_outlap_delta_s")
        - (F.col("stop_duration_s") - F.col("race_median_stop_s")),
        3,
    )
    return (
        df.withColumn("stop_delta_s", F.when(stop_ok, stop_delta))
        .withColumn("outlap_pace_residual_s", F.when(F.col("_eligible"), residual))
        .drop("_eligible")
    )


def _prior(partition: list[str]) -> WindowSpec:
    w = Window.partitionBy(*partition) if partition else Window.partitionBy()
    return w.orderBy("race_seq").rangeBetween(Window.unboundedPreceding, -1)


def shrunk(n: Column, total: Column, parent: Column, kappa: float = SHRINK_KAPPA) -> Column:
    """``(total + kappa*parent) / (n + kappa)``; the raw mean when ``parent`` is null; null when
    there is neither own history nor a parent."""
    own = F.when(n > 0, total / n)
    with_parent = (total + F.lit(kappa) * parent) / (n + F.lit(kappa))
    return F.when(parent.isNotNull(), with_parent).otherwise(own)


def expected_decomposed(entries: DataFrame, decomposed: DataFrame) -> DataFrame:
    """Per (race, driver): shrunk priors of the out-lap pace residual and the stop delta."""
    cols = ["driver_id", "team_history_key", "race_seq", "season", "round"]
    hist = decomposed.join(entries, _DRIVER).select(
        *cols,
        F.col("outlap_pace_residual_s").alias("_p"),
        F.col("stop_delta_s").alias("_d"),
        F.lit(False).alias("_anchor"),
    )
    anchors = entries.select(
        *cols,
        F.lit(None).cast("double").alias("_p"),
        F.lit(None).cast("double").alias("_d"),
        F.lit(True).alias("_anchor"),
    )
    rows = anchors.unionByName(hist)
    levels = {"dt": ["driver_id", "team_history_key"], "t": ["team_history_key"], "g": []}
    for v in ("_p", "_d"):
        for name, part in levels.items():
            w = _prior(part)
            rows = rows.withColumn(f"{v}_{name}_n", F.count(v).over(w).cast("int")).withColumn(
                f"{v}_{name}_sum", F.coalesce(F.sum(v).over(w), F.lit(0.0))
            )
    rows = rows.where("_anchor")
    for v in ("_p", "_d"):  # materialise each level so the expressions are not duplicated
        n_g, n_t, n_dt = (F.col(f"{v}_{x}_n") for x in ("g", "t", "dt"))
        rows = rows.withColumn(f"{v}_g", F.when(n_g > 0, F.col(f"{v}_g_sum") / n_g))
        rows = rows.withColumn(f"{v}_t", shrunk(n_t, F.col(f"{v}_t_sum"), F.col(f"{v}_g")))
        rows = rows.withColumn(f"{v}_dt", shrunk(n_dt, F.col(f"{v}_dt_sum"), F.col(f"{v}_t")))
    pn_dt, pn_t, pn_g = (F.col(f"_p_{x}_n") for x in ("dt", "t", "g"))
    rows = rows.withColumn(
        "_pace",
        F.when(pn_dt > 0, F.col("_p_dt")).when(pn_t > 0, F.col("_p_t")).otherwise(F.col("_p_g")),
    ).withColumn("_stop", F.when(F.col("_d_t_n") > 0, F.col("_d_t")).otherwise(F.col("_d_g")))
    pace_source = (
        F.when(pn_dt >= DRIVER_TEAM_MIN_N, "driver_team")
        .when(pn_dt > 0, "driver_team_shrunk")
        .when(pn_t > 0, "team")
        .when(pn_g > 0, "global")
        .otherwise("none")
    )
    sn_t, sn_g = F.col("_d_t_n"), F.col("_d_g_n")
    stop_source = F.when(sn_t > 0, "team").when(sn_g > 0, "global").otherwise("none")
    return rows.select(
        *_DRIVER,
        F.round("_pace", 3).alias("expected_outlap_pace_residual_s"),
        pace_source.alias("expected_outlap_pace_source"),
        F.col("_pace").isNull().alias("expected_outlap_pace_missing"),
        pn_dt.alias("pace_driver_team_n"),
        pn_t.alias("pace_team_n"),
        pn_g.alias("pace_global_n"),
        F.round("_stop", 3).alias("expected_stop_delta_s"),
        stop_source.alias("expected_stop_source"),
        sn_t.alias("stop_team_n"),
        sn_g.alias("stop_global_n"),
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
    # The realised table is used four times (two medians, three prior windows, the output). A
    # local checkpoint cuts its lineage; without it the optimised plan grows to ~13 MB and the
    # test JVM runs out of heap.
    realised = decompose_outlaps(realised_outlaps(laps, stints, pitstops)).localCheckpoint()
    entries = _entries(results, races)
    expected = expected_outlap(entries, realised)
    decomposed = expected_decomposed(entries, realised)
    df = (
        realised.join(entries, _DRIVER, "left")
        .join(expected, _DRIVER, "left")
        .join(decomposed, _DRIVER, "left")
        .withColumn("expected_outlap_source", F.coalesce("expected_outlap_source", F.lit("none")))
        .withColumn("expected_outlap_missing", F.coalesce("expected_outlap_missing", F.lit(True)))
        .withColumn(
            "expected_outlap_pace_source",
            F.coalesce("expected_outlap_pace_source", F.lit("none")),
        )
        .withColumn(
            "expected_outlap_pace_missing",
            F.coalesce("expected_outlap_pace_missing", F.lit(True)),
        )
        .withColumn("expected_stop_source", F.coalesce("expected_stop_source", F.lit("none")))
    )
    return conform(df, FS.OUTLAP_FEATURES, "outlap_features")
