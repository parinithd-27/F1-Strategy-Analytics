"""RDD API jobs (commitment #9), each with a DataFrame implementation of the same logic.

(a) Stint reconstruction. Laps are keyed by (season, round, driver_id), grouped with
    ``groupByKey``, sorted by lap and split after every pit-in lap that is not the driver's last
    lap. This rule is the same one the clean zone (``etl.clean.infer_stints``) and the synthetic
    generator use, so the boundaries match the clean stints table.
(b) Lap-time distribution per circuit and race, built with ``aggregateByKey``. The mergeable
    accumulator holds (n, mean, M2, min, max, 1-second histogram); Chan's parallel update keeps the
    variance numerically stable. Only green racing laps are counted: not lap 1, not a pit in or out
    lap, not neutralised or red-flag, and the time must be known.

The DataFrame twins (``df_stints``, ``df_lap_time_distribution``) exist so the tests can prove
that the two APIs agree row for row.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F

from f1sa.features import schemas as FS

DriverKey = tuple[int, int, str]
RaceCircuitKey = tuple[int, int, str]
StintTuple = tuple[int, int, int]
"""(stint, start_lap, end_lap)"""
Acc = tuple[int, float, float, float, float, dict[int, int]]
"""(n, mean, M2, min, max, {floor(seconds): count})"""


# ============================================================================== (a) stints
def split_stints(laps: Iterable[tuple[int, bool | None]]) -> list[StintTuple]:
    """Split one driver's ``(lap, is_pit_in_lap)`` pairs (any order) into stints.

    A pit-in lap ends a stint unless it is the driver's last lap (a stop on the last lap, or a
    retirement in the pits, starts no new stint). A null pit flag counts as no stop. An empty
    input gives no stints.
    """
    ordered = sorted(laps, key=lambda p: p[0])
    if not ordered:
        return []
    last = ordered[-1][0]
    out: list[StintTuple] = []
    stint, start = 1, ordered[0][0]
    for lap, pit_in in ordered:
        if pit_in and lap < last:
            out.append((stint, start, lap))
            stint, start = stint + 1, lap + 1
    out.append((stint, start, last))
    return out


def _stint_rows(item: tuple[DriverKey, Iterable[tuple[int, bool | None]]]) -> list[Row]:
    (season, round_, driver), laps = item
    return [
        Row(
            season=season,
            round=round_,
            driver_id=driver,
            stint=stint,
            start_lap=start,
            end_lap=end,
            laps=end - start + 1,
        )
        for stint, start, end in split_stints(laps)
    ]


def rdd_stints(laps: DataFrame) -> DataFrame:
    """Stint boundaries from the clean laps table, computed with the RDD API."""
    spark = laps.sparkSession
    rdd = (
        laps.select("season", "round", "driver_id", "lap", "is_pit_in_lap")
        .rdd.map(tuple)
        .keyBy(lambda t: (int(t[0]), int(t[1]), str(t[2])))
        .mapValues(lambda t: (int(t[3]), t[4]))
        .groupByKey()
        .flatMap(_stint_rows)
    )
    return spark.createDataFrame(rdd, FS.STINT_BOUNDARIES)


def df_stints(laps: DataFrame) -> DataFrame:
    """The same stint split with DataFrame windows (reference for ``rdd_stints``)."""
    key = ["season", "round", "driver_id"]
    w = Window.partitionBy(*key).orderBy("lap")
    upto = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    prev_pit = F.coalesce(F.lag("is_pit_in_lap").over(w), F.lit(False)).cast("int")
    with_prev = laps.select(*key, "lap", prev_pit.alias("_prev_pit"))
    with_stint = with_prev.withColumn("stint", (F.sum("_prev_pit").over(upto) + 1).cast("int"))
    g = with_stint.groupBy(*key, "stint").agg(
        F.min("lap").alias("start_lap"), F.max("lap").alias("end_lap")
    )
    return g.select(
        *key,
        "stint",
        "start_lap",
        "end_lap",
        (F.col("end_lap") - F.col("start_lap") + 1).cast("int").alias("laps"),
    )


# ============================================================================== (b) lap times
def green_laps(laps: DataFrame) -> DataFrame:
    """Green racing laps: lap > 1, no pit in/out, not neutralised or red-flag, time known."""
    return laps.where(
        (F.col("lap") > 1)
        & ~F.coalesce(F.col("is_pit_in_lap"), F.lit(False))
        & ~F.coalesce(F.col("is_pit_out_lap"), F.lit(False))
        & ~F.coalesce(F.col("is_neutralised_lap"), F.lit(False))
        & ~F.coalesce(F.col("is_red_flag_lap"), F.lit(False))
        & F.col("lap_time_s").isNotNull()
    )


def zero_acc() -> Acc:
    return (0, 0.0, 0.0, math.inf, -math.inf, {})


def seq_acc(acc: Acc, x: float) -> Acc:
    """Add one lap time (Welford update)."""
    n, mean, m2, lo, hi, bins = acc
    n += 1
    delta = x - mean
    mean += delta / n
    m2 += delta * (x - mean)
    b = math.floor(x)
    bins[b] = bins.get(b, 0) + 1
    return (n, mean, m2, min(lo, x), max(hi, x), bins)


def comb_acc(a: Acc, b: Acc) -> Acc:
    """Merge two partial accumulators (Chan et al. parallel variance)."""
    na, ma, m2a, loa, hia, ba = a
    nb, mb, m2b, lob, hib, bb = b
    if na == 0:
        return b
    if nb == 0:
        return a
    n = na + nb
    delta = mb - ma
    mean = ma + delta * nb / n
    m2 = m2a + m2b + delta * delta * na * nb / n
    bins = dict(ba)
    for k, v in bb.items():
        bins[k] = bins.get(k, 0) + v
    return (n, mean, m2, min(loa, lob), max(hia, hib), bins)


def _dist_row(item: tuple[RaceCircuitKey, Acc]) -> Row:
    (season, round_, circuit), (n, mean, m2, lo, hi, bins) = item
    return Row(
        season=season,
        round=round_,
        circuit_id=circuit,
        n_laps=n,
        mean_s=mean,
        std_s=math.sqrt(m2 / (n - 1)) if n > 1 else None,
        min_s=lo,
        max_s=hi,
        bins_1s=bins,
    )


def rdd_lap_time_distribution(laps: DataFrame, races: DataFrame) -> DataFrame:
    """Per (season, round, circuit_id) distribution of green lap times, via ``aggregateByKey``.

    The circuit of each race comes from a broadcast dict. Races with no green laps have no row.
    """
    spark = laps.sparkSession
    circuits: dict[tuple[int, int], str] = {
        (int(r["season"]), int(r["round"])): str(r["circuit_id"])
        for r in races.select("season", "round", "circuit_id").collect()
    }
    bc = spark.sparkContext.broadcast(circuits)

    def keyed(t: tuple[Any, ...]) -> list[tuple[RaceCircuitKey, float]]:
        season, round_ = int(t[0]), int(t[1])
        circuit = bc.value.get((season, round_))
        return [] if circuit is None else [((season, round_, circuit), float(t[2]))]

    rdd = (
        green_laps(laps)
        .select("season", "round", "lap_time_s")
        .rdd.map(tuple)
        .flatMap(keyed)
        .aggregateByKey(zero_acc(), seq_acc, comb_acc)
        .map(_dist_row)
    )
    return spark.createDataFrame(rdd, FS.LAP_TIME_DISTRIBUTION)


def df_lap_time_distribution(laps: DataFrame, races: DataFrame) -> DataFrame:
    """The same distribution with ``groupBy``/``agg`` (reference for the RDD job)."""
    key = ["season", "round", "circuit_id"]
    g = green_laps(laps).join(races.select("season", "round", "circuit_id"), ["season", "round"])
    stats = g.groupBy(*key).agg(
        F.count("*").alias("n_laps"),
        F.avg("lap_time_s").alias("mean_s"),
        F.stddev_samp("lap_time_s").alias("std_s"),
        F.min("lap_time_s").alias("min_s"),
        F.max("lap_time_s").alias("max_s"),
    )
    bins = (
        g.groupBy(*key, F.floor("lap_time_s").cast("int").alias("b"))
        .agg(F.count("*").alias("c"))
        .groupBy(*key)
        .agg(F.map_from_entries(F.collect_list(F.struct("b", "c"))).alias("bins_1s"))
    )
    j = stats.join(bins, key)
    return j.select(
        [F.col(f.name).cast(f.dataType).alias(f.name) for f in FS.LAP_TIME_DISTRIBUTION.fields]
    )
