"""Stint features (#12: tyre age). The boundaries come from the RDD job, and the compound and
starting tyre age come from the matching clean stint."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from f1sa.etl.clean import conform
from f1sa.etl.rdd_jobs import rdd_stints
from f1sa.features import schemas as FS

_KEY = ["season", "round", "driver_id", "stint", "start_lap", "end_lap"]


def stint_features(laps: DataFrame, clean_stints: DataFrame) -> DataFrame:
    """RDD-reconstructed stints joined to the clean stints on identical boundaries.

    ``matches_clean`` is false when no clean stint has the same boundaries. Compound, starting age
    and ``is_synthetic`` are null in that case. ``compound_source`` is the clean row's ``source``
    (fastf1, synthetic or jolpica_inferred).
    """
    clean = clean_stints.select(
        *_KEY,
        "compound",
        "tyre_age_at_start",
        "is_synthetic",
        F.col("source").alias("compound_source"),
        F.lit(True).alias("matches_clean"),
    ).dropDuplicates(_KEY)
    j = rdd_stints(laps).join(clean, _KEY, "left")
    j = j.withColumn("matches_clean", F.coalesce("matches_clean", F.lit(False)))
    return conform(j, FS.STINTS, "stints")
