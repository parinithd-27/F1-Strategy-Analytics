"""Pre-race context of one race for the stream job, read once when its first event arrives.

* ``pit_loss_prior_s`` / ``pit_loss_source`` from ``features/pit_loss`` (circuit / global prior of
  **strictly earlier** races, Step 6);
* per driver, the out-lap and stop priors from ``features/outlap_features`` (earlier races only,
  Step 7/8), reduced exactly as ``features.undercut._driver_priors`` does (max per driver);
* ``team_keys``: the lineage key of each entrant's constructor (``clean/results``; the entry list,
  known before the race), as ``undercut.race_lap_rows`` builds ``team_key``.

Only the target race's partition directories ``season=S/round=R`` are read, so no other race (and
never the sealed 2025 season, unless explicitly allowed) is listed or opened. A missing table is
an error; a race without a row gives null priors with source ``none``, as in the batch join.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from f1sa.etl.clean import spark_uri
from f1sa.features import schemas as FS
from f1sa.features.lineage import history_key
from f1sa.lake import schemas as S
from f1sa.streaming.live_features import DriverPrior, RacePriors
from f1sa.streaming.processor import SEALED_SEASON


class PriorsError(RuntimeError):
    pass


def _exists(spark: SparkSession, uri: str) -> bool:
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise PriorsError("Spark JVM gateway unavailable")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return bool(path.getFileSystem(conf).exists(path))


def read_race_partition(
    spark: SparkSession, root: str, table: str, schema: StructType, season: int, round_: int
) -> DataFrame:
    """``{root}/{table}/season=S/round=R`` only (partition columns kept); empty if the race has no
    partition. Raises ``PriorsError`` if the table itself does not exist."""
    base = f"{root}/{table}"
    if not _exists(spark, base):
        raise PriorsError(f"table not found: {base}")
    uri = f"{base}/season={int(season)}/round={int(round_)}"
    cols = [f.name for f in schema.fields]
    if not _exists(spark, uri):
        return spark.createDataFrame([], schema).select(cols)
    return spark.read.schema(schema).option("basePath", base).parquet(uri).select(cols)


def priors_from_frames(
    pit_loss: DataFrame, outlap: DataFrame, results: DataFrame, season: int, round_: int
) -> RacePriors:
    """``RacePriors`` of one race from the three tables (other races are filtered out)."""

    def race(df: DataFrame) -> DataFrame:
        return df.where((F.col("season") == season) & (F.col("round") == round_))

    loss = race(pit_loss).select("pit_loss_prior_s", "pit_loss_source").collect()
    if len(loss) > 1:
        raise PriorsError(f"{season} R{round_}: {len(loss)} pit_loss rows, expected at most 1")
    drivers = (
        race(outlap)
        .groupBy("driver_id")
        .agg(
            F.max("expected_outlap_pace_residual_s").alias("pace_residual"),
            F.max("expected_outlap_pace_source").alias("pace_source"),
            F.max("expected_stop_delta_s").alias("stop_delta"),
        )
        .collect()
    )
    teams: dict[str, str | None] = {}
    for r in race(results).select("driver_id", "constructor_id").collect():
        if r["driver_id"] not in teams:  # dropDuplicates in enrich_laps keeps one row
            cid = r["constructor_id"]
            teams[r["driver_id"]] = history_key(cid) if cid is not None else None
    return RacePriors(
        season=season,
        round=round_,
        pit_loss_prior_s=loss[0]["pit_loss_prior_s"] if loss else None,
        pit_loss_source=loss[0]["pit_loss_source"] if loss else None,
        team_keys=teams,
        drivers={
            r["driver_id"]: DriverPrior(r["pace_residual"], r["pace_source"], r["stop_delta"])
            for r in drivers
        },
    )


def load_priors(
    spark: SparkSession,
    lake_uri: str,
    season: int,
    round_: int,
    *,
    allow_test_season: bool = False,
) -> RacePriors:
    """The priors of one race from the lake at ``lake_uri`` (``Settings.data_lake_uri``)."""
    if season == SEALED_SEASON and not allow_test_season:
        raise PriorsError(f"season {season} is sealed; pass --allow-test-season to read it")
    lake = spark_uri(lake_uri)
    features, clean = f"{lake}/features", f"{lake}/clean"
    return priors_from_frames(
        read_race_partition(
            spark, features, "pit_loss", FS.FEATURE_SCHEMAS["pit_loss"], season, round_
        ),
        read_race_partition(
            spark,
            features,
            "outlap_features",
            FS.FEATURE_SCHEMAS["outlap_features"],
            season,
            round_,
        ),
        read_race_partition(spark, clean, "results", S.CLEAN_SCHEMAS["results"], season, round_),
        season,
        round_,
    )
