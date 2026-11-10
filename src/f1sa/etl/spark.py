"""SparkSession construction and the runtime settings every ETL job relies on."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

RUNTIME_CONF = {
    # reruns / subsets replace only the partitions they write
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    # pandas (FastF1) nanosecond timestamps/durations -> long nanoseconds instead of an error
    "spark.sql.legacy.parquet.nanosAsLong": "true",
    "spark.sql.session.timeZone": "UTC",
}


def configure_session(spark: SparkSession) -> None:
    """Apply the runtime SQL settings the clean job needs (safe on an existing session)."""
    for key, value in RUNTIME_CONF.items():
        spark.conf.set(key, value)


def build_spark(
    app: str,
    master: str = "local[2]",
    *,
    local_dir: Path | None = None,
    shuffle_partitions: int = 4,
) -> SparkSession:
    """A SparkSession for ETL jobs. ``local_dir`` keeps Spark scratch files inside the lake."""
    builder = (
        SparkSession.builder.master(master)
        .appName(app)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if master.startswith("local"):
        builder = builder.config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        )
    if local_dir is not None:
        local_dir.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", str(local_dir.resolve()))
    for key, value in RUNTIME_CONF.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
