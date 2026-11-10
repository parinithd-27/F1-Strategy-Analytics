"""Smoke test: the shared Spark fixture starts with the agreed test configuration."""

from __future__ import annotations

from pyspark.sql import SparkSession


def test_spark_fixture_config_and_job(spark: SparkSession) -> None:
    assert spark.sparkContext.master == "local[2]"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
    df = spark.createDataFrame([(1, "VER"), (2, "NOR"), (3, "LEC")], ["position", "driver"])
    assert df.count() == 3
    assert df.groupBy("driver").count().rdd.getNumPartitions() <= 4
