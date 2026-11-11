"""Smoke test: the shared Spark fixture starts with the agreed test configuration."""

from __future__ import annotations

from pyspark.sql import SparkSession


def test_spark_fixture_config_and_job(spark: SparkSession) -> None:
    assert spark.sparkContext.master == "local[2]"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
    df = spark.createDataFrame([(1, "VER"), (2, "NOR"), (3, "LEC")], ["position", "driver"])
    assert df.count() == 3
    assert df.groupBy("driver").count().rdd.getNumPartitions() <= 4


def test_spark_fixture_memory_settings(spark: SparkSession) -> None:
    """The suite shares one driver JVM: explicit 2 GB heap, default broadcast threshold, capped
    status-store history (see ``conftest.spark``)."""
    conf = spark.sparkContext.getConf()
    assert conf.get("spark.driver.memory") == "2g"
    max_mb = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory() // (1 << 20)  # type: ignore[union-attr]
    assert max_mb >= 1900
    assert spark.conf.get("spark.sql.autoBroadcastJoinThreshold") == str(10 * 1024 * 1024)
    assert conf.get("spark.ui.retainedStages") == "100"
    assert conf.get("spark.sql.ui.retainedExecutions") == "50"


def test_module_caches_are_released(spark: SparkSession) -> None:
    from conftest import _release_module_caches

    df = spark.range(10).cache()
    df.count()
    assert spark.sparkContext._jsc.getPersistentRDDs().size() >= 1
    gen = _release_module_caches.__wrapped__()  # type: ignore[attr-defined]
    next(gen)
    for _ in gen:
        pass
    assert not df.storageLevel.useMemory
