"""Build-time helper: resolve the Spark Kafka connector once and install it into SPARK_HOME/jars.

Only jars whose artifact is not already shipped with PySpark are copied, so we never put two
versions of the same library on the classpath. Run with --verify to check (offline) that the
connector class loads without spark.jars.packages.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1"
PROVIDER = "org.apache.spark.sql.kafka010.KafkaSourceProvider"
IVY_DIR = Path("/tmp/ivy")
_VERSION_SUFFIX = re.compile(r"-\d[\w.]*(-[\w.]+)?$")


def artifact_name(jar: Path) -> str:
    """'org.foo_bar-baz_2.13-1.2.3.jar' or 'bar-baz_2.13-1.2.3.jar' -> 'bar-baz_2.13'."""
    stem = jar.stem
    if "_" in stem and "." in stem.split("_", 1)[0]:  # ivy layout: <group>_<artifact>-<ver>
        stem = stem.split("_", 1)[1]
    return _VERSION_SUFFIX.sub("", stem)


def _load_provider(spark: SparkSession) -> None:
    # Spark adds --packages jars to the thread context classloader, not the system one.
    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise RuntimeError("Spark JVM gateway is not available")
    jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(PROVIDER)


def fetch(spark_home: Path) -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", PACKAGE)
        .config("spark.jars.ivy", str(IVY_DIR))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        _load_provider(spark)
    finally:
        spark.stop()
    jars_dir = spark_home / "jars"
    present = {artifact_name(j) for j in jars_dir.glob("*.jar")}
    resolved = sorted((IVY_DIR / "jars").glob("*.jar"))
    if not resolved:
        raise SystemExit(f"no jars resolved for {PACKAGE}")
    for jar in resolved:
        name = artifact_name(jar)
        if name in present:
            print(f"skip   {jar.name} (PySpark already ships {name})")
            continue
        target = jars_dir / jar.name.split("_", 1)[1] if "_" in jar.name else jars_dir / jar.name
        shutil.copy2(jar, target)
        present.add(name)
        print(f"added  {target.name}")
    shutil.rmtree(IVY_DIR)


def verify() -> None:
    spark = (
        SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    )
    try:
        _load_provider(spark)
    finally:
        spark.stop()
    print(f"ok: {PROVIDER} loads from SPARK_HOME/jars without --packages")


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    else:
        fetch(Path(os.environ["SPARK_HOME"]))
