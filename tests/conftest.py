"""Shared pytest fixtures."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from f1sa.config import Settings, get_settings

SETTINGS_ENV_VARS = tuple(name.upper() for name in Settings.model_fields)


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """Session-wide local Spark: local[2], 4 shuffle partitions, no UI, isolated warehouse.

    The whole suite (~11,000 stages) shares one driver JVM. With Spark's default 1 GB heap the
    heap after GC sat at 500-930 MB from the clean-zone tests on (persisted RDDs and the
    status store of finished jobs, stages, tasks and SQL executions accumulate), so a broadcast or
    a local checkpoint late in the suite failed with "Not enough memory to build and broadcast"
    or ``OutOfMemoryError`` depending on GC timing. The heap is therefore explicit (2 GB), the
    status-store history is capped, and ``_release_module_caches`` drops cached DataFrames after
    every test module. The broadcast threshold is pinned at Spark's default (10 MB) so the test
    plans match the production jobs; none of these settings changes a result.
    """
    base: Path = tmp_path_factory.mktemp("spark")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("f1sa-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .config("spark.ui.retainedJobs", "100")
        .config("spark.ui.retainedStages", "100")
        .config("spark.ui.retainedTasks", "1000")
        .config("spark.sql.ui.retainedExecutions", "50")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(base / "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={base / 'derby'}")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture(autouse=True, scope="module")
def _release_module_caches() -> Iterator[None]:
    """After each test module, drop the DataFrames its fixtures cached (``clearCache``), so
    persisted data does not pile up in the shared driver heap. Session-scoped fixtures only hold
    paths, so nothing later depends on these caches. Modules that never start Spark are untouched
    (no session is created here)."""
    yield
    active = SparkSession.getActiveSession()
    if active is not None:
        active.catalog.clearCache()


@pytest.fixture(autouse=True)
def _isolate_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Keep tests independent of the developer's real environment and .env file."""
    for name in SETTINGS_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = REPO_ROOT / "data" / "sample"


class SampleLake:
    """A tmp lake whose clean zone was built from the real sample bundle (data/sample)."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.lake = base / "data" / "lake"
        self.clean_root = f"file://{self.lake}/clean"

    def settings(self) -> Settings:
        return Settings(
            _env_file=None,  # type: ignore[call-arg]
            data_lake_uri=f"file://{self.lake}",
            sample_dir=SAMPLE_DIR,
        )


@pytest.fixture(scope="session")
def sample_clean_lake(
    spark: SparkSession, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[SampleLake]:
    """Clean zone of the 2-race sample, built once per session into a tmp dir (never data/lake)."""
    if not (SAMPLE_DIR / "MANIFEST.json").exists():
        pytest.skip("sample bundle missing (make sample-data)")
    from f1sa.etl.clean import run_clean

    lake = SampleLake(tmp_path_factory.mktemp("sample_lake"))
    cwd = os.getcwd()
    os.chdir(lake.base)  # the output guard resolves ./data/lake against the working directory
    try:
        run_clean(spark, lake.settings(), source="sample", out=lambda _m: None)
    finally:
        os.chdir(cwd)
    yield lake


@pytest.fixture(scope="session")
def sample_features_root(spark: SparkSession, sample_clean_lake: SampleLake) -> str:
    """The sample lake's feature zone (built once if another test has not built it yet)."""
    root = sample_clean_lake.lake / "features"
    if not (root / "undercut_attempts").is_dir():
        from f1sa.features.build import run_build

        cwd = os.getcwd()
        os.chdir(sample_clean_lake.base)
        try:
            run_build(spark, sample_clean_lake.settings(), source="lake", out=lambda _m: None)
        finally:
            os.chdir(cwd)
    return f"file://{root}"
