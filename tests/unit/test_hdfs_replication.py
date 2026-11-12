"""HDFS replication for files we write: Spark jobs (``spark.hadoop.dfs.replication``) and the
HttpFS sink (``replication`` on CREATE). The dev cluster has one datanode."""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

import pydantic
import pytest
import requests
from _fakes import FakeHttpFS

from f1sa.config import Settings, get_settings
from f1sa.etl import spark as spark_mod
from f1sa.ingest.sinks import HdfsSink

URI = "hdfs://namenode:8020/f1/raw/tracinginsights/laps/season=2024/round=1"


def test_default_is_one() -> None:
    assert Settings().hdfs_replication == 1
    assert spark_mod.hadoop_config() == {"spark.hadoop.dfs.replication": "1"}


def test_setting_is_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HDFS_REPLICATION", "2")
    get_settings.cache_clear()
    try:
        assert spark_mod.hadoop_config() == {"spark.hadoop.dfs.replication": "2"}
    finally:
        get_settings.cache_clear()
    assert spark_mod.hadoop_config(3) == {"spark.hadoop.dfs.replication": "3"}


@pytest.mark.parametrize("value", [0, 4])
def test_setting_is_validated(value: int) -> None:
    with pytest.raises(pydantic.ValidationError):
        Settings(hdfs_replication=value)


def test_explicit_zero_is_refused() -> None:
    with pytest.raises(ValueError, match=">= 1"):
        spark_mod.hadoop_config(0)


class _Builder:
    def __init__(self) -> None:
        self.conf: dict[str, str] = {}

    def master(self, _m: str) -> _Builder:
        return self

    def appName(self, _a: str) -> _Builder:
        return self

    def config(self, key: str, value: str) -> _Builder:
        self.conf[key] = value
        return self

    def getOrCreate(self) -> Any:
        class _Session:
            class sparkContext:
                @staticmethod
                def setLogLevel(_l: str) -> None:
                    return None

        return _Session()


def test_build_spark_applies_the_hadoop_config(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = _Builder()
    monkeypatch.setattr(spark_mod.SparkSession, "builder", builder)
    spark_mod.build_spark("t")
    assert builder.conf["spark.hadoop.dfs.replication"] == "1"
    spark_mod.build_spark("t", hdfs_replication=2)
    assert builder.conf["spark.hadoop.dfs.replication"] == "2"
    for key, value in spark_mod.RUNTIME_CONF.items():
        assert builder.conf[key] == value


class _Recording(FakeHttpFS):
    def __init__(self) -> None:
        super().__init__()
        self.creates: list[dict[str, str]] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        q = {k: v[0] for k, v in parse_qs(urlparse(request.url or "").query).items()}
        if q.get("op") == "CREATE":
            self.creates.append(q)
        return super().send(request, **kwargs)


@pytest.mark.parametrize("replication", [1, 2])
def test_httpfs_create_carries_replication(replication: int) -> None:
    fake = _Recording()
    session = requests.Session()
    session.mount("http://", fake)
    sink = HdfsSink(
        URI, Settings(hdfs_replication=replication), session=session, sleep=lambda _s: None
    )
    sink.write_bytes("laps.json", b"{}")
    assert [c["replication"] for c in fake.creates] == [str(replication)]
