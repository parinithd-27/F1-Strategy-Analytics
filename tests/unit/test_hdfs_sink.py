"""Tests for HdfsSink against an in-memory fake HttpFS gateway (no network)."""

from __future__ import annotations

import hashlib
import json

import pandas as pd
import pytest
import requests
from _fakes import FakeHttpFS

from f1sa.config import Settings
from f1sa.ingest.sinks import (
    MANIFEST_NAME,
    HdfsSink,
    SinkError,
    check_sink_matches_lake,
    make_sink,
)

URI = "hdfs://namenode:8020/f1/raw/jolpica/laps/season=2021/round=5"
PATH = "/f1/raw/jolpica/laps/season=2021/round=5"


def make(fake: FakeHttpFS, sleeps: list[float] | None = None) -> HdfsSink:
    session = requests.Session()
    session.mount("http://", fake)
    record = sleeps if sleeps is not None else []
    return HdfsSink(URI, Settings(), session=session, sleep=record.append, max_retries=3)


def test_write_creates_temp_then_renames() -> None:
    fake = FakeHttpFS()
    sink = make(fake)
    written = sink.write_bytes("laps.json", b'{"a": 1}', rows=1)
    assert fake.files == {f"{PATH}/laps.json": b'{"a": 1}'}
    ops = [op for op, _ in fake.ops]
    assert ops[:2] == ["MKDIRS", "CREATE"]
    create_path = next(p for op, p in fake.ops if op == "CREATE")
    assert create_path.startswith(f"{PATH}/_tmp.") and create_path.endswith(".laps.json")
    assert "RENAME" in ops
    assert written.sha256 == hashlib.sha256(b'{"a": 1}').hexdigest() and written.bytes == 8


def test_overwrite_replaces_existing_file() -> None:
    fake = FakeHttpFS()
    sink = make(fake)
    sink.write_bytes("laps.json", b"old")
    sink.write_bytes("laps.json", b"newer")
    assert fake.files == {f"{PATH}/laps.json": b"newer"}
    assert ("DELETE", f"{PATH}/laps.json") in fake.ops  # RENAME never overwrites on HttpFS
    assert len(sink.files) == 1


def test_repeat_identical_write_is_idempotent() -> None:
    fake = FakeHttpFS()
    sink = make(fake)
    a = sink.write_bytes("x.json", b"same")
    b = sink.write_bytes("x.json", b"same")
    assert a == b and fake.files == {f"{PATH}/x.json": b"same"}


def test_json_parquet_and_manifest() -> None:
    fake = FakeHttpFS()
    sink = make(fake)
    sink.write_json("results.json", [{"MRData": {}}], rows=3)
    df = pd.DataFrame({"lap": [1, 2]})
    sink.write_parquet("laps.parquet", df)
    sink.write_manifest({"source": "jolpica"})
    manifest = json.loads(sink.read_bytes(MANIFEST_NAME))
    assert {f["name"]: f["rows"] for f in manifest["files"]} == {
        "laps.parquet": 2,
        "results.json": 3,
    }
    assert manifest["partition"] == URI
    assert sink.exists("laps.parquet") and not sink.exists("nope.json")


def test_transient_5xx_is_retried() -> None:
    fake = FakeHttpFS(faults={"CREATE": [503, 500]})
    sleeps: list[float] = []
    make(fake, sleeps).write_bytes("a.json", b"x")
    assert fake.files[f"{PATH}/a.json"] == b"x"
    assert sleeps == [0.5, 1.0]


def test_connection_error_retried() -> None:
    class Flaky(FakeHttpFS):
        def __init__(self) -> None:
            super().__init__()
            self.fail = 1

        def send(self, request, **kwargs):  # type: ignore[no-untyped-def,override]
            if self.fail:
                self.fail -= 1
                raise requests.ConnectionError("reset")
            return super().send(request, **kwargs)

    fake = Flaky()
    make(fake).write_bytes("a.json", b"x")
    assert fake.files[f"{PATH}/a.json"] == b"x"


def test_4xx_fails_without_retry_and_cleans_temp() -> None:
    fake = FakeHttpFS(faults={"RENAME": [403]})
    sleeps: list[float] = []
    with pytest.raises(SinkError, match="HTTP 403"):
        make(fake, sleeps).write_bytes("a.json", b"x")
    assert sleeps == []
    assert fake.files == {}  # temp deleted, nothing committed


def test_retries_exhausted_raises() -> None:
    fake = FakeHttpFS(faults={"MKDIRS": [503] * 4})
    with pytest.raises(SinkError, match="503"):
        make(fake).write_bytes("a.json", b"x")


def test_length_mismatch_raises_and_cleans_up() -> None:
    fake = FakeHttpFS()
    fake.truncate_create = True
    with pytest.raises(SinkError, match="wrote"):
        make(fake).write_bytes("a.json", b"xyz")
    assert fake.files == {}


def test_keyboard_interrupt_mid_write_leaves_no_temp() -> None:
    class Interrupt(FakeHttpFS):
        def send(self, request, **kwargs):  # type: ignore[no-untyped-def,override]
            if "op=RENAME" in (request.url or ""):
                raise KeyboardInterrupt
            return super().send(request, **kwargs)

    fake = Interrupt()
    with pytest.raises(KeyboardInterrupt):
        make(fake).write_bytes("a.json", b"x")
    assert fake.files == {}


def test_cleanup_temp_removes_stale_files() -> None:
    fake = FakeHttpFS()
    sink = make(fake)
    sink.write_bytes("keep.json", b"1")
    fake.files[f"{PATH}/_tmp.dead.keep.json"] = b"partial"
    assert sink.cleanup_temp() == 1
    assert list(fake.files) == [f"{PATH}/keep.json"]
    missing = HdfsSink("hdfs://nn:8020/f1/none", Settings(), session=sink._session)
    assert missing.cleanup_temp() == 0


@pytest.mark.parametrize("name", ["", "../x", "a/b", ".hidden", "_tmp.x"])
def test_rejects_bad_names(name: str) -> None:
    with pytest.raises(ValueError):
        make(FakeHttpFS()).write_bytes(name, b"x")


def test_make_sink_and_scheme_check(monkeypatch: pytest.MonkeyPatch) -> None:
    file_settings = Settings()
    check_sink_matches_lake(file_settings, "file")
    with pytest.raises(ValueError, match="hdfs://namenode:8020/f1"):
        check_sink_matches_lake(file_settings, "hdfs")
    monkeypatch.setenv("DATA_LAKE_URI", "hdfs://namenode:8020/f1")
    hdfs_settings = Settings()
    check_sink_matches_lake(hdfs_settings, "hdfs")
    with pytest.raises(ValueError):
        check_sink_matches_lake(hdfs_settings, "file")
    assert isinstance(make_sink(hdfs_settings, "hdfs", URI), HdfsSink)
    with pytest.raises(ValueError):
        make_sink(hdfs_settings, "s3", URI)
    with pytest.raises(ValueError):
        HdfsSink("file://./x", hdfs_settings)
