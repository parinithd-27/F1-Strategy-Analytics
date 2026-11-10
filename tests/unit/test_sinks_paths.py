"""Tests for f1sa.lake.paths and f1sa.ingest.sinks."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from f1sa.config import Settings
from f1sa.ingest.sinks import MANIFEST_NAME, FileSink
from f1sa.lake.paths import local_path, raw_partition_uri


def test_raw_partition_uri_layout() -> None:
    uri = raw_partition_uri(Settings(), "jolpica", 2023, 5)
    assert uri == "file://./data/lake/raw/jolpica/season=2023/round=5"


def test_raw_partition_uri_uses_settings_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_LAKE_URI", "hdfs://namenode:8020/f1")
    assert raw_partition_uri(Settings(), "fastf1", 2021, 22) == (
        "hdfs://namenode:8020/f1/raw/fastf1/season=2021/round=22"
    )


@pytest.mark.parametrize(
    ("source", "season", "round_"),
    [("ergast", 2023, 5), ("jolpica", 1949, 1), ("jolpica", 2023, 0)],
)
def test_raw_partition_uri_validation(source: str, season: int, round_: int) -> None:
    with pytest.raises(ValueError):
        raw_partition_uri(Settings(), source, season, round_)


def test_local_path() -> None:
    assert local_path("file://./data/lake") == Path("data/lake")
    assert local_path("file:///abs/lake") == Path("/abs/lake")
    for bad in ("hdfs://nn:8020/x", "file://"):
        with pytest.raises(ValueError):
            local_path(bad)


def test_file_sink_writes_atomically_with_manifest(tmp_path: Path) -> None:
    sink = FileSink(f"file://{tmp_path}/raw/jolpica/season=2023/round=5")
    pages = [{"MRData": {"total": "1"}}]
    written = sink.write_json("results.json", pages, rows=1)
    df = pd.DataFrame({"a": [1, 2, 3]})
    sink.write_parquet("laps.parquet", df)
    sink.write_manifest({"source": "jolpica", "season": 2023, "round": 5})

    part = tmp_path / "raw/jolpica/season=2023/round=5"
    assert json.loads((part / "results.json").read_text()) == pages
    assert pd.read_parquet(part / "laps.parquet").equals(df)
    assert written.sha256 == hashlib.sha256((part / "results.json").read_bytes()).hexdigest()
    manifest = json.loads((part / MANIFEST_NAME).read_text())
    assert manifest["source"] == "jolpica" and manifest["partition"].endswith("round=5")
    assert [f["name"] for f in manifest["files"]] == ["laps.parquet", "results.json"]
    assert {f["name"]: f["rows"] for f in manifest["files"]} == {
        "laps.parquet": 3,
        "results.json": 1,
    }
    assert not list(part.glob(".*.tmp"))  # no temp files left behind
    assert all((part / f).stat().st_mode & 0o777 == 0o644 for f in ("results.json", MANIFEST_NAME))


def test_file_sink_overwrite_replaces_entry(tmp_path: Path) -> None:
    sink = FileSink(f"file://{tmp_path}/p")
    sink.write_json("a.json", [1])
    sink.write_json("a.json", [1, 2])
    assert len(sink.files) == 1
    assert json.loads((tmp_path / "p/a.json").read_text()) == [1, 2]


def test_file_sink_failed_write_leaves_no_file(tmp_path: Path) -> None:
    sink = FileSink(f"file://{tmp_path}/p")

    class Unserialisable:
        def __str__(self) -> str:
            raise RuntimeError("cannot serialise")

    with pytest.raises(RuntimeError):
        sink.write_json("bad.json", {"x": Unserialisable()})
    assert not (tmp_path / "p/bad.json").exists()


@pytest.mark.parametrize("name", ["", "../x.json", "a/b.json", ".hidden"])
def test_file_sink_rejects_bad_names(tmp_path: Path, name: str) -> None:
    with pytest.raises(ValueError):
        FileSink(f"file://{tmp_path}/p").write_json(name, {})
