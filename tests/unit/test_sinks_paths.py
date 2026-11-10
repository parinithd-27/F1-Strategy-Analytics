"""Tests for f1sa.lake.paths and f1sa.ingest.sinks."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from f1sa.config import Settings
from f1sa.ingest.sinks import MANIFEST_NAME, FileSink
from f1sa.lake.paths import hdfs_path, is_within, local_path, raw_table_uri


def test_raw_table_uri_layout() -> None:
    s = Settings()
    assert raw_table_uri(s, "jolpica", "laps", 2023, 5) == (
        "file://./data/lake/raw/jolpica/laps/season=2023/round=5"
    )
    assert raw_table_uri(s, "jolpica", "drivers", 2023) == (
        "file://./data/lake/raw/jolpica/drivers/season=2023"
    )
    assert raw_table_uri(s, "fastf1", "telemetry", 2021, 3, "VER") == (
        "file://./data/lake/raw/fastf1/telemetry/season=2021/round=3/driver=VER"
    )


def test_raw_table_uri_uses_settings_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_LAKE_URI", "hdfs://namenode:8020/f1")
    assert raw_table_uri(Settings(), "fastf1", "laps", 2021, 22) == (
        "hdfs://namenode:8020/f1/raw/fastf1/laps/season=2021/round=22"
    )


@pytest.mark.parametrize(
    ("source", "table", "season", "round_", "driver"),
    [
        ("ergast", "laps", 2023, 5, None),
        ("jolpica", "telemetry", 2023, 5, None),
        ("jolpica", "laps", 1949, 1, None),
        ("jolpica", "laps", 2023, 0, None),
        ("fastf1", "telemetry", 2023, None, "VER"),
        ("fastf1", "telemetry", 2023, 5, "ver"),
        ("fastf1", "telemetry", 2023, 5, "../x"),
    ],
)
def test_raw_table_uri_validation(
    source: str, table: str, season: int, round_: int | None, driver: str | None
) -> None:
    with pytest.raises(ValueError):
        raw_table_uri(Settings(), source, table, season, round_, driver)


def test_hdfs_path_and_is_within(tmp_path: Path) -> None:
    assert hdfs_path("hdfs://namenode:8020/f1/raw/x/") == "/f1/raw/x"
    for bad in ("file://./x", "hdfs://nn:8020", "hdfs://nn:8020/f1/../etc"):
        with pytest.raises(ValueError):
            hdfs_path(bad)
    assert is_within(Path("data/cache/x"), Path("data"))
    assert is_within(Path("data"), Path("data"))
    assert not is_within(Path("data/../elsewhere"), Path("data"))
    assert not is_within(tmp_path.parent, Path("data"))


def test_file_sink_cleanup_temp(tmp_path: Path) -> None:
    sink = FileSink(f"file://{tmp_path}/p")
    assert sink.cleanup_temp() == 0
    sink.write_json("a.json", [1])
    (tmp_path / "p/.a.json.dead.tmp").write_text("partial")
    assert sink.cleanup_temp() == 1
    assert sorted(x.name for x in (tmp_path / "p").iterdir()) == ["a.json"]
    assert sink.exists("a.json") and not sink.exists("b.json")


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


@pytest.mark.parametrize("name", ["", "../x.json", "a/b.json", ".hidden", "_tmp.x"])
def test_file_sink_rejects_bad_names(tmp_path: Path, name: str) -> None:
    with pytest.raises(ValueError):
        FileSink(f"file://{tmp_path}/p").write_json(name, {})
