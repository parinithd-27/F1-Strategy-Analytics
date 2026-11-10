"""Tests for the ingestion manifest: persistence, atomicity, kill-safety, locking, resume policy."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from f1sa.ingest import manifest as mf
from f1sa.ingest.manifest import (
    MAX_ERROR_CHARS,
    FileRecord,
    Manifest,
    ManifestLockedError,
    item_key,
)

PATH = Path("data/manifest.json")


def test_item_keys() -> None:
    assert item_key("hdfs", "jolpica", 2021, 5) == "hdfs:jolpica:2021:5"
    assert item_key("file", "jolpica", 2021, None) == "file:jolpica:2021:season"


def test_round_trip_and_transitions() -> None:
    m = Manifest(PATH)
    item = m.get("file", "jolpica", 2021, 5)
    m.mark_running(item)
    assert item.status == "running" and item.attempts == 1
    m.mark_done(item, [FileRecord("file://x/a.json", 3, 10, "ab" * 32)])
    m.mark_failed(m.get("file", "jolpica", 2021, 6), RuntimeError("boom"))
    m.mark_skipped(m.get("file", "jolpica", 2021, 7), "future")

    again = Manifest(PATH)
    done = again.peek("file", "jolpica", 2021, 5)
    assert done is not None and done.status == "done" and done.files[0].rows == 3
    failed = again.peek("file", "jolpica", 2021, 6)
    assert failed is not None and failed.error == "RuntimeError: boom"
    assert again.counts() == {"done": 1, "failed": 1, "skipped": 1}
    raw = json.loads(PATH.read_text())
    assert raw["version"] == 1 and "updated_at" in raw


def test_should_run_policy() -> None:
    m = Manifest(PATH)
    states = {}
    for status in ("done", "failed", "running", "interrupted", "skipped", "pending"):
        it = m.get("file", "jolpica", 2021, len(states) + 1)
        it.status = status  # type: ignore[assignment]
        states[status] = it
    assert Manifest.should_run(None, only_failed=False) is True
    assert Manifest.should_run(None, only_failed=True) is False
    assert not Manifest.should_run(states["done"], only_failed=False)
    for status in ("failed", "running", "interrupted", "skipped", "pending"):
        assert Manifest.should_run(states[status], only_failed=False)
    assert Manifest.should_run(states["failed"], only_failed=True)
    assert not Manifest.should_run(states["interrupted"], only_failed=True)


def test_running_item_from_killed_process_is_retried() -> None:
    m = Manifest(PATH)
    m.mark_running(m.get("file", "jolpica", 2021, 1))
    # process "dies" here; a new process loads the manifest
    resumed = Manifest(PATH).peek("file", "jolpica", 2021, 1)
    assert resumed is not None and resumed.status == "running"
    assert Manifest.should_run(resumed, only_failed=False)


def test_failed_save_keeps_previous_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    m = Manifest(PATH)
    m.mark_done(m.get("file", "jolpica", 2021, 1), [])
    before = PATH.read_bytes()

    def boom(*_a: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(mf.os, "replace", boom)
    with pytest.raises(OSError):
        m.mark_failed(m.get("file", "jolpica", 2021, 2), "x")
    assert PATH.read_bytes() == before  # old version intact and parseable
    json.loads(before)
    assert not list(PATH.parent.glob(".manifest.*.tmp"))


def test_sigkill_during_writes_never_corrupts(tmp_path: Path) -> None:
    script = textwrap.dedent(
        f"""
        import os, signal, sys
        sys.path.insert(0, {str(Path(mf.__file__).parents[2])!r})
        from pathlib import Path
        from f1sa.ingest.manifest import Manifest
        m = Manifest(Path("data/manifest.json"))
        for r in range(1, 10_000):
            m.mark_done(m.get("file", "jolpica", 2021, r), [])
            if r == 200:
                os.kill(os.getpid(), signal.SIGKILL)
        """
    )
    proc = subprocess.run([sys.executable, "-c", script], cwd=os.getcwd(), check=False)
    assert proc.returncode == -9
    loaded = Manifest(PATH)  # must parse
    assert len(loaded.items) >= 199


def test_stale_temp_files_are_ignored_and_removed() -> None:
    m = Manifest(PATH)
    m.mark_done(m.get("file", "jolpica", 2021, 1), [])
    stale = PATH.parent / ".manifest.dead.tmp"
    stale.write_text("{ torn")
    with Manifest(PATH) as locked:
        assert locked.peek("file", "jolpica", 2021, 1) is not None
    assert not stale.exists()


def test_lock_blocks_second_instance() -> None:
    with Manifest(PATH), pytest.raises(ManifestLockedError):
        Manifest(PATH).__enter__()
    with Manifest(PATH):  # released after exit
        pass


def test_reload_under_lock_sees_other_process_changes() -> None:
    stale = Manifest(PATH)
    other = Manifest(PATH)
    other.mark_done(other.get("file", "jolpica", 2021, 3), [])
    with stale:
        item = stale.peek("file", "jolpica", 2021, 3)
        assert item is not None and item.status == "done"


def test_error_text_truncated() -> None:
    m = Manifest(PATH)
    item = m.get("file", "jolpica", 2021, 1)
    m.mark_failed(item, "x" * (MAX_ERROR_CHARS + 500))
    assert item.error is not None and len(item.error) == MAX_ERROR_CHARS


def test_path_must_be_inside_data(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="inside"):
        Manifest(Path("elsewhere/manifest.json"))


@pytest.mark.parametrize(
    "content",
    [
        "not json",
        json.dumps({"version": 99, "items": {}}),
        json.dumps(
            {
                "version": 1,
                "items": {
                    "wrong:key": {"sink": "file", "source": "jolpica", "season": 2021, "round": 1}
                },
            }
        ),
        json.dumps(
            {
                "version": 1,
                "items": {
                    "file:jolpica:2021:1": {
                        "sink": "file",
                        "source": "jolpica",
                        "season": 2021,
                        "round": 1,
                        "status": "weird",
                    }
                },
            }
        ),
    ],
)
def test_corrupt_manifest_rejected(content: str) -> None:
    PATH.parent.mkdir(parents=True, exist_ok=True)
    PATH.write_text(content)
    with pytest.raises(ValueError):
        Manifest(PATH)
