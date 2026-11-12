"""Demo runs never overwrite the committed ``results/``: ``make stream-demo`` (via
``experiments/streaming_latency.py``) and API replays write to ``data/demo/<run_id>/``; only
``RECORD=1`` (``--record``) writes to ``results/``."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from f1sa.demo_runs import run_dir, run_id

REPO = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 11, 12, 18, 30, 5, 42000, tzinfo=UTC)
RID = "2024_5_20261112T183005.042Z"
STREAM_FILES = ("streaming_latency.csv", "stream_progress.jsonl", "stream_batches.jsonl")


def _script() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "streaming_latency_script", REPO / "experiments" / "streaming_latency.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _hashes(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


class FakeJob:
    """Stands in for the stream job: writes its two JSONL logs into ``--results-dir`` (as
    ``stream_job.main`` does) and its ready file."""

    def __init__(self, cmd: list[str]) -> None:
        self.returncode: int | None = None
        out = Path(cmd[cmd.index("--results-dir") + 1])
        out.mkdir(parents=True, exist_ok=True)
        (out / "stream_progress.jsonl").write_text('{"batchId": 0}\n')
        (out / "stream_batches.jsonl").write_text('{"batch_id": 0}\n')
        ready = Path(cmd[cmd.index("--ready-file") + 1])
        ready.parent.mkdir(parents=True, exist_ok=True)
        ready.write_text("q\n")

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float) -> int:
        self.returncode = 0
        return 0

    def terminate(self) -> None:
        self.returncode = -15


def _run(argv: list[str]) -> int:
    mod = _script()
    rows: list[dict[str, Any]] = [
        {"type": "lap", "end_to_end_ms": 100, "subscriber_received_at_ms": 1},
        {"type": "session_end"},
    ]
    return int(
        mod.main(
            argv,
            popen=FakeJob,
            run=lambda cmd: 0,
            check=lambda settings: 3,
            subscriber=lambda settings, s, r, t: rows,
            state=lambda settings, s, r: "redis: fake",
        )
    )


@pytest.fixture
def committed_results(tmp_path: Path) -> Path:
    """A ``results/`` in the test's working directory holding the committed stream files."""
    results = tmp_path / "results"
    results.mkdir()
    for name in STREAM_FILES:
        src = REPO / "results" / name
        (results / name).write_bytes(src.read_bytes() if src.exists() else b"committed\n")
    return results


def test_default_demo_run_leaves_results_unchanged(
    tmp_path: Path, committed_results: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_before = _hashes(REPO / "results")
    before = _hashes(committed_results)
    mod = _script()
    monkeypatch.setattr(mod, "_utc_now", lambda: NOW)
    rc = int(
        mod.main(
            ["--season", "2024", "--round", "5", "--speed", "60"],
            popen=FakeJob,
            run=lambda cmd: 0,
            check=lambda settings: 3,
            subscriber=lambda s, a, b, t: [{"type": "session_end"}],
            state=lambda s, a, b: "redis: fake",
        )
    )
    assert rc == 0
    assert _hashes(committed_results) == before, "a default demo run changed results/"
    assert _hashes(REPO / "results") == repo_before
    out = tmp_path / "data" / "demo" / RID
    assert sorted(p.name for p in out.iterdir()) == sorted(STREAM_FILES)


def test_record_run_writes_results(tmp_path: Path, committed_results: Path) -> None:
    before = _hashes(committed_results)
    assert _run(["--season", "2024", "--round", "5", "--speed", "60", "--record"]) == 0
    after = _hashes(committed_results)
    assert after.keys() == before.keys()
    assert after != before, "RECORD=1 must write results/"
    assert not (tmp_path / "data" / "demo").exists()


def test_demo_argv_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _script()
    base = ["--season", "2024", "--round", "5"]
    assert mod.demo_argv(base, now=lambda: NOW) == [*base, "--results-dir", f"data/demo/{RID}"]
    assert mod.demo_argv([*base, "--record"]) == [*base, "--results-dir", "results"]
    assert mod.demo_argv([*base, "--results-dir", "x"]) == [*base, "--results-dir", "x"]
    with pytest.raises(SystemExit):
        mod.demo_argv([*base, "--record", "--results-dir", "x"])
    monkeypatch.setenv("DEMO_DIR", "data/other")
    from f1sa.config import get_settings

    get_settings.cache_clear()
    assert mod.demo_argv(base, now=lambda: NOW)[-1] == f"data/other/{RID}"


def test_run_ids() -> None:
    assert run_id(2024, 5, NOW) == RID
    assert run_dir(Path("data/demo"), RID) == Path("data/demo") / RID
    for bad in ("../results", "2024_5", "2024_5_20261112T183005.042Z/../../x"):
        with pytest.raises(ValueError):
            run_dir(Path("data/demo"), bad)
    with pytest.raises(ValueError):
        run_id(2024, 5, datetime(2026, 1, 1))


def test_api_replay_writes_only_under_data_demo() -> None:
    """The replay argv sends the stream job's logs to data/demo; the producer writes no files."""
    from f1sa.api.replay import stream_job_argv

    argv = stream_job_argv(
        2024,
        5,
        checkpoint=Path("data/checkpoints/api_x"),
        ready_file=Path("data/checkpoints/api_x.ready"),
        results_dir=run_dir(Path("data/demo"), RID),
        timeout_s=10,
        allow_test_season=False,
    )
    assert argv[argv.index("--results-dir") + 1] == f"data/demo/{RID}"
    producer_src = (REPO / "src" / "f1sa" / "streaming" / "producer.py").read_text()
    for writer in ("open(", "write_text", "write_bytes", "results/"):
        assert writer not in producer_src, writer


def test_stream_demo_make_targets() -> None:
    import shutil
    import subprocess

    if shutil.which("make") is None:
        pytest.skip("make not installed")

    def dry(*args: str) -> str:
        return subprocess.run(
            ["make", "-n", "--no-print-directory", "-C", str(REPO), *args],
            capture_output=True,
            text=True,
            check=True,
        ).stdout

    assert "--record" not in dry("stream-demo")
    assert "--results-dir" not in dry("stream-demo")
    assert dry("stream-demo", "RECORD=1").rstrip().endswith("--speed 10 --record")
    assert "--record" in dry("stream-demo", "FULL=1", "RECORD=1")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
