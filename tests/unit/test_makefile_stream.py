"""The streaming make targets: sample mode on the host, FULL=1 and parity in the app container."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _dry_run(*args: str) -> str:
    result = subprocess.run(
        ["make", "-n", "--no-print-directory", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return result.stdout


def test_stream_demo_sample_mode_runs_on_the_host_with_the_file_lake() -> None:
    out = _dry_run("stream-demo")
    assert f"DATA_LAKE_URI=file://{REPO_ROOT}/data/lake " in out
    assert "run python experiments/streaming_latency.py" in out
    assert "--season 2024 --round 5 --speed 10" in out
    assert "make etl-sample features-sample" in out
    assert "docker compose" not in out


def test_stream_demo_full_mode_runs_in_the_app_container() -> None:
    out = _dry_run("stream-demo", "FULL=1", "SEASON=2023", "ROUND=7", "SPEED=60")
    assert "docker compose exec -T" in out and "DATA_LAKE_URI=hdfs://namenode:8020/f1" in out
    assert "app python experiments/streaming_latency.py --season 2023 --round 7 --speed 60" in out


def test_stream_parity_full_reads_2021_to_2024_only() -> None:
    out = _dry_run("stream-parity-full")
    assert "experiments/stream_parity.py --seasons 2021-2024" in out and "2025" not in out


@pytest.mark.parametrize(
    "args", [("stream-demo",), ("stream-demo", "FULL=1"), ("stream-parity-full",)]
)
def test_streaming_targets_never_touch_services(args: tuple[str, ...]) -> None:
    out = _dry_run(*args)
    for forbidden in (" up ", "--build", " down", " restart", " stop", "prune", " rm "):
        assert forbidden not in out, (args, forbidden)
