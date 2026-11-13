"""The what-if make targets (S15): where they run, what they may write, the RECORD gate."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="make not installed")


def _dry_run(*args: str) -> str:
    return subprocess.run(
        ["make", "-n", "--no-print-directory", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout


def test_tyre_models_are_built_on_the_host_into_models_sim() -> None:
    sample = _dry_run("sim-tyre-model-sample")
    assert f"DATA_LAKE_URI=file://{REPO_ROOT}/data/lake " in sample
    assert "python -m f1sa.sim.tyre_model --source sample" in sample and "--out" not in sample
    full = _dry_run("sim-tyre-model-full")
    assert "DATA_LAKE_URI=hdfs://namenode:8020/f1 HDFS_HTTPFS_URL=http://localhost:14000" in full
    assert "--source lake" in full and "--allow-test-season" not in full
    assert "--allow-test-season" in _dry_run("sim-tyre-model-full", "TEST_SEASON=1")


def test_only_record_writes_results_and_opens_2025() -> None:
    dev = _dry_run("whatif-consistency-full")
    assert (
        "docker compose exec -T" in dev
        and "app python experiments/whatif_consistency.py --source full" in dev
    )
    assert "--record" not in dev and "--allow-test-season" not in dev
    rec = _dry_run("whatif-consistency-full", "RECORD=1")
    assert "--record --allow-test-season" in rec
    smoke = _dry_run("whatif-consistency-sample")
    assert "--source sample" in smoke and "--record" not in smoke and "docker" not in smoke
