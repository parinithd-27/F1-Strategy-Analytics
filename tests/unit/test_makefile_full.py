"""The full-data make targets run in HDFS mode inside compose and stay away from raw and volumes."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGETS = ("hdfs-dirs", "etl-full", "features-full", "report-full", "pipeline-full")


def _dry_run(target: str) -> str:
    result = subprocess.run(
        ["make", "-n", "--no-print-directory", "-C", str(REPO_ROOT), target],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return result.stdout


@pytest.mark.parametrize("target", TARGETS)
def test_target_never_rebuilds_deletes_or_touches_raw(target: str) -> None:
    out = _dry_run(target)
    assert out.strip()
    for forbidden in ("--build", " down", "prune", " -rm", " rm ", "/f1/raw", "manifest.json"):
        assert forbidden not in out, (target, forbidden)


@pytest.mark.parametrize(
    ("target", "module"),
    [
        ("etl-full", "f1sa.etl.clean --source lake"),
        ("features-full", "f1sa.features.build --source lake --full-counts"),
        ("report-full", "f1sa.etl.full_report"),
    ],
)
def test_target_runs_in_app_against_hdfs(target: str, module: str) -> None:
    line = next(x for x in _dry_run(target).splitlines() if "python -m" in x)
    assert "exec -T" in line
    assert "PYTHONPATH=/workspace/src" in line
    assert "DATA_LAKE_URI=hdfs://namenode:8020/f1" in line
    assert line.endswith(f"app python -m {module}")


def test_hdfs_dirs_only_prepares_clean_and_features() -> None:
    lines = _dry_run("hdfs-dirs").splitlines()
    assert len(lines) == 2
    assert lines[0].endswith("hdfs dfs -mkdir -p /f1/clean /f1/features")
    assert lines[1].endswith("hdfs dfs -chown f1sa /f1/clean /f1/features")


def test_pipeline_runs_clean_then_features_then_report() -> None:
    out = _dry_run("pipeline-full")
    assert out.index("f1sa.etl.clean") < out.index("f1sa.features.build")
    assert out.index("f1sa.features.build") < out.index("f1sa.etl.full_report")
