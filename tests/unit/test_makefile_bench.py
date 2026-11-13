"""``make bench-*`` / ``experiments-spark`` and the compose app limit (Step 10)."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]


def _make(*args: str) -> str:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    out = subprocess.run(
        ["make", "-n", "-C", str(REPO), *args], capture_output=True, text=True, check=True
    )
    return out.stdout


def test_bench_smoke_runs_both_smokes_without_recording() -> None:
    out = _make("bench-smoke")
    assert "python experiments/scalability.py --smoke" in out
    assert "python experiments/partitioning.py --smoke" in out
    assert "--record" not in out
    assert "hdfs dfs -mkdir -p /f1/bench" in out and "hdfs dfs -chown f1sa /f1/bench" in out


def _python_lines(out: str) -> list[str]:
    return [ln for ln in out.splitlines() if "python experiments/" in ln]


def test_experiments_spark_records_only_with_record_1() -> None:
    plain = _python_lines(_make("experiments-spark"))
    assert len(plain) == 2 and not any("--record" in ln for ln in plain)
    rec = _python_lines(_make("experiments-spark", "RECORD=1"))
    assert all(ln.endswith("--record") or "--record " in ln for ln in rec)
    assert "--record" not in _make("experiments-spark", "RECORD=yes")
    grid = _make("experiments-spark", "GRID_ARGS=--factors 1 4 --large-repeats 3")
    assert "--factors 1 4 --large-repeats 3" in _python_lines(grid)[0]


def test_both_scripts_share_one_new_run_id() -> None:
    scal, part = _python_lines(_make("experiments-spark"))
    rid = scal.split("--run-id ")[1].split()[0]
    assert re.fullmatch(r"bench_\d{8}T\d{6}Z", rid)
    assert f"--run-id {rid}" in part and "--resume" not in scal + part


def test_resume_continues_the_given_run() -> None:
    rid = "bench_20261113T220000Z"
    lines = _python_lines(_make("experiments-spark", "RECORD=1", f"RESUME={rid}"))
    tails = [ln[ln.index("python ") :] for ln in lines]
    assert tails == [
        f"python experiments/scalability.py --run-id {rid} --resume --record ",
        f"python experiments/partitioning.py --run-id {rid} --resume --record ",
    ]


def test_bench_targets_never_restart_or_rebuild_services() -> None:
    for target in ("bench-smoke", "experiments-spark", "bench-clean", "bench-dirs"):
        out = _make(target, "RECORD=1", "CONFIRM=yes")
        for word in (" up ", " restart", " down", "--build", "--force-recreate", " rm "):
            assert word not in out, f"{target} runs {word!r}"
        for protected in ("/f1/raw", "/f1/clean", "/f1/features"):
            assert protected not in out


def test_app_container_has_6_gib_for_the_benchmarks() -> None:
    doc: dict[str, Any] = yaml.safe_load((REPO / "docker-compose.yml").read_text())
    assert doc["services"]["app"]["mem_limit"] == "6144m"
    assert doc["services"]["api"]["mem_limit"] == "2048m"


def test_data_bench_is_gitignored() -> None:
    out = subprocess.run(
        ["git", "-C", str(REPO), "check-ignore", "-q", "data/bench/x/scalability.csv"],
        check=False,
    )
    assert out.returncode == 0
