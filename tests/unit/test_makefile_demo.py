"""``make demo`` and the fresh-clone fixes of Step 17 (dry runs; nothing is executed)."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
SAMPLE_MODEL = "models/undercut/sample_deploy_real_G3.5_k2"


def _make(*args: str, cwd: Path = REPO, env: dict[str, str] | None = None) -> str:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    result = subprocess.run(
        ["make", "-n", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )
    return result.stdout


def _compose() -> dict[str, Any]:
    doc: dict[str, Any] = yaml.safe_load((REPO / "docker-compose.yml").read_text())
    return doc


def _exec_lines(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith("docker compose exec")]


def test_demo_runs_the_sample_pipeline_in_the_app_container_then_the_replay() -> None:
    out = _make("demo")
    execs = _exec_lines(out)
    steps = [
        "python -m f1sa.etl.clean --source sample",
        "python -m f1sa.features.build --source sample --results-dir data/demo/demo_",
        "python experiments/model_save.py --source sample --force",
        "python -m f1sa.sim.tyre_model --source sample",
        "python -m f1sa.demo_replay --api http://api:8000 --season 2024 --round 5 --speed 60",
    ]
    positions = []
    for step in steps:
        hits = [i for i, line in enumerate(execs) if step in line]
        assert len(hits) == 1, step
        positions.append(hits[0])
    assert positions == sorted(positions)
    for line in execs:
        assert line.startswith("docker compose exec -T -e PYTHONPATH=/workspace/src")
        assert "-e DATA_LAKE_URI=file:///workspace/data/lake" in line
        assert re.search(r"-e F1SA_GIT_SHA=\S+", line)
        assert line.split(" app ", 1)[0].endswith(("false", "true"))  # F1SA_GIT_DIRTY last
    assert f"--model-version {Path(SAMPLE_MODEL).name}" in execs[-1]
    assert "--wait state" in execs[-1]
    # the api is recreated with replay on and the sample model, before the replay starts
    api = out.index(f"API_MODEL_DIR={SAMPLE_MODEL}")
    assert "ENABLE_REPLAY=1" in out and api < out.index("f1sa.demo_replay")


def test_demo_preflight_needs_the_core_stack_and_the_sample() -> None:
    out = _make("demo")
    assert "scripts/wait_for.sh -t 10 kafka redis app" in out and "run 'make up' first" in out
    assert "test -d data/sample/raw" in out and "make sample-data" in out
    assert f"test -f {SAMPLE_MODEL}/lr/scorer.json" in out
    assert "scripts/wait_for.sh -t 120 web" in out


def test_demo_never_writes_results_and_never_touches_services_or_volumes() -> None:
    out = _make("demo")
    pipeline = _make("demo-pipeline")
    assert not re.search(r"(^|[\s=/])results(/|\s|$)", pipeline), (
        "make demo must not write results/"
    )
    assert " down" not in out and " -v" not in out and "--volumes" not in out
    assert "--build" not in out.replace("--no-build", "")
    ups = [line for line in out.splitlines() if " up -d" in line]
    assert ups == ["docker compose --profile core up -d --no-deps --no-build --force-recreate api"]
    assert "/f1/" not in out and "hdfs" not in out
    assert "deploy_real_G3.5_k2 " not in out.replace("sample_deploy_real_G3.5_k2", "")


def test_demo_options() -> None:
    out = _make("demo-replay", "DEMO_WAIT=finished", "DEMO_SPEED=10", "DEMO_ROUND=6")
    assert "--round 6 --speed 10 --wait finished" in out


def test_api_up_model_dir_default_and_override() -> None:
    assert "API_MODEL_DIR=models/undercut/deploy_real_G3.5_k2 " in _make("api-up")
    assert f"API_MODEL_DIR={SAMPLE_MODEL} " in _make("api-up", f"MODEL_DIR={SAMPLE_MODEL}")


def test_bind_mount_folders_are_created_before_compose() -> None:
    out = _make("up")
    mk = "mkdir -p data/demo data/checkpoints models/undercut models/sim"
    assert mk in out and out.index(mk) < out.index("docker compose --profile core up")


def _copy_makefile(tmp_path: Path, with_env: bool) -> Path:
    shutil.copy(REPO / "Makefile", tmp_path / "Makefile")
    if with_env:
        (tmp_path / ".env").write_text("# placeholder\n")
    return tmp_path


def _app_uid(cwd: Path) -> str:
    env = {k: v for k, v in os.environ.items() if k != "APP_UID"}
    out = subprocess.run(
        ["make", "-s", "-C", str(cwd), "--eval", "show-uid:\n\t@echo uid=$$APP_UID", "show-uid"],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    ).stdout
    return out.strip()


def test_app_uid_is_exported_only_without_a_project_env(tmp_path: Path) -> None:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    fresh = tmp_path / "fresh"
    fresh.mkdir()
    _copy_makefile(fresh, False)
    assert _app_uid(fresh) == f"uid={os.getuid()}"
    configured = tmp_path / "configured"
    configured.mkdir()
    _copy_makefile(configured, True)
    assert _app_uid(configured) == "uid=", "an existing .env decides APP_UID"


def test_compose_image_tag_and_api_model_dir_defaults() -> None:
    doc = _compose()
    services = doc["services"]
    assert services["app"]["image"] == "f1sa-app:${F1SA_IMAGE_TAG:-dev}"
    assert services["web"]["image"] == "f1sa-web:${F1SA_IMAGE_TAG:-dev}"
    assert services["api"]["image"] == services["app"]["image"]
    env = services["api"]["environment"]
    assert env["API_MODEL_DIR"] == "${API_MODEL_DIR:-models/undercut/deploy_real_G3.5_k2}"
    assert "./models:/workspace/models:ro" in services["api"]["volumes"]
