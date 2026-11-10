"""Tests for scripts/wait_for.sh argument handling and failure paths (no running infra needed)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "wait_for.sh"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT), *args], capture_output=True, text=True, timeout=60, check=False
    )


def test_script_is_executable() -> None:
    assert SCRIPT.is_file()
    assert SCRIPT.stat().st_mode & 0o111


def test_no_services_is_usage_error() -> None:
    result = _run()
    assert result.returncode == 2
    assert "usage:" in result.stderr


@pytest.mark.parametrize("timeout", ["abc", "0", "-5", ""])
def test_bad_timeout_is_usage_error(timeout: str) -> None:
    result = _run("-t", timeout, "kafka")
    assert result.returncode == 2


def test_unknown_option_is_usage_error() -> None:
    assert _run("-x", "kafka").returncode == 2


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not installed")
def test_unknown_service_fails_fast() -> None:
    result = _run("-t", "1", "no-such-service-f1sa")
    assert result.returncode == 1
    assert "no container for service 'no-such-service-f1sa'" in result.stderr


def test_missing_docker_fails(tmp_path: Path) -> None:
    # PATH containing only bash/coreutils essentials but no docker
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for tool in ("bash", "dirname", "basename", "date", "sleep"):
        found = shutil.which(tool)
        assert found is not None
        (bindir / tool).symlink_to(found)
    result = subprocess.run(
        [str(bindir / "bash"), str(SCRIPT), "-t", "1", "kafka"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env={"PATH": str(bindir)},
    )
    assert result.returncode == 1
    assert "docker not found" in result.stderr
