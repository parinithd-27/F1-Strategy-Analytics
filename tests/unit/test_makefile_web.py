"""The dashboard's compose service, Dockerfile and make targets (Step 14)."""

from __future__ import annotations

import json
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
    result = subprocess.run(
        ["make", "-n", "--no-print-directory", "-C", str(REPO), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _web() -> dict[str, Any]:
    doc: dict[str, Any] = yaml.safe_load((REPO / "docker-compose.yml").read_text())
    service: dict[str, Any] = doc["services"]["web"]
    return service


def test_web_service_is_hardened_and_local_only() -> None:
    web = _web()
    assert set(web["profiles"]) == {"core", "full"}
    assert web["image"] == "f1sa-web:${F1SA_IMAGE_TAG:-dev}"
    assert web["build"]["context"] == "./web"
    assert web["build"]["dockerfile"] == "../docker/web/Dockerfile"
    assert web["ports"] == ["127.0.0.1:${WEB_HOST_PORT:-3000}:3000"]
    assert web["read_only"] is True
    assert web["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in web["security_opt"]
    assert web["mem_limit"] == "512m"
    assert web["depends_on"]["api"]["condition"] == "service_healthy"
    assert "healthcheck" in web
    assert "volumes" not in web, "the dashboard mounts nothing from the host"


def test_dockerfile_is_multi_stage_non_root_and_pinned() -> None:
    text = (REPO / "docker" / "web" / "Dockerfile").read_text()
    assert "ARG NODE_IMAGE=node:26.13.0-bookworm-slim" in text
    assert text.count("FROM ${NODE_IMAGE} AS") == 3
    assert "npm ci" in text and "USER node" in text
    assert "NEXT_TELEMETRY_DISABLED=1" in text and "NEXT_OUTPUT=standalone" in text
    assert text.rstrip().endswith('CMD ["node", "server.js"]')


def test_versions_are_pinned_exactly() -> None:
    pkg = json.loads((REPO / "web" / "package.json").read_text())
    for deps in (pkg["dependencies"], pkg["devDependencies"]):
        for name, version in deps.items():
            assert version[0].isdigit(), f"{name} is not pinned exactly: {version}"
    assert pkg["dependencies"]["next"] == "16.4.5"
    assert pkg["dependencies"]["react"] == pkg["dependencies"]["react-dom"] == "19.3.1"
    assert (REPO / "web" / ".nvmrc").read_text().strip() == "26.13.0"
    assert "save-exact=true" in (REPO / "web" / ".npmrc").read_text()
    lock = json.loads((REPO / "web" / "package-lock.json").read_text())
    assert lock["packages"]["node_modules/next"]["version"] == "16.4.5"


def test_make_web_targets() -> None:
    dev = _make("web")
    assert "node web/scripts/check-port.mjs 127.0.0.1 3000" in dev
    assert "NEXT_TELEMETRY_DISABLED=1" in dev and "npm run dev" in dev
    assert "npm run lint && npm run typecheck && npm test" in _make("web-test")
    e2e = _make("web-e2e")
    assert "npx playwright install chromium" in e2e and "npm run test:e2e" in e2e


def test_make_up_waits_for_web() -> None:
    lines = [ln for ln in _make("up").splitlines() if "wait_for.sh" in ln]
    assert lines and lines[0].rstrip().endswith("kafka redis app api web")


def test_ci_has_a_web_job() -> None:
    ci = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text())
    steps = [s.get("run", "") for s in ci["jobs"]["web"]["steps"]]
    for cmd in ("npm ci --no-audit --no-fund", "npm run lint", "npm run typecheck", "npm test"):
        assert cmd in steps
