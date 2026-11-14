"""``.github/workflows/ci.yml`` (Step 17): the jobs, steps, caches and time limits."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REPO = Path(__file__).resolve().parents[2]


def _workflow() -> dict[str, Any]:
    doc: dict[str, Any] = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text())
    return doc


def _runs(job: dict[str, Any]) -> list[str]:
    return [str(s.get("run", "")) for s in job["steps"]]


def _uses(job: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(s["uses"]).split("@")[0]: s.get("with", {}) for s in job["steps"] if "uses" in s}


def test_three_parallel_jobs_each_under_30_minutes() -> None:
    jobs = _workflow()["jobs"]
    assert set(jobs) == {"python", "web", "e2e"}
    for name, job in jobs.items():
        assert 0 < job["timeout-minutes"] <= 30, name
        assert "needs" not in job, f"{name}: jobs run in parallel to keep the wall time short"
    assert _workflow()["permissions"] == {"contents": "read"}


def test_python_job_checks() -> None:
    job = _workflow()["jobs"]["python"]
    runs = _runs(job)
    for cmd in (
        "uv sync --all-extras --locked",
        "uv run ruff check src tests experiments scripts",
        "uv run ruff format --check src tests experiments scripts",
        "uv run mypy src scripts/check_traceability.py",
        "uv run python scripts/check_traceability.py",
        "uv run pytest tests/unit -q",
    ):
        assert cmd in runs, cmd
    uses = _uses(job)
    assert uses["astral-sh/setup-uv"]["enable-cache"] is True
    assert uses["actions/setup-java"]["java-version"] == "17"


def test_web_job_checks_with_npm_cache() -> None:
    job = _workflow()["jobs"]["web"]
    runs = _runs(job)
    for cmd in ("npm ci --no-audit --no-fund", "npm run lint", "npm run typecheck", "npm test"):
        assert cmd in runs, cmd
    node = _uses(job)["actions/setup-node"]
    assert node["cache"] == "npm" and node["cache-dependency-path"] == "web/package-lock.json"


def test_e2e_job_runs_the_demo_streaming_smoke_and_playwright_on_the_core_profile() -> None:
    job = _workflow()["jobs"]["e2e"]
    runs = _runs(job)
    order = [
        "sudo rm -rf /usr/share/dotnet",
        "make up",
        "make sample-data",
        "make demo DEMO_WAIT=finished",
        'test -z "$(git status --porcelain -- results/)"',
        "npx playwright install --with-deps chromium",
        "npx playwright test e2e/smoke.spec.ts",
        "make down",
    ]
    idx = [next(i for i, r in enumerate(runs) if cmd in r) for cmd in order]
    assert idx == sorted(idx)
    text = "\n".join(runs)
    assert "up-full" not in text and "tests/integration" not in text and "down -v" not in text
    smoke = next(s for s in job["steps"] if "playwright test" in str(s.get("run", "")))
    assert smoke["env"]["E2E_NO_SERVER"] == "1"
    assert next(s for s in job["steps"] if s.get("run") == "make down")["if"] == "always()"
    uses = _uses(job)
    assert uses["astral-sh/setup-uv"]["enable-cache"] is True
    assert uses["actions/setup-node"]["cache"] == "npm"
