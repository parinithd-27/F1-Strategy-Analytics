"""Clone-and-run documentation of Step 17: README, docs/reproduce.md and CITATION.cff."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
TEAM = ("Parinith D.", "Dakshinesh K.", "Rohan Deore", "Kaushal V.")


def _text(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


def _makefile_targets_writing_results() -> set[str]:
    """Targets whose recipe takes RECORD (writes results/ only with RECORD=1)."""
    targets: set[str] = set()
    current = None
    for line in _text("Makefile").splitlines():
        m = re.match(r"^([a-z][a-z0-9-]*):(?!=)", line)
        if m:
            current = m.group(1)
            if "RECORD=1" in line:
                targets.add(current)
        elif line.startswith("\t") and current and re.search(r"_RECORD\)|RECORD\)", line):
            targets.add(current)
    return targets


def test_citation_cff_names_the_team_and_licence() -> None:
    doc = yaml.safe_load(_text("CITATION.cff"))
    assert doc["cff-version"] == "1.2.0" and doc["type"] == "software"
    assert doc["license"] == "MIT"
    names = [f"{a['given-names']} {a['family-names']}" for a in doc["authors"]]
    assert names == list(TEAM)
    assert all("email" not in a for a in doc["authors"])
    assert doc["repository-code"].startswith("https://github.com/")


def test_readme_has_the_clone_and_run_essentials() -> None:
    readme = _text("README.md")
    for needle in (
        "```mermaid",
        "make up",
        "make sample-data",
        "make demo",
        "SYNTHETIC DEMO DATA",
        "Windows 11 + WSL 2",
        "Linux / macOS",
        ".wslconfig",
        "docs/reproduce.md",
        "DATA_LICENSES.md",
        "CITATION.cff",
        "DA231o",
        "Data Wranglers",
        "## Limitations",
        "## Key results",
        "0.670 [0.421, 0.898]",  # copied from docs/model_results.md
    ):
        assert needle in readme, needle
    for name in TEAM:
        assert name in readme, name
    for part in (
        "HDFS",
        "RDD",
        "MLlib",
        "Kafka",
        "Structured Streaming",
        "Redis",
        "FastAPI",
        "Next.js",
    ):
        assert part in readme.split("```mermaid", 1)[1].split("```", 1)[0], part


def test_reproduce_lists_every_record_target_and_the_integration_tests() -> None:
    doc = _text("docs/reproduce.md")
    targets = _makefile_targets_writing_results()
    assert {
        "ablation-full",
        "stream-latency-suite",
        "redis-latency",
        "experiments-spark",
    } <= targets
    for t in sorted(targets):
        assert f"make {t}" in doc, t
    overwriting = (
        "features-sample model-eval-sample model-dev-full model-test-full model-test-summary "
        "stream-parity-full report-full etl-ti-full demo"
    )
    for t in overwriting.split():
        assert f"make {t}" in doc, t
    assert "tests/integration" in doc and "make test-int" in doc
    assert "refuses" in doc and "overwrites, no guard" in doc
