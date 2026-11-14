"""``scripts/check_traceability.py`` (Step 17) on a tiny invented repository and on the real one."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "check_traceability", REPO / "scripts" / "check_traceability.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["check_traceability"] = mod
    spec.loader.exec_module(mod)
    return mod


CT = _load()

MAKEFILE = """\
.PHONY: demo
demo: demo-pipeline ## the demo
demo-pipeline:
\techo hi
VAR := x
web web-test: x
"""

FILES = {
    "src/f1sa/lake/paths.py": "def raw_uri():\n    pass\n",
    "src/f1sa/features/gaps.py": "",
    "src/f1sa/features/stints.py": "",
    "src/f1sa/serving/keys.py": "def order_key(s):\n    return s\nDRIVER_FIELDS: tuple = ()\n",
    "src/f1sa/api/routes/health.py": "",
    "src/f1sa/api/routes/races.py": "",
    "src/f1sa/config.py": "",
    "src/f1sa/etl/sql/a.sql": "",
    "tests/unit/test_keys.py": "def test_exact_fields():\n    pass\n",
    "results/run_s10.csv": "",
    "results/run_s60.csv": "",
    "results/calibration_G3.5_k2.csv": "",
    "results/stream_batches_s10.jsonl": "",
    "results/stream_progress_s10.jsonl": "",
    "web/src/components/Tower.tsx": "",
    "docker/web/Dockerfile": "",
    "docker-compose.yml": "",
}


def _repo(tmp_path: Path, rows: list[str]) -> Path:
    for rel, text in FILES.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    (tmp_path / "Makefile").write_text(MAKEFILE)
    table = ["# M", "", "| # | Commitment | Step | File or module |", "|---|---|---|---|", *rows]
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "traceability.md").write_text("\n".join(table) + "\n")
    return tmp_path


def _statuses(tmp_path: Path, cell: str, step: str = "S5") -> list[tuple[str, str]]:
    repo = _repo(tmp_path, [f"| 1 | x | {step} | {cell} |"])
    return [(f.ref, f.status) for f in CT.check(repo, Path("docs/traceability.md"), ["S18"])]


def test_paths_bases_and_the_previous_folder(tmp_path: Path) -> None:
    got = _statuses(
        tmp_path,
        "`lake/paths.py`, `features/gaps.py`, `stints.py`, `config.py`, `test_keys.py`, "
        "`docker-compose.yml`, `docker/web/Dockerfile`, `web/src/components/`",
    )
    assert got == [
        ("lake/paths.py", "OK"),
        ("features/gaps.py", "OK"),
        ("stints.py", "OK"),
        ("config.py", "OK"),
        ("test_keys.py", "OK"),
        ("docker-compose.yml", "OK"),
        ("docker/web/Dockerfile", "OK"),
        ("web/src/components/", "OK"),
    ]


def test_braces_placeholders_and_globs(tmp_path: Path) -> None:
    got = _statuses(
        tmp_path,
        "`api/routes/{health,races}.py`, `results/run_{s10,s60}.csv`, "
        "`stream_{batches,progress}_{tag}.jsonl`, `etl/sql/*.sql`, `results/calibration_*.csv`",
    )
    assert [s for _r, s in got] == ["OK"] * 5
    missing = _statuses(tmp_path / "b", "`api/routes/{health,nope}.py`")
    assert missing == [("api/routes/{health,nope}.py", "MISSING")]


def test_empty_brace_alternative() -> None:
    assert CT.expand_braces("m{,_G5}.csv") == ["m.csv", "m_G5.csv"]
    assert CT.expand_braces("s_{tag}.jsonl") == ["s_*.jsonl"]


def test_tests_attributes_and_make_targets(tmp_path: Path) -> None:
    got = _statuses(
        tmp_path,
        "`test_keys.py::test_exact_fields`, `test_keys.py::test_nope`, "
        "`serving/keys.order_key`, `serving/keys.DRIVER_FIELDS`, `serving/keys.missing_fn`, "
        "`make demo`, `make demo-pipeline FULL=1`, `make web-test`, `make nope`",
    )
    assert got == [
        ("test_keys.py::test_exact_fields", "OK"),
        ("test_keys.py::test_nope", "MISSING"),
        ("serving/keys.order_key", "OK"),
        ("serving/keys.DRIVER_FIELDS", "OK"),
        ("serving/keys.missing_fn", "MISSING"),
        ("make demo", "OK"),
        ("make demo-pipeline FULL=1", "OK"),
        ("make web-test", "OK"),
        ("make nope", "MISSING"),
    ]


def test_non_paths_are_not_checked_and_arguments_are_dropped(tmp_path: Path) -> None:
    got = _statuses(
        tmp_path,
        "`race:{s}:{r}:*`, `f1.race.events`, `--sink hdfs`, `/f1/bench`, "
        "`raw/x/season=2024/round=/`, `DRIVER_FIELDS`, `cache()`, `partitionBy(season, round)`, "
        "`.png`, `config.py --flag x`",
    )
    assert got == [("config.py", "OK")]


def test_generated_outputs_and_pending_rows_are_reported_not_failed(tmp_path: Path) -> None:
    repo = _repo(
        tmp_path,
        [
            "| 1 | x | S4 | `data/manifest.json`, `models/sim/m.json`, `data/sample/README.md` |",
            "| 2 | y | S18 | `docs/report/`, `make report` |",
            "| 3 | z | S5, S18 | `docs/also_missing.md` |",
        ],
    )
    findings = CT.check(repo, Path("docs/traceability.md"), ["S18"])
    got = [(f.row, f.ref, f.status) for f in findings]
    assert got == [
        ("1", "data/manifest.json", "GENERATED"),
        ("1", "models/sim/m.json", "GENERATED"),
        ("1", "data/sample/README.md", "MISSING"),  # the sample is committed: checked
        ("2", "docs/report/", "PENDING"),
        ("2", "make report", "PENDING"),
        ("3", "docs/also_missing.md", "MISSING"),  # a row with a done step is checked
    ]
    lines, missing = CT.report(findings)
    assert missing == 2
    assert lines[-1] == "traceability: GENERATED 2, PENDING 2, MISSING 2"
    assert any("PENDING   #2   docs/report/" in line for line in lines)


def test_main_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    ok = _repo(tmp_path / "ok", ["| 1 | x | S5 | `lake/paths.py` |"])
    assert CT.main(["--repo", str(ok)]) == 0
    assert "traceability: OK 1" in capsys.readouterr().out
    bad = _repo(tmp_path / "bad", ["| 1 | x | S5 | `lake/gone.py` |"])
    assert CT.main(["--repo", str(bad)]) == 1
    assert "MISSING   #1   lake/gone.py" in capsys.readouterr().out
    assert CT.main(["--repo", str(tmp_path / "nowhere")]) == 2


def test_the_real_matrix_resolves() -> None:
    findings = CT.check(REPO, CT.MATRIX, list(CT.DEFAULT_PENDING))
    missing = [f for f in findings if f.status == "MISSING"]
    assert missing == [], missing
    assert sum(f.status == "OK" for f in findings) > 200
