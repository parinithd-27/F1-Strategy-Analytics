"""``f1sa.model.test_summary`` (``make model-test-summary``) on invented fixture CSVs in tmp_path.

The fixtures have the layout of the committed ``model_eval`` files (stamp line, every model and
metric, validation and test splits, race-cluster and attempt intervals) with made-up numbers.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from f1sa.model import test_summary as T

REPO_ROOT = Path(__file__).resolve().parents[2]
ALL_MODELS = ("lr", "lr_calibrated", "gbt", "gbt_calibrated", "majority", "rule")
HEADER = (
    "source,smoke_test,feature_set,g,k,model,split,metric,value,ci_kind,ci_low,ci_high,"
    "n_valid,n_attempts,n_races,note,decision_threshold"
)
N = {("3.5", "2"): ("49", "19"), ("5.0", "3"): ("60", "20")}
DOC = (
    "# Model results\n\nIntro kept as is.\n\n## Results\n\nold tables\n\n"
    "## Does the primary model beat the rule baseline?\n\nprose kept as is.\n"
)


def _num(*key: str) -> float:
    h = hashlib.sha256("|".join(key).encode()).digest()
    return round(0.1 + 0.8 * h[0] / 255, 6)


def _eval_csv(fs: str, g: str, k: str, *, stamp: bool = True) -> str:
    n_att, n_races = N[(g, k)]
    lines = (["# a stamp line"] if stamp else []) + [HEADER]
    for split in ("validation", "test"):
        for model in ALL_MODELS:
            owner = "any" if model in ("majority", "rule") else fs  # baselines: same in every set
            for metric in T.METRICS:
                v = _num(owner, g, k, split, model, metric)
                for kind in ("race_cluster", "attempt"):
                    lo, hi = (round(v - 0.05, 6), round(v + 0.05, 6))
                    if kind == "attempt":
                        lo, hi = 0.0, 1.0
                    note = '"a, quoted note"' if metric == "ece" else ""
                    lines.append(
                        f"full,false,{fs},{g},{k},{model},{split},{metric},{v:.6f},{kind},"
                        f"{lo:.6f},{hi:.6f},1000,{n_att},{n_races},{note},0.5"
                    )
    return "\n".join(lines) + "\n"


def _write_inputs(root: Path) -> Path:
    results = root / "results"
    results.mkdir(parents=True, exist_ok=True)
    for g, k in T.SETTINGS:
        for fs in T.SETS:
            T.eval_file(results, fs, g, k).write_text(_eval_csv(fs, g, k), encoding="utf-8")
    return results


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    """tmp_path (already the cwd) with the six fixture inputs and a doc to fill."""
    _write_inputs(tmp_path)
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "model_results.md").write_text(DOC, encoding="utf-8")
    return tmp_path


def _no_check(_paths: Sequence[Path]) -> None:
    return None


# ------------------------------------------------------------------ inputs
def test_eval_file_names() -> None:
    r = Path("results")
    assert T.eval_file(r, "real", "3.5", "2") == r / "model_eval_G3.5_k2.csv"
    assert T.eval_file(r, "real_tyre", "5.0", "3") == r / "model_eval_real_tyre_G5.0_k3.csv"
    assert T.eval_file(r, "real_ti", "3.5", "2") == r / "model_eval_real_ti_G3.5_k2.csv"
    with pytest.raises(T.SummaryError, match="unknown feature set"):
        T.eval_file(r, "with_synthetic", "3.5", "2")


def test_input_files_are_the_six_test_runs_in_a_fixed_order() -> None:
    names = [p.name for p in T.input_files()]
    assert names == [
        "model_eval_G3.5_k2.csv",
        "model_eval_real_tyre_G3.5_k2.csv",
        "model_eval_real_ti_G3.5_k2.csv",
        "model_eval_G5.0_k3.csv",
        "model_eval_real_tyre_G5.0_k3.csv",
        "model_eval_real_ti_G5.0_k3.csv",
    ]
    assert all(p.parent == T.RESULTS_DIR for p in T.input_files())


def test_read_test_rows_keeps_only_test_race_cluster_rows(workdir: Path) -> None:
    rows = T.read_test_rows(T.eval_file(Path("results"), "real", "3.5", "2"))
    assert len(rows) == len(ALL_MODELS) * len(T.METRICS)
    assert {r["split"] for r in rows.values()} == {"test"}
    assert {r["ci_kind"] for r in rows.values()} == {"race_cluster"}
    assert rows[("rule", "ece")]["note"] == "a, quoted note"  # quoted commas survive


def test_read_test_rows_accepts_a_file_without_stamp(tmp_path: Path) -> None:
    path = tmp_path / "e.csv"
    path.write_text(_eval_csv("real", "3.5", "2", stamp=False))
    assert len(T.read_test_rows(path)) == len(ALL_MODELS) * len(T.METRICS)


def test_read_test_rows_failures(tmp_path: Path) -> None:
    with pytest.raises(T.SummaryError, match="missing input"):
        T.read_test_rows(tmp_path / "absent.csv")
    bad = tmp_path / "bad.csv"
    bad.write_text("model,split\nlr,test\n")
    with pytest.raises(T.SummaryError, match="header lacks"):
        T.read_test_rows(bad)
    text = _eval_csv("real", "3.5", "2")
    dup = tmp_path / "dup.csv"
    line = next(x for x in text.splitlines() if ",test,pr_auc," in x and "race_cluster" in x)
    dup.write_text(text + line + "\n")
    with pytest.raises(T.SummaryError, match="duplicate test row"):
        T.read_test_rows(dup)
    short = tmp_path / "short.csv"
    short.write_text("\n".join(x for x in text.splitlines() if ",rule,test,brier," not in x))
    with pytest.raises(
        T.SummaryError, match=r"no race_cluster test rows for \[\('rule', 'brier'\)\]"
    ):
        T.read_test_rows(short)


def test_check_baselines_requires_identical_values_and_intervals(workdir: Path) -> None:
    per_set = {fs: T.read_test_rows(T.eval_file(Path("results"), fs, "3.5", "2")) for fs in T.SETS}
    T.check_baselines(per_set, "3.5", "2")
    changed = dict(per_set["real_ti"])
    changed[("rule", "pr_auc")] = {**changed[("rule", "pr_auc")], "ci_high": "0.999999"}
    with pytest.raises(
        T.SummaryError, match=r"rule pr_auc differs across feature sets at G=3\.5 k=2"
    ):
        T.check_baselines({**per_set, "real_ti": changed}, "3.5", "2")


# ------------------------------------------------------------------ rows
def test_summary_row_copies_values_verbatim(workdir: Path) -> None:
    rows = T.read_test_rows(T.eval_file(Path("results"), "real", "3.5", "2"))
    out = T.summary_row(rows, "lr_calibrated", "calibrated LR", "real", "3.5", "2", "src.csv")
    assert set(out) == set(T.COLUMNS)
    assert out["primary"] == "true" and out["ci_kind"] == "race_cluster"
    assert out["ece10"] == rows[("lr_calibrated", "ece")]["value"]
    assert out["ece5_ci_high"] == rows[("lr_calibrated", "ece_5bin")]["ci_high"]
    assert (out["n_attempts"], out["n_races"]) == ("49", "19")
    other = T.summary_row(rows, "lr_calibrated", "calibrated LR", "real", "5.0", "3", "src.csv")
    assert other["primary"] == "false"  # only G=3.5, k=2 is primary


def test_summary_rows_order_primary_and_baselines(workdir: Path) -> None:
    rows = T.summary_rows(Path("results"))
    assert len(rows) == 2 * (len(T.SETS) * len(T.MODELS) + len(T.BASELINES)) == 16
    assert [r["primary"] for r in rows].count("true") == 1
    assert (rows[0]["feature_set"], rows[0]["model"], rows[0]["primary"]) == (
        "real",
        "lr_calibrated",
        "true",
    )
    for g, k in T.SETTINGS:
        sel = [r for r in rows if (r["g"], r["k"]) == (g, k)]
        assert [(r["feature_set"], r["model"]) for r in sel] == [
            *((fs, m) for fs in T.SETS for m, _ in T.MODELS),
            ("(any)", "majority"),
            ("(any)", "rule"),
        ]
        assert sel[-1]["source_file"].count("results/model_eval") == 3


# ------------------------------------------------------------------ rendering
def test_render_csv_is_stamped_and_deterministic(workdir: Path) -> None:
    rows = T.summary_rows(Path("results"))
    text = T.render_csv(rows)
    lines = text.splitlines()
    assert lines[0] == T.CSV_STAMP and lines[1] == ",".join(T.COLUMNS)
    assert len(lines) == 2 + 16 and text.endswith("\n") and "\r" not in text
    assert T.render_csv(T.summary_rows(Path("results"))) == text


def test_render_tables_marks_the_primary_and_shows_undefined() -> None:
    def row(fs: str, model: str, primary: str, g: str = "3.5", k: str = "2") -> dict[str, str]:
        r = {c: "0.5" for c in T.COLUMNS}
        r.update(
            g=g, k=k, feature_set=fs, model=model, label=model, primary=primary,
            n_attempts="49", n_races="19",
        )  # fmt: skip
        return r

    rows = [row("real", "lr_calibrated", "true"), {**row("(any)", "rule", "false"), "roc_auc": ""}]
    rows += [row("real", "lr_calibrated", "false", "5.0", "3")]
    md = T.render_tables(rows)
    assert "**G=3.5, k=2 (primary setting): 49 attempts, 19 races**" in md
    assert "**G=5.0, k=3 (sensitivity setting)" in md
    assert "| `real` | **lr_calibrated (PRIMARY)** | 0.500 | **0.500 [0.500, 0.500]** |" in md
    assert "| — | rule | 0.500 | 0.500 [0.500, 0.500] | undefined |" in md
    assert md.endswith("|\n")
    with pytest.raises(T.SummaryError, match=r"no rows for G=5\.0 k=3"):
        T.render_tables(rows[:2])


def test_replace_results_section_touches_only_that_section() -> None:
    out = T.replace_results_section(DOC, "NEW\n")
    assert out == DOC.replace("old tables\n", "NEW\n")
    assert T.replace_results_section(out, "NEW\n") == out  # idempotent
    at_end = "# T\n\n## Results\n\nold\n"
    assert T.replace_results_section(at_end, "NEW\n") == "# T\n\n## Results\n\nNEW\n"
    for bad in ("# no results heading\n", DOC + "\n## Results\n\nagain\n"):
        with pytest.raises(T.SummaryError, match="exactly one '## Results'"):
            T.replace_results_section(bad, "NEW\n")


# ------------------------------------------------------------------ committed check
def _runner(*results: tuple[int, str, str]) -> Any:
    calls: list[list[str]] = []
    queue = list(results)

    def run(cmd: list[str], **_kw: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        code, out, err = queue.pop(0)
        return subprocess.CompletedProcess(cmd, code, out, err)

    run.calls = calls  # type: ignore[attr-defined]
    return run


def test_check_committed() -> None:
    paths = [Path("results/a.csv"), Path("results/b.csv")]
    ok = _runner((0, "", ""), (0, "", ""))
    T.check_committed(paths, run=ok)
    assert ok.calls[0][:3] == ["git", "ls-files", "--error-unmatch"]
    assert ok.calls[1][:4] == ["git", "diff", "--name-only", "HEAD"]
    assert ok.calls[0][-2:] == ["results/a.csv", "results/b.csv"]
    with pytest.raises(T.SummaryError, match="untracked"):
        T.check_committed(
            paths, run=_runner((1, "", "error: pathspec 'results/b.csv'"), (0, "", ""))
        )
    with pytest.raises(T.SummaryError, match=r"differ from HEAD: \['results/a.csv'\]"):
        T.check_committed(paths, run=_runner((0, "", ""), (0, "results/a.csv\n", "")))
    with pytest.raises(T.SummaryError, match="git diff failed"):
        T.check_committed(paths, run=_runner((0, "", ""), (128, "", "fatal")))

    def no_git(*_a: Any, **_k: Any) -> subprocess.CompletedProcess[str]:
        raise FileNotFoundError("git")

    with pytest.raises(T.SummaryError, match="git is needed"):
        T.check_committed(paths, run=no_git)


def test_check_committed_against_a_real_repository(tmp_path: Path) -> None:
    if shutil.which("git") is None:
        pytest.skip("git not installed")
    env = ["-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]

    def git(*args: str) -> None:
        subprocess.run(["git", *env, *args], cwd=tmp_path, check=True, capture_output=True)

    git("init", "-q")
    f = tmp_path / "x.csv"
    f.write_text("1\n")
    with pytest.raises(T.SummaryError, match="untracked"):
        T.check_committed([Path("x.csv")])
    git("add", "x.csv")
    git("commit", "-q", "-m", "x")
    T.check_committed([Path("x.csv")])
    f.write_text("2\n")
    with pytest.raises(T.SummaryError, match="differ from HEAD"):
        T.check_committed([Path("x.csv")])


# ------------------------------------------------------------------ writing
def test_write_atomic(tmp_path: Path) -> None:
    path = tmp_path / "d" / "f.txt"
    T.write_atomic(path, "a\nb\n")
    assert path.read_bytes() == b"a\nb\n"
    assert sorted(p.name for p in path.parent.iterdir()) == ["f.txt"]


def test_run_summary_writes_both_outputs_deterministically(workdir: Path) -> None:
    seen: list[Sequence[Path]] = []
    rows = T.run_summary(check=seen.append)
    assert seen == [T.input_files()] and len(rows) == 16
    csv_bytes = T.SUMMARY_FILE.read_bytes()
    doc = T.DOC_FILE.read_text()
    assert doc.startswith("# Model results\n\nIntro kept as is.\n\n## Results\n\n**G=3.5, k=2")
    assert doc.endswith("## Does the primary model beat the rule baseline?\n\nprose kept as is.\n")
    assert "old tables" not in doc
    inputs_before = {p: p.read_bytes() for p in T.input_files()}
    T.run_summary(check=_no_check)
    assert T.SUMMARY_FILE.read_bytes() == csv_bytes and T.DOC_FILE.read_text() == doc
    assert {p: p.read_bytes() for p in T.input_files()} == inputs_before
    leftovers = [p.name for d in ("results", "docs") for p in Path(d).iterdir() if ".tmp" in p.name]
    assert leftovers == []


def test_run_summary_refuses_before_writing(workdir: Path) -> None:
    def refuse(_paths: Sequence[Path]) -> None:
        raise T.SummaryError("inputs differ from HEAD: ['x']")

    with pytest.raises(T.SummaryError, match="differ from HEAD"):
        T.run_summary(check=refuse)
    assert not T.SUMMARY_FILE.exists() and T.DOC_FILE.read_text() == DOC
    T.eval_file(Path("results"), "real_ti", "5.0", "3").unlink()
    with pytest.raises(T.SummaryError, match="missing inputs"):
        T.run_summary(check=_no_check)
    assert not T.SUMMARY_FILE.exists()


def test_main(workdir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def refuse(_paths: Sequence[Path]) -> None:
        raise T.SummaryError("inputs must be committed")

    assert T.main([], check=refuse) == 2
    assert "inputs must be committed" in capsys.readouterr().err
    assert not T.SUMMARY_FILE.exists()
    assert T.main([], check=_no_check) == 0
    assert "16 rows" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        T.main(["--unknown"], check=_no_check)


# ------------------------------------------------------------------ the committed outputs
def test_committed_outputs_match_the_committed_inputs() -> None:
    """The repo's summary CSV and doc tables are what the committed model_eval files give."""
    results, doc = REPO_ROOT / "results", REPO_ROOT / "docs" / "model_results.md"
    summary = REPO_ROOT / T.SUMMARY_FILE
    if not (summary.is_file() and doc.is_file()):
        pytest.skip("summary outputs not present")
    rows = T.summary_rows(results)
    assert T.render_csv(rows) == summary.read_text(encoding="utf-8")
    text = doc.read_text(encoding="utf-8")
    assert T.replace_results_section(text, T.render_tables(rows)) == text


def test_make_target_runs_the_summary_script() -> None:
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    out = subprocess.run(
        ["make", "-n", "--no-print-directory", "-C", str(REPO_ROOT), "model-test-summary"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout.strip()
    lines = out.splitlines()  # $(UV) may expand to a full path to uv (set by `uv run`)
    assert len(lines) == 1 and lines[0].endswith("uv run python experiments/model_test_summary.py")
    assert "--allow-test-season" not in out and "model_eval.py" not in out


# ------------------------------------------------------------------ Step 16 sections
STEP16_HEADINGS = (
    "## Paired ablation intervals (Step 16)\n",
    "## ORACLE variant: development data only (Step 16)\n",
)


def _section(doc: str, heading: str) -> str:
    start = doc.index(heading)
    nxt = doc.find("\n## ", start + len(heading))
    return doc[start:] if nxt == -1 else doc[start : nxt + 1]


def test_step16_sections_sit_outside_results_and_survive_the_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``make model-test-summary`` (``run_summary``, which the target runs) rewrites only the body
    of "## Results": the Step 16 sections of the real doc stay byte-identical. The run uses a copy
    of the doc and invented inputs in tmp_path, so nothing in the repo is written."""
    real_doc = (REPO_ROOT / "docs" / "model_results.md").read_text(encoding="utf-8")
    results_at = real_doc.index(T.RESULTS_HEADING)
    end_of_results = real_doc.find("\n## ", results_at + len(T.RESULTS_HEADING))
    before = {h: _section(real_doc, h) for h in STEP16_HEADINGS}
    for h in STEP16_HEADINGS:
        assert real_doc.index(h) > end_of_results, f"{h.strip()} must come after ## Results"
    monkeypatch.chdir(tmp_path)
    _write_inputs(tmp_path)
    doc = tmp_path / "docs" / "model_results.md"
    doc.parent.mkdir()
    doc.write_text(real_doc, encoding="utf-8")
    T.run_summary(check=lambda paths: None)
    after = doc.read_text(encoding="utf-8")
    assert after != real_doc  # the invented inputs changed the Results tables ...
    for h in STEP16_HEADINGS:  # ... and nothing else
        assert _section(after, h) == before[h]
    assert after[: after.index(T.RESULTS_HEADING)] == real_doc[:results_at]
    tail_after = after[after.find("\n## ", after.index(T.RESULTS_HEADING) + 1) :]
    assert tail_after == real_doc[end_of_results:]
