"""Summary of the single 2025 test run (Step 9f) from the committed result files. No evaluation.

``python experiments/model_test_summary.py`` (``make model-test-summary``) reads the six committed
``results/model_eval{,_real_tyre,_real_ti}_G{3.5_k2,5.0_k3}.csv`` files, copies their 2025
test-split values with the race-cluster intervals, and writes:

* ``results/model_test_summary.csv``: one row per feature set x {calibrated LR, calibrated GBT} per
  setting, plus the majority and rule baselines once per setting (they do not depend on the feature
  set; this is checked), with intervals for every metric. The primary row (``real`` calibrated LR,
  G=3.5, k=2) has ``primary=true``.
* the body of the ``## Results`` section of ``docs/model_results.md`` (the two setting tables). The
  rest of the document is left byte for byte as it is.

It refuses to run (exit 2) when an input is untracked or differs from ``HEAD``, so the outputs
always come from committed files. Both outputs are written atomically and are deterministic: the
same inputs give byte-identical files.
"""

from __future__ import annotations

import argparse
import csv
import io
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path

SETS = ("real", "real_tyre", "real_ti")
SETTINGS = (("3.5", "2"), ("5.0", "3"))
PRIMARY = ("real", "lr_calibrated", ("3.5", "2"))
MODELS = (("lr_calibrated", "calibrated LR"), ("gbt_calibrated", "calibrated GBT"))
BASELINES = (("majority", "majority baseline"), ("rule", "rule baseline"))
METRICS = ("f1_positive", "pr_auc", "roc_auc", "brier", "ece", "ece_5bin")
OUT_NAME = {"ece": "ece10", "ece_5bin": "ece5"}
CI_KIND = "race_cluster"
COLUMNS = (
    "g", "k", "feature_set", "model", "label", "primary", "n_attempts", "n_races",
    "f1_positive", "f1_positive_ci_low", "f1_positive_ci_high",
    "pr_auc", "pr_auc_ci_low", "pr_auc_ci_high",
    "roc_auc", "roc_auc_ci_low", "roc_auc_ci_high",
    "brier", "brier_ci_low", "brier_ci_high",
    "ece10", "ece10_ci_low", "ece10_ci_high",
    "ece5", "ece5_ci_low", "ece5_ci_high",
    "ci_kind", "source_file",
)  # fmt: skip
CSV_STAMP = (
    "# 9f 2025 TEST SEASON summary, copied from the committed model_eval files (no new evaluation)"
)
RESULTS_DIR = Path("results")
SUMMARY_FILE = RESULTS_DIR / "model_test_summary.csv"
DOC_FILE = Path("docs/model_results.md")
RESULTS_HEADING = "## Results\n"

Row = dict[str, str]
GitRunner = Callable[..., subprocess.CompletedProcess[str]]


class SummaryError(RuntimeError):
    """An input is missing, malformed, inconsistent or not committed."""


def eval_file(results_dir: Path, feature_set: str, g: str, k: str) -> Path:
    """The committed test-run file of one feature set and setting (``real`` has no set suffix)."""
    if feature_set not in SETS:
        raise SummaryError(f"unknown feature set {feature_set!r}; expected one of {SETS}")
    mid = "" if feature_set == "real" else f"_{feature_set}"
    return results_dir / f"model_eval{mid}_G{g}_k{k}.csv"


def input_files(results_dir: Path = RESULTS_DIR) -> list[Path]:
    """The six inputs, in a fixed order."""
    return [eval_file(results_dir, fs, g, k) for g, k in SETTINGS for fs in SETS]


def read_test_rows(path: Path) -> dict[tuple[str, str], Row]:
    """``{(model, metric): row}`` for the test-split race-cluster rows of one ``model_eval`` file.
    Comment lines (``# ...`` stamps) are skipped; a duplicate or missing (model, metric) pair
    fails."""
    if not path.is_file():
        raise SummaryError(f"missing input {path}")
    with path.open(newline="", encoding="utf-8") as fh:
        lines = [x for x in fh if not x.startswith("#")]
    reader = csv.DictReader(lines)
    need = {"model", "split", "metric", "value", "ci_kind", "ci_low", "ci_high", "n_attempts"}
    if reader.fieldnames is None or not need <= set(reader.fieldnames):
        raise SummaryError(f"{path}: header lacks {sorted(need - set(reader.fieldnames or []))}")
    out: dict[tuple[str, str], Row] = {}
    for r in reader:
        if r["split"] != "test" or r["ci_kind"] != CI_KIND:
            continue
        key = (r["model"], r["metric"])
        if key in out:
            raise SummaryError(f"{path}: duplicate test row {key}")
        out[key] = r
    missing = [(m, x) for m, _ in (*MODELS, *BASELINES) for x in METRICS if (m, x) not in out]
    if missing:
        raise SummaryError(f"{path}: no {CI_KIND} test rows for {missing}")
    return out


def check_baselines(per_set: Mapping[str, Mapping[tuple[str, str], Row]], g: str, k: str) -> None:
    """The baselines must be identical (value and interval) in every feature-set file of a
    setting."""
    for model, _ in BASELINES:
        for metric in METRICS:
            seen = {
                (rows[(model, metric)]["value"], rows[(model, metric)]["ci_low"],
                 rows[(model, metric)]["ci_high"])
                for rows in per_set.values()
            }  # fmt: skip
            if len(seen) != 1:
                raise SummaryError(f"{model} {metric} differs across feature sets at G={g} k={k}")


def summary_row(
    rows: Mapping[tuple[str, str], Row],
    model: str,
    label: str,
    feature_set: str,
    g: str,
    k: str,
    source: str,
) -> Row:
    """One output row: the six metrics with their intervals, copied as written."""
    first = rows[(model, "pr_auc")]
    out: Row = {
        "g": g,
        "k": k,
        "feature_set": feature_set,
        "model": model,
        "label": label,
        "primary": str((feature_set, model, (g, k)) == PRIMARY).lower(),
        "n_attempts": first["n_attempts"],
        "n_races": first.get("n_races", ""),
        "ci_kind": CI_KIND,
        "source_file": source,
    }
    for metric in METRICS:
        r = rows[(model, metric)]
        name = OUT_NAME.get(metric, metric)
        out[name], out[f"{name}_ci_low"], out[f"{name}_ci_high"] = (
            r["value"],
            r["ci_low"],
            r["ci_high"],
        )
    return out


def summary_rows(results_dir: Path = RESULTS_DIR) -> list[Row]:
    """All output rows in a fixed order: per setting, the models per feature set, then baselines."""
    out: list[Row] = []
    for g, k in SETTINGS:
        per_set = {fs: read_test_rows(eval_file(results_dir, fs, g, k)) for fs in SETS}
        check_baselines(per_set, g, k)
        for fs in SETS:
            src = f"results/{eval_file(results_dir, fs, g, k).name}"
            out += [summary_row(per_set[fs], m, lab, fs, g, k, src) for m, lab in MODELS]
        src = ", ".join(f"results/{eval_file(results_dir, fs, g, k).name}" for fs in SETS)
        out += [summary_row(per_set["real"], m, lab, "(any)", g, k, src) for m, lab in BASELINES]
    return out


def render_csv(rows: Iterable[Mapping[str, str]]) -> str:
    buf = io.StringIO()
    buf.write(CSV_STAMP + "\n")
    w = csv.DictWriter(buf, fieldnames=list(COLUMNS), lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def _f3(x: str) -> str:
    return f"{float(x):.3f}" if x != "" else "undefined"


def render_tables(rows: Sequence[Mapping[str, str]]) -> str:
    """The two setting tables (markdown), the primary row in bold."""
    parts: list[str] = []
    for g, k in SETTINGS:
        sel = [r for r in rows if (r["g"], r["k"]) == (g, k)]
        if not sel:
            raise SummaryError(f"no rows for G={g} k={k}")
        kind = "primary setting" if (g, k) == PRIMARY[2] else "sensitivity setting"
        head = sel[0]
        parts.append(
            f"**G={g}, k={k} ({kind}): {head['n_attempts']} attempts, {head['n_races']} races**\n"
        )
        parts.append(
            "| Feature set | Model | F1 (pos.) | PR-AUC [95% race-cluster CI] | ROC-AUC | Brier "
            "| ECE (10 bins) | ECE (5 bins) |\n|---|---|---|---|---|---|---|---|"
        )
        for r in sel:
            primary = r["primary"] == "true"
            fs = "—" if r["feature_set"] == "(any)" else f"`{r['feature_set']}`"
            label = f"**{r['label']} (PRIMARY)**" if primary else r["label"]
            pr = f"{_f3(r['pr_auc'])} [{_f3(r['pr_auc_ci_low'])}, {_f3(r['pr_auc_ci_high'])}]"
            if primary:
                pr = f"**{pr}**"
            parts.append(
                f"| {fs} | {label} | {_f3(r['f1_positive'])} | {pr} | {_f3(r['roc_auc'])} | "
                f"{_f3(r['brier'])} | {_f3(r['ece10'])} | {_f3(r['ece5'])} |"
            )
        parts.append("")
    return "\n".join(parts)


def replace_results_section(doc: str, tables: str) -> str:
    """``doc`` with the body of its ``## Results`` section (up to the next ``## `` heading or the
    end) replaced by ``tables``, one blank line before and after."""
    if doc.count(RESULTS_HEADING) != 1 or not (
        doc.startswith(RESULTS_HEADING) or f"\n{RESULTS_HEADING}" in doc
    ):
        raise SummaryError(f"{DOC_FILE} must contain exactly one '{RESULTS_HEADING.strip()}' line")
    start = doc.index(RESULTS_HEADING) + len(RESULTS_HEADING)
    nxt = doc.find("\n## ", start)
    end = len(doc) if nxt == -1 else nxt + 1
    body = f"\n{tables}" if nxt == -1 else f"\n{tables}\n"
    return doc[:start] + body + doc[end:]


def check_committed(paths: Sequence[Path], run: GitRunner = subprocess.run) -> None:
    """Every input must be tracked by git and identical to ``HEAD`` (no staged or unstaged edit)."""
    names = [str(p) for p in paths]
    try:
        tracked = run(
            ["git", "ls-files", "--error-unmatch", "--", *names],
            capture_output=True,
            text=True,
            check=False,
        )
        if tracked.returncode != 0:
            raise SummaryError(f"inputs must be committed; untracked: {tracked.stderr.strip()}")
        diff = run(
            ["git", "diff", "--name-only", "HEAD", "--", *names],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise SummaryError(f"git is needed to check the inputs are committed: {exc}") from exc
    if diff.returncode != 0:
        raise SummaryError(f"git diff failed: {diff.stderr.strip()}")
    if diff.stdout.strip():
        raise SummaryError(f"inputs differ from HEAD: {diff.stdout.split()}")


def write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="")
    tmp.replace(path)


def run_summary(
    results_dir: Path = RESULTS_DIR,
    summary_file: Path = SUMMARY_FILE,
    doc_file: Path = DOC_FILE,
    check: Callable[[Sequence[Path]], None] = check_committed,
) -> list[Row]:
    """Check the inputs, then write the summary CSV and the doc's Results tables."""
    inputs = input_files(results_dir)
    missing = [str(p) for p in inputs if not p.is_file()]
    if missing:
        raise SummaryError(f"missing inputs {missing}")
    check(inputs)
    if not doc_file.is_file():
        raise SummaryError(f"missing {doc_file}")
    rows = summary_rows(results_dir)
    doc = replace_results_section(doc_file.read_text(encoding="utf-8"), render_tables(rows))
    write_atomic(summary_file, render_csv(rows))
    write_atomic(doc_file, doc)
    return rows


def main(
    argv: Iterable[str] | None = None,
    check: Callable[[Sequence[Path]], None] = check_committed,
) -> int:
    p = argparse.ArgumentParser(prog="experiments/model_test_summary.py")
    p.parse_args(list(argv) if argv is not None else None)
    try:
        rows = run_summary(check=check)
    except SummaryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"wrote {SUMMARY_FILE} ({len(rows)} rows) and the Results tables of {DOC_FILE}")
    return 0
