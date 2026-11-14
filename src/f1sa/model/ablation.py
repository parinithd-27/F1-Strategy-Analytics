"""Paired feature-set ablation on the 2025 test season from the SAVED frozen 9f models (Step 16,
traceability #30, ``docs/model_spec_v2.md`` §4).

``make ablation-full [RECORD=1]`` runs ``experiments/ablation.py``. Nothing is retrained, re-tuned
or re-calibrated: the six ``models/undercut/full_{set}_G{g}_k{k}/`` folders (LR and GBT pipelines,
the Platt calibrators in ``metadata.json``) score the validation and test splits again, and

1. **exact match**: every row of the committed ``results/model_eval{_set}_G{g}_k{k}.csv`` (value,
   interval, counts, note) is recomputed and must equal the committed text at its printed precision
   (6 decimals). On any mismatch the run stops (exit 3) and writes nothing to ``results/``;
2. **earlier run**: the per-attempt scores and the printed differences must equal the run of
   2026-11-14T10:18Z (``data/ablation/full_20261114T101856Z``, ``data/ablation_record.log``), whose
   ``results/`` files were removed afterwards (exit 4 on any difference);
3. **paired bootstrap**: one list of 1000 race-cluster resamples (seed 42, the protocol of
   ``evaluate.bootstrap``) is applied to all three feature sets. The per-set percentiles from these
   shared draws must reproduce the committed per-set intervals (exit 3 otherwise), which shows they
   are the same race resamples. The differences ``real_tyre - real``, ``real_ti - real`` and
   ``real_ti - real_tyre`` are reported for calibrated LR and GBT, PR-AUC, Brier, ROC-AUC and ECE
   (10 bins), with 95% percentile intervals. No significance claim is made.

Per-attempt scores and the comparison tables always go to ``data/ablation/<run_id>/``; only
``--record`` writes ``results/ablation_paired.{csv,png}`` (refused if either exists) and appends a
fingerprint section to ``results/README.md``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import math
import os
import platform
import re
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt

from f1sa.model import features as MF
from f1sa.model import metrics as M

STAMP = (
    "2025 TEST SEASON paired ablation from the saved frozen 9f models (docs/model_spec_v2.md §4); "
    "nothing retrained, re-tuned or re-calibrated; no significance claim"
)
SETTINGS: tuple[tuple[float, int], ...] = ((3.5, 2), (5.0, 3))
SETS: tuple[str, ...] = MF.TEST_FEATURE_SETS
PAIRS: tuple[tuple[str, str], ...] = (
    ("real", "real_tyre"),
    ("real", "real_ti"),
    ("real_tyre", "real_ti"),
)
"""``(a, b)``: the difference is ``b - a``."""
DIFF_MODELS: tuple[str, ...] = ("lr_calibrated", "gbt_calibrated")
DIFF_METRICS: tuple[str, ...] = ("pr_auc", "brier", "roc_auc", "ece")
"""``ece`` is the primary 10-bin ECE."""
SELF_CHECK_METRICS: tuple[str, ...] = (*M.METRICS, "ece_5bin")
SCORE_COLUMNS: tuple[str, ...] = (
    "season", "round", "attacker_id", "lap", "race_id", "label",
    "lr", "lr_calibrated", "gbt", "gbt_calibrated",
)  # fmt: skip
SCORE_MODELS: tuple[str, ...] = ("lr", "lr_calibrated", "gbt", "gbt_calibrated")
KEY_COLUMNS: tuple[str, ...] = ("season", "round", "attacker_id", "lap")
N_BOOT, SEED, LEVEL = 1000, 42, 0.95
CI_KIND = "race_cluster_paired"
GBT_NOTE = (
    "GBT: validation PR-AUC moved by up to 0.043 from row order alone; differences of that size "
    "or smaller are not interpretable (docs/model_spec_v2.md §5)"
)
DIFF_COLUMNS: tuple[str, ...] = (
    "g", "k", "model", "metric", "set_a", "set_b", "difference", "value_a", "value_b",
    "diff", "ci_low", "ci_high", "n_valid", "n_boot", "seed", "ci_kind", "n_attempts",
    "n_races", "note",
)  # fmt: skip
RECORD_CSV, RECORD_PNG = "ablation_paired.csv", "ablation_paired.png"
README_NAME = "README.md"
OUT_ROOT = Path("data/ablation")
EARLIER_RUN = OUT_ROOT / "full_20261114T101856Z"
EARLIER_LOG = Path("data/ablation_record.log")
EXIT_OK, EXIT_REFUSED, EXIT_MISMATCH, EXIT_EARLIER = 0, 2, 3, 4

FloatArray = npt.NDArray[np.float64]


class AblationError(RuntimeError):
    """The run is refused or its inputs are inconsistent."""


# ------------------------------------------------------------------ names and guards
def setting_tag(g: float, k: int) -> str:
    return f"G{float(g):.1f}_k{int(k)}"


def model_dir(models_root: Path, feature_set: str, g: float, k: int) -> Path:
    check_feature_set(feature_set)
    return models_root / "undercut" / f"full_{feature_set}_G{float(g)}_k{int(k)}"


def eval_file(results_dir: Path, feature_set: str, g: float, k: int) -> Path:
    check_feature_set(feature_set)
    mid = "" if feature_set == "real" else f"_{feature_set}"
    return results_dir / f"model_eval{mid}_{setting_tag(g, k)}.csv"


def scores_name(feature_set: str, g: float, k: int) -> str:
    return f"scores_{feature_set}_{setting_tag(g, k)}.csv"


def check_feature_set(feature_set: str) -> None:
    """Only the pre-registered test-season sets (``MF.TEST_FEATURE_SETS``)."""
    if feature_set not in MF.TEST_FEATURE_SETS:
        raise AblationError(
            f"feature set {feature_set!r} is not a test-season set {list(MF.TEST_FEATURE_SETS)} "
            "(docs/model_spec_v2.md §1)"
        )


def check_record(results_dir: Path) -> None:
    """``--record`` never overwrites: refuse if either output already exists."""
    existing = [
        str(results_dir / n) for n in (RECORD_CSV, RECORD_PNG) if (results_dir / n).exists()
    ]
    if existing:
        raise AblationError(f"{existing} exist and are never overwritten")


def check_metadata(meta: Mapping[str, Any], feature_set: str, g: float, k: int) -> None:
    """The folder must be the frozen full test run of this set and setting."""
    want = {"source": "full", "feature_set": feature_set, "test_season_evaluated": True}
    for key, value in want.items():
        if meta.get(key) != value:
            raise AblationError(f"metadata {key}={meta.get(key)!r}, expected {value!r}")
    if float(meta.get("g", math.nan)) != float(g) or int(meta.get("k", -1)) != int(k):
        raise AblationError(f"metadata setting G={meta.get('g')} k={meta.get('k')} != {g} {k}")
    for kind in ("lr", "gbt"):
        if kind not in meta.get("calibrators", {}):
            raise AblationError(f"metadata has no {kind} calibrator")


# ------------------------------------------------------------------ exact match
def fmt(v: Any) -> str:
    """The text ``pipeline.write_csv`` writes for a value (floats with 6 decimals, NaN empty)."""
    if isinstance(v, float):
        return "" if math.isnan(v) else f"{v:.6f}"
    if isinstance(v, (bool, np.bool_)):
        return str(bool(v))
    return str(v)


def read_eval_rows(path: Path) -> list[dict[str, str]]:
    """Rows of a committed ``model_eval`` file (``#`` stamp lines skipped)."""
    if not path.is_file():
        raise AblationError(f"missing {path}")
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.startswith("#")]
    return list(csv.DictReader(io.StringIO("\n".join(lines) + "\n")))


RowKey = tuple[str, str, str, str]


def _row_key(r: Mapping[str, str]) -> RowKey:
    return (r["model"], r["split"], r["metric"], r["ci_kind"])


@dataclass(frozen=True)
class Match:
    feature_set: str
    setting: str
    model: str
    split: str
    metric: str
    ci_kind: str
    column: str
    committed: str
    recomputed: str

    @property
    def ok(self) -> bool:
        return self.committed == self.recomputed


MATCH_FIELDS = (
    "value", "ci_low", "ci_high", "n_valid", "n_attempts", "n_races", "note",
    "decision_threshold", "source", "smoke_test", "feature_set", "g", "k",
)  # fmt: skip


def compare_eval(
    committed: Sequence[Mapping[str, str]],
    recomputed: Sequence[Mapping[str, Any]],
    feature_set: str,
    setting: str,
) -> list[Match]:
    """One ``Match`` per (row, column). A row present on one side only is a mismatch on every
    column (the missing side is ``<missing>``)."""
    a = {_row_key(r): r for r in committed}
    b = {_row_key({c: fmt(v) for c, v in r.items()}): r for r in recomputed}
    if len(a) != len(committed) or len(b) != len(recomputed):
        raise AblationError(f"{feature_set} {setting}: duplicate (model, split, metric, ci_kind)")
    out = []
    for key in sorted(a.keys() | b.keys()):
        ca, rb = a.get(key), b.get(key)
        for col in MATCH_FIELDS:
            cv = "<missing>" if ca is None else ca.get(col, "")
            rv = "<missing>" if rb is None else fmt(rb.get(col, ""))
            out.append(Match(feature_set, setting, *key, col, cv, rv))
    return out


def match_table(matches: Sequence[Match], *, test_race_cluster_only: bool = True) -> str:
    """Plain-text table: set, model, setting, metric, committed, recomputed, match (value, low and
    high per line)."""
    groups: dict[tuple[str, str, str, str, str, str], dict[str, Match]] = {}
    for m in matches:
        if m.column not in ("value", "ci_low", "ci_high"):
            continue
        if test_race_cluster_only and (m.split != "test" or m.ci_kind != "race_cluster"):
            continue
        key = (m.feature_set, m.model, m.setting, m.split, m.metric, m.ci_kind)
        groups.setdefault(key, {})[m.column] = m
    head = (
        f"{'set':10s} {'model':15s} {'setting':9s} {'metric':12s} "
        f"{'committed value [low, high]':34s} {'recomputed value [low, high]':34s} match"
    )
    lines = [head, "-" * len(head)]
    for key, cols in groups.items():
        fs, model, setting, _split, metric, _kind = key

        def triple(side: str, c: Mapping[str, Match] = cols) -> str:
            v = [getattr(c[n], side) if n in c else "" for n in ("value", "ci_low", "ci_high")]
            return f"{v[0] or 'nan'} [{v[1] or 'nan'}, {v[2] or 'nan'}]"

        ok = all(m.ok for m in cols.values())
        lines.append(
            f"{fs:10s} {model:15s} {setting:9s} {metric:12s} {triple('committed'):34s} "
            f"{triple('recomputed'):34s} {'yes' if ok else 'NO'}"
        )
    return "\n".join(lines)


def write_matches(path: Path, matches: Sequence[Match]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(
            ["feature_set", "setting", "model", "split", "metric", "ci_kind", "column",
             "committed", "recomputed", "match"]
        )  # fmt: skip
        for m in matches:
            w.writerow(
                [m.feature_set, m.setting, m.model, m.split, m.metric, m.ci_kind, m.column,
                 m.committed, m.recomputed, "yes" if m.ok else "NO"]
            )  # fmt: skip


# ------------------------------------------------------------------ per-attempt scores
@dataclass(frozen=True)
class SetScores:
    """The test split of one feature set and setting, rows sorted by ``KEY_COLUMNS``."""

    feature_set: str
    keys: list[tuple[int, int, str, int]]
    race_ids: npt.NDArray[np.str_]
    labels: FloatArray
    scores: dict[str, FloatArray]


def set_scores(feature_set: str, rows: Any, scores: Mapping[str, Any]) -> SetScores:
    """From an ``evaluate.ScoredSplit`` (its pandas ``rows`` and ``scores``)."""
    keys = [
        (int(s), int(r), str(a), int(lap))
        for s, r, a, lap in zip(
            rows["season"], rows["round"], rows["attacker_id"], rows["lap"], strict=True
        )
    ]
    if len(set(keys)) != len(keys):
        raise AblationError(f"{feature_set}: duplicate attempt keys in the test split")
    return SetScores(
        feature_set,
        keys,
        np.asarray(rows["race_id"], dtype=str),
        np.asarray(rows["label"], dtype=np.float64),
        {m: np.asarray(v, dtype=np.float64) for m, v in scores.items()},
    )


def score_lines(s: SetScores) -> list[str]:
    """CSV lines (header first) of the per-attempt scores, in the earlier run's layout."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(SCORE_COLUMNS)
    for i, (season, rnd, att, lap) in enumerate(s.keys):
        w.writerow(
            [season, rnd, att, lap, s.race_ids[i], int(s.labels[i]),
             *(fmt(float(s.scores[m][i])) for m in SCORE_MODELS)]
        )  # fmt: skip
    return buf.getvalue().splitlines()


def write_scores(path: Path, s: SetScores) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {STAMP}\n" + "\n".join(score_lines(s)) + "\n", encoding="utf-8")


def compare_scores(new_lines: Sequence[str], earlier: Path) -> list[str]:
    """Differences against an earlier scores file (stamp lines ignored); empty when equal."""
    if not earlier.is_file():
        return [f"{earlier}: missing"]
    old = [ln for ln in earlier.read_text(encoding="utf-8").splitlines() if not ln.startswith("#")]
    out = []
    if len(old) != len(new_lines):
        out.append(f"{earlier.name}: {len(old)} lines before, {len(new_lines)} now")
    for i, (a, b) in enumerate(zip(old, new_lines, strict=False)):
        if a != b:
            out.append(f"{earlier.name} line {i + 1}: before {a!r}, now {b!r}")
    return out


def align(per_set: Mapping[str, SetScores]) -> tuple[FloatArray, npt.NDArray[np.str_]]:
    """Labels and race ids shared by every set; refuses when the attempts differ."""
    ref = next(iter(per_set.values()))
    for s in per_set.values():
        if s.keys != ref.keys:
            raise AblationError(f"{s.feature_set} and {ref.feature_set} have different attempts")
        if not np.array_equal(s.labels, ref.labels) or not np.array_equal(s.race_ids, ref.race_ids):
            raise AblationError(
                f"{s.feature_set}: labels or race ids differ from {ref.feature_set}"
            )
    return ref.labels, ref.race_ids


# ------------------------------------------------------------------ paired bootstrap
def metric_fn(name: str) -> Callable[[FloatArray, FloatArray], float]:
    if name == "ece_5bin":
        return lambda y, p: M.ece(y, p, n_bins=5)
    fns: dict[str, Callable[[Any, Any], float]] = {
        "f1_positive": M.f1_positive,
        "macro_f1": M.macro_f1,
        "pr_auc": M.pr_auc,
        "roc_auc": M.roc_auc,
        "brier": M.brier,
        "ece": M.ece,
    }
    if name not in fns:
        raise ValueError(f"unknown metric {name!r}")
    return fns[name]


DrawKey = tuple[str, str, str]
"""(feature set, model, metric)"""


def race_resamples(groups: npt.ArrayLike, n: int = N_BOOT, seed: int = SEED) -> list[Any]:
    """The ``n`` race-cluster index arrays of ``evaluate.bootstrap`` (same rng protocol)."""
    from f1sa.model.evaluate import resample_indices

    if n < 1:
        raise ValueError("n must be >= 1")
    rng = np.random.default_rng(seed)
    return [resample_indices(groups, rng, "race_cluster") for _ in range(n)]


def paired_draws(
    y: FloatArray,
    scores: Mapping[str, Mapping[str, FloatArray]],
    draws: Sequence[Any],
    metrics: Sequence[str],
) -> dict[DrawKey, FloatArray]:
    """``{(set, model, metric): value per draw}``: every draw applied to every set and model."""
    fns = {m: metric_fn(m) for m in metrics}
    out: dict[DrawKey, list[float]] = {
        (fs, model, m): [] for fs, ms in scores.items() for model in ms for m in metrics
    }
    for idx in draws:
        yb = y[idx]
        for fs, ms in scores.items():
            for model, p in ms.items():
                pb = p[idx]
                for m, fn in fns.items():
                    out[(fs, model, m)].append(float(fn(yb, pb)))
    return {k: np.asarray(v, dtype=np.float64) for k, v in out.items()}


def percentile_ci(values: FloatArray, level: float = LEVEL) -> tuple[float, float, int]:
    """Percentile interval over the finite values, as ``evaluate.bootstrap``."""
    v = values[np.isfinite(values)]
    if not v.size:
        return M.NAN, M.NAN, 0
    lo_q, hi_q = (1 - level) / 2, 1 - (1 - level) / 2
    return float(np.quantile(v, lo_q)), float(np.quantile(v, hi_q)), int(v.size)


def self_check(
    draws: Mapping[DrawKey, FloatArray],
    committed: Mapping[str, Sequence[Mapping[str, str]]],
    setting: str,
) -> list[Match]:
    """Per-set race-cluster intervals from the shared draws vs the committed test rows."""
    out = []
    for fs, rows in committed.items():
        for r in rows:
            if r["split"] != "test" or r["ci_kind"] != "race_cluster":
                continue
            key = (fs, r["model"], r["metric"])
            if key not in draws:
                continue
            lo, hi, nv = percentile_ci(draws[key])
            for col, val in (("ci_low", lo), ("ci_high", hi), ("n_valid", nv)):
                out.append(
                    Match(fs, setting, r["model"], "test", r["metric"], "race_cluster", col,
                          r[col], fmt(val))
                )  # fmt: skip
    return out


def difference_rows(
    y: FloatArray,
    groups: npt.NDArray[np.str_],
    scores: Mapping[str, Mapping[str, FloatArray]],
    draws: Mapping[DrawKey, FloatArray],
    g: float,
    k: int,
    *,
    n_boot: int = N_BOOT,
    seed: int = SEED,
) -> list[dict[str, Any]]:
    """One row per model x metric x pair: ``b - a`` on the full split and its paired interval
    (a draw counts only when the metric is defined for both sets)."""
    n_races = int(np.unique(groups).size)
    rows = []
    for model in DIFF_MODELS:
        for metric in DIFF_METRICS:
            fn = metric_fn(metric)
            for a, b in PAIRS:
                va, vb = float(fn(y, scores[a][model])), float(fn(y, scores[b][model]))
                d = draws[(b, model, metric)] - draws[(a, model, metric)]
                lo, hi, nv = percentile_ci(d)
                rows.append(
                    {
                        "g": f"{float(g):g}",
                        "k": int(k),
                        "model": model,
                        "metric": "ece10" if metric == "ece" else metric,
                        "set_a": a,
                        "set_b": b,
                        "difference": f"{b} - {a}",
                        "value_a": va,
                        "value_b": vb,
                        "diff": vb - va,
                        "ci_low": lo,
                        "ci_high": hi,
                        "n_valid": nv,
                        "n_boot": n_boot,
                        "seed": seed,
                        "ci_kind": CI_KIND,
                        "n_attempts": int(y.size),
                        "n_races": n_races,
                        "note": GBT_NOTE if model.startswith("gbt") else "",
                    }
                )
    return rows


# ------------------------------------------------------------------ earlier run (10:18Z)
_LOG_LINE = re.compile(
    r"^\s*G=(?P<g>[\d.]+) k=(?P<k>\d+) (?P<model>\w+)\s+(?P<metric>\w+)\s+"
    r"(?P<b>\w+) - (?P<a>\w+)\s+(?P<d>[+-]\d+\.\d+) \[(?P<lo>[+-]\d+\.\d+), (?P<hi>[+-]\d+\.\d+)\]"
)
EarlierKey = tuple[str, int, str, str, str, str]
"""(g as ``:g``, k, model, metric, set_a, set_b)"""


def parse_earlier_log(text: str) -> dict[EarlierKey, tuple[str, str, str]]:
    """The differences the earlier run printed, as text (3 decimals, signed)."""
    out: dict[EarlierKey, tuple[str, str, str]] = {}
    for line in text.splitlines():
        m = _LOG_LINE.match(line)
        if m:
            metric = "ece" if m["metric"] == "ece10" else m["metric"]
            key = (f"{float(m['g']):g}", int(m["k"]), m["model"], metric, m["a"], m["b"])
            out[key] = (m["d"], m["lo"], m["hi"])
    return out


def compare_earlier(
    rows: Iterable[Mapping[str, Any]], earlier: Mapping[EarlierKey, tuple[str, str, str]]
) -> list[str]:
    """Every printed earlier difference must equal this run's at 3 signed decimals."""
    if not earlier:
        return ["the earlier log has no difference lines"]
    mine: dict[EarlierKey, tuple[str, str, str]] = {}
    for r in rows:
        metric = "ece" if r["metric"] == "ece10" else str(r["metric"])
        key = (str(r["g"]), int(r["k"]), str(r["model"]), metric, str(r["set_a"]), str(r["set_b"]))
        mine[key] = tuple(f"{float(r[c]):+.3f}" for c in ("diff", "ci_low", "ci_high"))  # type: ignore[assignment]
    out = []
    for key, old in sorted(earlier.items()):
        new = mine.get(key)
        if new != old:
            out.append(f"{key}: before {old}, now {new}")
    return out


# ------------------------------------------------------------------ outputs
def render_diff_csv(rows: Sequence[Mapping[str, Any]]) -> str:
    buf = io.StringIO()
    buf.write(f"# {STAMP}\n")
    w = csv.DictWriter(buf, fieldnames=list(DIFF_COLUMNS), lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({c: fmt(r.get(c, "")) for c in DIFF_COLUMNS})
    return buf.getvalue()


def diff_lines(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    out = ["paired differences b - a, race-cluster 95% interval (no significance claim):"]
    for r in rows:
        out.append(
            f"  G={r['g']} k={r['k']} {r['model']:15s} {r['metric']:7s} {r['difference']:19s} "
            f"{float(r['diff']):+.3f} [{float(r['ci_low']):+.3f}, {float(r['ci_high']):+.3f}]"
            f" n_valid={r['n_valid']}"
        )
    return out


METRIC_LABEL = {
    "pr_auc": "PR-AUC",
    "brier": "Brier",
    "roc_auc": "ROC-AUC",
    "ece10": "ECE (10 bins)",
}
MODEL_LABEL = {"lr_calibrated": "calibrated LR", "gbt_calibrated": "calibrated GBT"}


def plot_forest(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    """One panel per metric: each difference as a point with its paired interval, zero marked."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    metrics = list(dict.fromkeys(str(r["metric"]) for r in rows))
    settings = list(dict.fromkeys((str(r["g"]), int(r["k"])) for r in rows))
    labels = [
        (m, d, s) for m in DIFF_MODELS for d in (f"{b} - {a}" for a, b in PAIRS) for s in settings
    ]
    colors = {"lr_calibrated": "#2a6fbb", "gbt_calibrated": "#c0612b"}
    fig, axes = plt.subplots(1, len(metrics), figsize=(4.2 * len(metrics), 6.4), sharey=True)
    axes_list = list(np.atleast_1d(axes))
    for ax, metric in zip(axes_list, metrics, strict=True):
        for i, (model, diff, (g, k)) in enumerate(labels):
            r = next(
                x
                for x in rows
                if x["metric"] == metric and x["model"] == model and x["difference"] == diff
                and (str(x["g"]), int(x["k"])) == (g, k)
            )  # fmt: skip
            y = len(labels) - 1 - i
            hollow = (g, k) != settings[0]
            ax.hlines(y, float(r["ci_low"]), float(r["ci_high"]), color=colors[model], lw=1.6)
            ax.plot(
                float(r["diff"]),
                y,
                "o",
                color=colors[model],
                mfc="white" if hollow else colors[model],
                ms=5,
            )
        ax.axvline(0.0, color="#555555", lw=0.8, ls="--")
        ax.set_title(METRIC_LABEL.get(metric, metric), fontsize=10)
        ax.set_xlabel("difference b - a (paired 95% race-cluster interval)", fontsize=8)
        ax.grid(axis="x", color="#dddddd", lw=0.6)
        ax.tick_params(labelsize=8)
    axes_list[0].set_yticks(range(len(labels)))
    axes_list[0].set_yticklabels(
        [f"{MODEL_LABEL[m]}: {d}  (G={g}, k={k})" for m, d, (g, k) in reversed(labels)],
        fontsize=7,
    )
    fig.suptitle(
        "2025 test season: paired feature-set differences (frozen 9f models). Filled: G=3.5 k=2 "
        "(primary); hollow: G=5 k=3 (sensitivity). No significance claim; GBT differences "
        "<= 0.043 PR-AUC are not interpretable (spec v2 §5).",
        fontsize=8,
        wrap=True,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120, metadata={"Software": None})
    plt.close(fig)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fingerprint(
    *, now: datetime, inputs: Sequence[Path], model_dirs: Sequence[Path], params: Mapping[str, Any]
) -> list[tuple[str, str]]:
    """``(item, value)`` pairs for ``results/README.md``. The app container has no git: the sha
    comes from ``F1SA_GIT_SHA`` / ``F1SA_GIT_DIRTY`` (set by the Makefile, as for model-save)."""
    import numpy
    import pyspark

    return [
        ("measured_at_utc", now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("git_sha", os.environ.get("F1SA_GIT_SHA") or "unavailable"),
        ("git_dirty", os.environ.get("F1SA_GIT_DIRTY") or "unknown"),
        ("python", platform.python_version()),
        ("pyspark", pyspark.__version__),
        ("numpy", numpy.__version__),
        ("os", f"{platform.system()} {platform.release()}"),
        ("cpu", f"{os.cpu_count()} logical CPUs"),
        ("committed_inputs", ", ".join(f"{p.name} sha256 {sha256(p)[:12]}" for p in inputs)),
        (
            "frozen_models",
            ", ".join(
                f"{d.name}/metadata.json sha256 {sha256(d / 'metadata.json')[:12]}"
                for d in model_dirs
            ),
        ),
        ("params", ", ".join(f"{k}={v}" for k, v in params.items())),
    ]


def append_readme(path: Path, name: str, pairs: Sequence[tuple[str, str]]) -> None:
    from f1sa.serving.latency_bench import append_readme as _append

    _append(path, name, pairs)


# ------------------------------------------------------------------ Spark part
@dataclass
class Scored:
    feature_set: str
    g: float
    k: int
    eval_rows: list[dict[str, Any]]
    test: SetScores


def build_eval_rows(
    val: Any,
    test: Any,
    rule: Any,
    feature_set: str,
    g: float,
    k: int,
    *,
    n_boot: int = N_BOOT,
    test_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """The rows of a test run's ``model_eval`` file from its scored validation and test splits,
    built exactly as ``pipeline.run`` builds them (validation without intervals; test with both
    interval kinds, the rule and ECE notes, and the supplementary 5-bin ECE)."""
    from f1sa.model.evaluate import metric_rows, supplementary_ece_rows
    from f1sa.model.pipeline import THRESHOLD_METRICS, annotate_rows

    rows = metric_rows(val, n_boot=0, seed=SEED)
    if test_rows is None:
        test_rows = metric_rows(test, n_boot=n_boot, seed=SEED)
    rows += annotate_rows(test_rows, test, rule)
    rows += supplementary_ece_rows(test, n_boot=n_boot, seed=SEED)
    extra = {
        "source": "full",
        "smoke_test": "false",
        "feature_set": feature_set,
        "g": f"{float(g):g}",
        "k": int(k),
    }
    return [
        {
            **extra,
            **r,
            "decision_threshold": M.DECISION_THRESHOLD if r["metric"] in THRESHOLD_METRICS else "",
        }
        for r in rows
    ]


def score_frozen(
    spark: Any, models_root: Path, feature_set: str, g: float, k: int, *, n_boot: int = N_BOOT
) -> Scored:
    """Score validation and test with the frozen folder of one set and setting and rebuild the
    rows of its ``model_eval`` file. No model or calibrator is fitted."""
    import json

    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F

    from f1sa.model.calibrate import Calibrator
    from f1sa.model.dataset import LABELLED, build_dataset
    from f1sa.model.evaluate import evaluate_test, score_split
    from f1sa.model.pipeline import RunConfig, _majority, _rule, load_frame
    from f1sa.model.train import KINDS, TrainedModel, check_feature_policy

    folder = model_dir(models_root, feature_set, g, k)
    meta = json.loads((folder / "metadata.json").read_text(encoding="utf-8"))
    check_metadata(meta, feature_set, g, k)
    cfg = RunConfig(source="full", g=g, k=k, feature_set=feature_set, allow_test_season=True)
    df = load_frame(spark, cfg)
    labelled = df.where(
        (F.col("gap_threshold_s") == F.lit(float(g)))
        & (F.col("response_window_laps") == k)
        & F.col("outcome_category").isin(*LABELLED)
    )
    check_feature_policy(labelled, feature_set, "full")
    ds = build_dataset(df, g=g, k=k, feature_set=feature_set)
    if list(ds.columns) != list(meta["assembled_columns"]):
        raise AblationError(f"{folder}: assembled columns differ from the frozen metadata")
    if ds.counts != meta["counts"]:
        raise AblationError(f"{folder}: counts {ds.counts} != frozen {meta['counts']}")
    models = {
        kind: TrainedModel(
            kind,
            PipelineModel.load(str(folder / kind)),
            dict(meta["models"][kind]["params"]),
            tuning_status="frozen 9f model (loaded, not refitted)",
        )
        for kind in KINDS
    }
    cals = {kind: Calibrator.from_json(meta["calibrators"][kind]) for kind in KINDS}
    majority, rule = _majority(ds), _rule(ds)
    val = score_split("validation", ds.validation, models, cals, majority, rule)
    # The sealed 2025 split is opened only through evaluate.evaluate_test (the one sanctioned path).
    test, test_rows = evaluate_test(ds, models, cals, majority, rule, n_boot=n_boot, seed=SEED)
    eval_rows = build_eval_rows(
        val, test, rule, feature_set, g, k, n_boot=n_boot, test_rows=test_rows
    )
    for part in (ds.train, ds.validation):
        part.unpersist()
    return Scored(feature_set, g, k, eval_rows, set_scores(feature_set, test.rows, test.scores))


# ------------------------------------------------------------------ CLI
def run_id(now: datetime | None = None) -> str:
    return f"full_{(now or datetime.now(UTC)).strftime('%Y%m%dT%H%M%SZ')}"


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="experiments/ablation.py", description=__doc__.split("\n\n")[0]
    )
    p.add_argument("--source", choices=("full",), required=True)
    p.add_argument("--allow-test-season", action="store_true", help="required: scores 2025")
    p.add_argument("--record", action="store_true", help="write results/ablation_paired.*")
    p.add_argument("--precheck", action="store_true", help="host: committed-input check only")
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    p.add_argument("--models-dir", type=Path, default=Path("models"))
    p.add_argument("--out-root", type=Path, default=OUT_ROOT)
    p.add_argument("--earlier-run", type=Path, default=EARLIER_RUN)
    p.add_argument("--earlier-log", type=Path, default=EARLIER_LOG)
    p.add_argument("--master", default="local[2]")
    return p.parse_args(list(argv) if argv is not None else None)


def input_files(results_dir: Path) -> list[Path]:
    return [eval_file(results_dir, fs, g, k) for g, k in SETTINGS for fs in SETS]


def main(
    argv: Iterable[str] | None = None,
    *,
    spark_factory: Callable[[str], Any] | None = None,
    score: Callable[..., Scored] = score_frozen,
    n_boot: int = N_BOOT,
    out: Callable[[str], None] = print,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> int:
    args = parse_args(argv)
    if not args.allow_test_season:
        out("refused: --allow-test-season is required (this scores the 2025 test season)")
        return EXIT_REFUSED
    if args.record and args.results_dir != Path("results"):
        out("refused: --record writes results/ only")
        return EXIT_REFUSED
    inputs = input_files(args.results_dir)
    try:
        missing = [str(p) for p in inputs if not p.is_file()]
        if missing:
            raise AblationError(f"missing committed inputs {missing}")
        if args.record:
            check_record(args.results_dir)
        if args.precheck:
            from f1sa.model.test_summary import SummaryError, check_committed

            try:
                check_committed(inputs)
            except SummaryError as exc:
                raise AblationError(str(exc)) from exc
            out("precheck ok: model_eval inputs are committed and unchanged")
            return EXIT_OK
        earlier_text = (
            args.earlier_log.read_text(encoding="utf-8") if args.earlier_log.is_file() else ""
        )
        earlier = parse_earlier_log(earlier_text)
        if not earlier or not args.earlier_run.is_dir():
            raise AblationError(
                f"the earlier run {args.earlier_run} / {args.earlier_log} is needed for the "
                "cross-check and is missing"
            )
    except AblationError as exc:
        out(f"refused: {exc}")
        return EXIT_REFUSED

    started = now()
    out_dir = args.out_root / run_id(started)
    out(f"{STAMP}\nper-attempt scores and tables: {out_dir}")
    if spark_factory is not None:
        spark = spark_factory(args.master)
    else:
        from f1sa.etl.spark import build_spark

        spark = build_spark("f1sa-ablation", args.master)
    scored: dict[tuple[float, int], dict[str, Scored]] = {}
    try:
        for g, k in SETTINGS:
            for fs in SETS:
                scored.setdefault((g, k), {})[fs] = score(
                    spark, args.models_dir, fs, g, k, n_boot=n_boot
                )
    finally:
        if spark_factory is None:
            spark.stop()

    # 1. exact match against the committed files, before any interval
    matches: list[Match] = []
    committed: dict[tuple[float, int], dict[str, list[dict[str, str]]]] = {}
    for (g, k), by_set in scored.items():
        for fs, s in by_set.items():
            rows = read_eval_rows(eval_file(args.results_dir, fs, g, k))
            committed.setdefault((g, k), {})[fs] = rows
            matches += compare_eval(rows, s.eval_rows, fs, setting_tag(g, k))
    write_matches(out_dir / "exact_match.csv", matches)
    out(match_table(matches))
    bad = [m for m in matches if not m.ok]
    n_rows = {(m.feature_set, m.setting, m.model, m.split, m.metric, m.ci_kind) for m in matches}
    out(f"exact match: {len(matches) - len(bad)}/{len(matches)} values over {len(n_rows)} rows")
    if bad:
        out("MISMATCH: nothing is written to results/. Differing values:")
        for m in bad:
            out(f"  {m.feature_set} {m.setting} {m.model} {m.split} {m.metric} {m.ci_kind} "
                f"{m.column}: committed {m.committed!r}, recomputed {m.recomputed!r}")  # fmt: skip
        return EXIT_MISMATCH

    # 2. per-attempt scores, and the earlier run's scores
    problems: list[str] = []
    for (g, k), by_set in scored.items():
        for fs, s in by_set.items():
            write_scores(out_dir / scores_name(fs, g, k), s.test)
            problems += compare_scores(
                score_lines(s.test), args.earlier_run / scores_name(fs, g, k)
            )
    out(f"wrote per-attempt scores to {out_dir}")
    if problems:
        out("EARLIER RUN DIFFERS (per-attempt scores): nothing is written to results/")
        for p_ in problems:
            out(f"  {p_}")
        return EXIT_EARLIER
    out(f"per-attempt scores equal the earlier run {args.earlier_run} (all 6 files)")

    # 3. one shared list of race resamples per setting, applied to every set
    diffs: list[dict[str, Any]] = []
    checks: list[Match] = []
    for (g, k), by_set in scored.items():
        tests = {fs: s.test for fs, s in by_set.items()}
        y, groups = align(tests)
        draws = race_resamples(groups, n_boot)
        scores = {fs: {m: t.scores[m] for m in M_ALL} for fs, t in tests.items()}
        values = paired_draws(y, scores, draws, SELF_CHECK_METRICS)
        checks += self_check(values, committed[(g, k)], setting_tag(g, k))
        diffs += difference_rows(y, groups, scores, values, g, k, n_boot=n_boot)
    write_matches(out_dir / "shared_draws_self_check.csv", checks)
    bad = [m for m in checks if not m.ok]
    out(
        "shared draws reproduce the committed per-set intervals: "
        f"{len(checks) - len(bad)}/{len(checks)}"
    )
    if bad:
        out("MISMATCH in the shared-draw self-check: nothing is written to results/")
        for m in bad:
            out(f"  {m.feature_set} {m.setting} {m.model} {m.metric} {m.column}: "
                f"committed {m.committed!r}, shared draws {m.recomputed!r}")  # fmt: skip
        return EXIT_MISMATCH
    diff_text = render_diff_csv(diffs)
    (out_dir / RECORD_CSV).write_text(diff_text, encoding="utf-8")
    earlier_problems = compare_earlier(diffs, earlier)
    if earlier_problems:
        out("EARLIER RUN DIFFERS (printed differences): nothing is written to results/")
        for p_ in earlier_problems:
            out(f"  {p_}")
        return EXIT_EARLIER
    out(f"the {len(earlier)} differences printed by the earlier run are reproduced exactly")
    for line in diff_lines(diffs):
        out(line)
    plot_forest(out_dir / RECORD_PNG, diffs)
    if args.record:
        check_record(args.results_dir)
        dirs = [model_dir(args.models_dir, fs, g, k) for g, k in SETTINGS for fs in SETS]
        params = {
            "n_boot": n_boot,
            "seed": SEED,
            "ci": "95% percentile, race-cluster, shared resamples across feature sets",
            "pairs": "; ".join(f"{b} - {a}" for a, b in PAIRS),
            "models": "/".join(DIFF_MODELS),
            "metrics": "/".join(DIFF_METRICS),
            "earlier_run": f"{args.earlier_run.name} reproduced exactly",
            "scores": str(out_dir),
        }
        pairs = fingerprint(now=started, inputs=inputs, model_dirs=dirs, params=params)
        (args.results_dir / RECORD_CSV).write_text(diff_text, encoding="utf-8")
        plot_forest(args.results_dir / RECORD_PNG, diffs)
        append_readme(
            args.results_dir / README_NAME,
            RECORD_CSV,
            pairs,
        )
        out(f"wrote {args.results_dir / RECORD_CSV} and {args.results_dir / RECORD_PNG}; "
            f"appended the fingerprint to {args.results_dir / README_NAME}")  # fmt: skip
    else:
        out(f"not recorded (no --record): outputs only in {out_dir}")
    return EXIT_OK


M_ALL = (*SCORE_MODELS, "majority", "rule")


if __name__ == "__main__":
    sys.exit(main())
