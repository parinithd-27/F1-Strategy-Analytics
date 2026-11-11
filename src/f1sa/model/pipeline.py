"""The model-evaluation run behind ``experiments/model_eval.py`` (#14-#18, #29, #39).

Sources:

* ``sample``: the sample lake (2023 R5, 2024 R5). Dataset, training and calibration run on the
  seasons that exist; the test-season step is **skipped** (there is no 2025) and every output is
  stamped ``SMOKE TEST`` and suffixed ``_sample``.
* ``rehearsal``: the GENERATED table (``model.rehearsal``) for 2021-2025. The whole pipeline runs,
  including the test season of the generated data; outputs are stamped ``REHEARSAL`` and suffixed
  ``_rehearsal``. It never reads the lake.
* ``full``: every race in the lake. Needs ``allow_test_season`` (the 2025 season is opened once);
  synthetic-valued features are refused (``train.check_feature_policy``).

Outputs (``{results_dir}/<name>{suffix}.<ext>``): ``model_eval.csv``, ``calibration.csv``,
``feature_importance.csv`` / ``.png``, ``reliability.png`` and ``confusion_matrix.png``. Models
and ``metadata.json`` go to ``{models_dir}/undercut/{source}_{feature_set}_G{g}_k{k}/``.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.config import get_settings
from f1sa.etl.clean import CleanError
from f1sa.etl.io import resolve_feature_roots, sample_races
from f1sa.features.undercut_rule import DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS
from f1sa.model import features as MF
from f1sa.model import metrics as M
from f1sa.model import rehearsal
from f1sa.model.baselines import MajorityBaseline, RuleBaseline
from f1sa.model.calibrate import PLATT_BELOW_N, Calibrator, choose
from f1sa.model.dataset import (
    LABELLED,
    DatasetError,
    ModelDataset,
    build_dataset,
    load_attempts,
)
from f1sa.model.evaluate import (
    BOOTSTRAP_N,
    ScoredSplit,
    evaluate_test,
    metric_rows,
    score_split,
)
from f1sa.model.explain import gbt_importances, lr_coefficients, permutation_importance
from f1sa.model.train import (
    KINDS,
    SyntheticFeatureError,
    TrainConfig,
    TrainedModel,
    TrainingError,
    check_feature_policy,
    check_models_dir,
    save_models,
    tune,
    versions,
)

SOURCES = ("sample", "rehearsal", "full")
STAMPS = {
    "sample": "SMOKE TEST: 2 sample races, synthetic compounds/telemetry; not findings",
    "rehearsal": rehearsal.STAMP,
    "full": "",
}
SUFFIX = {"sample": "_sample", "rehearsal": "_rehearsal", "full": ""}
SKIP_TEST_MESSAGE = (
    "SMOKE TEST: no 2025 season in the sample; test-season evaluation skipped "
    "(the sealed test split was never opened)"
)


class RunError(RuntimeError):
    """The run cannot start (bad arguments, guards)."""


@dataclass(frozen=True)
class RunConfig:
    source: str
    g: float = DEFAULT_GAP_S
    k: int = DEFAULT_WINDOW_LAPS
    feature_set: str = MF.DEFAULT_FEATURE_SET
    results_dir: Path = Path("results")
    models_dir: Path = Path("models")
    n_boot: int = BOOTSTRAP_N
    seed: int = 42
    allow_test_season: bool = False
    train: TrainConfig = field(default_factory=TrainConfig)
    platt_below_n: int = PLATT_BELOW_N
    rehearsal_kwargs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.source not in SOURCES:
            raise RunError(f"unknown source {self.source!r}; choose from {SOURCES}")
        if self.feature_set not in MF.FEATURE_SETS:
            raise RunError(f"unknown feature set {self.feature_set!r}")
        if self.source == "full" and not self.allow_test_season:
            raise RunError(
                "--source full evaluates the 2025 test season once; pass --allow-test-season only "
                "after the label definition is signed off"
            )
        if self.n_boot < 1:
            raise RunError("n_boot must be >= 1")

    @property
    def stamp(self) -> str:
        return STAMPS[self.source]

    @property
    def model_subdir(self) -> Path:
        return (
            self.models_dir / "undercut" / f"{self.source}_{self.feature_set}_G{self.g}_k{self.k}"
        )

    def output(self, name: str, ext: str) -> Path:
        return self.results_dir / f"{name}{SUFFIX[self.source]}.{ext}"


@dataclass
class RunResult:
    dataset: ModelDataset
    models: dict[str, TrainedModel]
    calibrators: dict[str, Calibrator]
    selected_model: str
    validation: ScoredSplit
    test: ScoredSplit | None
    eval_rows: list[dict[str, Any]]
    outputs: dict[str, Path]
    metadata: dict[str, Any]
    messages: list[str]


def check_results_dir(results_dir: Path) -> None:
    allowed = (Path.cwd() / "results").resolve()
    target = results_dir.resolve()
    if target != allowed and allowed not in target.parents:
        raise RunError(f"results dir {results_dir} is outside {allowed}")


def load_frame(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    if cfg.source == "rehearsal":
        return rehearsal.generate(spark, **dict(cfg.rehearsal_kwargs))
    settings = get_settings()
    _clean, features_root = resolve_feature_roots(settings)
    races = set(sample_races(settings)) if cfg.source == "sample" else None
    return load_attempts(spark, features_root, races)


# ------------------------------------------------------------------ outputs
def _fmt(v: Any) -> Any:
    if isinstance(v, float):
        return "" if math.isnan(v) else f"{v:.6f}"
    return v


def write_csv(
    path: Path, rows: Sequence[Mapping[str, Any]], columns: Sequence[str], stamp: str
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        if stamp:
            fh.write(f"# {stamp}\n")
        w = csv.DictWriter(fh, fieldnames=list(columns), lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: _fmt(r.get(c, "")) for c in columns})
    tmp.replace(path)


def _finish(fig: Any, path: Path, stamp: str) -> None:
    if stamp:
        fig.text(
            0.5,
            0.5,
            stamp.split(":")[0],
            fontsize=40,
            color="red",
            alpha=0.15,
            ha="center",
            va="center",
            rotation=30,
        )
        fig.suptitle(stamp, fontsize=8, color="red")
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=100, metadata={"Software": None})
    plt.close(fig)


def plot_reliability(path: Path, scored: ScoredSplit, model: str, stamp: str) -> None:
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.plot([0, 1], [0, 1], "--", color="grey", label="perfect")
    for name, style in ((model, "o-"), (f"{model}_calibrated", "s-")):
        bins = M.calibration_bins(scored.labels, scored.scores[name]) if scored.labels.size else []
        if bins:
            ax.plot([b[1] for b in bins], [b[2] for b in bins], style, label=name)
    ax.set_xlabel("mean predicted P(success)")
    ax.set_ylabel("observed success rate")
    ax.set_title(f"Reliability ({scored.split}, n={scored.labels.size}, 10 bins)")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.legend(loc="upper left", fontsize=8)
    _finish(fig, path, stamp)


def plot_confusion(path: Path, scored: ScoredSplit, model: str, stamp: str) -> None:
    tn, fp, fn, tp = (
        M.confusion(scored.labels, scored.scores[model]) if scored.labels.size else (0, 0, 0, 0)
    )
    mat = np.array([[tn, fp], [fn, tp]])
    fig, ax = plt.subplots(figsize=(4, 4))
    ax.imshow(mat, cmap="Blues")
    for (i, j), v in np.ndenumerate(mat):
        ax.text(j, i, str(v), ha="center", va="center")
    ax.set_xticks([0, 1], ["pred fail", "pred success"])
    ax.set_yticks([0, 1], ["fail", "success"])
    ax.set_title(f"{model} @0.5 ({scored.split}, n={scored.labels.size})", fontsize=9)
    _finish(fig, path, stamp)


def plot_importance(path: Path, rows: Sequence[Mapping[str, Any]], stamp: str) -> None:
    methods = ("gbt_importance", "permutation_pr_auc_drop")
    fig, axes = plt.subplots(1, 2, figsize=(10, 5))
    for ax, method in zip(axes, methods, strict=True):
        sel = [r for r in rows if r["method"] == method and not math.isnan(float(r["value"]))]
        sel = sorted(sel, key=lambda r: -float(r["value"]))[:15][::-1]
        ax.barh([r["feature"] for r in sel], [float(r["value"]) for r in sel])
        ax.set_title(method if sel else f"{method} (undefined)", fontsize=9)
        ax.tick_params(axis="y", labelsize=7)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    _finish(fig, path, stamp)


# ------------------------------------------------------------------ run
def run(
    spark: SparkSession,
    cfg: RunConfig,
    frame: DataFrame | None = None,
    out: Callable[[str], None] = print,
) -> RunResult:
    check_results_dir(cfg.results_dir)
    check_models_dir(cfg.models_dir)
    messages: list[str] = []

    def say(msg: str) -> None:
        messages.append(msg)
        out(msg)

    if cfg.stamp:
        say(cfg.stamp)
    df = frame if frame is not None else load_frame(spark, cfg)
    labelled = df.where(
        (F.col("gap_threshold_s") == F.lit(float(cfg.g)))
        & (F.col("response_window_laps") == cfg.k)
        & F.col("outcome_category").isin(*LABELLED)
    )
    check_feature_policy(labelled, cfg.feature_set, cfg.source)
    ds = build_dataset(df, g=cfg.g, k=cfg.k, feature_set=cfg.feature_set)
    say(f"dataset G={ds.g} k={ds.k} feature_set={ds.feature_set}: {ds.counts}")
    models = {kind: tune(kind, ds, cfg.train) for kind in KINDS}
    for tm in models.values():
        say(f"{tm.kind}: {tm.tuning_status}; params {tm.params}")
    raw_val = score_split(
        "validation", ds.validation, models, _identity(), _majority(ds), _rule(ds)
    )
    calibrators: dict[str, Calibrator] = {}
    calib_rows: list[dict[str, Any]] = []
    seasons = np.asarray(raw_val.rows["season"])
    for kind in KINDS:
        cal, briers = choose(
            spark, seasons, raw_val.labels, raw_val.scores[kind], cfg.platt_below_n
        )
        calibrators[kind] = cal
        say(f"{kind} calibration: {cal.reason or cal.method}")
        calib_rows.append(_calibration_row(kind, cal, briers, raw_val))
    majority, rule = _majority(ds), _rule(ds)
    val = score_split("validation", ds.validation, models, calibrators, majority, rule)
    selected = _select(models)
    rows = metric_rows(val)
    test: ScoredSplit | None = None
    if cfg.source == "sample":
        say(SKIP_TEST_MESSAGE)
    else:
        test, test_rows = evaluate_test(
            ds, models, calibrators, majority, rule, n_boot=cfg.n_boot, seed=cfg.seed
        )
        rows += test_rows
        say(f"test season {ds.split.test}: {test.labels.size} attempts evaluated once")
    importance = (
        gbt_importances(models["gbt"], ds.columns)
        + lr_coefficients(models["lr"], ds.columns)
        + permutation_importance(spark, models[selected], ds.validation, ds.columns, seed=cfg.seed)
    )
    plot_split = test if test is not None else val
    outputs = _write_outputs(cfg, rows, calib_rows, importance, plot_split, selected)
    metadata = _metadata(cfg, ds, models, calibrators, selected, test is not None)
    outputs["metadata"] = save_models(models, cfg.model_subdir, metadata)
    for name, path in sorted(outputs.items()):
        say(f"wrote {name}: {path}")
    return RunResult(
        ds, models, calibrators, selected, val, test, rows, outputs, metadata, messages
    )


def _identity() -> dict[str, Calibrator]:
    return {k: Calibrator("identity", reason="uncalibrated scores for fitting") for k in KINDS}


def _majority(ds: ModelDataset) -> MajorityBaseline:
    labels = [r[0] for r in ds.train.select("label").collect()]
    return MajorityBaseline.fit(labels)


def _rule(ds: ModelDataset) -> RuleBaseline:
    return RuleBaseline(fallback_rate=_majority(ds).positive_rate)


def _select(models: Mapping[str, TrainedModel]) -> str:
    """The model with the higher validation PR-AUC; ``gbt`` when undefined or tied."""
    lr, gbt = models["lr"].validation_pr_auc, models["gbt"].validation_pr_auc
    if not math.isnan(lr) and (math.isnan(gbt) or lr > gbt):
        return "lr"
    return "gbt"


def _calibration_row(
    kind: str, cal: Calibrator, briers: Mapping[str, float], scored: ScoredSplit
) -> dict[str, Any]:
    raw = scored.scores[kind]
    calibrated = cal.apply(raw)
    y = scored.labels
    return {
        "model": kind,
        "split": "validation",
        "chosen_method": cal.method,
        "brier_raw": M.brier(y, raw),
        "brier_isotonic": briers.get("isotonic", M.NAN),
        "brier_platt": briers.get("platt", M.NAN),
        "brier_calibrated": M.brier(y, calibrated),
        "ece_raw": M.ece(y, raw),
        "ece_calibrated": M.ece(y, calibrated),
        "n_attempts": int(y.size),
        "reason": cal.reason,
    }


EVAL_COLUMNS = (
    "source",
    "smoke_test",
    "feature_set",
    "g",
    "k",
    "model",
    "split",
    "metric",
    "value",
    "ci_kind",
    "ci_low",
    "ci_high",
    "n_valid",
    "n_attempts",
    "n_races",
    "note",
)
CALIBRATION_COLUMNS = (
    "model",
    "split",
    "chosen_method",
    "brier_raw",
    "brier_isotonic",
    "brier_platt",
    "brier_calibrated",
    "ece_raw",
    "ece_calibrated",
    "n_attempts",
    "reason",
)
IMPORTANCE_COLUMNS = ("model", "method", "feature", "value", "std", "note")


def _write_outputs(
    cfg: RunConfig,
    rows: Iterable[Mapping[str, Any]],
    calib: Sequence[Mapping[str, Any]],
    importance: Sequence[Mapping[str, Any]],
    plot_split: ScoredSplit,
    selected: str,
) -> dict[str, Path]:
    extra = {
        "source": cfg.source,
        "smoke_test": str(cfg.source != "full").lower(),
        "feature_set": cfg.feature_set,
        "g": f"{cfg.g:g}",
        "k": cfg.k,
    }
    paths = {
        "model_eval": cfg.output("model_eval", "csv"),
        "calibration": cfg.output("calibration", "csv"),
        "feature_importance": cfg.output("feature_importance", "csv"),
        "feature_importance_png": cfg.output("feature_importance", "png"),
        "reliability_png": cfg.output("reliability", "png"),
        "confusion_matrix_png": cfg.output("confusion_matrix", "png"),
    }
    write_csv(paths["model_eval"], [{**extra, **r} for r in rows], EVAL_COLUMNS, cfg.stamp)
    write_csv(paths["calibration"], calib, CALIBRATION_COLUMNS, cfg.stamp)
    write_csv(paths["feature_importance"], importance, IMPORTANCE_COLUMNS, cfg.stamp)
    plot_importance(paths["feature_importance_png"], importance, cfg.stamp)
    stamp = cfg.stamp or ""
    if plot_split.split != "test":
        stamp = f"{stamp} | plotted on the VALIDATION season (calibrator fitted on it)".strip(" |")
    plot_reliability(paths["reliability_png"], plot_split, selected, stamp)
    plot_confusion(paths["confusion_matrix_png"], plot_split, f"{selected}_calibrated", stamp)
    return paths


def _metadata(
    cfg: RunConfig,
    ds: ModelDataset,
    models: Mapping[str, TrainedModel],
    calibrators: Mapping[str, Calibrator],
    selected: str,
    test_evaluated: bool,
) -> dict[str, Any]:
    return {
        "source": cfg.source,
        "smoke_test": cfg.source != "full",
        "stamp": cfg.stamp,
        "feature_set": ds.feature_set,
        "provenances": sorted(MF.FEATURE_SETS[ds.feature_set]),
        "features": [s.name for s in MF.specs_for(ds.feature_set)],
        "assembled_columns": list(ds.columns),
        "g": ds.g,
        "k": ds.k,
        "split": {
            "train": ds.split.train,
            "validation": ds.split.validation,
            "test": ds.split.test,
        },
        "counts": ds.counts,
        "class_weights": {str(c): w for c, w in ds.class_weights.items()},
        "models": {
            k: {
                "params": tm.params,
                "tuning_status": tm.tuning_status,
                "validation_pr_auc": tm.validation_pr_auc,
                "grid_scores": tm.grid_scores,
            }
            for k, tm in models.items()
        },
        "calibrators": {k: c.to_json() for k, c in calibrators.items()},
        "selected_model": selected,
        "test_season_evaluated": test_evaluated,
        "seed": cfg.seed,
        "bootstrap_n": cfg.n_boot,
        "versions": versions(),
    }


# ------------------------------------------------------------------ CLI
def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="experiments/model_eval.py")
    p.add_argument("--source", choices=SOURCES, required=True)
    p.add_argument("--g", type=float, default=DEFAULT_GAP_S)
    p.add_argument("--k", type=int, default=DEFAULT_WINDOW_LAPS)
    p.add_argument("--feature-set", choices=sorted(MF.FEATURE_SETS), default=MF.DEFAULT_FEATURE_SET)
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    p.add_argument("--models-dir", type=Path, default=Path("models"))
    p.add_argument("--bootstrap", type=int, default=BOOTSTRAP_N)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--master", default="local[2]")
    p.add_argument(
        "--allow-test-season",
        action="store_true",
        help="required for --source full: evaluates the 2025 test season (once)",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(
    argv: Iterable[str] | None = None,
    spark_factory: Callable[[str], SparkSession] | None = None,
) -> int:
    args = parse_args(argv)
    try:
        cfg = RunConfig(
            source=args.source,
            g=args.g,
            k=args.k,
            feature_set=args.feature_set,
            results_dir=args.results_dir,
            models_dir=args.models_dir,
            n_boot=args.bootstrap,
            seed=args.seed,
            allow_test_season=args.allow_test_season,
        )
        check_results_dir(cfg.results_dir)
        check_models_dir(cfg.models_dir)
        if cfg.source == "sample":
            sample_races(get_settings())
    except (RunError, TrainingError, CleanError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if spark_factory is not None:
        spark = spark_factory(args.master)
    else:
        from f1sa.etl.spark import build_spark

        spark = build_spark("f1sa-model-eval", args.master)
    try:
        run(spark, cfg)
    except SyntheticFeatureError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except (DatasetError, TrainingError, CleanError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark_factory is None:
            spark.stop()
    return 0
