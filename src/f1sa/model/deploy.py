"""The deployment model (Step 9c): train once with the frozen specification and save it.

``docs/model_spec.md`` (``model-spec-v1``) fixes the features, grids, calibration rule and decision
threshold. This module reruns exactly the development procedure on the primary setting and saves the
result for streaming (S12) and serving (S13):

* data: ``pipeline.load_frame`` in development mode, so only the ``season=2021..2024`` feature
  partitions are listed and read (``dataset.load_attempts_for_seasons``); the split is
  ``pipeline.DEV_SPLIT`` (train 2021-2023, validation 2024, **no test seasons**). The test-season
  evaluation (``evaluate.evaluate_test``) is never imported or called;
* models: ``train.tune`` with the frozen grids and seed, ``calibrate.choose`` on validation scores
  (Platt below 300 rows), the same functions the development run uses;
* outputs: ``models/undercut/deploy_{feature_set}_G{g}_k{k}/`` (gitignored). The ``deploy`` source
  token keeps it apart from ``full_*`` (the test run's folder, which the test run checks before it
  starts). Per model: ``pipeline/`` (Spark ``PipelineModel``), ``calibrator.json`` and
  ``metadata.json``; for LR also ``scorer.json`` (``model.lr_scorer``, no Spark). Plus
  ``reproduction.json``.

**Reproduction check** (before anything is moved into place): the saved models are reloaded and
must give exactly the in-memory validation predictions; the selected parameters, calibrator
parameters, raw validation PR-AUC and calibrated validation Brier / PR-AUC must equal the
development run's files (``results/model_dev_*``); the pure-Python LR scorer must equal Spark within
``SCORER_TOLERANCE``. Any difference raises ``ReproductionError`` (exit 4) and leaves the staging
folder for inspection; nothing is adjusted.

**Sample source (Step 17, ``make demo``).** ``--source sample`` runs the same procedure on the two
sample races (2023 R5 trains, 2024 R5 calibrates) so a fresh clone has a model for the stream job
and the API. It writes ``models/undercut/sample_deploy_real_G{g}_k{k}/`` (never ``deploy_*``),
stamps every metadata file as a SMOKE TEST, and skips only the comparison with the development
run (the sample has none); the reload round trip and the scorer-vs-Spark check still run. Its
probabilities come from 4 training attempts and are not findings.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import shutil
import subprocess
import sys
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyspark
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import StandardScalerModel, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from f1sa.config import get_settings
from f1sa.etl.clean import CleanError
from f1sa.etl.io import resolve_feature_roots
from f1sa.features.undercut_rule import DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS
from f1sa.model import features as MF
from f1sa.model import metrics as M
from f1sa.model import pipeline as P
from f1sa.model.calibrate import EPS, PLATT_BELOW_N, Calibrator, choose
from f1sa.model.dataset import (
    KEY,
    LABELLED,
    DatasetError,
    ModelDataset,
    build_dataset,
    season_partition_paths,
)
from f1sa.model.evaluate import ScoredSplit, score_split
from f1sa.model.lr_scorer import SCORER_FORMAT, LRScorer
from f1sa.model.train import (
    KINDS,
    SyntheticFeatureError,
    TrainConfig,
    TrainedModel,
    TrainingError,
    check_feature_policy,
    check_models_dir,
    tune,
)

DEPLOY_SOURCE = "deploy"
SOURCES = ("full", "sample")
SAMPLE_PREFIX = "sample_"
SAMPLE_STAMP = (
    "SMOKE TEST: trained on the 2 sample races (Jolpica laps, SYNTHETIC tyres); demo only, "
    "not findings"
)
SAMPLE_TEST_SEASON_STATEMENT = (
    "The sample has no 2025 race: only the sample races 2023 R5 (training) and 2024 R5 "
    "(calibration) were read, and evaluate.evaluate_test was not called."
)
SAMPLE_DEV_SKIPPED = "skipped: the sample source has no development run to compare with"
MODEL_SPEC = "model-spec-v1"
PRIMARY_MODEL = "lr_calibrated"
SCORER_TOLERANCE = 1e-9
FEATURE_TABLES = ("undercut_attempts", "stints", "telemetry_lap_features")
TEST_SEASON_STATEMENT = (
    "The test season (2025) was not read: only the season=2021..2024 feature partitions were "
    "listed and loaded, the split has no test seasons, and evaluate.evaluate_test was not called."
)
REQUIRED_METADATA = (
    "model",
    "model_spec",
    "features",
    "assembled_columns",
    "feature_set",
    "g",
    "k",
    "training_seasons",
    "calibration_seasons",
    "hyperparameters",
    "calibrator",
    "decision_threshold",
    "seed",
    "versions",
    "git_sha",
    "git_dirty",
    "test_season_read",
    "test_season_statement",
)


class DeployError(RuntimeError):
    """The deployment run cannot start or cannot write (guards, missing git sha, bad paths)."""


class ReproductionError(RuntimeError):
    """The saved model does not reproduce the in-memory model or the development run."""

    def __init__(self, mismatches: Sequence[str], staging: Path) -> None:
        self.mismatches = list(mismatches)
        self.staging = staging
        super().__init__(
            f"reproduction check failed ({len(self.mismatches)} difference(s)); nothing was moved "
            f"into place, staging kept at {staging}:\n  " + "\n  ".join(self.mismatches)
        )


@dataclass(frozen=True)
class GitInfo:
    sha: str
    dirty: bool | None


@dataclass(frozen=True)
class DeployConfig:
    g: float = DEFAULT_GAP_S
    k: int = DEFAULT_WINDOW_LAPS
    feature_set: str = MF.DEFAULT_FEATURE_SET
    models_dir: Path = Path("models")
    results_dir: Path = Path("results")
    force: bool = False
    train: TrainConfig = field(default_factory=TrainConfig)
    platt_below_n: int = PLATT_BELOW_N
    source: str = "full"

    def __post_init__(self) -> None:
        if self.feature_set != "real":
            raise DeployError("the deployment model uses the 'real' feature set only")
        if self.source not in SOURCES:
            raise DeployError(f"unknown source {self.source!r}; choose from {SOURCES}")

    @property
    def is_sample(self) -> bool:
        return self.source == "sample"

    def run_config(self) -> P.RunConfig:
        """The development-mode run configuration (2021-2024 only) the deployment reuses; for the
        sample source the sample run configuration (its races are 2023 R5 and 2024 R5)."""
        return P.RunConfig(
            source=self.source,
            no_test=not self.is_sample,
            g=self.g,
            k=self.k,
            feature_set=self.feature_set,
            results_dir=self.results_dir,
            models_dir=self.models_dir,
            train=self.train,
            platt_below_n=self.platt_below_n,
        )

    @property
    def target(self) -> Path:
        prefix = SAMPLE_PREFIX if self.is_sample else ""
        return (
            self.models_dir
            / "undercut"
            / f"{prefix}{DEPLOY_SOURCE}_{self.feature_set}_G{float(self.g)}_k{int(self.k)}"
        )


@dataclass
class DeployResult:
    target: Path
    dataset: ModelDataset
    models: dict[str, TrainedModel]
    calibrators: dict[str, Calibrator]
    validation: ScoredSplit
    reproduction: dict[str, Any]
    metadata: dict[str, dict[str, Any]]


# ------------------------------------------------------------------ provenance
def git_info(env: Mapping[str, str] | None = None) -> GitInfo:
    """``F1SA_GIT_SHA`` / ``F1SA_GIT_DIRTY`` (passed by the Makefile into the container, which has
    no git), else ``git`` itself. Raises when no sha can be found: the field is required."""
    e = os.environ if env is None else env
    sha = e.get("F1SA_GIT_SHA", "").strip()
    dirty_text = e.get("F1SA_GIT_DIRTY", "").strip().lower()
    dirty: bool | None = {"true": True, "false": False}.get(dirty_text)
    if not sha:
        try:
            sha = subprocess.run(
                ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
            ).stdout.strip()
            status = subprocess.run(
                ["git", "status", "--porcelain"], capture_output=True, text=True, check=True
            ).stdout
            dirty = bool(status.strip())
        except (OSError, subprocess.CalledProcessError):
            sha = ""
    if not sha:
        raise DeployError(
            "git sha unknown: set F1SA_GIT_SHA (make model-save-full does) or run inside the repo"
        )
    return GitInfo(sha, dirty)


def library_versions(spark: SparkSession) -> dict[str, str | None]:
    jvm = spark.sparkContext._jvm
    java = str(jvm.java.lang.System.getProperty("java.version")) if jvm is not None else None
    return {
        "pyspark": pyspark.__version__,
        "spark": spark.version,
        "python": platform.python_version(),
        "numpy": np.__version__,
        "java": java,
    }


# ------------------------------------------------------------------ training (frozen procedure)
def train_frozen(
    spark: SparkSession,
    cfg: DeployConfig,
    frame: DataFrame | None = None,
    out: Callable[[str], None] = print,
) -> tuple[ModelDataset, dict[str, TrainedModel], dict[str, Calibrator], ScoredSplit]:
    """The development procedure of ``pipeline.run`` (no-test mode): dataset, tuning, calibration
    and the calibrated validation scores. No test season exists in the split."""
    rc = cfg.run_config()
    df = frame if frame is not None else P.load_frame(spark, rc)
    labelled = df.where(
        (F.col("gap_threshold_s") == F.lit(float(cfg.g)))
        & (F.col("response_window_laps") == cfg.k)
        & F.col("outcome_category").isin(*LABELLED)
    )
    check_feature_policy(labelled, cfg.feature_set, cfg.source)
    ds = build_dataset(df, g=cfg.g, k=cfg.k, feature_set=cfg.feature_set, split=P.DEV_SPLIT)
    if ds.split.test:
        raise DeployError(f"deployment split must have no test seasons, got {ds.split.test}")
    out(f"dataset G={ds.g} k={ds.k} feature_set={ds.feature_set}: {ds.counts}")
    models = {kind: tune(kind, ds, cfg.train) for kind in KINDS}
    for tm in models.values():
        out(f"{tm.kind}: {tm.tuning_status}; params {tm.params}")
    majority, rule = P._majority(ds), P._rule(ds)
    raw_val = score_split("validation", ds.validation, models, P._identity(), majority, rule)
    seasons = np.asarray(raw_val.rows["season"])
    calibrators: dict[str, Calibrator] = {}
    for kind in KINDS:
        cal, _briers = choose(
            spark, seasons, raw_val.labels, raw_val.scores[kind], cfg.platt_below_n
        )
        calibrators[kind] = cal
        out(f"{kind} calibration: {cal.method} {cal.params}")
    val = score_split("validation", ds.validation, models, calibrators, majority, rule)
    return ds, models, calibrators, val


# ------------------------------------------------------------------ artefacts
def scorer_document(
    model: PipelineModel, columns: Sequence[str], cal: Calibrator
) -> dict[str, Any]:
    """``scorer.json`` for a fitted LR pipeline (assembler -> scaler -> LR) and its calibrator."""
    stages = model.stages
    if len(stages) != 3:
        raise DeployError(f"not an assembler -> scaler -> LR pipeline: {stages}")
    assembler, scaler, lr = stages
    if not (
        isinstance(assembler, VectorAssembler)
        and isinstance(scaler, StandardScalerModel)
        and isinstance(lr, LogisticRegressionModel)
    ):
        raise DeployError(f"not an assembler -> scaler -> LR pipeline: {stages}")
    if list(assembler.getInputCols()) != list(columns):
        raise DeployError("assembler input order differs from the dataset's assembled columns")
    if not (scaler.getWithMean() and scaler.getWithStd()):
        raise DeployError("the scaler must use withMean and withStd")
    if cal.method not in ("identity", "platt"):
        raise DeployError(f"the pure-Python scorer supports identity or platt, not {cal.method}")
    inputs = [
        {"name": s.name, "kind": s.kind, "categories": list(s.categories)}
        for s in MF.specs_for(MF.DEFAULT_FEATURE_SET)
    ]
    expanded = [c for s in MF.specs_for(MF.DEFAULT_FEATURE_SET) for c in MF.model_columns(s)]
    if expanded != list(columns):
        raise DeployError("feature registry expansion differs from the assembled columns")
    return {
        "format": SCORER_FORMAT,
        "inputs": inputs,
        "assembled_columns": list(columns),
        "scaler_mean": [float(x) for x in scaler.mean.toArray()],
        "scaler_std": [float(x) for x in scaler.std.toArray()],
        "coefficients": [float(x) for x in lr.coefficients.toArray()],
        "intercept": float(lr.intercept),
        "calibrator": cal.to_json(),
        "eps": EPS,
    }


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def _metadata(
    kind: str,
    cfg: DeployConfig,
    ds: ModelDataset,
    tm: TrainedModel,
    cal: Calibrator,
    selected: str,
    git: GitInfo,
    versions: Mapping[str, Any],
    partitions: Sequence[str],
) -> dict[str, Any]:
    files = {"pipeline": "pipeline", "calibrator": "calibrator.json"}
    if kind == "lr":
        files["scorer"] = "scorer.json"
    sample: dict[str, Any] = (
        {"source": "sample", "smoke_test": True, "stamp": SAMPLE_STAMP} if cfg.is_sample else {}
    )
    return {
        **sample,
        "model": kind,
        "model_spec": MODEL_SPEC,
        "primary_model": PRIMARY_MODEL,
        "selected_model_by_validation_pr_auc": selected,
        "features": [s.name for s in MF.specs_for(ds.feature_set)],
        "assembled_columns": list(ds.columns),
        "feature_set": ds.feature_set,
        "provenances": sorted(MF.FEATURE_SETS[ds.feature_set]),
        "g": ds.g,
        "k": ds.k,
        "training_seasons": list(ds.split.train),
        "tuning_seasons": list(ds.split.validation),
        "calibration_seasons": list(ds.split.validation),
        "counts": ds.counts,
        "class_weights": {str(c): w for c, w in ds.class_weights.items()},
        "hyperparameters": tm.params,
        "tuning_status": tm.tuning_status,
        "validation_pr_auc_raw": tm.validation_pr_auc,
        "grid_scores": tm.grid_scores,
        "calibrator": cal.to_json(),
        "decision_threshold": M.DECISION_THRESHOLD,
        "seed": cfg.train.seed,
        "versions": dict(versions),
        "git_sha": git.sha,
        "git_dirty": git.dirty,
        "test_season_read": False,
        "test_season_statement": (
            SAMPLE_TEST_SEASON_STATEMENT if cfg.is_sample else TEST_SEASON_STATEMENT
        ),
        "partitions_read": list(partitions),
        "files": files,
        "created_utc": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def save_artefacts(
    staging: Path,
    cfg: DeployConfig,
    ds: ModelDataset,
    models: Mapping[str, TrainedModel],
    calibrators: Mapping[str, Calibrator],
    selected: str,
    git: GitInfo,
    versions: Mapping[str, Any],
    partitions: Sequence[str],
) -> dict[str, dict[str, Any]]:
    metas: dict[str, dict[str, Any]] = {}
    for kind in KINDS:
        tm, cal = models[kind], calibrators[kind]
        d = staging / kind
        tm.model.write().save((d / "pipeline").resolve().as_uri())
        _write_json(d / "calibrator.json", cal.to_json())
        if kind == "lr":
            _write_json(d / "scorer.json", scorer_document(tm.model, ds.columns, cal))
        metas[kind] = _metadata(kind, cfg, ds, tm, cal, selected, git, versions, partitions)
        _write_json(d / "metadata.json", metas[kind])
    return metas


# ------------------------------------------------------------------ reproduction check
def _same(a: Any, b: Any) -> bool:
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return bool(a == b)


def _read_stamped_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as fh:
        lines = [line for line in fh if not line.startswith("#")]
    return list(csv.DictReader(lines))


def reload_models(staging: Path) -> tuple[dict[str, TrainedModel], dict[str, Calibrator]]:
    models: dict[str, TrainedModel] = {}
    cals: dict[str, Calibrator] = {}
    for kind in KINDS:
        d = staging / kind
        meta = json.loads((d / "metadata.json").read_text())
        pm = PipelineModel.load((d / "pipeline").resolve().as_uri())
        models[kind] = TrainedModel(kind, pm, dict(meta["hyperparameters"]))
        cals[kind] = Calibrator.from_json(json.loads((d / "calibrator.json").read_text()))
    return models, cals


def compare_with_dev(
    cfg: DeployConfig,
    models: Mapping[str, TrainedModel],
    calibrators: Mapping[str, Calibrator],
    val: ScoredSplit,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Rows (check, model, saved, development, match) against the development run's files.

    ``models`` are the in-memory fits (selected parameters, raw validation PR-AUC); ``calibrators``
    and ``val`` come from the models reloaded from disk."""
    rc = cfg.run_config()
    meta_path, calib_path = rc.output("metadata", "json"), rc.output("calibration", "csv")
    eval_path = rc.output("model_eval", "csv")
    missing = [str(p) for p in (meta_path, calib_path, eval_path) if not p.is_file()]
    if missing:
        return [], [f"development results missing (run make model-dev-full first): {missing}"]
    dev = json.loads(meta_path.read_text())
    calib = {r["model"]: r for r in _read_stamped_csv(calib_path)}
    evals = {
        (r["model"], r["metric"]): r["value"]
        for r in _read_stamped_csv(eval_path)
        if r["split"] == "validation" and r["ci_kind"] in ("race_cluster", "")
    }
    rows: list[dict[str, Any]] = []

    def check(name: str, model: str, saved: Any, ref: Any) -> None:
        rows.append(
            {"check": name, "model": model, "saved": saved, "dev": ref, "match": _same(saved, ref)}
        )

    if float(dev["g"]) != float(cfg.g) or int(dev["k"]) != int(cfg.k):
        check("setting", "-", (cfg.g, cfg.k), (dev["g"], dev["k"]))
    for kind in KINDS:
        tm, cal = models[kind], calibrators[kind]
        d = dev["models"][kind]
        check("hyperparameters", kind, tm.params, d["params"])
        ref_ap = d.get("validation_pr_auc")
        ref = M.NAN if ref_ap is None else float(ref_ap)
        check("validation_pr_auc_raw", kind, tm.validation_pr_auc, ref)
        check("calibrator", kind, cal.to_json(), dev["calibrators"][kind])
        y, p = val.labels, val.scores[f"{kind}_calibrated"]
        brier, ap = P._fmt(M.brier(y, p)), P._fmt(M.pr_auc(y, p))
        check("brier_calibrated (6 dp)", kind, brier, calib[kind]["brier_calibrated"])
        check("pr_auc_calibrated (6 dp)", kind, ap, evals.get((f"{kind}_calibrated", "pr_auc")))
    return rows, [
        f"{r['check']} {r['model']}: saved {r['saved']!r} != development {r['dev']!r}"
        for r in rows
        if not r["match"]
    ]


def raw_validation_rows(df: DataFrame, cfg: DeployConfig) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Raw decision-time inputs of the labelled validation attempts, keyed by KEY (None kept)."""
    names = [s.name for s in MF.specs_for(cfg.feature_set)]
    sel = df.where(
        (F.col("gap_threshold_s") == F.lit(float(cfg.g)))
        & (F.col("response_window_laps") == cfg.k)
        & F.col("outcome_category").isin(*LABELLED)
        & F.col("label").isNotNull()
        & F.col("season").isin(*P.DEV_SPLIT.validation)
    ).select(*KEY, *names)
    return {tuple(r[c] for c in KEY): r.asDict() for r in sel.collect()}


def compare_scorer(
    scorer: LRScorer, raw_rows: Mapping[tuple[Any, ...], Mapping[str, Any]], val: ScoredSplit
) -> tuple[dict[str, Any], list[str]]:
    keys = [tuple(r) for r in val.rows[list(KEY)].itertuples(index=False, name=None)]
    if set(keys) != set(raw_rows) or len(keys) != len(raw_rows):
        return {"n": len(keys)}, ["scorer: validation rows and raw rows do not align"]
    py = np.asarray([scorer.probability(raw_rows[k]) for k in keys], dtype=np.float64)
    spark = np.asarray(val.scores["lr_calibrated"], dtype=np.float64)
    diff = float(np.max(np.abs(py - spark))) if py.size else 0.0
    summary = {"n": int(py.size), "max_abs_diff": diff, "tolerance": SCORER_TOLERANCE}
    bad = [] if diff <= SCORER_TOLERANCE else [f"scorer: max |python - spark| = {diff:.3e}"]
    return summary, bad


def reproduction_check(
    staging: Path,
    cfg: DeployConfig,
    ds: ModelDataset,
    models: Mapping[str, TrainedModel],
    val: ScoredSplit,
    raw_rows: Mapping[tuple[Any, ...], Mapping[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    reloaded, cals = reload_models(staging)
    again = score_split("validation", ds.validation, reloaded, cals, P._majority(ds), P._rule(ds))
    mismatches: list[str] = []
    round_trip: dict[str, bool] = {}
    for name in (*KINDS, *(f"{k}_calibrated" for k in KINDS)):
        same = bool(np.array_equal(val.scores[name], again.scores[name]))
        round_trip[name] = same
        if not same:
            diff = float(np.max(np.abs(val.scores[name] - again.scores[name])))
            mismatches.append(f"round trip {name}: reloaded != in-memory (max diff {diff:.3e})")
    dev_rows: list[dict[str, Any]] | str
    dev_bad: list[str]
    if cfg.is_sample:
        dev_rows, dev_bad = SAMPLE_DEV_SKIPPED, []
    else:
        dev_rows, dev_bad = compare_with_dev(cfg, models, cals, again)
    scorer_summary, scorer_bad = compare_scorer(
        LRScorer.load(staging / "lr" / "scorer.json"), raw_rows, again
    )
    report = {
        "n_validation": int(val.labels.size),
        "round_trip_exact": round_trip,
        "development_run": dev_rows,
        "scorer_vs_spark": scorer_summary,
        "match": not (mismatches or dev_bad or scorer_bad),
    }
    return report, mismatches + dev_bad + scorer_bad


# ------------------------------------------------------------------ run
def check_target(cfg: DeployConfig) -> None:
    check_models_dir(cfg.models_dir)
    if cfg.target.exists() and not cfg.force:
        raise DeployError(f"{cfg.target} exists; pass --force to replace it")


def _partitions(
    spark: SparkSession, frame: DataFrame | None, cfg: DeployConfig | None = None
) -> list[str]:
    if frame is not None:
        return ["(frame supplied by the caller)"]
    if cfg is not None and cfg.is_sample:
        return ["(sample races: data/lake/features, via pipeline.load_frame --source sample)"]
    _clean, root = resolve_feature_roots(get_settings())
    seasons = (*P.DEV_SPLIT.train, *P.DEV_SPLIT.validation)
    return [p for t in FEATURE_TABLES for p in season_partition_paths(spark, root, t, seasons)]


def save_deployment(
    spark: SparkSession,
    cfg: DeployConfig,
    frame: DataFrame | None = None,
    out: Callable[[str], None] = print,
) -> DeployResult:
    check_target(cfg)
    git = git_info()
    partitions = _partitions(spark, frame, cfg)
    for p in partitions:
        out(f"partition: {p}")
    df = frame if frame is not None else P.load_frame(spark, cfg.run_config())
    ds, models, calibrators, val = train_frozen(spark, cfg, df, out)
    selected = P._select(models)
    staging = cfg.target.parent / f".staging-{cfg.target.name}-{uuid.uuid4().hex[:8]}"
    try:
        metas = save_artefacts(
            staging,
            cfg,
            ds,
            models,
            calibrators,
            selected,
            git,
            library_versions(spark),
            partitions,
        )
        report, bad = reproduction_check(
            staging, cfg, ds, models, val, raw_validation_rows(df, cfg)
        )
        _write_json(staging / "reproduction.json", report)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    for line in reproduction_lines(report):
        out(line)
    if bad:
        raise ReproductionError(bad, staging)
    if cfg.target.exists():
        shutil.rmtree(cfg.target)
    staging.rename(cfg.target)
    out(f"saved deployment model: {cfg.target}")
    return DeployResult(cfg.target, ds, models, calibrators, val, report, metas)


def reproduction_lines(report: Mapping[str, Any]) -> list[str]:
    lines = [f"reproduction check on {report['n_validation']} validation attempts:"]
    for name, ok in report["round_trip_exact"].items():
        lines.append(f"  round trip {name}: {'exact' if ok else 'DIFFERS'}")
    dev = report["development_run"]
    if isinstance(dev, str):
        lines.append(f"  development run: {dev}")
        dev = []
    for r in dev:
        flag = "MATCH" if r["match"] else "DIFFERS"
        lines.append(f"  {r['check']:<26} {r['model']:<4} saved={r['saved']} dev={r['dev']} {flag}")
    s = report["scorer_vs_spark"]
    if "max_abs_diff" in s:
        lines.append(
            f"  pure-Python LR scorer vs Spark: n={s['n']} max |diff| = {s['max_abs_diff']:.3e} "
            f"(tolerance {s['tolerance']:g})"
        )
    lines.append(f"reproduction: {'MATCH' if report['match'] else 'MISMATCH'}")
    return lines


# ------------------------------------------------------------------ CLI
def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="experiments/model_save.py",
        description="Train the frozen specification on 2021-2023, calibrate on 2024 and save it "
        "(the 2025 test season is never read). --source sample: the same on the 2 sample races, "
        "into models/undercut/sample_deploy_*, stamped SMOKE TEST (make demo).",
    )
    p.add_argument("--source", choices=SOURCES, required=True)
    p.add_argument("--g", type=float, default=DEFAULT_GAP_S)
    p.add_argument("--k", type=int, default=DEFAULT_WINDOW_LAPS)
    p.add_argument("--models-dir", type=Path, default=Path("models"))
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    p.add_argument("--force", action="store_true", help="replace an existing deployment folder")
    p.add_argument("--master", default="local[2]")
    return p.parse_args(list(argv) if argv is not None else None)


def main(
    argv: Iterable[str] | None = None,
    spark_factory: Callable[[str], SparkSession] | None = None,
    train: TrainConfig | None = None,
) -> int:
    args = parse_args(argv)
    try:
        cfg = DeployConfig(
            g=args.g,
            k=args.k,
            models_dir=args.models_dir,
            results_dir=args.results_dir,
            force=args.force,
            train=train or TrainConfig(),
            source=args.source,
        )
        P.check_results_dir(cfg.results_dir)
        check_target(cfg)
        git_info()
    except (DeployError, TrainingError, P.RunError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if spark_factory is not None:
        spark = spark_factory(args.master)
    else:
        from f1sa.etl.spark import build_spark

        spark = build_spark("f1sa-model-save", args.master)
    try:
        save_deployment(spark, cfg)
    except SyntheticFeatureError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    except ReproductionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 4
    except DeployError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (DatasetError, TrainingError, CleanError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark_factory is None:
            spark.stop()
    return 0
