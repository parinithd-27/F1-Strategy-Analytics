"""The model-evaluation run behind ``experiments/model_eval.py`` (#14-#18, #29, #39).

Sources:

* ``sample``: the sample lake (2023 R5, 2024 R5). Dataset, training and calibration run on the
  seasons that exist; the test-season step is **skipped** (there is no 2025) and every output is
  stamped ``SMOKE TEST`` and suffixed ``_sample``.
* ``rehearsal``: the GENERATED table (``model.rehearsal``) for 2021-2025. The whole pipeline runs,
  including the test season of the generated data; outputs are stamped ``REHEARSAL`` and suffixed
  ``_rehearsal``. It never reads the lake.
* ``full``: every race in the lake. Needs ``allow_test_season`` (the 2025 season is opened once
  per feature set and setting); synthetic-valued features are refused
  (``train.check_feature_policy``) and only the feature sets of ``docs/model_spec_v2.md``
  (``features.TEST_FEATURE_SETS``: ``real`` primary, ``real_tyre`` and ``real_ti`` ablation) are
  allowed.
* ``full`` with ``no_test`` (``--no-test``, DEVELOPMENT): reads only the 2021-2024 feature
  partitions (``dataset.load_attempts_for_seasons``; the 2025 directories are never listed or
  opened), trains on 2021-2023, selects and calibrates on 2024, and reports validation metrics
  with both bootstraps, the per-season class balance and a leave-one-season-out check
  (``model.loso``). Every output is stamped DEVELOPMENT and named ``model_dev_*`` (``_G5.0_k3``
  etc. for a non-primary setting); no model binaries are saved. ``no_test`` and
  ``allow_test_season`` are mutually exclusive. Development mode also accepts the
  development-only feature set ``real_ti`` (TracingInsights tyre and telemetry inputs from the
  2021-2024 clean partitions; ``features.DEV_ONLY_FEATURE_SETS``). Its files carry ``_real_ti``
  (``model_dev_eval_real_ti.csv``) plus ``model_dev_missing_rates_real_ti.csv``, so the ``real``
  files are never overwritten. ``real`` file names are unchanged.

Outputs (``{results_dir}/<name>{suffix}.<ext>``): ``model_eval``, ``calibration``,
``feature_importance`` (``.csv`` / ``.png``), ``reliability.png`` and ``confusion_matrix.png``. The
suffix is ``_sample`` / ``_rehearsal`` for those sources. For the full test run it is the feature
set (none for ``real``, ``_real_tyre``, ``_real_ti``) plus the setting, ``_G3.5_k2`` (primary) or
``_G5.0_k3`` (sensitivity), so no two runs share a file, and a full test run refuses to start when
any of its result files or its saved metadata already exists (the first complete run is the
reported one). Models
and ``metadata.json`` go to ``{models_dir}/undercut/{source}_{feature_set}_G{g}_k{k}/``.
"""

from __future__ import annotations

import argparse
import csv
import json
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
    MISSING_RATE_COLUMNS,
    TI_CLEAN_TABLES,
    DatasetError,
    ModelDataset,
    SplitSpec,
    build_dataset,
    load_attempts,
    load_attempts_for_seasons,
    missing_rates,
    season_partition_paths,
)
from f1sa.model.evaluate import (
    BOOTSTRAP_N,
    ECE_BIAS_NOTE,
    ScoredSplit,
    evaluate_test,
    metric_rows,
    score_split,
    supplementary_ece_rows,
)
from f1sa.model.explain import gbt_importances, lr_coefficients, permutation_importance
from f1sa.model.loso import LOSO_COLUMNS, run_loso, summary_lines
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
RESULT_FILES: tuple[tuple[str, str], ...] = (
    ("model_eval", "csv"),
    ("calibration", "csv"),
    ("feature_importance", "csv"),
    ("feature_importance", "png"),
    ("reliability", "png"),
    ("confusion_matrix", "png"),
)
DEV_EXTRA_FILES: tuple[tuple[str, str], ...] = (
    ("class_balance", "csv"),
    ("loso", "csv"),
    ("metadata", "json"),
)
DEV_SPLIT = SplitSpec(train=(2021, 2022, 2023), validation=(2024,), test=())
TEST_SPLIT = SplitSpec()
"""The frozen split of the test run (train 2021-2023, validation 2024, test 2025): the split
``build_dataset`` uses when a test-mode run passes ``split=None``."""
DEV_STAMP = (
    "DEVELOPMENT: validation season 2024 only, not a final result. Validation numbers from so few "
    "labelled attempts (about 50) are noisy, and 2024 is used both to select hyperparameters and "
    "to fit the calibrator. The 2025 test season was not loaded"
)
DEV_SKIP_TEST_MESSAGE = (
    "DEVELOPMENT: --no-test; the 2025 partitions were never listed or read and no test-season "
    "evaluation exists in this run"
)
BALANCE_COLUMNS = (
    "season",
    "split",
    "n_labelled",
    "n_positive",
    "n_negative",
    "positive_rate",
    "n_races",
)
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
    no_test: bool = False
    train: TrainConfig = field(default_factory=TrainConfig)
    platt_below_n: int = PLATT_BELOW_N
    rehearsal_kwargs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.source not in SOURCES:
            raise RunError(f"unknown source {self.source!r}; choose from {SOURCES}")
        if self.feature_set not in MF.FEATURE_SETS:
            raise RunError(f"unknown feature set {self.feature_set!r}")
        if self.no_test and self.allow_test_season:
            raise RunError(
                "--no-test and --allow-test-season are mutually exclusive: --no-test never loads "
                "the 2025 season, --allow-test-season evaluates it once"
            )
        if self.no_test and self.source != "full":
            raise RunError("--no-test is the development mode of --source full only")
        if self.source == "full" and not (self.allow_test_season or self.no_test):
            raise RunError(
                "--source full evaluates the 2025 test season once; pass --allow-test-season only "
                "after the label definition is signed off, or --no-test for a development run"
            )
        if self.source == "full" and not self._full_feature_set_allowed():
            raise RunError(
                "--source full uses the 'real' feature set, or with --no-test the development sets "
                f"{sorted(MF.DEV_ONLY_FEATURE_SETS)}; --allow-test-season accepts only "
                f"{list(MF.TEST_FEATURE_SETS)} (docs/model_spec.md, docs/model_spec_v2.md); "
                f"got {self.feature_set!r}"
            )
        if self.source == "rehearsal" and MF.uses_tracinginsights(self.feature_set):
            raise RunError(
                f"--source rehearsal has no TracingInsights inputs; {self.feature_set!r} needs "
                "--source sample or full"
            )
        if self.n_boot < 1:
            raise RunError("n_boot must be >= 1")

    def _full_feature_set_allowed(self) -> bool:
        """Test season: only the sets of ``docs/model_spec_v2.md``. Development: ``real`` and the
        development sets."""
        fs = self.feature_set
        if self.allow_test_season:
            return fs in MF.TEST_FEATURE_SETS
        return fs == "real" or (self.no_test and fs in MF.DEV_ONLY_FEATURE_SETS)

    @property
    def stamp(self) -> str:
        return DEV_STAMP if self.no_test else STAMPS[self.source]

    @property
    def setting_suffix(self) -> str:
        """'' for the primary setting (G=3.5, k=2), else e.g. ``_G5.0_k3``."""
        if (float(self.g), int(self.k)) == (float(DEFAULT_GAP_S), int(DEFAULT_WINDOW_LAPS)):
            return ""
        return f"_G{float(self.g):.1f}_k{int(self.k)}"

    @property
    def full_suffix(self) -> str:
        """The full test run's setting suffix, always present: ``_G3.5_k2``, ``_G5.0_k3``."""
        return f"_G{float(self.g):.1f}_k{int(self.k)}"

    @property
    def model_subdir(self) -> Path:
        return (
            self.models_dir / "undercut" / f"{self.source}_{self.feature_set}_G{self.g}_k{self.k}"
        )

    @property
    def feature_set_suffix(self) -> str:
        """'' for ``real`` (every existing file name is unchanged), else ``_{feature_set}``."""
        return "" if self.feature_set == "real" else f"_{self.feature_set}"

    def output(self, name: str, ext: str) -> Path:
        fs = self.feature_set_suffix
        if self.no_test:
            short = name.removeprefix("model_")
            return self.results_dir / f"model_dev_{short}{fs}{self.setting_suffix}.{ext}"
        if self.source == "full":
            return self.results_dir / f"{name}{fs}{self.full_suffix}.{ext}"
        return self.results_dir / f"{name}{fs}{SUFFIX[self.source]}.{ext}"

    def result_paths(self) -> list[Path]:
        """Every file this run writes (result files and the saved metadata)."""
        paths = [self.output(name, ext) for name, ext in RESULT_FILES]
        if MF.uses_tracinginsights(self.feature_set):
            paths.append(self.output("missing_rates", "csv"))
        if self.no_test:
            paths += [self.output(n, e) for n, e in DEV_EXTRA_FILES]
        else:
            paths.append(self.model_subdir / "metadata.json")
        return paths


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


def check_no_previous_test_run(cfg: RunConfig) -> None:
    """A full test run never overwrites: if any of its files exists (this setting was already
    evaluated on the test season), refuse before anything is loaded."""
    if cfg.source != "full" or cfg.no_test:
        return
    existing = [str(p) for p in cfg.result_paths() if p.exists()]
    if existing:
        raise RunError(
            f"test-season results for G={cfg.g:g} k={cfg.k} already exist ({existing}); the first "
            "complete run is the reported one and is never overwritten"
        )


def load_frame(spark: SparkSession, cfg: RunConfig) -> DataFrame:
    if cfg.source == "rehearsal":
        return rehearsal.generate(spark, **dict(cfg.rehearsal_kwargs))
    settings = get_settings()
    clean_root, features_root = resolve_feature_roots(settings)
    ti_root = clean_root if MF.uses_tracinginsights(cfg.feature_set) else None
    if cfg.no_test:
        seasons = (*DEV_SPLIT.train, *DEV_SPLIT.validation)
        return _load_seasons(spark, features_root, ti_root, seasons)
    if cfg.source == "full" and ti_root is not None:
        # Test mode with TracingInsights inputs: the development loader, for the frozen split's
        # seasons (2021-2025). ``load_attempts`` below needs explicit races for those inputs.
        seasons = (*TEST_SPLIT.train, *TEST_SPLIT.validation, *TEST_SPLIT.test)
        return _load_seasons(spark, features_root, ti_root, seasons)
    races = set(sample_races(settings)) if cfg.source == "sample" else None
    return load_attempts(spark, features_root, races, ti_root)


def _load_seasons(
    spark: SparkSession, features_root: str, ti_root: str | None, seasons: tuple[int, ...]
) -> DataFrame:
    """``load_attempts_for_seasons`` for ``seasons``, printing the partitions it reads."""
    for table in ("undercut_attempts", "stints", "telemetry_lap_features"):
        paths = season_partition_paths(spark, features_root, table, seasons)
        print(f"reading {table} partitions: {[p.rsplit('/', 1)[-1] for p in paths]}")
    if ti_root is not None:
        for table in TI_CLEAN_TABLES:
            paths = season_partition_paths(spark, ti_root, table, seasons)
            print(f"reading clean {table} partitions: {[p.rsplit('/', 1)[-1] for p in paths]}")
    return load_attempts_for_seasons(spark, features_root, seasons, ti_root)


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
    ax.set_title(
        f"{model} @{M.DECISION_THRESHOLD:g} ({scored.split}, n={scored.labels.size})", fontsize=9
    )
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
    check_no_previous_test_run(cfg)
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
    split = DEV_SPLIT if cfg.no_test else None
    ds = build_dataset(df, g=cfg.g, k=cfg.k, feature_set=cfg.feature_set, split=split)
    say(f"dataset G={ds.g} k={ds.k} feature_set={ds.feature_set}: {ds.counts}")
    rates = (
        missing_rates(df, cfg.feature_set, cfg.g, cfg.k)
        if MF.uses_tracinginsights(cfg.feature_set)
        else []
    )
    for line in missing_rate_lines(rates):
        say(line)
    balance = class_balance(ds) if cfg.no_test else []
    for b in balance:
        say(
            f"class balance {b['season']} ({b['split']}): {b['n_labelled']} labelled, "
            f"{b['n_positive']} positive, {b['n_negative']} negative, {b['n_races']} races"
        )
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
    rows = metric_rows(val, n_boot=cfg.n_boot if cfg.no_test else 0, seed=cfg.seed)
    test: ScoredSplit | None = None
    loso: list[dict[str, Any]] = []
    if cfg.no_test:
        rows = annotate_rows(rows, val, rule) + supplementary_ece_rows(
            val, n_boot=cfg.n_boot, seed=cfg.seed
        )
        say(DEV_SKIP_TEST_MESSAGE)
        say(rule_fallback_note(val, rule))
        for line in _validation_lines(rows):
            say(line)
        loso = run_loso(spark, ds, cfg.train)
        say(
            "leave-one-season-out on 2021-2024 (stability check; fixed first grid point, "
            "raw scores):"
        )
        for line in summary_lines(loso):
            say(line)
    elif cfg.source == "sample":
        say(SKIP_TEST_MESSAGE)
    else:
        test, test_rows = evaluate_test(
            ds, models, calibrators, majority, rule, n_boot=cfg.n_boot, seed=cfg.seed
        )
        rows += annotate_rows(test_rows, test, rule) + supplementary_ece_rows(
            test, n_boot=cfg.n_boot, seed=cfg.seed
        )
        say(f"test season {ds.split.test}: {test.labels.size} attempts evaluated once")
    importance = (
        gbt_importances(models["gbt"], ds.columns)
        + lr_coefficients(models["lr"], ds.columns)
        + permutation_importance(spark, models[selected], ds.validation, ds.columns, seed=cfg.seed)
    )
    plot_split = test if test is not None else val
    outputs = _write_outputs(cfg, rows, calib_rows, importance, plot_split, selected)
    metadata = _metadata(cfg, ds, models, calibrators, selected, test is not None)
    if rates:
        metadata["coverage_policy"] = COVERAGE_POLICY
        metadata["missing_rates"] = rates
        outputs["missing_rates"] = cfg.output("missing_rates", "csv")
        write_csv(outputs["missing_rates"], rates, MISSING_RATE_COLUMNS, cfg.stamp)
    if cfg.no_test:
        outputs["class_balance"] = cfg.output("class_balance", "csv")
        write_csv(outputs["class_balance"], balance, BALANCE_COLUMNS, cfg.stamp)
        outputs["loso"] = cfg.output("loso", "csv")
        write_csv(outputs["loso"], loso, LOSO_COLUMNS, cfg.stamp)
        outputs["metadata"] = cfg.output("metadata", "json")
        outputs["metadata"].write_text(
            json.dumps(
                {**metadata, "class_balance": balance}, indent=2, sort_keys=True, default=str
            )
            + "\n"
        )
    else:
        outputs["metadata"] = save_models(models, cfg.model_subdir, metadata)
    for name, path in sorted(outputs.items()):
        say(f"wrote {name}: {path}")
    return RunResult(
        ds, models, calibrators, selected, val, test, rows, outputs, metadata, messages
    )


COVERAGE_POLICY = (
    "fill_0.0_plus_missing; categorical unknown level = missing; partial telemetry keeps values "
    "plus telemetry_partial_ti; no race or attempt dropped, excluded or down-weighted for coverage"
)


def missing_rate_lines(rates: Sequence[Mapping[str, Any]]) -> list[str]:
    """One line per season: TracingInsights inputs with any missing value, and partial laps."""
    by_season: dict[int, list[Mapping[str, Any]]] = {}
    for r in rates:
        if r["provenance"] == MF.TRACINGINSIGHTS:
            by_season.setdefault(int(r["season"]), []).append(r)
    lines = []
    for season, rows in sorted(by_season.items()):
        parts = [
            f"{r['feature']} {r['n_missing']}/{r['n_attempts']}" for r in rows if r["n_missing"]
        ]
        partial = sum(int(r["n_telemetry_partial"] or 0) for r in rows)
        lines.append(
            f"missing (TracingInsights) {season}: "
            f"{', '.join(parts) if parts else 'none'}; partial telemetry flags {partial}"
        )
    return lines


def class_balance(ds: ModelDataset) -> list[dict[str, Any]]:
    """Labelled attempts, positives and races per season of the train and validation splits."""
    out = []
    for name, part in (("train", ds.train), ("validation", ds.validation)):
        agg = part.groupBy("season").agg(
            F.count(F.lit(1)).alias("n"),
            F.coalesce(F.sum("label"), F.lit(0)).alias("pos"),
            F.countDistinct("race_id").alias("races"),
        )
        for r in agg.collect():
            n, pos = int(r["n"]), int(r["pos"])
            out.append(
                {
                    "season": int(r["season"]),
                    "split": name,
                    "n_labelled": n,
                    "n_positive": pos,
                    "n_negative": n - pos,
                    "positive_rate": pos / n if n else M.NAN,
                    "n_races": int(r["races"]),
                }
            )
    return sorted(out, key=lambda b: b["season"])


def rule_fallback_note(scored: ScoredSplit, rule: RuleBaseline) -> str:
    """How many rows of ``scored`` got a real rule probability and how many fell back to the
    training positive rate (the rule cannot be computed when a decision-time input is missing)."""
    n = len(scored.rows)
    fallback = int(rule.fallback_mask(scored.rows).sum()) if n else 0
    return (
        f"rule baseline: decision-time rule probability on {n - fallback} of {n} rows; "
        f"{fallback} fall back to the training positive rate {rule.fallback_rate:.4f}"
    )


def annotate_rows(
    rows: Sequence[Mapping[str, Any]], scored: ScoredSplit, rule: RuleBaseline
) -> list[dict[str, Any]]:
    """Notes for the development and test reports: the rule's fallback count on every rule row,
    and the bootstrap bias of ECE intervals on every ECE row. Values are never changed."""
    fallback = rule_fallback_note(scored, rule)
    out = []
    for r in rows:
        notes = [str(r.get("note") or "")]
        if r["model"] == "rule":
            notes.append(fallback)
        if r["metric"] == "ece":
            notes.append(f"primary 10-bin ECE; {ECE_BIAS_NOTE}")
        out.append({**r, "note": "; ".join(n for n in notes if n)})
    return out


def _validation_lines(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """Validation point values with the race-cluster CI, per model (NaN shown as undefined)."""
    lines = ["validation 2024 (race-cluster 95% CI; DEVELOPMENT, noisy):"]
    for model in ("lr_calibrated", "gbt_calibrated", "majority", "rule"):
        parts = []
        for r in rows:
            if r["model"] == model and r["ci_kind"] == "race_cluster":
                v, lo, hi = (float(r[c]) for c in ("value", "ci_low", "ci_high"))
                text = "undefined" if math.isnan(v) else f"{v:.3f} [{lo:.3f}, {hi:.3f}]"
                parts.append(f"{r['metric']}={text}")
        if parts:
            lines.append(f"  {model}: " + " ".join(parts))
    return lines


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
        "ece_5bin_raw": M.ece(y, raw, n_bins=5) if y.size else M.NAN,
        "ece_5bin_calibrated": M.ece(y, calibrated, n_bins=5) if y.size else M.NAN,
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
    "decision_threshold",
)
"""``decision_threshold``: ``metrics.DECISION_THRESHOLD`` on the F1 rows, empty otherwise."""
THRESHOLD_METRICS = ("f1_positive", "macro_f1")
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
    "ece_5bin_raw",
    "ece_5bin_calibrated",
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
    eval_rows = [
        {
            **extra,
            **r,
            "decision_threshold": M.DECISION_THRESHOLD if r["metric"] in THRESHOLD_METRICS else "",
        }
        for r in rows
    ]
    write_csv(paths["model_eval"], eval_rows, EVAL_COLUMNS, cfg.stamp)
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
        "decision_threshold": M.DECISION_THRESHOLD,
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
    p.add_argument(
        "--no-test",
        action="store_true",
        help="--source full DEVELOPMENT run: 2021-2024 only, the 2025 season is never loaded",
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
            no_test=args.no_test,
        )
        check_results_dir(cfg.results_dir)
        check_models_dir(cfg.models_dir)
        check_no_previous_test_run(cfg)
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
