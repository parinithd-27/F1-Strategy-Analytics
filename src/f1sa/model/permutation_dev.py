"""DEVELOPMENT: permutation importance for **both** models on calibrated validation scores.

The development pipeline (``pipeline.run``) computes permutation importance for the selected model
only, on raw scores. This entry point fills the gap without touching any pipeline output:

1. it rebuilds the same dataset (``pipeline.load_frame`` in ``--no-test`` mode: only the
   2021-2024 partitions are listed and read; 2025 is never loaded) and refits LR and GBT with the
   same grid and seed;
2. it checks the refit against the development run's metadata (counts, chosen parameters,
   validation PR-AUC) and against the selected model's permutation rows in its
   ``feature_importance`` file; any mismatch exits 4 and writes nothing;
3. it writes ``model_dev_permutation{_feature_set}{_setting}.csv`` with the PR-AUC drop of every
   registry feature for ``lr`` and ``gbt``, on calibrated scores (the run's saved calibrators) and
   on raw scores. With a Platt calibrator of slope a > 0 the two are identical (PR-AUC depends on
   the ranking only).

    python experiments/model_dev_permutation.py --feature-set real_ti [--g 5.0 --k 3]
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from f1sa.features.undercut_rule import DEFAULT_GAP_S, DEFAULT_WINDOW_LAPS
from f1sa.model import features as MF
from f1sa.model.calibrate import Calibrator
from f1sa.model.dataset import build_dataset
from f1sa.model.explain import permutation_importance
from f1sa.model.pipeline import DEV_SPLIT, RunConfig, RunError, load_frame, write_csv
from f1sa.model.train import KINDS, TrainedModel, tune

COLUMNS = ("model", "scores", "feature", "value", "std", "note")
TOLERANCE = 1e-12


class ReproductionError(RuntimeError):
    """The refit does not reproduce the development run; nothing is written."""


def read_selected_permutation(path: Path) -> dict[str, float]:
    """``feature -> value`` of the permutation rows in a development ``feature_importance`` file."""
    lines = [x for x in path.read_text(encoding="utf-8").splitlines() if not x.startswith("#")]
    return {
        r["feature"]: float(r["value"]) if r["value"] != "" else math.nan
        for r in csv.DictReader(lines)
        if r["method"] == "permutation_pr_auc_drop"
    }


def _key(v: float) -> str:
    """6-decimal text as the CSV writes it ('' for NaN), so NaN compares equal to NaN."""
    return "" if math.isnan(v) else f"{v:.6f}"


def check_reproduction(
    meta: Mapping[str, Any],
    counts: Mapping[str, Any],
    models: Mapping[str, TrainedModel],
) -> None:
    if dict(counts) != meta["counts"]:
        raise ReproductionError(f"counts {dict(counts)} != development run {meta['counts']}")
    for kind, tm in models.items():
        ref = meta["models"][kind]
        if tm.params != ref["params"]:
            raise ReproductionError(f"{kind} params {tm.params} != {ref['params']}")
        a, b = tm.validation_pr_auc, float(ref["validation_pr_auc"])
        if not (
            math.isclose(a, b, rel_tol=0.0, abs_tol=TOLERANCE) or (math.isnan(a) and math.isnan(b))
        ):
            raise ReproductionError(f"{kind} validation PR-AUC {a!r} != {b!r}")


def check_selected(
    rows: Sequence[Mapping[str, Any]], selected: str, reference: Mapping[str, float]
) -> None:
    """The refit's raw permutation rows of the selected model equal the file (6 decimals)."""
    got = {
        str(r["feature"]): _key(float(r["value"]))
        for r in rows
        if r["model"] == selected and r["scores"] == "raw"
    }
    want = {f: _key(v) for f, v in reference.items()}
    if got != want:
        diff = sorted(f for f in set(got) | set(want) if got.get(f) != want.get(f))
        raise ReproductionError(f"{selected} permutation rows differ for {diff[:5]}")


def run_permutation(
    spark: SparkSession, cfg: RunConfig, out: Callable[[str], None] = print
) -> Path:
    if not cfg.no_test or cfg.source != "full":
        raise RunError(
            "model_dev_permutation runs in development mode only (--source full --no-test)"
        )
    meta_path = cfg.output("metadata", "json")
    fi_path = cfg.output("feature_importance", "csv")
    if not meta_path.is_file() or not fi_path.is_file():
        raise RunError(f"run the development model first: {meta_path} / {fi_path} missing")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    df = load_frame(spark, cfg)
    ds = build_dataset(df, g=cfg.g, k=cfg.k, feature_set=cfg.feature_set, split=DEV_SPLIT)
    models = {kind: tune(kind, ds, cfg.train) for kind in KINDS}
    check_reproduction(meta, ds.counts, models)
    cals = {k: Calibrator.from_json(meta["calibrators"][k]) for k in KINDS}
    rows: list[dict[str, Any]] = []
    for kind in KINDS:
        for scores, transform in (("calibrated", cals[kind].apply), ("raw", None)):
            rows += [
                {**r, "scores": scores, "note": f"{r['note']}; {scores} scores"}
                for r in permutation_importance(
                    spark,
                    models[kind],
                    ds.validation,
                    ds.columns,
                    seed=cfg.seed,
                    transform=transform,
                )
            ]
    check_selected(rows, meta["selected_model"], read_selected_permutation(fi_path))
    target = cfg.output("permutation", "csv")
    write_csv(target, rows, COLUMNS, cfg.stamp)
    out(f"reproduction MATCH (counts, params, validation PR-AUC, {meta['selected_model']} rows)")
    out(f"wrote permutation: {target}")
    return target


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="experiments/model_dev_permutation.py")
    p.add_argument("--feature-set", choices=sorted(MF.FEATURE_SETS), required=True)
    p.add_argument("--g", type=float, default=DEFAULT_GAP_S)
    p.add_argument("--k", type=int, default=DEFAULT_WINDOW_LAPS)
    p.add_argument("--master", default="local[2]")
    args = p.parse_args(list(argv) if argv is not None else None)
    try:
        cfg = RunConfig(
            source="full", no_test=True, feature_set=args.feature_set, g=args.g, k=args.k
        )
    except RunError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    from f1sa.etl.spark import build_spark

    spark = build_spark("f1sa-model-dev-permutation", args.master)
    try:
        run_permutation(spark, cfg)
    except RunError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ReproductionError as exc:
        print(f"error: reproduction MISMATCH, nothing written: {exc}", file=sys.stderr)
        return 4
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
