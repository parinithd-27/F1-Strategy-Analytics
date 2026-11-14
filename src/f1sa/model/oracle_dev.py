"""ORACLE variant on DEVELOPMENT data only (Step 16, traceability #30; ``docs/model_spec_v2.md``
§4): the ``real`` feature set plus the realised out-lap delta of the attacker's stop.

**ORACLE: realised out-lap delta, not usable at decision time.** The column is measured on the
out-lap, after the decision lap, so it shows only how much of the outcome the out-lap explains.
``docs/model_spec_v2.md`` §1 does not list ORACLE among the test-season feature sets, so 2025 is
never loaded here: only the 2021-2024 partitions are read (train 2021-2023, hyperparameters and
Platt calibration on 2024 validation, leave-one-season-out over 2021-2024), at both settings. There
is no ``--allow-test-season`` option.

The column is added after ``dataset.build_dataset`` (null -> 0.0 plus a ``_missing`` indicator, as
every numeric input), so the feature registry, which bans ``oracle_`` columns from every model
feature set, is unchanged. Rows are put in ``canonical_order`` after the join so GBT fits are
reproducible.

``make oracle-dev-full`` writes ``data/oracle/<run_id>/``; ``RECORD=1`` (``--record``) writes
``./results/model_dev_{eval,loso}_oracle{,_G5.0_k3}.csv``, refused if any of them exists.
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ORACLE_COLUMN = "oracle_attacker_realised_outlap_delta_s"
ASSEMBLED = (f"f_{ORACLE_COLUMN}", f"f_{ORACLE_COLUMN}_missing")
VARIANT = "real_oracle"
LABEL = "ORACLE: realised out-lap delta, not usable at decision time"
STAMP = (
    f"DEVELOPMENT, {LABEL}. 2021-2024 only (validation 2024, LOSO 2021-2024); the 2025 test "
    "season is never loaded"
)
DEV_SEASONS: tuple[int, ...] = (2021, 2022, 2023, 2024)
SETTINGS: tuple[tuple[float, int], ...] = ((3.5, 2), (5.0, 3))
OUT_ROOT = Path("data/oracle")
RESULTS_DIR = Path("results")
EXIT_OK, EXIT_FAILED, EXIT_REFUSED = 0, 1, 2


class OracleError(RuntimeError):
    """The ORACLE run is refused or its data are inconsistent."""


def setting_suffix(g: float, k: int) -> str:
    """'' for the primary setting, else ``_G5.0_k3`` (as the development files)."""
    return "" if (float(g), int(k)) == (3.5, 2) else f"_G{float(g):.1f}_k{int(k)}"


def output_paths(results_dir: Path, g: float, k: int) -> dict[str, Path]:
    sfx = setting_suffix(g, k)
    return {
        "eval": results_dir / f"model_dev_eval_oracle{sfx}.csv",
        "loso": results_dir / f"model_dev_loso_oracle{sfx}.csv",
    }


def all_outputs(results_dir: Path) -> list[Path]:
    return [p for g, k in SETTINGS for p in output_paths(results_dir, g, k).values()]


def check_outputs_absent(results_dir: Path) -> None:
    existing = [str(p) for p in all_outputs(results_dir) if p.exists()]
    if existing:
        raise OracleError(f"{existing} exist and are never overwritten")


def check_seasons(seasons: Iterable[int]) -> None:
    """Only development seasons: anything outside 2021-2024 (above all 2025) is refused."""
    bad = sorted(set(int(s) for s in seasons) - set(DEV_SEASONS))
    if bad:
        raise OracleError(f"ORACLE is development-only; seasons {bad} must never be read")


def with_oracle(ds: Any, frame: Any, g: float, k: int) -> Any:
    """``ds`` (the ``real`` development dataset) with the ORACLE column and its indicator joined
    to train and validation; the attempt count must not change."""
    from pyspark.sql import functions as F

    from f1sa.model.dataset import ORDER_KEY, canonical_order

    if ORACLE_COLUMN not in frame.columns:
        raise OracleError(f"undercut_attempts has no {ORACLE_COLUMN} column")
    oracle = (
        frame.where(
            (F.col("gap_threshold_s") == F.lit(float(g))) & (F.col("response_window_laps") == k)
        )
        .select(*ORDER_KEY, F.col(ORACLE_COLUMN).cast("double").alias("_oracle"))
        .dropDuplicates(list(ORDER_KEY))
    )
    value, missing = ASSEMBLED

    def add(part: Any) -> Any:
        n = part.count()
        out = (
            part.join(oracle, list(ORDER_KEY), "left")
            .withColumn(value, F.coalesce(F.col("_oracle"), F.lit(0.0)))
            .withColumn(missing, F.col("_oracle").isNull().cast("double"))
            .drop("_oracle")
        )
        out = canonical_order(out).cache()
        if out.count() != n:
            raise OracleError(f"the ORACLE join changed the attempt count: {n} -> {out.count()}")
        return out

    return dataclasses.replace(
        ds, train=add(ds.train), validation=add(ds.validation), columns=(*ds.columns, *ASSEMBLED)
    )


def run_setting(
    spark: Any, g: float, k: int, results_dir: Path, out: Callable[[str], None] = print
) -> list[dict[str, Any]]:
    """Train, tune, calibrate and evaluate the ORACLE variant on 2024, then LOSO; write the two
    files of this setting and return the validation rows."""
    import numpy as np

    from f1sa.model import metrics as M
    from f1sa.model.calibrate import Calibrator, choose
    from f1sa.model.dataset import build_dataset
    from f1sa.model.evaluate import metric_rows, score_split, supplementary_ece_rows
    from f1sa.model.loso import LOSO_COLUMNS, run_loso
    from f1sa.model.pipeline import (
        DEV_SPLIT,
        EVAL_COLUMNS,
        THRESHOLD_METRICS,
        RunConfig,
        _identity,
        _majority,
        _rule,
        annotate_rows,
        load_frame,
        write_csv,
    )
    from f1sa.model.train import KINDS, TrainConfig, tune

    cfg = RunConfig(source="full", g=g, k=k, feature_set="real", no_test=True)
    frame = load_frame(spark, cfg)
    check_seasons(r[0] for r in frame.select("season").distinct().collect())
    ds = with_oracle(
        build_dataset(frame, g=g, k=k, feature_set="real", split=DEV_SPLIT), frame, g, k
    )
    out(f"ORACLE G={g:g} k={k}: {ds.counts}; columns {len(ds.columns)} ({LABEL})")
    models = {kind: tune(kind, ds, TrainConfig()) for kind in KINDS}
    for tm in models.values():
        out(f"  {tm.kind}: {tm.tuning_status}; params {tm.params}")
    majority, rule = _majority(ds), _rule(ds)
    raw = score_split("validation", ds.validation, models, _identity(), majority, rule)
    seasons = np.asarray(raw.rows["season"])
    cals: dict[str, Calibrator] = {}
    for kind in KINDS:
        cals[kind], _briers = choose(spark, seasons, raw.labels, raw.scores[kind])
        out(f"  {kind} calibration: {cals[kind].reason or cals[kind].method}")
    val = score_split("validation", ds.validation, models, cals, majority, rule)
    rows = annotate_rows(metric_rows(val, n_boot=cfg.n_boot, seed=cfg.seed), val, rule)
    rows += supplementary_ece_rows(val, n_boot=cfg.n_boot, seed=cfg.seed)
    extra = {
        "source": "full",
        "smoke_test": "false",
        "feature_set": VARIANT,
        "g": f"{float(g):g}",
        "k": int(k),
    }
    eval_rows = [
        {
            **extra,
            **r,
            "note": "; ".join(x for x in (LABEL, str(r.get("note") or "")) if x),
            "decision_threshold": M.DECISION_THRESHOLD if r["metric"] in THRESHOLD_METRICS else "",
        }
        for r in rows
    ]
    loso = [{**r, "note": "; ".join(x for x in (LABEL, str(r.get("note") or "")) if x)}
            for r in run_loso(spark, ds, TrainConfig())]  # fmt: skip
    paths = output_paths(results_dir, g, k)
    write_csv(paths["eval"], eval_rows, EVAL_COLUMNS, STAMP)
    write_csv(paths["loso"], loso, LOSO_COLUMNS, STAMP)
    for name, p in paths.items():
        out(f"wrote {name}: {p}")
    for part in (ds.train, ds.validation):
        part.unpersist()
    return eval_rows


def summary_lines(rows: Sequence[dict[str, Any]], g: float, k: int) -> list[str]:
    out = []
    for r in rows:
        if (
            r["model"] in ("lr_calibrated", "gbt_calibrated")
            and r["ci_kind"] == "race_cluster"
            and r["metric"] in ("pr_auc", "brier")
        ):
            out.append(
                f"  G={g:g} k={k} {r['model']} {r['metric']} = {float(r['value']):.3f} "
                f"[{float(r['ci_low']):.3f}, {float(r['ci_high']):.3f}] ({LABEL})"
            )
    return out


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="experiments/oracle_dev.py", description=__doc__.split("\n\n")[0]
    )
    p.add_argument("--source", choices=("full",), required=True)
    p.add_argument("--record", action="store_true", help="write ./results/model_dev_*_oracle*")
    p.add_argument("--out-root", type=Path, default=OUT_ROOT)
    p.add_argument("--master", default="local[2]")
    return p.parse_args(list(argv) if argv is not None else None)


def main(
    argv: Iterable[str] | None = None,
    *,
    spark_factory: Callable[[str], Any] | None = None,
    setting: Callable[..., list[dict[str, Any]]] = run_setting,
    out: Callable[[str], None] = print,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> int:
    args = parse_args(argv)
    results_dir = (
        RESULTS_DIR if args.record else args.out_root / f"full_{now().strftime('%Y%m%dT%H%M%SZ')}"
    )
    try:
        check_outputs_absent(results_dir)
    except OracleError as exc:
        out(f"refused: {exc}")
        return EXIT_REFUSED
    out(STAMP)
    if spark_factory is not None:
        spark = spark_factory(args.master)
    else:
        from f1sa.etl.spark import build_spark

        spark = build_spark("f1sa-oracle-dev", args.master)
    try:
        for g, k in SETTINGS:
            rows = setting(spark, g, k, results_dir, out)
            for line in summary_lines(rows, g, k):
                out(line)
    except OracleError as exc:
        out(f"error: {exc}")
        return EXIT_FAILED
    finally:
        if spark_factory is None:
            spark.stop()
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
