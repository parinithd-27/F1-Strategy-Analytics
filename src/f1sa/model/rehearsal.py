"""A deterministic, GENERATED undercut-attempts table for rehearsing the model pipeline.

**Not F1 data.** It has the columns of ``undercut_attempts`` plus the derived model inputs, for
seasons 2021-2025, and a known logistic signal, so the whole pipeline (including the test-season
evaluation) can run in unit tests and in ``experiments/model_eval.py --source rehearsal`` without
ever opening the real 2025 season.

Signal (per attempt, with a per-race random effect so attempts in one race are correlated)::

    z = 0.4 - 1.1*gap_s - 1.5*pace_delta_last3_s
        + 0.12*(defender_stint_laps - attacker_stint_laps) + 6*defender_deg_slope_s_per_lap
        + race_effect,      race_effect ~ N(0, 0.6)
    label ~ Bernoulli(sigmoid(z))

About 15% of attempts are ``defender_stayed_out`` and 5% ``excluded_neutralised`` (no label).
Each attempt is emitted for G = 3.5 and G = 5.0 (k = 2), so the (G, k) filter has work to do.
Some priors are null (with the matching ``*_source = 'none'``) to exercise the missing flags.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StructField, StructType

from f1sa.features import schemas as FS
from f1sa.model import features as MF

SEASONS = (2021, 2022, 2023, 2024, 2025)
STAMP = "REHEARSAL: GENERATED DATA, NOT F1; pipeline check only"


def extras_schema() -> list[StructField]:
    """The derived model-input columns ``dataset.with_derived_inputs`` adds."""
    out = [
        StructField("attacker_stint_laps", IntegerType()),
        StructField("defender_stint_laps", IntegerType()),
    ]
    for side in ("attacker", "defender"):
        for col in MF.TELEMETRY_COLUMNS:
            out.append(
                StructField(
                    f"{side}_{col}", IntegerType() if col == "gear_changes" else DoubleType()
                )
            )
        out.append(StructField(f"{side}_telemetry_is_synthetic", BooleanType()))
    return out


def model_input_schema() -> StructType:
    return StructType([*FS.UNDERCUT_ATTEMPTS.fields, *extras_schema()])


def generate_rows(
    seed: int = 7,
    *,
    races_per_season: int = 20,
    attempts_per_race: int = 12,
    seasons: tuple[int, ...] = SEASONS,
    synthetic_flags: bool = True,
) -> list[dict[str, Any]]:
    """Plain dict rows (deterministic for a seed)."""
    if races_per_season < 1 or attempts_per_race < 1:
        raise ValueError("races_per_season and attempts_per_race must be >= 1")
    rng = np.random.default_rng(seed)
    rows: list[dict[str, Any]] = []
    for season in seasons:
        for rnd in range(1, races_per_season + 1):
            effect = rng.normal(0.0, 0.6)
            for i in range(attempts_per_race):
                gap = float(rng.uniform(0.2, 3.5))
                pace = float(rng.normal(0.0, 0.4))
                a_stint = int(rng.integers(5, 25))
                d_stint = max(1, a_stint + int(rng.integers(-3, 9)))
                slope = float(rng.normal(0.05, 0.03))
                z = 0.4 - 1.1 * gap - 1.5 * pace + 0.12 * (d_stint - a_stint) + 6.0 * slope + effect
                label = int(rng.random() < 1.0 / (1.0 + math.exp(-z)))
                kind = rng.random()
                category = "success" if label else "fail"
                lab: int | None = label
                if kind < 0.15:
                    category, lab = "defender_stayed_out", None
                elif kind < 0.20:
                    category, lab = "excluded_neutralised", None
                has_loss = rng.random() > 0.1
                has_resid = rng.random() > 0.15
                lap = 10 + i * 3
                base: dict[str, Any] = {
                    "season": season,
                    "round": rnd,
                    "attacker_id": f"a{i:02d}",
                    "defender_id": f"d{i:02d}",
                    "lap": lap,
                    "decision_lap": lap - 1,
                    "response_window_laps": 2,
                    "detector_eligible": True,
                    "outcome_category": category,
                    "label": lab,
                    "exclusion_reasons": []
                    if lab is not None or category.startswith("def")
                    else ["neutralised"],
                    "gap_s": round(gap, 3),
                    "gap_ahead_change_1l": round(float(rng.normal(0, 0.3)), 3),
                    "gap_ahead_change_3l": round(float(rng.normal(0, 0.6)), 3),
                    "attacker_position": int(rng.integers(2, 20)),
                    "attacker_tyre_age_laps": a_stint + int(rng.integers(0, 3)),
                    "defender_tyre_age_laps": d_stint + int(rng.integers(0, 3)),
                    "attacker_compound": str(rng.choice(["SOFT", "MEDIUM", "HARD"])),
                    "defender_compound": str(rng.choice(["SOFT", "MEDIUM", "HARD"])),
                    "compound_is_synthetic": synthetic_flags,
                    "attacker_pace_last3_s": round(90.0 + pace, 3),
                    "defender_pace_last3_s": 90.0,
                    "pace_delta_last3_s": round(pace, 3),
                    "attacker_deg_slope_s_per_lap": round(float(rng.normal(0.04, 0.03)), 4),
                    "defender_deg_slope_s_per_lap": round(slope, 4),
                    "laps_remaining": int(rng.integers(5, 50)),
                    "pit_loss_prior_s": round(float(rng.normal(21.0, 2.0)), 3)
                    if has_loss
                    else None,
                    "pit_loss_source": "circuit_prior" if has_loss else "none",
                    "rejoin_traffic_cars": int(rng.integers(0, 3)),
                    "attacker_expected_outlap_pace_residual_s": round(float(rng.normal(0, 0.5)), 3)
                    if has_resid
                    else None,
                    "attacker_expected_outlap_pace_source": "driver_team" if has_resid else "none",
                    "defender_expected_outlap_pace_residual_s": round(float(rng.normal(0, 0.5)), 3),
                    "defender_expected_outlap_pace_source": "team",
                    "attacker_expected_stop_delta_s": round(float(rng.normal(0, 0.4)), 3),
                    "defender_expected_stop_delta_s": round(float(rng.normal(0, 0.4)), 3),
                    "attacker_stint_laps": a_stint,
                    "defender_stint_laps": d_stint,
                }
                base["tyre_age_delta_laps"] = (
                    base["defender_tyre_age_laps"] - base["attacker_tyre_age_laps"]
                )
                for side in ("attacker", "defender"):
                    base[f"{side}_mean_speed_kph"] = round(float(rng.normal(200, 10)), 2)
                    base[f"{side}_max_speed_kph"] = round(float(rng.normal(320, 8)), 2)
                    base[f"{side}_pct_full_throttle"] = round(float(rng.uniform(40, 75)), 2)
                    base[f"{side}_brake_fraction"] = round(float(rng.uniform(0.1, 0.25)), 3)
                    base[f"{side}_gear_changes"] = int(rng.integers(40, 70))
                    base[f"{side}_mean_rpm"] = round(float(rng.normal(10500, 300)), 1)
                    base[f"{side}_telemetry_is_synthetic"] = synthetic_flags
                for g in (3.5, 5.0):
                    rows.append({**base, "gap_threshold_s": g, "is_default_params": g == 3.5})
    return rows


def generate(spark: SparkSession, seed: int = 7, **kwargs: Any) -> DataFrame:
    """The rehearsal table as a Spark DataFrame with the model-input schema."""
    schema = model_input_schema()
    rows = generate_rows(seed, **kwargs)
    data = [tuple(r.get(f.name) for f in schema.fields) for r in rows]
    return spark.createDataFrame(data, schema)
