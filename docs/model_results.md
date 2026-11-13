# Model results: the 2025 test season (Step 9f)

This page summarises the single pre-registered 2025 test run (`docs/model_spec.md`, `docs/model_spec_v2.md`).
Every number is copied from the committed run outputs `results/model_eval{,_real_tyre,_real_ti}_G{3.5_k2,5.0_k3}.csv`
(test split, race-cluster intervals). Nothing was retrained, rerun or re-evaluated to produce it. The same rows,
with intervals for every metric, are in `results/model_test_summary.csv`.

- Primary: feature set `real`, calibrated LR, G=3.5 s, k=2. It was named before the test run and is not re-chosen
  here. `real_tyre` and `real_ti` are pre-registered ablations. G=5.0, k=3 is the sensitivity setting.
- The intervals are 95% percentile intervals from 1000 race-cluster bootstrap resamples (seed 42).
- F1 is for the positive class at the fixed threshold 0.5 on calibrated probabilities. It is conservative at this
  base rate (`docs/model_spec.md`, Decision threshold).
- ECE with 10 bins is primary and 5 bins is supplementary. Bootstrap ECE intervals are biased upward.
- The majority and rule baselines are fixed reference points that do not depend on the feature set. Their values
  are identical in all three files of a setting (checked), so each appears once.
- 2025 is small: 49 labelled attempts in 19 races at the primary setting (17 successes, a base rate of 0.347) and
  60 attempts in 20 races at the sensitivity setting (base rate 0.333). The intervals are correspondingly wide.

## Results

**G=3.5, k=2 (primary setting): 49 attempts, 19 races**

| Feature set | Model | F1 (pos.) | PR-AUC [95% race-cluster CI] | ROC-AUC | Brier | ECE (10 bins) | ECE (5 bins) |
|---|---|---|---|---|---|---|---|
| `real` | **calibrated LR (PRIMARY)** | 0.571 | **0.670 [0.421, 0.898]** | 0.792 | 0.177 | 0.107 | 0.054 |
| `real` | calibrated GBT | 0.300 | 0.703 [0.505, 0.872] | 0.752 | 0.194 | 0.113 | 0.081 |
| `real_tyre` | calibrated LR | 0.417 | 0.691 [0.483, 0.865] | 0.770 | 0.179 | 0.109 | 0.109 |
| `real_tyre` | calibrated GBT | 0.516 | 0.653 [0.416, 0.855] | 0.731 | 0.202 | 0.138 | 0.058 |
| `real_ti` | calibrated LR | 0.348 | 0.630 [0.403, 0.819] | 0.730 | 0.193 | 0.123 | 0.088 |
| `real_ti` | calibrated GBT | 0.516 | 0.605 [0.411, 0.784] | 0.702 | 0.210 | 0.093 | 0.093 |
| — | majority baseline | 0.000 | 0.347 [0.162, 0.517] | 0.500 | 0.229 | 0.051 | 0.051 |
| — | rule baseline | 0.357 | 0.513 [0.246, 0.799] | 0.702 | 0.273 | 0.258 | 0.258 |

**G=5.0, k=3 (sensitivity setting): 60 attempts, 20 races**

| Feature set | Model | F1 (pos.) | PR-AUC [95% race-cluster CI] | ROC-AUC | Brier | ECE (10 bins) | ECE (5 bins) |
|---|---|---|---|---|---|---|---|
| `real` | calibrated LR | 0.562 | 0.723 [0.541, 0.858] | 0.799 | 0.171 | 0.097 | 0.045 |
| `real` | calibrated GBT | 0.471 | 0.617 [0.375, 0.801] | 0.741 | 0.189 | 0.069 | 0.046 |
| `real_tyre` | calibrated LR | 0.429 | 0.708 [0.529, 0.864] | 0.791 | 0.176 | 0.103 | 0.072 |
| `real_tyre` | calibrated GBT | 0.486 | 0.666 [0.416, 0.824] | 0.739 | 0.190 | 0.127 | 0.127 |
| `real_ti` | calibrated LR | 0.370 | 0.650 [0.478, 0.813] | 0.766 | 0.184 | 0.109 | 0.086 |
| `real_ti` | calibrated GBT | 0.513 | 0.635 [0.373, 0.806] | 0.740 | 0.194 | 0.127 | 0.051 |
| — | majority baseline | 0.000 | 0.333 [0.200, 0.457] | 0.500 | 0.226 | 0.062 | 0.062 |
| — | rule baseline | 0.312 | 0.480 [0.257, 0.720] | 0.686 | 0.272 | 0.253 | 0.253 |

## Does the primary model beat the rule baseline?

On 2025, the primary model (`real` calibrated LR, G=3.5, k=2) scores better than the rule baseline on every
reported metric, but every interval overlaps:

| Metric | Primary [95% race-cluster CI] | Rule baseline [95% race-cluster CI] |
|---|---|---|
| PR-AUC (headline) | 0.670 [0.421, 0.898] | 0.513 [0.246, 0.799] |
| Brier (headline, lower is better) | 0.177 [0.106, 0.232] | 0.273 [0.168, 0.353] |
| ROC-AUC | 0.792 [0.667, 0.929] | 0.702 [0.516, 0.860] |
| F1 (positive class, 0.5) | 0.571 [0.323, 0.778] | 0.357 [0.000, 0.542] |
| ECE, 10 bins (lower is better) | 0.107 [0.076, 0.234] | 0.258 [0.162, 0.360] |

The point estimates favour the primary model, but the intervals are separate per-model intervals, not an interval
for the difference. With 49 attempts in 19 races they overlap widely, and **no significance claim is made**
(`docs/model_spec.md`, Headline test results). The 2025 result is reported as it is.

For context only: in development, no model beat the rule on LOSO at the primary setting (`docs/model_spec_v2.md`
§7). LOSO uses raw scores at the first grid point, so those numbers are not comparable with this table.

Against the majority baseline (traceability #17), the primary model's F1 for the positive class is 0.571 against
0.000, and its PR-AUC is 0.670 against 0.347.

## Ablations (`real_tyre`, `real_ti`)

- Neither ablation changes the primary feature set or the deployed model (`docs/model_spec_v2.md` §4).
- GBT differences of 0.043 PR-AUC or less come from row order alone and are not interpretable (§5).
- Open item: §4 also requires a **paired race-cluster bootstrap interval for each feature-set difference**
  (`real_tyre − real`, `real_ti − real`, `real_ti − real_tyre`, per model and setting, PR-AUC and Brier).
  - These intervals are not in any committed output: the test runs save metrics, not per-attempt scores.
  - So they are not reported here, and no difference is interpreted.
  - They are computed in Step 16 from the saved frozen test-run models (no retraining, re-tuning or
    re-calibration). An exact-match check against the committed `model_eval` metrics runs first, and on any
    mismatch no interval is written.

## 2025 attempt-level missing rates (`real_tyre`, `real_ti`)

Source: `results/missing_rates_{real_tyre,real_ti}_G{3.5_k2,5.0_k3}.csv`, 2025 rows. Features with no missing
value are left out. The rates are identical in the two sets for every feature they share.

| Feature | Provenance | G=3.5, k=2 (n=49) | G=5.0, k=3 (n=60) |
|---|---|---|---|
| `attacker_compound_ti`, `defender_compound_ti` | TracingInsights | 1 (2.0%) | 2 (3.3%) |
| `attacker_tyre_age_laps_ti`, `defender_tyre_age_laps_ti`, `tyre_age_delta_laps_ti` | TracingInsights | 1 (2.0%) | 2 (3.3%) |
| `gap_ahead_change_1l` | Jolpica | 5 (10.2%) | 5 (8.3%) |
| `gap_ahead_change_3l` | Jolpica | 9 (18.4%) | 10 (16.7%) |
| `attacker_deg_slope_s_per_lap`, `deg_slope_delta_s_per_lap` | Jolpica | 1 (2.0%) | 1 (1.7%) |
| `rejoin_nearest_ahead_s` | Jolpica | 30 (61.2%) | 37 (61.7%) |

- `real_ti` telemetry features (speed, throttle, brake, gear, rpm, for both attacker and defender): 0 missing
  at both settings, and 0 attempts with partial telemetry.
- Missing values become 0.0 plus a `_missing` indicator (categorical: `__unknown`). No race or attempt was
  dropped, excluded or down-weighted (`docs/model_spec_v2.md` §6).

**Miami R6 / Belgium R13.** Two 2025 races have low lap-level compound coverage in TracingInsights, known from
the Step 4C audit before the spec freeze:
- Miami R6: 55.6% of laps have a compound.
- Belgium R13: 93.5%.

Both races stay in the test set unchanged. At the attempt level, the decision-lap compound and tyre age are
missing for only 1 of 49 attempts at the primary setting and 2 of 60 at the sensitivity setting. Which races those
attempts come from is not in the committed outputs, and it was not looked up for this summary.
