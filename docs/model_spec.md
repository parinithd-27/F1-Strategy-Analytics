# Model specification (pre-registered for the single test-season run)

**Status: FROZEN from the commit that adds this file.** No feature, feature set, grid, calibration rule,
selection rule, decision threshold, metric definition, headline metric or evaluated setting may change after any
test-season (2025) number has been seen. A change before the test run requires a new, dated version of this file
committed before the run.

The label definition is frozen separately (`docs/label_definition.md`, git tag `label-definition-v1`).

**Disclosure.** The development results (Step 9b, `make model-dev-full`: 2021-2023 training, 2024 validation,
only the 2021-2024 feature partitions read) were seen before this freeze. The choice of the primary model below
uses them. No 2025 data, count or schema has been read by any model step.

## Data and split
- Labelled attempts only (`success` / `fail`), from `undercut_attempts` (`docs/label_definition.md`).
- Chronological split: train 2021-2023, validation 2024, test 2025. Validation is used for hyperparameter selection
  and for calibration. The test season is opened once per evaluated setting, by `model/evaluate.evaluate_test` only.
- Class weights `n / (2 * n_class)` from the training rows only.
- Labelled attempts at G=3.5, k=2: train 108 (32 positive), validation 49 (16 positive, base rate 0.33), test 49.
  At G=5.0, k=3: train 184 (50 positive), validation 64 (22 positive), test 60. Test counts come from the label
  counts CSV; no model step read them.

## Settings
| Role | G (gap threshold, s) | k (response window, laps) |
|---|---|---|
| **Primary** | 3.5 | 2 |
| Sensitivity | 5.0 | 3 |

Both were decided from attempt counts before any model ran. The sensitivity setting is reported alongside the
primary and is never used to choose a setting.

## Feature set: `real` (only)
`model/features.py`, provenances `jolpica` and `stint_boundary`. In full mode a feature with synthetic-capable
provenance is refused (`train.check_feature_policy`), and `--source full` accepts only `real`. The 21 inputs
become 52 assembled columns (numeric: constant 0.0 plus a `_missing` indicator, never imputed from data;
categorical: one-hot plus `unknown`):

`gap_s`, `gap_ahead_change_1l`, `gap_ahead_change_3l`, `attacker_position`, `pace_delta_last3_s`,
`attacker_deg_slope_s_per_lap`, `defender_deg_slope_s_per_lap`, `deg_slope_delta_s_per_lap`, `laps_remaining`,
`pit_loss_prior_s`, `pit_loss_source`, `rejoin_traffic_cars`, `rejoin_nearest_ahead_s`,
`attacker_expected_outlap_pace_residual_s`, `attacker_expected_outlap_pace_source`,
`defender_expected_outlap_pace_residual_s`, `defender_expected_outlap_pace_source`,
`attacker_expected_stop_delta_s`, `defender_expected_stop_delta_s`, `attacker_stint_laps`, `defender_stint_laps`.

No tyre-state (compound, tyre age) or telemetry feature is used.

## Models and grids (exactly as run, `model/train.py`)
Each grid point is fitted on the training seasons; the one with the highest validation PR-AUC is kept (ties: the
first). Seed 42.

| Model | Pipeline | Grid |
|---|---|---|
| LR | VectorAssembler → StandardScaler (mean, std) → LogisticRegression (maxIter 200, weightCol) | regParam ∈ {0.0, 0.01, 0.1} × elasticNetParam ∈ {0.0, 0.5} (6 points) |
| GBT | VectorAssembler → GBTClassifier (weightCol, seed 42) | maxDepth ∈ {2, 3, 5} × maxIter ∈ {20, 50} × stepSize ∈ {0.05, 0.1} (12 points) |

Selected in development: LR regParam 0.1, elasticNetParam 0.5; GBT maxDepth 2, maxIter 50, stepSize 0.05, in
both settings. The test run refits on the same data with the same seed and grid.

## Calibration (`model/calibrate.choose`, exactly as coded)
Fitted per model on validation-season scores only:
- **fewer than 300 labelled validation rows:** Platt (monotonic, slope ≥ 0) is used **without comparing** with
  isotonic;
- **300 or more:** isotonic vs Platt is chosen by validation Brier;
- identity only when validation cannot support a fit (fewer than 2 rows or one class).

With 49 (G=3.5, k=2) and 64 (G=5.0, k=3) validation rows, **Platt is used for both models in both settings**.

## Primary model
**Calibrated LR** (`lr_calibrated`), because it has the lowest calibrated validation Brier: 0.168 against 0.205
for calibrated GBT at G=3.5, k=2 (0.176 against 0.193 at G=5.0, k=3). Both LR and GBT, raw and calibrated, are
reported on the test season; the primary model is named in advance and is not re-chosen on test results.

The pipeline's `selected_model` (which model the reliability and confusion plots and the permutation importance
use) is chosen by validation PR-AUC. In development it is also LR in both settings (0.594 against 0.560, and
0.738 against 0.590), so it agrees with the primary model.

## Baselines: fixed reference points (traceability #17)
The majority-class baseline (the training positive rate) and the rule baseline (expected fresh-tyre gain vs gap,
decision-time inputs only, `model/baselines.py`) are **fixed reference points**. They are scored on 2025 next to
the models, but they are **never candidates and never selected**, and nothing is tuned on them.
Traceability item #17 ("F1 vs. majority-class baseline") is answered by the comparison with the majority
baseline. The rule falls back to the training positive rate when an input is missing (0 of 49 and 0 of 64
validation rows).

## Decision threshold
F1 (positive class) and macro-F1 use the **fixed threshold 0.5** on calibrated probabilities (`p ≥ 0.5` predicts
success; `metrics.DECISION_THRESHOLD`). It was set in Step 9 (commit `64739ca`), before any full-data model ran,
and is **never tuned on any season**. Every F1 row of the result files carries `decision_threshold = 0.5`.

**Limitation:** at 0.5 on calibrated probabilities with a base rate of about 0.33, F1 for the positive class
will be conservative. PR-AUC (threshold-free) and Brier are therefore the headline metrics, and F1 is reported
as specified.

## Metrics and uncertainty
- Reported for every model and baseline: F1 (positive class), macro-F1, PR-AUC (step-wise average precision),
  ROC-AUC, Brier, ECE.
- **ECE: 10 equal-width bins is primary.** 5 bins (`ece_5bin`) is supplementary, in both the development and the
  test-season reports. Bootstrap ECE intervals are biased upward (resampling duplicates rows into bins).
- Undefined metrics (one class, no rows) are reported as undefined, never as 0.
- **Bootstrap:** 1000 paired resamples, seed 42, 95% percentile intervals. The **race-cluster** interval (whole
  races resampled) is primary; attempt-level is reported for comparison only.

## Headline test results
- **Discrimination against the majority baseline:** PR-AUC (headline), and F1 for the positive class at 0.5
  (as specified, #17).
- **Calibration:** Brier (headline) and ECE (10 bins).
- **No significance claim is made against the rule baseline.** If the primary model does not beat the rule
  baseline on 2025, that is reported as the result, as it is.

## What the single test run evaluates on 2025
Exactly this:
- candidate models: LR and GBT, each raw and calibrated;
- feature set: `real`;
- settings: G=3.5, k=2 (primary) and G=5.0, k=3 (sensitivity), one test evaluation each;
- fixed references: the majority and rule baselines.

"Nothing else" means no other candidate model and no other feature set, setting, threshold or post-hoc subgroup
is evaluated on 2025 as a result. Any later analysis of 2025 must be labelled exploratory.

**How it is run:** `make model-test-full CONFIRM=yes` runs `experiments/model_eval.py --source full
--allow-test-season` for the primary setting, then with `--g 5.0 --k 3`. The outputs carry the setting suffix
(`results/model_eval_G3.5_k2.csv`, `results/model_eval_G5.0_k3.csv`, and the same for calibration,
feature-importance, reliability and confusion-matrix files), so the two settings never share a file.

**Reruns:** a crashed test run may be rerun only if no test-season number was seen. The first complete run is
the reported one. The pipeline refuses to start a test run for a setting whose result files or saved metadata
already exist, so a complete run is never overwritten.
