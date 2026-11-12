# Model specification v2 (pre-registered test-season ablation)

**Status: FROZEN from the commit that adds this file (git tag `model-spec-v2`).** This version **adds to**
`docs/model_spec.md` (v1, git tag `model-spec-v1`) and replaces nothing in it. Where this file is silent, v1
applies word for word. No item below may change after any test-season (2025) number has been seen. A change
before the test run requires a new, dated version (v3) committed before the run.

Dated 12 Nov 2026. The label definition is unchanged (`docs/label_definition.md`, git tag
`label-definition-v1`). No 2025 data, count or schema was read to write this file, and no model was trained for it.

## 1. What is evaluated on 2025

| Feature set | Role | Assembled columns | Inputs (`model/features.py`) |
|---|---|---|---|
| `real` | **PRIMARY** (v1, unchanged) | 52 | the v1 list (Jolpica and stint-boundary provenance) |
| `real_tyre` | ABLATION (traceability #30) | 70 | `real` + attacker and defender compound and tyre age at L−1 (TracingInsights) + `tyre_age_delta_laps_ti` |
| `real_ti` | ABLATION (traceability #30) | 98 | `real` + the TracingInsights tyre and per-lap telemetry inputs at L−1 + the `telemetry_partial` flags |

For each feature set, LR and GBT are evaluated, each raw and calibrated. They use the v1 grids, seed 42 and the v1
hyperparameter selection by 2024 validation PR-AUC. Both settings are evaluated:

| Role | G (gap threshold, s) | k (response window, laps) |
|---|---|---|
| **Primary** | 3.5 | 2 |
| Sensitivity | 5.0 | 3 |

The sensitivity setting is reported next to the primary and is never used to choose anything.

The majority-class and rule baselines are **fixed reference points**, as in v1. They are scored on 2025 next to
the models, but they are never candidates, never selected and never tuned.

Test-season feature sets: `real`, `real_tyre`, `real_ti`

The code allows `--allow-test-season` only for this list (`model/features.TEST_FEATURE_SETS`), and a unit test
keeps the list in the code equal to the line above. Every other feature set (for example `with_synthetic`) is
still refused.

**Deployment is unchanged.** The model used by streaming and serving stays the `real` calibrated LR from Step 9c
(`models/undercut/deploy_real_G3.5_k2/lr/scorer.json`, sha256 `0a9b7dfb…`). No ablation result can replace it.

## 2. Primary feature set: the selection rule and how it was applied

**Rule.** The primary feature set is the one with the highest mean leave-one-season-out (LOSO) PR-AUC over the
folds 2021-2024 for calibrated LR at G=3.5, k=2. It replaces `real` only if it beats `real` by at least 0.02.

**Timeline.** The two parts of the rule were fixed at different times:
- **The form of the rule** was written in the v2 playbook (`docs/planning/playbook_v2.md`, 12 Nov 2026) before
  any Step 9d development table existed. It named calibrated LR, the mean LOSO PR-AUC over 2021-2024, and a
  minimum margin over `real`, with `real` staying primary otherwise. The playbook gave the margin only as an
  example, with no number.
- **The 0.02 margin** was fixed after the first Step 9d tables had been seen.

**Application.** The rule was applied to the committed development files, using the rows with `model = lr` and
`metric = pr_auc`, averaged over the four folds:

| Feature set | File | Mean LOSO PR-AUC (LR) |
|---|---|---|
| `real` | `results/model_dev_loso.csv` | 0.413 |
| `real_tyre` | `results/model_dev_loso_real_tyre.csv` | 0.402 |
| `real_ti` | `results/model_dev_loso_real_ti.csv` | 0.374 |

Neither alternative scores above `real`, so **`real` stays primary**. Because `real_tyre` (0.402) and `real_ti`
(0.374) are both below `real` (0.413), the outcome is the same for every margin of 0 or more. The late choice
of 0.02 therefore cannot have changed the outcome.

**Note on "calibrated".** The playbook says "calibrated LR". The LOSO `lr` rows are raw LR scores at the first
grid point (`model/loso.py`, which fits no calibrator per fold), and the rule was applied to those rows as they
are. A strictly increasing calibration map would leave PR-AUC unchanged, since PR-AUC depends only on the ranking.

## 3. Unchanged from v1

- **Decision threshold:** 0.5 on calibrated probabilities. It is never tuned on any season.
- **Calibration rule:** with fewer than 300 labelled validation rows, Platt is used without comparing it with
  isotonic. That means Platt for every model, feature set and setting here (49 and 64 validation rows).
- **Bootstrap:** 1000 paired resamples, seed 42, 95% percentile intervals. The race-cluster interval is primary;
  the attempt-level interval is reported for comparison.
- **Headline metrics:** PR-AUC and Brier. F1 for the positive class at 0.5 is reported for #17.
- **ECE:** 10 equal-width bins is primary; 5 bins (`ece_5bin`) is supplementary.
- **Baselines:** no significance claim against the rule baseline.
- **Split:** train 2021-2023, validation 2024 (selection and calibration), test 2025. Class weights come from the
  training rows only.
- **Rerun rule:** a test run may be rerun **only after a crash that produced no test-season number**. The first
  complete run is the reported one. The pipeline refuses to start a test run whose result files or saved
  metadata already exist. This now also covers the `_real_tyre` and `_real_ti` file names.

## 4. Ablation reporting

- `real_tyre` and `real_ti` use the same split, the same labelled attempts and the same settings as `real`. In
  development the labelled counts were identical across the three sets: 108 training attempts (32 positive) and
  49 validation attempts (16 positive) at G=3.5, k=2; 184 (50) and 64 (22) at G=5.0, k=3.
- Each feature-set difference (`real_tyre − real`, `real_ti − real`, `real_ti − real_tyre`) is reported per
  model and per setting, for PR-AUC and Brier as headline metrics. Each comes with a **paired race-cluster
  bootstrap interval**: the same resampled races are used for both sets, with 1000 resamples and seed 42.
- **No significance claim** is made for any difference. No ablation result changes the primary feature set or
  the deployed model.
- The ORACLE variant (`realised_outlap_delta_s`, the realised out-lap delta) is **not usable at decision time**,
  and it is labelled that way wherever it appears. It is not a model input in any of the three sets.

## 5. Determinism

- `real`: its code path is unchanged. Its development files reproduce the committed hashes
  (`data/real_dev_committed.sha`).
- `real_tyre` and `real_ti`: training and validation rows pass through `dataset.canonical_order` inside
  `build_dataset`. That gives one partition, sorted by season, round, attacker_id, defender_id and lap. Two
  consecutive development runs produced byte-identical files.
- **Disclosure:** GBT validation PR-AUC moved by up to **0.043** from row order alone (`real_ti` at G=3.5, k=2:
  0.732 → 0.689). **GBT differences of that size or smaller are not interpretable**, in development or test
  tables. LR did not change with row order.

## 6. Missing data

- Every numeric null becomes 0.0 plus its `_missing` indicator. A categorical null becomes `__unknown`. Nothing is
  imputed from data.
- **No race or attempt is dropped, excluded or down-weighted** for low coverage. Class weights are unchanged.
- The `real_tyre` and `real_ti` test runs write attempt-level missing rates per season and feature, 2025
  included (`results/missing_rates_{set}_G{g}_k{k}.csv`, `model/dataset.missing_rates`). They are reported
  together with the test results. Missing rates for `real` are not reported (its v1 outputs are unchanged).
- Two 2025 races are known to have low lap-level compound coverage (from the Step 4C audit, read before this
  freeze): Miami R6, where 55.6% of laps have a compound, and Belgium R13, at 93.5%.

## 7. Disclosure

- The following development results were seen before this freeze:
  - the Step 9b tables (`real`);
  - the Step 9d tables (`real_tyre`, `real_ti`: `results/model_dev_eval*`, `results/model_dev_loso*`);
  - the permutation importances (`results/model_dev_permutation_real_ti*`).
- In development, **no model beat the rule baseline on LOSO at the primary setting**. The rule scored 0.559 mean
  PR-AUC; the best model, `real_tyre` GBT, scored 0.533. At the sensitivity setting, `real_ti` GBT (0.584) and
  `real` GBT (0.558) are above the rule (0.513).
- LOSO uses raw scores at the first grid point. Its numbers are therefore not comparable with the calibrated
  validation columns.
- Whatever 2025 shows is reported as it is.

## 8. How it is run

`make model-test-full CONFIRM=yes` makes six runs: `real`, then `real_tyre`, then `real_ti`, each at G=3.5, k=2
and then at G=5.0, k=3. The two `real` commands are identical to v1.

`make model-test-full CONFIRM=yes FEATURE_SET=<set>` runs a single set. It is only for recovery after a crash,
under the rerun rule. Any set outside the list in section 1 is refused.

Outputs go to `results/<name>{_set}_G{g}_k{k}.<ext>`, with no set suffix for `real`. Models and metadata go to
`models/undercut/full_{set}_G{g}_k{k}/`.

v1's "nothing else" now means that no feature set, model, setting, threshold or subgroup outside this file is
evaluated on 2025 as a result. Any later analysis of 2025 is labelled exploratory.
