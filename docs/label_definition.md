# Label and decision-time feature definitions

This file is the contract for which information a model may use, and when.

**Core rule:** a feature for a decision at lap *L* may only use information available at the **end of lap L − 1**.

- **Information set at the end of lap L − 1:** every lap row with lap number ≤ L − 1 in the race (for every car,
  so a same-lap car behind the attacker that crosses the line a few seconds later counts), plus every strictly
  earlier race (`race_seq = season*100 + round`).
- Step 8 defines the undercut attempt, its label and its features ("Undercut attempts", below).
- Step 7 defines the out-lap pace features. Step 8 adds the stop-time decomposition and the shrinkage
  ("Out-lap pace features").

## Undercut attempts (`features/undercut.py`, table `undercut_attempts`)
Approved in Step 8. The candidate rule is in `features/undercut_rule.py`, which the batch label and the real-time
detector (`streaming/detector.py`) share, so the two cannot drift apart.

### Timing-line facts the definition relies on
- **The Jolpica pit-stop `lap` is the in-lap.** The car crosses the timing line inside the pit lane, before the
  boxes. Lap *p* gets the pit entry, and lap *p + 1* (the out-lap) gets the stationary time, the pit exit and the
  tyre warm-up. On the sample's green stops, about 26% of the in+out loss is on the in-lap and 74% is on the out-lap
  (sd 0.04 in both races; see "Stop-time attribution" below).
- **"Directly behind"**: at the end of lap L − 1, A's `position` = D's `position` + 1, and D is A's
  `ahead_driver_id`. The gap is `gap_ahead_s` = cum_A − cum_D, the difference of the cumulative times at the same lap
  number (the Step 6 timing-line gaps).

### Definition
For each gap threshold G and response window k, an **undercut attempt** is a tuple
(season, round, attacker A, defender D, lap L) such that:

1. **Candidate rule** (end of lap L − 1; shared with the detector). All of the following hold:
   - A is directly behind D and 0 < gap ≤ G;
   - L ≥ `min_attempt_lap` (4);
   - A and D are not teammates (same team history key, via `features/lineage.py`);
   - no other car crossed the line strictly between D and A (its crossing of lap L − 1 or L − 2), which catches a
     lapped car between them;
   - neither car is lapped (`gap_leader_s` ≥ the leader's lap time at L − 1);
   - neither car is on an in-lap or out-lap at L − 1;
   - lap L − 1 is green and is not a restart lap.
2. **A pits on lap L** (a Jolpica pit row with `lap = L`). A lap-1 stop never qualifies, because there is no lap 0.
3. **D's response**: D's first stop in [L + 1, L + k]. If there is none, the outcome is `defender_stayed_out`, which
   is not part of the binary label. If D also stops on lap L, the outcome is `excluded_same_lap_stop`.
4. **The window** (laps L − 1 … label lap, or L − 1 … L + k when D stays out) passes every exclusion below.

**Label lap** = D's out-lap = D's pit lap + 1. A's out-lap (L + 1) is at or before it, so both stops are complete.

**Label** = 1 if A's `position` < D's `position` on the label lap, otherwise 0. `outcome_gap_s` = cum_D − cum_A on
the label lap (positive means A is ahead) is kept as a continuous outcome.

`detector_eligible` is true when the pair passed the candidate rule at L − 1, that is, when the real-time detector
would have flagged it before the stop.

### Parameter grid
| Parameter | Values | Default | Where |
|---|---|---|---|
| G, gap threshold (s) | 2.5, 3.5, 5.0 | **3.5** | `undercut_rule.GAP_GRID`, `DEFAULT_GAP_S` |
| k, defender response window (laps) | 1, 2, 3 | **2** | `undercut_rule.WINDOW_GRID`, `DEFAULT_WINDOW_LAPS` |
| min_attempt_lap | 4 | 4 | `undercut_rule.MIN_ATTEMPT_LAP` |
| restart buffer (laps) | 1 | 1 | `undercut.RESTART_BUFFER_LAPS` |
| slow-stop margin (s) | 3.0 | 3.0 | `undercut.SLOW_STOP_MARGIN_S` |
| drive-through margin (s) | 2.0 | 2.0 | `undercut.DRIVE_THROUGH_MARGIN_S` |
| incident lap ratio | 1.07 | 1.07 | `undercut.INCIDENT_LAP_RATIO` |
| rejoin traffic window (s) | 3.0 | 3.0 | `undercut.TRAFFIC_WINDOW_S` |

One build emits all 9 (G, k) combinations; `is_default_params` marks (3.5, 2). The attempt set grows with G
(2.5 ⊆ 3.5 ⊆ 5.0). A larger k can turn a `defender_stayed_out` into success or fail. With k = 2, both an immediate
cover and a one-lap-late cover count; k = 3 starts to mix in unrelated strategy offsets. Sensitivity is reported
for the whole grid.

### Edge cases and exclusions
The first reason in this priority order sets `outcome_category = excluded_<reason>`. All the reasons that apply are
kept in `exclusion_reasons`. Reasons marked **rule** come from the shared candidate rule at L − 1.

| # | Reason | When | Why / data caveat |
|---|---|---|---|
| 1 | `missing_data` (rule) | the gap or a position is unknown at L − 1, or a position is unknown on the label lap | Jolpica gaps are cumulative times, and one missing lap time makes every later gap null |
| 2 | `inconsistent_timing` (rule) | gap ≤ 0 (position and time disagree, or a tie) | |
| 3 | `early_lap` (rule) | L < 4 | Stops on laps 2–3 are lap-1 damage, red-flag entries recorded as stops, or early wet-to-dry changes, not strategy (sample: Sargeant L2, 48.9 s). A stop on lap 1 is never a candidate, because there is no decision lap 0 |
| 4 | `teammates` (rule) | same team history key | team orders, not a competitive call |
| 5 | `traffic_between` (rule) | another car's crossing of lap L − 1 or L − 2 lies strictly between D's and A's crossing of L − 1 | a lapped car between them on track |
| 6 | `lapped` (rule + window) | at L − 1: `gap_leader_s` ≥ the leader's lap time at L − 1; in the window: the car's crossing of lap j is after the leader's crossing of lap j + 1 | blue flags make the fight meaningless |
| 7 | `in_pit` (rule) | A or D is on an in-lap or out-lap at L − 1 | D has just stopped (fresh tyres), or A is already mid-cycle |
| 8 | `neutralised` (rule + window) | any lap in the window is SC/VSC/red-flag, or is in a restart period: the first green lap after a neutralised run, or the first **3** green laps after a neutralised run that contains a red-flag lap | a stop under SC/VSC is cheap, and restart gaps are compressed. After a red flag every car has free new tyres and the gap and pace lookbacks span the stoppage (see "Track status") |
| 9 | `same_lap_stop` | D stops on lap L too | no lap offset, so there is no undercut. A double stack behind a teammate is flagged, not excluded |
| 10 | `retired` | A or D has no lap row for some lap in L … label lap (or L … L + k) | Jolpica lap rows stop at retirement |
| 11 | `long_stop` | A's or D's stop is > 60 s (`is_long_stop`) | repairs, or a red-flag stoppage |
| 12 | `penalty_suspected` | a stop is more than 2.0 s below the race's median green stop | a drive-through or stop-go recorded as a stop (lane transit with no stationary time) |
| 13 | `attacker_restop` | A stops again in L + 1 … the window end | puncture or damage |
| 14 | `incident` | a green racing lap of A or D in the window (not an in- or out-lap, lap ≥ 2) is > 107% of the race's green median | a spin, damage or an unrecorded drive-through |
| 15 | `wet` | A's or D's compound is INTERMEDIATE or WET in the window | crossover strategy, not an undercut. Synthetic compounds are dry only |
| 16 | `race_end` | race_laps − L < k + 2 | the window does not fit before the flag. This also removes late stops for the fastest-lap point |

**Flagged, not excluded.**
- `outcome_attacker_slow_stop` / `outcome_defender_slow_stop`: the stop is more than 3.0 s over the race's median
  green stop. A served 5 s or 10 s time penalty and a slow wheel change look the same in Jolpica, and a slow stop is a
  real undercut cost. The counts CSV reports `success_rate_excl_slow_stop` as a sensitivity check.
- `outcome_attacker_double_stack` / `outcome_defender_double_stack`: a teammate stopped on the same lap earlier
  (by Jolpica `time_of_day`, or the order is unknown).

**Other decisions.**
- **Penalties.** Post-race time penalties do not change the timing-line order, so they do not affect the label.
  Jolpica has no penalty data. FastF1 race-control messages, once ingested, will replace the heuristics in
  reasons 12 and 14.
- **Red-flag restarts.** Jolpica records the red-flag entry to the pit lane as a pit stop for the whole field
  (checked on the full 2021-2025 data in Step 8b), so inferred stints start again after the stoppage. The free tyre
  change makes tyre age and pace comparisons across the restart meaningless, so the red-flag restart period (below)
  is excluded as `neutralised`. FastF1 stints fix the tyre data.
- **Sprint races** are out of scope. Jolpica laps and pit stops cover the Grand Prix only, and a sprint has no
  mandatory stop.

### Track status (SC / VSC / red flag)
- **Jolpica-only heuristic** (`etl/clean.heuristic_track_flags`, tightened in Step 8). Lap j ≥ 2 is neutralised when:
  - the field median lap time is > **1.10** × the race's green median, **or**
  - at least **50%** of the cars running that lap (pit laps excluded) are > 1.07 × the green median.

  Red flag (Step 8b): **either** the median of the racing cars (pit laps excluded) is > **3.0** × the green median,
  **or** the median of **every** car on the lap (pit laps and lap 1 included) is > 3.0 × the green median. A red-flag
  lap is also a neutralised lap. Timing only.
- **Why the field-wide red-flag test.** Jolpica marks every car as pitting on a red-flag lap, so the racing-car
  median had no cars and no red flag was ever detected: on the full data, 17 laps in 14 races had a field median of
  5×–34× the green median and none was flagged. An ordinary stop costs about 20–30 s (1.2–1.4×), so a lap where
  every car pits under green stays far below 3×, and a lap with a normal pit stop or a slow standing start is never
  a red flag. The 3.0 threshold is the Step 5 value and was not tuned on the full data.
- **Restart period after a red flag: 3 green laps** (`features/undercut.RED_FLAG_RESTART_LAPS`), counted from the
  last lap of the neutralised run that contains the red-flag lap (the run usually includes SC laps before the
  stoppage and the rolling restart). After an SC or VSC without a red flag it stays 1 lap
  (`RESTART_BUFFER_LAPS`). Why 3: the field restarts bunched on free new tyres, and the longest decision-feature
  lookback is 3 laps (`gap_ahead_change_3l` compares L − 1 with L − 4). With 3 restart laps, an eligible decision
  lap L − 1 is at least 4 laps after the run, so no decision feature reaches back into the stoppage or the restart.
  The real-time detector receives the same flag through `LapSnapshot.restart_lap`, so batch and stream agree.
- **Why it was tightened.** The old rule (ratio > 1.25) missed the deployment and restart laps of the 2024 Shanghai SC
  run: lap 21 (ratio 1.155, 81% slow), lap 25 (1.248, the 145 s lap maximum) and lap 27 (1.121). Green laps in the
  sample never exceed ratio 1.026 or share 0.056. Shanghai 2024 is now laps 21–31, with lap 32 as the restart lap.
- **FastF1 override.** When FastF1 has the lap, its TrackStatus codes replace the heuristic for that lap (per lap, in
  `etl/clean.build_laps`): 4 = SC, 6/7 = VSC, 5 = red flag. A lap with any of these codes at any point is flagged.
  Both restart periods apply on top of either source.
- **Leakage.** The heuristic uses the whole race's median, but track flags are used only for eligibility and
  exclusion, never as features. In a live race the SC status is known the moment it is called, and the heuristic
  only stands in for that signal.

### Decision-time features (`undercut.DECISION_FEATURES`)
Only these columns may feed the S9 model or the detector. All of them use the end-of-L − 1 information set.

| Feature | Definition |
|---|---|
| `gap_s`, `gap_ahead_change_1l`, `gap_ahead_change_3l` | A's gap to D and its 1- and 3-lap change (null if the car ahead changed) |
| `attacker_position` | A's position at L − 1 |
| `attacker_/defender_tyre_age_laps`, `tyre_age_delta_laps` | tyre ages, and D − A |
| `attacker_/defender_compound`, `compound_is_synthetic` | compounds (synthetic in sample mode) |
| `attacker_/defender_pace_last3_s`, `pace_delta_last3_s` | mean of the last (up to) 3 green laps of the current stint; A − D |
| `attacker_/defender_deg_slope_s_per_lap`, `deg_slope_delta_s_per_lap` | least-squares slope of lap time on tyre age over the stint's green laps (≥ 3 laps, else null) |
| `laps_remaining` | race laps − (L − 1). `race_laps` is the winner's lap count, a stand-in for the scheduled distance; it is wrong only in shortened races, which are mostly excluded as neutralised anyway |
| `pit_loss_prior_s`, `pit_loss_source` | Step 6 prior pit loss (circuit → global, strictly earlier races) |
| `rejoin_traffic_cars`, `rejoin_nearest_ahead_s` | cars whose on-track gap behind A at L − 1 is in (pit_loss − 3.0 s, pit_loss]: A would rejoin just behind them. The nearest margin is also given |
| `attacker_/defender_expected_outlap_pace_residual_s` (+ `_source`), `attacker_/defender_expected_stop_delta_s` | Step 8 decomposed out-lap priors (below) |

- **Legacy:** `attacker_expected_outlap_delta_s_legacy` (Step 7 `expected_outlap_delta_s`) is decision-time safe but
  collinear with `pit_loss` (see below). It is kept for the S16 comparison only.
- **ORACLE only** (`oracle_*`): A's realised out-lap delta, its pace residual and its stop delta. They use the stop
  and later laps, and may only appear in the labelled ORACLE ablation (S16).
- **Outcome** (`outcome_*`): what happened after the decision. These are never features.

### Leakage rules
1. Lap-level features are joined at the decision lap L − 1 (`decision_lap`), never at L.
2. Priors (pit loss, out-lap pace residual, stop delta) use expanding windows over strictly earlier races
   (`RANGE … 1 PRECEDING` over `race_seq`). They never use the current race or a later one.
3. `realised_outlap_delta_s`, `outlap_pace_residual_s` and `stop_delta_s` of the current race are ORACLE-only.
4. **Leakage test** (`tests/unit/test_undercut.py`): with four races, the target race from lap L on and every later
   race are scaled ×100 (lap times and stop durations). Every `DECISION_FEATURES` column of the target's attempts
   must stay unchanged, and every attempt must still be generated.

### Smoke-test counts
`python -m f1sa.features.build --source sample` writes `results/undercut_counts_sample.csv`. It has one row per
(season, G, k): candidates, labelled, success, fail, success_rate, success_rate_excl_slow_stop, and a count per
outcome category. **These are smoke-test numbers, not findings.** The file covers 2 races, its tyre compounds are
synthetic, and its first line says so. The real counts come from the full 2021–2025 ingest.

## Model inputs (Step 9, `model/features.py`, `model/dataset.py`)
- **One setting per run.** The default is G = 3.5 s, k = 2. Only labelled rows are used (`success` / `fail`).
  `defender_stayed_out` and every `excluded_*` row are dropped. A duplicated attempt key (season, round, attacker_id,
  lap) after the filter is an error.
- **Feature registry.** Every `DECISION_FEATURES` column is either a model input with a provenance, or in `EXCLUDED`
  with a reason:
  - the absolute `*_pace_last3_s` columns are circuit proxies; `pace_delta_last3_s` is used instead;
  - `compound_is_synthetic` is a provenance flag, not a feature.

  `DERIVED` documents the extra inputs, all taken at `decision_lap` (L − 1): the stint length of A and D, and the
  per-lap telemetry of A and D. ORACLE, outcome and legacy columns can never be registered (`check_registry`).
- **Feature sets** are sets of provenances:

  | Set | Provenances |
  |---|---|
  | `real` | `jolpica`, `stint_boundary` |
  | `with_synthetic` | `real` + `tyre_state`, `telemetry` |

  Step 16 adds `with_fastf1` as one new entry. `real` uses the stint length (real stint boundaries) instead of tyre
  age, because tyre age includes the synthetic starting age in sample mode.
- **Full mode refuses synthetic values.** `train.check_feature_policy` fails if any selected row has
  `compound_is_synthetic` or `*_telemetry_is_synthetic` that is true or null.
- **Missing values** are never imputed from data:
  - a numeric value becomes the constant 0.0, plus a `_missing` indicator;
  - the Step 7/8 `*_source` columns are one-hot encoded, with an `unknown` level.
- **Split** by season: train 2021–2023, validation and calibration 2024, test 2025.
  - The test split is sealed. Only `evaluate.evaluate_test` can open it, and only once.
  - Class weights come from the training rows only.
  - Tuning uses validation PR-AUC. Calibration uses validation scores. Neither ever reads 2025, and a spy test
    enforces this.

## Out-lap pace features (`features/outlap.py`, table `outlap_features`)

### Clean lap
A lap is **clean** when all of the following hold:

| # | Rule | Parameter (in `features/outlap.py`) | Value |
|---|---|---|---|
| 1 | Not the first lap. The standing start is slow and not comparable. | `FIRST_RACING_LAP` | 2: lap must be ≥ 2 |
| 2 | Not an in-lap (`is_pit_in_lap`). | none | |
| 3 | Not an out-lap (`is_pit_out_lap`). | none | |
| 4 | Not under SC, VSC or red flag. | none | all flags false |
| 5 | The lap time is known. | none | |
| 6 | Not slower than 107% of the race's median **candidate** lap. A candidate lap is one that passes rules 1–5. | `CLEAN_LAP_MAX_RATIO` | 1.07 |

- **Flags used by rule 4:** `is_neutralised_lap`, `is_sc_lap`, `is_vsc_lap` and `is_red_flag_lap` from the clean zone.
  - Without FastF1, these come from the lap-time heuristic.
  - That heuristic misses some slow laps; the 2024 Shanghai maximum is 145 s. Rule 6 catches them.
- **The rule-6 median** is taken over the candidate laps of the whole field in that race. A lap exactly at 107% is
  still clean.

### realised_outlap_delta_s (ORACLE only)
For stint *k* > 1, the out-lap is the stint's first lap:

```
realised_outlap_delta_s = out-lap time − median(clean laps of the driver in stint k)
```

- If stint *k* has no clean lap, the median of stint *k − 1* is used.
- `reference_source` records which reference was used: `same_stint`, `previous_stint` or `none`.
- When there is no reference, the value is null.

> **realised_outlap_delta_s is NOT known at decision time.** It uses the out-lap itself and the laps that follow in
> the new stint, all of which happen after the pit decision. It may only be used in the labelled **ORACLE** ablation
> (S16, "with vs without out-lap features, oracle"). It must never be an input to the deployed model or to the
> streaming detector.

### expected_outlap_delta_s (decision-time safe)
`expected_outlap_delta_s` is the mean `realised_outlap_delta_s` from **strictly earlier races**:
`race_seq < current`, with `race_seq = season*100 + round`. The value is fixed before the race starts, so it is safe
at any lap. The first level that has history is used:

| Level | `expected_outlap_source` | History used |
|---|---|---|
| 1 | `driver_team` | the same driver in the same team history key |
| 2 | `team` | the same team history key, any driver |
| 3 | `circuit` | the same circuit, any team |
| 4 | `global` | all earlier races |
| 5 | `none` | none: the value is null and `expected_outlap_missing = true` |

- **History rows** are realised deltas that are not null, whose out-lap is not neutralised (`outlap_neutralised`),
  and whose stop is not long (`is_long_stop`, more than 60 s).
- **Team history key:** teams are matched through `features/lineage.py`. Old ids continue as new ones
  (alphatauri → rb, alfa → sauber), so a rename does not reset the history.
- **Support:** each level's support is in `*_prior_n`.
- **Leakage test:** the data of a race and all later races is scaled ×100, and the race's
  `expected_outlap_delta_s` is asserted unchanged (`tests/unit/test_outlap.py`).

> **Legacy (Step 8 review).** `expected_outlap_delta_s` mostly measures stop time, not warm-up pace (see below).
> It is about 0.74 × `pit_loss_prior_s` plus a small term, so it is collinear with `pit_loss`. It is kept for the S16
> comparison. The model uses the decomposed priors instead.

### Stop-time attribution (Step 8)
Jolpica `duration_s` is the pit-lane time from entry to exit, stationary time included. The timing line is inside
the pit lane, before the boxes. Green, non-long stops in the sample:

| Race | Lane time (median) | In-lap Δ | Out-lap Δ | In+out (= Step 6 `pit_loss_s`) | Out-lap share |
|---|---|---|---|---|---|
| 2023 R5 (Miami) | 22.55 s | +5.5 s | +14.8 s | 20.7 s | 0.74 (sd 0.04) |
| 2024 R5 (Shanghai) | 23.03 s | +6.5 s | +18.2 s | 24.6 s | 0.74 (sd 0.04) |

- The in-lap gets the pit entry (about 26% of the loss). The out-lap gets the stationary time, the rest of the lane
  and the tyre warm-up (about 74%).
- On the 42 history-eligible stops in `outlap_features`, `realised_outlap_delta_s` correlates with `stop_duration_s`
  at r = 0.965. The Phase 1 check, against each driver's race-median lap, gave r = 0.986 with the in+out loss.
  The realised delta has a median of 15.5 s (2023) and 18.4 s (2024).
- Once the race median and the stop-duration deviation are removed, `outlap_pace_residual_s` has sd 0.64 s (2023)
  and 0.79 s (2024). That remainder is the pace signal.
- The share is circuit-specific (it depends on where the line sits in the lane). It is re-checked per circuit on the
  full ingest.

### Decomposition (Step 8; ORACLE values)
For a **history-eligible** stop (realised delta known, out-lap not neutralised, stop ≤ 60 s, duration known):

| Column | Definition | Meaning |
|---|---|---|
| `stop_duration_s` | the in-lap's total Jolpica pit-lane time | |
| `race_median_stop_s` | median `stop_duration_s` over the race's eligible stops | the circuit's typical lane time |
| `race_median_outlap_delta_s` | median realised delta over the race's eligible stops | the circuit's stop share on the out-lap |
| `stop_delta_s` | `stop_duration_s − race_median_stop_s` (any non-long stop) | pit-crew execution |
| `outlap_pace_residual_s` | realised − `race_median_outlap_delta_s` − `stop_delta_s` | warm-up and tyre effect |

The residual assumes that stationary-time deviations land 1:1 on the out-lap, which follows from the attribution
above. It is relative to each race, so it carries no circuit-level stop cost and is uncorrelated with the per-race
`pit_loss_prior_s` by construction. So `pit_loss` carries the circuit's stop cost, `expected_stop_delta_s` the team's
stop execution, and `expected_outlap_pace_residual_s` the warm-up effect.

### expected_outlap_pace_residual_s and expected_stop_delta_s (decision-time safe, shrinkage)
The same strictly-earlier windows as above, with shrinkage toward the parent level:

```
global       = mean over all earlier races
team         = (n_t  · mean_t  + κ · global) / (n_t  + κ)
driver_team  = (n_dt · mean_dt + κ · team)   / (n_dt + κ)          κ = SHRINK_KAPPA = 5
```

| `expected_outlap_pace_source` | Condition |
|---|---|
| `driver_team` | n_dt ≥ `DRIVER_TEAM_MIN_N` = 5 prior stops |
| `driver_team_shrunk` | 1 ≤ n_dt < 5: the estimate leans toward the team |
| `team` | no driver_team history: the shrunk team value |
| `global` | no team history (a new team) |
| `none` | no history; the value is null and `expected_outlap_pace_missing` = true |

- There is no jump at the threshold. At n_dt = 5 the driver's own history gets 50% weight.
- A driver makes about 30 stops a season, so the threshold is reached after 3–4 races. In the sample, every 2024
  driver has n_dt ≤ 2, so the value is mostly the team's.
- `expected_stop_delta_s` is a team property. It uses the team → global levels (`expected_stop_source`).
- Support is in `pace_driver_team_n`, `pace_team_n`, `pace_global_n`, `stop_team_n` and `stop_global_n`.
- κ is a documented constant. If it is re-estimated in S9, that may use 2021–2023 only.
- **Leakage test:** `tests/unit/test_outlap.py` scales a race and all later races ×100 and asserts the race's
  decomposed priors are unchanged.
