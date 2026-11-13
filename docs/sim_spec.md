# What-if simulator specification (Step 15, pre-registered)

This file was written and committed **before** any backtest run of `experiments/whatif_consistency.py`. It fixes
the following in advance:
- the strategies;
- every constant of the simulator;
- the consistency checks and their pass rules;
- the backtest subsets;
- the stop rule.

None of these are tuned against backtest results. If the backtest is unsatisfactory it is reported as it is, and any
later change gets a new, dated section here.

## Question
At the end of lap C of a race (the last lap the attacker completed), attacker A is directly behind defender D. For
each strategy the simulator gives:
- the probability that A is ahead of D once both have stopped;
- the expected gap;
- the 80% interval of the gap.

The calibrated model probability for the same pair is shown next to it, unchanged. It comes from the stream's
`undercut_prob` and is never rescored.

## Strategies
L = C + 1 is the label's attempt lap (`docs/label_definition.md`): pitting "now" means that lap L is A's in-lap.

| strategy | A in-lap | D in-lap, primary response | D in-lap, sensitivity response |
|---|---|---|---|
| `pit_now` | L | L + 1 | L + k |
| `pit_next_lap` | L + 1 | L + 2 | L + 1 + k |
| `stay_out` (overcut) | D's in-lap + 1 | L + 1 | L + k |

- k = 2, the deployed model's response window.
- The primary response is the undercut definition (D covers on the next lap). The sensitivity response has D
  staying out k − 1 more laps.
- **Evaluation lap** = the later of the two out-laps (out-lap = in-lap + 1).
- **Gap** = cum_D − cum_A at the timing line on the evaluation lap, so a positive gap means A is ahead (the
  label's `outcome_gap_s`).
- P = the share of draws with gap > 0.
- The interval is the 10th to 90th percentile.
- A strategy whose evaluation lap is past the race distance is infeasible (`race_end`): it is returned with null
  numbers.

## Lap-time model
For driver i on lap l with compound c and tyre age a (the age at the end of the lap, as in `df_jobs.tyre_age`):

```
t_il = m_i + f_c(a, l) + u_i + e_il        u_i ~ N(0, s_i^2 / n_i),   e_il ~ N(0, s_i^2)
```

**f_c (`sim/tyre_model.py`)** is fitted on green laps of strictly earlier races (`race_seq < target`), the same
expanding window as the pit-loss priors.
- **Target:** `y = lap_time − the driver's green median lap of that race`.
- **Green laps** use the `driver_green_pace.sql` filter: lap > 1, not a pit in or out lap, not neutralised or red
  flag, lap time known, and at least 5 green laps for the driver.
- **Trim:** laps with `lap_time > 1.07 × median` are dropped (the label's incident ratio).
- **Form:** `f = α + β·a + φ·l`, or `+ γ·a²`.
  - φ is the per-lap fuel trend, one value per target race. It comes from a joint fit over all earlier green laps,
    with an intercept and an age slope per compound and one shared lap term. Pooled over stints that start on
    different laps, age and lap number are not collinear, so φ is identified there.
  - Each level then fits α, β (and γ) on `y − φ·l`.
  - Amendment (2026-11-13, before any backtest run, found by a unit test): a separate φ per compound is not
    identified. Within one stint, age and lap move together, so a compound used mostly in first stints got the
    wrong offset against one used in later stints (−0.29 s fitted against +0.4 s generated). If φ is not
    identified, φ = 0 and `fuel_s_per_lap` is null.
  - The quadratic form is used only if its leave-one-race-out RMSE over the earlier races is strictly lower than
    the linear one. That needs at least 2 races at that level; otherwise the form is linear.
- **Levels, in fallback order:** circuit × compound, then compound (all circuits), then global (all compounds),
  then `none` (f = 0).
  - A level needs at least 30 laps from at least 2 stints.
  - The level used is reported as the source flag.
  - An `UNKNOWN` compound uses the global level directly.
- **Compounds:**
  - Full mode uses the real compounds from `clean/stints_tracinginsights`.
  - Sample mode uses the clean `stints`, which are synthetic, and every output is flagged `synthetic`.
- **Noise prior** `s0` is the residual SD of the chosen fit.

**Pace level m_i and noise s_i** come from the driver's recent laps (at most the last 10 non-pit laps with lap ≥ 2,
up to lap C).
- A causal filter replaces the batch track flags, which are not on the Redis channel. It keeps laps with
  `lap_time ≤ 1.07 × the minimum of those laps`.
- The last at most 5 kept laps give residuals `r = lap_time − f_c(a, l)`.
  - `m_i` = the mean of r.
  - `n_i` = the number of residuals.
  - `s_i² = (n_i·var(r) + 3·s0²) / (n_i + 3)`: shrinkage to the prior with n0 = 3, and `var` taken as 0 when
    n_i < 2.
- With no kept lap, the pace comes from Redis `last_lap_time_s`, with n_i = 1 and `noise_source = prior`.

**Tyre age and compound after a stop.**
- The new tyre is age 1 on the out-lap.
- The new compound follows a fixed rule and is never read from the future:

  | old | new |
  |---|---|
  | SOFT | MEDIUM |
  | MEDIUM | HARD |
  | HARD | MEDIUM |
  | INTERMEDIATE, WET | unchanged |
  | UNKNOWN | UNKNOWN |

## Stop loss
For driver i, the loss added to the out-lap is:

```
loss_i = pit_loss_prior_s + expected_stop_delta_s(i) + expected_outlap_pace_residual_s(i) + η,
η ~ N(0, sd(stop_delta_s)^2 + sd(outlap_pace_residual_s)^2)
```

- These are the additive Step 8 decomposition: the circuit's stop cost, the team's execution and the warm-up
  effect.
- The first three come from the same `features/pit_loss` and `features/outlap_features` rows the stream uses,
  reduced to one value per driver with max, as in `streaming/priors.py`. A null term is 0 and is flagged.
- The two SDs are taken over `outlap_features` rows of strictly earlier races: non-long stops whose out-lap is not
  neutralised.
  - Each SD is a **robust SD**, `1.4826 × MAD`, not the sample SD.
  - Amendment (2026-11-13, before any backtest run, decided from the stop history alone): on 2021–2024 the sample
    SDs are 3.4 s (`stop_delta_s`) and 8.9 s (`outlap_pace_residual_s`). Unsafe releases, damage and penalties
    make the tails heavy (p1/p99 of −5/+17 s and −29/+36 s), while the MAD-based SDs are 0.72 s and 1.09 s. With
    the sample SD, the noise of a single stop would swamp every strategy difference.
- A null `pit_loss_prior_s` makes every strategy unavailable (`no_pit_loss_prior`).

## Traffic
When a pitting car's out-lap crossing comes 0 to 1.0 s after a projected crossing of a **third** car, the pitting
car is held to exactly 1.0 s behind it. This repeats until no third car is within 1.0 s ahead.
- Third cars are projected from their latest crossing (`race_time_s`) at their recent pace. They do not stop
  during the horizon.
- A car whose latest crossing is more than 2 leader lap times old counts as retired.
- A and D are not traffic for each other; their relative order is the outcome.
- The hold is non-decreasing in the arrival time, so a larger loss never gives an earlier crossing.

## Randomness
- n = 500 draws by default; 100 to 5000 are allowed.
- seed = 42 by default.
- One `numpy.random.Generator(PCG64(SeedSequence(seed)))` draws the same standard normals for every strategy and
  response (common random numbers). So the same seed gives byte-identical results, and the strategies are
  compared on the same draws.

## Consistency checks (`results/whatif_consistency.*`, `experiments/whatif_consistency.py`)
1. **Monotonicity (pass/fail).** For every backtest state, A's loss is shifted by δ ∈ {−2, −1, 0, +1, +2, +4} s.
   P(`pit_now`) must be non-increasing in δ at every step.
   - Shifting the race's `pit_loss_prior_s` for **both** cars is reported for information only: both cars pay
     it, so it cancels apart from traffic.
2. **Determinism (pass/fail).** For every backtest state, the same seed must give byte-identical result JSON.
   With seed + 1, the largest change in P is reported as the Monte Carlo spread.
3. **Ordering (pass/fail on a synthetic grid).** The grid is gap ∈ {0.5, 1, 2, 3.5, 5} s × closing rate
   ∈ {−0.5, 0, +0.5} s/lap, where closing rate = D's pace − A's pace and positive means A is faster. Each cell has
   equal tyres and the global degradation.
   - P(`pit_now`) is non-increasing in the gap for every closing rate.
   - When closing at +0.5, P(`pit_next_lap`) ≥ P(`pit_now`); when opening at −0.5, P(`pit_now`) ≥
     P(`pit_next_lap`). Each is allowed a tolerance of one Monte Carlo SE, `sqrt(p(1−p)/n)`.
   - Information only: on the backtest states, the share where sign(P_next − P_now) equals the sign of the
     attacker's last-3-lap closing rate.
4. **Backtest.** Labelled attempts (`success` / `fail`) at G = 3.5, k = 2, with the decision state at the end of
   lap L − 1 built by the same code as the live state. For P(`pit_now`) of the simulator (primary and sensitivity
   response) and for the model's P against the label:
   - Brier score;
   - calibration in 10 bins and in 5 bins, and ECE;
   - agreement: mean signed difference, mean absolute difference and Spearman correlation.

   Subsets:
   - 2021–2023: the model's training rows, so the model's numbers are optimistic there;
   - 2024: calibration;
   - 2021–2024;
   - **2025 as a separate held-out row**, declared here before any run. It is read only by a `RECORD=1` run with
     `--allow-test-season`.

**Stop rule.** If |mean(P_sim(`pit_now`, primary) − P_model)| > 0.20 on the 2021–2024 labelled attempts, the run
stops with exit code 3.
- It does so before 2025 is read, and it writes nothing to `results/`.
- The result is reported, and nothing is tuned.

## Post-hoc supplement (2026-11-13, added after the 2021–2024 development run)
**Labelled post hoc.** Everything in this section was added after the development run had been seen. It is
not a pass/fail criterion, and it does not change any pre-registered rule above.

**The pre-registered ordering rule is kept unchanged.** Its one failing cell is recorded as part of the result:
- closing rate −0.5 s/lap (the attacker is losing time), gap 5.0 s;
- P(`pit_next_lap`) − P(`pit_now`) = +0.022 (0.062 → 0.084), above the one-SE tolerance.

**Why the cell fails.**
- The mean gap does move against the attacker: E[gap] goes from −6.10 s to −6.58 s.
- But `pit_next_lap` simulates one lap more, and that widens the 80% interval from 10.4 s to 12.1 s.
- At a 5 s gap the attacker is ahead only in the far tail, so the wider spread lifts that small probability even
  though the mean worsens.
- The rule compares probabilities, so in the tail it also measures spread, not only direction.

**Supplementary column.** Every ordering cell also gets rows with `check = ordering_posthoc` and
`post_hoc = true` in `results/whatif_consistency.csv`, always with an empty `pass`:
- `egap_next_minus_now_s`: E[gap | `pit_next_lap`] − E[gap | `pit_now`];
- its sign;
- `egap_sign_follows_rate`, for the two non-zero rates.

On the development run the sign follows the closing rate in all 10 such cells:

| closing rate | E[gap] difference |
|---|---|
| −0.5 s/lap | −0.479 s |
| 0 | +0.021 s |
| +0.5 s/lap | +0.521 s |

The difference is the same at every gap, as expected with equal cars. A `post_hoc` column was added to the CSV
(`false` on every pre-registered row).
