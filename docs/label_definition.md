# Label and decision-time feature definitions

This file is the contract for which information a model may use, and when.

**Core rule:** a feature for a decision at lap *L* may only use information available at the **end of lap L − 1**.

- Step 7 defines the out-lap pace features below.
- Step 8 will extend this file with the undercut label.

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
