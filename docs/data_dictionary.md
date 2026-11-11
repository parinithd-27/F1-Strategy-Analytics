# Data dictionary

This file grows step by step: the **clean zone** (Step 5, entities #7), the **sample bundle** and its
**synthetic tables** (Step 4B). S6 adds feature tables.

## Clean zone (`{DATA_LAKE_URI}/clean/{table}/season=S/round=R/*.parquet`, Step 5)
Built by `python -m f1sa.etl.clean`. Schemas are in `f1sa.lake.schemas` (explicit Spark StructTypes). Every table is
partitioned by `season` (int) and `round` (int), which are also its first key columns. Times are **seconds (double)**,
and ids are **Jolpica ids** (`driver_id` = Jolpica `driverId`, canonical).

| Table | Entity | Grain (primary key) | Rows in the sample |
|---|---|---|---|
| races | Race | season, round | 2 |
| drivers | Driver (race entry) | season, round, driver_id | 40 |
| constructors | Race entry | season, round, constructor_id | 20 |
| results | Position (classification) | season, round, driver_id | 40 |
| qualifying | Position (grid) | season, round, driver_id | 40 |
| laps | Lap, Position, Gap | season, round, driver_id, lap | 2168 |
| pitstops | Pit Stop | season, round, driver_id, stop | 60 |
| stints | Tyre | season, round, driver_id, stint | 100 (synthetic) |
| telemetry_lap | Telemetry (per lap) | season, round, driver_id, lap | 2168 (synthetic) |
| telemetry_raw | Telemetry (samples) | season, round, driver_id, session_time_s | 0 (FastF1 only) |

### races
Only races that have results in the lake. Source: Jolpica schedule.

| Column | Type | Meaning |
|---|---|---|
| race_name | string | e.g. "Miami Grand Prix" |
| circuit_id, circuit_name | string | Jolpica circuit id and name |
| locality, country | string | location |
| lat, long | double | circuit coordinates |
| race_date | date | race day |
| race_start_utc | timestamp | race start (UTC; 00:00 if Jolpica has no time) |
| has_sprint | boolean | weekend had a sprint |
| url | string | Wikipedia URL |

### drivers
One row per driver **entered in the race**, taken from the results and completed from the season driver table.

| Column | Type | Meaning |
|---|---|---|
| driver_id | string | Jolpica driverId (canonical) |
| code | string | 3-letter code (matches FastF1 `Driver`) |
| permanent_number | int | career number |
| car_number | int | number raced in this race |
| given_name, family_name | string | |
| date_of_birth | date | |
| nationality | string | |
| constructor_id | string | team in this race |

### constructors
One row per team in the race: `constructor_id`, `name`, `nationality`.

### results

| Column | Type | Meaning |
|---|---|---|
| driver_id, constructor_id | string | |
| car_number | int | |
| grid | int | starting position (0 = pit lane) |
| position | int | finishing order (always set by Jolpica, also for retirements) |
| position_text | string | "1".."20", or "R" (retired), "D" (disqualified), "W" (withdrawn), … |
| points | double | |
| laps | int | laps completed |
| status | string | "Finished", "Lapped", "+1 Lap", "Retired", … |
| finished | boolean | status is Finished, Lapped or "+N Lap(s)" |
| classified | boolean | position_text is numeric |
| race_time_s | double | race time from Jolpica `millis` (null if not classified on time) |
| fastest_lap, fastest_lap_rank | int | lap number and rank of the driver's fastest lap |
| fastest_lap_time_s | double | |

### qualifying
`position` (int), `q1_s`/`q2_s`/`q3_s` (double; null if the driver did not run that session), and `best_s` (their
minimum), plus `driver_id`, `constructor_id` and `car_number`.

### laps (Lap, Position, Gap)
One row per driver per completed lap, from Jolpica lap timings. Laps split across API pages are merged.

| Column | Type | Meaning |
|---|---|---|
| lap | int | lap number (1-based) |
| position | int | running position at the end of the lap |
| lap_time_s | double | lap time |
| cumulative_time_s | double | sum of the driver's lap times up to this lap (null after a missing lap time) |
| gap_to_leader_s | double | cumulative − the leader's (position 1) cumulative **at the same lap number**; 0 for the leader. Measured at the timing line, so a lapped car's gap is the true time behind and can exceed one lap time |
| gap_ahead_s | double | cumulative − the cumulative of the car one position ahead at the same lap; null for the leader |
| is_pit_in_lap | boolean | the driver pitted at the end of this lap (pit stop `lap`) |
| is_pit_out_lap | boolean | the lap after a pit stop |
| is_sc_lap | boolean | safety car on this lap (FastF1 TrackStatus contains 4); **null** if only the heuristic is available |
| is_vsc_lap | boolean | virtual safety car (TrackStatus 6 or 7); **null** with the heuristic |
| is_red_flag_lap | boolean | red flag (TrackStatus 5, or the heuristic below) |
| is_neutralised_lap | boolean | SC or VSC (exact with FastF1, heuristic otherwise) |
| track_status_source | string | `fastf1` (exact) or `heuristic` |
| lap_start_session_s, lap_end_session_s | double | FastF1 `LapStartTime` / `Time` (session seconds): the lap's telemetry window `[start, end)`. Null without FastF1 |

**Jolpica-only track-status heuristic.** It is used when a race has no FastF1 laps.
- Lap 1 and pit in/out laps are excluded.
- `ratio` = median lap time of the field on that lap / the race's median lap time over all such laps.
- `slow_share` = the share of those cars on that lap that are slower than 1.07 × the race median.
- `ratio > 1.10` **or** `slow_share ≥ 0.5` means **neutralised** (SC or VSC; lap times alone cannot tell them apart,
  so `is_sc_lap` and `is_vsc_lap` stay null). Step 8 tightened this from `ratio > 1.25`, which missed SC deployment
  and restart laps whose field median stays low. Green laps in the sample peak at ratio 1.026 and share 0.056.
- `ratio > 3.0` means **red flag** (the stoppage is included in that lap's time).
- Lap 1 is never flagged, so incidents on lap 1 are missed.
- Example: 2024 R5 (China) gives laps 21–31 (the old rule gave 22–24, 26 and 28–31).
- FastF1 TrackStatus (codes 4 SC, 6/7 VSC, 5 red) replaces the heuristic per lap when FastF1 has that lap.

### pitstops
`stop` (int, per driver), `lap` (int; the in-lap), `time_of_day` (string, local time of day), `duration_s` (double; pit
lane time) and `is_long_stop` (`duration_s` > 60, for example a stop under a red flag).

### stints (Tyre)
One row per stint. Per race, the best available source is used:
1. **fastf1**: FastF1 `Stint`/`Compound`/`TyreLife`. `tyre_age_at_start` = the first lap's TyreLife − 1.
2. **synthetic**: the sample bundle's `tyre_stints`, with `is_synthetic = true`. **Not real.**
3. **jolpica_inferred**: inferred from pit stops, with `compound` and `tyre_age_at_start` null.

Columns: `stint`, `start_lap`, `end_lap`, `laps` (int), `compound` (string: SOFT/MEDIUM/HARD/INTERMEDIATE/WET),
`tyre_age_at_start` (int), `is_synthetic` (boolean), `source` (string).

### telemetry_lap (Telemetry, per lap)
One row per driver per lap.
- `source = fastf1`: aggregated from the car-data samples within the lap's `[LapStartTime, Time)`. Each column is a
  sample average or extreme (samples are not time-weighted):
  - `mean_speed_kph`, `max_speed_kph`;
  - `pct_full_throttle` (share of samples with throttle ≥ 98 %; one definition shared with the feature table,
    `features/telemetry_agg.py`);
  - `brake_fraction` (share of samples braking);
  - `gear_changes` (changes between consecutive samples);
  - `mean_rpm`.
- `source = synthetic`: the sample bundle's `lap_telemetry`, with `is_synthetic = true`. **Not real.**

### telemetry_raw (Telemetry, samples; FastF1 only)
| Column | Type | Meaning |
|---|---|---|
| driver_id | string | mapped from the FastF1 abbreviation |
| driver_code | string | FastF1 `Driver` |
| lap | int | the lap whose window contains the sample (null outside every lap) |
| session_time_s | double | session time (s) |
| date | timestamp | sample wall-clock time (UTC) |
| speed_kph | double | |
| throttle_pct | double | 0–100 |
| brake | boolean | |
| n_gear | int | |
| rpm | double | |

**FastF1 → Jolpica id mapping.**
- The FastF1 `Driver` abbreviation is matched to `drivers.code` per race, with `DriverNumber` → `car_number` as the
  fallback.
- Rows that can't be mapped are dropped and reported (soft quality issue `fastf1_drivers_mapped`).

### Quality report (`clean/_quality/report.json`)
- Hard checks fail the job:
  - null keys;
  - duplicate keys;
  - a race without results or laps;
  - a pit-stop lap outside 1..last+1.
- Soft checks are reported:
  - laps not contiguous from 1;
  - positions not 1..N per lap;
  - pit lap missing from laps (except last+1);
  - `results.laps` ≠ last lap;
  - unparseable times;
  - unmapped FastF1 drivers;
  - stints not tiling the laps.
- Each check records a count and up to 5 examples.

## Feature zone (`{DATA_LAKE_URI}/features/{table}/season=S/round=R/*.parquet`, Step 6)
Schemas: `f1sa.features.schemas`. Every table has `season` and `round`. A row about lap L describes the state at the
**end** of lap L, so the model joins lap L − 1 (see `docs/architecture.md`).

### laps_enriched (key: season, round, driver_id, lap)
| column | type | meaning |
|---|---|---|
| driver_code, constructor_id, constructor_name | string | entry in this race (drivers, results, constructors) |
| grid, final_position, status, finished | int/int/string/bool | from results (the race outcome: not a pre-lap feature) |
| lap_time_s, position | double/int | as in clean laps |
| cumulative_time_s | double | recomputed sum of lap times; null from the first unknown lap time |
| gap_leader_s | double | cumulative − P1's cumulative **at the same lap number** (lapped cars: true time behind, can exceed a lap) |
| gap_ahead_s, ahead_driver_id | double/string | gap to and id of the car one position ahead at the same lap; null for P1 |
| gap_ahead_change_1l / _3l | double | gap_ahead_s now − k laps earlier; null if that lap is missing or the car ahead is a different driver |
| gap_leader_change_1l / _3l | double | gap_leader_s now − k laps earlier; null if that lap is missing |
| is_pit_in_lap, is_pit_out_lap | bool | as in clean laps |
| pit_stops_in_lap, pit_duration_s, is_long_stop | int/double/bool | stops whose in-lap is this lap (0 / null / false if none) |
| stint, compound | int/string | the clean stint covering the lap (null if none) |
| tyre_age_laps | int | tyre age at the end of the lap: tyre_age_at_start + lap − start_lap + 1 |
| tyre_age_assumed_new | bool | the stint's starting age is unknown (jolpica_inferred stint) and assumed to be 0 |
| race_laps, laps_remaining | int | the winner's lap count (fallback: max lap); race_laps − lap |
| lap_start_utc, lap_end_utc | timestamp | **estimated**: race_start_utc + 180 s + cumulative time; null if the start time is unknown |
| lap_time_estimated | bool | always true (Jolpica has no per-lap timestamps) |
| is_sc_lap, is_vsc_lap, is_red_flag_lap, is_neutralised_lap | bool | as in clean laps |

### gaps (key: season, round, driver_id, lap)
`position`, `ahead_driver_id`, `cumulative_time_s`, `gap_leader_s`, `gap_ahead_s` and the four `*_change_*l` columns,
the same as in laps_enriched.

### stints (key: season, round, driver_id, stint)
| column | type | meaning |
|---|---|---|
| stint, start_lap, end_lap, laps | int | reconstructed with the RDD job (split after every pit-in lap that is not the last lap) |
| compound, tyre_age_at_start, is_synthetic | string/int/bool | from the clean stint with the same boundaries |
| compound_source | string | that clean stint's `source` (fastf1, synthetic or jolpica_inferred) |
| matches_clean | bool | a clean stint with identical boundaries exists |

### lap_time_distribution (key: season, round, circuit_id; RDD `aggregateByKey`)
Green racing laps only: not lap 1, not a pit in or out lap, not neutralised or red-flag, time known.
| column | type | meaning |
|---|---|---|
| n_laps | long | number of green laps |
| mean_s, std_s, min_s, max_s | double | std_s is the sample std (null if n_laps = 1) |
| bins_1s | map<int,long> | floor(lap time in s) → count. Histograms merge across races (per-circuit view) |

### pit_loss (key: season, round): history from strictly earlier races
| column | type | meaning |
|---|---|---|
| circuit_id, race_seq | string/int | race_seq = season*100 + round |
| pit_loss_prior_s | double | coalesce(circuit_prior_s, global_prior_s). Pit loss = in-lap + out-lap − 2 × the driver's green median |
| pit_loss_source | string | circuit_prior, global_prior or none |
| pit_loss_missing | bool | no earlier race has a usable stop |
| circuit_prior_s, circuit_prior_stops, circuit_prior_races | double/int/int | median and support at this circuit, earlier races only |
| global_prior_s, global_prior_stops, global_prior_races | double/int/int | median and support over all earlier races |

### team_outlap_history (key: season, round, constructor_id): history from strictly earlier races
| column | type | meaning |
|---|---|---|
| team_history_key | string | lineage key used for the team window (`features/lineage.py`: alphatauri → rb, alfa → sauber) |
| outlap_delta_prior_s | double | coalesce(team, circuit, global prior). Out-lap delta = out-lap − the driver's green median |
| outlap_source | string | team_prior, circuit_prior, global_prior or none |
| outlap_missing | bool | no history at any level |
| team_prior_s/_n, circuit_prior_s/_n, global_prior_s/_n | double/int | median and number of out-laps at each level |

### race_summary (key: season, round): reporting only (describes the race itself)
`circuit_id`, `race_name`, `race_laps`, `entrants`, `finishers`, `classified`, `pit_stops`, `long_stops`,
`stops_per_driver`, `neutralised_laps`, `green_median_lap_s`, `fastest_lap_s`, `fastest_lap_driver_id`,
`winner_driver_id`, `winner_constructor_id`, `winner_race_time_s`.

### telemetry_lap_features (key: season, round, driver_id, lap; `features/telemetry_agg.py`)
The source is chosen per race:
- **`fastf1_raw`**, used when the race has clean `telemetry_raw` samples and lap windows.
  - Samples are re-assigned by `session_time_s` to the half-open window `[lap_start_session_s, lap_end_session_s)`.
    A boundary sample goes to the later lap; samples outside every window are dropped.
  - Every windowed lap gets a row. A lap without samples has `n_samples = 0` and null aggregates.
  - `is_synthetic = false`.
- **Pass-through**, used otherwise. The clean `telemetry_lap` rows are copied unchanged (synthetic in sample mode,
  `is_synthetic = true`), with `n_samples` null.

| column | type | meaning |
|---|---|---|
| mean_speed_kph, max_speed_kph | double | over the lap's samples |
| pct_full_throttle | double | share of samples with throttle ≥ 98 % |
| brake_fraction | double | share of samples braking |
| gear_changes | int | consecutive samples within the lap whose gear differs (not counted across laps) |
| mean_rpm | double | sample mean |
| n_samples | int | samples in the lap (null for pass-through rows) |
| is_synthetic, source | bool/string | `fastf1_raw`, or the clean row's source (`synthetic`, `fastf1`) |

### outlap_features (key: season, round, driver_id, stint; `features/outlap.py`)
Definitions and parameters: `docs/label_definition.md`, "Out-lap pace features". There is one row per clean stint.
`realised_*` is set only for stints > 1. `expected_*` is the same on every row of a (race, driver).

| column | type | meaning |
|---|---|---|
| constructor_id, team_history_key, circuit_id, race_seq | string/string/string/int | entry and lineage key |
| outlap, outlap_time_s | int/double | the stint's first lap (null for stint 1) |
| reference_clean_lap_s, reference_source | double/string | the driver's median clean lap in this stint (`same_stint`), the previous stint (`previous_stint`), or `none` |
| realised_outlap_delta_s | double | **ORACLE only, not known at decision time**: out-lap − reference |
| outlap_neutralised, is_long_stop | bool | the out-lap is neutralised or red-flag / the in-lap stop was > 60 s (both excluded from history) |
| expected_outlap_delta_s | double | **decision-time safe**: the mean realised delta from strictly earlier races |
| expected_outlap_source | string | driver_team, team, circuit, global or none |
| expected_outlap_missing | bool | no history at any level |
| driver_team_prior_s/_n, team_prior_s/_n, circuit_prior_s/_n, global_prior_s/_n | double/int | the mean and support at each level |
| stop_duration_s | double | the in-lap's total Jolpica pit-lane time (Step 8) |
| race_median_stop_s, race_median_outlap_delta_s | double | medians over the race's history-eligible stops: typical lane time, and the circuit's stop share on the out-lap |
| stop_delta_s | double | **ORACLE**: stop_duration_s − race_median_stop_s (pit-crew execution; null for long stops) |
| outlap_pace_residual_s | double | **ORACLE**: realised − race_median_outlap_delta_s − stop_delta_s (warm-up and tyre effect; eligible stops only) |
| expected_outlap_pace_residual_s, expected_outlap_pace_source, expected_outlap_pace_missing | double/string/bool | **decision-time safe**: the shrunk prior of the residual. Source: driver_team (n ≥ 5), driver_team_shrunk, team, global or none |
| pace_driver_team_n, pace_team_n, pace_global_n | int | support of the residual prior at each level |
| expected_stop_delta_s, expected_stop_source, stop_team_n, stop_global_n | double/string/int | **decision-time safe**: the shrunk team prior of stop_delta_s (team, global or none), with its support |

`expected_outlap_delta_s` is legacy: it is mostly stop time, so it is collinear with `pit_loss`. The model uses the
decomposed priors. See `docs/label_definition.md`.

### undercut_attempts (key: season, round, attacker_id, lap, gap_threshold_s, response_window_laps; `features/undercut.py`)
There is one row per (race, attacker, attempt lap L, G, k). The definition, edge cases and leakage rules are in
`docs/label_definition.md`, "Undercut attempts". A race with no attempt has no partition.

| Column group | Columns | Use |
|---|---|---|
| identity | season, round, attacker_id, defender_id, lap (L, A's in-lap), decision_lap (L − 1), gap_threshold_s (G), response_window_laps (k), is_default_params (G = 3.5, k = 2) | |
| label | detector_eligible (passed the shared rule at L − 1), outcome_category (success, fail, defender_stayed_out, excluded_<reason>), label (1/0 for success/fail, else null), exclusion_reasons (array, priority order) | target |
| decision-time | gap_s, gap_ahead_change_1l/3l, attacker_position, attacker/defender tyre age and compound, tyre_age_delta_laps, compound_is_synthetic, attacker/defender pace_last3_s and deg_slope_s_per_lap and their deltas, laps_remaining, pit_loss_prior_s/_source, rejoin_traffic_cars, rejoin_nearest_ahead_s, attacker/defender expected out-lap pace residual (+ source), attacker/defender expected stop delta | model inputs (`undercut.DECISION_FEATURES`) |
| legacy | attacker_expected_outlap_delta_s_legacy | S16 comparison only |
| ORACLE | oracle_attacker_realised_outlap_delta_s, oracle_attacker_outlap_pace_residual_s, oracle_attacker_stop_delta_s | ORACLE ablation only |
| outcome | outcome_defender_pit_lap, outcome_label_lap, outcome_attacker/defender_position, outcome_gap_s (cum_D − cum_A on the label lap), outcome_attacker/defender_stop_s, outcome_*_slow_stop, outcome_*_double_stack | analysis only, never features |

`results/undercut_counts_sample.csv` (written by `--source sample`) contains **smoke-test counts only**: 2 races and
synthetic compounds.

## Sample bundle (`data/sample/`)
`make sample-data` validates the bundle offline, or builds it if it is missing (`REBUILD=1` forces a rebuild). It is a
read-only mini raw lake with the same table-first layout as the raw zone (`docs/architecture.md`), covering
**2023 round 5 (Miami)** and **2024 round 5 (China)**.

| Path | Kind | Content |
|---|---|---|
| `raw/jolpica/schedule/season=S/schedule.json` | real | Jolpica `{S}/races` pages (whole season) |
| `raw/jolpica/drivers/season=S/drivers.json` | real | Jolpica `{S}/drivers` (whole season) |
| `raw/jolpica/constructors/season=S/constructors.json` | real | Jolpica `{S}/constructors` (whole season) |
| `raw/jolpica/{results,qualifying,pitstops,laps}/season=S/round=5/<t>.json` | real | Jolpica race pages, as received |
| `raw/synthetic/tyre_stints/season=S/round=5/tyre_stints.parquet` | **SYNTHETIC** | see below |
| `raw/synthetic/lap_telemetry/season=S/round=5/lap_telemetry.parquet` | **SYNTHETIC** | see below |
| `MANIFEST.json`, `LICENSE`, `README.md` | meta | sha256, size, rows and origin of every file; seed; licence |

- Real data is from Jolpica-F1, CC BY-NC-SA 4.0 (`data/sample/LICENSE`).
- **No FastF1 data** is included; validation rejects `raw/fastf1/`, `.ff1pkl` files and FastF1 column names.
- Not included: 2024 R5 sprint results (not requested).

## Synthetic data (`f1sa.ingest.synthetic`, generator `synthetic-v1`)

> **This is NOT real compound or telemetry data.** It is generated from the fixed assumptions below so that the
> pipeline can run offline without F1 TV credentials. **Sample-mode model results are smoke tests only and must
> never be reported as findings.** Every row has `is_synthetic = true`, plus `generator_version` and `seed`.

**Determinism.**
- Every draw comes from `numpy.random.SeedSequence([seed, season, round, crc32(driver_id), stream])`.
  A driver's rows therefore depend only on the seed and that driver's real data, not on row order or other drivers.
- Validation regenerates the tables and requires byte-identical Parquet. This holds with the locked dependency
  versions in `uv.lock`; a different NumPy/pyarrow version may change the bytes.
- The bundle uses seed 4242.

**Plausible in distribution, not per race.** The output matches the assumed distributions overall, but any
single stint can be unusual. For example, in the 2024 R5 bundle Verstappen runs a 33-lap final stint on SOFT (a 5%
draw). Never read individual synthetic values as facts.

### `tyre_stints`
One row per stint, with stints inferred from **real** Jolpica laps and pit stops.

| Column | Type | Meaning |
|---|---|---|
| season, round | Int16, Int8 | race |
| driver_id | string | Jolpica driver id |
| stint | Int8 | 1, 2, … |
| start_lap, end_lap, laps | Int16 | stint lap range (inclusive) and length |
| compound | string | **synthetic**: SOFT / MEDIUM / HARD |
| tyre_age_at_start | Int16 | **synthetic**: 0 = new set, 1–3 = used set |
| is_synthetic | boolean | always true |
| generator_version, seed | string, Int64 | provenance |

Stint inference, from real data:
- A driver's race ends at their last timed lap, so DNFs end early and lapped cars have fewer laps.
- A stop on lap *p* ends a stint at *p*; the next stint starts at *p* + 1.
- Stops on or after the last lap start no stint, and duplicate stops are collapsed.
- Drivers with no laps (DNS) get no rows.
- Limitation: red-flag tyre changes are not pit stops in Jolpica and are invisible.

Compound assumptions (dry race only; no INTERMEDIATE/WET):
- First stint: SOFT .25 / MEDIUM .55 / HARD .20. If longer than 45% of race laps: .10/.45/.45. If shorter than
  20%: .45/.45/.10.
- Later stints: longer than 40% of race laps: .05/.35/.60. Shorter than 20%: .55/.35/.10. Otherwise .20/.50/.30.
- A driver with two or more stints always uses at least two compounds (as the FIA dry-race rule requires). If all
  draws match, the longest later stint is redrawn among the other compounds using its own length-based odds.
- `tyre_age_at_start` is 1–3 laps (uniform) with probability 0.2, otherwise 0.

### `lap_telemetry`
One row per **real** (driver, lap) timing.

| Column | Type | Meaning (all **synthetic** except keys) |
|---|---|---|
| season, round, driver_id, lap | Int16, Int8, string, Int16 | keys from real Jolpica laps |
| mean_speed_kph | Float64 | lap mean speed, 60–260 |
| max_speed_kph | Float64 | lap max speed, ≥ mean + 20, ≤ 360 |
| pct_full_throttle | Float64 | share of the lap at full throttle, 0–1 |
| brake_fraction | Float64 | share of the lap braking, 0 to 1 − pct_full_throttle |
| gear_changes | Int16 | gear changes in the lap, ≥ 0 |
| mean_rpm | Float64 | 7,000–13,500 |
| is_synthetic, generator_version, seed | | provenance |

Generating assumptions:
- Per race, circuit baselines are drawn uniformly from these ranges:
  - mean speed 190–230 km/h, max 315–340;
  - full throttle 0.55–0.75, brake 0.12–0.22;
  - gear changes 40–70, mean RPM 10,500–11,500.
- Per lap, `pace = driver's median lap time / lap time`, clipped to 0.5–1.1. Slow laps (lap 1, pit in/out, safety car)
  therefore get lower values:
  - mean speed × pace, max speed × (0.92 + 0.08·pace), full throttle × pace²;
  - brake × pace, gear changes × pace, RPM × (0.9 + 0.1·pace).
- Mean speed loses 0.03 km/h per lap of synthetic tyre age.
- Gaussian noise is added: speed σ 0.8 (plus a per-driver offset σ 1.0), max σ 2, throttle σ 0.015, brake σ 0.01,
  gears σ 2, RPM σ 60. Values are then clipped to the bounds above.
- A missing real lap time gives missing aggregates. The row is kept and still flagged.
- Caveat: because speed is derived from the real lap time and the synthetic tyre age, these columns are partly
  redundant with real data and partly random. Any "signal" a model finds in them is an artefact.

## Model outputs (`experiments/model_eval.py`, Step 9)
Every file of a `sample` or `rehearsal` run has a `_sample` / `_rehearsal` suffix and a `# SMOKE TEST …` /
`# REHEARSAL …` first line (CSV) or a title and watermark (PNG). **They are not findings.**

| File | Columns / content |
|---|---|
| `results/model_eval*.csv` | source, smoke_test, feature_set, g, k, model (`lr`, `lr_calibrated`, `gbt`, `gbt_calibrated`, `majority`, `rule`), split (`validation`, `test`), metric (`f1_positive`, `macro_f1`, `pr_auc`, `roc_auc`, `brier`, `ece`), value, ci_kind (`race_cluster` / `attempt`, test only), ci_low, ci_high (95% percentile), n_valid (usable resamples), n_attempts, n_races, note (why a metric is undefined) |
| `results/calibration*.csv` | model, split, chosen_method, brier_raw / _isotonic / _platt / _calibrated, ece_raw / _calibrated, n_attempts, reason |
| `results/feature_importance*.csv` | model, method (`gbt_importance`, `lr_std_coefficient`, `permutation_pr_auc_drop`), feature, value, std, note |
| `results/reliability*.png` | observed vs predicted (10 bins), raw vs calibrated, for the selected model (test season; validation in sample mode, marked) |
| `results/confusion_matrix*.png` | the selected calibrated model at 0.5 |
| `results/feature_importance*.png` | GBT importance and permutation drop (top 15) |


## Full-data quality notes (Step 8b)

First run of clean and features on the complete 2021-2025 Jolpica raw zone, in HDFS mode
(`make pipeline-full`). Source: Jolpica only, so there are no FastF1 or synthetic inputs. Two complete
runs gave identical table checksums and byte-identical result files. Per-race detail is in
`results/full_data_quality.json` and `results/neutralisation_by_race_full.csv`.

### Coverage

| Season | Races | Results | Laps | Pit stops | Stints | Undercut rows (all 9 G, k settings) |
|---|---|---|---|---|---|---|
| 2021 | 22 | 440 | 23,688 | 798 | 1,223 | 3,543 |
| 2022 | 22 | 440 | 23,529 | 805 | 1,232 | 3,321 |
| 2023 | 22 | 440 | 24,386 | 912 | 1,342 | 4,074 |
| 2024 | 24 | 479 | 26,574 | 825 | 1,289 | 3,582 |
| 2025 | 24 | 479 | 26,657 | 821 | 1,285 | 4,257 |

Every season has the expected number of races. Races with unexpected counts:

- **2021 R12** has one lap, no pit stops and no green laps, so it has no `lap_time_distribution` row. The
  build expects exactly the races that have at least one green lap.
- **2024 R3 and 2025 R9** list 19 drivers in results. The other 112 races list 20.
- **37 further races** have one to four drivers in results with no lap rows (62 drivers overall). All 62 have `results.laps = 0`, with a status such as Retired, Collision or Did not
  start, so these are retirements before completing a lap and not missing data.

### Quality checks

28 checks: 0 hard failures and 1 soft issue. `results_laps_match_lap_table` reports 5 drivers, all
Disqualified: Jolpica gives them `laps = 0` although they have lap rows (2024 R21 hulkenberg; 2025 R2 gasly,
hamilton and leclerc; 2025 R4 hulkenberg).

### Constructors

Each season has 10 ids. 2021-2023 use `alfa` and `alphatauri` (both in the lineage map); 2024-2025 use
`sauber` and `rb`. `alpine`, `aston_martin`, `ferrari`, `haas`, `mclaren`, `mercedes`, `red_bull` and `williams`
appear in every season. No id is unknown, so the lineage map is verified on the full data.

### Provenance

No stint or attempt is flagged synthetic, and every stint has `source = jolpica_inferred`. Compound is null
everywhere, telemetry tables have 0 rows, and every lap with a tyre age has `tyre_age_assumed_new = true`. The
`real` feature set builds (52 assembled columns) with no input that can be synthetic. The synthetic generator
did not run.

### Undercut attempt counts

`results/undercut_counts_full.csv` has the full (G, k) grid. Labelled attempts after the red-flag fix (below):

| Split | Seasons | G = 3.5, k = 2 | G = 5.0, k = 3 |
|---|---|---|---|
| Train | 2021-2023 | 108 (32, 32, 44) | 184 (48, 62, 74) |
| Validation | 2024 | 49 | 64 |
| Test | 2025 | 49 | 60 |

At G = 3.5, k = 2 every season has fewer than 50 labelled attempts. At G = 5.0, k = 3 only 2021 does (48).

**Neutralised exclusions, split for reporting.** The last two CSV columns split `n_excluded_neutralised` into safety
car / VSC / red flag and sustained slow running (the wet or drying pattern). This is a timing-only heuristic for
reporting and does not affect labels (rule: `docs/label_definition.md`, "Reporting split of excluded_neutralised").
All 20 table checksums and the labels checksum (`73fe89cf9f4e8ee6`) are identical with and without it.

| Season | G = 3.5, k = 2: SC/VSC/red flag + sustained slow | G = 5.0, k = 3 |
|---|---|---|
| 2021 | 71 + 0 = 71 | 74 + 0 = 74 |
| 2022 | 75 + 12 = 87 | 99 + 15 = 114 |
| 2023 | 88 + 7 = 95 | 114 + 8 = 122 |
| 2024 | 63 + 8 = 71 | 72 + 9 = 81 |
| 2025 | 72 + 10 = 82 | 84 + 11 = 95 |

At G = 3.5, k = 2, 37 of 406 neutralised exclusions (9%) are sustained slow running; 2021 has none.

**Early decision laps.** No attempt at L = 4 or L = 5 is labelled in any season (G = 3.5, k = 2). The 25 candidates
at L = 4 are all excluded first as `in_pit` (15), `neutralised` (5), `teammates` (3) or `defender_stayed_out` (2);
the 19 at L = 5 as `defender_stayed_out` (11), `neutralised` (4) or other reasons. The earliest labelled L is 6
(2023, 2024) or 8-9 (2021, 2022, 2025).

**`excluded_early_lap` varies by season** (G = 3.5: 76, 21, 50, 3, 39 for 2021-2025; it does not depend on k). The
rule excludes an attempt whose stop lap L is below 4. A stop on lap 1 is never a candidate (there is no decision
lap 0), so every early-lap exclusion is a stop on lap 2 or 3 with a car directly ahead within G at the end of
lap L - 1:

| Season | L = 2 | L = 3 | All stops on laps 1 / 2 / 3 | Red-flag stops on laps 2-3 | Red-flag share of the exclusions |
|---|---|---|---|---|---|
| 2021 | 50 | 26 | 14 / 55 / 34 | 47 | 37 of 76 |
| 2022 | 17 | 4 | 23 / 23 / 4 | 18 | 14 of 21 |
| 2023 | 44 | 6 | 45 / 48 / 8 | 17 | 15 of 50 |
| 2024 | 1 | 2 | 40 / 1 / 2 | 0 | 0 of 3 |
| 2025 | 23 | 16 | 13 / 26 / 17 | 0 | 0 of 39 |

An independent recount from `laps` and `pitstops` (stops on laps 2-3 by a car with 0 < gap ahead <= 3.5 s at the
end of L - 1) gives exactly the pipeline's numbers in every season, so the variation comes from the data. It is
driven by a few races with a mass stop on lap 2 or 3 (12-19 exclusions on a single lap): red-flag entries recorded
as stops in 2021 R10, 2021 R11, 2022 R18 and 2023 R20, and mass early stops without a red flag in 2021 R3, 2023 R9
and 2025 R1. 2024 has 3 because almost all its early stops are on lap 1 (40 of 43). Its two early red flags
(2024 R4 and R8) have their stops recorded on lap 1, and lap-1 stops are never candidates.

### Neutralisation rule on real races

After the Step 8b red-flag fix, the Jolpica heuristic flags 548 of 6,804 race laps as neutralised (8.1%), and 18 of
them as red-flag laps.

| Season | Race laps | Neutralised | Share | Red-flag laps |
|---|---|---|---|---|
| 2021 | 1,297 | 80 | 6.2% | 6 |
| 2022 | 1,294 | 162 | 12.5% | 3 |
| 2023 | 1,325 | 111 | 8.4% | 6 |
| 2024 | 1,444 | 89 | 6.2% | 3 |
| 2025 | 1,444 | 106 | 7.3% | 0 |

Per race, the median share is 5.3% (Q1 0%, Q3 12.1%). Using Tukey fences (above 30.2%), the unusually high races
are 2022 R7 (28 of 64 laps, 43.8%), 2023 R6 (27 of 78, 34.6%) and 2023 R13 (23 of 72, 31.9%). 2022 R4 (18 of 63,
28.6%) is just under the fence. No race falls below the low fence, because it is negative. 31 races have no flagged
laps (`neutralisation.zero` in `results/full_data_quality.json`).

**Red-flag fix (Step 8b).** Jolpica records a red-flag entry to the pit lane as a pit stop for the whole field, and
the original heuristic removed pit laps before testing for a red flag, so none was ever detected. The field-wide
test (`docs/label_definition.md`, "Track status") now flags these laps:

| Race | Neutralised laps before -> after | Red-flag laps |
|---|---|---|
| 2021 R2 | 15 -> 16 | 1 |
| 2021 R6 | 7 -> 8 | 1 |
| 2021 R10 | 1 -> 2 | 1 |
| 2021 R11 | 0 -> 1 | 1 |
| 2021 R21 | 12 -> 14 | 2 |
| 2022 R7 | 27 -> 28 | 1 |
| 2022 R10 | 5 -> 6 | 1 |
| 2022 R18 | 3 -> 4 | 1 |
| 2023 R3 | 7 -> 10 | 3 |
| 2023 R13 | 22 -> 23 | 1 |
| 2023 R19 | 3 -> 4 | 1 |
| 2023 R20 | 1 -> 2 | 1 |
| 2024 R4 | 1 -> 2 | 1 |
| 2024 R21 | 11 -> 12 | 1 |
| 2024 R8 | 1 -> 2 | 1 (lap 1) |

2024 R8 was not one of the 14 races found before the fix, because that check started at lap 2; its stoppage is on
lap 1. No other race changed. With the 3-lap red-flag restart period, exactly one attempt moved from labelled to
excluded (2024 R4, at both G = 3.5, k = 2 and G = 5.0, k = 3), and three `defender_stayed_out` attempts (2022 R10
and 2024 R4) became `excluded_neutralised`. No attempt was added or removed, and no 2025 attempt changed (2025 has
no red-flag lap).

**Spot check of the high-share races.** The flagged shares are plausible as "not green dry running", but most of the
flagged laps in these four races are a wet or drying track, not a safety car. The timing shows two patterns:

- a safety car: a step to a uniform 1.3-1.7x for a few laps, every car slow (2022 R7 laps 26-29, 2023 R13 laps
  16-21, 2022 R4 laps 2-4);
- a wet track: every car slow at 1.1-1.3x for 10-25 laps, slowly converging to 1.0, and ending in a wave of pit stops
  (the crossover to slicks or to intermediates): 2022 R7 laps 5-22 (stops on laps 17-22), 2022 R4 laps 5-19 (14
  cars stop on laps 18-19), 2023 R6 laps 52-78 (stops on laps 53-56), 2023 R13 laps 2-9 and 61-63.

Red-flag laps are now flagged in 2022 R7 (lap 30) and 2023 R13 (lap 64). Excluding wet laps is consistent with the
label definition, which excludes wet running (reason 15). In full mode, compounds are null, so reason 15 cannot fire
and the heuristic is the only thing that excludes them; those attempts are counted as `neutralised`, not `wet`.
