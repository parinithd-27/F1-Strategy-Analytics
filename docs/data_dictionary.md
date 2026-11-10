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

**Jolpica-only track-status heuristic.** It is used when a race has no FastF1 laps.
- Lap 1 and pit in/out laps are excluded.
- `ratio` = median lap time of the field on that lap / the race's median lap time over all such laps.
- `ratio > 1.25` means **neutralised** (SC or VSC; lap times alone cannot tell them apart, so `is_sc_lap` and
  `is_vsc_lap` stay null).
- `ratio > 3.0` means **red flag** (the stoppage is included in that lap's time).
- Lap 1 is never flagged, so incidents on lap 1 are missed.
- Example: 2024 R5 (China) gives laps 22–24, 26 and 28–31.

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
  - `pct_full_throttle` (share of samples with throttle ≥ 99 %);
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
