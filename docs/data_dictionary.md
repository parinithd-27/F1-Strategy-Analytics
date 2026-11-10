# Data dictionary

This file grows step by step. Step 4B documents the **sample bundle** and its **synthetic tables**.
S5/S6 will add the clean-zone entities (Race, Driver, Lap, Tyre, Pit Stop, Position, Gap, Telemetry; #7).

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
