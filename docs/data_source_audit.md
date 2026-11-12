# Data-source audit: TracingInsights (primary) and OpenF1 (cross-check)

Step 4C, Phase 1. Audit only: no code in `src/`, nothing written to `data/`, no model trained, no 2025 label read.
Measured on 2026-11-11. Every number below was measured unless it is marked *estimate*.

**Why:** since mid-October 2026, FastF1 downloads need an F1 TV Access login, which nobody on the team has. As a
result, compound, tyre life, race control, exact track status and telemetry are null or synthetic in full mode
(PROGRESS "Known issues").

**2025 is sealed.** For 2025 this file reports only field availability, completeness shares and the compound
agreement with OpenF1. It gives no lap times, positions, pit laps, stint lengths, compound mix or race-control
content. The 2025 files were kept in a separate scratch folder and deleted once the counts were computed.

## 1. Where the data lives

| Repository | Owner now | Default branch | Pinned HEAD (audit) | GitHub `size` | Licence (LICENSE file) |
|---|---|---|---|---|---|
| 2021 | `TracingInsights/2021` | main | `5815eb898310d63a9054dbb98cc27c54355fac06` | 21.6 GB | MIT, "Copyright (c) 2023 TracingInsights-Archive" |
| 2022 | `TracingInsights/2022` | main | `df396d091c4a40a3a6b733631da3497f8c91d89f` | 18.4 GB | MIT |
| 2023 | `TracingInsights/2023` | main | `737c84d3ae14b9883a671ca248d015b69cf1cb58` | 19.0 GB | MIT, "Copyright (c) 2023 TracingInsights-Archive" |
| 2024 | `TracingInsights/2024` | main | `d1543a71370d9b617b627319f8ad80b2f85dfd5e` | 19.5 GB | MIT, "Copyright (c) 2024 TracingInsights-Archive" |
| 2025 | `TracingInsights/2025` | main | `dfb3ea3f50b69857fe8e40cabe5e899743c59f6a` | 15.6 GB | Apache-2.0 |
| PitStops | `TracingInsights-Archive/PitStops` | main | `4d92192f5a987d2468a4a878e4363864c7e0ced1` | 252 KB | MIT |

- All five season repos are live under `TracingInsights`: not archived, last pushed 2026-10-30. Older snapshots
  are under the `TracingInsights-Archive` account (`2018`…`2026`, last pushed April to June 2026). Because the live
  repos are still being pushed, **Phase 2 must pin a commit SHA per season**.
- **Correction to the brief:** only 2025 is Apache-2.0. GitHub detects MIT for 2021-2024, and the LICENSE files
  confirm it.
- The README is a single line ("Public F1 telemetry files"), so there is **no data reference**. The field meanings
  below come from the files themselves and their FastF1 equivalents.
- Each repo has a `sync_hf_bucket.py` that mirrors the tree to a Hugging Face bucket (`tracinginsights/<season>`).
  It is not used here, because Hugging Face is not on the allowed host list.
- **PitStops** is not a pit-stop timing table. It is the DHL "fastest pit stop" award data: per race, one row per
  team with `Pos.`, `Team`, `Driver` (surname), `Time (sec)` (stationary time), `Lap` and `Points`. It covers
  2018-2026 (22/22/22/24/24 files for 2021-2025). It has some value as a cross-check of the fastest stationary times
  only. It is not needed for stints.

### Layout (the same in every season)
`<Event name>/<Session>/` where the session is `Practice 1-3`, `Qualifying`, `Sprint`, `Sprint Qualifying`,
`Sprint Shootout` or **`Race`**. In 2021, `Sprint Qualifying` is the sprint itself. A Race folder holds:

| File | Content | Size |
|---|---|---|
| `session_laptimes.json` | all drivers' laps, column-oriented (42 fields) | ≈ 300-400 KB |
| `rcm.json` | race-control messages (9 fields) | 5-20 KB |
| `session_info.json` | t0 date, session start, total laps, **track status timeline**, session status, meeting metadata | ≈ 1 KB |
| `drivers.json`, `corners.json`, `weather.json`, `team_radio.json` | driver↔team, circuit corners, weather, radio index | small |
| `<DRV>/laptimes.json` | that driver's rows of `session_laptimes.json` | ≈ 17 KB |
| `<DRV>/<lap>_tel.json` | **one telemetry file per driver per lap** | 125-150 KB |
| `<DRV>/radio/*.mp3` | team radio audio (F1 audio, **never download**) | ≈ 180 KB each |

## 2. Sizes and the download plan

Method:
- File paths come from blob-free shallow clones (`git clone --filter=blob:none --depth=1 --no-checkout`; 15.5 MB
  of tree objects for all five).
- Blob sizes come from the GitHub trees API. The recursive tree is truncated at about 68k entries for 2021/2023
  (91% of Race files have an exact size) and returns HTTP 500 for 2022/2024/2025.
- Where no exact size was available, the size is *estimated* as file count × the pooled mean size from the exact
  trees (telemetry 149,081 B per file).

| Season | Race files | Telemetry files | Race-only, no radio | of which telemetry | Lap/rcm/info tables | Radio (excluded) |
|---|---:|---:|---:|---:|---:|---:|
| 2021 | 27,196 | 23,757 | 3.70 GB (91% exact) | 3.69 GB | 15.4 MB | 0.55 GB |
| 2022 | 24,146 | 23,576 | 3.53 GB *estimate* | 3.51 GB | 14.6 MB | none |
| 2023 | 27,877 | 24,422 | 3.51 GB (91% exact) | 3.49 GB | 16.3 MB | 0.47 GB |
| 2024 | 30,043 | 26,606 | 3.98 GB *estimate* | 3.97 GB | 17.3 MB | 0.50 GB |
| 2025 | 28,104 | 26,692 | 4.00 GB *estimate* | 3.98 GB | 17.3 MB | 0.14 GB |
| **Total** | 137,366 | 125,053 | **18.7 GB** | **18.6 GB** | **≈ 81 MB** | 1.7 GB |

The measured per-driver sample confirms the telemetry estimate: one driver's whole race is 7.7-9.8 MB, with
125-146 KB per lap (median) across the four non-sealed seasons.

**Plan for Phase 2 (to be decided by you):**
- **A. Tables (≈ 81 MB, about 570 files).** `session_laptimes.json`, `rcm.json`, `session_info.json` and
  `drivers.json` for all 114 races, fetched from `raw.githubusercontent.com/<owner>/<season>/<SHA>/<path>`, with a
  sha256 manifest and resume. This is below the 1 GB limit.
- **B. Telemetry (18.6 GB, 125k files).** This is well above 1 GB, and it would take about 35 h at 1 request per
  second over raw.githubusercontent. Options:
  - B1: everything, through a sparse partial clone (`git sparse-checkout` of `*/Race/*/*_tel.json`), which fetches
    the blobs in packs. This needs 18.6 GB in the lake (HDFS capacity not checked; that needs a docker command).
  - B2: a subset for the experiments, for example all 2023-2024 races (≈ 7.5 GB) or one season (≈ 3.5-4 GB), for
    #1/#4 and the S10 scaling runs. The per-lap aggregates for other races stay null with a source flag.
  - B3: only the laps the features need (L−1 of the attacker and defender of each candidate pair). This is a few
    thousand files (≈ 1-2 GB *estimate*). The selection uses decision-time candidates, never labels. It still needs
    your OK for 2025.
- Never: radio mp3s, practice, qualifying or sprint sessions.

## 3. Field reference (identical for 2021-2025)

The key sets are identical in all 114 races: `session_laptimes.json` has the same 42 keys every season, `rcm.json`
the same 9, and telemetry the same 18 channels. Nulls are the **string `"None"`**, so numeric columns mix float and
str; the loader must parse them.

### Laps: `session_laptimes.json` (column arrays, one entry per driver-lap; FastF1 `Laps` equivalent)
| Need | TI field | Type / unit | FastF1 origin |
|---|---|---|---|
| Driver | `drv`, `dNum`, `team` | code (VER), racing number as str, team name | Driver, DriverNumber, Team |
| Lap | `lap` | int | LapNumber |
| Lap time | `time`; sectors `s1` `s2` `s3` | seconds (float) | LapTime, Sector1-3Time |
| **Compound** | `compound` | SOFT, MEDIUM, HARD, INTERMEDIATE, WET, UNKNOWN or "None" | Compound |
| **Tyre life** | `life` (+ `fresh` bool) | laps on the set, including earlier use (1 on the first lap of a new set) | TyreLife, FreshTyre |
| **Stint** | `stint` | int, 1-based | Stint |
| Lap start / end | `lST` (start) and `sesT` (end), plus `lSD` (start date) | seconds of session time; `lSD` is UTC ISO with ns | LapStartTime, Time, LapStartDate |
| **Pit in / out** | `pin` (on the in-lap), `pout` (on the out-lap) | seconds of session time or "None" | PitInTime, PitOutTime |
| **Position** | `pos` | int, at the end of the lap | Position |
| Track status per lap | `status` | concatenated TrackStatus codes, e.g. "1", "4", "41", "124", "126" (1 green, 2 yellow, 4 SC, 5 red, 6 VSC, 7 VSC ending) | TrackStatus |
| Quality flags | `iacc`, `del`/`delR`, `ff1G`, `pb` | bool / str | IsAccurate, Deleted/DeletedReason, FastF1Generated, IsPersonalBest |
| Speed traps | `vi1` `vi2` `vfl` `vst` | km/h | SpeedI1/I2/FL/ST |
| Weather at lap | `wT` `wAT` `wH` `wP` `wR` `wTT` `wWD` `wWS` | s, °C, %, mbar, bool, °C, deg, m/s | Laps.get_weather_data() |

`session_info.json`: `t0_date`, `session_start_time` (s), `total_laps`, `track_status` {`Time` (s), `Status`,
`Message`} (the session-level timeline, so SC/VSC/red flag are exact), `session_status`, `session_info.Meeting`
(Name, Key, Location, Country, Circuit).

### Race control: `rcm.json`
`time` (UTC ISO, ns) · `cat` (Flag, Other, Drs, CarEvent, SafetyCar) · `msg` (text, e.g. "SAFETY CAR DEPLOYED",
"FIA STEWARDS: 5 SECOND TIME PENALTY FOR CAR 14 (ALO) - …") · `status` · `flag` (GREEN, YELLOW, DOUBLE YELLOW, RED,
BLUE, CLEAR, CHEQUERED, BLACK AND WHITE) · `scope` (Track, Sector, Driver) · `sector` · `dNum` · **`lap` (int)**.
At least 99.95% of messages carry a lap number in every season (2021-2024: 2034/2035, 1998/1998, 1849/1849,
2130/2131). Penalties are machine-readable from the text, which fills the Jolpica penalty gap in Known issues.

### Telemetry: `<DRV>/<lap>_tel.json` → `{"tel": {...}}` (FastF1 car data merged with position data)
| Channel | Unit / values (seen in samples) |
|---|---|
| `time` | seconds **from lap start** (the first sample is 0.0) |
| `speed` | km/h (0-350) |
| `rpm` | 1/min (max ≈ 12.6k) |
| `gear` | 1-8 |
| `throttle` | % (0-100) |
| `brake` | 0/1 |
| `drs` | 0/1, already binarised (FastF1 raw has codes 0-14); always 0 in the 2021 Monaco sample, to be checked in Phase 2 |
| `distance`, `rel_distance` | m from lap start; fraction of the lap (0-1) |
| `x` `y` `z` | position (FastF1 units, 1/10 m) |
| `acc_x` `acc_y` `acc_z` | accelerations derived by TracingInsights (unit not documented) |
| `DriverAhead`, `DistanceToDriverAhead` | code; m |
| `dataKey` | "season-event-session-driver-lap" |

Sampling: median Δt 0.116-0.131 s (≈ 8 Hz merged), p95 0.22-0.27 s, 660-770 samples per lap. There were no nulls
in speed, rpm, gear, throttle, brake, drs or distance in the four sampled driver-races (2021-2024). Because samples
come per lap, the `[lap_start, lap_end)` assignment in `features/telemetry_agg.py` is already done
(session time = `lST` + `time`).

## 4. Joining to our Jolpica keys

- **Event → round:** the folder name (equal to `session_info.Meeting.Name`) matches the Jolpica `raceName` after
  normalisation (NFKD, letters only) for **114/114** manifest races, with no ambiguity. That includes
  "São Paulo Grand Prix", "Mexico City Grand Prix" and "Emilia Romagna Grand Prix". Phase 2 also checks the date
  (`t0_date` against the Jolpica race date).
- **Driver code → `driverId`:** TI `drv` equals the Jolpica `Driver.code` for **2,272/2,272** TI driver-races
  (2021-2025). The fallback `dNum` → Jolpica `number` was never needed, but stays, as in `etl/clean.py`. Every
  Jolpica driver with laps > 0 is present in TI.
- **Lap universe:** TI has 476 driver-laps more than Jolpica's `results.laps` (all seasons). 221 of them have a
  null lap time (the lap in progress at retirement or the flag). Phase 2 joins on Jolpica `(driverId, lap)` and
  reports TI-only rows, so the v1 lap universe is unchanged.

## 5. Completeness per season (all 114 Race sessions)

Shares are of TI driver-laps. "Compound" excludes UNKNOWN and "None".

| Season | Races present | Races with RCM | TI drivers / Jolpica entries | Compound | Tyre life | Stint | Position | Lap start+end | Telemetry file per lap | IsAccurate |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2021 | 22/22 | 22/22 | 439/440 | 99.75% | 100% | 100% | 99.89% | 100% | 100% | 87.3% |
| 2022 | 22/22 | 22/22 | 438/440 | 98.58% | 97.79% | 100% | 99.83% | 100% | 100% | 83.3% |
| 2023 | 22/22 | 22/22 | 438/440 | 99.86% | 99.86% | 100% | 99.86% | 100% | 100% | 87.2% |
| 2024 | 24/24 | 24/24 | 478/479 | 100% | 100% | 100% | 99.89% | 100% | 100% | 88.5% |
| 2025 | 24/24 | 24/24 | 479/479 | 98.12% | 98.03% | 98.33% | 99.88% | 100% | 100% | 86.3% |

- The missing driver-races are entries without a lap (DNS).
- The telemetry share is file presence from the tree, not content. Content was checked on the downloaded driver
  samples only, as agreed.
- `IsAccurate` false (12-17%) is FastF1's usual flag on in- and out-laps, lap 1 and neutralised laps. TI copies it
  and does not mark anything else as inaccurate.

**Gaps (2021-2024, races where compound or tyre life is below 95%):**
- 2021 R12 Belgium: compound 0% (all UNKNOWN). The race was run entirely behind the safety car and has no green
  lap; it already has no labelled attempt in v1.
- 2022 R7 Monaco: tyre life 86.3%; compound 97.0%.
- 2022 R11 Austria: compound 77.4%, tyre life 72.9%. This is the one material gap: those laps need inferred or
  synthetic stints with a `source` flag.
- 2025: 2 races are below 95% compound coverage (count only).

## 6. OpenF1 cross-check (compounds)

Requests: `GET /v1/sessions?year=Y&session_name=Race` (3), then `/v1/stints?session_key=K` and
`/v1/drivers?session_key=K` (3 + 3). That is 9 requests at ≤ 1 per second. OpenF1 stints were expanded to laps
(`lap_start..lap_end`, tyre age = `tyre_age_at_start` + offset + 1) and joined to TI on (code, lap).

| Race | Overlapping laps | Compound agreement | Tyre-life agreement | Stint-number agreement |
|---|---:|---:|---:|---:|
| 2023 R5 Miami (session 9078) | 1138 / 1138 TI | **100.00%** | 100.00% | 100.00% |
| 2024 R5 China (session 9673) | 1030 / 1032 TI | **100.00%** | 100.00% | 100.00% |
| 2025 R5 Saudi Arabia (session 10022) | all TI laps | **100.00%** (0 disagreeing laps) | not reported (sealed) | not reported |

- 2024: two TI laps have no OpenF1 stint coverage.
- Both sources come from the same F1 live-timing feed, so this agreement confirms that TI converted the data
  faithfully. It is not independent ground truth.
- OpenF1 also lists a 2023 Imola "Race" session, although that race was cancelled. This is one more reason to keep
  the Jolpica schedule as the master list.

## 7. Licences and terms

- **TracingInsights:** MIT (2021-2024) and Apache-2.0 (2025). These licences cover the authors' code, files and
  packaging. The underlying timing, telemetry and race-control data is Formula 1 / FOM data taken from the F1
  live-timing feed through FastF1. Neither licence grants rights to FOM data. We therefore treat it exactly like
  FastF1 data:
  - private academic study only;
  - never committed or redistributed; `data/` stays gitignored;
  - attribution to TracingInsights and FastF1 in DATA_LICENSES.md;
  - no radio audio downloaded.
- **OpenF1:** unofficial and not affiliated with F1; historical data is free without login. Its terms page
  (openf1.org) is not on the allowed host list, so the exact terms were **not re-verified**. They are recorded in
  DATA_LICENSES.md as "check before any non-academic use", and stay cross-check only.
- **PitStops (DHL award data):** MIT on the repository; the data comes from DHL's public award API.

## 8. Risks

- No data reference, and fields are inferred from FastF1 names. Mitigation: validate the schema on load (all 42/9/18
  keys present, types parsed), with tests on trimmed fixtures.
- The repos are rewritten and pushed regularly (2026-10-30). Mitigation: pin SHAs, keep a sha256 manifest, fetch
  `raw.githubusercontent.com/.../<SHA>/...` only.
- The data is FastF1-derived, so FastF1 inaccuracies carry over (`IsAccurate` false on about 13% of laps;
  FastF1-generated laps 0.1-0.2%).
- Telemetry volume (18.6 GB) against HDFS capacity and the time to download it; see options B1-B3.
- Two GitHub API limits apply: the unauthenticated limit is 60 requests per hour (30 used in this audit), and
  recursive trees fail for the large repos. Phase 2 therefore needs no GitHub API at all: paths come from a
  blob-free clone, and the files come from raw.githubusercontent or a sparse checkout.

## 9. Recommendation: **GO** (tables), **GO with a decision** (telemetry)

- **Tyre compound, tyre life, stints, pit in/out, exact track status and race control: GO for all of 2021-2025.**
  Coverage is ≥ 97.8% in every season, OpenF1 agrees 100% on 3 races, and the download is 81 MB. 2021-2022 are
  **not** incomplete except for 2022 R11 Austria (77%) and the 2021 R12 non-race. The rule for those: the TI value
  where it exists, otherwise null with `source = 'inferred'` (never synthetic in full mode). Any other gap is
  filled the same way.
- **Telemetry: GO, but choose B1 / B2 / B3** (section 2). My suggestion is **B2 (2023-2024, ≈ 7.5 GB, through a
  sparse partial clone)** for #1/#4/#28, and per-lap aggregates computed in the clean zone. For #30 (the telemetry
  ablation) all seasons would be needed, so B1 is the option if HDFS has room for it.
- Fallback options if you decide against TI for some seasons: OpenF1 stints for 2023+ only (no 2021-2022), or keep
  2021-2022 on inferred stints and restrict real-compound features to 2023+.

## 10. Notes for Phase 2 (constraints carried forward)

- **Additive only.** New clean tables (`stints_ti`, `telemetry_lap_ti`, `race_control`, `track_status_ti`, names to
  be confirmed) get `is_synthetic = false` and `source = 'tracinginsights'`.
- The current `etl/clean.py` precedence (FastF1 → synthetic → inferred) must **not** pick up TI rows. Otherwise the
  v1 `stints`, tyre age, `undercut_attempts` and labelled-set checksums would change. Any switch to TI-backed
  features is a later, explicit v2 decision.
- The loader parses the "None" strings, validates the 42/9/18 key sets, maps codes to `driverId` through Jolpica
  results and events to rounds by name plus date, and never reads radio files.

## Appendix: requests made in this audit

| Host | Requests | What |
|---|---:|---|
| api.github.com | 30 (unauthenticated) | 5 repos, 5 HEAD SHAs, 9 trees (2 truncated, 6 HTTP 500), 2 org (404) + 2 user listings, 2 PitStops (rate-limit checks excluded) |
| github.com | 5 git clones | `--filter=blob:none --depth=1 --no-checkout`, 15.5 MB of tree objects |
| raw.githubusercontent.com | about 500 | 228 lap-time + rcm files (all 114 races), session info and drivers for 5 samples, one driver's full-race telemetry for 2021-2024 R5 (one lap file for 2025, schema only), 9 README/LICENSE/PitStops files |
| api.openf1.org | 9 | sessions, stints and drivers for 3 races |

Total downloaded ≈ 89 MB (73 MB of data files plus 15.5 MB of git trees), all in the session scratchpad and none
in `data/`. 2025 files were deleted after counting.
