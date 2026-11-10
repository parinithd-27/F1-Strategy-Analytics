# Test fixtures

## `jolpica/`: hand-trimmed Jolpica-F1 API responses

- **Source:** `https://api.jolpi.ca/ergast/f1/` (Jolpica-F1, Ergast-compatible API), recorded on 2026-11-10.
- **Licence:** © Jolpica-F1 contributors, licensed under **CC BY-NC-SA 4.0**
  (https://creativecommons.org/licenses/by-nc-sa/4.0/). The files are used here for non-commercial academic testing,
  with attribution. They are adapted (trimmed), and the adaptations below are shared under the same licence.
- **Contents:** the 2023 Miami Grand Prix (season 2023, round 5) and small season and race listings.

### How they were trimmed
- `results`, `qualifying`, `drivers`, `pitstops`: only `max_verstappen`, `perez` and `sargeant`.
  `constructors`: only `red_bull` and `williams`. `MRData.total` was set to the trimmed count.
- `seasons`: only 1950 and 2021–2025. `races_2023`: only rounds 4–6.
- `laps_2023_5_p{0,1,2}`: laps 1–3 for the same three drivers (9 timings), re-paged with
  `MRData.limit = 4` (offsets 0, 4 and 8). This deliberately splits laps 2 and 3 across pages, as
  happens in real data once retirements change the timings-per-lap count. It exercises page merging and the
  "server caps `limit`" path. Each timing value is real and unchanged.
- `race_2023_5`: unmodified.
- Added in Step 4A (recorded on 2026-11-10): the 2023 Azerbaijan GP (round 4, a sprint weekend).
  `sprint_2023_4`, `results_2023_4`, `qualifying_2023_4` and `pitstops_2023_4` are trimmed to the same three
  drivers. `laps_2023_4_p0` holds laps 1–2 for those drivers (6 timings, one page).
  `drivers_2023` and `constructors_2023` are season-level and trimmed the same way.

No other values were edited. Do not add data beyond what the tests need (see `DATA_LICENSES.md`).
