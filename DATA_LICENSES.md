# Data licences and usage terms

The **code** in this repository is MIT-licensed (see [LICENSE](LICENSE)). The **data** it works with is not covered by that licence. Each source has its own terms, listed below.

| Source | What we use | Licence / owner | Redistributed in this repo? |
|---|---|---|---|
| [FastF1](https://github.com/theOehrly/Fast-F1) (library) | Python client for timing and telemetry | MIT (the library code only) | No (it is a dependency) |
| Formula 1 timing and telemetry (via FastF1) | Laps, sector times, tyres, car telemetry (speed, throttle, brake, gear, RPM) | Owned by Formula 1 / Formula One Management | **No.** It is never committed; each user downloads it to their local cache |
| [Jolpica-F1](https://github.com/jolpica/jolpica-f1) API | Races, drivers, constructors, qualifying, results, pit stops, laps | **CC BY-NC-SA 4.0** | Only a small sample under `data/sample/` (see below) |
| [TracingInsights](https://github.com/TracingInsights) season repositories (`TracingInsights/2021` … `/2025`) | Race sessions only: laps with compound, tyre life, stint, pit in/out, position and track status; race-control messages; session track-status timeline; per-lap telemetry (speed, throttle, brake, gear, RPM) | Repository licence **MIT** (2021–2024) / **Apache-2.0** (2025); the underlying data is Formula 1 / FOM timing data (via FastF1) and is **not** covered by either | **No.** Downloaded into the lake only; tests use invented values in the same format |
| [OpenF1](https://openf1.org) API (`api.openf1.org`) | Cross-check of compounds only (Step 4C audit, 3 races) | Unofficial, not affiliated with F1; free without login for historical data | **No** |

## Jolpica-F1 (CC BY-NC-SA 4.0)
- **Non-commercial** use only. Give **attribution** to Jolpica-F1. Share derived datasets **under the same licence**.
- API rate limits: **4 requests/second** and **500 requests/hour**. Our client stays below them, with a configurable budget (`JOLPICA_MAX_PER_HOUR`, default **450**). It also caches responses and can resume interrupted runs.

## FastF1 / Formula 1 data
- Since **October 2026**, FastF1 needs an **F1 TV Access login** to fetch timing data. Without one, it returns empty sessions. Credentials are read only from environment variables (`F1_USERNAME`, `F1_PASSWORD`) and are never committed. Our loaders fail loudly on empty sessions.
- F1 data is used for private academic study only. Raw or cleaned F1 timing data is not redistributed.

## TracingInsights (Step 4C, replaces FastF1 as the source of tyres, race control and telemetry)
- **Repository licences:** `TracingInsights/2021`–`2024` carry the **MIT** licence ("Copyright (c) 2023/2024
  TracingInsights-Archive"); `TracingInsights/2025` carries **Apache-2.0**. Verified from each repository's
  LICENSE file at the pinned commit (`src/f1sa/ingest/tracinginsights_pins.json`, `docs/data_source_audit.md`).
- **The data itself is not covered by those licences.** It is Formula 1 / FOM timing data obtained through the F1
  live-timing feed with FastF1. We treat it exactly like FastF1 data: private academic study only, never committed,
  never redistributed (raw or cleaned), with attribution to TracingInsights and FastF1.
- What we download: the Race session only (`session_laptimes.json`, `rcm.json`, `session_info.json`, `drivers.json`
  and per-lap telemetry files), from `raw.githubusercontent.com` at a pinned commit, each file checked against its
  git blob id. Team-radio audio (`*.mp3`), practice, qualifying and sprint sessions are never downloaded.
- Telemetry is aggregated per lap at ingest and the raw files are discarded, except one season (2024) kept as
  Parquet in the lake for the data-lake and partitioning experiments (`--skip-raw-telemetry` turns it off).
- Test fixtures (`tests/fixtures/tracinginsights/`) copy the file *format* with invented values; they contain no F1 data.

## OpenF1
- Used once, in the Step 4C audit, to cross-check compounds for three races (no login). OpenF1 is unofficial and not
  affiliated with Formula 1. Its terms page (openf1.org) was not re-checked from this environment; check it before
  any use beyond academic cross-checking. No OpenF1 data is stored or committed.

## Sample data (`data/sample/`)
- Licence and attribution: `data/sample/LICENSE`. Contents and generator: `data/sample/README.md` and
  `docs/data_dictionary.md`.
- The sample combines a subset of **Jolpica-F1** data (CC BY-NC-SA 4.0, with attribution) with **SYNTHETIC** tyre and telemetry values, so the pipeline can run without F1 TV credentials.
- Every synthetic record is marked `is_synthetic=true`. Synthetic values are **not** real Formula 1 data and must not be presented as real.
- The sample contains **no** TracingInsights data; it stays Jolpica + synthetic and remains the demo default.
- No other data is committed to this repository.
