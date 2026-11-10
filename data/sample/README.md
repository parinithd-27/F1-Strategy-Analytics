# Sample bundle (offline sample mode)

> **Warning:** the `raw/synthetic/` tables are **NOT real Formula 1 compound or telemetry
> data**. They are generated from fixed, documented assumptions so the pipeline can run
> offline. Any model result computed in sample mode is a **smoke test only and must never be
> reported as a finding**.

## What is in it
- Races: 2023 round 5, 2024 round 5.
- **Real** data (Jolpica-F1, CC BY-NC-SA 4.0): `raw/jolpica/` tables constructors, drivers, laps, pitstops, qualifying, results, schedule.
  These are the JSON pages exactly as the API returned them. Season-level tables (schedule,
  drivers, constructors) cover the whole season.
- **Synthetic** data (`is_synthetic=true` on every row): `raw/synthetic/tyre_stints` (a compound
  and starting tyre age per stint; stints are inferred from the real pit stops and laps) and
  `raw/synthetic/lap_telemetry` (per-lap speed, throttle, brake, gear and RPM aggregates).
  The generator is `f1sa.ingest.synthetic` (synthetic-v1) with seed 4242.
  The assumptions are listed in `docs/data_dictionary.md`.
- **No FastF1 data.**
- 18 data files, 495 KiB. `MANIFEST.json` lists every file with its
  size, sha256 and origin.

## How it was built
`make sample-data` (`python -m f1sa.ingest.sample_builder`). Each Jolpica table is read
from the local raw zone, then from the local HTTP cache, and only if both miss, from the
rate-limited API. The synthetic tables are then generated, and the whole bundle is
validated before it replaces the previous one.

When the bundle exists, `make sample-data` validates it offline: hashes, schemas,
`is_synthetic`, no FastF1 data, the size limit, and that the synthetic tables regenerate
identically from the seed. `make sample-data REBUILD=1` rebuilds it.

## Licence
See `LICENSE`: Jolpica-F1 data under CC BY-NC-SA 4.0 (non-commercial, attribution,
share-alike).
