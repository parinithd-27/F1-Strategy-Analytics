# Data licences and usage terms

The **code** in this repository is MIT-licensed (see [LICENSE](LICENSE)). The **data** it works with is not covered by that licence. Each source has its own terms, listed below.

| Source | What we use | Licence / owner | Redistributed in this repo? |
|---|---|---|---|
| [FastF1](https://github.com/theOehrly/Fast-F1) (library) | Python client for timing and telemetry | MIT (the library code only) | No (it is a dependency) |
| Formula 1 timing and telemetry (via FastF1) | Laps, sector times, tyres, car telemetry (speed, throttle, brake, gear, RPM) | Owned by Formula 1 / Formula One Management | **No.** It is never committed; each user downloads it to their local cache |
| [Jolpica-F1](https://github.com/jolpica/jolpica-f1) API | Races, drivers, constructors, qualifying, results, pit stops, laps | **CC BY-NC-SA 4.0** | Only a small sample under `data/sample/` (see below) |

## Jolpica-F1 (CC BY-NC-SA 4.0)
- **Non-commercial** use only. Give **attribution** to Jolpica-F1. Share derived datasets **under the same licence**.
- API rate limits: **4 requests/second** and **500 requests/hour**. Our client stays below them, with a configurable budget (`JOLPICA_MAX_PER_HOUR`, default **450**). It also caches responses and can resume interrupted runs.

## FastF1 / Formula 1 data
- Since **October 2026**, FastF1 needs an **F1 TV Access login** to fetch timing data. Without one, it returns empty sessions. Credentials are read only from environment variables (`F1_USERNAME`, `F1_PASSWORD`) and are never committed. Our loaders fail loudly on empty sessions.
- F1 data is used for private academic study only. Raw or cleaned F1 timing data is not redistributed.

## Sample data (`data/sample/`)
- The sample combines a subset of **Jolpica-F1** data (CC BY-NC-SA 4.0, with attribution) with **SYNTHETIC** tyre and telemetry values, so the pipeline can run without F1 TV credentials.
- Every synthetic record is marked `is_synthetic=true`. Synthetic values are **not** real Formula 1 data and must not be presented as real.
- No other data is committed to this repository.
