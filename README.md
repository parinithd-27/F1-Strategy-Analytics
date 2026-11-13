# F1 Strategy Analytics at Scale

> **Work in progress:** IISc MTech big-data project by team **Data Wranglers**. Interfaces, data and results will change. See [docs/PROGRESS.md](docs/PROGRESS.md).

This project builds a partitioned, multi-season (2021–2025) Formula 1 data lake from Jolpica-F1 and FastF1. Spark (RDD, DataFrame and SQL) turns that history into features such as gap change, tyre age, out-lap pace and pit loss. Those features train a calibrated MLlib model of undercut success. Races are then replayed through Kafka and Spark Structured Streaming to flag undercut opportunities lap by lap. Live race state goes to Redis and on to a FastAPI + Next.js dashboard, where a what-if engine compares *pit now*, *pit next lap* and *stay out* from the same race state.

- Architecture: [docs/architecture.md](docs/architecture.md)
- Proposal → implementation map: [docs/traceability.md](docs/traceability.md)
- Code licence: [LICENSE](LICENSE) (MIT). Data licences and usage limits: [DATA_LICENSES.md](DATA_LICENSES.md)

## Quick start (developer)

```bash
make setup      # uv sync --all-extras
make lint typecheck test
```

## API (Step 13)

The FastAPI backend serves live race state from Redis, Server-Sent Events from the race channel,
replays and the replayable races. Docs: `http://127.0.0.1:<port>/api/docs`.

| Command | What runs | Port |
|---|---|---|
| `make up` | infra plus the `api` container in **sample mode** (data/lake, replay and debug off) | 127.0.0.1:8000 |
| `make api-up` | recreates **only** the `api` container (`--no-deps --no-build`: no other service restarts, no volume is touched) | 127.0.0.1:8000 |
| `make api-up FULL=1 REPLAY=1 DEBUG=0` | the same, against the **full HDFS lake** (`hdfs://namenode:8020/f1` via HttpFS; needs `make up-full`) with replay on | 127.0.0.1:8000 |
| `make api` | the API on the host with uvicorn (file lake; needs `make up` for Redis and Kafka) | 127.0.0.1:8001 (`API_PORT=…`) |

`api-up` flags are `FULL`, `REPLAY`, `DEBUG` and `TEST_SEASON` (lists and replays 2025). Only `1`
enables a flag, and leaving one out means off. `make up` puts the container back in sample mode.

A replay (`POST /api/races/{season}/{round}/replay {"speed": 1|10|60}`) runs the whole pipeline:
the stream job, then the producer once the job is ready. It shows up in `/state` and on
`/events`. Progress and errors are in `GET /api/replays`. A stream job that is not ready within
60 s is killed, and the replay ends as `failed`.

```bash
make api-up FULL=1 REPLAY=1
curl -s localhost:8000/api/races | python3 -m json.tool | head
curl -s -X POST localhost:8000/api/races/2023/10/replay -H 'content-type: application/json' -d '{"speed":60}'
curl -N -s localhost:8000/api/races/2023/10/events
```

**Outputs.** Demo runs (`make stream-demo` and API replays) write to `data/demo/<run_id>/`
(gitignored). Only `make stream-demo RECORD=1`, the recorded run of Step 16, writes to
`results/`, so the committed results are never overwritten by a demo.
`make demo-clean` lists those folders and their checkpoints; `make demo-clean CONFIRM=yes` deletes
them (and nothing outside `data/demo` and `data/checkpoints`). Do not run it while a replay is running.

## What-if simulator (Step 15)

`POST /api/races/{season}/{round}/whatif {"attacker": "<driverId>", "defender": "<car ahead>"}`
compares pit now / pit next lap / stay out (an overcut) at the attacker's latest lap. It returns P(attacker
ahead once both cars have stopped), the expected gap and an 80% interval for each, next to the stream's
calibrated model probability. The race page has a what-if panel. The tyre model is built offline:

| Command | What it does |
|---|---|
| `make sim-tyre-model-sample` | tyre model from the sample lake -> `models/sim/tyre_model_sample.json` |
| `make sim-tyre-model-full [TEST_SEASON=1]` | from the HDFS lake via HttpFS -> `models/sim/tyre_model_full.json` (2025 entries only with `TEST_SEASON=1`) |
| `make whatif-consistency-sample` | consistency smoke test on the sample (`data/whatif/` only) |
| `make whatif-consistency-full [RECORD=1]` | checks and backtest 2021-2024 in the app container; `RECORD=1` adds the 2025 held-out row and writes `results/whatif_consistency.*` |

The method, constants and checks are pre-registered in `docs/sim_spec.md`.

## Dashboard (Step 14)

A Next.js dashboard on the API: race picker, timing tower, gap-to-leader chart, tyres, undercut
alerts, replay control. Open **http://localhost:3000** (or `http://127.0.0.1:3000`; both are CORS
origins of the API, `API_CORS_ORIGINS`). Live updates are pushed over SSE, including a full-state
`snapshot` event after connect and once per leader lap; the dashboard never polls the race state.

| Command | What runs |
|---|---|
| `make up` | infra, the `api` container (sample mode) and the `web` container (127.0.0.1:3000, non-root, read-only) |
| `make api-up REPLAY=1` | lets the dashboard start replays (`FULL=1` for the 90 races of the HDFS lake) |
| `make web` | Next.js dev server on 127.0.0.1:3000; stop the `web` container first (`docker compose stop web`) |
| `make web-test` | lint, typecheck and the Vitest suites |
| `make web-e2e` | Playwright smoke test against the API (needs `make api-up REPLAY=1`; downloads Chromium once into `~/.cache/ms-playwright`) |

Pinned: Node 26.13.0 (LTS), Next.js 16.4.5, React 19.3.1, TypeScript 6.0.3. The API URL is
`NEXT_PUBLIC_API_URL` (default `http://localhost:8000`, inlined at build time). With
`NEXT_PUBLIC_DEBUG_LATENCY=1` and `make api-up DEBUG=1`, the dashboard posts its render time per
event to `results/ui_latency.csv`. In sample mode a **SYNTHETIC DEMO DATA** banner is always shown.

