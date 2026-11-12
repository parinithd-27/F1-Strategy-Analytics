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
