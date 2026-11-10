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
