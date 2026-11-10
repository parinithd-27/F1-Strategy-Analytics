# Progress
## Done
- Step 0: repo bootstrapped, CLAUDE.md, traceability matrix and Claude Code permissions added
- Step 1 (W1): monorepo scaffold and tooling
  - `pyproject.toml` (uv, Python 3.12, hatchling, src layout; pyspark==4.2.1, fastf1==3.9.2) + committed `uv.lock`
  - `src/f1sa/` skeleton (ingest, lake, etl, features, model, streaming, serving, sim, api) + `py.typed`
  - `src/f1sa/config.py`: validated `Settings` (lake URI file/hdfs, kafka host:port list, redis URL, TTL>0,
    Jolpica budget 1..500 default 450, FastF1 cache dir, paired SecretStr F1 credentials) + cached `get_settings()`
  - `.env.example`, `.editorconfig`, `.pre-commit-config.yaml` (ruff), Makefile (help/setup/lint/typecheck/test/test-int)
  - `tests/conftest.py` (session Spark local[2], 4 shuffle partitions; autouse settings isolation),
    `tests/unit/test_config.py`, `tests/unit/test_spark_fixture.py`
  - LICENSE (MIT), DATA_LICENSES.md, README.md, docs/architecture.md (mermaid), `.github/workflows/ci.yml`
  - `.gitignore` / `.gitattributes` extended; `.gitkeep` in empty dirs (experiments, scripts, docker, results,
    data/sample, docs/decisions, tests/integration, tests/e2e)
## Next
- Step 2: docker-compose infra (Kafka KRaft, Redis, optional HDFS/Spark profiles) and Makefile infra targets
## Known issues
- `make test-int` treats pytest exit 5 (no tests collected) as success until integration tests exist (S2+).
- DATA_LICENSES.md statements (Jolpica limits, FastF1 F1 TV login requirement since Oct 2026) were taken
  as provided by the team and not re-verified online.
- CI has not yet run on GitHub (validated locally against the GitHub workflow JSON schema only).

## Verify
`uv sync --all-extras && make lint && make typecheck && make test && make test-int`
