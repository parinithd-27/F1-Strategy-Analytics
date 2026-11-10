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
- Step 2 (W1): Docker Compose infrastructure
  - `docker-compose.yml` with profiles core | hdfs | spark | full. Every long-running service has a healthcheck, a
    `mem_limit`, and `depends_on: service_healthy` / `service_completed_successfully`.
  - core: kafka 4.3.2 (KRaft, host 9092 / internal kafka:29092, auto-create off), kafka-init (f1.race.events, 3 partitions),
    redis 8.10.3 (no persistence, 256mb, volatile-ttl), app image (`docker/spark/Dockerfile`: temurin 17 + py3.12 +
    uv-locked deps + spark-sql-kafka 4.2.1 jars baked in, verified offline at build time; non-root UID = host UID)
  - hdfs: namenode + datanode (`docker/hadoop/`, from apache/hadoop:3.4.3, replication 1, UI 9870, named volumes)
  - spark: standalone master + 2 workers (memory and cores set from .env)
  - Makefile: up, up-full, down (never -v), ps, logs, nuke (needs CONFIRM=yes). `scripts/wait_for.sh`. `.dockerignore`
  - `confluent-kafka` runtime dependency. `Settings.kafka_topic_events` (validated). `.env.example` has ports, worker sizing, APP_UID
  - Tests: `tests/integration/test_infra.py` (compose config, topic/partitions, Kafka round-trip, Redis PING, HDFS
    put/cat, skipped without hdfs) and `tests/unit/test_wait_for.py`
  - docs/architecture.md: infrastructure table and memory budget
- Step 3 (W1): ingestion clients (#4, #5)
  - Checked the docs first. Jolpica caps `limit` at 100 (the brief said 1000), and lap paging counts timings.
    Rate limits are 4/s and 500/h, with 429 and no Retry-After header. Terms are CC BY-NC-SA 4.0.
    FastF1 3.9.2 auth uses F1_USERNAME/F1_PASSWORD or the TTY prompt or `python -m fastf1 auth f1tv`.
    `Cache.configure` needs an existing directory.
  - `ingest/ratelimit.py`: thread-safe dual token bucket with an injectable clock and sleep.
  - `ingest/jolpica_client.py`: cached (SQLite), rate-limited in the adapter (cache hits are free), retries with
    backoff and Retry-After, pages by the server's limit, merges split laps. Typed DataFrames for seasons, races, results,
    qualifying, drivers, constructors, pitstops and laps.
  - `ingest/fastf1_loader.py`: cache configuration, an explicit auth check (secrets never logged), validated laps,
    car and position data. Raises `FastF1AuthError` / `FastF1DataError` instead of continuing silently.
  - `ingest/sinks.py` (atomic FileSink plus manifest), `lake/paths.py` (raw partition URIs), and `ingest/cli.py`
    (`f1sa-ingest race ...`, where `--sink hdfs` exits 2 until Step 4).
  - Settings: `JOLPICA_CACHE_DIR` and `JOLPICA_USER_AGENT` (repo name only, no personal details).
  - `.claude/settings.json`: deny `Read(~/.local/share/fastf1/**)`, where FastF1 3.9.2 stores the F1 TV token.
  - Tests: trimmed CC BY-NC-SA fixtures in `tests/fixtures/jolpica` (README with attribution). 140 unit tests, with
    no network or credentials needed.
  - Verified: a real ingest of 2023 R5 into the file sink took 20 s cold (18 requests) and 0.7 s warm (0 requests).
    FastF1 without credentials gives exit 1 with a clear FastF1AuthError, and nothing is written.
## Next
- Step 4: multi-season ingest (`--seasons 2021-2025`, `data/manifest.json`, resumable) and the HDFS sink through an
  **HttpFS gateway**, chosen in Step 3:
  - New `httpfs` service in the hdfs profile (f1sa-hadoop:3.4.3, `hdfs httpfs`, port 14000, 384m).
  - `HdfsSink` with requests: CREATE with `data=true` (no datanode redirect), write to a temp name and then RENAME.
## Known issues
- `make test-int` still treats pytest exit 5 (no tests collected) as success. That can only happen now if the
  integration tests are deselected.
- The integration test writes one `infra_test` message (header `test=infra`) to `f1.race.events` on each run. S12
  consumers must use their own group and offsets, or filter out that header.
- The first `make up` builds the app image, which needs network access (PyPI and Maven Central). Later runs are offline.
- If the host UID is not 1000, set `APP_UID` in `.env` before building, so that files written to data/ and results/
  stay owned by the host user.
- FastF1 makes its own Jolpica calls (its internal limit is 200/h) on the same IP as our client (450/h), and
  Jolpica's limit is 500/h per IP. Lower JOLPICA_MAX_PER_HOUR for long runs that use both sources (Step 4).
- The hourly token bucket is per process. Separate runs within one hour don't share a budget, but the cache
  makes re-runs free.
- The credentialed FastF1 path (a real download) has not been run yet. The user will run it with their own login.
- mypy treats `fastf1` and `pandas` as untyped (`ignore_missing_imports`). `pandas-stubs` could be added later.
- DATA_LICENSES.md statements (Jolpica limits, FastF1 F1 TV login requirement since Oct 2026) were taken
  as provided by the team and not re-verified online.
- CI has not yet run on GitHub (validated locally against the GitHub workflow JSON schema only).

## Verify
`uv sync --all-extras && make lint && make typecheck && make test && make up-full && make test-int && make down`
