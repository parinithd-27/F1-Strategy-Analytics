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
- Step 4A (W2): bulk, resumable raw-zone ingestion and the HDFS sink (#2, #6, bulk part of #1/#4/#5)
  - HttpFS gateway (`httpfs` service in the hdfs/full profiles, f1sa-hadoop:3.4.3, user hadoop, port 14000, 384m, health
    check). Proxyuser settings are in core-site.xml, and `docker/hadoop/httpfs-site.xml` is new.
  - `HdfsSink`: uses the same interface as `FileSink`. CREATE `data=true` (no datanode redirect) to `_tmp.<uuid>`, length
    check, then DELETE + RENAME, with the boolean checked. Verified live: HttpFS RENAME never overwrites and
    `renameoptions` is ignored. Retries 5xx and connection errors; temp files are cleaned on failure or Ctrl+C.
  - Table-first raw layout `raw/{source}/{table}/season=S[/round=R][/driver=DRV]/` with a `_manifest.json` per
    partition. Season tables (schedule, drivers, constructors) are fetched once per season. `sprint_results` only on
    sprint weekends.
  - `data/manifest.json` (`ingest/manifest.py`): atomic save after every change (tested with a real SIGKILL), flock
    against concurrent runs, stale `running` items retried, `--only-failed`.
  - `ingest/planner.py`: plan, request and ETA estimate (cache-aware, no network), budget split when both sources run.
    `--dry-run` writes nothing.
  - `ingest/bulk.py` and `cli.py season-range` (plus `race` on the same code path):
    - FastF1 sign-in is checked first; auth or hard rate-limit errors abort with exit 3.
    - Other errors are recorded in the manifest and the run continues (exit 1).
    - SIGINT/SIGTERM give exit 130 with the item `interrupted`.
  - FastF1: per-driver telemetry and position partitions, one race in memory at a time, races paced (20/h), and the
    cache dir enforced inside `./data`.
  - **Rate limiter fix:** the real run got 63 HTTP 429s. Root cause: a token bucket admits capacity + rate·T, so the Step 3
    buckets allowed 8 requests per second and about 900 in the first hour. The limiter is now a 1-token bucket at
    3/s (≤ 4 in any second) plus a persisted sliding hourly window (≤ budget in any rolling hour, across runs).
    A 429 without Retry-After now backs off 5–60 s.
  - Tests: 218 unit tests (manifest, kill-safety, HdfsSink against a fake HttpFS with the real quirks, planner and
    estimates, bulk runs incl. interrupt/abort/resume, window guarantees) and HttpFS integration tests from the host.
- Step 4B (W2): synthetic generator and offline sample bundle
  - `ingest/synthetic.py` (generator `synthetic-v1`):
    - Stints are inferred from real Jolpica laps and pit stops (DNF, stop on the last lap, duplicates, DNS handled).
    - Synthetic dry compounds (≥ 2 per driver who stopped) and starting tyre age.
    - Synthetic per-lap telemetry aggregates (mean/max speed, full throttle, brake, gear changes, RPM) scaled by real
      lap pace.
    - Seeded per driver with a SeedSequence, byte-deterministic; `is_synthetic=true` on every row; no FastF1.
  - `ingest/sample_builder.py` + `make sample-data` (`REBUILD=1`):
    - Builds `data/sample/` for 2023 R5 and 2024 R5 in the raw-zone layout. Pages come from the raw zone, then the
      cache, then the network. The bundle is built in a temp dir, validated, then swapped in atomically.
    - When the bundle exists it is validated offline: sha256 and size, schemas, `is_synthetic`, no FastF1 data, no
      unlisted files, < 5 MB, and byte-identical regeneration of the synthetic tables.
    - It generates `data/sample/LICENSE` (CC BY-NC-SA 4.0, attribution and link to Jolpica-F1) and `README.md`
      (real vs synthetic, how built, smoke-test-only warning).
  - Verified: first build gave 18 data files and 502 KiB (2023 R5 and the schedules from cache; 18 network requests for
    2024 R5 and the season tables). The rebuild and offline validation used 0 network.
  - Rate limiter is now cross-process: before every admission the shared `requests.log` is re-read under `flock`, the
    hourly and per-second (≤ 4 in any second, 50 ms margin) checks count every process, and SQLite gets
    `busy_timeout`. The new two-process test found and fixed a window-boundary bug (6 in one closed second).
  - `docs/data_dictionary.md` (new): bundle tables, every synthetic assumption, and the NOT-real / smoke-test-only warning.
  - Tests: 251 unit tests (generator edge cases and determinism, builder order, offline validation, tamper/FastF1/size
    rejection, atomic swap, gitignore, two-process limiter).
## Next
- User: full pass `f1sa-ingest season-range --seasons 2021-2025 --sources jolpica,fastf1` (dry-run first). Jolpica
  is about 1,750 requests (about 4 h at 450/h). FastF1 needs the F1 TV login and about 115 races at 20/h (about 6 h).
- Step 5: clean Parquet zone (`etl/clean.py`, `lake/schemas.py`) reading the table-first raw zone. Sample mode should
  copy `data/sample/raw` to a scratch lake (`data/lake-sample`) so derived output never lands in tracked `data/sample/`.
## Known issues
- `make test-int` still treats pytest exit 5 (no tests collected) as success. That can only happen now if the
  integration tests are deselected.
- The integration test writes one `infra_test` message (header `test=infra`) to `f1.race.events` on each run. S12
  consumers must use their own group and offsets, or filter out that header.
- The first `make up` builds the app image, which needs network access (PyPI and Maven Central). Later runs are offline.
- If the host UID is not 1000, set `APP_UID` in `.env` before building, so that files written to data/ and results/
  stay owned by the host user.
- Jolpica still throttles occasionally, even with the corrected limiter (strictly ≤ 4 in any second, ≤ 450 in any hour):
  22 HTTP 429s in 184 requests on the resumed 2021 run, against 63 in about 170 before the fix. All recovered after
  one 5–6 s backoff, and no item failed. Jolpica probably enforces an undocumented short window (its docs say limits
  "will decrease"). If 429s rise on the full run, lower the burst to 2/s or lower `JOLPICA_MAX_PER_HOUR`.
- The hourly request log only knows requests made since it was introduced (it was added in the middle of the
  2021 run).
- FastF1 request counts and load time per race in the estimate are approximations (12 calls, about 60 s). The
  credentialed FastF1 path has still not been run (it needs the user's login).
- Synthetic data is plausible in distribution, not per race (e.g. 2024 R5 VER has a 33-lap SOFT stint, a 5% draw).
  Regeneration is byte-identical with the locked NumPy/pyarrow versions; a different version may change the bytes,
  in which case `make sample-data REBUILD=1` rebuilds the bundle.
- A bulk-ingest process started with the Step 4A code reads the shared request log only at start-up, so it does not
  see requests made later by other processes. Processes started now share the budget fully.
- mypy treats `fastf1` and `pandas` as untyped (`ignore_missing_imports`). `pandas-stubs` could be added later.
- DATA_LICENSES.md statements (Jolpica limits, FastF1 F1 TV login requirement since Oct 2026) were taken
  as provided by the team and not re-verified online.
- CI has not yet run on GitHub (validated locally against the GitHub workflow JSON schema only).

## Verify
`uv sync --all-extras && make lint && make typecheck && make test && make up-full && make test-int && make down`

HDFS sink: `DATA_LAKE_URI=hdfs://namenode:8020/f1 uv run f1sa-ingest season-range --seasons 2021 --sources jolpica --sink hdfs`,
then `docker compose exec namenode hdfs dfs -du -h /f1/raw`.
