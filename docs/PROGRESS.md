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
- Step 5 (W2): clean zone (#3, the clean half of #1, #7)
  - `lake/schemas.py`:
    - explicit raw schemas (Jolpica pages, FastF1 laps and car data, synthetic tables);
    - 10 clean StructTypes: races, drivers, constructors, results, qualifying, laps, pitstops, stints, telemetry_lap,
      telemetry_raw, each with primary keys.
  - `etl/clean.py` (+ `etl/spark.py`, `f1sa-clean`, `make etl-sample`):
    - Spark reads raw (file:// or hdfs://) with explicit schemas. Jolpica `driverId` is canonical; FastF1
      abbreviations are mapped via the race entries, with car number as fallback.
    - Times are converted to seconds. Laps carry cumulative time, gap to leader and gap ahead at the timing line, and
      pit in/out flags.
    - SC/VSC/red-flag flags: exact from FastF1 TrackStatus, otherwise a documented Jolpica heuristic (neutralised
      > 1.25× median, red flag > 3×; tightened in Step 8).
    - Stints and telemetry prefer FastF1, then synthetic, then stints inferred from pit stops.
    - Writes `partitionBy(season, round)` with dynamic overwrite. File mode refuses to write outside ./data/lake.
  - `lake/quality.py`: hard checks (null/duplicate keys, races without results/laps, pit lap out of range) and soft
    checks (lap contiguity, positions 1..N, pit lap in laps, results.laps, unparseable times, unmapped FastF1 drivers,
    stint tiling). Writes the JSON report to `clean/_quality/report.json`.
  - Verified on data/sample: exact counts (laps 2168, pitstops 60, stints 100, …), quality PASS (28 checks, 0 hard,
    0 soft), and identical checksums on rerun. 2024 R5 neutralised laps are 22–24, 26 and 28–31 (21–31 since the Step 8 heuristic).
  - HDFS mode (your choice): `docker compose exec app env PYTHONPATH=/workspace/src DATA_LAKE_URI=hdfs://… python -m
    f1sa.etl.clean --source lake`. Run on the full data in Step 8b.
  - Tests:
    - the whole pipeline on the real sample, with exact counts, schemas, gaps, flags, rerun and subset idempotency;
    - a FastF1 path with hand-made Parquet: nanosecond timedeltas, mapping and unmapped driver, exact TrackStatus,
      stints, per-lap aggregates, lap assignment of samples;
    - quality checks and helpers.
- Step 6 (W3): Spark RDD / DataFrame / SQL feature jobs (#8–#11, S6 part of #7 and #12)
  - `etl/rdd_jobs.py` (RDD API):
    - stints: `keyBy` → `groupByKey` → sort the laps → split after pit-in laps;
    - per-race/circuit green lap-time distribution: `aggregateByKey` with a Welford/Chan accumulator, a 1 s
      histogram and a broadcast circuit map;
    - each job has a DataFrame twin.
  - `etl/df_jobs.py` (DataFrame API): `enrich_laps` joins laps, results, pit stops, drivers, constructors, stints and
    races. Windows give gaps (lapped cars are compared with the leader at the same lap number), 1- and 3-lap gap
    changes (null when the car ahead changes), tyre age, laps remaining and estimated lap timestamps (start + 180 s +
    cumulative time).
  - `etl/sql/*.sql` + `etl/sql_jobs.py` (Spark SQL): pit loss and team out-lap history from strictly earlier races
    (`RANGE … 1 PRECEDING` over `race_seq`). Fallbacks: team (out-lap only) → circuit → global → null with source and
    missing flags. Plus a race summary.
  - `features/{schemas,gaps,stints,pitloss,build}.py`. Run `python -m f1sa.features.build --source sample|lake`
    (also `make features-sample` or `f1sa-features`). It writes 7 tables to `data/lake/features`, partitioned by
    season and round.
  - Verified on the sample: exit 0, both partitions for every table (laps_enriched 2168, gaps 2168, stints 100,
    distribution 2, pit_loss 2, team_outlap 20, summary 2), and identical checksums on rerun.
    - RDD stints = DataFrame stints = clean stints (100/100). Recomputed gaps = clean gaps (2168/2168).
    - Pit loss: 2023 R5 `none`, 2024 R5 `global_prior`.
  - Tests:
    - a 3-driver, 5-lap fixture with a lapped car and a pit lap;
    - a 4-race leakage fixture (the target race and later races scaled ×100, the target's history unchanged);
    - RDD vs DataFrame equality;
    - pit-cycle exclusions;
    - build, rerun and `_quality` junk.

    49 new tests, 344 unit tests in total. Docs: architecture.md (API rationale, timestamp limits, fallbacks) and
    data_dictionary.md.
- Step 7 (W4): telemetry aggregates, out-lap pace and constructor lineage (rest of #12)
  - `features/telemetry_agg.py`: the single per-lap aggregation definition (full throttle ≥ 98%). Samples are
    assigned to `[lap_start, lap_end)` by session time; a boundary sample goes to the later lap. The clean FastF1 path
    uses it too. Races with raw telemetry are aggregated; synthetic `telemetry_lap` passes through unchanged with
    `is_synthetic`. Batch only: a test checks that `f1sa.streaming` never imports telemetry.
  - Clean laps gain `lap_start_session_s` and `lap_end_session_s` (from FastF1, null otherwise). `make etl-sample`
    was re-run; quality still PASS.
  - `features/outlap.py`:
    - clean lap (excludes lap 1, in-laps, out-laps, SC/VSC/red flag, and laps over 107% of the race median);
    - `realised_outlap_delta_s` (ORACLE): same-stint median, falling back to the previous stint;
    - `expected_outlap_delta_s`: the mean over strictly earlier races, with fallbacks
      driver_team → team → circuit → global → none, plus a flag.
  - `features/lineage.py`: alphatauri→rb and alfa→sauber, plus stable ids. It is applied to SQL
    `team_outlap_history` (new `team_history_key`) and to `outlap_features`. The build fails on an unknown
    constructor id.
  - `docs/label_definition.md` (new): the "Out-lap pace features" section, with parameters and the ORACLE
    vs decision-time rule. architecture.md and data_dictionary.md are updated.
  - Verified on the sample: two builds, both exit 0, with identical checksums for all 9 tables × 2 partitions
    (telemetry_lap_features 2168, outlap_features 100). In 2024, rb and sauber now use `team_prior`; out-lap
    expected values are `driver_team` (58) or `team` (2).
  - Tests (28 new, 372 unit tests in total):
    - raw telemetry fixture: boundaries, a lap with no samples, exact values;
    - synthetic pass-through and mixed sources;
    - the clean-lap rule (including 108% vs 106%);
    - realised fallbacks;
    - 4-race ×100 leakage;
    - history exclusions;
    - lineage, including a check that the build fails on an unknown id.
- Step 8 (W4): undercut attempts, labels and the real-time detector (#13, rule half of #20)
  - Phase 1 review approved by the user. The definition, grid, edge cases and leakage rules are in
    `docs/label_definition.md` ("Undercut attempts").
  - `features/undercut_rule.py` (pure Python): `candidate_pairs(LapSnapshot, RuleParams)`. A is directly behind D,
    0 < gap ≤ G, L ≥ 4, not teammates, no car crossing between them, neither lapped or in the pit cycle, and L − 1
    green and not a restart lap. Failed conditions are returned as reasons.
  - `streaming/detector.py`: `detect()` returns eligible pairs with tyre-age and last-lap deltas.
    `snapshot_from_state()` validates the per-driver state (Redis-hash style, string values allowed). No Spark
    import (subprocess test).
  - `features/undercut.py`:
    - `evaluate_race` runs per race through RDD `keyBy` → `cogroup` → `flatMap` and emits all 9 (G, k) rows;
    - the label is A ahead on D's out-lap;
    - `defender_stayed_out`, plus 16 `excluded_<reason>` categories in a fixed priority order, all reasons in
      `exclusion_reasons`;
    - slow-stop and double-stack flags;
    - DataFrame decision features (stint pace and degradation slope, rejoin traffic, priors) in `DECISION_FEATURES`;
      `oracle_*` and `outcome_*` columns are kept apart;
    - `detector_eligible`.
  - Track status: the Jolpica heuristic is tightened to ratio > 1.10 or ≥ 50% of cars > 107%. 2024 R5 laps 21, 25
    and 27 are now caught (21–31 neutralised); 2023 R5 has none. FastF1 TrackStatus still overrides per lap.
  - Out-lap review: 74% of the stop loss lands on the out-lap, so `expected_outlap_delta_s` is collinear with
    `pit_loss` and is now marked legacy.
    - New: `stop_delta_s`, `outlap_pace_residual_s` (ORACLE) and the shrunk priors `expected_outlap_pace_residual_s`
      (driver_team → team → global, κ = 5, `driver_team` once n ≥ 5) and `expected_stop_delta_s` (team → global).
    - `outlap_features` now `localCheckpoint()`s its realised table: the plan had grown to 13 MB and caused a JVM OOM.
  - Build: `undercut_attempts` (sparse; stale partitions deleted) and `results/undercut_counts_sample.csv` (smoke-test
    header and column; `--results-dir` must be under `./results`).
  - Verified on the sample: `make etl-sample` gives quality PASS. Two `--source sample` builds both exit 0 with
    identical checksums for all 10 tables (`undercut_attempts` 303 rows, checksum 4098e93c10d0dcdf) and an
    identical CSV (sha256 387846fd…).
    - Default params, smoke test only: 2023 had 1 success, 3 fail and 8 defender_stayed_out; 2024 had 1 success.
  - Tests:
    - six hand-built scenarios (success, fail, stays out, SC in window, attacker retires, lapped car between);
    - ×100 leakage over 4 races;
    - G monotonicity and k window;
    - detector vs batch agreement on every scenario, lap and G;
    - 15 pure-Python edge cases;
    - rule and detector validation;
    - shrinkage at n = 0/1/4/5/6 and decomposition leakage;
    - the new heuristic boundary laps;
    - build CSV and results-dir guard.

    81 new tests, 453 unit tests in total (full suite: 453 passed in 225 s).
- Step 9 (W5): MLlib undercut model, baselines, calibration and explanations (#14–#18, #29, #39)
  - `model/features.py`: feature registry with provenance (jolpica, stint_boundary, tyre_state, telemetry,
    fastf1).
    - Every `DECISION_FEATURES` column is either a model input or excluded with a reason; derived inputs are
      documented.
    - Feature sets: `real` and `with_synthetic`. `with_fastf1` will be one new entry.
  - `model/dataset.py`:
    - one (G, k) setting, labelled rows only, a duplicate-key check;
    - derived stint length and telemetry at L−1;
    - constant fill plus `_missing` indicators, and one-hot `*_source` columns;
    - season split (`SplitSpec`) with a `SealedSplit` test season (token from `evaluate_test`, opened once);
    - class weights from the training rows only.
  - `model/baselines.py`: majority class, and a rule (expected fresh-tyre gain vs gap, decision-time only).
  - `model/train.py`:
    - LR pipeline (assembler → scaler → LR) and GBT pipeline;
    - grid tuned on validation PR-AUC (fit on train only, no CrossValidator);
    - explicit degenerate-validation path;
    - full-mode synthetic refusal (`SyntheticFeatureError`, exit 3);
    - model and metadata save under `./models` only.
  - `model/calibrate.py`: isotonic (Spark) and Platt (slope ≥ 0), chosen by validation Brier; Platt below 300 rows,
    identity when validation cannot support a fit.
  - `model/metrics.py`: numpy AP, ROC (equal to Spark's), Brier, ECE (10 bins), F1, macro-F1, with NaN and a reason
    when undefined.
  - `model/evaluate.py`: race-cluster and attempt bootstraps (1000, paired, seeded); `evaluate_test` is the only
    place the test season is opened.
  - `model/explain.py`: GBT importances (grouped per feature), standardised LR coefficients, permutation importance
    on validation (5 repeats, one stacked Spark job).
  - `model/rehearsal.py`: a GENERATED 2021–2025 table with a known logistic signal and race effects.
  - `model/pipeline.py`, `experiments/model_eval.py`, `make model-eval-sample` / `model-eval-rehearsal`:
    - `--source sample|rehearsal|full`;
    - full needs `--allow-test-season`;
    - stamped, suffixed outputs.
  - Verified:
    - `DATA_LAKE_URI=file://$PWD/data/lake uv run python experiments/model_eval.py --source sample` exits 0. Its
      data is train 2023 (4 rows), validation 2024 (1 row). Tuning is skipped (one class) and calibration is
      identity. The test season is skipped and the sealed split is never opened. All outputs are `_sample` and
      SMOKE TEST.
    - Rehearsal (generated data, small grid): the whole pipeline in about 20 s; the calibrated model beats the
      majority baseline on test PR-AUC.
  - Tests:
    - an `Estimator.fit` spy: no fit, tuning or calibration sees 2025, and no fit mixes train with validation;
    - poisoned 2025 rows leave the weights, parameters, coefficients, importances and calibrator unchanged;
    - the test split opens once;
    - the (G, k) filter and duplicates;
    - training-only class weights;
    - monotonic calibration, isotonic equal to Spark, the chooser rules;
    - the race-cluster bootstrap draws whole races, and its CI is wider under correlation;
    - full-mode refusal (static, flags, null flag);
    - CLI guards; sample smoke mode; explanations recover the generated signal.

    41 new tests, 494 unit tests in total (full suite: 494 passed in 295 s).
- Step 8b (W4): full 2021-2025 clean, features and undercut counts in HDFS mode (#1, #3, #6, #13)
  - Runs in the running app container (`PYTHONPATH=/workspace/src`, `local[2]`): no rebuild or restart.
    `/f1/clean` and `/f1/features` are owned by `f1sa`; `/f1/raw` stays `hadoop`-owned, so HDFS refuses writes to it.
  - Makefile: `hdfs-dirs`, `etl-full`, `features-full`, `report-full`, `pipeline-full`.
  - `features/build.py`: `--full-counts` writes `results/undercut_counts_full.csv` and refuses synthetic attempts;
    `telemetry_lap_features` is sparse; `lap_time_distribution` expects only races with a green lap (2021 R12 has
    none); a stale partition on `hdfs://` is an error instead of a delete.
  - `etl/full_report.py` (new): counts, constructors, provenance, `real` feature-set build (no fit, test sealed),
    neutralisation per race. Writes `results/full_data_quality.json`, `constructors_full.csv`,
    `neutralisation_by_race_full.csv`.
  - Verified: 114 races (22, 22, 22, 24, 24), 124,834 laps, quality PASS (0 hard, 1 soft: 5 disqualified drivers
    with `results.laps = 0`). Lineage map verified: no unknown constructor. Nothing synthetic; compound null;
    telemetry 0 rows; `real` builds.
  - Labelled at G=3.5, k=2: train 108, validation 50, test 49. Below 50: 2021, 2022, 2023, 2025.
  - Two complete runs: identical checksums for all tables and byte-identical result files. `/f1/raw` listing and
    `data/manifest.json` unchanged. Sample counts CSV unchanged.
  - Docs: "Full-data quality notes" in `docs/data_dictionary.md`. 61 new tests, 555 unit tests (555 passed in 298 s).
## Next
- User: decide the red-flag fix (see Known issues) **before** running `--source full`, which opens 2025 once.
- User: the FastF1 pass (needs the F1 TV login) adds exact track status, real stints and telemetry. The clean job
  already handles them (tested on hand-made FastF1 Parquet).
- Re-check the 74% out-lap stop-time share per circuit, and the heuristic thresholds (1.10 / 50%) on wet races,
  using 2021-2024 only.
- User: sign off the label definition. Then run
  `uv run python experiments/model_eval.py --source full --allow-test-season` (opens 2025 once).
- Step 10: scalability and partitioning experiments.
## Known issues
- **Red flags are not detected.** `is_red_flag_lap` is false on all 114 races: Jolpica marks the whole field as
  pitting on those laps and `heuristic_track_flags` drops pit laps first. 17 laps in 14 races (2021-2024) are
  missed. Of 207 labelled attempts at G=3.5, k=2, 20 are in those races and 1 (2024) is in a restart period that
  should be excluded. Not changed: it alters label eligibility, so it needs the user's decision.
- Sample size is small: 108 labelled training attempts at G=3.5, k=2.
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
- Without FastF1, `is_sc_lap` and `is_vsc_lap` are null and `is_neutralised_lap` comes from a lap-time heuristic.
  The heuristic misses lap-1 incidents and can flag laps run in heavy rain. It is exact once FastF1 data exists.
- Per-lap telemetry aggregates are averages over samples, not time-weighted (sample spacing is roughly even).
- The FastF1 path is tested only on hand-made Parquet that mimics what the Step 4A writer produces, because real
  FastF1 data needs the user's login.
- Sprint results are not yet a clean table (the raw zone has `sprint_results` on sprint weekends).
- mypy treats `fastf1` and `pandas` as untyped (`ignore_missing_imports`). `pandas-stubs` could be added later.
- DATA_LICENSES.md statements (Jolpica limits, FastF1 F1 TV login requirement since Oct 2026) were taken
  as provided by the team and not re-verified online.
- CI has not yet run on GitHub (validated locally against the GitHub workflow JSON schema only).

- The raw telemetry path is tested only on hand-built fixtures (real FastF1 data needs the F1 TV login).
- Step 6 `team_outlap_history` (SQL, median, green-lap reference) and Step 7 `outlap_features` (mean, clean-lap
  stint reference) are two different out-lap histories by design. S8 should use `outlap_features`.
- Lap timestamps are estimates (a fixed 180 s formation offset, error of about ±1 min). Lap-time distribution
  medians can still include isolated slow laps (2024 R5 max is now 115 s, down from 145 s after the Step 8 heuristic).
- The global prior window has no partition (single-partition sort). That is fine at about 100 races; check it in S10.
- Step 8: the sample undercut counts are tiny and use synthetic compounds. They are smoke-test output, not findings.
- Jolpica has no penalty data. A served 5/10 s penalty looks like a slow stop (flagged, kept), and a drive-through is
  only suspected (a stop more than 2 s below the median, or an incident lap). FastF1 race-control messages are not
  ingested yet.
- Jolpica does record pit entries under a red flag (Step 8b), so inferred stints split there and tyre ages after a
  red flag are unreliable. Those windows are not yet excluded (see the red-flag issue above).
- `laps_remaining` uses the winner's lap count as the scheduled distance, which is wrong in shortened races.
- The track-status heuristic still uses the whole race's median. That is fine because it is used only for
  eligibility and exclusion, never as a feature, but it will flag laps in mixed wet/dry races.
- The out-lap residual assumes stationary-time deviations land 1:1 on the out-lap (the line comes before the boxes).
  This is not yet verified per circuit.
- Step 9: the sample run is plumbing only (4 training rows, 1 validation row). Every sample metric is a smoke test,
  and tuning and calibration fall back to defaults. Rehearsal numbers come from GENERATED data.
- The PySpark Python worker daemon prints harmless `BrokenPipeError` tracebacks to stderr when a job stops early
  (e.g. `isEmpty`/`limit` on Python-RDD-backed frames). Results are unaffected.
- `real` excludes compounds and tyre age, so the model cannot use compound effects until FastF1 stints replace the
  synthetic compounds (`with_synthetic` is then allowed in full mode).

## Verify
`uv sync --all-extras && make lint && make typecheck && make test && make up-full && make test-int && make down`

HDFS sink: `DATA_LAKE_URI=hdfs://namenode:8020/f1 uv run f1sa-ingest season-range --seasons 2021 --sources jolpica --sink hdfs`,
then `docker compose exec namenode hdfs dfs -du -h /f1/raw`.

Features: `make etl-sample && uv run python -m f1sa.features.build --source sample`, then
`uv run pytest tests/unit/test_rdd_jobs.py tests/unit/test_df_jobs.py tests/unit/test_sql_jobs.py tests/unit/test_features_build.py -q`.

Step 7: `uv run pytest tests/unit/test_telemetry_agg.py tests/unit/test_outlap.py tests/unit/test_lineage.py -q`.

Step 8: `make etl-sample && uv run python -m f1sa.features.build --source sample` (twice: same checksums, same
`sha256sum results/undercut_counts_sample.csv`), then
`uv run pytest tests/unit/test_undercut.py tests/unit/test_detector.py tests/unit/test_outlap.py tests/unit/test_etl_helpers.py -q`.

Step 8b (needs `make up-full`): `make pipeline-full` (twice: same checksums, same `sha256sum results/*_full.*`), then
`uv run pytest tests/unit/test_full_report.py tests/unit/test_makefile_full.py tests/unit/test_features_build.py tests/unit/test_lineage.py -q`.

Step 9: `make etl-sample && make features-sample && make model-eval-sample`, then
`uv run pytest tests/unit/test_model_dataset.py tests/unit/test_model_pipeline.py tests/unit/test_calibrate.py tests/unit/test_metrics.py tests/unit/test_explain.py -q`.
