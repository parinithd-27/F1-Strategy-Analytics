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
  - Red-flag fix (user-approved): `heuristic_track_flags` also tests the field median of every car (pit laps and
    lap 1 included) against 3.0x, because Jolpica records a red flag as a stop for the whole field. 0 -> 18 red-flag
    laps in 15 races. `undercut.RED_FLAG_RESTART_LAPS = 3` green laps after a run containing a red flag count as
    neutralised (the longest decision lookback is 3 laps). One attempt moved from labelled to excluded (2024 R4);
    no 2025 attempt changed.
  - Labelled at G=3.5, k=2: train 108, validation 49, test 49 (every season below 50). At G=5.0, k=3: 184, 64, 60.
  - `excluded_early_lap` (76, 21, 50, 3, 39) matches an independent recount: mass stops on laps 2-3 (red flags,
    early wet stops); 2024's early stops are on lap 1, which is never a candidate. Data, not a bug.
  - Wet running stays `neutralised` (user decision). Reporting-only split (`features/neutral_split.py`): the counts
    CSVs gain `n_excluded_neutralised_sc_vsc_red_flag` and `_sustained_slow` (run >= 8 laps, no red flag, run
    median < 1.25x, lap < 1.35x; set from 2021-2024 only). G=3.5, k=2: 37 of 406 are sustained slow running.
    All 20 table checksums and `labels_checksum` (73fe89cf9f4e8ee6, new in `full_data_quality.json`) unchanged.
  - `clean.track_ratios` and `undercut.attempt_window` factored out unchanged. `docs/label_definition.md`
    gains "Known limitations" and the L=4 feature note: no labelled attempt at L=4 or 5 in any season.
  - Split tests (18): the rule (wet run, long SC, red-flag run, thresholds, restarts, windows), and labels checksum
    plus every other count identical with and without the split. 579 unit tests (579 passed in 444 s). The sample
    CSV gains the two columns (other columns unchanged; sha256 1c93ec30…).
  - Early-lap threshold L < 5 (user decision, from counts only, before any model result; final label change, the
    definition is now frozen): `undercut_rule.MIN_ATTEMPT_LAP = 5`, shared by the batch label and the detector.
    All 213 L = 4 candidates (9 settings; 25 at G=3.5, k=2) are now `excluded_early_lap`; no other attempt changed
    category; none was labelled. New `undercut.labelled_set_checksum` per (G, k) in `full_data_quality.json`:
    identical before and after at all 9 settings. Only `undercut_attempts` changed (f82c2f9c47398d67).
    Tests: L < 5 vs L < 4 moves only L = 4 rows at every (G, k); detector and batch agree at L = 4 and 5;
    the labelled-set checksum ignores unlabelled rows.
  - Two complete runs: identical checksums for all tables and byte-identical result files. `/f1/raw` listing and
    `data/manifest.json` unchanged. Sample counts CSV unchanged.
  - Docs: "Full-data quality notes" in `docs/data_dictionary.md`. 67 new tests, 561 unit tests (561 passed in 382 s). Red-flag
    tests: a lap where every car "pits" is a red flag, ordinary all-car and half-field pit laps are not, lap-1
    stoppage, restart period excludes an attempt, ends after 3 laps, counts from the end of the run.
- Step 9b (W5): development mode for the model (#14-#18, #29), label definition frozen (`label-definition-v1`)
  - `--source full --no-test` / `make model-dev-full` (in the app container, like `pipeline-full`; no model
    binaries saved). `dataset.read_feature_seasons` reads only the `season=2021..2024` partition directories, so
    2025 is never listed or opened; `SplitSpec(test=())`. `--no-test` with `--allow-test-season` exits 2; full mode
    allows only the `real` feature set. Final-run names and the `--allow-test-season` path are unchanged.
  - Outputs `results/model_dev_*` (DEVELOPMENT stamp; `_G5.0_k3` for the sensitivity run): validation metrics
    with race-cluster and attempt bootstraps vs majority and rule, calibration, importances, plots, class
    balance, leave-one-season-out (`model/loso.py`: 2021-2024 only, fixed first grid point, raw scores,
    fold-only class weights; class metrics undefined below 5 positives or negatives), metadata JSON.
  - Ran on the full features: both settings exit 0 in 3m14s. G=3.5, k=2 (primary): train 108 (32 positive),
    validation 49 (16 positive). G=5.0, k=3 (sensitivity, not used to choose): 184 (50) and 64 (22).
  - Tests: junk `season=2025` partitions plus a `DataFrameReader.parquet` spy (no 2025 path), the `Estimator.fit`
    spy, `evaluate_test` never called, stamps, distinct names, guards, LOSO folds, Makefile dry run.
  - Follow-up: the full suite failed intermittently (broadcast "Not enough memory" / `OutOfMemoryError`). Root
    cause: the shared test driver JVM used Spark's default 1 GB heap, and after GC it sat at 500-930 MB late in
    the suite (69 persisted RDDs plus the status store of ~11,000 stages), not a large broadcast (the fixture
    tables are KB). `tests/conftest.py`: explicit `spark.driver.memory=2g`, broadcast threshold pinned at the
    10 MB default, capped job/stage/task/SQL history, and `clearCache()` after every test module. After the fix
    the heap returns to 150-450 MB between modules (peak 1.27 of 2 GB). Test configuration only.
  - Development outputs gain `ece_5bin` (supplementary; 10 bins stays primary), an ECE bootstrap-bias note and
    the rule baseline's fallback count (0 of 49 and 0 of 64 rows: its Brier 0.221291 is a real value, close to
    the majority's 0.220822 by coincidence). Regenerated by a deterministic refit (same seed, grid, data).
- Step 9c: pre-registered model spec `docs/model_spec.md` (frozen once committed)
  - Full test-run outputs always carry the setting (`model_eval_G3.5_k2.csv`, `model_eval_G5.0_k3.csv`, same for
    calibration, feature importance, reliability, confusion matrix); a test run refuses to start if its files or
    saved metadata exist (first complete run is the reported one). `make model-test-full CONFIRM=yes` runs both.
    Dev names unchanged: all 18 `model_dev_*` files byte-identical after the rename.
  - F1 threshold: fixed 0.5 (`metrics.DECISION_THRESHOLD`, set in 64739ca before any full-data model, never
    tuned); new `decision_threshold` column and metadata key. 5-bin ECE also in the test report.
  - Dev outputs regenerated once (reads only `season=2021..2024`): 6,036 existing cells identical, only the new
    column and key added.
- Step 9c (deployment model for streaming, no test season)
  - New entry point (not a flag on `model_eval.py`, so `pipeline.run` and the test path are untouched):
    - `model/deploy.py`, `experiments/model_save.py`, `make model-save-full` (`FORCE=1` replaces).
    - Reuses `load_frame` (no-test mode), `DEV_SPLIT`, `tune`, `score_split` and `choose` unchanged; `evaluate_test`
      is not imported.
  - Output `models/undercut/deploy_real_G3.5_k2/` (gitignored, about 280 KB):
    - `{lr,gbt}/pipeline/`, `calibrator.json` and `metadata.json` (every field asked for, including git sha, dirty
      flag and `test_season_read: false`);
    - `lr/scorer.json` and `reproduction.json`.
  - The run writes to `.staging-*` and moves into place only if the reproduction check passes.
  - `model/lr_scorer.py`: pure-Python calibrated-LR scorer. It imports no Spark or numpy, and every number it uses
    comes from `scorer.json`.
  - Streaming will deploy the LR only; GBT stays Spark-only, for batch use.
  - `docker-compose.yml`: the app mounts `./models:/workspace/models` read-write (user choice). Only the app
    container was recreated (`docker compose up -d --no-deps app`, user-approved). The other 8 services kept
    running and no volume was touched.
  - Ran `make model-save-full` (2m02s):
    - partitions read: `season=2021..2024` only;
    - train 108 (32 positive), validation 49 (16);
    - LR regParam 0.1, elasticNetParam 0.5; GBT depth 2, 50 iterations, step 0.05; Platt for both;
    - reproduction MATCH: the round trip is exact for all 4 score vectors, and the parameters and Platt a/b are
      identical to the dev run;
    - calibrated validation Brier / PR-AUC: LR 0.168438 / 0.593775, GBT 0.205233 / 0.560198, equal to
      `model_dev_*`;
    - scorer vs Spark: max |diff| 2.2e-16.
  - Tests (38 new): `test_model_deploy.py` (19) and `test_lr_scorer.py` (19), plus 3 in
    `test_makefile_full.py` and 1 compose-mount integration test.
    - The loader and fit spies see no 2025 data, and `evaluate_test` is never called.
    - Exact round trip.
    - The dev-run match, and the tampered-reference mismatch (exit 4, staging kept, nothing moved).
    - Every metadata field; distinct folders per setting, never the test-run folder; refusal without `--force`.
    - CLI guards and the git sha refusal.
    - Scorer vs Spark within 1e-9: validation rows, None numerics and categoricals, unseen categories, std-0
      columns, NaN/inf/bool rejected.
- Step 4C Phase 1 (audit only): `docs/data_source_audit.md`, a replacement for FastF1 (F1 TV login needed since
  Oct 2026)
  - TracingInsights `2021`..`2025` are live under `TracingInsights`, with SHAs pinned in the audit.
    - Licences: MIT for 2021-2024 (not Apache-2.0 as the brief said) and Apache-2.0 for 2025.
    - Race-only data is 18.7 GB, of which 18.6 GB is per-lap telemetry. Lap, race-control and session tables are
      about 81 MB.
  - All 114 manifest races are present, with names matching Jolpica (114/114), and the driver code equals the
    Jolpica code for 2,272/2,272 driver-races.
  - Compound coverage is 99.75 / 98.58 / 99.86 / 100 / 98.12%. Race control is present in every race, with lap
    numbers on at least 99.95% of messages, and track status is exact.
  - OpenF1 agrees 100% on compound (2023 R5, 2024 R5, 2025 R5), and also on tyre life and stint for 2023/2024.
  - Network use: 30 GitHub API calls, 5 blob-free clones, about 500 raw files (89 MB, scratchpad only) and 9
    OpenF1 calls. 2025 is reported as aggregates only, and its files were deleted.
  - No changes to `src/`, `tests/`, `data/` or `results/`: the v1 hashes are identical before and after.
- Step 4C Phase 2: TracingInsights loader (option B4), additive clean tables and an HDFS replication fix
  - `f1sa-ingest-ti` (`ingest/tracinginsights_loader.py`, `ingest/tracinginsights_parse.py`, pins in
    `ingest/tracinginsights_pins.json`):
    - Fetches only `raw.githubusercontent.com` at the pinned commits. Each file is checked against its git blob
      SHA-1, and sha256 is recorded in `data/manifest_tracinginsights.json` (separate from `data/manifest.json`).
    - Writes through the existing sinks (HttpFS). It resumes per race item (`tracinginsights`,
      `tracinginsights_telemetry`), supports `--only-failed`, and is kill-safe (tested with a real SIGKILL).
    - Disk guard on `/mnt/c`: it refuses below 20 GB and prints the free GB it saw.
    - "None"/"nan"/"" are converted to nulls explicitly and counted per field in each partition's `_manifest.json`.
  - Telemetry (B4) for every race of 2021-2025:
    - Downloaded into memory, then aggregated per (driver, lap) by `features/telemetry_agg_ti.py`.
    - It reuses `telemetry_agg.assign_to_laps` + `aggregate_samples` unchanged (exact equality test against the
      clean FastF1 path).
    - It adds `n_samples` and `telemetry_coverage`: full; partial = span < 90% of the window or a gap > 2.0 s;
      none = 0 samples.
    - The gap threshold is 2.0 s, not the planned 1.0 s: 1.0 s flagged ordinary 1.0–1.3 s feed dropouts
      (14 of 56 real laps).
    - Boundary samples shared by consecutive lap files are deduplicated (the later lap's copy is kept).
    - Raw samples are discarded, except 2024, which is kept as Parquet under
      `raw/tracinginsights/telemetry/season=/round=/driver=/` (skip it with `--skip-raw-telemetry`).
    - Gate (2024 R5): 114 s per race (download 89 s, aggregate 13 s), below the 10-minute limit.
  - `f1sa-clean-ti` (`etl/clean_tracinginsights.py`, schemas in `lake/schemas_tracinginsights.py`) writes five new
    tables, partitioned by season/round:
    - `stints_tracinginsights`: 6,421 rows;
    - `race_control`: 10,191;
    - `race_control_track_status`: 1,338;
    - `telemetry_lap_real`: 125,055, with `is_synthetic=false`;
    - `compound_coverage`: 114.

    It also writes `clean/_quality/tracinginsights.json` and `results/tracinginsights_coverage_full.csv`.
    - Missing compounds stay null with `compound_missing` (2022 Austria 77.4%, 2021 Belgium all UNKNOWN); nothing
      is imputed.
    - The race-control car number comes from `dNum`, or else is parsed from the message text ("CAR n (",
      "PENALTY FOR CAR n"), because `dNum` is null on real messages.
    - Unmapped drivers: 0 in every season.
    - 2025 is in the lake, but reports give counts only.
  - Real run (2026-11-11/12):
    - 228/228 items done, 0 failed, in 3.7 h.
    - 2021 R8 telemetry was redone once (149 s) after a dtype fix: `toPandas()` turned null `gear_changes` into
      DOUBLE, so it is now cast to nullable Int32, with a regression test.
    - The clean job takes 60 s.
    - HDFS added 1.9 GB raw (1.8 GB of it 2024 raw telemetry, 48 MB laps, 4.2 MB aggregates) and 5.9 MB clean.
    - Coverage per season: compound 99.75 / 98.58 / 99.86 / 100 / 98.12%; telemetry laps full 23,745 / 23,504 /
      24,357 / 26,585 / 26,673.
  - Real-data check (2024 R5): 40/40 TracingInsights stint changes match Jolpica pit stops, and the SC/VSC timeline
    covers laps 21–31, the same as the v1 heuristic.
  - HDFS replication: the cause of the 1,936 under-replicated blocks was the Spark jobs in the app container.
    - There is no `HADOOP_CONF_DIR` there, so the Hadoop client used its default `dfs.replication=3`. Those were the
      /f1/clean files (912) and the /f1/features files (1,024).
    - `build_spark` now sets `spark.hadoop.dfs.replication` from `HDFS_REPLICATION` (default 1), and HttpFS CREATE
      passes `replication` explicitly.
    - All 2,552 new raw files and all 570 new clean files are at replication 1. Old blocks are untouched (the count
      is still 1,936).
  - v1 is untouched:
    - The read-only HDFS snapshot (10 clean tables, 10 feature tables, `labels_checksum` and 9 labelled sets) is
      identical before and after.
    - `results/*` (22 files), `data/lake` (39 files) and `data/manifest.json` are identical. `data/sample` is
      unchanged.
    - `clean.py`, `lake/schemas.py`, the label definition and the model spec are not modified.
  - Tests: 736 unit tests pass (79 new in the TracingInsights files plus `test_hdfs_replication.py`), plus the
    integration test `test_spark_in_app_writes_hdfs_with_replication_one`. Lint, format and mypy are clean.
- Step 4C wrap-up: lap reconciliation of TracingInsights with Jolpica (read-only, counts only)
  - `etl/reconcile_ti.py` and `make reconcile-ti-full` (app container, HDFS). It writes nothing: no sink and no
    results file, and a test checks the source.
    - Key uniqueness of all 5 TI clean tables, plus a lap-level stint check (`start_lap..end_lap`, no lap in two
      stints).
    - Left join from Jolpica laps to `telemetry_lap_real` (no fan-out).
    - Rows on one side only, each assigned the first matching cause: lap < 1, duplicate, driver absent, after
      the last lap, before the first lap, gap. These are split by Jolpica status class, with cross-tabs for a
      null raw TI `lap_time_s` and `results.laps + 1`.
    - Races below 95% compound coverage.
  - Result (PASS):
    - 0 duplicate keys in every TI table, and 0 stint-lap overlaps.
    - The left join keeps the Jolpica count in every season, and every Jolpica lap matches.
    - Jolpica-only: 0. TI-only: 70 / 48 / 36 / 32 / 35 (221 rows), exactly the count differences. Every one has a
      null TI lap time (an uncompleted lap).
      - 56 rows: retired on lap 1 or did not start (`results.laps = 0`).
      - 145 rows: the lap after `results.laps` (a car's retirement lap, or the lap it was on at the finish).
      - 20 rows: 2021 R12 Spa, laps 2–3 behind the SC.
    - 0 lap-0, 0 duplicate and 0 mapping causes.
    - The 2025 TI count is 26,692, not the 26,711 in the brief.
    - 2025 races below 95%: R6 Miami 0.5562, R13 Belgium 0.9352.
  - Docs, in `docs/data_dictionary.md`:
    - a "Lap reconciliation" table, with what was verified and what is interpretation;
    - the 2.0 s partial-gap threshold and why it was chosen.
  - `ti-sample-check` does not appear in the README, `traceability.md`, the Makefile, src or tests. Nothing to
    rename.
  - Tests: `tests/unit/test_reconcile_ti.py` (15, in `make test-ti`), covering:
    - uniqueness on each key, and stint overlap;
    - no fan-out vs fan-out, and per season;
    - every cause in priority order, with the totals identity;
    - the coverage boundary at 0.95;
    - the fixture race end to end;
    - the CLI exit codes, and the lake left unchanged.

    `make test-ti`: 102 passed.
  - v1 unchanged. Before and after are identical for:
    - `results/*`, `data/manifest.json`, `data/manifest_tracinginsights.json`, `data/lake`, `data/sample`;
    - the HDFS listings and file checksums of `/f1/raw`, `/f1/clean` and `/f1/features`;
    - 25 Spark table checksums (10 v1 clean, 10 features, 5 TI), plus `labels_checksum` and the 9 labelled sets.
- Step 4C → model: feature set `real_ti` (TracingInsights compound, tyre age and telemetry), development mode only
  - `model/features.py`:
    - new provenance `tracinginsights`;
    - `FEATURE_SETS["real_ti"]` = `real` + 19 inputs (46 assembled columns, 98 in total), appended last, so the
      `real` (52) and `with_synthetic` (94) columns are unchanged;
    - `DEV_ONLY_FEATURE_SETS`.
  - `model/dataset.with_ti_inputs`:
    - compound and tyre age at L−1 from the `stints_tracinginsights` stint covering L−1. The age is null when the
      tyre life is unknown (never assumed new).
    - Telemetry plus a `telemetry_partial` flag come only through `jolpica laps LEFT JOIN telemetry_lap_real`.
    - `DatasetError` on any fan-out (lap count or attempt count changes).
  - Also in `model/dataset.py`:
    - `read_clean_seasons` reads only the requested `season=` directories;
    - `missing_rates` gives per season and feature: n_attempts, n_missing, missing_rate, n_telemetry_partial.
  - Coverage policy (user rule): every null becomes 0.0 + `_missing`, or `__unknown` for the compound. Partial
    laps keep their values plus the flag. No race or attempt is dropped, excluded or down-weighted. Class weights
    are unchanged.
  - `model/pipeline.py`:
    - `real_ti` only with `--source full --no-test` (or `sample`). It is refused with `--allow-test-season`, because
      `docs/model_spec.md` is frozen to `real`, and refused for `rehearsal`.
    - Its files carry `_real_ti`; the `real` names are unchanged (pinned by a test).
    - A new `model_dev_missing_rates_real_ti*.csv`, and `coverage_policy` plus `missing_rates` in the metadata.
  - `make model-dev-ti-full` (both settings) ran in 2021-2024 only. The console log was lost when the user
    interrupted the session; the output files are complete for both settings.
    - The counts and class weights equal `real` (108/32 + 49/16; 184/50 + 64/22). Every TI input has missing rate
      0.0 in every season with 0 partial flags.
    - 2021 R12 has no labelled attempt. 2022 R11's 3 attempts are kept and none sits in a compound-null stint.
  - v1 unchanged, compared with the before-values recorded at the start of the step:
    - 37 local files (`results/*` v1, both manifests, deployment model json);
    - `data/lake` and `data/sample` aggregates;
    - the HDFS listings and checksums (`/f1/raw` 3,542 files, `/f1/clean`, `/f1/features`);
    - 35 Spark checksums (incl. `labels_checksum` and the 9 labelled sets).

    No model file changed after the step started. New files: only `results/model_dev_*real_ti*` (20).
  - Tests: `test_model_ti_inputs.py` (9), plus 2 `real_ti` guard/name tests and a `real_ti` dev run in
    `test_model_pipeline.py`, plus a Makefile dry run.
- Step 4C → model, part 2: `real_tyre`, the leakage test, permutation importance for both models, and the reruns
  - `real_tyre` = `real` + `{attacker,defender}_{compound,tyre_age_laps}_ti` (68 columns at the time; part 3 adds
    the tyre-age delta). It uses `features.FEATURE_SET_ONLY`, with the same guards, missing rules and loader
    as `real_ti`, and its files carry `_real_tyre`. `make model-dev-tyre-full`.
    - `real` is still 52 columns, equal to the deployed `lr/scorer.json` list. `scorer.json` sha256 is
      `0a9b7dfb…`, unchanged.
  - Leakage test (new):
    `test_model_ti_inputs.py::test_ti_inputs_unchanged_when_future_laps_and_later_races_are_scaled_x100[real_tyre|real_ti]`.
    Every value after L−1 in the race and every value of a later race ×100 (future compounds WET, telemetry
    partial) leaves the decision-time inputs identical.
    - Probe: the whole-stint TI compound equals the compound from laps ≤ L−1 for 0 differing of 157 / 248
      labelled decision laps.
  - `model/permutation_dev.py` + `make model-dev-permutation-ti-full`: permutation importance for LR and GBT on
    calibrated (and raw) scores. It refits and checks the dev run (reproduction MATCH at both settings), writes
    `results/model_dev_permutation_real_ti*.csv`, and leaves the pipeline outputs untouched.
    `explain.permutation_importance` gains an optional `transform`; the default is unchanged.
  - Reruns in tmux, logged to `data/model_dev_ti.log`: `real_tyre`, `real_ti` and the permutation, both settings.
    All exit 0, and only `season=2021..2024` partitions were read.
    - The 20 rerun `real_ti` files are byte-identical to the first run (`data/real_ti_before.sha`).
    - New: 20 `real_tyre` files and 2 permutation files.
  - The comparison tables are reported in chat, stamped DEVELOPMENT, with no winner picked. They come from the
    `model_dev_eval*`, `model_dev_loso*` and `model_dev_permutation*` files.
  - v1 unchanged:
    - 37 local files, the `models/` aggregate, `data/lake` and `data/sample`;
    - the HDFS listings and checksums;
    - 35 Spark checksums.
- Step 4C → model, part 3: strict decision-time compound, determinism for `real_ti`/`real_tyre`, delta in `real_tyre`
  - Compound from laps ≤ L−1 only: `dataset.lap_compounds` (raw TracingInsights per-lap labels, mapped to driver_id
    through the stints) and `dataset.tyre_at_decision` (the single known label over start_lap..L−1 of the stint
    covering L−1). The leakage test gains a case where same-stint laps after L−1 carry `SOFT` or null and the
    whole-stint compound is null; the decision-time compound is unchanged. The old known issue is closed.
  - Determinism: `dataset.canonical_order` puts train and validation in 1 partition, sorted by (season, round,
    attacker_id, defender_id, lap), for the TracingInsights sets only, inside `build_dataset`, so the dev run and
    `permutation_dev` use the identical path. `real`'s plan is unchanged (tested).
    - Why: after the compound change the same code gave GBT validation PR-AUC 0.72970 in the dev run and 0.73216 in
      the permutation refit (row-order nondeterminism in GBT; LR unchanged). 19 of the 42 development files
      changed, so the permutation self-check stopped (exit 4).
    - **The earlier 19 files were replaced.** Their hashes are in `data/dev_ti_tyre_before.sha` only. The new
      reference is `data/dev_ti_tyre_reference.sha`.
    - The size of the change: the Part 1 rerun moved GBT by up to 0.0025 PR-AUC, but the fix itself (a different,
      now fixed, row order) moved it more. Validation GBT PR-AUC: `real_ti` G=3.5 0.7322 → 0.6893 (−0.043; chosen
      stepSize 0.1 → 0.05), G=5.0 0.642 → 0.632; `real_tyre` G=3.5 0.591 → 0.621, G=5.0 0.611 → 0.617. LR is
      unchanged in every file.
  - Plan fix: `with_ti_inputs` builds each side's tyre lookup from `attempts` (not the growing frame) and caches
    its result. Without this, dynamic partition pruning nested exponentially under the (G, k) filter (a 32 MB
    physical plan, out of memory on the sample-lake fixtures in `test_model_pipeline` and `test_permutation_dev`).
  - Runs (tmux, `data/model_dev_ti.log`), only `season=2021..2024` read:
    - `model-dev-tyre-full`, `model-dev-ti-full`, `model-dev-permutation-ti-full`, twice: all 42 files
      byte-identical between the two rounds. The permutation refit gave reproduction MATCH at both settings in
      both rounds.
    - `real` read-only check: `model-dev-full` twice. All 18 `real` development files equal the committed hashes
      both times (`data/real_dev_committed.sha`), so `real` is reproducible as is; its code was not changed.
  - `real_tyre` gains `tyre_age_delta_laps_ti` (defender − attacker): 70 columns, and it differs from `real_ti`
    only by the telemetry inputs and the partial flag (tested). `model-dev-tyre-full` run twice: 20/20 files
    byte-identical. Counts unchanged (108/32 and 49/16; 184/50 and 64/22).
    - Validation PR-AUC: LR 0.604 → 0.650 (G=3.5), 0.710 → 0.720 (G=5.0); GBT 0.621 → 0.674, 0.617 → 0.636.
    - LR LOSO mean 0.398 → 0.402 (G=3.5), 0.468 → 0.468 (G=5.0).
  - Unchanged: `real` 52 columns, `scorer.json`, the `real_ti` files after the reference run, and v1.
- Step 9e: pre-registered model spec v2 `docs/model_spec_v2.md` (frozen once committed and tagged `model-spec-v2`)
  - It adds to v1 and replaces nothing. On 2025: `real` (PRIMARY, 52 columns), with `real_tyre` (70) and `real_ti` (98) as
    ABLATION for #30. Each gets LR and GBT, raw and calibrated, at G=3.5 k=2 (primary) and G=5.0 k=3. The majority
    and rule baselines are fixed references. Deployment stays `real` calibrated LR (`scorer.json` `0a9b7dfb…`, unchanged).
  - Selection rule (v2 playbook, 12 Nov 2026) applied to `results/model_dev_loso{,_real_tyre,_real_ti}.csv`: mean LOSO
    LR PR-AUC is 0.413 / 0.402 / 0.374, so `real` stays primary for any margin ≥ 0.
  - It also covers the v1 rules (unchanged), the paired race-cluster ablation intervals with no significance claim,
    the ORACLE label, determinism (GBT is row-order sensitive up to 0.043), missing data (Miami R6 55.6%, Belgium R13
    93.5%) and what was seen before the freeze (no model beat the rule on LOSO at the primary setting: 0.533 vs 0.559).
  - Code: `features.TEST_FEATURE_SETS = ("real", "real_tyre", "real_ti")`. `RunConfig` accepts `--allow-test-season` only
    for these, so `with_synthetic` is still refused. The overwrite guard already covers the `_real_tyre`/`_real_ti`
    names, and this is now tested.
  - `make model-test-full CONFIRM=yes` makes 6 runs (`real`'s two commands are byte-identical to v1), and
    `FEATURE_SET=<set>` runs one set (crash recovery). Any other value is refused.
  - Tests: the spec marker line equals `TEST_FEATURE_SETS`; all 6 test runs write disjoint files and `real`'s names
    are unchanged; the overwrite guard for `real_tyre`/`real_ti` (each file blocks only its own run, and `real`'s files
    and the other set never block it); Makefile order, `FEATURE_SET`, refusals. No 2025 read, no training, no test run.
- Step 11 (W5): Redis serving layer (#21–#24) and the Redis latency benchmark (#37)
  - `serving/keys.py`: `driver_key`, `order_key`, `events_channel` and `race_pattern`, exactly the CLAUDE.md
    names. Validation: season 1950–2100, round 1–30, driverId `[a-z0-9_]+`, ints only, else `InvalidKeyError`.
    Also defines `DRIVER_FIELDS` (the 9 fields) and `EVENT_TYPES`.
  - `serving/redis_store.py`, sync `RaceStore` and async `AsyncRaceStore` (`redis.asyncio`), built on the same
    command code:
    - `upsert_driver_state` writes all 9 fields as strings, with `""` for null (user choice) and `updated_at`
      ISO-8601 UTC with ms;
    - `set_order` is a ZADD with score = position;
    - `publish_event` sends JSON with `type`, `event_ts` and `published_at`;
    - `expire_race` SCANs `race:{s}:{r}:*` and EXPIREs every key, the order included; `end_session` publishes
      `session_end` and then expires;
    - `write_batch` sends a micro-batch through one `pipeline(transaction=False)` with one `execute()`;
    - `DriverState` validation rejects NaN, inf, negatives, bad compounds and duplicate positions before anything
      is sent;
    - the URL comes only from `Settings.redis_url`.
  - Settings: new `REDIS_SAFETY_TTL_S` (default 21600 s), set on every key write. `.env.example` is updated.
  - `serving/latency_bench.py` + `experiments/redis_latency.py`. 20 drivers at 1, 10 and 60 micro-batches/s for 60 s
    each (user choice: rate = micro-batches/s). Each tick times one pipelined write, 20 HGETALL and 1 ZRANGE.
    The benchmark uses its own namespace `race:2100:30:*`, refuses if that namespace is occupied, and cleans up
    afterwards.
    - `redis-cli --latency -i 1` exits after one 1 s summary when there is no TTY (checked: 1.2 s, 97 samples).
      It is therefore run 30 times back to back in one `docker compose exec` and the results are combined: min,
      max, sample-weighted mean, total samples.
  - Run (core and full profile containers up), `results/redis_latency.csv`, p50 / p95 / p99 in ms:

    | rate | write (20 drivers, 1 pipeline) | HGETALL | ZRANGE | achieved, late ticks |
    |---|---|---|---|---|
    | 1/s | 1.31 / 2.04 / 2.24 | 0.25 / 0.49 / 0.63 | 0.28 / 0.52 / 0.68 | 1.00/s, 0 |
    | 10/s | 2.06 / 3.48 / 25.75 | 0.41 / 0.67 / 1.42 | 0.50 / 0.82 / 4.10 | 10.00/s, 8 |
    | 60/s | 2.10 / 9.20 / 19.77 | 0.44 / 1.09 / 8.17 | 0.54 / 1.70 / 8.77 | 51.09/s, 2876 |

    In-container PING (redis-cli, 30 × 1 s): min 0.053, mean 0.282, max 10.02 ms, 2914 samples. The environment
    fingerprint is in `results/README.md`.
  - Tests: `test_keys.py`, `test_redis_store.py` (sync + async on fakeredis), `test_latency_bench.py`,
    `test_config.py` (new setting), and `tests/integration/test_redis_store.py` (real Redis 8.10: batch, pubsub,
    TTL, async).
- Step 12 (W6): Kafka race replay and Structured Streaming undercut detector into Redis (#19, #20, #23, #31)
  - User decisions: track status and race distance cannot be computed live (the heuristic uses the whole race's
    green median; `race_laps` is the winner's lap count). They are replayed as race context on each lap event
    (`track_neutral`, `track_red_flag`, `race_laps`; disclosed). On the host the Kafka connector comes from
    `spark.jars.packages` (pinned 4.2.1, ivy cache `data/cache/ivy`).
  - `streaming/`: `events.py` (schema, JSON, Spark StructType, infra-header filter), `producer.py` (one race in
    race order x SPEED, acks=all, idempotent, key=driver_id, `session_end` per partition, 2025 refused),
    `stream_job.py` (readStream latest, maxOffsetsPerTrigger, trigger 500 ms, foreachBatch, progress listener),
    `processor.py` (driver-side state, watermark over partitions, state snapshot and batch-id replay skip),
    `live_state.py` / `live_features.py` (Spark-free ports of the 21 `real` inputs and the shared rule),
    `priors.py` (pit-loss and out-lap priors of earlier races, one partition read), `redis_sink.py` (one
    `write_batch` per batch and race, `end_session`, nothing after it), `latency.py`, `demo.py`, `parity.py`.
  - `make stream-demo SEASON ROUND SPEED [FULL=1]`, `make stream-parity-full`; features/, model/, serving/ untouched.
  - Parity (21 inputs, docs/architecture.md "Streaming"): full lake 2021-2024, 90 races, 34,608 eligible pairs at
    every decision lap and 157 labelled attempts identical; max input diff 0.0, max P diff 3.3e-16
    (`results/stream_parity_full.csv`). Sample and Step 8 scenarios too, at batch sizes 1/7/50 and a lagging
    partition, with the rehearsal model and the deployed model.
  - Runs (sample 2024 R5): host at speed 60 and 10 and FULL=1 at 60, 20 drivers in the order set, TTL ~3600 s
    after session_end. Recorded run (`make stream-demo` speed 60, after the alert policy): 1,252 messages, end
    to end p50 814 / p95 1481 / p99 3303 ms; the 500 ms trigger dominates.
  - Tests: 12 new unit files (parity, events, producer, live state and features, priors, sink, job, latency,
    demo, Makefile) and `tests/integration/test_streaming.py` (real Kafka + Redis, speed 60, infra message).
  - Alert policy (`alerts.py`; `reason`/`trigger` in `events.UndercutAlert`): `opportunity` on a new candidate
    or a P crossing 0.5 upward, `attempt` (P at L-1) when the attacker pits; hashes, rule, scorer, parity
    unchanged. 2024 R5: 409 -> 141 alerts (113 new, 19 crossed, 9 attempts = batch detector-eligible attempts,
    labelled hulkenberg/bottas L-1=7 included); `parity.check_alerts` in the unit and integration tests.
- Step 13 (W6): FastAPI backend `src/f1sa/api/` (#25, API half)
  - User decisions: the clean zone is read over file:// **and** HttpFS (read-only: LISTSTATUS and OPEN over
    GET only, `{lake}/clean/{races,results,laps}` only, `season=2025` never listed or opened unless
    `ALLOW_TEST_SEASON=1`); one replay at a time (`API_MAX_REPLAYS=1`).
  - Endpoints, all under `/api`, docs at `/api/docs`:
    - `GET /health`: Redis PING, Kafka topic describe, and the deployed model loaded with the stream job's own
      `load_model`. It reports version, git sha, dirty flag and the scorer sha256 (`0a9b7dfb…`). 200, or 503
      naming the failed check.
    - `GET /races`: replayable races from the clean zone, using the producer's race-distance rule.
    - `GET /races/{s}/{r}/state`: the 9-field `DriverStateOut` (`""` → null), plus the order; 404, 422,
      500 and 503.
    - `GET /races/{s}/{r}/events`: SSE, with event = type and id = `payload.event_id` (or a deterministic
      uuid5 for alerts and `session_end`, which carry no id). It adds `sse_sent_at_ms`, sends a heartbeat
      every 15 s, unsubscribes on disconnect (both ASGI spec paths) and closes after `session_end`.
    - `POST /races/{s}/{r}/replay {speed: 1|10|60}`: a producer subprocess, only with `ENABLE_REPLAY=1`.
      Gives 409 for the same race or at the limit, and refuses 2025 with the producer's own `check_season`.
    - `POST /debug/latency`: only with `ENABLE_DEBUG=1`. It joins the client's render time with the
      server-side timings of the event it sent and appends to `results/ui_latency.csv`.
  - Settings (`config.py`, `.env.example`): `API_HOST` (127.0.0.1), `API_PORT`, `API_CORS_ORIGINS` (a list since the Step 14 follow-up; was one
    origin, `*` refused), `ENABLE_REPLAY`, `ENABLE_DEBUG`, `ALLOW_TEST_SEASON`, `API_MAX_REPLAYS`,
    `API_SSE_HEARTBEAT_S`, `API_RACES_CACHE_S`, `API_KAFKA_TIMEOUT_S`, `API_MODEL_DIR`, `API_UI_LATENCY_CSV`.
  - JSON-lines logging: one access line per request with no query string; exceptions are logged by class
    name only; no settings appear in responses or logs (tested with a password in the Redis URL).
  - Compose `api` service (core profile, app image, `python -m f1sa.api`, mem_limit 1536m):
    - published on `127.0.0.1:${API_HOST_PORT:-8000}` only;
    - mounts the repo, `./data` and `./models` read-only, and `./results` read-write;
    - its healthcheck accepts 200 or 503;
    - `make up` now waits for it.
  - Makefile: `make api` (host, file lake) and `make test-api`. `pyproject.toml`: a `f1sa-api` script, and
    pyarrow added to mypy's untyped modules. No new dependency.
  - No change to `streaming/`, `serving/`, `features/`, `model/`, `lake/`, `ingest/`, `scorer.json` or the
    label definition and model specs (`git diff` empty there).
  - Verified live (core and full profiles up; only the new `api` container was created, with `--no-deps`):
    - Host `make api` gives health 200, races 2023 R5 (57 laps) and 2024 R5 (56), bound to 127.0.0.1.
    - Container SSE during `make stream-demo SEASON=2024 ROUND=5 SPEED=60`: 1,252 frames (1,030 lap, 40
      pit_in, 40 pit_out, 141 undercut_alert, 1 session_end, the same counts as the Step 12 run), all ids
      unique. The stream closed after `session_end`. Redis write → SSE send took p50 2 / p95 5 / max 332 ms.
      State: 20 drivers with exactly the 9 fields.
    - Container replay: 202, then 409 (running), 409 (limit), 403 (2025) and 422 (speed 7). The producer
      delivered 1,113 events and exited 0, which freed the slot. Peak container memory 775 MiB.
    - HttpFS catalog on the real HDFS lake (app container): 90 races (2021–2024) in 3.2 s. Only GET
      LISTSTATUS (195) and OPEN (180), all under `/f1/clean/{races,results,laps}`, and 0 paths with
      `season=2025`. With `ALLOW_TEST_SEASON=1`: 114 races.
    - The `make stream-demo` check overwrote the three recorded Step 12 results files. They were restored
      from git, and their sha256 equals the before-values.
  - Tests:
    - 10 unit files: `test_api_{config,health,races,state,events,replay,debug,app}.py`,
      `test_makefile_api.py` and `_api_fixtures.py`.
    - They cover every status code; the state model's exact fields; SSE frames, heartbeat and close; raw
      ASGI disconnect for spec 2.3 and 2.4; the replay 409s, cap, concurrency, 2025 and a real subprocess;
      the ENABLE flags; CORS; and logs.
    - The three HttpFS guards (read-only, clean zone only, 2025) each have a test.
    - `tests/integration/test_api.py` runs uvicorn over TCP on real Redis and Kafka: health 200, state,
      live SSE with heartbeat, unsubscribe after a real disconnect, and close after `session_end`.
- Step 13 follow-up (user's three fixes, plus the whole-pipeline replay the user chose)
  - **Full-lake api container.** `make api-up [FULL=1] [REPLAY=1] [DEBUG=1] [TEST_SEASON=1]` recreates only
    the `api` service (`up -d --no-deps --no-build --force-recreate api`: no other service restarts, no
    volume touched). `FULL=1` sets `API_DATA_LAKE_URI=hdfs://namenode:8020/f1` (HttpFS `http://httpfs:14000`)
    and refuses unless the hdfs profile is healthy. The default stays sample mode, and `make up` restores it.
    README "API" documents it.
  - **Port clash.** `make api` uses `API_PORT=8001` by default; the container keeps 8000. If the port is
    taken, `python -m f1sa.api` exits 2 naming the holder (psutil, else the Docker container publishing
    it) and the remedy. Verified live: "port 127.0.0.1:8000 is already in use by Docker container
    f1sa-api-1 …".
  - **Demo outputs.** `make stream-demo` and API replays write to `data/demo/<run_id>/`; only
    `RECORD=1` writes to `results/` (`experiments/streaming_latency.py --record`, new `demo_runs.py`,
    `DEMO_DIR`). `streaming/` is unchanged: `demo.py` and `stream_job.py` already took `--results-dir`.
  - **Replay = the whole pipeline** (user choice):
    - order: stream job, ready file, producer, then the stream job stops after `session_end`;
    - outputs in `data/demo/<run_id>/`;
    - 60 s readiness timeout (`API_REPLAY_READY_TIMEOUT_S`) and 180 s end timeout; on a timeout or failure
      both processes are killed and the slot is idle again;
    - state and error in the new `GET /api/replays`;
    - process groups with SIGTERM, then SIGKILL and a final sweep, so no orphan JVMs.
    - Bug found by the real-subprocess test and fixed: Python 3.12's `asyncio` `wait()` also waits for
      the pipes to close, so a leftover child holding stdout hung the replay. Waiting now uses the
      leader's exit code.
  - Compose `api` (final, user-approved):
    - `mem_limit` is 2048m (the user is raising WSL to 14 GB);
    - mounts: `./data` read-only, with `./data/demo` and `./data/checkpoints` read-write on top;
      `./models` read-only; `./results` read-write;
    - `make up`, `up-full` and `api-up` create the two host folders first, so Docker does not create them
      root-owned.
    - Checked live: inside the container, `>> /workspace/data/manifest.json` gives "Read-only file
      system" (EROFS), and the host file's sha256 is unchanged. `/app/data` does not exist in this image;
      the app works under `/workspace`. `models/` is read-only too.
    - A sample replay of 2024 R5 at 60× `finished` (0/0) in 2 min 2 s with 1,252 SSE events. Afterwards
      only uvicorn runs in the container, `results/` is identical, and peak memory was 1.49 GiB.
  - `make demo-clean [CONFIRM=yes]` (`python -m f1sa.demo_runs`):
    - deletes only the `<run_id>` folders of `./data/demo` and the `api_<run_id>` / `demo_<S>_<R>_<unix>`
      checkpoints (and their `.ready` files), and prints what it deleted;
    - without CONFIRM it lists and exits 1;
    - it refuses (exit 2) any other root (`..`, absolute paths, a symlinked root, `DEMO_DIR`
      elsewhere), and keeps other names and symlinked entries.
    - Live dry run: 19 entries listed, nothing deleted.
    - Tests: `test_demo_clean.py` (17), covering 10 refused paths, the symlinked root, a symlinked
      entry, the listing, and the make target.
  - Live (`make api-up FULL=1 REPLAY=1`, only the api container recreated):
    - `/api/races` gives 90 races, from 2021 R1 Bahrain (56 laps) to 2024 R24 Abu Dhabi (58).
    - Replay of 2023 R10 at 60×: the first SSE lap events came about 23 s after the POST. It
      `finished` with exit codes 0/0 in 106 s, and Redis holds 20 drivers.
    - Afterwards only tini and uvicorn run in the container. Outputs are in `data/demo/<run_id>/`, every
      `results/` file is byte-identical, and `git status results/` is empty.
    - Peak api container memory: 1.33 GiB (the limit was 1.5 GiB then; it is now 2 GiB).
  - Tests:
    - `test_api_replay.py`, rewritten (26 tests): the pipeline order and argv; the timeout path
      (killed, failed, slot idle, error in `/api/replays`); SIGTERM ignored then SIGKILL; exit before
      ready; producer failure; no stop after the producer; shutdown; and two real-subprocess tests
      checking that the stream job's child (a stand-in JVM) is gone after the timeout and after a
      normal finish.
    - `test_demo_outputs.py`: a default demo run leaves `results/` unchanged (sha256), `RECORD=1`
      writes it, the argv rules, the run ids, and the make targets.
    - Port-clash and `main` tests in `test_api_app.py`; the `api-up` make tests.
- Step 14 (W7): live race dashboard `web/` (#25, #41)
  - User decisions: the gap chart is hand-written SVG (no chart library), and Playwright uses a host
    Chromium download.
  - Pinned exactly, with `package-lock.json` and `save-exact`:
    - Node 26.13.0 (LTS);
    - Next.js 16.4.5, React 19.3.1, TypeScript 6.0.3;
    - ESLint 9.39.5, Vitest 5.0.5, Playwright 1.64.1.
  - No UI, CSS or schema library.
  - Pages:
    - `/` is a race picker grouped by season. It has a search, arrow/Home/End keys, a polite count, and a
      tag on each synthetic race.
    - `/race/[season]/[round]` has the timing tower (8 columns, order-set order, a "laps down" rule), a
      15-lap gap chart, a tyre panel (text + glyph + age bar), the alert feed (grouped by pair; the
      `aria-live` region speaks only attempts and threshold crossings), replay control (1/10/60, status
      from `/api/replays`, failed + message), a connection indicator and the lap counter.
    - A fixed banner strip shows SYNTHETIC DEMO DATA in sample mode.
  - One pure reducer (`web/src/lib/reducer.ts`). Race state is pushed only: SSE events plus the API's
    `event: snapshot` (see the follow-up below). On a reconnect the stream is opened first, then `/state`
    is reloaded once, then the buffered events are applied.
  - API change (user-allowed, tested): `/api/races` items gain `synthetic` (from the clean `stints`
    `is_synthetic`; `stints` joins the read-only HttpFS table allowlist), and `/api/health` gains
    `lake {scheme, mode, synthetic}`, which is informational and never changes the status.
  - Docker:
    - `docker/web/Dockerfile`: 3 stages, standalone output, runs as `node` (uid 1000);
    - compose `web` (core and full): 127.0.0.1:3000, read-only root filesystem, `cap_drop ALL`,
      `no-new-privileges`, 512 MiB;
    - `make web` (dev, with a port preflight), `make web-test`, `make web-e2e`;
    - a CI `web` job.
  - Found and fixed with the screenshots and the e2e test:
    - At 375 px the visually hidden chart table caused a 15 px horizontal scroll: a `<table>` ignores the
      1 px width, so a wrapper is hidden instead.
    - "+1 L" showed for cars that were merely behind on the line: it now needs the leader to have
      completed the next lap first.
    - The driver column collapsed to 0 at 375 px: all columns now get fixed widths.
    - Chart end labels piled up at the top edge: a two-pass layout now shrinks the spacing evenly.
    - Undercut flags stuck on every car: alerts only report new candidates. Fixed on the server with the
      pushed `snapshot` event (see the follow-up below).
    - Dark border and grid colours were under 3:1: caught by the contrast test and raised.
  - Verified live (sample mode, `make api-up REPLAY=1`):
    - the Playwright smoke test passes against `next start` (49.5 s) and against the production
      container (19.1 s, 45.6 s);
    - `curl -sI localhost:3000` gives 200 with the security headers; `id` in the container gives
      uid 1000 (node);
    - only the `api` and `web` containers were recreated;
    - `results/` is byte-identical after every replay, and `git status results/` is empty.
  - Tests:
    - Vitest: 128 in 5 files (reducer 42, aria-live filter, event parser, guards, format, chart geometry
      and label layout, params, picker and replay helpers, WCAG contrast of both themes);
    - Playwright smoke test: 1;
    - Python: `test_api_races.py` (synthetic per race, stints over HttpFS), `test_api_health.py`
      (`lake` block) and `test_makefile_web.py` (6).
- Step 14 follow-up: pushed snapshots instead of client polling, and two CORS origins
  - The 5 s `/state` polling is removed from the client. The API now pushes `event: snapshot` (id
    `snapshot-<lap>`), with all driver hashes plus the order zset in one pipeline (`api/state.py`, shared
    with `GET /state`). It is sent right after connect and once per leader lap (the first `lap` event of a
    higher lap number). Per-lap snapshots never SCAN. The client keeps the open-stream-then-reload-`/state`
    logic for reconnects only.
  - CORS: `API_CORS_ORIGINS` is a comma-separated list, defaulting to `http://localhost:3000` and
    `http://127.0.0.1:3000`; `*` is refused. Checked live: both are echoed back, and `http://evil.example`
    gets no header.
  - Mid-race, without polling (page opened at `127.0.0.1:3000`):
    - 28 snapshots; the UI's flagged count equals the snapshot's on all 27 laps;
    - it also equals a direct Redis read on 26/27 laps (lap 20: Redis 13 vs UI 12, one batch later);
    - 0 page requests to `/state`.
  - Tests:
    - API, 5 new SSE tests: one snapshot after connect and one per leader lap (not per event); stale flag
      refreshed; one pipeline and no SCAN per lap; an unreadable snapshot skipped while the stream goes
      on; snapshots are not latency samples;
    - CORS for both origins plus 5 refused origins; the origin list from settings and env;
    - Vitest 133: snapshot replacing stale flags, no dedupe, zset order, malformed and foreign snapshots;
    - `make web-e2e` passes (58.7 s).
- Step 10 (W5): Spark scalability, partitioning and caching experiments (#27, #28, #33, #34, #38)
  - New package `src/f1sa/bench/` (user-approved layout), thin entry points `experiments/scalability.py`,
    `experiments/partitioning.py`, `experiments/_resources.py`. Method and limits: `docs/experiments.md`.
    - `paths.py`: `check_bench_uri` allows only `hdfs://namenode:8020/f1/bench/**` and `./data/bench/**`
      (refuses `/f1/raw`, `/f1/clean`, `/f1/features`, `..`, symlinked roots, other hosts and schemes);
      run ids; outputs in `data/bench/<run_id>/`, `results/` only with `--record`; `bench-clean`.
    - `replicate.py`: x1/x4/x16/x64 of the 9 clean feature inputs (season + 100·r, dates + 100·r years,
      `driver_id__r<r>`, constructors unchanged, `is_replicated`); exactly factor x rows, unique keys.
    - `workloads.py`: `compute_features` unchanged, written with `write_partitioned`; `undercut_attempts`
      to `noop` (labels never read or persisted). Telemetry: `aggregate_samples` over the 2024 raw
      samples (first 5/14/24 rounds for 1/3/5 seasons, x1 cells only), joined to laps, into `noop`.
    - `cell.py` (one JVM per cell; local driver 2g + `MALLOC_ARENA_MAX=2`; cluster driver 1g, executors
      768m x 2 cores, current `src/f1sa` shipped as a zip and checked on the executors), `grid.py`
      (cells, guard, estimate, reduced grid), `timing.py` (warm-up + timed runs, median/IQR/min/max,
      `MemoryGuard`), `spark_metrics.py` (REST API bytes per job group), `resources.py` (psutil +
      cgroup sampler, `docker stats` sampler, window peaks), `partitioning.py` (2 layouts x 3 caching
      variants x file/hdfs, result checksums, speed-ups), `report.py` (CSV, PNG, fingerprint), `host.py`.
  - Makefile: `bench-dirs`, `bench-smoke`, `experiments-spark [RECORD=1] [GRID_ARGS=] [PART_ARGS=]`,
    `bench-clean [CONFIRM=yes]`, `test-bench`. All use `docker compose exec` into the running app container.
  - Compose: app `mem_limit` 1536m -> 4096m (user-approved); only the app container was recreated
    (`up -d --no-deps --no-build --force-recreate app`), limit verified 4294967296.
  - Changes from the approved plan, all forced by smoke-run measurements:
    - local driver heap 2g instead of 2560m: at 2560m the driver RSS was 3.4 GB and the guard killed the
      first cell (1 season, x1) at 3.61 GiB;
    - no linear memory projection: it projected 3.16 GiB (x1) to 12.65 GiB (x4) and skipped a cell that
      ran at 3.55 GiB. The guard now kills at 90 % of the container at runtime and then skips larger
      factors of the same (phase, seasons, master);
    - the telemetry summary uses every per-lap aggregate: with only three of them Spark pruned the
      gear-change lag window from the plan (unit test checks the plan has a `Window`);
    - partitioning averages over `decimal(20,6)`: a double average flipped a 6-decimal rounding between
      layouts (108.706062 vs .063), caught by the checksum test;
    - the smoke also runs local[4] at x4 so the estimate is an affine x1/x4 fit, not linear.
  - Smoke runs (data/bench only; `results/` and HDFS raw/clean/features listings identical before/after):
    - scalability `bench_20261113T072050Z_smoke`: 11/11 cells ok. Build at 1 season x1, one run:
      local[1] 44.5 s, local[2] 37.0 s, local[4] 33.1 s, cluster 42.3 s; local[4] x4 94.0 s. Telemetry
      (5 rounds): 8.5 / 6.8 / 5.5 / 7.7 s. Peak app container 2.9-3.1 GiB at x1, 3.55 GiB at x4.
      Input ~75 MB, shuffle ~13 MB per x1 build run; executors imported the shipped zip.
    - partitioning `bench_20261113T072933Z_psmoke` (laps x1, 5 telemetry rounds, 1 run): 84 rows,
      every checksum identical across layouts, variants and file systems.
    - Estimate (affine x1/x4 fit, excludes costs that grow faster than the rows): full grid 72 cells
      ~119 h; partitioning ~0.5 h. Proposed reduced grid, ~6.3 h (45 cells): 1/3/5 seasons x every
      master at x1 and x4, x16 for 1 season on local[4] and the cluster, no x64, 1 warm-up + 3 timed runs.
  - Tests: 104 new (`test_bench_*.py`, `test_makefile_bench.py`): timing harness with a trivial and a
    real Spark job, median/IQR, guard skip and runtime kill of a real child, bench-clean refusals
    (10 roots) and fake HttpFS, replication x1/x4 row counts and unique keys, REST metrics, docker stats
    parsing, plots, record vs default outputs. Full unit suite: 1400 passed in 744 s, results guard silent.
- Step 10 follow-up (user request): resumable `make experiments-spark`
  - The grid appends each finished cell's row to `data/bench/<run_id>/scalability.csv` at once
    (`report.append_rows_atomic`: temp file, fsync, rename), with the host's docker peaks of that cell
    (read from the shared `docker_stats.jsonl`) and the git sha passed into the container.
  - `RESUME=<run_id>` (the exact command is printed at the start of every run): the stored
    `grid_spec.json` is used (`GRID_ARGS` must not differ); stale cell processes of the run are killed;
    cells with a row are skipped; an interrupted cell reruns from its warm-up (its old result file and
    outputs are removed first); a prep with pending cells reruns and reuses its HDFS copy via the
    `_BENCH_PREP.json` marker (or prepares it again); the guard's skips are restored from the CSV.
  - Partitioning resumes by group (dataset, fs, layout, variant), reusing its marked layouts.
  - `RECORD=1`: `results/` only when the grid (then partitioning) is complete; one fingerprint per run id.
    HDFS data are deleted only after a complete run. The host exits 0 when complete (aborted or
    skipped cells are recorded results), so make goes on to partitioning; before, a single aborted
    cell would have stopped make before partitioning.
  - Tests (`test_bench_resume.py`, updated host, workload and make tests): fake grid killed after 2 cells
    then resumed (every cell exactly once; a second grid on the same run is refused by a flock; the interrupted cell rerun, prep rerun without a second row);
    a real grid process SIGKILLed mid-cell then resumed (orphan killed, CSV complete, each cell once);
    guard state restored; refusals; atomic CSV/JSONL append with a partial last line; marker reuse of
    the replicated copy and of the partitioning layouts (Spark); record only when complete.
  - Live check on the stack (`bench_20261113T100000Z_smoke`): the grid was SIGKILLed in the container
    after 2 cells; the resume killed the orphaned cell (pid 50092), reused the x1 copy (0.3 s), ran the
    other 9 cells, and the CSV has all 11 cells once; HDFS data deleted at completion.
  - Finding: in that run `build_s1_x4_local4` was killed by the guard at 3.64 GiB (it passed at 3.55 GiB
    in the previous smoke). Local x4 cells at 3 and 5 seasons in the approved grid will very likely be
    killed too (see Next).
- Step 10 follow-up (user-approved): app container `mem_limit` 4096m -> 6144m
  - Only the app container was recreated (`up -d --no-deps --no-build --force-recreate app`); limit
    6442450944 bytes (docker inspect and the container's cgroup `memory.max`); every other service kept
    running. The local driver heap stays 2g.
  - Re-run of the cell alone (`bench_20261113T095530Z`: `--seasons 1 --factors 4 --masters local4
    --warmup 0 --repeats 1`, prep + build): `build_s1_x4_local4` ok, 74.5 s; peak app container memory
    3.49 GiB (process tree RSS 3.61 GiB, docker stats 3.28 GiB) against the 5.40 GiB guard (90 % of 6 GiB);
    host MemAvailable never below 5.98 GiB (api and web still running). The prep peaked at 1.29 GiB.
  - The x4 cells at 3 and 5 seasons have 2.9x and 4.7x the rows. Memory grew from 3.14 to 3.49-3.64 GiB
    between x1 and x4 at 1 season, so they should fit under 5.40 GiB, but that is not measured.
  - `docs/architecture.md` memory budget: app 6 GiB, core about 9.6 GiB, full about 15 GiB of limits;
    api and web are stopped during the overnight grid (about 12.5 GiB). `docs/experiments.md` updated.
- Step 15 (W7): what-if pit strategy simulator, API route and dashboard panel (#26, #32, #40, #41)
  - User decisions (plan): `stay_out` is an overcut (A pits the lap after D); live lap history comes from an
    API lap recorder on the race channel; the tyre model is an offline artefact in `models/sim/`.
  - `docs/sim_spec.md`: pre-registration of the strategies, every constant, the checks, the 2025 held-out row and
    the stop rule. It was written before any backtest run. Two amendments are recorded there, both decided before
    any backtest, from the data and a unit test:
    - the stop noise is a robust SD (1.4826 x MAD), because the sample SDs (3.4 s and 8.9 s) are dominated by
      tails;
    - the fuel trend phi is one joint value per race, because a per-compound phi is not identified.
  - `sim/tyre_model.py`: per circuit x compound degradation from strictly earlier races. Linear unless quadratic
    wins on leave-one-race-out RMSE; fallbacks circuit -> compound -> global -> none with flags. Real
    TracingInsights compounds in full mode (`compound_conflict` gives UNKNOWN), synthetic and flagged in sample
    mode. Written atomically to `models/sim/tyre_model_{sample,full}.json` only.
    - Full lake 2021-2024: 90 races; phi median -0.046 s/lap (range -0.067 to -0.040); 2024 Catalunya MEDIUM
      beta 0.071 s/lap; stop-noise SDs 0.76 s and 1.13 s.
  - `sim/race_sim.py`: seeded Monte Carlo (n=500, NumPy) with common random numbers, traffic hold, race-end
    infeasibility; about 1 ms per state.
  - `sim/whatif.py`: one decision-state builder for live and backtest; a pure-Python port of
    `streaming.priors.priors_from_frames`, equal to Spark on the sample lake; `WhatIfService` caches 8 races.
  - API: `POST /api/races/{s}/{r}/whatif` (200 / 404 / 409 / 422 / 503) and `api/lap_log.py`, fed by the SSE
    `on_sent` hook (no second subscription, so the Step 13 disconnect tests are unchanged) and a subscriber
    started by a replay start or the first what-if.
    - The lake readers gained a zone and table allowlist. The race catalog is unchanged; the what-if readers are
      `clean/{races,laps,stints,stints_tracinginsights}` and `features/{pit_loss,outlap_features}`; raw is never
      readable.
    - Setting `API_SIM_DIR` (default `models/sim`); the tyre model file is chosen per race from the catalog's
      `synthetic` flag.
  - Dashboard: `WhatIfPanel` (labelled selects, the defender follows the car ahead, keyboard run, status and alert
    lines, a captioned table with row headers, P as text plus an `aria-hidden` bar, sensitivity row group,
    model P as text, reserved height, reduced motion respected, no overflow at 375 px), `lib/whatif.ts` (guards
    and helpers).
  - Experiment: `experiments/whatif_consistency.py` -> `sim/consistency.py` + `sim/backtest.py`;
    `make whatif-consistency-{sample,full}`, `make sim-tyre-model-{sample,full}`. Only `RECORD=1` writes
    `results/` (refuses to overwrite) and opens 2025.
  - **Development run** (`make whatif-consistency-full`, 2021-2024, no 2025 read, output in
    `data/whatif/full_20261113T142444Z/`, not committed):
    - 157 labelled attempts (as in the Step 12 parity check), 155 feasible: the 2 infeasible ones have no
      pit-loss prior (the first race).
    - Mean P_sim - P_model = -0.025 (stop rule 0.20: not triggered); mean |difference| 0.138; Spearman 0.66.
    - Simulator Brier 0.185 (model 0.152 on its own training and calibration rows), ECE (10 bins) 0.090 (model
      0.058), mean P 0.295 against an observed 0.303.
    - Monotonicity in the attacker's loss: 0 of 155 states with an increase (pass). Both cars' loss is flat, as
      expected (info).
    - Determinism: 157/157 byte-identical (pass); seed + 1 changes P by at most 0.06.
    - Ordering grid: 17/18 pass/fail rows pass. **One failure**: opening at -0.5 s/lap with a 5.0 s gap,
      P_next - P_now = +0.022. The mean gap moves against the attacker (-6.10 -> -6.58 s), but the extra
      simulated lap widens the 80% interval (10.4 -> 12.1 s), and that raises a small tail probability (0.062
      -> 0.084). Reported as it is; nothing was tuned.
    - User decision (a): the pre-registered rule stays unchanged, and the failed cell is part of the result.
      - A post-hoc supplementary column was added, labelled as such and never pass/fail: `check = ordering_posthoc`,
        `post_hoc = true`, the sign of E[gap next] − E[gap now]. It is recorded in `docs/sim_spec.md`
        ("Post-hoc supplement", dated).
      - Rerun (`data/whatif/full_20261113T145605Z/`): identical pre-registered results; the expected-gap sign
        follows the closing rate in 10/10 cells (−0.479 / +0.021 / +0.521 s); the failing cell's E[gap]
        difference is −0.479 s.
  - Test fix: `test_bench_grid.py::test_subprocess_cell_ok` read the host's live cgroup and `MemAvailable`. It
    failed when the machine used more than 0.9 x 4 GiB, and it did here at 3.7 GiB. It now injects a fake cgroup
    and meminfo through `sampler_kwargs`, as its sibling test does. The guard's behaviour in real runs is unchanged
    (`src/f1sa/bench` is untouched).
  - Verified live on the host (no container changed): `make api` with replay plus `next start` on :3001; a 60x
    replay with the Playwright smoke test including the what-if steps (pass, 1.8 min); `POST …/whatif` 200 in
    about 9 ms, byte-identical body for the same seed, 409 / 404 / 422 / 404 for the bad cases.
- Step 10 follow-up (2026-11-14): StackOverflowError in 6 build cells (`build_s1_x16_{local4,cluster}`,
  `build_s5_x4_{local1,local2,local4,cluster}` of `bench_20261113T152740Z`)
  - **Root cause.** `etl/clean.select_races` OR-ed one `(season, round)` condition per race (`reduce(|, ...)`).
    The benchmark passes every replicated race to the unchanged `compute_features`, and Spark's recursive
    Column conversion overflowed at the `.filter` call (trace in `data/bench.log`). Reproduced: 264 race keys
    pass, 384 and 456 overflow.
  - **Fix.** One flat `struct(season, round) IN (...)` (an `InSet` after optimisation). Same rows, nulls match
    nothing, partition pruning is kept, no `-Xss`. The full `compute_features` now runs on the real s5_x4
    input (456 races).
  - **Same result.** `compute_features` with the old and the fixed filter on s1_x4 (96 races) and s3_x1 (68
    races): all 9 persisted feature tables have identical checksums. A rerun of `build_s1_x4_local4` /
    `build_s3_x1_local4` with the fix (no RECORD) gives identical `output_bytes`, with input and shuffle bytes
    within 0.31 %.
  - **Timing.** Both cells are faster than their recorded IQR: 54.0 s against 64.7 s [64.2, 70.6], and 43.7 s
    against 49.4 s [48.6, 51.8].
    - The cause is mostly the fix: filtering the 9 cached input tables once takes 5.38 s with the OR chain
      and 3.08 s with `IN` at 96 races, planning of the deep tree included.
    - Some environment variance is likely too (the unaffected prep cell was faster as well).
    - Cells timed with the fix are therefore not strictly comparable with the recorded ones (documented
      in `docs/experiments.md` and in the supplementary rows' `note`).
  - **Regression test.** `tests/unit/test_select_races.py`: 540 / 2000 keys of 600+ synthetic race keys,
    null keys, a flat `InSet` plan, `restrict`, and partition pruning. 4 of its 6 tests fail on the old
    implementation with StackOverflowError.
  - **Build plot.** `bench/report.plot_build_axes` draws (a) real seasons at x1 and (b)–(d) replication per
    season count, from the same data. Missing cells are marked and named; a hollow marker is a re-run.
    `make bench-replot [RECORD=1]` redraws it. Only `results/scalability_build.png` changed (the CSV, README
    and telemetry PNG were checked byte-identical).
  - **Supplementary re-run tooling (not run).**
    - `GridSpec.only` / `--only-cells` selects cells plus their prep, stored in `grid_spec.json`.
    - `--supplement-of <run>` takes the original grid's settings.
    - `make bench-supplement [RECORD=1]` merges the rows into `results/scalability.csv` with the new
      columns `rerun_of` and `note`; the failed rows stay, and the run's own `run_id` and `git_sha` are kept.
    - It refuses before running if a cell's recorded row is ok or already re-run.
    - Tests are in `tests/unit/test_bench_supplement.py` and `test_makefile_bench.py`, and the results-guard
      windows cover the new targets.
## Next
- Step 10, user: `make bench-supplement RECORD=1 SUPPLEMENT_OF=bench_20261113T152740Z CELLS="build_s1_x16_local4
  build_s1_x16_cluster build_s5_x4_local1 build_s5_x4_local2 build_s5_x4_local4 build_s5_x4_cluster"` with api and
  web stopped (about 2.3 h, allow 3 h). Then commit `results/scalability.csv`, `scalability_build.png`,
  `scalability_telemetry.png` and `README.md`, and fill the Results of `docs/experiments.md`.
- Step 15, user (ordering decision taken: rule kept, post-hoc column added): `make sim-tyre-model-full TEST_SEASON=1`
  (adds the 2025 entries; the 2021-2024 entries are unchanged by construction) and
  `make whatif-consistency-full RECORD=1` (adds the 2025 held-out row and writes `results/whatif_consistency.*`).
  Then commit the results and fill the what-if part of the report.
- Step 15, user: `make api-up` (recreates the api container with the what-if route) and rebuild the `web` image
  (`docker compose build web`) so that the containers serve the panel; `make demo-clean` removes the demo output
  of the interrupted verification replay.
- S16: UI latency from `results/ui_latency.csv` (needs a dashboard run with `ENABLE_DEBUG=1`).
- User: run `docker compose exec namenode hdfs dfs -setrep -R -w 1 /f1`, then
  `docker compose exec namenode hdfs fsck /f1 | grep -i under-replicated` (expect 0). This changes metadata only;
  no bytes or checksums change.
- User: commit Step 9e and `git tag model-spec-v2` (the v2 freeze). Then, only when the user says so,
  `make model-test-full CONFIRM=yes` (opens 2025 once per feature set and setting: 6 runs).
- Step 16 `experiments/ablation.py`: the paired race-cluster bootstrap intervals of the feature-set differences
  (`docs/model_spec_v2.md` §4; deferred here from the 9f summary). Method: score 2025 with the SAVED frozen
  test-run models (`models/undercut/full_{real,real_tyre,real_ti}_G*_k*`, no retraining, no re-tuning, no
  re-calibration). First check the point metrics reproduce the committed `results/model_eval*_G*_k*.csv` values
  EXACTLY, per model and setting; on any mismatch stop and report, and write no interval. Then compute the paired
  intervals (same resampled races for both sets, 1000 resamples, seed 42) for PR-AUC and Brier. The 2025 missing
  rates are already reported in `docs/model_results.md`.
  `race_control` is not a model input yet.
- User: the FastF1 pass (needs the F1 TV login) adds exact track status, real stints and telemetry. The clean job
  already handles them (tested on hand-made FastF1 Parquet).
- Re-check the 74% out-lap stop-time share per circuit, and the heuristic thresholds (1.10 / 50%) on wet races,
  using 2021-2024 only.
- Overnight run (user): first `docker compose stop api web` (the full profile's limits are about 15 GiB with
  the 6 GiB app container, above the 14 GB VM; with api and web stopped about 12.5 GiB), then
  `make experiments-spark RECORD=1 GRID_ARGS="..."` (below); afterwards `docker compose start api web`.
- User (overnight, nothing else running): `make experiments-spark RECORD=1 GRID_ARGS="--seasons 1 3 5
  --factors 1 4 16 --masters local1 local2 local4 cluster --warmup 1 --repeats 3 --large-from 16
  --large-repeats 3 --large-masters local4 cluster --large-seasons 1"` (~6.3 h + ~0.5 h partitioning,
  estimate). The full grid (no GRID_ARGS) is ~119 h. Then commit `results/scalability.*`,
  `results/partitioning.*` and `results/README.md` right away, and fill the Results of `docs/experiments.md`.
- Optional, after the record run: stop api and web for it (`docker compose stop api web`) to give the
  benchmark the memory headroom; that is a service stop and needs a decision first.
- After committing Step 9c, optionally run `make model-save-full FORCE=1` so that `git_dirty` is false in the
  deployment metadata.
- S16: rerun `uv run python experiments/redis_latency.py` with only the core profile up (`make down` and then
  `make up`) and compare it with the Step 11 run (see Known issues).
- S16: streaming latency and throughput (#35, #36) from `make stream-demo` on the core profile only, with the
  run-to-run variance; S13 API reads the Redis state and SSE-forwards the events channel.
## Known issues
- Step 10 follow-up: the 20 recorded ok build cells (telemetry cells do not use the filter) were timed with the
  old OR-chain filter, while the
  supplementary rows will be timed with the flat `IN`. That is about 10 s per run faster at 96 races and grows
  with the race count, so the supplementary points are not strictly comparable with their neighbours. A full
  re-run of the grid would remove the mix; it was not done (hours).
- Step 15: the ordering check fails in the far tail (opening car, 5 s gap). One more simulated lap adds variance,
  so a small P can rise while the mean gap falls. The pre-registered rule compares P, not the mean gap, and was
  kept (user decision); the post-hoc expected-gap sign is reported alongside it.
- Step 15: the tyre target is relative to each driver's race median, so per-driver offsets stay in the residual
  SD. The noise prior (about 1 s) is larger than real lap-to-lap noise; live states shrink it with the driver's
  own laps (n0 = 3).
- Step 15: traffic is third cars only, projected without stops; A and D are not traffic for each other.
- Step 15: the lap log starts when a replay starts through the API, a dashboard connects or the first what-if
  arrives. Pub/Sub has no history, so a what-if after a mid-race connect has fewer laps (`history: none` and a
  warning when there are none).
- Step 15: the model P shown next to the simulator is the stream's latest evaluated decision for the attacker,
  which may be one lap behind lap C. The new compound after a stop follows a fixed rule (S->M, M->H, H->M).
- Step 10: the grid estimate is an affine fit through two points (x1, x4 at local[4]) applied to every
  master. Costs that grow faster than the rows (single-partition prior windows, many small partitions,
  spills) are not in it, and single-run smoke times vary by about 20 % (local[1] x1: 52.7, 40.1, 44.5 s).
- Step 10: x16 local cells will probably be killed by the guard (x4 already peaked at 3.55 GiB of the
  3.6 GiB guard). That is recorded as `aborted_memory_guard`, not hidden. Executor memory on the workers
  is not guarded: an executor OOM shows as `failed`.
- Step 10: the build at x1 is overhead-dominated (about 25 s fixed per run): local[4] is only 1.3x faster
  than local[1]. Per-core scaling shows only at larger factors.
- Step 10: `docker stats --no-stream` takes 1-2 s per call, so container peaks are 2 s samples and can
  miss short spikes. The in-container sampler (0.5 s) is the finer one.
- Step 10: `input_bytes` is what Spark reports: reads from a cached DataFrame count as input, and file://
  and hdfs:// readers report differently. Compare bytes within one file system and variant.
- Step 10: if the host script dies, `docker compose exec` may leave the grid running in the container.
  A `RESUME=` while it still runs is refused ("another grid is running", flock on the run folder);
  stop it (`docker compose exec app pkill -f f1sa.bench.grid`) or let it finish. Cells orphaned by a
  hard-killed grid are killed by the resume itself.
- Step 14: ESLint 9.39.5 is marked "no longer supported" by npm. ESLint 10 cannot be used until
  `eslint-plugin-react`, `-jsx-a11y` and `-import` (pulled in by `eslint-config-next` 16.4.5) accept it. It
  is a lint-time tool only.
- Step 14: the gap chart shows only laps received since the page connected (Pub/Sub has no history). After
  a reload in mid-race the chart starts at the current lap and fills from there.
- Step 14: `NEXT_PUBLIC_API_URL` is inlined at build time, so pointing the container at another API means
  rebuilding the `web` image. Other dashboard origins must be added to `API_CORS_ORIGINS`.
- Step 14 follow-up: the replay control still polls `GET /api/replays` (1 s while running, 5 s otherwise),
  because replay status (starting/failed) is not on the race channel. The race state itself is never
  polled.
- Step 14 follow-up: a snapshot is taken right after the leader's lap event. Redis keeps changing during
  the lap (later cars' batches), so the UI is exact at each snapshot and event-driven in between (measured
  UI = Redis on 26/27 laps when read right after).
- 2026-11-12: Docker Desktop's host port forwards for Redis (6379) and Kafka (9092) broke after a failed
  re-expose of port 3000 ("/forwards/expose returned unexpected status: 500"). TCP connects but the
  protocol is reset; containers reach both over the compose network. Host-side integration tests need
  the forwards back: restart the redis and kafka containers or Docker Desktop.
- Step 14: three-letter codes come from the surname in the Jolpica id (`max_verstappen` gives VER), not
  from the official codes, so a few differ (`mick_schumacher` gives SCH, officially MSC).
- Step 14: the web image is about 400 MB (the Debian slim Node base); a distroless runtime could shrink it.
- Step 13 follow-up: a replay of 2025 needs `make api-up TEST_SEASON=1`. The demo script
  (`make stream-demo`) still has no `--allow-test-season` (unchanged `streaming/demo.py`).
- Step 13 follow-up: a replay peaks at about 1.5 GiB (two JVMs) of the api container's 2 GiB. An OOM kill
  would take the API process down with it.
- Step 13 follow-up: demo outputs and replay checkpoints build up until `make demo-clean CONFIRM=yes`, which
  must not be run during a demo or a replay.
- Step 13: Pub/Sub has no history. An SSE client sees only events published after it subscribed;
  `Last-Event-ID` is ignored, and a reconnecting dashboard must reload `/state`. A client that connects after
  `session_end` gets heartbeats until it disconnects.
- Step 13: with ASGI spec ≥ 2.4 (uvicorn), a client disconnect is noticed at the next write, so the
  subscription can outlive the client by up to one heartbeat (15 s).
- Step 13: a replay only feeds Kafka. The dashboard sees changes only while a stream job runs
  (`make stream-demo` starts its own producer, so it does not use the API replay).
- Step 13: the latency join and the duplicate check live in the memory of one API process (the last
  10,000 events). After a restart, earlier event ids give 404. Run a single uvicorn worker.
- Step 13: memory limits for the full profile now add up to about 10 GiB. Until WSL is raised to 14 GB they
  exceed the ~9 GiB VM (limits are ceilings, not reservations).
- Step 13: the shutdown path that terminates a running replay is tested in unit tests only. The container
  logs of the recreate that exercised it live were replaced by the new container.
- Step 12: `track_neutral`, `track_red_flag` and `race_laps` on the lap events are hindsight values (whole-race
  heuristic, winner's lap count); live they would come from race control and the scheduled distance.
- Step 12: `redis_written_at_ms` is the client time just before the batch's single pipeline is sent (a PUBLISH
  in the pipeline cannot carry its completion time); the write duration is in `results/stream_batches.jsonl`.
  The first batch of a race also loads its priors (Spark), so p99 latency includes ~2-3 s once per race.
- Step 12: a car awaited by a decision (lapped car, or a missing car ahead) counts as retired after 2 leader
  lap times; driver state survives restarts only through `driver_state/state.json` in the same checkpoint.
- Redis latency varies from run to run on this WSL2 / Docker Desktop host.
  - An earlier run in the same session met every schedule: 60.00/s with 0 late ticks; write p99 3.0 ms at 60/s,
    HGETALL p99 0.84 ms.
  - That run was discarded only because its redis-cli step covered 1 s instead of 30. Its redis-py numbers were
    valid.
  - The recorded run fell behind at 60/s: 51.09/s achieved, 2876 late ticks, write p99 19.8 ms. It also had
    p99 outliers at 10/s.
  - The full profile (HDFS and Spark) was running during both runs. Report the S16 number from a core-only
    run, with the variance stated.
- The benchmark is a single-client, sequential loop: write, then reads, in one thread. At 60/s each tick spends
  about 5.5 ms (first run) to 11.4 ms (recorded run) at the medians on its 22 round trips, out of a 16.7 ms
  period. Latency spikes therefore make ticks late, and the achieved rate measures the client loop as well as
  Redis.
- `redis-cli --latency` measures inside the container, so it is a floor with no host or Docker network hop.
  The redis-py numbers include the Docker Desktop port forward.
- The v2 selection rule says "calibrated LR", but the LOSO rows it was applied to are raw LR at the first grid
  point (no per-fold calibrator). PR-AUC is unchanged by a strictly increasing calibration map. Disclosed in
  `docs/model_spec_v2.md` §2.
- The v2 playbook (12 Nov 2026) that states the selection rule is not in the repo. The spec cites it.
- GBT is sensitive to row order. Fixing the order of the TracingInsights sets moved `real_ti` GBT validation
  PR-AUC by 0.043 at G=3.5, which is larger than the gaps between some feature sets in the tables. `real` is
  reproducible run to run (its plan gives a stable order), but its GBT numbers also hold for one row order only.
  Worth noting before any model is chosen from GBT validation numbers.
- LOSO in the comparison tables uses raw scores at the fixed first grid point (as in Step 9b), not the calibrated
  models of the validation columns.
- `real_ti`: every TracingInsights input is present on every labelled attempt in 2021-2024, so its `_missing`
  columns are constant 0 in development. The fill path is exercised only by the unit tests. 2025 attempt-level
  missing rates are unknown until the spec v2 test run (`results/missing_rates_*_G*.csv`).
- The categorical missing indicator is named `__unknown`, not `_missing`. Renaming it would change `real`'s
  columns and the deployed `scorer.json`.
- TracingInsights (Step 4C):
  - Two races are below 95% compound coverage outside 2025: 2021 R12 (Belgium, 3 laps behind the SC, every
    compound UNKNOWN) and 2022 R11 (Austria, 77.4%). In 2025, 2 races are below 95%: R6 Miami (55.6%) and R13
    Belgium (93.5%).
  - 190 telemetry laps are `partial` and 1 is `none` (2021 R8) across all seasons.
  - Telemetry aggregation takes per-lap files on the session clock (lap start + time in lap). A sample outside every
    lap window is dropped and counted (`n_samples_outside_window` in the meta).
  - `telemetry_lap_real` has 221 more rows than Jolpica laps (2021-2025). Each is an uncompleted lap with a null TI lap
    time (see "Lap reconciliation" in `docs/data_dictionary.md`). Later feature steps must left-join TI onto Jolpica
    laps.
  - The data are MIT (2021-2024) or Apache-2.0 (2025) licensed as repos only. Neither covers the underlying F1/FOM
    data (see DATA_LICENSES.md).
- The 1,936 old under-replicated blocks remain until the user runs `setrep` (see Next).
- The deployment metadata records `git_dirty: true`, because it was saved before the Step 9c code was committed.
  The sha (36c369b) is the parent commit. The model itself is unaffected (it is the same frozen procedure, and the
  reproduction check matched).
- Wet running is excluded as `neutralised` in full mode (compounds are null, so `wet` cannot fire). Kept on
  purpose; the reporting split estimates the share. See `docs/label_definition.md` "Known limitations".
- At L=5 `gap_ahead_change_3l` uses the lap-1 gap (standing start). L=4 is now excluded as `early_lap`.
- The app container does not come back after a Docker Desktop/WSL restart (exit 128, not OOM; seen twice). The
  scratchpad in /tmp is wiped at the same time.
- Sample size is small: 108 labelled training attempts at G=3.5, k=2 (184 at G=5.0, k=3). Development
  validation numbers (49 attempts, 18 races) have wide race-cluster CIs, and 2024 is used for both selection and
  calibration. The ECE bootstrap interval can sit above the point value (resampling duplicates bins).
- LOSO uses raw class-weighted scores, so its Brier and ECE are not comparable with the calibrated validation
  numbers.
- `make test-int` still treats pytest exit 5 (no tests collected) as success. That can only happen now if the
  integration tests are deselected.
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
- Jolpica records pit entries under a red flag (Step 8b), so inferred stints split there. The red-flag lap and the
  3-lap restart period are excluded as neutralised.
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
- Uncommitted files in `results/` can be deleted by the editor's Source Control "Discard Changes" (it runs
  `git clean -f` on untracked files, irreversibly on WSL). That is how the first 9f `real` results were lost
  (see 9f approval). Commit run outputs right after a run, and never discard changes under `results/`.
  Optionally, in the editor's user settings, enable `git.discardUntrackedChangesToTrash` where a trash exists.
- `tests/conftest.py` fails any pytest session that deletes, overwrites or adds a file in the repo's `results/`
  (stat snapshot at start and end, contents never read). A make target writing `results/` at the same time as
  the tests also trips it: run the tests after the target finishes.

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
`uv run pytest tests/unit/test_full_report.py tests/unit/test_makefile_full.py tests/unit/test_features_build.py tests/unit/test_lineage.py tests/unit/test_etl_helpers.py tests/unit/test_undercut.py tests/unit/test_neutral_split.py -q`.

Step 9: `make etl-sample && make features-sample && make model-eval-sample`, then
`uv run pytest tests/unit/test_model_dataset.py tests/unit/test_model_pipeline.py tests/unit/test_calibrate.py tests/unit/test_metrics.py tests/unit/test_explain.py -q`.

Step 9c (needs `make up-full`; the app must have the `./models` mount): `make model-save-full` (add `FORCE=1` if
the folder exists), then
`uv run pytest tests/unit/test_model_deploy.py tests/unit/test_lr_scorer.py tests/unit/test_makefile_full.py -q`.

Step 4C Phase 2 (needs `make up-full`): `make ingest-ti-full` (resumable; `SKIP_RAW_TELEMETRY=1` skips the 2024 raw
samples), then `make etl-ti-full`, then `make test-ti` and
`docker compose exec namenode hdfs dfs -stat %r /f1/clean/stints_tracinginsights/season=2024/round=5/*` (expect 1).

Step 4C wrap-up (needs `make up-full`): `make reconcile-ti-full` (read-only, expect `result: PASS`), then `make test-ti`.

Step 4C → model (needs `make up-full`): `make model-dev-tyre-full model-dev-ti-full model-dev-permutation-ti-full`, then `uv run pytest tests/unit/test_permutation_dev.py -q` and
`uv run pytest tests/unit/test_model_ti_inputs.py tests/unit/test_model_pipeline.py tests/unit/test_makefile_full.py -q`.
Determinism: run those three make targets twice and compare `sha256sum` of the 42 files with
`data/dev_ti_tyre_reference.sha` (expect all OK); `make model-dev-full` then `sha256sum -c data/real_dev_committed.sha`.

Step 12 (needs `make up`; `make up-full` for FULL=1 and parity): `make stream-demo SEASON=2024 ROUND=5 SPEED=10`
(add `FULL=1` for HDFS), `make stream-parity-full` (expect `mismatching races: 0`), then
`uv run pytest tests/unit/test_stream_parity.py tests/unit/test_live_features.py tests/unit/test_stream_job.py -q`
and `uv run pytest tests/integration/test_streaming.py -q -m integration`.

Step 13: `make test-api` (API tests plus `test_demo_outputs.py`), then
with `make up`: `uv run pytest tests/integration/test_api.py -q -m integration`, `curl -s localhost:8000/api/health`,
`curl -s localhost:8000/api/races`, and `curl -N localhost:8000/api/races/2024/5/events` during
`make stream-demo SEASON=2024 ROUND=5 SPEED=60` (writes `data/demo/<run_id>/`; `results/` unchanged). Full lake:
`make api-up FULL=1 REPLAY=1`, POST a replay, then `curl -s localhost:8000/api/replays` until `finished`. On the host:
`make api` (port 8001) and the same curls.

Step 14 (needs `make up`): `make web-test`, then `make api-up REPLAY=1`, then either `docker compose stop web && make web-e2e`
or `cd web && E2E_NO_SERVER=1 npx playwright test` against the container, and finally `git status --short results/` (expect
empty). Python: `uv run pytest tests/unit/test_api_races.py tests/unit/test_api_health.py tests/unit/test_makefile_web.py -q`.


Step 15: `make sim-tyre-model-sample && make whatif-consistency-sample`, then
`uv run pytest tests/unit/test_sim_tyre_model.py tests/unit/test_race_sim.py tests/unit/test_whatif.py tests/unit/test_lap_log.py tests/unit/test_api_whatif.py tests/unit/test_sim_consistency.py tests/unit/test_makefile_sim.py -q`
and `make web-test`. Full lake (needs `make up-full`): `make sim-tyre-model-full && make whatif-consistency-full`.
Live: `ENABLE_REPLAY=1 make api`, then `curl -X POST localhost:8001/api/races/2024/5/whatif -H 'content-type: application/json' -d '{"attacker":"<P4>","defender":"<P3>"}'` during a replay.

## 9f approval
- 2026-11-13: label-definition-v1, model-spec-v1 and model-spec-v2 approved for the single 2025 test run by Parinith D. on behalf of the team (Data Wranglers). No further sign-off required.
- 2026-11-12 21:02 UTC: first 9f attempt aborted. The app container went down overnight mid-run. No results files were written and no 2025 test numbers were seen. The partial model folder is kept as models/undercut/_aborted_20261112T2102Z_full_real_G3.5_k2 and its log as data/test_run_aborted_20261112.log (both gitignored). Rerun allowed under the rerun rule (crash with no test number seen).
- 2026-11-13: 9f bug in the test run, fixed before anyone viewed the `real` test results.
  - What happened: `make model-test-full CONFIRM=yes` completed the two `real` runs. Then the first
    ablation run (`--source full --allow-test-season --feature-set real_tyre`) stopped with "TracingInsights
    inputs need explicit races or seasons".
  - Where: inside `pipeline.load_frame`, **before any `real_tyre`/`real_ti` data was read, and before any
    training or evaluation**. No `real_tyre`/`real_ti` test files exist (checked by file name only).
  - Root cause: test mode sent the TracingInsights sets to `dataset.load_attempts(races=None, ti_root)`,
    which refuses TracingInsights inputs without races. The dev path (`--no-test`) calls
    `load_attempts_for_seasons` with explicit seasons, so it never hit this.
  - Fix (plumbing only, `src/f1sa/model/pipeline.py`):
    - test mode with a TracingInsights set now calls the dev path's loader (`load_attempts_for_seasons`,
      through the shared `_load_seasons`) for the seasons of the frozen split;
    - that split is `pipeline.TEST_SPLIT = SplitSpec()`: train 2021-2023, validation 2024, test 2025, the
      split `build_dataset(split=None)` already uses.
  - Unchanged: the `real` path (still `load_attempts(features, None, None)`), features, models, grids,
    calibration, threshold, metrics, `scorer.json` and every spec doc.
  - Tests (`tests/unit/test_model_pipeline.py`):
    - `test_test_mode_ti_sets_load_tracinginsights_for_the_frozen_split[real_tyre|real_ti]` checks that
      the TracingInsights loader receives seasons 2021-2025 and nothing raises. It fails on the old code
      at the same call.
    - `test_test_mode_real_path_is_unchanged` and `test_test_split_is_the_frozen_split`.
  - The existing tests confirm that `make model-test-full CONFIRM=yes FEATURE_SET=real_tyre` runs only
    `real_tyre` at both settings (`make -n`), that its own files block a rerun, and that the `real` files
    neither block it nor change.
  - Dev outputs are byte-identical: `make model-dev-tyre-full model-dev-ti-full` (2021-2024 only), then
    `sha256sum -c data/dev_ti_tyre_reference.sha` gave 42/42 OK.
  - No `--allow-test-season` command was run, and no protected test-run file was opened, read or hashed.
    The `real_tyre`/`real_ti` test runs are for the user to start.
- 2026-11-13: 9f `real` results files (results/*_G3.5_k2.*, *_G5.0_k3.*) from the 04:38 UTC run were deleted from the working tree before being committed (cause under investigation; not a user command). Nobody had viewed them. The saved models and metadata survived (kept as models/undercut/_lostresults_20261113T043[89]Z_full_real_*). The `real` runs were regenerated with the same frozen code; the new metadata.json files and the filtered console output are IDENTICAL to the first run (checked key by key and line by line without printing values). The regenerated files are therefore the first run's results; committed in b6aae52. Ablation results (real_tyre, real_ti) were not affected and were committed separately.
- 2026-11-13: cause of the lost 9f `real` results found; it was not repo code, a test or a make target.
  - Evidence: the editor's git log (`~/.vscode-server/data/logs/20261113T043245/exthost1/vscode.git/Git.log`, line 146)
    has `2026-11-13 04:51:21.551 [info] > git clean -f -q -- results/calibration_G3.5_k2.csv ... results/reliability_G5.0_k3.png`,
    exactly the 12 lost files. This is the command the Source Control view runs for "Discard Changes" on untracked
    files (after a confirmation dialog). `remoteagent.log` logs a watched path deleted at 04:51:21.655.
  - Why only those files: at 04:51:21 they were the only untracked files and no tracked file was modified;
    `models/` is gitignored, so `git clean` (without `-x`) left `models/undercut/full_real_*`; the `real_tyre`/`real_ti`
    files did not exist yet (written 05:23-05:28).
  - Ruled out: the bug-fix session ran no delete (its 04:51:15 `git status` still listed the 12 files); every writer
    in `src/`/`experiments/` uses `check_results_dir` and tmp+replace on its own names; the only deleter
    (`demo_runs.clean`) is pinned to `./data/demo` and `./data/checkpoints`; every test and module fixture works in a
    tmp dir; no Makefile recipe deletes, moves or cleans; `git fsck` shows no dropped stash from 11-13.
  - The earlier line "cause under investigation; not a user command" is corrected by this one: it was an editor action.
  - Safeguards added (tests only; no pipeline, label, Redis or layout change): the session-wide results guard in
    `tests/conftest.py` (`tests/_results_guard.py`), and `tests/unit/test_results_untouched.py`, which runs the six
    test-mode commands (refused, nothing touched) and the `real_tyre`/`real_ti` dev runs (only their own files
    change) against a placeholder copy of `results/`, and checks that no Makefile recipe or window target deletes or moves.
- 2026-11-13: 9f 2025 summary written: `results/model_test_summary.csv` and `docs/model_results.md`.
  - Copied from the committed test-run outputs (`model_eval*_G*_k*.csv`, `missing_rates_*`). Nothing was
    retrained, rerun or re-evaluated. Baselines were checked identical across the three feature-set files.
  - Primary (`real` calibrated LR, G=3.5 k=2): PR-AUC 0.670 [0.421, 0.898] vs rule 0.513 [0.246, 0.799];
    Brier 0.177 vs 0.273. The point estimates favour the primary model on every metric, all intervals overlap,
    and no significance claim is made.
  - Open (spec v2 §4): paired race-cluster bootstrap intervals for the feature-set differences are not in any
    committed output (no per-attempt test scores are saved). They need a separate, agreed step; until then no
    ablation difference is interpreted.
- 2026-11-13: the 9f summary is reproducible: `make model-test-summary` (`experiments/model_test_summary.py`,
  logic in `src/f1sa/model/test_summary.py`, tests in `tests/unit/test_model_test_summary.py`).
  - It reads only the six committed `results/model_eval*_G*_k*.csv` files (refuses with exit 2 if one is
    untracked or differs from HEAD). It writes `results/model_test_summary.csv` and the body of the `## Results`
    section of `docs/model_results.md`, and leaves the rest of the doc as it is.
  - Rerun on the committed inputs: both outputs were byte-identical to the hand-run versions (sha256 `57a8e546…`
    for the CSV, `39ecd4b8…` for the doc), and identical again on a second run.
  - Decision: the paired ablation intervals (`docs/model_spec_v2.md` §4) are deferred to Step 16. They will be
    computed from the saved frozen test-run models, after an exact-match check against the committed model_eval
    metrics (see Next). Until then no ablation difference is interpreted.
