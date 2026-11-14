.DEFAULT_GOAL := help
SHELL := /bin/bash
UV ?= uv

.PHONY: help setup lint typecheck test test-int sample-data etl-sample features-sample model-eval-sample model-eval-rehearsal

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Install Python deps (incl. dev extras) into .venv
	$(UV) sync --all-extras

lint: ## Ruff lint + format check
	$(UV) run ruff check src tests experiments
	$(UV) run ruff format --check src tests experiments

typecheck: ## mypy (strict) on src
	$(UV) run mypy src

test: ## Unit tests
	$(UV) run pytest tests/unit -q

sample-data: ## Validate data/sample offline, or build it if missing (REBUILD=1 forces a rebuild)
	$(UV) run python -m f1sa.ingest.sample_builder $(if $(REBUILD),--rebuild,)

etl-sample: ## Clean zone from data/sample into data/lake/clean (file mode, Spark local)
	DATA_LAKE_URI=file://$(CURDIR)/data/lake $(UV) run python -m f1sa.etl.clean --source sample

features-sample: ## Feature tables from data/lake/clean into data/lake/features (run etl-sample first)
	DATA_LAKE_URI=file://$(CURDIR)/data/lake $(UV) run python -m f1sa.features.build --source sample

model-eval-sample: ## Model smoke test on the sample features (no test season; outputs *_sample, SMOKE TEST)
	DATA_LAKE_URI=file://$(CURDIR)/data/lake $(UV) run python experiments/model_eval.py --source sample

model-eval-rehearsal: ## Whole model pipeline on GENERATED data (outputs *_rehearsal, never reads the lake)
	$(UV) run python experiments/model_eval.py --source rehearsal

test-int: ## Integration tests (needs infra); exit 5 = none collected is OK
	$(UV) run pytest tests/integration -q -m integration; rc=$$?; \
	if [ $$rc -eq 5 ]; then echo "no integration tests collected yet"; exit 0; fi; exit $$rc

# ---------------------------------------------------------------- infrastructure (Docker Compose)
COMPOSE := docker compose
CORE_SERVICES := kafka redis app api web
FULL_SERVICES := $(CORE_SERVICES) namenode datanode httpfs spark-master spark-worker-1 spark-worker-2

.PHONY: up up-full down ps logs nuke

up: api-dirs ## Start core infra (kafka, redis, app, api, web) and wait until healthy
	$(COMPOSE) --profile core up -d --build
	scripts/wait_for.sh -t 240 $(CORE_SERVICES)

up-full: api-dirs ## Start core + hdfs + spark and wait until healthy
	$(COMPOSE) --profile full up -d --build
	scripts/wait_for.sh -t 300 $(FULL_SERVICES)

down: ## Stop and remove all infra containers (keeps volumes)
	$(COMPOSE) --profile full down

ps: ## Show infra container status
	$(COMPOSE) --profile full ps -a

logs: ## Tail infra logs (S=<service> for one service)
	$(COMPOSE) --profile full logs --tail 100 -f $(S)

# ---------------------------------------------------------------- full data (HDFS mode, inside compose)
# Runs in the already-running app container: no build, no restart. /f1/raw stays hadoop-owned, so
# HDFS itself refuses writes to it from the app user.
HDFS_LAKE := hdfs://namenode:8020/f1
APP_EXEC := $(COMPOSE) exec -T -e PYTHONPATH=/workspace/src -e DATA_LAKE_URI=$(HDFS_LAKE) app

.PHONY: hdfs-dirs etl-full features-full report-full pipeline-full

hdfs-dirs: ## Create /f1/clean and /f1/features owned by the app user (idempotent)
	$(COMPOSE) exec -T namenode hdfs dfs -mkdir -p /f1/clean /f1/features
	$(COMPOSE) exec -T namenode hdfs dfs -chown f1sa /f1/clean /f1/features

etl-full: hdfs-dirs ## Clean zone from /f1/raw into /f1/clean (needs make up-full)
	$(APP_EXEC) python -m f1sa.etl.clean --source lake

features-full: hdfs-dirs ## Features from /f1/clean into /f1/features + results/undercut_counts_full.csv
	$(APP_EXEC) python -m f1sa.features.build --source lake --full-counts

report-full: ## Full-data quality report into results/ (reads HDFS, writes nothing there)
	$(APP_EXEC) python -m f1sa.etl.full_report

pipeline-full: etl-full features-full report-full ## etl-full, features-full, then report-full

# ---------------------------------------------------------------- TracingInsights (Step 4C)
# ingest-ti-full runs on the HOST: files go through HttpFS (HDFS_HTTPFS_URL) as in Step 4A, and the
# per-lap telemetry aggregates are computed with a local Spark session before upload. Resumable
# (data/manifest_tracinginsights.json); SKIP_RAW_TELEMETRY=1 keeps no raw samples (default: 2024).
.PHONY: ingest-ti-full etl-ti-full reconcile-ti-full test-ti
ingest-ti-full: ## TracingInsights tables + telemetry aggregates for 2021-2025 into /f1/raw (host -> HttpFS)
	DATA_LAKE_URI=$(HDFS_LAKE) uv run f1sa-ingest-ti --seasons 2021-2025 --sink hdfs$(if $(SKIP_RAW_TELEMETRY), --skip-raw-telemetry,)

etl-ti-full: hdfs-dirs ## Additive TracingInsights clean tables in /f1/clean + results/tracinginsights_coverage_full.csv
	$(APP_EXEC) python -m f1sa.etl.clean_tracinginsights

reconcile-ti-full: ## Read-only TracingInsights vs Jolpica lap reconciliation (counts only, writes nothing)
	$(APP_EXEC) python -m f1sa.etl.reconcile_ti

test-ti: ## TracingInsights unit tests (invented fixtures, no network)
	uv run pytest tests/unit/test_tracinginsights_parse.py tests/unit/test_tracinginsights_loader.py tests/unit/test_telemetry_agg_ti.py tests/unit/test_clean_tracinginsights.py tests/unit/test_hdfs_replication.py tests/unit/test_reconcile_ti.py -q

.PHONY: model-dev-full
model-dev-full: ## DEVELOPMENT model run on the full features (2021-2024 only; 2025 never loaded)
	$(APP_EXEC) python experiments/model_eval.py --source full --no-test
	$(APP_EXEC) python experiments/model_eval.py --source full --no-test --g 5.0 --k 3

.PHONY: model-dev-ti-full
model-dev-ti-full: ## DEVELOPMENT run of feature set real_ti (TracingInsights tyre + telemetry; 2021-2024 only)
	$(APP_EXEC) python experiments/model_eval.py --source full --no-test --feature-set real_ti
	$(APP_EXEC) python experiments/model_eval.py --source full --no-test --feature-set real_ti --g 5.0 --k 3

.PHONY: model-dev-tyre-full model-dev-permutation-ti-full
model-dev-tyre-full: ## DEVELOPMENT run of feature set real_tyre (real + TracingInsights compound/tyre age; 2021-2024 only)
	$(APP_EXEC) python experiments/model_eval.py --source full --no-test --feature-set real_tyre
	$(APP_EXEC) python experiments/model_eval.py --source full --no-test --feature-set real_tyre --g 5.0 --k 3

model-dev-permutation-ti-full: ## DEVELOPMENT permutation importance of LR and GBT (calibrated) for real_ti; needs model-dev-ti-full
	$(APP_EXEC) python experiments/model_dev_permutation.py --feature-set real_ti
	$(APP_EXEC) python experiments/model_dev_permutation.py --feature-set real_ti --g 5.0 --k 3

.PHONY: model-save-full
# Recursively expanded: git runs only when the target is used (the app container has no git).
GIT_SHA = $(shell git rev-parse HEAD 2>/dev/null)
GIT_DIRTY = $(shell if [ -n "$$(git status --porcelain 2>/dev/null)" ]; then echo true; else echo false; fi)
model-save-full: ## Deployment model (frozen spec, G=3.5 k=2, 2025 never read) into models/; FORCE=1 replaces
	@$(COMPOSE) exec -T app test -w /workspace/models || { echo "Refusing: /workspace/models is not writable in the app container; it needs the ./models mount ('docker compose up -d app' recreates only the app container)." >&2; exit 1; }
	$(COMPOSE) exec -T -e PYTHONPATH=/workspace/src -e DATA_LAKE_URI=$(HDFS_LAKE) -e F1SA_GIT_SHA=$(GIT_SHA) -e F1SA_GIT_DIRTY=$(GIT_DIRTY) app python experiments/model_save.py --source full$(if $(FORCE), --force,)

.PHONY: model-test-full
# docs/model_spec_v2.md: real (primary), real_tyre and real_ti (ablation). FEATURE_SET=<set> runs one
# set (crash recovery under the rerun rule); real's two commands are those of docs/model_spec.md.
TEST_FEATURE_SETS := real real_tyre real_ti
FEATURE_SET ?=
TEST_RUN_SETS = $(if $(FEATURE_SET),$(FEATURE_SET),$(TEST_FEATURE_SETS))
test_fs_flag = $(if $(filter real,$(1)),, --feature-set $(1))
define test_season_runs
	$(APP_EXEC) python experiments/model_eval.py --source full --allow-test-season$(call test_fs_flag,$(1))
	$(APP_EXEC) python experiments/model_eval.py --source full --allow-test-season$(call test_fs_flag,$(1)) --g 5.0 --k 3

endef
model-test-full: ## THE single test-season run (docs/model_spec_v2.md): 3 feature sets x 2 settings, opens 2025; CONFIRM=yes [FEATURE_SET=]
	@if [ "$(CONFIRM)" != "yes" ]; then echo "Refusing: this evaluates the 2025 test season once per feature set and setting (docs/model_spec_v2.md). Re-run as 'make model-test-full CONFIRM=yes'." >&2; exit 1; fi
	@if [ -n "$(filter-out $(TEST_FEATURE_SETS),$(FEATURE_SET))" ] || [ "$(words $(FEATURE_SET))" -gt 1 ]; then echo "Refusing: FEATURE_SET must be one of: $(TEST_FEATURE_SETS) (docs/model_spec_v2.md)." >&2; exit 1; fi
	$(foreach fs,$(TEST_RUN_SETS),$(call test_season_runs,$(fs)))

.PHONY: model-test-summary
model-test-summary: ## 2025 test summary from the COMMITTED 9f files (no evaluation): results/model_test_summary.csv + Results tables in docs/model_results.md
	$(UV) run python experiments/model_test_summary.py

nuke: ## DESTRUCTIVE: remove containers AND volumes (requires CONFIRM=yes)
	@echo "WARNING: this deletes all f1sa volumes (Kafka log data, HDFS namenode/datanode data)."
	@if [ "$(CONFIRM)" != "yes" ]; then echo "Refusing: re-run as 'make nuke CONFIRM=yes'." >&2; exit 1; fi
	$(COMPOSE) --profile full down -v --remove-orphans

# ---------------------------------------------------------------- streaming (Step 12)
# stream-demo never starts, stops or rebuilds services: it needs `make up` (kafka, redis) already
# running. Sample mode runs producer, stream job and subscriber on the host (the first run fetches
# the pinned Kafka connector into data/cache/ivy); FULL=1 runs all three in the app container.
# Outputs go to data/demo/<run_id>/; only RECORD=1 (the Step 16 recorded run) writes to results/.
SEASON ?= 2024
ROUND ?= 5
SPEED ?= 10
STREAM_DEMO_ARGS = --season $(SEASON) --round $(ROUND) --speed $(SPEED)$(if $(filter 1,$(RECORD)), --record,)

.PHONY: stream-demo stream-parity-full
stream-demo: ## Replay one race: Kafka -> Streaming -> Redis, outputs in data/demo/<run_id> (SEASON ROUND SPEED [FULL=1] [RECORD=1 -> results/])
ifeq ($(FULL),)
	@test -d data/lake/features/pit_loss -a -d data/lake/clean/laps || { echo "Missing data/lake: run 'make etl-sample features-sample' first." >&2; exit 1; }
	DATA_LAKE_URI=file://$(CURDIR)/data/lake $(UV) run python experiments/streaming_latency.py $(STREAM_DEMO_ARGS)
else
	$(APP_EXEC) python experiments/streaming_latency.py $(STREAM_DEMO_ARGS)
endif

stream-parity-full: ## Batch vs stream feature parity over 2021-2024 (app container, HDFS lake; 2025 never read)
	$(APP_EXEC) python experiments/stream_parity.py --seasons 2021-2024

# ---------------------------------------------------------------- what-if (Step 15)
# Tyre models are written only to models/sim/ (never /f1/*). The consistency run writes to
# data/whatif/<source>_<run id>/; only RECORD=1 writes results/whatif_consistency.* and adds the
# 2025 held-out backtest row (it needs the tyre model built with TEST_SEASON=1). See docs/sim_spec.md.
SIM_TEST_SEASON = $(if $(filter 1,$(TEST_SEASON)), --allow-test-season,)
WHATIF_RECORD = $(if $(filter 1,$(RECORD)), --record --allow-test-season,)

.PHONY: sim-tyre-model-sample sim-tyre-model-full whatif-consistency-sample whatif-consistency-full
sim-tyre-model-sample: ## What-if tyre model from the sample lake -> models/sim/tyre_model_sample.json
	DATA_LAKE_URI=file://$(CURDIR)/data/lake $(UV) run python -m f1sa.sim.tyre_model --source sample

sim-tyre-model-full: ## What-if tyre model from the HDFS lake (host, via HttpFS) -> models/sim/tyre_model_full.json [TEST_SEASON=1]
	DATA_LAKE_URI=$(HDFS_LAKE) HDFS_HTTPFS_URL=http://localhost:14000 $(UV) run python -m f1sa.sim.tyre_model --source lake$(SIM_TEST_SEASON)

whatif-consistency-sample: ## What-if consistency smoke test on the sample lake (data/whatif/ only)
	DATA_LAKE_URI=file://$(CURDIR)/data/lake $(UV) run python experiments/whatif_consistency.py --source sample

whatif-consistency-full: ## What-if consistency + backtest 2021-2024 (app container) [RECORD=1 -> results/, adds the 2025 held-out row]
	$(APP_EXEC) python experiments/whatif_consistency.py --source full$(WHATIF_RECORD)

# ---------------------------------------------------------------- Spark experiments (Step 10)
# The benchmarks run in the RUNNING app container (docker compose exec): no service is started,
# stopped, rebuilt or recreated; docker stats is sampled on the host. Outputs go to
# data/bench/<run_id>/; only RECORD=1 writes results/scalability.* and results/partitioning.* and
# appends the fingerprints to results/README.md. Benchmark data lives only in HDFS /f1/bench and
# ./data/bench (make bench-clean CONFIRM=yes). experiments-spark takes hours: run it with nothing
# else running. GRID_ARGS / PART_ARGS pass options through (e.g. the reduced grid bench-smoke prints).
# Resumable: each finished grid cell is appended to data/bench/<run_id>/scalability.csv at once (and
# each partitioning group to data/bench/<run_id>_part/); RESUME=<run_id> (printed at the start of
# every run) reuses that run's prepared data, skips finished cells and continues. With RECORD=1,
# results/ is written only when the whole grid (and then partitioning) is complete.
GRID_ARGS ?=
PART_ARGS ?=
RESUME ?=
BENCH_RECORD = $(if $(filter 1,$(RECORD)), --record,)
# := so both scripts get the same run id (evaluated once per make invocation)
BENCH_RUN := $(or $(RESUME),bench_$(shell date -u +%Y%m%dT%H%M%SZ))
BENCH_RUN_ARGS = --run-id $(BENCH_RUN)$(if $(RESUME), --resume,)$(BENCH_RECORD)

.PHONY: bench-dirs bench-smoke experiments-spark bench-supplement bench-replot bench-clean test-bench
bench-dirs: ## Create HDFS /f1/bench (owned by the app user) and ./data/bench (idempotent)
	@mkdir -p data/bench
	$(COMPOSE) exec -T namenode hdfs dfs -mkdir -p /f1/bench
	$(COMPOSE) exec -T namenode hdfs dfs -chown f1sa /f1/bench

bench-smoke: bench-dirs ## Spark smoke (1 season, x1, every master, 1 run; partitioning x1) + the full-grid estimate; data/bench only
	$(UV) run python experiments/scalability.py --smoke
	$(UV) run python experiments/partitioning.py --smoke

experiments-spark: bench-dirs ## FULL Spark grid + partitioning (hours, nothing else running) [RECORD=1 -> results/] [GRID_ARGS=] [PART_ARGS=] [RESUME=<run_id>]
	$(UV) run python experiments/scalability.py $(BENCH_RUN_ARGS) $(GRID_ARGS)
	$(UV) run python experiments/partitioning.py $(BENCH_RUN_ARGS) $(PART_ARGS)

# Supplementary re-run of selected cells of a recorded grid (Step 10 follow-up): the grid settings
# come from data/bench/$(SUPPLEMENT_OF)/grid_spec.json; RECORD=1 appends the re-run rows to
# results/scalability.csv (columns rerun_of, note; the original failed rows stay) and redraws the
# plots. Only the scalability script runs (no partitioning). RESUME=<run_id> continues it.
SUPPLEMENT_OF ?=
CELLS ?=
SUPP_RUN := $(or $(RESUME),bench_$(shell date -u +%Y%m%dT%H%M%SZ)_supp)
bench-supplement: bench-dirs ## Re-run CELLS="..." of the recorded run SUPPLEMENT_OF=<run_id> with its grid [RECORD=1 -> merge into results/] [RESUME=]
	@test -n "$(SUPPLEMENT_OF)" -a -n "$(CELLS)" || { echo 'bench-supplement needs SUPPLEMENT_OF=<run_id> CELLS="<cell ids>"' >&2; exit 2; }
	$(UV) run python experiments/scalability.py --run-id $(SUPP_RUN)$(if $(RESUME), --resume,)$(BENCH_RECORD) --supplement-of $(SUPPLEMENT_OF) --only-cells $(CELLS)

# Redraw the build plot from a scalability CSV (no measurement). RECORD=1 redraws
# results/scalability_build.png only; otherwise the plot goes to data/bench/replot/.
REPLOT_OUT = $(if $(filter 1,$(RECORD)),results/scalability_build.png,data/bench/replot/scalability_build.png)
bench-replot: ## Redraw the build plot from results/scalability.csv [RECORD=1 -> results/scalability_build.png only]
	@mkdir -p $(dir $(REPLOT_OUT))
	$(UV) run python -m f1sa.bench.report --replot results/scalability.csv --out $(REPLOT_OUT)

bench-clean: ## Delete benchmark data ./data/bench/* and HDFS /f1/bench/* (CONFIRM=yes; lists first)
	$(UV) run python -m f1sa.bench.paths$(if $(filter yes,$(CONFIRM)), --yes,)

test-bench: ## Step 10 benchmark unit tests
	$(UV) run pytest tests/unit/test_bench_*.py tests/unit/test_makefile_bench.py -q

# ---------------------------------------------------------------- API (Step 13)
# `make api` runs the FastAPI backend on the host (uvicorn, 127.0.0.1:$(API_PORT)) against the file
# lake; it needs `make up` for Redis and Kafka. API_PORT defaults to 8001 because the compose `api`
# service holds 8000. `make api-up` recreates only the compose `api` service with the chosen mode
# and flags (--no-deps --no-build: no other service restarts, no image build, no volume touched).
API_PORT ?= 8001
API_HDFS_LAKE := hdfs://namenode:8020/f1
API_FILE_LAKE := file:///workspace/data/lake
flag01 = $(if $(filter 1,$(1)),1,0)

.PHONY: api api-up api-dirs demo-clean test-api
api: ## FastAPI on the host at 127.0.0.1:$(API_PORT) (default 8001; docs /api/docs; needs make up)
	DATA_LAKE_URI=file://$(CURDIR)/data/lake API_PORT=$(API_PORT) $(UV) run python -m f1sa.api

api-up: api-dirs ## Recreate only the api container: FULL=1 (HDFS lake; needs make up-full) REPLAY=1 DEBUG=1 TEST_SEASON=1
ifeq ($(call flag01,$(FULL)),1)
	@scripts/wait_for.sh -t 5 namenode datanode httpfs >/dev/null 2>&1 || { echo "FULL=1 needs the hdfs profile (namenode, datanode, httpfs healthy): run 'make up-full' first." >&2; exit 1; }
endif
	API_DATA_LAKE_URI=$(if $(filter 1,$(FULL)),$(API_HDFS_LAKE),$(API_FILE_LAKE)) \
	ENABLE_REPLAY=$(call flag01,$(REPLAY)) ENABLE_DEBUG=$(call flag01,$(DEBUG)) \
	ALLOW_TEST_SEASON=$(call flag01,$(TEST_SEASON)) \
	$(COMPOSE) --profile core up -d --no-deps --no-build --force-recreate api
	scripts/wait_for.sh -t 120 api
	@echo "api: lake=$(if $(filter 1,$(FULL)),hdfs (full),file (sample)) replay=$(call flag01,$(REPLAY)) debug=$(call flag01,$(DEBUG)) test_season=$(call flag01,$(TEST_SEASON))"

api-dirs: # host folders of the api's read-write mounts (Docker would create them root-owned)
	@mkdir -p data/demo data/checkpoints

demo-clean: ## Delete demo outputs data/demo/<run_id> and demo/replay checkpoints (CONFIRM=yes; lists first)
	$(UV) run python -m f1sa.demo_runs $(if $(filter yes,$(CONFIRM)),--yes,)

test-api: ## API unit tests (fakeredis, no infra)
	$(UV) run pytest tests/unit/test_api_*.py tests/unit/test_makefile_api.py tests/unit/test_demo_outputs.py tests/unit/test_demo_clean.py -q

# ---------------------------------------------------------------- dashboard (Step 14)
# `make web` runs the Next.js dev server on 127.0.0.1:3000 (the API's CORS origin is
# http://localhost:3000, so it must be port 3000: stop the compose `web` container first; the
# preflight says so). `make web-e2e` builds and starts the app and runs the Playwright smoke test
# against the API (sample mode with replay: `make api-up REPLAY=1`).
WEB_ENV := NEXT_TELEMETRY_DISABLED=1 NEXT_PUBLIC_API_URL=$(or $(NEXT_PUBLIC_API_URL),http://localhost:8000)

.PHONY: web web-test web-build web-e2e
web/node_modules: web/package-lock.json
	cd web && npm ci --no-audit --no-fund
	@touch web/node_modules

web: web/node_modules ## Next.js dev server on 127.0.0.1:3000 (needs the API; stop the web container first)
	@node web/scripts/check-port.mjs 127.0.0.1 3000
	cd web && $(WEB_ENV) npm run dev

web-test: web/node_modules ## Dashboard lint + typecheck + Vitest
	cd web && npm run lint && npm run typecheck && npm test

web-build: web/node_modules ## Production build of the dashboard (.next/standalone)
	cd web && $(WEB_ENV) npm run build

web-e2e: web/node_modules ## Playwright smoke test (needs make api-up REPLAY=1; downloads Chromium once)
	@node web/scripts/check-port.mjs 127.0.0.1 3000
	cd web && npx playwright install chromium
	cd web && $(WEB_ENV) npm run build && $(WEB_ENV) npm run test:e2e
