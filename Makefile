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
CORE_SERVICES := kafka redis app
FULL_SERVICES := $(CORE_SERVICES) namenode datanode httpfs spark-master spark-worker-1 spark-worker-2

.PHONY: up up-full down ps logs nuke

up: ## Start core infra (kafka, redis, app) and wait until healthy
	$(COMPOSE) --profile core up -d --build
	scripts/wait_for.sh -t 240 $(CORE_SERVICES)

up-full: ## Start core + hdfs + spark and wait until healthy
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

nuke: ## DESTRUCTIVE: remove containers AND volumes (requires CONFIRM=yes)
	@echo "WARNING: this deletes all f1sa volumes (Kafka log data, HDFS namenode/datanode data)."
	@if [ "$(CONFIRM)" != "yes" ]; then echo "Refusing: re-run as 'make nuke CONFIRM=yes'." >&2; exit 1; fi
	$(COMPOSE) --profile full down -v --remove-orphans

# ---------------------------------------------------------------- streaming (Step 12)
# stream-demo never starts, stops or rebuilds services: it needs `make up` (kafka, redis) already
# running. Sample mode runs producer, stream job and subscriber on the host (the first run fetches
# the pinned Kafka connector into data/cache/ivy); FULL=1 runs all three in the app container.
SEASON ?= 2024
ROUND ?= 5
SPEED ?= 10
STREAM_DEMO_ARGS = --season $(SEASON) --round $(ROUND) --speed $(SPEED)

.PHONY: stream-demo stream-parity-full
stream-demo: ## Replay one race: Kafka -> Structured Streaming -> Redis + latency CSV (SEASON ROUND SPEED [FULL=1]; needs make up)
ifeq ($(FULL),)
	@test -d data/lake/features/pit_loss -a -d data/lake/clean/laps || { echo "Missing data/lake: run 'make etl-sample features-sample' first." >&2; exit 1; }
	DATA_LAKE_URI=file://$(CURDIR)/data/lake $(UV) run python experiments/streaming_latency.py $(STREAM_DEMO_ARGS)
else
	$(APP_EXEC) python experiments/streaming_latency.py $(STREAM_DEMO_ARGS)
endif

stream-parity-full: ## Batch vs stream feature parity over 2021-2024 (app container, HDFS lake; 2025 never read)
	$(APP_EXEC) python experiments/stream_parity.py --seasons 2021-2024
