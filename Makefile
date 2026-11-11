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

.PHONY: model-dev-full
model-dev-full: ## DEVELOPMENT model run on the full features (2021-2024 only; 2025 never loaded)
	$(APP_EXEC) python experiments/model_eval.py --source full --no-test
	$(APP_EXEC) python experiments/model_eval.py --source full --no-test --g 5.0 --k 3

.PHONY: model-test-full
model-test-full: ## THE single test-season run (docs/model_spec.md): both settings, opens 2025; CONFIRM=yes
	@if [ "$(CONFIRM)" != "yes" ]; then echo "Refusing: this evaluates the 2025 test season once per setting (docs/model_spec.md). Re-run as 'make model-test-full CONFIRM=yes'." >&2; exit 1; fi
	$(APP_EXEC) python experiments/model_eval.py --source full --allow-test-season
	$(APP_EXEC) python experiments/model_eval.py --source full --allow-test-season --g 5.0 --k 3

nuke: ## DESTRUCTIVE: remove containers AND volumes (requires CONFIRM=yes)
	@echo "WARNING: this deletes all f1sa volumes (Kafka log data, HDFS namenode/datanode data)."
	@if [ "$(CONFIRM)" != "yes" ]; then echo "Refusing: re-run as 'make nuke CONFIRM=yes'." >&2; exit 1; fi
	$(COMPOSE) --profile full down -v --remove-orphans
