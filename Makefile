.DEFAULT_GOAL := help
SHELL := /bin/bash
UV ?= uv

.PHONY: help setup lint typecheck test test-int sample-data

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Install Python deps (incl. dev extras) into .venv
	$(UV) sync --all-extras

lint: ## Ruff lint + format check
	$(UV) run ruff check src tests
	$(UV) run ruff format --check src tests

typecheck: ## mypy (strict) on src
	$(UV) run mypy src

test: ## Unit tests
	$(UV) run pytest tests/unit -q

sample-data: ## Validate data/sample offline, or build it if missing (REBUILD=1 forces a rebuild)
	$(UV) run python -m f1sa.ingest.sample_builder $(if $(REBUILD),--rebuild,)

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

nuke: ## DESTRUCTIVE: remove containers AND volumes (requires CONFIRM=yes)
	@echo "WARNING: this deletes all f1sa volumes (Kafka log data, HDFS namenode/datanode data)."
	@if [ "$(CONFIRM)" != "yes" ]; then echo "Refusing: re-run as 'make nuke CONFIRM=yes'." >&2; exit 1; fi
	$(COMPOSE) --profile full down -v --remove-orphans
