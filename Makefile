##@ Development
## Everyday targets. `check` and `test` run on every commit; CI invokes
## `make check` by name (.github/workflows/main.yml), so do not rename it.

.PHONY: install
install: ## Install the virtual environment and the pre-commit hooks (once per clone)
	@echo "Creating virtual environment using uv"
	@uv sync --all-extras
	@uv pip install -e .
	@uv run pre-commit install

.PHONY: check
check: ## Run code quality tools (lock check, pre-commit, mypy, deptry)
	@echo "Checking lock file consistency with 'pyproject.toml'"
	@uv lock --locked
	@echo "Linting code: Running pre-commit"
	@uv run pre-commit run -a
	@echo "Static type checking: Running mypy"
	@uv run mypy
	@echo "Checking for obsolete dependencies: Running deptry"
	@uv run deptry src

.PHONY: test
test: ## Test the code with pytest (excludes -m slow)
	@echo "Testing code: Running pytest"
	@uv run python -m pytest -m "not slow" --cov --cov-config=pyproject.toml --cov-report=xml

##@ Packaging
## Local wheel builds. CI publishing does not use these targets --
## .github/workflows/publish.yml runs `uv run python -m build` directly.

.PHONY: build
build: clean-build ## Build wheel file
	@echo "Creating wheel file"
	@uvx --from build pyproject-build --installer uv

.PHONY: clean-build
clean-build: ## Clean build artifacts
	@echo "Removing build artifacts"
	@uv run python -c "import shutil; import os; shutil.rmtree('dist') if os.path.exists('dist') else None"

##@ Housekeeping

.PHONY: docker-clean
docker-clean: ## Prune dangling Docker images and containers (reclaim build disk)
	@echo "Cleaning up dangling Docker images and containers"
	@docker system prune -f

##@ Database (local Supabase)
## The tunnel serves both hostnames off the single `datasmith-db` tunnel:
## db.formulacode.org (CF Access + service-role key) and
## api.formulacode.org (public anon reads, no Access gate).

.PHONY: supabase-up
supabase-up: ## Start local Supabase instance
	@npx supabase start

.PHONY: supabase-down
supabase-down: ## Stop local Supabase instance
	@npx supabase stop

.PHONY: supabase-status
supabase-status: ## Show Supabase service status and URLs
	@npx supabase status

.PHONY: db-tunnel
db-tunnel: ## Expose PostgREST via Cloudflare Tunnel (db + api .formulacode.org)
	@cloudflared tunnel --config ~/.cloudflared/config-db.yml run datasmith-db

##@ Monitoring (Grafana)

.PHONY: grafana-migrate
grafana-migrate: ## Apply the grafana_ro read-only role (once per Supabase volume)
	@docker exec supabase_db_datasmith_new psql -U postgres -d postgres -f /dev/stdin < supabase/migrations/00009_grafana_readonly.sql
	@echo "grafana_ro role created"

.PHONY: grafana-up
grafana-up: ## Start Grafana dashboard (http://localhost:3001)
	@docker compose -f grafana/docker-compose.yml up -d
	@echo "Grafana is running at http://localhost:3001"

.PHONY: grafana-down
grafana-down: ## Stop Grafana dashboard
	@docker compose -f grafana/docker-compose.yml down

.PHONY: grafana-logs
grafana-logs: ## Tail Grafana container logs
	@docker compose -f grafana/docker-compose.yml logs -f

.PHONY: grafana-tunnel
grafana-tunnel: ## Expose Grafana via Cloudflare Tunnel (datasmith-grafana)
	@cloudflared tunnel run datasmith-grafana

##@ Model proxy (LiteLLM + vLLM)

LITELLM_VENV := .venv-litellm

.PHONY: model-proxy-install
model-proxy-install: $(LITELLM_VENV)/.installed ## Build the .venv-litellm venv (once per machine; also the stale-venv fix)

$(LITELLM_VENV)/.installed:
	@echo "Creating $(LITELLM_VENV) and installing litellm[proxy] + prisma"
	@uv venv $(LITELLM_VENV) --python 3.12 --quiet --allow-existing
	@uv pip install --quiet --python $(LITELLM_VENV)/bin/python 'litellm[proxy]' prisma
	@echo "Running prisma generate against LiteLLM's bundled schema"
	@SCHEMA=$$($(LITELLM_VENV)/bin/python -c "import litellm, os; print(os.path.join(os.path.dirname(litellm.__file__),'proxy','schema.prisma'))"); \
	    VENV_BIN=$$(realpath $(LITELLM_VENV)/bin); \
	    cd $(LITELLM_VENV) && PATH="$$VENV_BIN:$$PATH" prisma generate --schema=$$SCHEMA
	@touch $@

.PHONY: model-tunnel
model-tunnel: $(LITELLM_VENV)/.installed ## Start LiteLLM + reconciler + Cloudflare Tunnel (model.formulacode.org)
	@set -eu; \
	set -a; . ./tokens.env; set +a; \
	REPO_ROOT=$$(pwd); \
	( cd "$$REPO_ROOT/$(LITELLM_VENV)" && \
	      exec "$$REPO_ROOT/$(LITELLM_VENV)/bin/litellm" \
	          --config "$$REPO_ROOT/infra/litellm.config.yaml" \
	          --port 4100 --host 127.0.0.1 ) & \
	LITELLM_PID=$$!; \
	trap 'kill $$LITELLM_PID 2>/dev/null || true; wait $$LITELLM_PID 2>/dev/null || true' EXIT INT TERM; \
	until curl -fsS http://127.0.0.1:4100/health/liveliness >/dev/null 2>&1; do \
	    if ! kill -0 $$LITELLM_PID 2>/dev/null; then echo "litellm exited before becoming ready" >&2; exit 1; fi; \
	    sleep 1; \
	done; \
	echo "litellm ready on :4100 (pid $$LITELLM_PID); starting model reconciler"; \
	( exec "$$REPO_ROOT/$(LITELLM_VENV)/bin/python" \
	      "$$REPO_ROOT/infra/refresh_models.py" --watch ) & \
	REFRESH_PID=$$!; \
	trap 'kill $$LITELLM_PID $$REFRESH_PID 2>/dev/null || true; wait $$LITELLM_PID $$REFRESH_PID 2>/dev/null || true' EXIT INT TERM; \
	echo "reconciler running (pid $$REFRESH_PID); starting cloudflared"; \
	cloudflared tunnel --config ~/.cloudflared/config-model.yml run datasmith-model

.PHONY: model-refresh
model-refresh: ## Reconcile the LiteLLM registry with live vLLM servers once (ARGS="--dry-run")
	@set -a; . ./tokens.env; set +a; \
	python3 infra/refresh_models.py $(ARGS)

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*## "; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} \
	      /^##@ / {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)} \
	      /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-21s\033[0m %s\n", $$1, $$2}' \
	      $(MAKEFILE_LIST)
	@echo

.DEFAULT_GOAL := help
