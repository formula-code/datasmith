.PHONY: install
install: ## Install the virtual environment and install the pre-commit hooks
	@echo "Creating virtual environment using uv"
	@uv sync --all-extras
	@uv pip install -e .
	@uv run pre-commit install

.PHONY: check
check: ## Run code quality tools.
	@echo "Checking lock file consistency with 'pyproject.toml'"
	@uv lock --locked
	@echo "Linting code: Running pre-commit"
	@uv run pre-commit run -a
	@echo "Static type checking: Running mypy"
	@uv run mypy
	@echo "Checking for obsolete dependencies: Running deptry"
	@uv run deptry src

.PHONY: test
test: ## Test the code with pytest
	@echo "Testing code: Running pytest"
	@uv run python -m pytest -m "not slow" --cov --cov-config=pyproject.toml --cov-report=xml

.PHONY: build
build: clean-build ## Build wheel file
	@echo "Creating wheel file"
	@uvx --from build pyproject-build --installer uv

.PHONY: clean-build
clean-build: ## Clean build artifacts
	@echo "Removing build artifacts"
	@uv run python -c "import shutil; import os; shutil.rmtree('dist') if os.path.exists('dist') else None"

.PHONY: docker-clean
docker-clean: ## Clean up dangling Docker images and containers
	@echo "Cleaning up dangling Docker images and containers"
	@docker system prune -f

.PHONY: supabase-up
supabase-up: ## Start local Supabase instance
	@npx supabase start

.PHONY: supabase-down
supabase-down: ## Stop local Supabase instance
	@npx supabase stop

.PHONY: supabase-status
supabase-status: ## Show Supabase service status and URLs
	@npx supabase status

.PHONY: grafana-migrate
grafana-migrate: ## Apply the grafana_ro read-only database role
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
grafana-tunnel: ## Expose Grafana publicly via Cloudflare Tunnel
	@cloudflared tunnel run datasmith-grafana

.PHONY: db-tunnel
db-tunnel: ## Expose Supabase PostgREST API via Cloudflare Tunnel (db.formulacode.org)
	@cloudflared tunnel --config ~/.cloudflared/config-db.yml run datasmith-db

LITELLM_VENV := .venv-litellm

.PHONY: model-proxy-install
model-proxy-install: $(LITELLM_VENV)/.installed ## Set up the persistent venv used by model-tunnel

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
model-tunnel: $(LITELLM_VENV)/.installed ## Start LiteLLM proxy + Cloudflare Tunnel for vLLM (model.formulacode.org)
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
	echo "litellm ready on :4100 (pid $$LITELLM_PID); starting cloudflared"; \
	cloudflared tunnel --config ~/.cloudflared/config-model.yml run datasmith-model


.PHONY: help
help:
	@uv run python -c "import re; \
	[[print(f'\033[36m{m[0]:<20}\033[0m {m[1]}') for m in re.findall(r'^([a-zA-Z_-]+):.*?## (.*)$$', open(makefile).read(), re.M)] for makefile in ('$(MAKEFILE_LIST)').strip().split()]"



.DEFAULT_GOAL := help
