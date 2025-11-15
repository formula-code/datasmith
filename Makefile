.PHONY: install
install: ## Install the virtual environment and install the pre-commit hooks
	@echo "Creating virtual environment using uv"
	@uv sync --all-extras
	@uv pip install -e .
	@uv run pre-commit install

.PHONY: backup
backup: ## Create a backup of the datasets, results, and analysis directories
	@echo "Syncing backup mirror with rsync"
	@/usr/bin/env bash -euo pipefail -c '\
		if [ ! -f tokens.env ]; then \
			echo "❌ Error: tokens.env file not found"; exit 1; \
		fi; \
		BACKUP_DIR=$$(awk -F= '"'"'/^BACKUP_DIR=/{print $$2; exit}'"'"' tokens.env); \
		if [ -z "$$BACKUP_DIR" ]; then \
			echo "❌ Error: BACKUP_DIR not defined in tokens.env"; exit 1; \
		fi; \
		DEST="$$BACKUP_DIR/datasmith.mirror"; \
		mkdir -p "$$DEST"; \
		rsync -a --delete --human-readable --info=stats1 \
			scratch/ "$$DEST/scratch/"; \
	'
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
	@uv run python -m pytest --cov --cov-config=pyproject.toml --cov-report=xml

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


.PHONY: help
help:
	@uv run python -c "import re; \
	[[print(f'\033[36m{m[0]:<20}\033[0m {m[1]}') for m in re.findall(r'^([a-zA-Z_-]+):.*?## (.*)$$', open(makefile).read(), re.M)] for makefile in ('$(MAKEFILE_LIST)').strip().split()]"



.DEFAULT_GOAL := help
