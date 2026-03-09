# Repository Guidelines

This guide helps contributors work effectively in the datasmith repository.

## Project Structure & Module Organization
- Source: `src/datasmith/` — core modules: `agents/`, `docker/`, `scrape/`, `benchmark/`, `detection/`, `execution/`, `collation/`, `core/`.
- Tests: `tests/` — pytest suites (e.g., `tests/test_docker_*`, `tests/agents/`).
- Assets/Docs: `static/`, `docs/`.
- Artifacts: `scratch/` (generated data), `dist/` (wheels). Do not commit contents.

## Build, Test, and Development Commands
- `make install` — create env with uv and install pre-commit.
- `make check` — lock check, ruff lint/format, mypy, deptry.
- `make test` — run pytest with coverage (XML for CI/Codecov).
- `make build` — build wheel into `dist/`.
- `uv run <cmd>` — run tools inside the env (e.g., `uv run pytest`).
- `uvx tox -q` — run the tox matrix (py39–py312) if tox is installed.
- Optional: `make backup` uses `tokens.env` for `BACKUP_DIR` rsync.
- To run commands using the same environment variables as the user, use `uv run <command>`.

## Coding Style & Naming Conventions
- Python 3.9–3.12. 4‑space indentation, type hints required (mypy strict; see `pyproject.toml`).
- Lint/format via Ruff (line length 120). Run `make check` before pushing.
- Naming: modules/functions `snake_case`, classes `CamelCase`, constants `UPPER_SNAKE_CASE`.
- Prefer `logging` (see `src/datasmith/logging_config.py`) over prints.

## Testing Guidelines
- Framework: pytest + pytest‑cov. Place tests in `tests/` named `test_*.py`.
- Run locally: `make test` or `uv run pytest`.
- Coverage: Codecov target 90% (see `codecov.yaml`). Add tests for new code paths.
- Tests must be deterministic and offline; use fakes for network/AWS.

## Commit & Pull Request Guidelines
- History is informal; please use clear, present‑tense summaries, optionally prefixing a subsystem tag: `docker: prune dangling layers`, `agents: improve build plan`.
- PRs must include: description, rationale, test coverage notes, and any docs updates. Link issues. For CLI/UX changes, include sample output or screenshots.
- Ensure `make check` and `make test` pass; CI should be green.

## Security & Configuration Tips
- Create `tokens.env` (ignored) for `GH_TOKEN`, `CODECOV_TOKEN`, `CACHE_LOCATION`, `BACKUP_DIR`. Never commit secrets.
- Docker tooling exists in `src/datasmith/docker/`; validate locally before pushing remote runs.

## Agent‑Specific Instructions
- Keep changes small and focused; update/cover adjacent tests. Follow this guide for all files under the repo root.
