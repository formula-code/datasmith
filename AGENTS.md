# Repository Guidelines

This guide helps contributors work effectively in the datasmith repository.

## Project Structure & Module Organization
- Source: `src/datasmith/` — current modules: `agents/`, `docker/`, `github/`, `publish/`, `resolution/`, `runners/`, `update/`, `utils/`, plus helper modules like `filters.py` and `preflight.py`.
- Tests: `tests/` — pytest suites in grouped folders (e.g., `tests/agents/`, `tests/docker/`, `tests/github/`, `tests/publish/`, `tests/runners/`, `tests/update/`).
- Assets/Docs/Configs: `static/`, `docs/`, `dataset/`, `scripts/`, `supabase/`.
- Generated artifacts: `dataset/` task outputs, `scratch/`, and `dist/` should be treated as build/runtime artifacts and not committed.

## Build, Test, and Development Commands
- `make install` — create env with uv and install pre-commit.
- `make check` — run lock check, pre-commit, mypy, deptry.
- `make test` — run pytest with coverage (XML for CI/Codecov).
- `make build` — build wheel into `dist/`.
- `uv run <cmd>` — run tools inside the env (e.g., `uv run pytest`, `uv run mypy`, `uv run pre-commit run -a`).
- `uv run python -m pytest --doctest-modules` — run doctests with the standard test suite.
- `uvx tox -q` — run the tox matrix (py39–py312) as defined in `tox.ini`.
- To run commands using the same environment variables as the user, use `uv run <command>`.
- Pipeline entrypoint: `ds-update --help` and `ds-update --start-date ... --end-date ...`.

## Coding Style & Naming Conventions
- Python 3.9–3.12. 4‑space indentation, type hints required (mypy strict; see `pyproject.toml`).
- Lint/format via Ruff (line length 120). Run `make check` before pushing.
- Naming: modules/functions `snake_case`, classes `CamelCase`, constants `UPPER_SNAKE_CASE`.
- Prefer package logging via `datasmith.utils.core` over `print`.

## Testing Guidelines
- Framework: pytest + pytest‑cov. Place tests in `tests/` named `test_*.py`.
- Run locally: `make test` or `uv run pytest`.
- Coverage: Codecov target 90% (see `codecov.yaml`). Add tests for new code paths.
- Tests must be deterministic and offline; use fakes for network/AWS.

## Commit & Pull Request Guidelines
- History is informal; please use clear, present‑tense summaries, optionally prefixing a subsystem tag: `docker: prune dangling layers`, `agents: improve build plan`.
- PRs must include: description, rationale, test coverage notes, and any docs updates. Link issues. For CLI/UX changes, include sample output.
- Ensure `make check` and `make test` pass; CI should be green.

## Security & Configuration Tips
- Create `tokens.env` (git-ignored) for environment credentials (`GH_TOKENS` or `GH_TOKEN`, `SUPABASE_URL`, `SUPABASE_KEY`, `CACHE_LOCATION`) and any service tokens used by your workflow.
- Docker tooling exists in `src/datasmith/docker/`; validate locally before pushing remote runs.

## Agent‑Specific Instructions
- Keep changes small and focused; update/cover adjacent tests. Follow this guide for all files under the repo root.
