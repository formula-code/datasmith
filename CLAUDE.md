# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is this project?

DataSmith is the Python toolchain for building the **FormulaCode** dataset — a benchmark of 67+ repositories with 964+ performance-improving commits, designed to evaluate LLMs' ability to optimize real-world codebases. It scores LLMs relative to the human-authored speedup using ASV (Airspeed Velocity) benchmarks rather than binary pass/fail tests.

## Common commands

```bash
# Setup
make install          # Create venv with uv, install pre-commit hooks
make check            # Lint (ruff), format, type-check (mypy), dependency check (deptry)
make test             # Run pytest with coverage

# Development
uv run pytest tests/                         # Run all tests
uv run pytest tests/test_docker_context.py   # Run a single test file
uv run pytest -xvs tests/test_scraper.py     # Verbose, stop on first failure
uv run mypy                                  # Type checking
uv run pre-commit run -a                     # Run all pre-commit hooks

# Dataset verification (iterative Docker build debugging)
python dataset/verify.py --task dataset/formulacode_verified/<owner_repo>/<sha>

# Pipeline update (monthly, the primary entrypoint for DataSmith)
ds-update --start-date YYYY-MM-DD --end-date YYYY-MM-DD          # Run all 6 stages
ds-update --start-date 2026-01-01 --end-date 2026-01-31 --stage 4  # Run a single stage
ds-update --start-date 2026-01-01 --end-date 2026-01-31 --resume   # Resume from last completed
ds-update --help                                                    # See all options
```

## Architecture

### Source modules (`src/datasmith/`)

| Module | Purpose |
|--------|---------|
| `core/` | Infrastructure: SQLite storage (`storage.py`), GitHub/Codecov API clients (`api/`), caching (`cache/`), git operations (`git/`), data models (`models/`) |
| `docker/` | Docker image lifecycle: build context creation (`context.py` — largest file), orchestration, DockerHub publishing (`dockerhub.py`), AWS Batch integration, multi-stage shell scripts |
| `scrape/` | GitHub PR scraping, report generation with Jinja2 templates, issue extraction, code coverage integration. See `src/datasmith/scrape/CLAUDE.md` for report builder data flow |
| `agents/` | DSPy-based LLM agents for build context synthesis and performance commit classification |
| `resolution/` | Dependency resolution: parses pyproject.toml/setup.py/setup.cfg, resolves pinned deps via `uv pip compile`, validates installability. Pipeline stage 4. |
| `execution/` | Commit collection/filtering from GitHub, Python environment management |
| `detection/` | Performance breakpoint detection in benchmark results |
| `benchmark/` | ASV benchmark collection |
| `collation/` | Data aggregation |

### Pipeline stages (`ds-update`)

1. **scrape_repos** — Fetch repository metadata from GitHub
2. **scrape_commits** — Scrape merged PR commits and patches
3. **classify_prs** — LLM-based performance classification
4. **resolve_packages** — Resolve Python dependencies via `uv pip compile`, persist to `packages` table
5. **synthesize_images** — Agent-based Docker build context synthesis (uses env_payload/python_version from stage 4)
6. **publish** — Build, verify, and publish Docker images to DockerHub

### Dataset verification (`dataset/`)

Each task lives in `dataset/formulacode_verified/<owner_repo>/<sha>/` with a multi-stage Dockerfile, shell build scripts, and validation scripts. The verification loop is:
1. Run `verify.py` → check `failure.json` for errors → edit `docker_build_pkg.sh` and/or `docker_build_run.sh` → rerun until `verification_success.json` appears.

**Only modify `docker_build_pkg.sh` and `docker_build_run.sh`** during verification fixes. See `dataset/CLAUDE.md` and `dataset/AGENTS.md` for detailed guidance.

## Code quality standards

- **Python**: 3.9–3.12, type hints required (mypy strict)
- **Linting**: Ruff with 120-char line length
- **Testing**: pytest + pytest-cov
- **Build**: hatchling backend, uv for dependency management
- **CI**: GitHub Actions runs `make check` + tests on Python 3.11 and 3.12

## Environment setup

Requires a `tokens.env` file in the repo root with `GH_TOKEN`, `CACHE_LOCATION`, and optionally `DSPY_*` vars for LLM backends and `DOCKERHUB_*` vars for publishing. See README.md for the full template.
