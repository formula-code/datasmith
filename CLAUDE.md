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

# Pipeline update (monthly, orchestrates all 6 stages)
python scratch/scripts/update_formulacode.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

## Architecture

### Source modules (`src/datasmith/`)

| Module | Purpose |
|--------|---------|
| `core/` | Infrastructure: SQLite storage (`storage.py`), GitHub/Codecov API clients (`api/`), caching (`cache/`), git operations (`git/`), data models (`models/`) |
| `docker/` | Docker image lifecycle: build context creation (`context.py` — largest file), orchestration, DockerHub publishing (`dockerhub.py`), AWS Batch integration, multi-stage shell scripts |
| `scrape/` | GitHub PR scraping, report generation with Jinja2 templates, issue extraction, code coverage integration. See `src/datasmith/scrape/CLAUDE.md` for report builder data flow |
| `agents/` | DSPy-based LLM agents for build context synthesis and performance commit classification |
| `execution/` | Commit collection/filtering from GitHub, dependency resolution, Python environment management |
| `detection/` | Performance breakpoint detection in benchmark results |
| `benchmark/` | ASV benchmark collection |
| `collation/` | Data aggregation |

### Pipeline stages (`scratch/scripts/update_formulacode.py`)

1. `collect_commits.py` — Find perf commits via GitHub API
2. `collect_and_filter_commits.py` — Clone repos, filter irrelevant commits
3. `prepare_commits_for_building_reports.py` — Tokenize patches, crude perf filter
4. `collect_perf_commits.py` — LLM-based performance classification
5. `synthesize_contexts.py` — Agent-based Docker build context synthesis
6. `build_and_publish_to_dockerhub.py` — Build and push final images

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

Requires a `tokens.env` file in the repo root with `GH_TOKEN`, `CACHE_LOCATION`, `PIPELINE_DB`, and optionally `DSPY_*` vars for LLM backends and `DOCKERHUB_*` vars for publishing. See README.md for the full template.
