# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is this project?

fc-data is the Python toolchain for building the **FormulaCode** dataset — a benchmark of 67+ repositories with 964+ performance-improving commits, designed to evaluate LLMs' ability to optimize real-world codebases. It scores LLMs relative to the human-authored speedup using ASV (Airspeed Velocity) benchmarks rather than binary pass/fail tests.

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

# Pipeline update (monthly, the primary entrypoint for fc-data)
fc-data --start-date YYYY-MM-DD --end-date YYYY-MM-DD          # Run all 8 stages
fc-data --start-date 2026-01-01 --end-date 2026-01-31 --stage 4  # Run a single stage
fc-data --start-date 2026-01-01 --end-date 2026-01-31 --resume   # Resume from last completed
fc-data --help                                                    # See all options

# Stage 7 (harbor_healthcheck) knobs
fc-data --stage 7 --harbor-environment docker                          # Local Docker (default)
fc-data --stage 7 --harbor-environment daytona --n-concurrent 16       # Daytona, 16 parallel trials
fc-data --stage 7 --harbor-limit 10                                    # Smoke test on 10 tasks
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

### Pipeline stages (`fc-data`)

1. **scrape_repos** — Fetch repository metadata from GitHub
2. **scrape_commits** — Scrape merged PR commits and patches
3. **classify_prs** — LLM-based performance classification
4. **resolve_packages** — Resolve Python dependencies via `uv pip compile`, persist to `packages` table
5. **render_problems** — Scrape linked issues and render deconstructed problem contexts
6. **synthesize_images** — Agent-based Docker build context synthesis (uses env_payload/python_version from stage 4)
7. **harbor_healthcheck** — Run every synthesized container through Harbor's oracle agent, record per-benchmark speedups to `harbor_runs`. Supports local Docker and Daytona via `--harbor-environment`; the row records which one in `harbor_runs.environment`. Local runs are useful for iteration; only Daytona runs gate stage 8.
8. **publish** — Build, verify, and publish Docker images to DockerHub. Only publishes PRs with at least one successful **Daytona** `harbor_runs` row whose `max_speedup >= 1.05`.

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

### Documentation

After making a feature change, decide whether the change is significant enough to warrant updating the documentation in `docs/`. Changes that affect user-facing behavior, CLI flags, configuration knobs, pipeline stages, agent backends, or architectural decisions should be reflected in the relevant guide or design doc. Internal refactors, bug fixes, and implementation details generally do not need doc updates unless they change observable behavior.

### Tunable constants

Any module-level constant that is a knob — timeouts, retries, caps, windows,
concurrency limits, thresholds — **must** be overridable from `tokens.env`
without a code change. The `datasmith` package auto-loads `tokens.env` at
import time (`src/datasmith/__init__.py` → `dotenv.load_dotenv`), so reading
`os.environ.get(...)` at module scope picks up `tokens.env` values.

- **Naming**: prefix every overridable constant and its env variable with
  `DATASMITH_` so it is globally greppable in both Python and shell env.
- **Pattern**: read the env var at module top, coerce to the target type,
  and fall back to a literal default:

  ```python
  import os

  DATASMITH_RL_MAX_RETRIES: int = int(os.environ.get("DATASMITH_RL_MAX_RETRIES", "3"))
  DATASMITH_NEIGHBOR_WINDOW_DAYS: int = int(
      os.environ.get("DATASMITH_NEIGHBOR_WINDOW_DAYS", "60")
  )
  ```

- **Scope**: this rule applies to *tunable* knobs. Magic strings that
  identify protocol fields, schema columns, or on-disk paths are not
  constants in this sense and should stay as literals.
- **Existing uses** (non-exhaustive, grep `DATASMITH_` for the full list):
  `DATASMITH_RL_DEFAULT_PAUSE_S`, `DATASMITH_RL_PAUSE_JITTER_S`,
  `DATASMITH_RL_MAX_RETRIES`, `DATASMITH_NEIGHBOR_WINDOW_DAYS`,
  `DATASMITH_NEIGHBOR_CAP`.

## Supabase

fc-data uses a **local Supabase** instance for all persistent state. Connection details live in `tokens.env`:

- `SUPABASE_URL=http://127.0.0.1:54321` (PostgREST API)
- `SUPABASE_KEY=sb_secret_...` (service-role key)
- Direct Postgres: `host=127.0.0.1 port=54322 dbname=postgres user=postgres password=postgres`

### Remote access

The Supabase PostgREST API is also available at `https://db.formulacode.org` via a Cloudflare Tunnel, protected by Cloudflare Access service tokens. To connect from a remote machine, set these in `tokens.env`:

- `SUPABASE_URL=https://db.formulacode.org`
- `DATASMITH_CF_ACCESS_CLIENT_ID` — Cloudflare Access service-token Client ID
- `DATASMITH_CF_ACCESS_CLIENT_SECRET` — Cloudflare Access service-token Client Secret

When both `DATASMITH_CF_ACCESS_*` vars are set, `get_client()` and `get_async_client()` in `utils/db.py` automatically inject the required headers. See `docs/guide/remote-access.md` for full setup instructions.

### Key tables

| Table | Purpose | Populated by |
|-------|---------|-------------|
| `pull_requests` | All scraped PRs with classification, patches, rendered problems, container names | Stages 1-3, 5-6 |
| `packages` | Resolved `env_payload` (pinned deps) and `python_version` per commit | Stage 4 |
| `candidate_containers` | Successful agent-generated `build_pkg_sh` / `build_run_sh` per SHA | Stage 6 (on success) |
| `harbor_runs` | One row per Harbor oracle trial for a synthesized container: `max_speedup`, `geomean_speedup`, `n_benchmarks`, `wallclock_sec`, `reward_payload`, `status`. One-to-many FK on `candidate_containers(owner, repo, sha)`. | Stage 7 |
| `error_logs` | Per-attempt synthesis results: agent output, failure stage/return code, error messages | Stage 6 (`Synthesizer._log_attempt`) |
| `runner_progress` | Live progress counters (total/completed/failed) per pipeline run | `BaseRunner` (all stages) |
| `runner_failures` | One row per item failure with error message + traceback | `BaseRunner._log_failure` |
| `candidate_prs` | Deconstructed PR context components for re-rendering | Stage 5 |
| `hook_cache` | Memoization cache for `@supabase_cached` decorated functions | Various |

### Migrations

SQL migrations live in `supabase/migrations/` (numbered `00001_` through `00007_`). To apply a new migration against the local instance:

```python
import psycopg2
conn = psycopg2.connect(host='127.0.0.1', port=54322, dbname='postgres', user='postgres', password='postgres')
conn.autocommit = True
conn.cursor().execute(open('supabase/migrations/00007_error_logs.sql').read())
# Don't forget: GRANT ALL ON <table> TO anon, authenticated;
```

### Querying

Use `datasmith.utils.db.fetch_all(table, select=..., filters=..., gte_filters=..., ...)` for paginated reads, or `get_client()` for direct Supabase client access.

## Environment setup

Requires a `tokens.env` file in the repo root with `GH_TOKEN`, `CACHE_LOCATION`, `SUPABASE_URL`, `SUPABASE_KEY`, and optionally `DSPY_*` vars for LLM backends and `DOCKERHUB_*` vars for publishing. See README.md for the full template.
