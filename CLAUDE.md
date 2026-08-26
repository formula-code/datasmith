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
uv run pytest tests/ -m "not slow"           # Run all tests (excludes tests that build/run real containers)
uv run pytest tests/docker/test_context.py   # Run a single test file
uv run pytest -xvs tests/docker/test_manifest.py -k invariant  # Verbose, stop on first failure, filter by name
uv run pytest tests/docker/test_manifest_integration.py -v -m slow  # Docker integration tests (opt-in, needs a daemon)
uv run mypy                                  # Type checking (checks `src/` only — see [tool.mypy] files)
uv run pre-commit run -a                     # Run all pre-commit hooks

# Local services (`make help` lists every target, grouped by subsystem)
make supabase-up / supabase-down / supabase-status   # Local Supabase via npx
make grafana-up / grafana-down / grafana-logs        # Grafana on :3001
make db-tunnel                                       # Expose PostgREST as db + api .formulacode.org
make model-tunnel                                    # LiteLLM + reconciler + model.formulacode.org

# Dataset verification (iterative Docker build debugging)
python dataset/verify.py --task dataset/formulacode_verified/<owner_repo>/<sha>

# Pipeline update (monthly, the primary entrypoint for fc-data)
fc-data --start-date YYYY-MM-DD --end-date YYYY-MM-DD          # Run all 9 stages
fc-data --start-date 2026-01-01 --end-date 2026-01-31 --stage 4  # Run a single stage
fc-data --start-date 2026-01-01 --end-date 2026-01-31 --stage 5 --stage 6  # --stage is repeatable
fc-data --start-date 2026-01-01 --end-date 2026-01-31 --resume   # Resume from last completed
fc-data --help                                                    # See all options

# Stage 7 (harbor_healthcheck) knobs
fc-data --stage 7 --harbor-environment docker                          # Local Docker (default)
fc-data --stage 7 --harbor-environment daytona --n-concurrent 16       # Daytona, 16 parallel trials
fc-data --stage 7 --harbor-limit 10                                    # Smoke test on 10 tasks
```

## Architecture

### Source modules (`src/datasmith/`)

The dependency direction is roughly `utils/` → `github/` + `docker/` + `agents/` → `runners/` → `update/`. Each module's `__init__.py` re-exports its public surface; import from the package (`from datasmith.github import PR`), not the submodule.

| Module | Purpose |
|--------|---------|
| `utils/` | Foundation layer, imported by everything else: Supabase client + query helpers (`db.py` — `fetch_all`/`afetch_all`, `batch_upsert`/`abatch_upsert`, `supabase_cached`), `Settings`/logging/backoff (`core.py`), GitHub token pool (`tokens.py`), Docker disk reclamation (`docker_prune.py`) |
| `github/` | Frozen Pydantic PR/Issue models (`models.py`), async GitHub client (`client.py`), linked-issue BFS (`links.py`), repo discovery via code search (`search.py`), Jinja2 problem-statement rendering + anonymization (`render.py`), and a `HookRegistry` (`hooks.py`) for dataset-specific extensions |
| `docker/` | Image lifecycle: build-context synthesis and persistence (`context.py` — largest file), image naming/build (`images.py`), DockerHub push (`publish.py`), build-manifest invariants (`manifest.py`). `templates/` holds the multi-stage Dockerfiles and `docker_build_*.sh` scripts baked into every task image |
| `agents/` | LLM agents: DSPy perf classification (`classifiers.py`) and problem extraction (`extractors.py`); CLI-agent synthesis of build scripts (`synthesizer.py`, `sandbox.py`, `installed/`), plus rate limiting and tamper auditing |
| `resolution/` | Dependency resolution: parses pyproject.toml/setup.py/setup.cfg, resolves pinned deps via `uv pip compile`, validates installability. Pipeline stage 4 |
| `runners/` | One module per pipeline stage, all subclassing `BaseRunner` (`base.py`), which supplies bounded concurrency plus `runner_progress`/`runner_failures` bookkeeping. A runner logs per-item failures and keeps going — it never aborts the stage |
| `update/` | Pipeline orchestrator (`pipeline.py` — owns the `STAGES` list and stage dispatch) and the `fc-data` CLI (`cli.py`). `offline.py` imports PR data from parquet instead of the GitHub API |
| `harbor_adapter/` | Vendored subset of Harbor's `formulacode` adapter: materializes a Harbor task directory from Supabase rows and parses trial results. `template/parser.py` owns the geomean level1–level4 rollup |
| `publish/` | DockerHub + HuggingFace publishing (stage 8) and the parquet record round-trip |
| `scrape/` | AST extraction of ASV benchmark functions from a checked-out repo (stage 9). Note: PR scraping lives in `github/`, not here |
| `filters.py` | Cheap symbolic pre-screening. Stage 2 stores **every** merged PR and evaluates only the two components GraphQL can answer — the title keyword filter and file compliance — recording the verdict in `is_performance_commit_symbolic`. `check_patch_size` still lives here, but now runs from stage 3, where it gates the diff fetch and the LLM call rather than storage |
| `preflight.py` | Startup environment checks run before the pipeline does any work |

### Pipeline stages (`fc-data`)

1. **scrape_repos** — Fetch repository metadata from GitHub
2. **scrape_commits** — Scrape merged PRs via `GitHubClient.fetch_merged_prs`, which windows `merged_at` server-side through GitHub's search API and bisects the range when the 1000-result cap would truncate the answer. GraphQL-only apart from a ~1% REST fallback for PRs whose `files(first: 100)` list is truncated; no diffs are fetched here. A truncated leaf (`Truncated`) or a missing repository (`RepositoryNotFoundError`) fails that repository rather than reading as a healthy zero
3. **classify_prs** — Fetch each candidate PR's diff, apply the `check_patch_size` gate, and only then call the LLM. Three outcomes stay distinct: an oversized patch is recorded with both `is_performance_commit` and `is_performance_commit_symbolic` false and no model call; a diff GitHub definitively will not serve records `is_performance_commit` false and leaves the symbolic column alone, because the size gate never ran; a failed request raises and lands in `runner_failures`. A fetched patch is persisted so a resumed run does not buy the same diff twice
4. **resolve_packages** — Emit one dependency **seed** per commit, and the story of how it was reached. Six units compose it: `discover` picks the packaging root, `declare` reads only what the project states it needs, `interpreter` walks a declared ladder (`requires-python` → trove classifiers → `asv.conf.json` `pythons` → newest release at commit date) and records the rung in `interpreter_source`, `pin` runs one `uv pip compile` with the commit date as `--exclude-newer`, `probe` dry-runs the result, and the row is written. What it deliberately does not read: `requirements*.txt` globs, `environment.yml`, and import statements — so a project that declares nothing gets an empty seed and says so, rather than a list of invented PyPI names. Benchmark tooling (`asv`, `pytest`, `hypothesis`, `setuptools`, `wheel`, `pip`, `versioneer`) is stripped from both the declared set and the compiled one: the base image owns it, and a second owner only starts a version fight. **The stage gates nothing.** `can_install` is retained, nullable, and no longer read or written; `probe_status` (`installable` → `unresolved` → `failed` → `empty`) orders the stage 5 queue best-first and excludes nobody. Stage 6 is the sole arbiter of buildability, because it is the only stage that builds in the real container.
5. **render_problems** — Scrape linked issues and render deconstructed problem contexts
6. **synthesize_images** — Agent-based Docker build context synthesis (uses env_payload/python_version from stage 4)
7. **harbor_healthcheck** — Run every synthesized container through Harbor's oracle agent, record per-benchmark speedups to `harbor_runs`. Supports local Docker and Daytona via `--harbor-environment`; the row records which one in `harbor_runs.environment`. Local runs are useful for iteration; only Daytona runs gate stage 8. An **LSV cache** (`DATASMITH_LSV_CACHE_ENABLED`, default on) lets a repeat trial skip LSV's two expensive passes: the runner bakes the cached survey (`lsv_deps_cache`) into the image and injects datasmith Supabase creds + an 11-column resource key so `lsv_init.py` fetches the cached baselines (`lsv_baseline_cache`) and passes `force=False` only on a full hit; the oracle trial writes both back. It is a pure cost optimization — every miss or error degrades to today's `force=True`, changing no reward or gate — and requires `SUPABASE_URL` to be the `db.formulacode.org` tunnel so the trial container can reach it.
8. **publish** — Build, verify, and publish Docker images to DockerHub. Two gates, and a PR must clear both: it needs at least one successful `harbor_runs` row whose `max_speedup >= 1.05` in an admitted environment (`DATASMITH_PUBLISH_ENVIRONMENTS`, default **daytona** only), *and* its `candidate_containers` row must be `verification_state = 'verified'`. The harbor row says the container is fast; `verification_state` says it is honest, and neither substitutes for the other — `harbor_runs` outlives the container generation that produced it, so a pre-honesty-gate row can carry a fast trial. That second gate is deliberately **not** a knob.
9. **scrape_benchmark_source** — For each `(owner, repo)` in `candidate_containers`, check out the repo at its container SHA, AST-parse every ASV-style benchmark function under the repo's `benchmark_dir`, and upsert one row per `(owner, repo, benchmark_without_params)` into `benchmark_codes` for the FormulaCode website's data sync.

Stage 6 has two synthesis paths. `TRY_DEFAULT` uses the stock template and
needs no agent. `PRODUCE_VERIFY` (behind `DATASMITH_PV_ENABLED`) runs a
reflexive loop between a producer agent that owns `docker_build_pkg.sh` and
`docker_build_run.sh` and a verifier agent that runs the container and grades
it. Severity is decided in `agents/reflexive/severity.py`, never in the
verifier's prompt.

`--stage` is `action="append"` — repeat it to run a subset (`--stage 5 --stage 6`); stages then run in ascending order regardless of flag order. `--start-date`/`--end-date` are required even for a single stage. Other cross-stage knobs: `--dry-run` (log what each stage would do), `--force` (re-run already-processed tasks; stages 3–7), `--tasks-per-repo` (stages 5–7 only), `--agent {claude,codex,gemini,qwen,none}` for stage 6 synthesis, and `--tasks owner/repo#PR,...` to pin stage 7 to specific tasks, bypassing date/repo filters.

**The date window means `merged_at`, half-open `[start, end)`** — in stages 2 through 8 alike. A PR merged exactly at midnight on the end date belongs to the next window, so consecutive runs partition the corpus instead of overlapping, and a PR merged inside the window but opened months before it is now in scope. The contract lives in one place, `window_filters()` in `utils/db.py`; stages 3–8 call it rather than hand-writing filter kwargs, and stage 2 applies the same half-open bounds to GitHub's search API instead of to the database. Duplication is what let the original defect survive — with no single home for the contract there was nothing to test — so a fail-closed AST test asserts every windowing site either routes through the helper or is an audited exemption. Stage 9 windows nothing on purpose (the website wants the full corpus), and `synthesize_images._fetch_neighbor_items` keeps its own ±`DATASMITH_NEIGHBOR_WINDOW_DAYS` band around a PR's `created_at`, which is a similarity heuristic and deliberately reaches outside the run window.

### Build manifest

Every task image seals a **build manifest** at `/opt/formulacode/build_manifest.json`. `templates/emit_manifest.py` writes it at build time from breadcrumbs the `docker_build_*.sh` scripts drop into `notes.jsonl`; `docker/manifest.py` reads it back and evaluates invariants over it.

The manifest has two blocks with different lifetimes: `build` is sealed inside the image and immutable, while `verify` does not exist until the container has actually run and is merged in afterwards by `agents/templates/local_ci.py` — the script the synthesis agent runs inside its sandbox. Invariants are three-valued — an invariant whose inputs are absent is **skipped**, not failed, so the module works against an image that has never been run. Severity is `fatal` (fails the step) or `warn` (recorded in `candidate_containers.manifest_warnings`, non-blocking). A check must never raise on a partially-populated manifest.

The `verify` block also carries **measurement** facts: after `run_tests` passes, `local_ci.py` runs the image a second time against `/measure.sh`, which measures the impacted benchmarks with LSV at `base_commit`, applies the oracle patch (mounted read-only from `task/solution.patch`, never baked into the image), re-measures, and prints a `FORMULACODE_MEASURE_START/END` block. Three FATAL invariants gate it — `measure_timed_out`, `asv_exec_failed`, `oracle_patch_failed`. **Measurement facts must never be sealed via `fc_note`**: `fc_note` lives in the cached base image, so a breadcrumb change silently no-ops and produces an all-null `build` block indistinguishable from a healthy one.

Because the manifest is what gates publishing, the sealer is deliberately hardened against synthesis agents tampering with it — several commits on this branch exist purely to close bypasses (reassigning `rc` after the sealer, `|| true` swallowing an exit code). Treat changes near the sealer as security-relevant and add a regression test.

### Dataset verification (`dataset/`)

Each task lives in `dataset/formulacode_verified/<owner_repo>/<sha>/` with a multi-stage Dockerfile, shell build scripts, and validation scripts. The verification loop is:
1. Run `verify.py` → check `failure.json` for errors → edit `docker_build_pkg.sh` and/or `docker_build_run.sh` → rerun until `verification_success.json` appears.

**Only modify `docker_build_pkg.sh` and `docker_build_run.sh`** during verification fixes. See `dataset/CLAUDE.md` and `dataset/AGENTS.md` for detailed guidance.

### Where not to look

`archive/`, `docs/archive/`, `docs/design/archived/`, `scratch/`, `jobs/`, `backups/`, and `dist/` are dead or historical — Ruff's `extend-exclude` skips several of them. `archive/scrape/CLAUDE.md` documents a report-builder layout that no longer exists. When searching for current behavior, scope to `src/`, `tests/`, `docs/design/`, `docs/guide/`, and `dataset/`.

## Code quality standards

- **Python**: `pyproject.toml` declares `requires-python = ">=3.12"` and Ruff targets `py312`, but the CI matrix still runs tests on **3.11 and 3.12** — so avoid 3.12-only syntax in `src/` (PEP 695 type params are explicitly ignored in the Ruff config for this reason). Type hints required (mypy strict; `disallow_untyped_defs`)
- **Linting**: Ruff with 120-char line length. `E501`, `TRY003`, `SIM108`, `S603`/`S607` are globally ignored; `tests/*` additionally waives `S101` and friends
- **Testing**: pytest + pytest-cov, `asyncio_mode = "auto"` (no `@pytest.mark.asyncio` needed). Tests that build or run real containers must be marked `slow` — `make test` and CI both run `-m "not slow"`
- **Build**: hatchling backend, uv for dependency management. `make check` runs `uv lock --locked`, so a `pyproject.toml` dependency edit must be accompanied by a refreshed `uv.lock`
- **CI**: GitHub Actions runs `make check` + tests on Python 3.11 and 3.12

### Specs and plans

Substantial features are designed before they are built: a design doc in `docs/superpowers/specs/<date>-<slug>-design.md`, an implementation plan in `docs/superpowers/plans/<date>-<slug>.md`, and a branch named `spec/<slug>`. Before changing a subsystem, check whether a spec covers it — the spec is the authority on intent, and the design docs under `docs/design/components/` are per-module references.

### Documentation

After making a feature change, decide whether the change is significant enough to warrant updating the documentation in `docs/`. Changes that affect user-facing behavior, CLI flags, configuration knobs, pipeline stages, agent backends, or architectural decisions should be reflected in the relevant guide or design doc. Internal refactors, bug fixes, and implementation details generally do not need doc updates unless they change observable behavior.

**Diagrams**: use Mermaid (`` ```mermaid `` fenced blocks) for any architecture, flow, or state diagram in `.md` files. Do not use ASCII box-drawing art (`┌ ─ │ └ ──▶`). Mermaid renders natively on GitHub and in the docs site; ASCII does not, and is harder to edit.

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
  `DATASMITH_NEIGHBOR_CAP`, `DATASMITH_BENCH_SCRAPE_MAX_FILE_BYTES`,
  `DATASMITH_BENCH_SCRAPE_DIRS`, `DATASMITH_ASV_PIP_ENV_TYPES`,
  `DATASMITH_PYTHON_FLOOR`, `DATASMITH_PYTHON_CEILING`. Ingestion (stages 2–5) adds four families:
  GitHub search and retry — `DATASMITH_GH_SEARCH_CAP`,
  `DATASMITH_GH_SEARCH_PAGE_SIZE`, `DATASMITH_GH_SEARCH_MAX_PAGES`,
  `DATASMITH_GH_MIN_SHARD_SECONDS`, `DATASMITH_GH_RETRIES`,
  `DATASMITH_GH_BACKOFF_BASE_S`, `DATASMITH_GH_MAX_RETRY_WAIT_S`,
  `DATASMITH_GH_FILES_PER_PAGE`, `DATASMITH_GH_FILES_MAX_PAGES`,
  `DATASMITH_GH_FILES_FALLBACK_CONCURRENCY`; stage 3's diff pacing —
  `DATASMITH_CLASSIFY_DIFF_CONCURRENCY`,
  `DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S`,
  `DATASMITH_CLASSIFY_DIFF_STALL_LOG_S`; per-stage concurrency —
  `DATASMITH_SCRAPE_COMMITS_CONCURRENCY`,
  `DATASMITH_RESOLVE_PACKAGES_WORKERS`,
  `DATASMITH_CLASSIFY_LLM_WORKERS`, `DATASMITH_RENDER_PROBLEMS_WORKERS`
  (stages 3 and 5 each own a bounded pool rather than inheriting the
  interpreter default sized `min(32, cpu_count + 4)`),
  `DATASMITH_GH_SEARCH_CONCURRENCY` (bounds the bisection fan-out, so a
  PostHog-scale month cannot burst ~80 concurrent GraphQL POSTs from a
  one-token pool into GitHub's secondary rate limit),
  `DATASMITH_PREFLIGHT_DB_PINGS`; publication —
  `DATASMITH_PUBLISH_ENVIRONMENTS` (comma-separated Harbor environments
  whose runs may gate stage 8; default `daytona`, since local Docker
  trials share the build host and their timings move with load — set it
  to `docker,daytona` only deliberately, and record that you did;
  it does **not** relax `MIN_HARBOR_SPEEDUP`); and the database read guards —
  `DATASMITH_KEY_FILTER_CHUNK`, `DATASMITH_LARGE_TABLES`,
  `DATASMITH_LARGE_COLUMNS` (the last two are comma-separated name sets,
  not numbers).
  The producer/verifier loop (stage 6, `agents/reflexive/`) adds
  `DATASMITH_PV_ENABLED`, `DATASMITH_PV_MAX_ROUNDS`,
  `DATASMITH_PV_STALL_REPEATS` (how many consecutive identical progress
  keys end the loop; 1 restores the original single-shot detector, which
  ended 27 of 38 recorded failures — 12 of them at round 2, giving the
  producer one attempt per failure),
  `DATASMITH_PV_AGENT_TIMEOUT_S`, `DATASMITH_PV_BATTERY_TIMEOUT_S`,
  `DATASMITH_VERIFY_TIMEOUT_S` (wall-clock for ONE verification — build,
  tests and measurement together; default 5400. It must stay larger than
  `DATASMITH_VERIFY_TEST_TIMEOUT_S` and `DATASMITH_VERIFY_MEASURE_TIMEOUT_S`,
  which bound single steps: it was 3600, equal to each of them, so work still
  inside its own allowance was killed and reported as a hang. Measured
  tests+measurement on the verified corpus reached 3351 s),
  `DATASMITH_PV_BATTERY_CPUS` (cores one battery container may use; 0
  disables the cap. modin, dask and pymc size their worker pools from
  the visible core count, so an uncapped container claims the whole
  host — three modin containers once ran 776 Ray workers between them),
  `DATASMITH_PV_EVIDENCE_BUDGET`, `DATASMITH_PV_PRODUCER_AGENT` and
  `DATASMITH_PV_VERIFIER_AGENT`, plus three for the host-side image scan
  (`agents/reflexive/image_integrity.py`) --
  `DATASMITH_PV_IMAGE_SCAN_TIMEOUT_S`,
  `DATASMITH_PV_IMAGE_SCAN_MAX_FILE_BYTES` and
  `DATASMITH_PV_IMAGE_SCAN_MAX_HITS`. `DATASMITH_PV_ENABLED` is off by default and
  must stay off until the 16-container validation set passes — see
  `docs/superpowers/specs/2026-08-23-producer-verifier-design.md` section 9.
- **Two budgets, two dials.** Cores scale stage 4
  (`DATASMITH_RESOLVE_PACKAGES_WORKERS`); the single GitHub token scales
  stages 2 and 3 and does not care how large the machine is. Keep them on
  separate knobs so raising one cannot trip the other's rate limits, and
  never size a pool from an implicit `cpu_count()` — `run_in_executor(None,
  ...)` silently means something different on every host.
- `DATASMITH_PYTHON_FLOOR` (default `3.8`) and `DATASMITH_PYTHON_CEILING`
  (default `3.12`) bound stage 4's interpreter ladder. The floor is the oldest
  interpreter the container toolchain still supports; the ceiling is the newest
  it is known to build against, and it is a ceiling on purpose — a fresh run
  must not silently start choosing an interpreter no existing image was built
  with. Raise the ceiling only together with a base-image rebuild **and a
  regeneration of `tests/resolution/fixtures/jan2026/`** — all 13 golden
  fixtures record the ceiling as their `python_version`, so a raise fails
  every one of them at once.
- `DATASMITH_ASV_PIP_ENV_TYPES` (default `virtualenv,venv,existing`) is the
  comma-separated set of `asv.conf.json` `environment_type` values whose
  `matrix` names PyPI distributions. Every other value — `conda`, `mamba`,
  `rattler`, site plugins such as `oggm_conda` — names conda packages, which
  are a different namespace: `boost-cpp` and `libprotobuf` are not on PyPI at
  all, and `geos`, `snappy`, `re2` and `zstd` resolve there to unrelated
  projects. Under those, only asv's own `pip+` prefix reaches the seed.

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

### Model proxy

Local vLLM servers (on whatever ports they were launched with) are exposed via a LiteLLM proxy on `https://model.formulacode.org` (OpenAI-compatible, Bearer-auth via `LITELLM_MASTER_KEY` or scoped virtual keys). LiteLLM runs in DB-backed mode against the `litellm` database in the local Supabase Postgres, which enables the admin UI at `/ui/`, virtual keys, and spend logs. The model list is reconciled dynamically: `infra/refresh_models.py` discovers live `vllm serve` processes (parsing `ps`, the same source as the `model-blame` helper), probes each `/v1/models`, and adds/removes them via LiteLLM's admin API — so `infra/litellm.config.yaml` carries only settings and an empty `model_list: []`. Persistent venv at `.venv-litellm/` (set up by `make model-proxy-install`); `make model-tunnel` starts LiteLLM (`infra/litellm.config.yaml`), the reconcile loop, and `cloudflared` (`datasmith-model` tunnel) together. `make model-refresh` runs a single reconcile (`ARGS="--dry-run"` to preview). See `docs/guide/model-proxy.md`.

### Public read-only access (RLS)

Access is gated by two independent layers: a **grant** (does the role have the privilege at all) and an **RLS policy** (which rows). Migration `00015_revoke_anon_select.sql` revokes Supabase's default broad `anon` `SELECT` and re-grants it only on the public set, so every table not listed below returns `permission denied`. The service-role key bypasses both layers, so pipeline processes are unaffected.

Fifteen tables are currently anon-readable, each with a `public_read` SELECT policy — verify with `grep -n "TO anon" supabase/migrations/*.sql` rather than trusting this list to stay current:

| Migration | Tables |
|-----------|--------|
| `00012` + `00015` | `repositories`, `pull_requests`, `candidate_containers`, `harbor_runs` |
| `00016` / `00017` | `benchmark_information`, `benchmark_codes` |
| `00021` / `00023` | the eight `findings_*` tables |
| `00022` | `task_id_map` |

Note that `00016` and `00017` used `GRANT ALL ... TO anon` rather than `GRANT SELECT`. Writes are still blocked, but only by RLS (the policy is SELECT-only) — the grant layer is not doing its share. Follow `00021`'s `GRANT SELECT` pattern for new public tables; don't copy `00016`/`00017`. Public anon access is served on `https://api.formulacode.org` (no Cloudflare Access gate); pipeline operators continue to use `https://db.formulacode.org` with CF Access + service-role key.

### Key tables

| Table | Purpose | Populated by |
|-------|---------|-------------|
| `pull_requests` | All scraped PRs with classification, patches, rendered problems, container names | Stages 1-3, 5-6 |
| `packages` | One seed per `(owner, repo, sha)`: `env_payload` (pinned deps) and `python_version`, plus `interpreter_source` (which ladder rung chose that interpreter), `primary_root`, `requires_python`, the advisory `probe_status` / `probe_log`, `dropped_requirements` (JSON-encoded text, like `env_payload` — every requirement that was refused, with its reason), and provenance: `resolver_version`, `uv_version`, `resolved_at`, `cutoff_used` (null when the commit-date cutoff had to be relaxed). `can_install` is deprecated — nullable, no longer read or written; `resolver_version = 'legacy'` marks the rows the predecessor wrote. | Stage 4 |
| `candidate_containers` | Successful agent-generated `build_pkg_sh` / `build_run_sh` per SHA, plus `build_manifest` (sealed build facts merged with verify observations), `manifest_warnings` (non-fatal invariant ids), and `verification_state` (`unverified` / `verified`, with `verified_at`). `build_manifest IS NULL` identifies rows built before manifests existed; `verification_state = 'unverified'` identifies rows built before the honesty gate applied to them. | Stage 6 (on success) |
| `harbor_runs` | One row per Harbor oracle trial for a synthesized container: `max_speedup`, `geomean_speedup`, `n_benchmarks`, `wallclock_sec`, `reward_payload`, `status`. One-to-many FK on `candidate_containers(owner, repo, sha)`. | Stage 7 |
| `lsv_baseline_cache` | Resource-keyed cache of LSV base-commit baseline timings so stage 7 skips the timing pass on a repeat trial. 11-column PK pins every fact that moves a timing (task + env + image + host/machine_class + cgroup pins + in-sandbox `detected_cpu_model`); `baselines` JSONB is `session.export_baselines()`. Advisory, no FK. Oracle trials write it; every trial reads it. A miss falls back to `force=True`. | Stage 7 (oracle writeback) |
| `lsv_deps_cache` | Task-keyed (`owner, repo, issue_number` PK) cache of the LSV coverage **survey** — the `lightspeed_deps.db` SQLite file, baselines stripped, as `deps_db` BYTEA — so stage 7 skips the survey pass. Resource-independent (survey depends only on code, not CPU), hence one row per task. Required for the baseline cache to load at all (`load_baselines` needs the surveyed DB on disk first). | Stage 7 (oracle writeback) |
| `benchmark_information` | Per-benchmark speedup measurements from terminal-bench eval runs: one row per (run, owner/repo/issue, benchmark, agent, model). `speedup` is `(agent/nop)/(oracle/nop)` so 1.0 = parity with the human expert. `benchmark_type` (`time`/`mem`/`peakmem`/`track`) is a generated column derived from the ASV naming convention. Loaded out-of-band via `scripts/load_benchmark_information.py`. | (manual) |
| `benchmark_codes` | One row per `(owner, repo, benchmark_without_params)` carrying the Python source of each ASV benchmark function plus its setup. Joined to `benchmark_information` on `(owner, repo, benchmark_name)` by the FormulaCode website. | Stage 9 |
| `error_logs` | Per-attempt synthesis results: agent output, failure stage/return code, error messages | Stage 6 (`Synthesizer._log_attempt`) |
| `runner_progress` | Live progress counters (total/completed/failed) per pipeline run | `BaseRunner` (all stages) |
| `runner_failures` | One row per item failure with error message + traceback | `BaseRunner._log_failure` |
| `candidate_prs` | Deconstructed PR context components for re-rendering | Stage 5 |
| `hook_cache` | Memoization cache for `@supabase_cached` decorated functions | Various |
| `findings_*` | Eight denormalized tables backing the FormulaCode website's results pages (leaderboard, stratified/tag advantage, repo quintiles, cost Pareto, workload tradeoff, temporal generalization) plus `findings_metadata`. Public-read. | fc-eval's `analysis/export_website_findings.py` (out-of-band) |
| `task_id_map` | Maps fc-eval's legacy on-disk task ids (`pandas_dev-pandas_3`) to canonical `(owner, repo, issue_number)`. Needed because the legacy suffix is a per-run sequence number, **not** the issue number, and its `_`/`-` sanitization is inconsistent. | fc-eval (out-of-band) |

`resource_metrics` is a JSONB column (not a table) on both `error_logs` and `candidate_containers`, holding observed build/test cost — durations, image size, peak memory. It is always advisory; contrast `build_manifest`, which holds declared facts that gate behavior. The split is deliberate, so keep new cost measurements in `resource_metrics` and new gating facts in `build_manifest`.

### Migrations

SQL migrations live in `supabase/migrations/`, numbered `00001_` upward (currently through `00032_`). The sequence has gaps because numbers get claimed on branches before they land: `00018_lsv_cache_drop_cpu_model.sql` lives on `origin/lsv-cache-integration`, and `00024` is authored in a separate working tree (per `00025`'s header) — `00026` re-lands that same table under a number that is free here. So check other branches before claiming a number, and record in the file header why you skipped one. `00027_pull_requests_window_indexes.sql` adds the two indexes the stage 2–5 window predicates need on `pull_requests` — `merged_at` for the stage-wide scan, `(owner, repo, merged_at)` for the per-repository skip set — and deliberately grants nothing to `anon`. `00028_packages_resolution_v2.sql` carries the stage 4 redesign's provenance columns, and `00029_candidate_containers_verification_state.sql` adds `verification_state` — every pre-existing row defaults to `unverified`, because the corpus predates the honesty gate and has not earned the label. `00031_lsv_baseline_cache.sql` and `00032_lsv_deps_cache.sql` add the stage-7 LSV cache tables (baseline timings and the survey deps DB); both are private (`GRANT SELECT ... TO grafana_ro`, no anon) and squash the old `origin/lsv-cache-integration` cpu-model churn (`00016`→`00019`) into one clean pair.

To apply a new migration against the local instance:

```python
import psycopg2
conn = psycopg2.connect(host='127.0.0.1', port=54322, dbname='postgres', user='postgres', password='postgres')
conn.autocommit = True
conn.cursor().execute(open('supabase/migrations/00025_candidate_containers_build_manifest.sql').read())
```

**Do not grant broad access to `anon`.** Migration `00015_revoke_anon_select.sql` deliberately revoked Supabase's default `anon` `SELECT`; a new table is private by default and should stay that way. Only if a table is genuinely meant to be public do you add RLS plus a narrow grant, following the pattern in `00021_findings_tables.sql`:

```sql
ALTER TABLE <table> ENABLE ROW LEVEL SECURITY;
CREATE POLICY "public_read" ON <table> FOR SELECT TO anon USING (true);
GRANT SELECT ON <table> TO anon;   -- SELECT only, never GRANT ALL
```

### Querying

Use `datasmith.utils.db.fetch_all(table, select=..., filters=..., gte_filters=..., ...)` for paginated reads, or `get_client()` for direct Supabase client access. `afetch_all` and `abatch_upsert` are the async siblings.

Two kwargs carry the ingestion window: `lt_filters` is a strict less-than, so the half-open `[start, end)` bound means the same thing in the database as it does in the GitHub query it must agree with, and `in_filters` scopes a read to the keys the caller actually asked about so a skip set stops pulling its whole table (chunk the value list at the call site — PostgREST puts it in the URL).

`fetch_all` **refuses an unfiltered select of a large text column on a large table**, raising `UnfilteredLargeSelectError`: `fetch_all("pull_requests", select="patch")` streams `patch` across every row and has already killed PostgREST with an out-of-memory abort. Narrow it with any filter, ask for only the columns you need, or override the two sets via `DATASMITH_LARGE_TABLES` / `DATASMITH_LARGE_COLUMNS`.

### Level aggregation

Per-benchmark speedups are rolled up into four levels via **geometric mean** in `src/datasmith/harbor_adapter/template/parser.py` (`geometric_mean()` + `aggregate_by_hierarchy()`):

- **level1** — identity, one entry per benchmark (`module.Class.method`)
- **level2** — grouped by `module.Class` (drop the last dotted segment), geomean within each group
- **level3** — grouped by `module` (top dotted segment), geomean within each group
- **level4** — a single overall geomean across every benchmark

`benchmark_information.benchmark_name` is already param-stripped (e.g. `benchmarks.ConstructorsSuite.time_point`), so consumers can replicate the rollup locally without re-parsing. The FormulaCode website CSV's `1-Params` row is equivalent to datasmith's `level1`; the four upper website rows map onto `level2`/`level3`/`level4` modulo column naming.

### Task identity

The canonical identifier for one PR / one task is the tuple `(owner, repo, issue_number)`. Tables that need a single-column join key expose a `task_id` field whose value equals `issue_number`; never construct it as a derived string. `candidate_containers.task_id` is a STORED generated column (see migration `00019_candidate_containers_task_id.sql`).

The one exception is `task_id_map`, which exists precisely to translate *external* identifiers into this tuple. Its `legacy_task_id` and `canonical_task_id` columns are fc-eval's string formats — read them, never mint them, and never treat `canonical_task_id` as a substitute for the tuple inside datasmith.

## Environment setup

Requires a `tokens.env` file in the repo root with `GH_TOKEN`, `CACHE_LOCATION`, `SUPABASE_URL`, `SUPABASE_KEY`, and optionally `DSPY_*` vars for LLM backends and `DOCKERHUB_*` vars for publishing. See README.md for the full template.
