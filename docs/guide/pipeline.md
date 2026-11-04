# Pipeline (`fc-data`)

`fc-data` runs the FormulaCode update pipeline. It finds performance commits on GitHub, classifies them with LLM agents, resolves dependencies, synthesizes Docker build contexts, checks them with the Harbor oracle and publishes the verified images and records.

## Quick reference

```bash
# Run all 9 stages for a date range
fc-data --start-date 2026-02-01 --end-date 2026-03-01

# Resume from where you left off (skips completed stages)
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --resume

# Run only specific stages
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 3
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 5 --stage 6

# Preview what would run without executing
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --dry-run
```

## The date window

`--start-date` and `--end-date` mean one thing everywhere: a PR belongs to a run
when its **`merged_at`** falls in the half-open interval `[start-date, end-date)`.

* **`merged_at`, not `created_at`.** A run takes the PRs that were merged in
  the window. Many projects merge PRs weeks or months after they were opened,
  and a `created_at` window would drop them.
* **Half-open.** A PR merged at midnight on `--end-date` belongs to the next
  run, so `[Jan, Feb)` and `[Feb, Mar)` do not overlap and no PR is processed twice.

Stages 3 to 8 use one definition, `datasmith.utils.db.window_filters`. Stage 2
asks the GitHub search API for the same window. Stage 1 (repository discovery)
and stage 9 (benchmark source) use no window, because stage 9 feeds the website,
which shows the whole corpus.

The `±DATASMITH_NEIGHBOR_WINDOW_DAYS` band that stage 6 uses to enqueue
neighbor PRs is a different window. It is centred on one PR and can reach
outside the run's range.

## CLI flags

| Flag | Description | Default |
|------|-------------|---------|
| `--start-date` | Start of date range to scan (YYYY-MM-DD) | **required** |
| `--end-date` | End of date range to scan (YYYY-MM-DD) | **required** |
| `--resume` | Skip already-completed stages, resume from next pending | `false` |
| `--stage N` | Run only stage N (1–9); repeat for multiple (e.g. `--stage 1 --stage 2`) | all stages |
| `--dry-run` | Log what each stage would do without executing | `false` |
| `--n-concurrent N` | Max concurrent items per runner stage | auto |
| `--tasks-per-repo N` | Cap tasks per repo for stages 5–7 | unlimited |
| `--agent AGENT` | Agent for stage 6 synthesis: `claude`, `codex`, `gemini`, `qwen`, or `none` | auto-detect |
| `--force` | Re-process already-completed items in stages 3–7 | `false` |
| `--offline-source PATH` | Import PR data from a Parquet file instead of scraping GitHub (stages 1–2) | — |
| `--min-stars N` | Minimum GitHub stars for repo discovery in stage 1 | `500` |
| `--harbor-environment` | Harbor environment for stage 7: `docker` or `daytona` | `docker` |
| `--harbor-rounds N` | LSV timing rounds per Harbor trial in stage 7 | `2` |
| `--harbor-limit N` | Cap total PRs sent to Harbor in stage 7 | no cap |
| `--tasks SPECS` | Comma-separated `owner/repo#PR` specs; stages 6–7 run only these and ignore the date window | — |

## Pipeline stages

The pipeline runs 9 stages in order. Each stage uses an async runner that tracks progress in Supabase. A failed item is logged and does not stop the run.

---

### Stage 1: Scrape Repos

Finds Python repositories that have ASV (Airspeed Velocity) benchmarks, from three sources:

1. **GitHub code search** — Searches for repositories containing `asv.conf.json`, filtered by language (Python) and minimum star count (`--min-stars`, default 500).
2. **Offline import** — If `--offline-source` is provided, imports repository names from a Parquet file.
3. **Existing repos** — Re-processes repositories already in the database to refresh metadata (description, stars, topics).

For each repository found, the runner fetches and stores metadata including description, primary language, star count, topics, and creation date.

```bash
# Discover repos with at least 1000 stars
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 1 --min-stars 1000

# Include repos from an offline dataset
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 1 --offline-source data.parquet
```

**Writes to:** `repositories` table
**Runner:** `ScrapeReposRunner`

---

### Stage 2: Scrape Commits

For every repository in the `repositories` table, scrapes all merged pull requests within the `--start-date` to `--end-date` range. For each PR, the runner fetches:

- **Metadata** — title, body, state, labels, merge commit SHA, base/head SHAs, timestamps
- **Diff** — the full unified diff of the PR
- **File changes** — per-file additions, deletions, and change counts
- **Timeline events** — comments, cross-references, review events (used later for link scraping)

If `--offline-source` is given, it also imports PR records from the Parquet file, for example to load historical data.

```bash
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 2
```

**Writes to:** `pull_requests` table (one row per PR)
**Runner:** `ScrapeCommitsRunner`

---

### Stage 3: Classify PRs

Runs two LLM agents in sequence on each unclassified PR:

1. **`PerfClassifier`** — Reads the PR title, body, diff, and file change summary. Outputs a binary YES/NO decision on whether this PR improves performance.
2. **`ClassifyJudge`** — For PRs classified as YES, determines the optimization type (one of 14 categories like "algorithmic", "caching", "parallelism", "memory") and difficulty level (easy/medium/hard).

Only PRs that pass a keyword filter (`is_performance_commit_symbolic = True`, set during scraping from the PR title, body and labels) go to the LLM. This keeps the LLM cost low.

```bash
# Classify all unclassified PRs
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 3

# Re-classify all PRs (including already-classified ones)
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 3 --force
```

**Updates:** `pull_requests` table (sets `is_performance_commit`, `classification`)
**Runner:** `ClassifyPRsRunner`
**Requires:** LLM backend (`DSPY_*` variables in `tokens.env`). See [Configuration](configuration.md).

---

### Stage 4: Resolve Packages

For each performance PR, checks out the repository at the merge commit SHA and writes a dependency **seed**: an environment in which the repository builds and asv runs. The seed is not the environment of the commit date, and it does not say whether the container builds. The stage has six steps:

1. **discover** — Scan the commit tree for packaging roots and choose one, deterministically. A directory that holds only a `requirements.txt` or an `environment.yml` is not a packaging root. The choice is stored in `primary_root` and becomes the image's `BUILD_ROOT`, so apache/arrow builds in `python/` rather than at the repository root
2. **declare** — Read only what the project *states* it needs: `[project].dependencies` and its optional groups, `setup.cfg` `install_requires`, a static `setup.py` parse, `[build-system].requires`, and the `asv.conf.json` `matrix.req`. No `requirements*.txt` glob, no `environment.yml`, and no names inferred from import statements. Every string is parsed with `packaging.requirements.Requirement`; one that will not parse is dropped and recorded, never rewritten, and never aborts its siblings
3. **interpreter** — Walk a declared ladder and record the rung that answered in `interpreter_source`: `requires-python`, then trove classifiers, then `asv.conf.json` `pythons`, then the newest release that existed at commit date. `DATASMITH_PYTHON_FLOOR` and `DATASMITH_PYTHON_CEILING` bound the answer
4. **pin** — One `uv pip compile` over the declared runtime and build requirements, with the commit date as `--exclude-newer`. If the cutoff makes the set unsatisfiable the compile is retried without it, and `cutoff_used` is then null. No `--all-extras`: extras are opt-in
5. **probe** — A dry-run install against the interpreter that was chosen, recorded in `probe_status` and `probe_log`. **Advisory only**
6. **emit** — One row per `(owner, repo, sha)`, carrying provenance: `resolver_version`, `uv_version`, `resolved_at`

Benchmark tools (`asv`, `pytest`, `hypothesis`, `setuptools`, `wheel`, `pip`, `versioneer`) are removed from the declared set and from the compiled set. The base image installs them, and a second pin in `env_payload` would conflict with the image's versions.

The seed can be empty. Where a project declares no dependencies, stage 4 writes `[]` and sets `probe_status = 'empty'`.

Stage 6 uses the resolved dependency set (`env_payload`) and the Python version in `docker_build_env.sh`, the script that installs dependencies inside the Docker image.

```bash
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 4
```

**Writes to:** `packages` table (`env_payload`, `python_version`, `interpreter_source`, `primary_root`, `requires_python`, `probe_status`, `probe_log`, `dropped_requirements`, `cutoff_used`, `resolver_version`, `uv_version`, `resolved_at`)
**Runner:** `ResolvePackagesRunner`

!!! note "Stage 4 gates nothing"
    Every PR goes on, whatever the probe saw. `probe_status` only orders the queue (`installable`, then `unresolved`, then `failed`, then `empty`), so seeds that install run first when `--tasks-per-repo` caps a run. Only stage 6 decides whether a commit builds, because it builds in the real container and retries on failure. Its agent can replace the seed through `env_payload_override.json`.

    The runner does not read or write the nullable `can_install` column. Rows with `resolver_version = 'legacy'` have no provenance.

!!! tip "Diagnosing a thin seed"
    `dropped_requirements` records each refused requirement and the reason, so you can find a missing package without a rerun. It is JSON-encoded text, like `env_payload`, so query it with a cast: `dropped_requirements::jsonb`. If resolution raises outright, check the `runner_failures` table.

---

### Stage 5: Render Problems

Builds the problem context for each PR. The synthesis agent (stage 6) reads it, and it goes into the FormulaCode dataset. The steps:

1. **Scrape linked issues** — Follows `#123`, `owner/repo#456`, and GitHub URL references in the PR body via BFS (up to depth 2). Fetches the full issue body and comments for each linked issue.
2. **Extract problem observations** — `ProblemExtractor` (an LLM agent) separates the *problem description* from the *solution details* in the PR body, so the task shows the problem and not the author's solution.
3. **Store components** — Stores the linked issues JSON, extracted observations, repo description and raw PR fields in the `candidate_prs` table, so the problem statement can be rendered again without a new scrape.

```bash
# Render problem contexts
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 5

# Limit to 3 PRs per repo (useful for testing)
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 5 --tasks-per-repo 3

# Re-render already-processed PRs
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 5 --force
```

**Writes to:** `candidate_prs` table
**Runner:** `RenderProblemsRunner`
**Requires:** LLM backend (for `ProblemExtractor`) and GitHub tokens (for issue scraping).

---

### Stage 6: Synthesize Images

For each PR with resolved packages and a rendered problem context, writes a working Docker build context: the shell scripts (`docker_build_pkg.sh`, `docker_build_run.sh` and others) that install dependencies, build the repository at the right commit and run the ASV benchmarks.

The synthesizer tries the cheapest option first: a cached context, then contexts from other PRs of the same repository, then the stock template, then an LLM coding agent in a sandbox. The [Synthesis guide](synthesis.md) describes each state.

Each attempt is logged to the `error_logs` table with the agent output, failure stage, return code and stderr, so a failure can be diagnosed and retried.

```bash
# Use the default auto-detected agent
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 6

# Force a specific agent
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 6 --agent claude

# Skip LLM generation entirely — only use cached/similar scripts
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 6 --agent none

# Limit concurrency and tasks per repo (controls cost)
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 6 --n-concurrent 2 --tasks-per-repo 5

# Re-synthesize already-completed PRs
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 6 --force
```

**Writes to:** `candidate_containers` table (on success), `error_logs` table (every attempt)
**Runner:** `SynthesizeImagesRunner`
**Requires:** Docker daemon running, resolved packages (stage 4), rendered problems (stage 5). LLM agent CLI (`claude`, `codex`, `gemini`, or `qwen`) must be on `$PATH` unless `--agent none`.

!!! warning
    Each LLM attempt uses many tokens and each Docker build takes minutes. Use `--n-concurrent` and `--tasks-per-repo` to limit cost. Run with `--agent none` first to use all cached and similar scripts before LLM generation.

---

### Stage 7: Harbor Health Check

Renders a Harbor task for each PR with a synthesized container and runs it with Harbor's oracle agent, which applies the human patch. All tasks go to Harbor in one job. Each trial, failed or not, writes one `harbor_runs` row with its speedup. A PR that already has a successful run with a speedup is skipped, unless `--force` or `--tasks` is given.

```bash
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 7 --harbor-environment daytona --n-concurrent 4
```

**Writes to:** `harbor_runs` table
**Requires:** Docker daemon (or Daytona access for `--harbor-environment daytona`). More rounds (`--harbor-rounds`) make each trial longer, and large repositories can exceed Harbor's verifier time limit.

---

### Stage 8: Publish

Selects the PRs to publish and pushes them:

1. **Select** — Performance PRs merged in the window with no `published_at`, that have a successful, timed `harbor_runs` row in an admitted environment, and whose container has `verification_state = 'verified'`. `DATASMITH_PUBLISH_ENVIRONMENTS` lists the admitted environments (default `daytona`).
2. **Push to DockerHub** — Pushes each selected container image under the `formulacode/` namespace.
3. **Push to HuggingFace** — Exports the records as versioned Parquet (for example `formulacode@2026-03`).
4. **Mark published** — Sets `published_at` on each published PR row.

`--dry-run` lists the image tags that the push would overwrite on DockerHub.

```bash
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 8
```

**Reads from:** `pull_requests`, `harbor_runs`, `candidate_containers`
**Requires:** `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`, `HF_TOKEN_PATH` in `tokens.env`. Docker daemon running.

---

### Stage 9: Scrape Benchmark Source

For one commit per repository in `candidate_containers`, checks out the repository, parses the ASV benchmark files and stores their source code for the website. This stage uses no date window.

```bash
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 9
```

**Writes to:** `benchmark_codes` table
**Runner:** `ScrapeBenchmarkSourceRunner`

---

## Typical workflow

A monthly update usually looks like this:

```bash
# 1. Run the full pipeline
fc-data --start-date 2026-03-01 --end-date 2026-04-01

# 2. If it gets interrupted, resume where it left off
fc-data --start-date 2026-03-01 --end-date 2026-04-01 --resume

# 3. After fixing a synthesis issue, run stages 6 and 7 again
fc-data --start-date 2026-03-01 --end-date 2026-04-01 --stage 6 --stage 7 --force
```

## Monitoring progress

Each runner writes live counters to the `runner_progress` Supabase table. You can monitor progress via Supabase Studio (default `http://127.0.0.1:54323`) or query directly:

```python
from datasmith.utils.db import get_client

sb = get_client()
rows = sb.table("runner_progress").select("*").execute()
for row in rows.data:
    print(f"{row['runner_name']}: {row['completed']}/{row['total']} ({row['failed']} failed)")
```

Per-item failures are logged to `runner_failures` with the error message and traceback, so you can diagnose them and run single stages again.

## Architecture

All runners extend `BaseRunner`, which provides:

- **Concurrency control** — `asyncio.Semaphore` limits parallelism (set via `--n-concurrent`)
- **Progress tracking** — Upserts to `runner_progress` every 10 items or 30 seconds
- **Error isolation** — Per-item failures are logged to `runner_failures`; one failure does not stop the runner
- **Shutdown** — CTRL+C stops agent subprocesses; press it twice to kill them

## Database tables

| Table | Written by | Purpose |
|-------|-----------|---------|
| `repositories` | Stage 1 | Tracked GitHub repos (language, stars, topics) |
| `pull_requests` | Stages 2–3 | PR metadata, classification, diffs, rendered problems |
| `packages` | Stage 4 | Pinned `env_payload` and `python_version` per commit, plus `interpreter_source`, `primary_root`, the advisory `probe_status`, `dropped_requirements`, and provenance |
| `candidate_prs` | Stage 5 | Deconstructed PR context for re-rendering |
| `candidate_containers` | Stage 6 | Successful agent-generated build scripts per SHA |
| `error_logs` | Stage 6 | Per-attempt synthesis results (agent output, failure details) |
| `harbor_runs` | Stage 7 | Oracle trial results and speedups |
| `benchmark_codes` | Stage 9 | ASV benchmark source code per repository |
| `runner_progress` | All stages | Live progress counters (total/completed/failed) |
| `runner_failures` | All stages | Per-item failure details (error message, traceback) |
| `hook_cache` | Various | Memoization cache for `@supabase_cached` functions |
