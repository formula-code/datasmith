# Pipeline (`ds-update`)

`ds-update` is the primary command for running DataSmith. It discovers performance-improving commits from GitHub, classifies them with LLM agents, resolves dependencies, synthesizes Docker build contexts, and publishes verified images.

## Quick reference

```bash
# Run all 7 stages for a date range
ds-update --start-date 2026-02-01 --end-date 2026-03-01

# Resume from where you left off (skips completed stages)
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --resume

# Run only specific stages
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 3
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 5 --stage 6

# Preview what would run without executing
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --dry-run
```

## CLI flags

| Flag | Description | Default |
|------|-------------|---------|
| `--start-date` | Start of date range to scan (YYYY-MM-DD) | **required** |
| `--end-date` | End of date range to scan (YYYY-MM-DD) | **required** |
| `--resume` | Skip already-completed stages, resume from next pending | `false` |
| `--stage N` | Run only stage N (1–7); repeat for multiple (e.g. `--stage 1 --stage 2`) | all stages |
| `--dry-run` | Log what each stage would do without executing | `false` |
| `--n-concurrent N` | Max concurrent items per runner stage | auto |
| `--tasks-per-repo N` | Cap tasks per repo for stages 5–6 (useful for large repos) | unlimited |
| `--agent AGENT` | Agent for stage 6 synthesis: `claude`, `codex`, `gemini`, or `none` | auto-detect |
| `--force` | Re-process already-completed tasks in stages 5–6 | `false` |
| `--offline-source PATH` | Import PR data from a Parquet file instead of scraping GitHub (stages 1–2) | — |
| `--min-stars N` | Minimum GitHub stars for repo discovery in stage 1 | `500` |

## Pipeline stages

The pipeline runs 7 stages in order. Each stage is backed by an async runner that tracks progress in Supabase and isolates per-item failures (a single failure never aborts the run).

---

### Stage 1: Scrape Repos

Discovers Python repositories that have ASV (Airspeed Velocity) benchmarks. The discovery strategy combines multiple sources:

1. **GitHub code search** — Searches for repositories containing `asv.conf.json`, filtered by language (Python) and minimum star count (`--min-stars`, default 500).
2. **Offline import** — If `--offline-source` is provided, imports repository names from a Parquet file.
3. **Existing repos** — Re-processes repositories already in the database to refresh metadata (description, stars, topics).

For each repository found, the runner fetches and stores metadata including description, primary language, star count, topics, and creation date.

```bash
# Discover repos with at least 1000 stars
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 1 --min-stars 1000

# Include repos from an offline dataset
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 1 --offline-source data.parquet
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

If `--offline-source` is provided, also bulk-imports PR records from the Parquet file (useful for seeding the database with historical data).

```bash
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 2
```

**Writes to:** `pull_requests` table (one row per PR)
**Runner:** `ScrapeCommitsRunner`

---

### Stage 3: Classify PRs

Runs two LLM agents in sequence on each unclassified PR:

1. **`PerfClassifier`** — Reads the PR title, body, diff, and file change summary. Outputs a binary YES/NO decision on whether this PR improves performance.
2. **`ClassifyJudge`** — For PRs classified as YES, determines the optimization type (one of 14 categories like "algorithmic", "caching", "parallelism", "memory") and difficulty level (easy/medium/hard).

Only PRs that pass a symbolic pre-filter (`is_performance_commit_symbolic = True`, set during scraping based on keywords in the PR title/body/labels) are sent to the LLM — this keeps costs down by filtering out obviously irrelevant PRs first.

```bash
# Classify all unclassified PRs
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 3

# Re-classify all PRs (including already-classified ones)
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 3 --force
```

**Updates:** `pull_requests` table (sets `is_performance_commit`, `classification`)
**Runner:** `ClassifyPRsRunner`
**Requires:** LLM backend (`DSPY_*` variables in `tokens.env`). See [Configuration](configuration.md).

---

### Stage 4: Resolve Packages

For each performance-classified PR, checks out the repository at the merge commit SHA and resolves a complete, pinned Python dependency set. The process:

1. **Parse metadata** — Reads `pyproject.toml`, `setup.py`, or `setup.cfg` to extract declared dependencies
2. **Resolve with uv** — Runs `uv pip compile` to produce a fully pinned requirements file, trying multiple Python versions if needed
3. **Validate installability** — Confirms the resolved set can actually be installed (sets `can_install = True/False`)

The resolved dependency set (`env_payload`) and the Python version used are stored in the `packages` table. These are consumed by stage 6 to build `docker_build_env.sh` — the shell script that installs dependencies inside the Docker image.

```bash
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 4
```

**Writes to:** `packages` table (`env_payload`, `python_version`, `can_install`)
**Runner:** `ResolvePackagesRunner`

!!! note
    Only PRs with `can_install = True` proceed to later stages. If resolution fails for a commit, check the `runner_failures` table for the error.

---

### Stage 5: Render Problems

Builds a rich, deconstructed problem context for each PR. This context is what the LLM agent sees during synthesis (stage 6) and what ends up in the final FormulaCode dataset. The process:

1. **Scrape linked issues** — Follows `#123`, `owner/repo#456`, and GitHub URL references in the PR body via BFS (up to depth 2). Fetches the full issue body and comments for each linked issue.
2. **Extract problem observations** — Uses `ProblemExtractor` (an LLM agent) to separate the *problem description* from the *solution details* in the PR body. This prevents information leakage — the benchmark evaluator should see the problem, not how the author solved it.
3. **Persist components** — Stores the linked issues JSON, extracted observations, repo description, and raw PR fields in the `candidate_prs` table, allowing the problem statement to be re-rendered later without re-scraping.

```bash
# Render problem contexts
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 5

# Limit to 3 PRs per repo (useful for testing)
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 5 --tasks-per-repo 3

# Re-render already-processed PRs
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 5 --force
```

**Writes to:** `candidate_prs` table
**Runner:** `RenderProblemsRunner`
**Requires:** LLM backend (for `ProblemExtractor`) and GitHub tokens (for issue scraping).

---

### Stage 6: Synthesize Images

The most complex stage. For each PR with resolved packages and a rendered problem context, generates a working Docker build context — the set of shell scripts (`docker_build_pkg.sh`, `docker_build_run.sh`, etc.) that build the repository, install dependencies, check out the right commit, and run ASV benchmarks.

The synthesizer is a state machine that tries the cheapest option first:

1. **Check cache** — Look up Supabase for an existing, verified build script for this exact PR
2. **Find similar** — Query for working scripts from the same repository (different PRs)
3. **Try similar** — Run each similar script against the verifier chain (smoke test + profile test)
4. **LLM generate** — Invoke a coding agent (Claude Code, Codex, or Gemini) in a sandboxed workspace to write the build scripts from scratch, iterating until verification passes or attempts are exhausted

Each attempt (success or failure) is logged to the `error_logs` table with the agent output, failure stage, return code, and stderr — making it possible to diagnose and retry later.

```bash
# Use the default auto-detected agent
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 6

# Force a specific agent
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 6 --agent claude

# Skip LLM generation entirely — only use cached/similar scripts
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 6 --agent none

# Limit concurrency and tasks per repo (controls cost)
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 6 --n-concurrent 2 --tasks-per-repo 5

# Re-synthesize already-completed PRs
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 6 --force
```

**Writes to:** `candidate_containers` table (on success), `error_logs` table (every attempt)
**Runner:** `SynthesizeImagesRunner`
**Requires:** Docker daemon running, resolved packages (stage 4), rendered problems (stage 5). LLM agent CLI (`claude`, `codex`, or `gemini`) must be on `$PATH` unless `--agent none`.

!!! warning
    Synthesis can be expensive — each LLM attempt may consume significant tokens and each Docker build takes minutes. Use `--n-concurrent` and `--tasks-per-repo` to control cost. Start with `--agent none` to exhaust cached/similar scripts before using LLM generation.

---

### Stage 7: Publish

The final stage. Builds three-tier Docker images (base → repo → PR) from synthesized contexts, verifies them, and pushes to DockerHub. Then exports all verified PRs as a versioned Parquet dataset to HuggingFace.

The publish pipeline:

1. **Query** — Fetches all performance-classified PRs with successful synthesis (`container_name IS NOT NULL`) that haven't been published yet
2. **Build images** — Constructs the Docker image hierarchy: base image, per-repo image, per-PR image
3. **Verify** — Runs the verifier chain (smoke import test + ASV profile test) on each PR image
4. **Push to DockerHub** — Publishes verified images under the `formulacode/` namespace
5. **Push to HuggingFace** — Exports FormulaCode records as versioned Parquet (e.g., `formulacode@2026-03`)
6. **Mark published** — Sets `published_at` timestamp on each published PR row

```bash
ds-update --start-date 2026-02-01 --end-date 2026-03-01 --stage 7
```

**Reads from:** `pull_requests`, `packages`, `candidate_containers`
**Requires:** `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`, `HF_TOKEN_PATH` in `tokens.env`. Docker daemon running.

---

## Typical workflow

A monthly update usually looks like this:

```bash
# 1. Run the full pipeline
ds-update --start-date 2026-03-01 --end-date 2026-04-01

# 2. If it gets interrupted, resume where it left off
ds-update --start-date 2026-03-01 --end-date 2026-04-01 --resume

# 3. After fixing a synthesis issue, re-run just stages 6-7
ds-update --start-date 2026-03-01 --end-date 2026-04-01 --stage 6 --stage 7 --force
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

Per-item failures are logged to `runner_failures` with full error messages and tracebacks, so you can diagnose and re-run specific stages without restarting from scratch.

## Architecture

All runners extend `BaseRunner`, which provides:

- **Concurrency control** — `asyncio.Semaphore` limits parallelism (set via `--n-concurrent`)
- **Progress tracking** — Upserts to `runner_progress` every 10 items or 30 seconds
- **Error isolation** — Per-item failures logged to `runner_failures`; the runner never aborts
- **Graceful shutdown** — CTRL+C terminates agent subprocesses cleanly; press twice to force-kill

## Database tables

| Table | Written by | Purpose |
|-------|-----------|---------|
| `repositories` | Stage 1 | Tracked GitHub repos (language, stars, topics) |
| `pull_requests` | Stages 2–3 | PR metadata, classification, diffs, rendered problems |
| `packages` | Stage 4 | Pinned `env_payload` and `python_version` per commit |
| `candidate_prs` | Stage 5 | Deconstructed PR context for re-rendering |
| `candidate_containers` | Stage 6 | Successful agent-generated build scripts per SHA |
| `error_logs` | Stage 6 | Per-attempt synthesis results (agent output, failure details) |
| `runner_progress` | All stages | Live progress counters (total/completed/failed) |
| `runner_failures` | All stages | Per-item failure details (error message, traceback) |
| `hook_cache` | Various | Memoization cache for `@supabase_cached` functions |
