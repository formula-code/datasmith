# Pipeline (`fc-data`)

`fc-data` is the primary command for running fc-data. It discovers performance-improving commits from GitHub, classifies them with LLM agents, resolves dependencies, synthesizes Docker build contexts, and publishes verified images.

## Quick reference

```bash
# Run all 7 stages for a date range
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

* **`merged_at`, not `created_at`.** A run owns the work that *landed* while it
  was watching. A project with a deliberate review process merges PRs that were
  opened weeks or months earlier, and a `created_at` window silently drops all
  of them.
* **Half-open.** A PR merged at exactly midnight on `--end-date` belongs to the
  next run, so `[Jan, Feb)` and `[Feb, Mar)` partition the corpus instead of
  both claiming the boundary and doing every downstream step twice.

Stages 3 through 8 all get this from a single definition,
`datasmith.utils.db.window_filters`, rather than each spelling out its own
filter; stage 2 asks GitHub's search API the same question in the API's own
terms. Stage 1 (repository discovery) and stage 9 (benchmark-source scraping)
are deliberately unwindowed — stage 9 feeds the website, which wants the full
corpus rather than one run's slice of it.

The `±DATASMITH_NEIGHBOR_WINDOW_DAYS` band that stage 6 uses to enqueue
neighbor PRs is *not* this window. It is a similarity heuristic centred on one
PR, and it is meant to reach outside the run's range.

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
| `--agent AGENT` | Agent for stage 6 synthesis: `claude`, `codex`, `gemini`, `qwen`, or `none` | auto-detect |
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

If `--offline-source` is provided, also bulk-imports PR records from the Parquet file (useful for seeding the database with historical data).

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

Only PRs that pass a symbolic pre-filter (`is_performance_commit_symbolic = True`, set during scraping based on keywords in the PR title/body/labels) are sent to the LLM — this keeps costs down by filtering out obviously irrelevant PRs first.

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

For each performance-classified PR, checks out the repository at the merge commit SHA and emits a dependency **seed** — an environment in which the repository builds and asv runs. It is not a record of the environment as it was at commit time, and it is not a verdict on whether the container builds. Six units compose the stage:

1. **discover** — Scan the commit tree for packaging roots and choose one, deterministically. A directory that holds only a `requirements.txt` or an `environment.yml` is not a packaging root. The choice is stored in `primary_root` and becomes the image's `BUILD_ROOT`, so apache/arrow builds in `python/` rather than at the repository root
2. **declare** — Read only what the project *states* it needs: `[project].dependencies` and its optional groups, `setup.cfg` `install_requires`, a static `setup.py` parse, `[build-system].requires`, and the `asv.conf.json` `matrix.req`. No `requirements*.txt` glob, no `environment.yml`, and no names inferred from import statements. Every string is parsed with `packaging.requirements.Requirement`; one that will not parse is dropped and recorded, never rewritten, and never aborts its siblings
3. **interpreter** — Walk a declared ladder and record the rung that answered in `interpreter_source`: `requires-python`, then trove classifiers, then `asv.conf.json` `pythons`, then the newest release that existed at commit date. `DATASMITH_PYTHON_FLOOR` and `DATASMITH_PYTHON_CEILING` bound the answer
4. **pin** — One `uv pip compile` over the declared runtime and build requirements, with the commit date as `--exclude-newer`. If the cutoff makes the set unsatisfiable the compile is retried without it, and `cutoff_used` is then null. No `--all-extras`: extras are opt-in
5. **probe** — A dry-run install against the interpreter that was chosen, recorded in `probe_status` and `probe_log`. **Advisory only**
6. **emit** — One row per `(owner, repo, sha)`, carrying provenance: `resolver_version`, `uv_version`, `resolved_at`

Benchmark tooling — `asv`, `pytest`, `hypothesis`, `setuptools`, `wheel`, `pip`, `versioneer` — is stripped from the declared set *and* from the compiled one. The base image already installs it, and naming it again in `env_payload` only lets the seed fight the image over versions.

The seed may legitimately be empty. Where a project declares no dependencies, stage 4 writes `[]` and sets `probe_status = 'empty'`.

The resolved dependency set (`env_payload`) and the Python version used are consumed by stage 6 to build `docker_build_env.sh` — the shell script that installs dependencies inside the Docker image.

```bash
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 4
```

**Writes to:** `packages` table (`env_payload`, `python_version`, `interpreter_source`, `primary_root`, `requires_python`, `probe_status`, `probe_log`, `dropped_requirements`, `cutoff_used`, `resolver_version`, `uv_version`, `resolved_at`)
**Runner:** `ResolvePackagesRunner`

!!! note "Stage 4 gates nothing"
    Every PR proceeds, whatever the probe saw. `probe_status` is a queue *ordering* key — `installable`, then `unresolved`, then `failed`, then `empty` — so confidently-installable seeds run first and `--tasks-per-repo` caps a run. Stage 6 is the sole arbiter of buildability, because it is the only stage that builds in the real container and iterates on failure; its agent can replace the seed outright through `env_payload_override.json`.

    `can_install` is deprecated. It is nullable, and the redesigned runner neither reads nor writes it. Rows carrying `resolver_version = 'legacy'` came from the predecessor and have no provenance.

!!! tip "Diagnosing a thin seed"
    `dropped_requirements` records every requirement that was refused and why, so a missing package is diagnosable without a re-run. It is JSON-encoded text, like `env_payload`, so query it with a cast: `dropped_requirements::jsonb`. If resolution raises outright, check the `runner_failures` table.

---

### Stage 5: Render Problems

Builds a rich, deconstructed problem context for each PR. This context is what the LLM agent sees during synthesis (stage 6) and what ends up in the final FormulaCode dataset. The process:

1. **Scrape linked issues** — Follows `#123`, `owner/repo#456`, and GitHub URL references in the PR body via BFS (up to depth 2). Fetches the full issue body and comments for each linked issue.
2. **Extract problem observations** — Uses `ProblemExtractor` (an LLM agent) to separate the *problem description* from the *solution details* in the PR body. This prevents information leakage — the benchmark evaluator should see the problem, not how the author solved it.
3. **Persist components** — Stores the linked issues JSON, extracted observations, repo description, and raw PR fields in the `candidate_prs` table, allowing the problem statement to be re-rendered later without re-scraping.

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

The most complex stage. For each PR with resolved packages and a rendered problem context, generates a working Docker build context — the set of shell scripts (`docker_build_pkg.sh`, `docker_build_run.sh`, etc.) that build the repository, install dependencies, check out the right commit, and run ASV benchmarks.

The synthesizer is a state machine that tries the cheapest option first:

1. **Check cache** — Look up Supabase for an existing, verified build script for this exact PR
2. **Find similar** — Query for working scripts from the same repository (different PRs)
3. **Try similar** — Run each similar script against the verifier chain (smoke test + profile test)
4. **LLM generate** — Invoke a coding agent (Claude Code, Codex, or Gemini) in a sandboxed workspace to write the build scripts from scratch, iterating until verification passes or attempts are exhausted

Each attempt (success or failure) is logged to the `error_logs` table with the agent output, failure stage, return code, and stderr — making it possible to diagnose and retry later.

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
fc-data --start-date 2026-02-01 --end-date 2026-03-01 --stage 7
```

**Reads from:** `pull_requests`, `packages`, `candidate_containers`
**Requires:** `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`, `HF_TOKEN_PATH` in `tokens.env`. Docker daemon running.

---

## Typical workflow

A monthly update usually looks like this:

```bash
# 1. Run the full pipeline
fc-data --start-date 2026-03-01 --end-date 2026-04-01

# 2. If it gets interrupted, resume where it left off
fc-data --start-date 2026-03-01 --end-date 2026-04-01 --resume

# 3. After fixing a synthesis issue, re-run just stages 6-7
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
| `packages` | Stage 4 | Pinned `env_payload` and `python_version` per commit, plus `interpreter_source`, `primary_root`, the advisory `probe_status`, `dropped_requirements`, and provenance |
| `candidate_prs` | Stage 5 | Deconstructed PR context for re-rendering |
| `candidate_containers` | Stage 6 | Successful agent-generated build scripts per SHA |
| `error_logs` | Stage 6 | Per-attempt synthesis results (agent output, failure details) |
| `runner_progress` | All stages | Live progress counters (total/completed/failed) |
| `runner_failures` | All stages | Per-item failure details (error message, traceback) |
| `hook_cache` | Various | Memoization cache for `@supabase_cached` functions |
