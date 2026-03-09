---
tags:
  - documentation
  - formulacode
  - datasmith
---

## Abstract

This document covers the `ds.publish` module — publishing verified tasks to DockerHub and HuggingFace with `@YYYY-MM` versioning.

## High level overview

```mermaid
graph LR
    A <--> B
    B --> C
    B --> D

    A[Supabase]
    B["`ds.publish
    (This Feature)`"]
    C[DockerHub]
    D[HuggingFace Hub]
```

## Publishing Pipeline

1. **Query Supabase** for all PRs with `container_name IS NOT NULL` and `published_at IS NULL` (or re-publish if forced).
2. **Push Docker images** to DockerHub under the `formulacode/` namespace.
3. **Generate dataset records** — convert each PR to a `FormulaCodeRecord` and serialize.
4. **Upload to HuggingFace** as a versioned dataset with a dataset card.
5. **Mark as published** in Supabase (`published_at = now()`, `version = @YYYY-MM`).

## Versioning Mechanics

- Dataset versioning follows `@YYYY-MM` (e.g. `formulacode@2026-03`).
- Each publish run creates a new version tag on the HuggingFace dataset.
- Versions are **append-only** — a new version includes all tasks from previous versions plus newly verified ones.
- The `latest` tag always points to the most recent version.

### Rollback strategy
If a published version contains bad data:
- Remove the version tag from HuggingFace (does not delete data, just the tag).
- Mark affected PRs in Supabase as `published_at = NULL` so they are re-evaluated.
- Re-publish with the same `@YYYY-MM` tag after fixes.
- Docker images on DockerHub are immutable once pushed — if an image itself is bad, push a corrected image with the same tag (DockerHub allows tag overwrites).

## HuggingFace Dataset Schema

Each row in the published dataset corresponds to a `FormulaCodeRecord`:

| Field | Type | Source |
|-------|------|--------|
| `task_id` | string | `{owner}__{repo}-{issue_number}` |
| `container_name` | string | Docker image reference |
| `patch` | string | PR diff patch |
| `gt_hash` | string | `pr.merge_commit_sha` |
| `base_commit` | string | `pr.base_sha` |
| `date` | string | `pr.merged_at` |
| `instructions` | string | Rendered problem statement |
| `classification` | string | Optimization category |
| `difficulty` | string | Normalized difficulty estimate |
| `image_name` | string | Full Docker image reference |

**Research needed**: HuggingFace datasets Hub API for versioned uploads, dataset card generation, whether `datasets.push_to_hub` supports version tags directly.

## Key Design Questions

### DockerHub publishing
**Resolved.**
- **Rate limits**: DockerHub push operations are rate-limited. The publish pipeline must include blocking/backoff on 429 responses (similar pattern to `ds.utils.tokens` for GitHub).
- **Delta publishing**: Only push new images — check if the tag already exists on DockerHub before pushing. Skip images that are already present.
- **Tag naming**: All images use `:latest` only. No version-specific tags (e.g. no `:2026-03`). Versioning lives entirely in the HuggingFace dataset, not in Docker image tags. This keeps the Docker side simple — each PR image has exactly one tag.

### HuggingFace publishing
**Partially resolved.**
- **Dataset format**: Parquet. It's HuggingFace's recommended format — efficient compression, rich typing, and the Hub auto-generates a dataset viewer for Parquet files.
- **Versioning**: `push_to_hub` does not support version tags directly. The workflow is:
  1. `dataset.push_to_hub("formulacode/formulacode")` — pushes to `main` branch.
  2. `huggingface_hub.create_tag("formulacode/formulacode", tag="2026-03", repo_type="dataset")` — creates a git tag for the version.
  3. Users load a specific version with `load_dataset("formulacode/formulacode", revision="2026-03")`.
- **Dataset card**: Create a `README.md` with YAML frontmatter (license, language, task categories). Can auto-generate from Supabase metadata (task count, date range, repository list). HuggingFace provides a template format.
- **Visibility**: Start private during staging, flip to public on release. Set `private=True` in `push_to_hub`, then toggle via the Hub UI or API when ready.

## Verification

* Unit tests with mocked DockerHub and HuggingFace APIs.
* Integration test: publish a small set of verified PRs, verify dataset appears on HuggingFace with correct schema.
* Rollback test: publish, roll back, re-publish, verify data integrity.
* Idempotency test: running publish twice with the same data produces the same result.

## Current implementation details

### DockerHub publishing

Implemented in `src/datasmith/docker/dockerhub.py:publish_images_to_dockerhub()`:
- **Single mode** (default): all images pushed to `{namespace}/{single_repo}:{encoded_tag}` (e.g., `formulacode/all:owner-repo-sha--final`). Tag encoding: `/` → `__`, `:` → `--`. Tags >128 chars truncated with SHA256 suffix.
- **Mirror mode**: each local repo maps to `{namespace}/{prefix}-{local_repo}`.
- **Delta publishing**: checks existing tags via Docker Registry HTTP API v2 (`https://registry.hub.docker.com/v2/{namespace}/{repo}/tags/list`) with bearer token auth. Skips existing images by default.
- **Rate limiting**: exponential backoff on 429 (`wait_time = min(rate_limit_wait * 2^attempt, 3600)`), max 3 retries per image. Configurable via `DOCKERHUB_RATE_LIMIT_WAIT` env var (default: 60s).
- **Parallel push**: configurable workers (default: 4).
- **Credentials**: checked in order: function params → env vars (`DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN`/`DOCKERHUB_PASSWORD`) → `~/.docker/config.json` (base64-decoded auth).

Also callable per-task via `DockerContext.build_and_publish_to_dockerhub()` in `src/datasmith/docker/context.py:796-909`, which builds the image and then pushes.

### Task model (current equivalent of FormulaCodeRecord)
**Assessment: Merge into PR model.** `Task` duplicates identity information (owner, repo, sha) that already lives on the PR. The build-specific fields (`env_payload`, `python_version`, `tag`, `benchmarks`) should become attributes of the PR/Issue model or a lightweight build config attached to it. `get_image_name()` and `with_tag()` are useful interfaces worth preserving, but `Task` as a standalone entity creates a parallel identity system that drifts from the PR data.

`Task` frozen dataclass in `src/datasmith/core/models/task.py`:
```python
Task(owner, repo, sha, commit_date, env_payload, python_version, tag, benchmarks)
```
- `get_image_name()` → `{owner}-{repo}-{sha}:{tag}`
- `with_tag(tag)` → copy with different tag
- `with_benchmarks(benchmarks)` → copy with benchmark info

`BuildResult` dataclass in `src/datasmith/core/models/build.py`:
```python
BuildResult(ok, image_name, image_id, rc, duration_s, stderr_tail, stdout_tail, failure_stage, benchmarks)
```

No `FormulaCodeRecord` dataclass exists. No `PR.to_record()` method. The bridge between PR data and terminal-bench is not yet implemented.

### Verification records

- Per-task: `dataset/formulacode_verified/{owner}_{repo}/{sha}/verification_success.json` containing `{"local_image": "...", "dockerhub_image": "..."}`.
- Aggregated: `dataset/all_verification_successes.jsonl` — one JSON object per line with `dockerhub_image` field only.
- ~113 verified tasks as of 2026-02-28.

### Pipeline storage

Pipeline data stored in SQLite (`PIPELINE_DB` env var, default: `scratch/artifacts/pipeflush.db`), not Supabase. Tables are pandas DataFrames written via `write_table()` in `src/datasmith/core/storage.py`. Complex columns JSON-serialized.

### Versioning

Not implemented. No `@YYYY-MM` versioning scheme in code. Docker images use `:final` tag only (no version-specific tags).

### HuggingFace publishing

Not implemented. No `push_to_hub()`, no HuggingFace token handling, no dataset card generation, no Parquet export for HuggingFace.

### Publishing orchestration

`scratch/scripts/build_and_publish_to_dockerhub.py` — reads commit list, processes via `ThreadPoolExecutor` with `threading.Semaphore` (build: 24 concurrent, push: 8 concurrent). Accumulates results in JSONL. On pkg-stage build failure, removes failed context from `ContextRegistry` and saves immediately.

### Configuration

| Env var | Default | Purpose |
|---------|---------|---------|
| `DOCKERHUB_NAMESPACE` | Required | DockerHub namespace |
| `DOCKERHUB_USERNAME` | Required | DockerHub user |
| `DOCKERHUB_TOKEN` / `DOCKERHUB_PASSWORD` | Required | Auth token |
| `DOCKERHUB_RATE_LIMIT_WAIT` | 60 | Seconds to wait on rate limit |
| `DOCKERHUB_SINGLE_REPO` | `all` | Repo name in single mode |
| `BUILD_CONCURRENCY` | 24 | Max parallel builds |
| `PUSH_CONCURRENCY` | 8 | Max parallel pushes |
