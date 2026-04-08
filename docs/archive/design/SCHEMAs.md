# FormulaCode Pipeline Data Schemas

This document describes every data file consumed and produced by `scratch/scripts/update_formulacode.py` and its constituent pipeline scripts. For each file, we list the exact CLI arguments, row-level schema, and provenance of nested/complex fields.

---

## Pipeline Overview

```
repos_valid.csv
    |
    v
[Step 1] collect_and_filter_commits.py
    |
    v
merge_commits_filtered_{dates}.parquet            (48 columns, ~78k rows)
    |
    v
[Step 2] prepare_commits_for_building_reports.py
    |
    v
merge_commits_filtered_with_patch_{dates}.parquet  (146 columns, ~26k rows)
    |
    v
[Step 3] collect_perf_commits.py
    |  produces two files:
    |  perfonly_commits_{dates}.raw.parquet   (all rows + enrichment)
    v  perfonly_commits_{dates}.parquet       (is_performance_commit == True only)
perfonly_commits_{dates}.parquet                    (161 columns, ~2.9k rows)
    |
    +---> [Step 4] synthesize_contexts.py
    |         outputs: results_synthesis/*.pkl, results.jsonl,
    |                  all_files_by_image.json, context_registry.json
    |
    +---> [Step 5] build_and_publish_to_dockerhub.py
    |         reads: perfonly parquet + context_registry.json
    |         outputs: Docker images pushed to DockerHub
    |
    v
[Step 6] merge_perfonly_commits_master.py
    |
    v
perfonly_commits_master.parquet                    (161 columns, de-duped union)
```

---

## File 0: `repos_valid.csv`

**Role:** Seed input -- the curated list of ASV-enabled repositories to scrape.

**Produced by:** Manual curation + `collect_commits.py` validation.

**CLI (consumed by Step 1):**
```
--filtered-benchmarks-pth scratch/artifacts/pipeflush/repos_valid.csv
```

### Schema (9 columns)

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `repo_name` | str | GitHub `owner/repo` identifier | `scikit-learn/scikit-learn` |
| `url` | str | Full GitHub URL | `https://github.com/scikit-learn/scikit-learn` |
| `is_accessible` | bool | Whether the repo is publicly accessible | `True` |
| `is_fork` | bool | Whether the repo is a fork | `False` |
| `is_archived` | bool | Whether the repo is archived | `False` |
| `fork_parent` | float | Parent repo ID if forked (NaN otherwise) | `NaN` |
| `forked_at` | float | Timestamp of fork (NaN if not a fork) | `NaN` |
| `watchers` | int | Number of watchers | `2135` |
| `stars` | int | Number of GitHub stars | `62570` |

### Related: `repos_discovered.csv`

A headerless, single-column CSV of `owner/repo` strings -- the raw list of all repos found via GitHub Search API before validation filtering. ~766 entries.

---

## File 1: `merge_commits_filtered_{dates}.parquet`

**Role:** All merged PRs for the target repos/date-range that modified "core" files and have `asv.conf.json`.

**Produced by:** `collect_and_filter_commits.py` (Step 1).

**CLI:**
```
python collect_and_filter_commits.py \
  --filtered-benchmarks-pth repos_valid.csv \     # input
  --output-pth merge_commits_filtered.parquet \    # output
  --threads 8 --procs 32 \
  --since 2025-10-01 --until 2025-11-01
```

### Schema (48 columns)

Each row represents one merged PR/commit for a repository that has ASV benchmarks.

#### Commit Metadata (from `_get_commit_info_offline()`)

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `sha` | str | Git merge commit SHA | `5b8378c077dc0160...` |
| `date` | str | ISO-8601 commit timestamp | `2020-03-06T21:16:00+01:00` |
| `message` | str | Full commit message | `Benchmarking suite (asv) (#96)\n\n` |
| `total_additions` | int | Lines added across all files | `180` |
| `total_deletions` | int | Lines deleted | `0` |
| `total_files_changed` | int | Number of files modified | `4` |
| `files_changed` | str | Newline-separated list of changed file paths | `.gitignore\nasv.conf.json\n...` |
| `patch` | str | Full `git format-patch` output for the commit | `From 5b8378c0... Subject: [PATCH]...` |
| `has_asv` | bool | Whether repo has `asv.conf.json` at this commit | `True` |
| `file_change_summary` | str | Markdown table of per-file additions/deletions | `\| File \| Lines Added \|...` |
| `kind` | str | Always `"commit"` | `commit` |
| `repo_name` | str | GitHub `owner/repo` | `pygeos/pygeos` |

#### PR Metadata (from GitHub API via `collect_merge_shas()`, prefixed `pr_`)

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `pr_url` | str | GitHub API URL for the PR | `https://api.github.com/repos/.../pulls/96` |
| `pr_id` | int | GitHub PR ID | `377342901` |
| `pr_node_id` | str | GitHub GraphQL node ID | `MDExOlB1bGxSZXF1ZXN0...` |
| `pr_html_url` | str | Human-readable PR URL | `https://github.com/.../pull/96` |
| `pr_diff_url` | str | URL to `.diff` format | `https://github.com/.../pull/96.diff` |
| `pr_patch_url` | str | URL to `.patch` format | `https://github.com/.../pull/96.patch` |
| `pr_issue_url` | str | API URL for the associated issue | `https://api.github.com/repos/.../issues/96` |
| `pr_number` | int | PR number | `96` |
| `pr_state` | str | PR state (always `"closed"`) | `closed` |
| `pr_locked` | bool | Whether PR is locked | `False` |
| `pr_title` | str | PR title | `Benchmarking suite (asv)` |
| `pr_user` | dict | PR author info (GitHub user object) | `{"avatar_url": ..., "login": ...}` |
| `pr_body` | str | PR description body (markdown) | `See here for an example...` |
| `pr_created_at` | str | ISO timestamp of PR creation | `2020-02-19T19:09:38Z` |
| `pr_updated_at` | str | ISO timestamp of last update | `2020-03-10T08:57:29Z` |
| `pr_closed_at` | str | ISO timestamp when PR was closed | `2020-03-06T20:16:01Z` |
| `pr_merged_at` | str | ISO timestamp when PR was merged | `2020-03-06T20:16:01Z` |
| `pr_merge_commit_sha` | str | SHA of the merge commit | `5b8378c077dc0160...` |
| `pr_assignee` | dict/None | Assigned reviewer | `None` |
| `pr_assignees` | list[dict] | All assigned reviewers | `[]` |
| `pr_requested_reviewers` | list[dict] | Requested reviewers | `[]` |
| `pr_requested_teams` | list[dict] | Requested team reviewers | `[]` |
| `pr_labels` | list[dict] | PR labels | `[]` |
| `pr_milestone` | dict/None | Associated milestone | `None` |
| `pr_draft` | bool | Whether PR is a draft | `False` |
| `pr_commits_url` | str | API URL for PR commits | `https://api.github.com/.../pulls/96/commits` |
| `pr_review_comments_url` | str | API URL for review comments | `https://api.github.com/.../pulls/96/comments` |
| `pr_review_comment_url` | str | Template URL for review comments | `https://api.github.com/.../comments{/number}` |
| `pr_comments_url` | str | API URL for issue comments | `https://api.github.com/.../issues/96/comments` |
| `pr_statuses_url` | str | API URL for commit statuses | `https://api.github.com/.../statuses/{sha}` |
| `pr_head` | dict | Head branch info (label, ref, sha, repo object) | `{"label": "user:branch", "ref": ...}` |
| `pr_base` | dict | Base branch info (label, ref, sha, repo object) | `{"label": "org:main", "ref": ..., "sha": ...}` |
| `pr__links` | dict | HATEOAS links for PR | `{"comments": {"href": ...}, ...}` |
| `pr_author_association` | str | Author's relationship to repo | `MEMBER` |
| `pr_auto_merge` | dict/None | Auto-merge configuration | `None` |
| `pr_active_lock_reason` | str/None | Lock reason if locked | `None` |

#### `pr_base` nested dict structure (important -- used downstream)

```json
{
  "label": "pygeos:master",
  "ref": "master",
  "sha": "d51e87ec1bd230bffb05882b3bf84b52540a89d1",
  "repo": {
    "id": 191151963,
    "full_name": "pygeos/pygeos",
    "description": "Wraps GEOS geometry functions...",
    "language": "Python",
    "default_branch": "master",
    "license": {"key": "bsd-3-clause", ...},
    ... (full GitHub repository object)
  }
}
```

The `pr_base.sha` field is the **base commit SHA** used by downstream steps as the commit to check out for benchmarking.

---

## File 2: `merge_commits_filtered_with_patch_{dates}.parquet`

**Role:** Filtered + enriched commits with dependency analysis and fresh patches.

**Produced by:** `prepare_commits_for_building_reports.py` (Step 2).

**CLI:**
```
python prepare_commits_for_building_reports.py \
  --input merge_commits_filtered.parquet \         # input (File 1)
  --output merge_commits_filtered_with_patch.parquet \ # output
  --max-workers 200 \
  --fetch-patches
```

### Schema (146 columns)

Inherits all 48 columns from File 1 (with `patch` renamed to `original_patch` and fresh diff stored as `patch`), plus the following additions:

#### Filtering columns (from `crude_perf_filter()`)

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `n_patch_tokens` | int | Token count of patch text (tiktoken `o200k_base`) | `840` |
| `total_changes` | int | `total_additions + total_deletions` | `49` |
| `n_files_changed` | int | Count of files in `files_changed` | `3` |
| `is_perf` | bool | Whether commit message passes `basic_message_filter()` | `True` |

#### Dependency analysis columns (from `analyze_commit()`, prefixed `analysis_`)

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `analysis_sha` | str | Commit SHA analyzed (= `pr_base.sha`) | `d51e87ec1bd230bf...` |
| `analysis_repo_name` | str | Repository name | `pygeos/pygeos` |
| `analysis_package_name` | str | PyPI package name | `pygeos` |
| `analysis_package_version` | str/None | Package version if detectable | `None` |
| `analysis_python_version` | str | Selected Python version for env | `3.8` |
| `analysis_build_command` | list[str] | Build commands from ASV config | `[]` |
| `analysis_install_command` | list[str] | Install commands from ASV config | `[]` |
| `analysis_final_dependencies` | list[str] | Resolved pip dependency list | `["attrs==19.3.0", "numpy==1.18.5", ...]` |
| `analysis_can_install` | bool | Whether deps install successfully (dry-run) | `True` |
| `analysis_dry_run_log` | str | Full log from `uv pip install --dry-run` | `\nUsing Python 3.8.20...` |
| `analysis_primary_root` | str | Root directory of the package | `.` |
| `analysis_resolution_strategy` | str | Strategy descriptor | `cutoff=strict, extras=on, python=3.8` |

#### Expanded `pr_base.repo` columns (prefixed `pr_base_`)

~80 columns from the GitHub repository object nested inside `pr_base`, flattened with the `pr_base_` prefix. Key ones:

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `pr_base_sha` | str | Base commit SHA (the commit to benchmark against) | `d51e87ec1bd230bf...` |
| `pr_base_full_name` | str | Full repo name | `pygeos/pygeos` |
| `pr_base_default_branch` | str | Default branch name | `master` |
| `pr_base_language` | str | Primary language | `Python` |
| `pr_base_description` | str | Repo description | `Wraps GEOS geometry functions...` |
| `pr_base_stargazers_count` | int | Stars count | `388` |
| `pr_base_license` | dict | License info | `{"key": "bsd-3-clause", ...}` |
| `pr_base_clone_url` | str | Git clone URL | `https://github.com/pygeos/pygeos.git` |
| ... | ... | ~70 more GitHub repo metadata fields | ... |

#### Container and patch columns

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `container_name` | str | Docker image name for this commit | `pygeos-pygeos-d51e87ec...:run` |
| `original_patch` | str | Original `git format-patch` output (renamed from `patch`) | `From 01fbbe37b27...` |
| `patch` | str | Fresh diff from GitHub API (replaces original) | `diff --git a/.dockerignore...` |

---

## File 3: `perfonly_commits_{dates}.parquet`

**Role:** Only performance-improving commits, enriched with LLM classification.

**Produced by:** `collect_perf_commits.py` (Step 3).

**CLI:**
```
python collect_perf_commits.py \
  --commits merge_commits_filtered_with_patch.parquet \  # input (File 2)
  --outfile perfonly_commits \                            # output prefix
  --max-workers 32
```

**Outputs two files:**
- `perfonly_commits_{dates}.raw.parquet` -- all rows with enrichment columns
- `perfonly_commits_{dates}.parquet` -- only rows where `is_performance_commit == True`

### Schema (161 columns)

Inherits all 146 columns from File 2, plus 15 new columns from `ReportBuilder.build()`:

#### LLM Classification columns (from `ReportBuilder`)

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `is_performance_commit` | bool | LLM-determined performance relevance | `True` |
| `classification` | str | Performance optimization category | `Cache & reuse` |
| `difficulty` | str | Estimated difficulty level | `medium` |
| `classification_reason` | str | LLM's reasoning | `The primary optimization technique...` |
| `classification_confidence` | float | Confidence score (0-100) | `90.0` |

#### Problem statement columns (from `ReportBuilder`)

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `problem_statement` | str | LLM-extracted problem description | `The main problem is how to...` |
| `problem_statement_with_sol` | str | Problem description including solution hints | `The main problem is...` |
| `raw_problem_statement` | str | Raw PR title + body + comments concatenated | `### ENH: prepared geometry...` |
| `hints` | str | Extracted hints section for the evaluator | `The following information may help...` |

#### Report rendering columns (from `ReportBuilder`)

| Column | Dtype | Description | Example |
|--------|-------|-------------|---------|
| `final_md` | str | Full markdown prompt for LLM evaluation | `**Role:**\nYou are a performance...` |
| `final_md_no_hints` | str | Same report but without hints section | `**Role:**\nYou are a performance...` |
| `final_with_sol` | str | Report including the solution patch | (empty str or full report) |
| `all_data` | str (JSON) | Structured context blob (serialized dict) | `{"hints_context": {...}, ...}` |
| `problem_sections` | str (JSON) | Structured LLM extraction output | `{"problem_statement": "...", ...}` |
| `final_results` | str (JSON) | Individual rendered sections | `{"final_report": "...", ...}` |

#### `all_data` JSON structure (key fields)

```json
{
  "hints_context": {
    "items": [
      {"user_login": "...", "timestamp": "...", "body": "..."}
    ]
  },
  "issues_expanded": [
    {"title": "...", "body": "...", "comments": [...]}
  ],
  "performance_section": "...",
  "raw_comments": "...",
  "raw_pr_title": "...",
  "raw_pr_body": "...",
  "raw_patch": "...",
  "raw_file_change_summary": "...",
  "classification": "Cache & reuse",
  "difficulty": "medium",
  "classification_reason": "...",
  "classification_confidence": 90,
  "raw_issue_data": [{"title": "...", "body": "..."}],
  "git_issue_str": "..."
}
```

---

## File 4: `context_registry.json` (and `context_registry_final_filtered.json`)

**Role:** Maps `(owner, repo, sha)` tasks to their Docker build contexts (shell scripts for building the ASV environment).

**Produced/updated by:** `synthesize_contexts.py` (Step 4).

**CLI:**
```
python synthesize_contexts.py \
  --commits perfonly_commits.parquet \       # input (File 3)
  --output-dir results_synthesis/ \         # output directory
  --context-registry context_registry.json  # input/output
```

### JSON Structure

Top-level keys: `"contexts"`, `"default"`.

```json
{
  "contexts": {
    "Task(owner='Qiskit', repo='qiskit', sha='6df146fa...', ...)": {
      "dockerfile_data": "FROM ...",
      "entrypoint_data": "#!/bin/bash\n...",
      "env_building_data": "#!/bin/bash\n...",
      "run_building_data": "#!/bin/bash\n...",
      "base_building_data": "#!/bin/bash\n...",
      "building_data": "#!/bin/bash\n...",
      "profile_data": "#!/bin/bash\n...",
      "run_tests_data": "#!/bin/bash\n...",
      "final_building_data": "#!/bin/bash\n...",
      "created_unix": 1762520411.0
    },
    ...
  },
  "default": "Task(owner='default', repo='default', ...)"
}
```

Each key in `"contexts"` is a string representation of a `Task` object. Each value is a serialized `DockerContext` with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `dockerfile_data` | str | Full Dockerfile content |
| `entrypoint_data` | str | Docker entrypoint script |
| `env_building_data` | str | `docker_build_env.sh` -- base environment setup |
| `base_building_data` | str | `docker_build_base.sh` -- base image build |
| `building_data` | str | `docker_build_pkg.sh` -- package installation |
| `run_building_data` | str | `docker_build_run.sh` -- run-stage build |
| `final_building_data` | str | `docker_build_final.sh` -- final image build |
| `profile_data` | str | `profile.sh` -- ASV benchmark profiling script |
| `run_tests_data` | str | `run-tests.sh` -- pytest/test runner script |
| `created_unix` | float | Unix timestamp when context was registered |

The `Task` key encodes:

| Field | Type | Description |
|-------|------|-------------|
| `owner` | str | Repository owner |
| `repo` | str | Repository name |
| `sha` | str/None | Commit SHA |
| `commit_date` | float | Unix timestamp of commit |
| `env_payload` | str | JSON string of resolved dependencies |
| `python_version` | str | Python version (e.g., `"3.12"`) |
| `tag` | str | Always `"pkg"` for registry keys |
| `benchmarks` | str | Benchmark specification (usually empty) |

---

## File 4b: Synthesis output files

### `results_synthesis/*.pkl`

Pickle files storing `DockerContext` objects. Naming convention:
- `{owner}-{repo}-{sha}-attempt-{N}.pkl` -- intermediate build attempt
- `{owner}-{repo}-{sha}-final.pkl` -- successful final context

### `results_synthesis/results.jsonl`

One JSON object per line, one per task processed. Schema:

| Field | Type | Description |
|-------|------|-------------|
| `owner` | str | Repo owner |
| `repo` | str | Repo name |
| `sha` | str | Commit SHA |
| `image_name` | str | Docker image name (e.g., `owner-repo-sha:pkg`) |
| `ok` | bool | Whether build + validation succeeded |
| `rc` | int | Return code (0=success, 1=failure) |
| `duration_s` | float | Total build+validate time in seconds |
| `stderr_tail` | str | Last N chars of stderr |
| `stdout_tail` | str | Last N chars of stdout |
| `stage` | str | Stage where processing stopped (`analysis`, `probe`, `build`, `validation`) |
| `attempts` | list[dict] | Per-attempt summaries (see below) |
| `context_pickle` | str/None | Path to saved context pickle file |

Each entry in `attempts`:

| Field | Type | Description |
|-------|------|-------------|
| `attempt` | int | Attempt index |
| `ok` | bool | Whether this attempt succeeded |
| `rc` | int/None | Return code |
| `stderr_tail` | str | Stderr tail for this attempt |
| `stdout_tail` | str | Stdout tail for this attempt |
| `building_data` | str | The `docker_build_pkg.sh` script used |

### `results_synthesis/all_files_by_image.json`

Rollup dict keyed by `image_name`. Same fields as `results.jsonl` entries plus `files` list.

---

## File 5: `perfonly_commits_master.parquet`

**Role:** De-duplicated union of all perfonly parquets across pipeline runs.

**Produced by:** `merge_perfonly_commits_master.py` (Step 6).

**CLI:**
```
python merge_perfonly_commits_master.py \
  --new-perfonly perfonly_commits_{dates}.parquet \  # input (File 3)
  --master perfonly_commits_master.parquet           # input/output
```

### Schema (161 columns)

Identical schema to File 3 (`perfonly_commits_{dates}.parquet`). All 161 columns are preserved.

**De-duplication key:** `(pr_merge_commit_sha, repo_name, pr_base_sha)` -- keeps the last occurrence on collision.

---

## Supplementary: `collect_commits.py` output (`commits_all.jsonl`)

**Role:** Initial commit discovery (run separately from the main pipeline, referenced in README Section 1).

**CLI:**
```
python collect_commits.py \
  --dashboards repos_valid.csv \
  --outfile commits_all.jsonl \
  --max-pages 50
```

### Schema (JSONL, one object per line)

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `idx` | int | Sequential index | `0` |
| `commit_id` | str | Identifier `{repo_name}_{i}` | `scikit-learn/scikit-learn_0` |
| `repo_name` | str | GitHub `owner/repo` | `scikit-learn/scikit-learn` |
| `commit_sha` | str | Git commit SHA | `abc123def456...` |

Sources: `find_perf_commits()` (performance-related commits) and `find_tagged_releases()` (tagged version releases), de-duplicated.

---

## Data Flow Summary

| Step | Script | Input | Output | Rows (typical) |
|------|--------|-------|--------|-----------------|
| 0 | (seed) | -- | `repos_valid.csv` | ~127 |
| 1 | `collect_and_filter_commits.py` | `repos_valid.csv` | `merge_commits_filtered.parquet` | ~78k |
| 2 | `prepare_commits_for_building_reports.py` | `merge_commits_filtered.parquet` | `merge_commits_filtered_with_patch.parquet` | ~26k |
| 3 | `collect_perf_commits.py` | `merge_commits_filtered_with_patch.parquet` | `perfonly_commits.parquet` | ~2.9k |
| 4 | `synthesize_contexts.py` | `perfonly_commits.parquet` + `context_registry.json` | `results_synthesis/`, updated `context_registry.json` | -- |
| 5 | `build_and_publish_to_dockerhub.py` | `perfonly_commits.parquet` + `context_registry.json` | Docker images on DockerHub | -- |
| 6 | `merge_perfonly_commits_master.py` | `perfonly_commits.parquet` + `perfonly_commits_master.parquet` | `perfonly_commits_master.parquet` | ~2.3k |

### Column count progression

- `repos_valid.csv`: 9 columns
- `merge_commits_filtered.parquet`: 48 columns (12 commit + 36 PR)
- `merge_commits_filtered_with_patch.parquet`: 146 columns (+4 filter + 12 analysis + ~80 pr_base_repo + 2 container/patch)
- `perfonly_commits.parquet`: 161 columns (+15 LLM classification/report)
- `perfonly_commits_master.parquet`: 161 columns (same schema, de-duped)
