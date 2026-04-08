# Report Builder: Data Sources, Processing, and Rendering

This note documents how `ReportBuilder` constructs a report in `src/datasmith/scrape/report_builder.py`, including
every input used, how it is gathered, how it is transformed, and where it is rendered. It also calls out data that
is computed but not currently rendered.

## High-level overview

- Inputs arrive as a `pr_dict` payload (PR URL, title/body, labels, base repo metadata, patch, change summary, and
  timestamps). These are the canonical raw sources for everything else.
- GitHub REST API calls enrich the report with referenced issues and timeline comments. The timeline and issue
  metadata provide the text for issue descriptions, comments, and cross-references.
- Optional LLM backends (DSPy) extract a problem statement and, separately, classify performance commits.
- Jinja2 templates assemble markdown sections. The final report is `final.md.j2` with repository description,
  problem statement, and hints.

## Detailed data inventory and flow

### 1) Core PR identity and raw content

**Inputs**
- `pr_dict["pr_url"]` (required), plus `pr_dict["pr_title"]`, `pr_dict["pr_body"]`, `pr_dict["pr_labels"]`.
- `pr_dict["pr_created_at"]`, `pr_dict["pr_merged_at"]`, `pr_dict["patch"]`, `pr_dict["file_change_summary"]`.

**Procurement**
- This is direct input to `ReportBuilder.build()`. There are no API calls at this stage.

**Processing**
- `pr_url` is normalized to a string and parsed via `datasmith.scrape.utils._parse_pr_url` into `(owner, repo, pr_number)`.
- Labels are normalized to a list of label dicts and joined into a comma-separated string (or `—` if empty).

**Rendering**
- The PR header string is rendered using `src/datasmith/scrape/templates/pr_header.md.j2`.
  - Fields: title, PR number, repo full name, labels, and body.
- The PR header is *not* included in the final report body; it is stored in
  `ReportResult.final_results["pr_header"]` and `ReportResult.all_data["raw_pr_*"]` for downstream use.

### 2) Repository description (from PR base)

**Inputs**
- `pr_dict["pr_base"]["repo"]` (expected to be a GitHub API repo object).

**Procurement**
- This comes from the PR metadata loader outside the builder (not fetched in `ReportBuilder`).

**Processing**
- Topics are normalized to a comma-separated string.
- Name, description, and language are passed through with safe fallbacks.

**Rendering**
- Rendered via `src/datasmith/scrape/templates/repo.md.j2` and inserted into the final report
  as the **Repository Description** section (`final.md.j2`).

### 3) Referenced issues (problem statement source)

**Inputs**
- The rendered PR header text (`pr_body_text`) and extracted PR comments (see section 4).

**Procurement**
- `datasmith.scrape.issue_extractor.extract_issues_from_description()` scans the PR text for issue/PR references:
  - Full URLs: `https://github.com/owner/repo/issues/123` or `/pull/123`
  - Cross-repo shorthand: `owner/repo#123`
  - Same-repo shorthand: `#123`
- For each reference, `datasmith.utils._get_github_metadata` calls the GitHub REST API:
  - Issue metadata: `GET /repos/{owner}/{repo}/issues/{number}`
  - Issue timeline: `GET /repos/{owner}/{repo}/issues/{number}/timeline`

**Processing**
- If a referenced item is a merged PR, it is skipped; unmerged PRs are retained for discussion content.
- Timeline events are scanned for:
  - `commented` events prior to the PR creation time (`pr_created_at`), which become issue comments.
  - `cross-referenced` events, whose source bodies are collected as cross-reference text.
- Issue data is normalized into `IssueExpanded` models with description, comments, cross-references, and timestamps.
- A stable sort by issue number provides deterministic rendering order.
- Optional anonymization (`ReportBuilder.anonymize_output`) redacts URLs, issue numbers, mentions, emails, and SHAs
  via `anonymize_github_issue()` (in `src/datasmith/scrape/report_utils.py`).

**Rendering**
- `src/datasmith/scrape/templates/issues.md.j2` renders each `IssueExpanded` into markdown blocks.
- This section is inserted into `final.md.j2` as the **Problem Statement** content.

### 4) PR discussions (hints context)

**Inputs**
- The PR number and metadata (`pr_dict["pr_merged_at"]`, `pr_dict["pr_created_at"]`).

**Procurement**
- `datasmith.scrape.report_utils.issue_timeline()` calls the GitHub REST API:
  - `GET /repos/{owner}/{repo}/issues/{number}/timeline` for the PR itself.

**Processing**
- Timeline events are filtered to those with:
  - A body, a user login, and a `created_at`/`updated_at` earlier than `pr_merged_at`.
- Each event is converted to a `HintComment`:
  - `timestamp` formatted with `report_utils.iso()` (ISO -> `%H:%M %d/%m/%Y`).
  - `links` populated by re-running issue extraction on the comment body.
- `HintsContext.summary` is the concatenated comment bodies.

**Rendering**
- The hints template is `src/datasmith/scrape/templates/hints.md.j2` and expects a variable `h`
  (an instance of `HintsContext`). However, `ReportBuilder.build()` currently renders the template
  with only `problem_description` and **does not pass `h`**, so the comment list/summary is not shown.
- The structured hints data is still present in `ReportResult.all_data["hints_context"]` for downstream consumers.

### 5) LLM-based problem extraction (optional)

**Inputs**
- `pr_body_text` (rendered PR header text).
- `pr_raw_comments_text` (concatenated discussion comments).

**Procurement**
- When `enable_llm_backends=True` and `summarize_llm=True`:
  - `datasmith.agents.config.configure_agent_backends()` sets up DSPy LLM backends.
  - `ProblemExtractor.extract_problem()` runs a DSPy prediction to extract verbatim problem/solution text.
  - Extractiveness is validated with `datasmith.scrape.verbatim_checker.validate_section_extractiveness`
    (LCS and exact n-gram checks).
- When LLMs are disabled or extraction fails, the fallback is a `ProblemExtraction` with an empty
  `problem_statement` and a `solution_overview` containing the raw PR body plus comments.

**Processing**
- The extracted `ProblemExtraction.problem_statement` is used as a concise, verbatim problem summary.
- If there are **no referenced issues** but a problem statement exists, the builder moves this
  text into the **Problem Statement** section and clears **Hints**.

**Rendering**
- The extracted problem statement is inserted into `hints.md.j2` as `problem_description`.
- If no issues are found but a problem statement exists, the problem statement is rendered in place of
  the issues section.

### 6) Performance detection and classification (optional)

**Inputs**
- `raw_pr_text`: PR header + concatenated comment text.
- `file_change_summary` and `patch` from `pr_dict`.

**Procurement**
- `PerfClassifier.get_response()` (DSPy) determines whether the PR is performance-related.
- When enabled, `ClassifyJudge` produces a `ClassificationDecision` with category, difficulty, and confidence.

**Processing**
- Performance detection is run when `filter_performance_only` or `add_classification` is true.
- When `filter_performance_only=True` and the PR is not performance-related, the builder returns
  `final_md="NOT_A_PERFORMANCE_COMMIT"` early with metadata in `all_data`.
- Patch text is truncated in `ClassifyJudge` to fit token budgets (with a truncation marker).

**Rendering**
- Classification and performance JSON are stored in `ReportResult` fields and `all_data`, but **not** rendered
  into the final markdown report (there is no template hook for them).

### 7) Final assembly and outputs

**Inputs**
- `repo_description` (section 2)
- `issues_rendered` or `problem_statement` (section 3/5)
- `hints_block` (section 4/5)

**Processing**
- The final report is rendered with `src/datasmith/scrape/templates/final.md.j2`.
- Anonymization may be applied to the whole rendered markdown.
- Two variants are produced:
  - `final_md` (with hints)
  - `final_md_no_hints` (hints suppressed)

**Rendering**
- `final.md.j2` contains static optimization instructions, then inserts:
  - **Repository Description** (if present)
  - **Task Description / Problem Statement**
  - **Hints** (if non-empty)

## Template map (what renders what)

- `repo.md.j2`: Repository description (name, language, description, topics).
- `pr_header.md.j2`: PR header used for extraction and metadata, not in final report.
- `issues.md.j2`: Referenced issues list; becomes the **Problem Statement** section.
- `hints.md.j2`: Intended to show extracted problem text plus discussion comments; currently only
  receives `problem_description`.
- `comment.md.j2`: Per-comment rendering for hints (wired through `hints.md.j2`, but requires `h`).
- `final.md.j2`: Top-level report layout and narrative instructions.

## Early-exit states

- **Invalid PR**: If there are no referenced issues *or* the PR body is empty, the builder returns
  `final_md="NOT_A_VALID_PR"` with minimal metadata.
- **Filtered non-performance PR**: When `filter_performance_only=True`, a non-performance PR returns
  `final_md="NOT_A_PERFORMANCE_COMMIT"` and skips rendering the full report.
