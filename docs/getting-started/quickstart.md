# Quickstart

This guide walks through common fc-data operations with code examples.

## Working with pull requests

Every task starts with a PR. You can construct one directly or fetch it from the database:

```python
from datasmith.github import PR, GitHubClient
from datasmith.utils import TokenPool

# Construct a bare PR (fields empty — useful when you already have the data)
pr = PR(repository="astropy/astropy", issue_number=16222)

# PRs are frozen Pydantic v2 models — immutable after creation
pr.merge_commit_sha   # the merge commit sha
pr.base_sha           # base branch commit
pr.cache_key          # "astropy/astropy:16222" — used for Supabase caching

# Or fetch a fully-hydrated PR (tries Supabase first, then GitHub API)
pr = await PR.fetch("astropy/astropy", 16222)
```

## Fetching data from GitHub

Use the async client to fetch live data:

```python
pool = TokenPool()   # reads GH_TOKENS env var, rotates tokens on rate-limit
gh = GitHubClient(pool)

pr = await gh.get_pr("pandas-dev", "pandas", 16222)
diff = await gh.get_diff("pandas-dev", "pandas", 16222)
events = await gh.get_timeline("pandas-dev", "pandas", 16222)
```

## Rendering problem statements

Turn a PR into a problem statement for LLM evaluation:

```python
from datasmith.github import render_problem_statement, scrape_links

# Render with anonymization (@alice -> @user_1, emails stripped)
statement = render_problem_statement(pr, anonymize=True)

# Scrape linked issues via BFS for richer context
issues = await scrape_links(pr, gh.get_issue_expanded, depth=2, only_issues=True, limit=6)
statement = render_problem_statement(
    pr, issues=issues, repo_description="pandas is a data analysis library"
)
```

## Custom hooks

Register custom operations that are automatically cached in Supabase:

```python
from datasmith.github import HookRegistry
from dspy import ChainOfThought

summarizer = ChainOfThought("document -> summary")

def summarize(pr):
    doc = render_problem_statement(pr, anonymize=True)
    return summarizer(doc).summary

HookRegistry.register("summarize", summarize)   # auto-wrapped with @supabase_cached

HookRegistry.call("summarize", pr)   # first call: hits LLM
HookRegistry.call("summarize", pr)   # second call: reads from cache
```

## Classification at scale

Run LLM-based classification across many PRs concurrently:

```python
from datasmith.runners import ClassifyPRsRunner
from datasmith.agents import PerfClassifier, ClassifyJudge

runner = ClassifyPRsRunner(PerfClassifier(), ClassifyJudge(), n_concurrent=64)
await runner.run(pr_items)
# Progress tracked in Supabase runner_progress table
# Per-item failures logged in runner_failures — the runner never aborts
```

## Building Docker images

Build reproducible environments using the three-tier image hierarchy:

```python
from datasmith.docker import ImageManager

mgr = ImageManager()
mgr.build_base_image()                               # formulacode/base:latest
mgr.build_repo_image("pandas-dev", "pandas")          # formulacode/pandas-dev-pandas:latest
mgr.build_pr_image("pandas-dev", "pandas", 16222)     # formulacode/pandas-dev-pandas:16222

# Each call also takes a custom build context in place of the default template.
mgr.build_pr_image("pandas-dev", "pandas", 16222, context="path/to/custom/context")
```

Every task image holds a build manifest. Read it and evaluate its invariants without a rebuild or a container run:

```python
from datasmith.docker import read_build_manifest, evaluate_invariants

manifest = read_build_manifest("formulacode/pandas-dev-pandas:16222")
report = evaluate_invariants(manifest)
# report.ok: True (all fatal invariants hold), False (a fatal invariant failed), None (no manifest).
# report.fatal, report.warnings and report.skipped list the invariant ids.
```

## Synthesizing a build context

The synthesizer is a state machine. It checks Supabase for a cached context, tries build scripts from similar PRs, then runs an installed CLI agent (Claude Code, Codex, Gemini or Qwen Code):

```python
from datasmith.agents import Synthesizer

synth = Synthesizer(max_attempts=3)
ctx = synth.run(
    owner="pandas-dev",
    repo="pandas",
    issue_number=16222,
    pr_context="This PR optimizes groupby performance by ...",
    sha="abc123def456",
    base_sha="def456abc123",   # the commit before the optimization, which the image checks out
    env_payload='{"dependencies": ["numpy==1.26.0", "cython==3.0.0"]}',
    python_version="3.10",
    solution_patch=pr_patch,   # the oracle patch, used only to check that the task is measurable
)
```

`local_ci.py` checks each attempt inside the sandbox: it builds the image, runs the tests, runs the measure step and checks the fatal invariants of the build manifest. A success is stored in the `candidate_containers` table and `ctx` is the `DockerContext`. Every attempt is logged to the `error_logs` table. If all attempts fail, `ctx` is `None`.

`SynthesizeImagesRunner(synth, n_concurrent=8)` runs synthesis over many PRs. Each LLM attempt costs agent tokens.

## Publishing a dataset

```python
from datasmith.publish import records_from_supabase, HuggingFacePublisher

# Verified, unpublished performance PRs merged in the window.
records = records_from_supabase(start_date="2026-02-01", end_date="2026-03-01")

HuggingFacePublisher().publish(records, version="formulacode@2026-03")
```

## Rendering Harbor tasks

```python
from datasmith.harbor_adapter.adapter import FormulaCodeAdapter, FormulaCodeRecord

FormulaCodeAdapter(harbor_tasks_root="tasks/").generate_task(FormulaCodeRecord(**record))
```

The `fc-render-tasks render` command does the same from a records file.

## Next steps

- [Pipeline guide](../guide/pipeline.md) — Run the monthly update workflow
- [Synthesis guide](../guide/synthesis.md) — Automatically generate Docker build contexts
- [Configuration](../guide/configuration.md) — Environment variables reference
- [Overview](../OVERVIEW.md) — From a rendered task to measurement, review and RL training
