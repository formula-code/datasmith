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
```

## Next steps

- [Pipeline guide](../guide/pipeline.md) — Run the monthly update workflow
- [Synthesis guide](../guide/synthesis.md) — Automatically generate Docker build contexts
- [Configuration](../guide/configuration.md) — Environment variables reference
