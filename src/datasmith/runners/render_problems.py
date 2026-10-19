from __future__ import annotations

import asyncio
import functools
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.render_problems")

# Threads for the blocking DSPy extraction and Jinja2 render; this paces the LLM
# backend. An explicit pool, because the default executor's size varies by host.
DATASMITH_RENDER_PROBLEMS_WORKERS: int = int(os.environ.get("DATASMITH_RENDER_PROBLEMS_WORKERS", "8"))


class RenderProblemsRunner(BaseRunner):
    """Scrape linked issues, run ProblemExtractor, and persist deconstructed context.

    For each PR this runner:
    1. BFS-scrapes linked GitHub issues via the GitHub API.
    2. Runs :class:`~datasmith.agents.extractors.ProblemExtractor` (DSPy) once to
       split the PR body into four structured fields.
    3. Renders the full Jinja2 problem-statement template.
    4. Upserts everything into the ``candidate_prs`` table (raw components + rendered).
    5. Updates ``pull_requests.rendered_problem`` and the new
       ``pull_requests.problem_description`` column (problem-only text).
    """

    def __init__(self, gh: Any, n_concurrent: int = 5, max_workers: int | None = None) -> None:
        super().__init__(name="render_problems", n_concurrent=n_concurrent)
        self._gh = gh
        self._max_workers = max(1, DATASMITH_RENDER_PROBLEMS_WORKERS if max_workers is None else max_workers)
        self._executor: ThreadPoolExecutor | None = None
        # One extractor per stage. Threads can share it, because its only state
        # is a ``dspy.Predict`` with no per-call state.
        from datasmith.agents.extractors import ProblemExtractor

        self._extractor = ProblemExtractor()

    async def run(self, items: list[Any]) -> None:
        """Run the stage on a thread pool that is created here and shut down at the end.

        ``n_concurrent`` limits PRs in flight; the pool limits concurrent DSPy calls.
        """
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix="render-problems",
        )
        logger.info(
            "Rendering with %d worker thread(s), %d concurrent item(s)",
            self._max_workers,
            self._n_concurrent,
        )
        try:
            await super().run(items)
        finally:
            # Shut down even when the stage raises, so no threads are left running.
            self._executor.shutdown(wait=True)
            self._executor = None

    def _issues_for_render(self, issues: list[Any]) -> list[Any]:
        return issues

    def _check(self, problem_description: str, item: dict[str, Any]) -> None:
        """Raise to keep a problem text from being stored; BaseRunner counts the error."""

    async def _process_item(self, item: Any) -> None:
        """Render the problem statement for a single PR dict."""
        owner: str = item["owner"]
        repo: str = item["repo"]
        issue_number: int = item["issue_number"]
        merge_commit_sha: str = item.get("merge_commit_sha", "") or ""
        repo_description: str = item.get("repo_description", "") or ""

        from datasmith.github.links import scrape_links
        from datasmith.github.models import PR
        from datasmith.github.render import render_problem_statement

        pr = PR(
            repository=f"{owner}/{repo}",
            issue_number=issue_number,
            title=item.get("title", ""),
            body=item.get("body", ""),
            created_at=item.get("created_at"),
        )

        # BFS-scrape linked issues (async GitHub API calls)
        issues = await scrape_links(
            pr,
            self._gh.get_issue_expanded,
            depth=2,
            only_issues=True,
            limit=6,
        )

        logger.info(
            "Scraped %d linked issues for %s/%s#%d",
            len(issues),
            owner,
            repo,
            issue_number,
        )

        # Run ProblemExtractor once (DSPy LLM call — run on the runner's pool)
        loop = asyncio.get_running_loop()
        extraction = await loop.run_in_executor(
            self._executor,
            functools.partial(
                self._extractor.extract_problem,
                item.get("title", ""),
                item.get("body", ""),
            ),
        )
        problem_description = extraction.to_problem_markdown()
        self._check(problem_description, item)

        # Render the full problem statement using the pre-extracted observations
        # (pass initial_observations to avoid a second ProblemExtractor call)
        rendered = await loop.run_in_executor(
            self._executor,
            functools.partial(
                render_problem_statement,
                pr,
                issues=self._issues_for_render(issues),
                repo_description=repo_description,
                anonymize=True,
                extract=False,
                initial_observations=problem_description or getattr(pr, "body", ""),
            ),
        )

        # Serialize linked issues for storage (mode="json" converts datetime → ISO string)
        issues_json = [issue.model_dump(mode="json") for issue in issues]

        # Upsert all raw components + rendered output into candidate_prs
        client = get_client()
        client.table("candidate_prs").upsert({
            "owner": owner,
            "repo": repo,
            "issue_number": issue_number,
            "merge_commit_sha": merge_commit_sha,
            "repo_description": repo_description,
            "issues_json": issues_json,
            "initial_observations": extraction.initial_observations,
            "triage_attempts": extraction.triage_attempts,
            "solution_overview": extraction.solution_overview,
            "solution_observations": extraction.solution_observations,
            "rendered_problem": rendered,
        }).execute()

        # Keep pull_requests in sync: rendered_problem (used by synthesize_images)
        # and problem_description (problem-only extracted text)
        client.table("pull_requests").update({
            "rendered_problem": rendered,
            "problem_description": problem_description,
        }).eq("owner", owner).eq("repo", repo).eq("issue_number", issue_number).execute()

        logger.info("Rendered problem context for %s/%s#%d", owner, repo, issue_number)


class PromptLeak(ValueError):
    """The problem text names something that gives away the fix."""


_ASV = re.compile(r"\bASV\b|\basv\b|\b[Bb]enchmarks?\b|\btime_\w+|\bpeakmem_\w+")
_LINK = re.compile(r"https?://github\.com/\S+/(pull|issues)/\d+|(?<![\w/])#\d{2,}\b")
_CODE = re.compile(r"```|^\+\+\+ |^--- a/|^@@ ", re.M)
_DEF = re.compile(r"^[+-]?\s*(?:async\s+)?(?:def|class|cdef|cpdef)\s+([A-Za-z_]\w{3,})", re.M)
_HUNK = re.compile(r"^@@ .*@@\s*(?:async\s+)?(?:def|class|cdef|cpdef)\s+([A-Za-z_]\w{3,})", re.M)
# IPython display hooks such as _repr_html_ are public.
_PUBLIC_HOOK = re.compile(r"_(repr_\w+|ipython_\w+)_")


def _leaks(text: str, patch: str) -> list[str]:
    """Giveaways in a problem text: benchmark names, PR or issue links, code or a diff, internal code the patch changes."""
    found = [
        f"{kind}: {m.group(0)}"
        for kind, rx in (("benchmarks", _ASV), ("link", _LINK), ("code", _CODE))
        if (m := rx.search(text))
    ]
    changed = set(_DEF.findall("\n".join(line for line in patch.splitlines() if line[:1] in "+-")))
    changed |= set(_HUNK.findall(patch))
    # A public entry point may be named; an internal function or class may not.
    found += [
        f"internal code: {n}"
        for n in sorted(changed)
        if n.startswith("_") and not _PUBLIC_HOOK.fullmatch(n) and re.search(rf"\b{re.escape(n)}\b", text)
    ]
    return found


class RenderShortProblemsRunner(RenderProblemsRunner):
    """The default prompt: only the symptom, without the linked issues (they often state the fix), and no giveaways."""

    def _issues_for_render(self, issues: list[Any]) -> list[Any]:
        return []

    def _check(self, problem_description: str, item: dict[str, Any]) -> None:
        if found := _leaks(problem_description, item.get("patch") or ""):
            raise PromptLeak(f"{item['owner']}/{item['repo']}#{item['issue_number']}: {'; '.join(found)}")
