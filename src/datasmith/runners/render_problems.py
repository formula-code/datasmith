from __future__ import annotations

import asyncio
import functools
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.render_problems")

# Threads for the two blocking calls per item: the DSPy extraction and the
# Jinja2 render.  ``asyncio.to_thread`` puts both on the interpreter's default
# executor, sized ``min(32, cpu_count + 4)`` — a number that means something
# different on every host, and that everything else in the process shares
# (spec section 6.4).  This pool is explicit, bounded, and owned by the runner.
# It paces the LLM backend, not GitHub: the linked-issue scrape above it is
# async and rides the token pool instead.
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
        # Built once per stage, not once per PR.  A fresh ProblemExtractor was
        # being constructed inside every threaded extraction call, so each
        # item paid to rebuild the dspy.Signature subclass and re-run
        # ``ensure_configured`` before it could ask a question.
        #
        # Sharing is safe: the instance's only state is the memoised
        # ``dspy.Predict``, and a Predict carries no per-call state — the
        # request lives on the LM.  The unlocked assignment in
        # ``_get_predictor`` can at worst build the predictor twice if two
        # threads race the first call, and the loser is simply discarded.
        from datasmith.agents.extractors import ProblemExtractor

        self._extractor = ProblemExtractor()

    async def run(self, items: list[Any]) -> None:
        """Run the stage against a pool this runner owns and shuts down.

        ``n_concurrent`` bounds how many PRs are in flight; the pool bounds how
        many of them are inside a blocking DSPy call at once.  They are
        separate numbers because the first is a queue depth and the second is
        what the LLM backend has to survive.
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
            # Shut down even when the stage raises, so a failed run does not
            # leave extractor threads alive behind it.
            self._executor.shutdown(wait=True)
            self._executor = None

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

        # Render the full problem statement using the pre-extracted observations
        # (pass initial_observations to avoid a second ProblemExtractor call)
        rendered = await loop.run_in_executor(
            self._executor,
            functools.partial(
                render_problem_statement,
                pr,
                issues=issues,
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
