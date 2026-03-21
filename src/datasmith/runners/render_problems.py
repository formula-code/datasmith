from __future__ import annotations

import asyncio
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.render_problems")


class RenderProblemsRunner(BaseRunner):
    """Render problem statements for PRs by scraping linked issues."""

    def __init__(self, gh: Any, n_concurrent: int = 5) -> None:
        super().__init__(name="render_problems", n_concurrent=n_concurrent)
        self._gh = gh

    async def _process_item(self, item: Any) -> None:
        """Render the problem statement for a single PR dict."""
        owner: str = item["owner"]
        repo: str = item["repo"]
        issue_number: int = item["issue_number"]

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

        # Render the problem statement (may invoke ProblemExtractor LLM — run in thread)
        repo_description: str = item.get("repo_description", "")
        rendered = await asyncio.to_thread(
            render_problem_statement,
            pr,
            issues=issues,
            repo_description=repo_description,
            anonymize=True,
            extract=True,
        )

        # Persist to DB
        client = get_client()
        client.table("pull_requests").update({"rendered_problem": rendered}).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

        logger.info("Rendered problem statement for %s/%s#%d", owner, repo, issue_number)
