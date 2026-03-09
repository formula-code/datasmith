from __future__ import annotations

import asyncio
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.synthesize_images")


class SynthesizeImagesRunner(BaseRunner):
    """Run Synthesizer for each PR to produce Docker build contexts."""

    def __init__(
        self,
        synthesizer: Any,
        verifier: Any,
        gh: Any | None = None,
        n_concurrent: int = 3,
    ) -> None:
        super().__init__(name="synthesize_images", n_concurrent=n_concurrent)
        self._synthesizer = synthesizer
        self._verifier = verifier
        self._gh = gh  # GitHubClient, optional — needed for rendering problem statements

    async def _render_problem(self, item: dict[str, Any]) -> str | None:
        """Render the problem statement for a PR, scraping linked issues.

        Returns the rendered markdown, or ``None`` if rendering is skipped
        (no GitHubClient) or fails.
        """
        if self._gh is None:
            return None

        owner: str = item["owner"]
        repo: str = item["repo"]
        issue_number: int = item["issue_number"]

        from datasmith.github.links import scrape_links
        from datasmith.github.models import PR
        from datasmith.github.render import render_problem_statement

        # Build a PR object for scrape_links and render_problem_statement
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
        return rendered

    async def _process_item(self, item: Any) -> None:
        """Process a PR dict with owner, repo, issue_number, pr_context."""
        owner = item["owner"]
        repo = item["repo"]
        issue_number = item["issue_number"]
        pr_context = item.get("pr_context", "")

        # Render the problem statement before synthesis
        await self._render_problem(item)

        # Run synthesizer in thread (Docker operations are blocking)
        ctx = await asyncio.to_thread(
            self._synthesizer.run,
            owner,
            repo,
            issue_number,
            pr_context,
            self._verifier,
        )

        if ctx is not None:
            logger.info("Successfully synthesized image for %s/%s#%d", owner, repo, issue_number)
        else:
            raise RuntimeError(f"Synthesis failed for {owner}/{repo}#{issue_number}")
