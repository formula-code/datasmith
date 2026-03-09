from __future__ import annotations

from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.scrape_commits")


class ScrapeCommitsRunner(BaseRunner):
    """Scrape PRs for each repo, run compliance hooks, store in pull_requests table."""

    def __init__(self, github_client: Any, n_concurrent: int = 5) -> None:
        super().__init__(name="scrape_commits", n_concurrent=n_concurrent)
        self._gh = github_client

    async def _process_item(self, item: Any) -> None:
        """Process a (owner, repo) tuple — scrape its merged PRs."""
        owner, repo = item if isinstance(item, tuple) else item.split("/")

        # List merged PRs
        resp = await self._gh._request(
            "GET",
            f"/repos/{owner}/{repo}/pulls",
            params={"state": "closed", "sort": "updated", "direction": "desc", "per_page": 100},
        )
        if resp is None:
            return

        client = get_client()
        for pr_data in resp.json():
            if not pr_data.get("merged_at"):
                continue

            issue_number = pr_data["number"]

            # Store PR data
            client.table("pull_requests").upsert({
                "owner": owner,
                "repo": repo,
                "issue_number": issue_number,
                "title": pr_data.get("title", ""),
                "body": pr_data.get("body", "") or "",
                "state": pr_data.get("state", ""),
                "created_at": pr_data.get("created_at"),
                "merged_at": pr_data.get("merged_at"),
                "closed_at": pr_data.get("closed_at"),
                "merge_commit_sha": pr_data.get("merge_commit_sha", ""),
                "base_sha": pr_data.get("base", {}).get("sha", ""),
                "head_sha": pr_data.get("head", {}).get("sha", ""),
                "labels": [label["name"] for label in pr_data.get("labels", [])],
            }).execute()

        logger.info("Scraped PRs for %s/%s", owner, repo)
