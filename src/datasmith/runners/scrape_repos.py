from __future__ import annotations

from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.scrape_repos")


class ScrapeReposRunner(BaseRunner):
    """Scrape GitHub repos via search API and store in repositories table."""

    def __init__(self, github_client: Any, n_concurrent: int = 5) -> None:
        super().__init__(name="scrape_repos", n_concurrent=n_concurrent)
        self._gh = github_client

    async def _process_item(self, item: Any) -> None:
        """Process a search query or repo identifier."""
        owner, repo = item if isinstance(item, tuple) else item.split("/")

        # Check if already exists
        client = get_client()
        resp = client.table("repositories").select("owner").eq("owner", owner).eq("repo", repo).execute()
        if resp.data:
            logger.debug("Skipping existing repo: %s/%s", owner, repo)
            return

        # Fetch repo info via GitHub API
        resp_gh = await self._gh._request("GET", f"/repos/{owner}/{repo}")
        if resp_gh is None:
            return

        data = resp_gh.json()
        client.table("repositories").upsert({
            "owner": owner,
            "repo": repo,
            "url": data.get("html_url", ""),
            "language": data.get("language", ""),
            "stars": data.get("stargazers_count", 0),
            "topics": data.get("topics", []),
            "description": data.get("description", ""),
        }).execute()
        logger.info("Stored repo: %s/%s", owner, repo)
