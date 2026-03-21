from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from datasmith.filters import symbolic_compliance
from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.scrape_commits")


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 date or datetime string to a timezone-aware datetime."""
    if not value:
        return None
    # Handle date-only strings like "2024-01-01"
    if "T" not in value:
        return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    # Handle full ISO datetime strings (with or without trailing Z)
    cleaned = value.replace("Z", "+00:00")
    return datetime.fromisoformat(cleaned)


def _should_skip_pr(
    pr_data: dict[str, Any],
    since: datetime | None,
    until: datetime | None,
    seen_shas: set[str],
) -> bool | str:
    """Return False if the PR should be processed, 'skip' to skip, 'stop' to halt pagination."""
    if since:
        created = _parse_iso(pr_data.get("created_at"))
        if created and created < since:
            return "stop"

    if not pr_data.get("merged_at"):
        return "skip"

    merged = _parse_iso(pr_data.get("merged_at"))
    if merged:
        if since and merged < since:
            return "skip"
        if until and merged >= until:
            return "skip"

    sha = pr_data.get("merge_commit_sha", "")
    if sha:
        if sha in seen_shas:
            return "skip"
        seen_shas.add(sha)

    return False


async def _build_record(
    gh: Any,
    owner: str,
    repo: str,
    pr_data: dict[str, Any],
) -> dict[str, Any]:
    """Fetch diff/files and build the upsert record for a single PR."""
    issue_number = pr_data["number"]
    title = pr_data.get("title", "")

    diff = await gh.get_diff(owner, repo, issue_number)
    files = await gh.get_files(owner, repo, issue_number)

    file_changes: list[dict[str, Any]] | None = None
    if files:
        file_changes = [
            {
                "filename": f.get("filename", ""),
                "additions": f.get("additions", 0),
                "deletions": f.get("deletions", 0),
            }
            for f in files
        ]

    record: dict[str, Any] = {
        "owner": owner,
        "repo": repo,
        "issue_number": issue_number,
        "title": title,
        "body": pr_data.get("body", "") or "",
        "state": pr_data.get("state", ""),
        "created_at": pr_data.get("created_at"),
        "merged_at": pr_data.get("merged_at"),
        "closed_at": pr_data.get("closed_at"),
        "merge_commit_sha": pr_data.get("merge_commit_sha", ""),
        "base_sha": pr_data.get("base", {}).get("sha", ""),
        "head_sha": pr_data.get("head", {}).get("sha", ""),
        "labels": [label["name"] for label in pr_data.get("labels", [])],
        "is_performance_commit_symbolic": symbolic_compliance(
            title=title,
            patch=diff or None,
            file_changes=file_changes,
        ),
    }
    if diff:
        record["patch"] = diff
    if file_changes:
        record["file_changes"] = file_changes
    return record


class ScrapeCommitsRunner(BaseRunner):
    """Scrape PRs for each repo, run compliance hooks, store in pull_requests table."""

    def __init__(
        self,
        github_client: Any,
        n_concurrent: int = 5,
        since: str | None = None,
        until: str | None = None,
    ) -> None:
        super().__init__(name="scrape_commits", n_concurrent=n_concurrent)
        self._gh = github_client
        self._since = _parse_iso(since)
        self._until = _parse_iso(until)

    async def _process_item(self, item: Any) -> None:
        """Process a (owner, repo) tuple — scrape its merged PRs."""
        owner, repo = item if isinstance(item, tuple) else item.split("/")

        # Determine default branch for base filter
        repo_resp = await self._gh._request("GET", f"/repos/{owner}/{repo}")
        default_branch = "main"
        if repo_resp is not None:
            default_branch = repo_resp.json().get("default_branch", "main")

        # Sort by "created" when date-filtering to enable early termination
        sort = "created" if self._since else "updated"
        params: dict[str, Any] = {
            "state": "closed",
            "sort": sort,
            "direction": "desc",
            "base": default_branch,
        }

        seen_shas: set[str] = set()
        client = get_client()

        async for page in self._gh.paginate("GET", f"/repos/{owner}/{repo}/pulls", params=params):
            stop = False
            for pr_data in page:
                verdict = _should_skip_pr(pr_data, self._since, self._until, seen_shas)
                if verdict == "stop":
                    stop = True
                    break
                if verdict == "skip":
                    continue

                record = await _build_record(self._gh, owner, repo, pr_data)
                client.table("pull_requests").upsert(record).execute()

            if stop:
                break

        logger.info("Scraped PRs for %s/%s", owner, repo)
