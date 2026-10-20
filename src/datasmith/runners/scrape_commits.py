"""Stage 2: store every pull request merged inside the ingestion window.

The window is ``merged_at`` in ``[since, until)``. :meth:`GitHubClient.fetch_merged_prs`
runs a ``merged:`` search per repository and bisects it when the search cap
would truncate the result. Data comes from GraphQL; stage 3 fetches the diff.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from datasmith.filters import MAX_FILES_CHANGED, check_file_compliance, message_filter
from datasmith.runners.base import BaseRunner
from datasmith.utils import get_logger
from datasmith.utils.db import abatch_upsert, afetch_all, window_filters

logger = get_logger("runners.scrape_commits")

# Repositories scraped at once when the CLI does not say; this paces GitHub,
# not the machine.
DATASMITH_SCRAPE_COMMITS_CONCURRENCY: int = int(os.environ.get("DATASMITH_SCRAPE_COMMITS_CONCURRENCY", "5"))


def _parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 date or datetime into a timezone-aware UTC datetime.

    A naive input is read as UTC, because the fetcher rejects naive bounds.
    """
    cleaned = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_window_bound(name: str, value: str) -> datetime:
    """Parse and validate one end of the ingestion window.

    Called once at construction, so a bad date fails the stage at once
    instead of once per repository.
    """
    if not value:
        raise ValueError(
            f"scrape_commits needs an explicit {name}: the merge window is now expressed in the GitHub "
            "query itself, so there is no unwindowed fetch to fall back to"
        )
    parsed = _parse_iso(value)
    if parsed.microsecond:
        raise ValueError(
            f"scrape_commits needs a whole-second {name}, got {value!r}: GitHub's merged: qualifier has "
            "one-second resolution and a sub-second bound would silently shorten the window"
        )
    return parsed


def _sanitize_text(value: str) -> str:
    """Strip Postgres-illegal null bytes from text values."""
    return value.replace("\u0000", "")


def _symbolic_without_patch(
    title: str,
    file_changes: list[dict[str, Any]],
    changed_files: int,
) -> bool:
    """Evaluate the title filter and file compliance; stage 3 checks patch size.

    ``changed_files`` is GraphQL's ``files.totalCount``. A file list shorter than
    that total is truncated, so the PR is refused rather than checked on part of its files.
    """
    if not message_filter(title):
        return False
    if changed_files >= MAX_FILES_CHANGED:
        return False
    if changed_files > len(file_changes):
        return False
    return not (file_changes and not check_file_compliance(file_changes))


def _build_record(owner: str, repo: str, pr_data: dict[str, Any]) -> dict[str, Any]:
    """Build the ``pull_requests`` row for one merged PR.

    Every record has the same keys, because PostgREST rejects a bulk upsert
    whose objects have different key sets.
    """
    title = _sanitize_text(pr_data.get("title", "") or "")
    file_changes: list[dict[str, Any]] = list(pr_data.get("file_changes") or [])
    changed_files = int(pr_data.get("changed_files", len(file_changes)) or 0)

    return {
        "owner": owner,
        "repo": repo,
        "issue_number": pr_data["number"],
        "title": title,
        "body": _sanitize_text(pr_data.get("body", "") or ""),
        "state": pr_data.get("state", "") or "",
        "created_at": pr_data.get("created_at"),
        "merged_at": pr_data.get("merged_at"),
        "closed_at": pr_data.get("closed_at"),
        "merge_commit_sha": pr_data.get("merge_commit_sha", "") or "",
        "base_sha": (pr_data.get("base") or {}).get("sha", "") or "",
        "head_sha": (pr_data.get("head") or {}).get("sha", "") or "",
        "labels": [label["name"] for label in pr_data.get("labels", [])],
        "file_changes": file_changes,
        "is_performance_commit_symbolic": _symbolic_without_patch(title, file_changes, changed_files),
    }


class ScrapeCommitsRunner(BaseRunner):
    """Store every PR merged in ``[since, until)``, one batched upsert per repository."""

    def __init__(
        self,
        github_client: Any,
        since: str,
        until: str,
        n_concurrent: int = DATASMITH_SCRAPE_COMMITS_CONCURRENCY,
    ) -> None:
        super().__init__(name="scrape_commits", n_concurrent=n_concurrent)
        self._gh = github_client
        self._since = _parse_window_bound("start date", since)
        self._until = _parse_window_bound("end date", until)
        if self._until <= self._since:
            raise ValueError(
                f"empty merge window: [{self._since.isoformat()}, {self._until.isoformat()}) — "
                "the window is half-open, so the end date is excluded"
            )

    async def _process_item(self, item: Any) -> None:
        """Scrape one ``(owner, repo)``: fetch, screen, and store in one batch.

        Fetch errors such as ``Truncated`` and ``RepositoryNotFoundError`` are not
        caught, so ``BaseRunner`` records a failure for this repository only.
        """
        owner, repo = item if isinstance(item, tuple) else item.split("/")

        stats: dict[str, int] = {}
        prs = await self._gh.fetch_merged_prs(owner, repo, self._since, self._until, stats=stats)
        if not prs:
            logger.info(
                "%s/%s: no PRs merged in [%s, %s)",
                owner,
                repo,
                self._since.isoformat(),
                self._until.isoformat(),
            )
            return

        existing = await self._existing_issue_numbers(owner, repo)

        records: list[dict[str, Any]] = []
        seen: set[int] = set()
        for pr_data in prs:
            number = int(pr_data["number"])
            if number in seen:
                # Postgres refuses an upsert that has the same primary key twice.
                continue
            seen.add(number)
            if not pr_data.get("merged_at"):
                logger.warning("%s/%s#%d: search returned an unmerged PR; not storing it", owner, repo, number)
                continue
            if number in existing:
                continue
            records.append(_build_record(owner, repo, pr_data))

        stored = await abatch_upsert("pull_requests", records)
        logger.info(
            "%s/%s: %d merged in window, %d already stored, %d written "
            "(%d GraphQL queries, %d bisections, %d files fallbacks)",
            owner,
            repo,
            len(prs),
            len(prs) - len(records),
            stored,
            stats.get("queries", 0),
            stats.get("bisections", 0),
            stats.get("files_fallbacks", 0),
        )

    async def _existing_issue_numbers(self, owner: str, repo: str) -> set[int]:
        """Read the issue numbers already stored *for this window*.

        Limited to the window, so the read uses the ``(owner, repo, merged_at)``
        index and does not grow with the table.
        """
        rows = await afetch_all(
            "pull_requests",
            select="issue_number",
            filters={"owner": owner, "repo": repo},
            **window_filters(self._since.isoformat(), self._until.isoformat()),
        )
        return {int(row["issue_number"]) for row in rows}
