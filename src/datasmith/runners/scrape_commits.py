"""Stage 2 — store every pull request merged inside the ingestion window.

The window means ``merged_at``, half-open ``[since, until)``, and GitHub is
asked that question directly: :meth:`GitHubClient.fetch_merged_prs` issues a
``merged:`` search per repository and bisects it when the search cap would
truncate the answer.

This replaces a client-side filter that paged the repository connection in
``CREATED_AT DESC`` order and halted on the first PR created before the window
while accepting on ``merged_at``.  Stage 2 therefore captured only PRs both
created *and* merged inside the window: measured on 2026-08-01, 46 of 81
in-window PRs were lost, and the loss was biased — whole repositories with a
deliberate review process disappeared (devito 6 of 6, newton-physics 3 of 3)
while same-day-merge repositories survived intact.

Stage 2 spends no REST budget.  Everything stored here comes from GraphQL; the
only REST calls are the ~1% ``files`` fallback inside the fetcher.  The diff is
fetched by stage 3, next to the classifier the patch-size gate exists to
protect.
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

# How many repositories stage 2 scrapes at once when the CLI does not say.
# Stage 2 is GraphQL-only and one GitHub token supplies 5 000 points an hour
# against a need of roughly 170, so this dial is about GitHub-side pacing
# rather than about machine size — see spec section 6.4.
DATASMITH_SCRAPE_COMMITS_CONCURRENCY: int = int(os.environ.get("DATASMITH_SCRAPE_COMMITS_CONCURRENCY", "5"))


def _parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 date or datetime into a timezone-aware UTC datetime.

    A naive input is read as UTC rather than left naive: the fetcher rejects
    naive bounds, and a bound that raises per repository turns one bad flag
    into one ``runner_failures`` row per tracked repository.
    """
    cleaned = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_window_bound(name: str, value: str) -> datetime:
    """Parse and validate one end of the ingestion window.

    Validated once at construction, not once per repository, so a malformed
    ``--start-date`` fails the stage immediately instead of producing a
    failure row for every repository in the corpus.
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
    """Evaluate the two attribute-compliance components that GraphQL can answer.

    ``symbolic_compliance`` has three components — the title filter, file
    compliance, and ``check_patch_size``.  Stage 2 has no patch, so **the
    patch-size component is not evaluated here at all**.  It is not estimated
    either: no proxy for a token count was available, so inventing one would
    put a number in the database that nothing measured.  Stage 3 fetches the
    diff and applies ``check_patch_size`` there, next to the classifier the
    gate exists to protect.

    ``changed_files`` is the GraphQL ``files.totalCount``, which stays truthful
    when ``files(first: 100)`` truncates the node list.  The fetcher normally
    repairs a truncated list over REST; when that fallback comes back empty the
    list is short and both size guards would read from it — the 500-file guard
    could not fire and the 40 000-line sum would undercount.  A PR measured
    with ``totalCount = 3000`` passes the screen on its truncated list and must
    not, so a list still short of its own total is refused rather than trusted.
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

    Every record carries the same keys.  PostgREST rejects a bulk upsert whose
    objects disagree on their key set, so the conditional ``patch`` /
    ``file_changes`` keys that were harmless for a per-PR upsert would fail a
    whole repository's batch.
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

        Nothing here is caught.  ``Truncated`` from the fetcher, and
        ``RepositoryNotFoundError`` for a repository that no longer resolves,
        propagate to ``BaseRunner``, which writes a ``runner_failures`` row and
        carries on with the rest of the corpus — that repository fails, not the
        stage.  Returning an empty list instead is what made a failed run
        indistinguishable from a quiet one.

        A repository answering under a different ``nameWithOwner`` — a silent
        rename such as ``pymc-devs/pymc3`` → ``pymc-devs/pymc`` — is warned
        about inside ``fetch_merged_prs``, which still holds the node's
        ``repository`` block; normalisation drops it.
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
                # One batch per repository means duplicates are no longer
                # merely wasteful: Postgres refuses an upsert that touches the
                # same primary key twice in one statement.
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

        Scoped to the window and served by the ``(owner, repo, merged_at)``
        index from migration 00027.  The predecessor read every stored
        ``issue_number`` for the repository — 25 802 rows for pandas, roughly
        265 000 across a run, about 18 s of a 56 s run — so the cost grew with
        the table while the work stayed flat.
        """
        rows = await afetch_all(
            "pull_requests",
            select="issue_number",
            filters={"owner": owner, "repo": repo},
            **window_filters(self._since.isoformat(), self._until.isoformat()),
        )
        return {int(row["issue_number"]) for row in rows}
