"""Pydantic v2 models for GitHub issues, PRs, and FormulaCode records."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from datasmith.github.client import GitHubClient

logger = logging.getLogger(__name__)


class Issue(BaseModel):
    """A GitHub issue."""

    model_config = ConfigDict(frozen=True)

    repository: str  # "owner/repo"
    issue_number: int
    title: str = ""
    body: str = ""
    created_at: datetime | None = None
    closed_at: datetime | None = None
    _meta: dict[str, Any] | None = None

    @property
    def cache_key(self) -> str:
        return f"{self.repository}:{self.issue_number}"

    @property
    def owner(self) -> str:
        return self.repository.split("/")[0]

    @property
    def repo(self) -> str:
        return self.repository.split("/")[1]


class PRFileChange(BaseModel):
    """A single file change in a pull request."""

    model_config = ConfigDict(frozen=True)

    filename: str
    status: str = ""
    additions: int = 0
    deletions: int = 0
    changes: int = 0


class PRChangeSummary(BaseModel):
    """Summary of all file changes in a pull request."""

    model_config = ConfigDict(frozen=True)

    files: list[PRFileChange] = []
    total_additions: int = 0
    total_deletions: int = 0
    total_changes: int = 0


class PR(Issue):
    """A GitHub pull request (extends Issue)."""

    state: str = ""
    merged_at: datetime | None = None
    merge_commit_sha: str = ""
    base_sha: str = ""
    head_sha: str = ""
    labels: list[str] = []
    file_changes: PRChangeSummary | None = None

    @classmethod
    async def fetch(
        cls,
        repository: str,
        issue_number: int,
        *,
        client: GitHubClient | None = None,
    ) -> PR:
        """Hydrate a PR from Supabase cache, falling back to the GitHub API.

        Parameters
        ----------
        repository:
            ``"owner/repo"`` string.
        issue_number:
            PR number.
        client:
            Optional :class:`~datasmith.github.client.GitHubClient`.  One is
            created automatically (from ``GH_TOKENS`` env) when not supplied.

        Raises
        ------
        LookupError
            If the PR cannot be found in either Supabase or GitHub.
        """
        owner, repo = repository.split("/", 1)

        # --- 1. Try Supabase -----------------------------------------------
        pr = cls._fetch_from_supabase(owner, repo, issue_number)
        if pr is not None:
            return pr

        # --- 2. Fall back to GitHub API ------------------------------------
        if client is None:
            from datasmith.github.client import GitHubClient
            from datasmith.utils import TokenPool

            client = GitHubClient(TokenPool())

        pr = await client.get_pr(owner, repo, issue_number)
        if pr is not None:
            return pr

        raise LookupError(f"PR {repository}#{issue_number} not found in Supabase or GitHub")

    @classmethod
    def _fetch_from_supabase(cls, owner: str, repo: str, issue_number: int) -> PR | None:
        """Attempt to load a PR from the ``pull_requests`` Supabase table."""
        try:
            from datasmith.utils import get_client

            sb = get_client()
            resp = (
                sb.table("pull_requests")
                .select("*")
                .eq("owner", owner)
                .eq("repo", repo)
                .eq("issue_number", issue_number)
                .execute()
            )
        except Exception:
            logger.debug("Supabase lookup failed for %s/%s#%d", owner, repo, issue_number)
            return None

        if not resp.data:
            return None

        row: dict[str, Any] = resp.data[0]  # type: ignore[assignment]
        return cls(
            repository=f"{row['owner']}/{row['repo']}",
            issue_number=row["issue_number"],
            title=row.get("title", "") or "",
            body=row.get("body", "") or "",
            state=row.get("state", "") or "",
            created_at=row.get("created_at"),
            closed_at=row.get("closed_at"),
            merged_at=row.get("merged_at"),
            merge_commit_sha=row.get("merge_commit_sha", "") or "",
            base_sha=row.get("base_sha", "") or "",
            head_sha=row.get("head_sha", "") or "",
            labels=row.get("labels") or [],
        )

    def to_record(self) -> FormulaCodeRecord | None:
        """Convert to a FormulaCodeRecord, or None if not merged."""
        if not self.merge_commit_sha:
            return None
        return FormulaCodeRecord(
            owner=self.owner,
            repo=self.repo,
            issue_number=self.issue_number,
            task_id=f"{self.owner}__{self.repo}-{self.issue_number}",
            gt_hash=self.merge_commit_sha,
            base_commit=self.base_sha,
            date=self.merged_at,
            instructions="",
            classification="",
            difficulty="",
        )


class IssueExpanded(BaseModel):
    """An expanded issue with comments and cross-references."""

    number: int
    title: str = ""
    url: str = ""
    description: str = ""
    comments: list[str] = []
    created_at: datetime | None = None
    closed_at: datetime | None = None
    cross_references: list[str] = []


class FormulaCodeRecord(BaseModel):
    """A record in the FormulaCode dataset."""

    owner: str
    repo: str
    issue_number: int
    task_id: str
    gt_hash: str = ""
    base_commit: str = ""
    date: datetime | None = None
    instructions: str = ""
    classification: str = ""
    difficulty: str = ""
    container_name: str = ""
    patch: str = ""
    image_name: str = ""
