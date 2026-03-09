"""Pydantic v2 models for GitHub issues, PRs, and FormulaCode records."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


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
