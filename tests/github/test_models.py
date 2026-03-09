"""Tests for datasmith.github.models — Issue, PR, FormulaCodeRecord."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from datasmith.github.models import PR, FormulaCodeRecord, Issue, IssueExpanded


class TestIssue:
    def test_cache_key_format(self) -> None:
        issue = Issue(repository="owner/repo", issue_number=42)
        assert issue.cache_key == "owner/repo:42"

    def test_owner_property(self) -> None:
        issue = Issue(repository="myorg/myrepo", issue_number=1)
        assert issue.owner == "myorg"

    def test_repo_property(self) -> None:
        issue = Issue(repository="myorg/myrepo", issue_number=1)
        assert issue.repo == "myrepo"

    def test_frozen_immutability(self) -> None:
        issue = Issue(repository="owner/repo", issue_number=1, title="Hello")
        with pytest.raises(ValidationError):
            issue.title = "Changed"  # type: ignore[misc]

    def test_json_roundtrip(self) -> None:
        issue = Issue(
            repository="owner/repo",
            issue_number=10,
            title="Bug",
            body="Details here",
        )
        data = issue.model_dump(mode="json")
        restored = Issue.model_validate(data)
        assert restored.repository == issue.repository
        assert restored.issue_number == issue.issue_number
        assert restored.title == issue.title
        assert restored.body == issue.body

    def test_defaults(self) -> None:
        issue = Issue(repository="a/b", issue_number=1)
        assert issue.title == ""
        assert issue.body == ""
        assert issue.created_at is None
        assert issue.closed_at is None


class TestPR:
    def test_pr_inherits_issue(self) -> None:
        pr = PR(repository="owner/repo", issue_number=5, title="Perf fix")
        assert isinstance(pr, Issue)
        assert pr.cache_key == "owner/repo:5"
        assert pr.owner == "owner"
        assert pr.repo == "repo"

    def test_pr_frozen_immutability(self) -> None:
        pr = PR(repository="owner/repo", issue_number=1, title="Test")
        with pytest.raises(ValidationError):
            pr.title = "Changed"  # type: ignore[misc]

    def test_pr_json_roundtrip(self) -> None:
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        pr = PR(
            repository="org/lib",
            issue_number=99,
            title="Speed up parsing",
            body="This PR speeds up parsing by 3x",
            merged_at=now,
            merge_commit_sha="abc123",
            base_sha="def456",
            head_sha="ghi789",
            labels=["performance", "enhancement"],
        )
        data = pr.model_dump(mode="json")
        restored = PR.model_validate(data)
        assert restored.merge_commit_sha == "abc123"
        assert restored.labels == ["performance", "enhancement"]
        assert restored.merged_at == now

    def test_to_record_success(self) -> None:
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        pr = PR(
            repository="myorg/mylib",
            issue_number=42,
            merge_commit_sha="sha123",
            base_sha="base456",
            merged_at=now,
        )
        record = pr.to_record()
        assert record is not None
        assert isinstance(record, FormulaCodeRecord)
        assert record.owner == "myorg"
        assert record.repo == "mylib"
        assert record.issue_number == 42
        assert record.task_id == "myorg__mylib-42"
        assert record.gt_hash == "sha123"
        assert record.base_commit == "base456"
        assert record.date == now

    def test_to_record_returns_none_without_merge_sha(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=10,
            merge_commit_sha="",
        )
        assert pr.to_record() is None

    def test_pr_defaults(self) -> None:
        pr = PR(repository="a/b", issue_number=1)
        assert pr.merged_at is None
        assert pr.merge_commit_sha == ""
        assert pr.base_sha == ""
        assert pr.head_sha == ""
        assert pr.labels == []
        assert pr.file_changes is None


class TestIssueExpanded:
    def test_issue_expanded_defaults(self) -> None:
        ie = IssueExpanded(number=5)
        assert ie.title == ""
        assert ie.url == ""
        assert ie.description == ""
        assert ie.comments == []
        assert ie.cross_references == []

    def test_issue_expanded_roundtrip(self) -> None:
        ie = IssueExpanded(
            number=10,
            title="Slow indexing",
            description="Indexing takes too long",
            comments=["I agree", "Me too"],
            cross_references=["#11", "#12"],
        )
        data = ie.model_dump(mode="json")
        restored = IssueExpanded.model_validate(data)
        assert restored.number == 10
        assert len(restored.comments) == 2


class TestFormulaCodeRecord:
    def test_record_fields(self) -> None:
        record = FormulaCodeRecord(
            owner="org",
            repo="lib",
            issue_number=1,
            task_id="org__lib-1",
            gt_hash="abc",
            patch="diff content",
        )
        assert record.owner == "org"
        assert record.patch == "diff content"
        assert record.container_name == ""
        assert record.image_name == ""
