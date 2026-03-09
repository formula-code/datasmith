"""Tests for datasmith.runners.scrape_commits — ScrapeCommitsRunner."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from datasmith.runners.scrape_commits import ScrapeCommitsRunner


def _mock_supabase() -> MagicMock:
    """Create a mock Supabase client with fluent API."""
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table
    table.upsert.return_value = table
    table.insert.return_value = table
    table.execute.return_value = MagicMock()
    return client


def _make_pr(number: int, merged: bool = True) -> dict[str, Any]:
    """Create a mock PR data dict."""
    pr: dict[str, Any] = {
        "number": number,
        "title": f"PR #{number}",
        "body": f"Body of PR #{number}",
        "state": "closed",
        "created_at": "2024-01-01T00:00:00Z",
        "closed_at": "2024-01-02T00:00:00Z",
        "merge_commit_sha": f"abc{number}",
        "base": {"sha": f"base{number}"},
        "head": {"sha": f"head{number}"},
        "labels": [{"name": "perf"}, {"name": "bug"}],
    }
    if merged:
        pr["merged_at"] = "2024-01-02T00:00:00Z"
    else:
        pr["merged_at"] = None
    return pr


class TestStoresPRData:
    async def test_stores_pr_data(self) -> None:
        """Mock GitHub API with merged PRs, verify upsert to pull_requests."""
        mock_client = _mock_supabase()

        gh_response = MagicMock()
        gh_response.json.return_value = [
            _make_pr(1, merged=True),
            _make_pr(2, merged=True),
        ]

        gh_client = MagicMock()
        gh_client._request = AsyncMock(return_value=gh_response)

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        # Verify pull_requests table was upserted to
        pr_calls = [call for call in mock_client.table.call_args_list if call.args[0] == "pull_requests"]
        # Should have 2 upsert calls (one per merged PR)
        assert len(pr_calls) == 2


class TestSkipsUnmerged:
    async def test_skips_unmerged(self) -> None:
        """Mock PRs without merged_at, verify they are not stored."""
        mock_client = _mock_supabase()

        gh_response = MagicMock()
        gh_response.json.return_value = [
            _make_pr(1, merged=False),
            _make_pr(2, merged=False),
            _make_pr(3, merged=True),
        ]

        gh_client = MagicMock()
        gh_client._request = AsyncMock(return_value=gh_response)

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("numpy", "numpy")])

        # Only the merged PR (#3) should produce a pull_requests upsert
        pr_calls = [call for call in mock_client.table.call_args_list if call.args[0] == "pull_requests"]
        assert len(pr_calls) == 1
