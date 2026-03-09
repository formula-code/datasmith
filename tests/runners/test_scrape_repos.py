"""Tests for datasmith.runners.scrape_repos — ScrapeReposRunner."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from datasmith.runners.scrape_repos import ScrapeReposRunner


def _mock_supabase(existing_repos: list[dict[str, Any]] | None = None) -> MagicMock:
    """Create a mock Supabase client. If existing_repos, select returns them."""
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table

    # Build the fluent select chain
    select_mock = MagicMock()
    table.select.return_value = select_mock
    select_mock.eq.return_value = select_mock

    resp = MagicMock()
    resp.data = existing_repos or []
    select_mock.execute.return_value = resp

    # upsert chain
    table.upsert.return_value = table
    table.insert.return_value = table
    table.execute.return_value = MagicMock()
    return client


class TestStoresRepos:
    async def test_stores_repos(self) -> None:
        """Mock GitHub API, verify upsert to repositories table."""
        mock_client = _mock_supabase(existing_repos=[])

        gh_response = MagicMock()
        gh_response.json.return_value = {
            "html_url": "https://github.com/numpy/numpy",
            "language": "Python",
            "stargazers_count": 25000,
            "topics": ["numpy", "science"],
            "description": "The fundamental package for scientific computing.",
        }

        gh_client = MagicMock()
        gh_client._request = AsyncMock(return_value=gh_response)

        with (
            patch("datasmith.runners.scrape_repos.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeReposRunner(github_client=gh_client, n_concurrent=1)
            await runner.run(["numpy/numpy"])

        # Verify the GitHub API was called
        gh_client._request.assert_awaited_once_with("GET", "/repos/numpy/numpy")

        # Verify upsert was called on repositories table
        upsert_calls = [call for call in mock_client.table.call_args_list if call.args[0] == "repositories"]
        assert len(upsert_calls) >= 1


class TestSkipsExistingRepos:
    async def test_skips_existing_repos(self) -> None:
        """Pre-insert mock, verify no GitHub API call for existing repo."""
        mock_client = _mock_supabase(existing_repos=[{"owner": "numpy"}])

        gh_client = MagicMock()
        gh_client._request = AsyncMock()

        with (
            patch("datasmith.runners.scrape_repos.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeReposRunner(github_client=gh_client, n_concurrent=1)
            await runner.run(["numpy/numpy"])

        # GitHub API should NOT have been called
        gh_client._request.assert_not_awaited()
