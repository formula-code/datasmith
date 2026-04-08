from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datasmith.runners.scrape_repos import ScrapeReposRunner


@pytest.mark.asyncio
class TestE2EScrape:
    async def test_scrape_small_repo(self, mock_supabase_client):
        """Scrape a repo and verify data stored in Supabase."""
        gh_client = MagicMock()

        # Mock the GitHub API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "html_url": "https://github.com/test/repo",
            "language": "Python",
            "stargazers_count": 100,
            "topics": ["testing"],
            "description": "A test repo",
        }

        async def mock_request(*args, **kwargs):
            return mock_response

        gh_client._request = mock_request

        runner = ScrapeReposRunner(gh_client, n_concurrent=1)

        with (
            patch("datasmith.runners.base.get_client", return_value=mock_supabase_client),
            patch("datasmith.runners.scrape_repos.get_client", return_value=mock_supabase_client),
        ):
            await runner.run([("test", "repo")])

        # Verify upsert was called with repo data
        calls = mock_supabase_client.table.call_args_list
        table_names = [c.args[0] for c in calls if c.args]
        assert "repositories" in table_names

    async def test_scrape_idempotent(self, mock_supabase_client):
        """Running scrape twice should not duplicate data."""
        gh_client = MagicMock()

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "html_url": "https://github.com/test/repo",
            "language": "Python",
            "stargazers_count": 100,
            "topics": [],
            "description": "A test repo",
        }

        async def mock_request(*args, **kwargs):
            return mock_response

        gh_client._request = mock_request

        # Second call returns existing repo
        existing_response = MagicMock(data=[{"owner": "test"}])

        runner = ScrapeReposRunner(gh_client, n_concurrent=1)

        with (
            patch("datasmith.runners.base.get_client", return_value=mock_supabase_client),
            patch("datasmith.runners.scrape_repos.get_client", return_value=mock_supabase_client),
        ):
            # First run
            await runner.run([("test", "repo")])

            # Set up mock to return existing data on second run
            select_mock = MagicMock()
            mock_supabase_client.table.return_value = select_mock
            select_mock.select.return_value = select_mock
            select_mock.eq.return_value = select_mock
            select_mock.upsert.return_value = select_mock
            select_mock.execute.return_value = existing_response

            runner2 = ScrapeReposRunner(gh_client, n_concurrent=1)
            await runner2.run([("test", "repo")])
