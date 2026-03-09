"""Tests for datasmith.github.client — GitHubClient with httpx mocking."""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
import pytest
import respx

from datasmith.github.client import GitHubClient
from datasmith.github.models import PR, Issue
from datasmith.utils import TokenPool


@pytest.fixture()
def token_pool() -> TokenPool:
    return TokenPool(tokens=["ghp_test_token_1", "ghp_test_token_2"])


@pytest.fixture()
def client(token_pool: TokenPool) -> GitHubClient:
    return GitHubClient(token_pool)


class TestGetPR:
    @respx.mock
    async def test_get_pr_success(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/pandas-dev/pandas/pulls/123").mock(
            return_value=httpx.Response(
                200,
                json={
                    "title": "Speed up groupby",
                    "body": "This PR improves groupby performance",
                    "created_at": "2024-01-15T10:00:00Z",
                    "closed_at": "2024-01-20T10:00:00Z",
                    "merged_at": "2024-01-20T10:00:00Z",
                    "merge_commit_sha": "abc123def",
                    "base": {"sha": "base111"},
                    "head": {"sha": "head222"},
                    "labels": [{"name": "performance"}, {"name": "enhancement"}],
                },
            )
        )

        pr = await client.get_pr("pandas-dev", "pandas", 123)
        assert pr is not None
        assert isinstance(pr, PR)
        assert pr.title == "Speed up groupby"
        assert pr.merge_commit_sha == "abc123def"
        assert pr.base_sha == "base111"
        assert pr.head_sha == "head222"
        assert pr.labels == ["performance", "enhancement"]
        assert pr.repository == "pandas-dev/pandas"
        assert pr.issue_number == 123

        await client.close()

    @respx.mock
    async def test_get_pr_404_returns_none(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/owner/repo/pulls/999").mock(return_value=httpx.Response(404))

        pr = await client.get_pr("owner", "repo", 999)
        assert pr is None

        await client.close()

    @respx.mock
    async def test_get_pr_cached_single_http_call(self, client: GitHubClient) -> None:
        """Verify repeated calls still make HTTP requests (client itself is not cached)."""
        route = respx.get("https://api.github.com/repos/org/lib/pulls/1").mock(
            return_value=httpx.Response(
                200,
                json={
                    "title": "Test PR",
                    "body": "",
                    "merge_commit_sha": "sha1",
                    "base": {"sha": "b"},
                    "head": {"sha": "h"},
                    "labels": [],
                },
            )
        )

        pr1 = await client.get_pr("org", "lib", 1)
        pr2 = await client.get_pr("org", "lib", 1)
        assert pr1 is not None
        assert pr2 is not None
        assert pr1.title == pr2.title
        # Two separate HTTP calls (client doesn't cache on its own)
        assert route.call_count == 2

        await client.close()


class TestRateLimitRetry:
    @respx.mock
    async def test_rate_limit_retry(self, client: GitHubClient) -> None:
        """On 429, the client retries with a different token."""
        call_count = 0

        def _side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(
                    429,
                    headers={"X-RateLimit-Reset": "9999999999"},
                )
            return httpx.Response(
                200,
                json={
                    "title": "Retry PR",
                    "body": "",
                    "merge_commit_sha": "s",
                    "base": {"sha": "b"},
                    "head": {"sha": "h"},
                    "labels": [],
                },
            )

        respx.get("https://api.github.com/repos/o/r/pulls/1").mock(side_effect=_side_effect)

        pr = await client.get_pr("o", "r", 1)
        assert pr is not None
        assert pr.title == "Retry PR"
        assert call_count == 2

        await client.close()


class TestAstropyPR16222:
    """Regression test: validate all fields match the real astropy/astropy#16222 API response."""

    ASTROPY_PR_JSON: ClassVar[dict[str, Any]] = {
        "title": "PERF: skip angle wrapping when possible",
        "body": "Fixes some obvious low hanging fruit for angle performance.",
        "state": "closed",
        "created_at": "2024-03-19T22:21:14Z",
        "closed_at": "2024-03-20T13:56:37Z",
        "merged_at": "2024-03-20T13:56:37Z",
        "merge_commit_sha": "1ff8068f4378c64c15dc7a37cfd05e6ad1d69f93",
        "base": {"sha": "96cc7fbefd59e79d096802efe9f75b2f1d042487"},
        "head": {"sha": "74ff5d39fd3b42e50ba3ec6ae155a719b5c0b4d0"},
        "labels": [
            {"name": "coordinates"},
            {"name": "Performance"},
            {"name": "no-changelog-entry-needed"},
            {"name": "benchmark"},
        ],
    }

    @respx.mock
    async def test_get_pr_astropy_16222(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/astropy/astropy/pulls/16222").mock(
            return_value=httpx.Response(200, json=self.ASTROPY_PR_JSON)
        )

        pr = await client.get_pr("astropy", "astropy", 16222)
        assert pr is not None
        assert pr.repository == "astropy/astropy"
        assert pr.issue_number == 16222
        assert pr.title == "PERF: skip angle wrapping when possible"
        assert pr.state == "closed"
        assert pr.merge_commit_sha == "1ff8068f4378c64c15dc7a37cfd05e6ad1d69f93"
        assert pr.base_sha == "96cc7fbefd59e79d096802efe9f75b2f1d042487"
        assert pr.head_sha == "74ff5d39fd3b42e50ba3ec6ae155a719b5c0b4d0"
        assert pr.labels == ["coordinates", "Performance", "no-changelog-entry-needed", "benchmark"]
        assert pr.merged_at is not None
        assert pr.merged_at.year == 2024
        assert pr.merged_at.month == 3
        assert pr.merged_at.day == 20

        # to_record should succeed since merge_commit_sha is populated
        record = pr.to_record()
        assert record is not None
        assert record.gt_hash == "1ff8068f4378c64c15dc7a37cfd05e6ad1d69f93"
        assert record.task_id == "astropy__astropy-16222"

        await client.close()

    async def test_direct_construction_has_empty_defaults(self) -> None:
        """Direct PR construction gives empty defaults — use GitHubClient.get_pr() for populated models."""
        pr = PR(repository="astropy/astropy", issue_number=16222)
        assert pr.merge_commit_sha == ""
        assert pr.state == ""
        assert pr.to_record() is None


class TestGetIssue:
    @respx.mock
    async def test_get_issue_success(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/org/repo/issues/55").mock(
            return_value=httpx.Response(
                200,
                json={
                    "title": "Slow performance on large files",
                    "body": "Detailed description",
                    "created_at": "2024-03-01T00:00:00Z",
                    "closed_at": None,
                },
            )
        )

        issue = await client.get_issue("org", "repo", 55)
        assert issue is not None
        assert isinstance(issue, Issue)
        assert issue.title == "Slow performance on large files"
        assert issue.repository == "org/repo"
        assert issue.issue_number == 55

        await client.close()


class TestUsesTokenPool:
    @respx.mock
    async def test_uses_token_pool(self, token_pool: TokenPool, client: GitHubClient) -> None:
        """Verify the Authorization header contains a token from the pool."""
        captured_headers: list[dict[str, str]] = []

        def _capture(request: httpx.Request) -> httpx.Response:
            captured_headers.append(dict(request.headers))
            return httpx.Response(
                200,
                json={
                    "title": "",
                    "body": "",
                    "merge_commit_sha": "",
                    "base": {"sha": ""},
                    "head": {"sha": ""},
                    "labels": [],
                },
            )

        respx.get("https://api.github.com/repos/o/r/pulls/1").mock(side_effect=_capture)

        await client.get_pr("o", "r", 1)

        assert len(captured_headers) == 1
        auth = captured_headers[0].get("authorization", "")
        assert auth.startswith("Bearer ghp_test_token_")

        await client.close()


class TestGraphQL:
    @respx.mock
    async def test_graphql_query(self, client: GitHubClient) -> None:
        respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {
                        "repository": {
                            "name": "pandas",
                        }
                    }
                },
            )
        )

        result = await client.graphql(
            query="query { repository(owner: $owner, name: $name) { name } }",
            variables={"owner": "pandas-dev", "name": "pandas"},
        )

        assert result["data"]["repository"]["name"] == "pandas"

        await client.close()


class TestGetTimeline:
    @respx.mock
    async def test_get_timeline(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/o/r/issues/1/timeline").mock(
            return_value=httpx.Response(
                200,
                json=[{"event": "cross-referenced"}, {"event": "closed"}],
            )
        )

        timeline = await client.get_timeline("o", "r", 1)
        assert len(timeline) == 2
        assert timeline[0]["event"] == "cross-referenced"

        await client.close()


class TestGetDiff:
    @respx.mock
    async def test_get_diff(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/o/r/pulls/1").mock(
            return_value=httpx.Response(200, text="diff --git a/file.py b/file.py\n+optimized")
        )

        diff = await client.get_diff("o", "r", 1)
        assert "diff --git" in diff
        assert "+optimized" in diff

        await client.close()


class TestGetFiles:
    @respx.mock
    async def test_get_files(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/o/r/pulls/1/files").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"filename": "src/module.py", "status": "modified", "additions": 10, "deletions": 3},
                ],
            )
        )

        files = await client.get_files("o", "r", 1)
        assert len(files) == 1
        assert files[0]["filename"] == "src/module.py"

        await client.close()
