"""Tests for datasmith.github.client — GitHubClient with httpx mocking.

Everything here runs offline against fixtures; nothing reaches GitHub.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any, ClassVar

import httpx
import pytest
import respx

from datasmith.filters import check_file_compliance
from datasmith.github import client as client_mod
from datasmith.github.client import (
    DiffStatus,
    GitHubClient,
    GitHubGraphQLError,
    RepositoryNotFoundError,
    Truncated,
    _parse_retry_after,
    _rate_limit_wait,
)
from datasmith.github.models import PR, Issue, IssueExpanded
from datasmith.utils import TokenPool


@pytest.fixture(autouse=True)
def _no_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collapse every backoff and rate-limit wait to zero.

    The client now honours ``Retry-After`` and ``X-RateLimit-Reset`` for real,
    so without this a fixture advertising a distant reset would make the suite
    sleep for the whole window.
    """
    monkeypatch.setattr(client_mod, "DATASMITH_GH_BACKOFF_BASE_S", 0.0)
    monkeypatch.setattr(client_mod, "DATASMITH_GH_MAX_RETRY_WAIT_S", 0.0)


@pytest.fixture()
def token_pool() -> TokenPool:
    return TokenPool(tokens=["ghp_test_token_1", "ghp_test_token_2"])


@pytest.fixture()
def client(token_pool: TokenPool) -> GitHubClient:
    return GitHubClient(token_pool)


class _RecordingPool(TokenPool):
    """Token pool that remembers what the client reported to it."""

    def __init__(self, tokens: list[str]) -> None:
        super().__init__(tokens)
        self.reports: list[tuple[str, int, float]] = []

    def report_rate_limit(self, token: str, remaining: int = 0, reset_at: float = 0.0) -> None:
        self.reports.append((token, remaining, reset_at))
        super().report_rate_limit(token, remaining=remaining, reset_at=reset_at)


# ---------------------------------------------------------------------------
# GraphQL search fixtures
# ---------------------------------------------------------------------------


def _pr_node(
    number: int,
    *,
    merged_at: str = "2026-07-01T12:00:00Z",
    created_at: str = "2026-06-01T09:00:00Z",
    title: str = "PERF: speed up the thing",
    files: list[dict[str, Any]] | None = None,
    files_total: int | None = None,
    name_with_owner: str = "octo/repo",
) -> dict[str, Any]:
    """Build one GraphQL PullRequest node as the search API shapes it."""
    file_nodes = files if files is not None else [{"path": "src/core.py", "additions": 10, "deletions": 2}]
    return {
        "number": number,
        "title": title,
        "body": "body text",
        "createdAt": created_at,
        "mergedAt": merged_at,
        "closedAt": merged_at,
        "mergeCommit": {"oid": f"sha{number}"},
        "baseRefOid": f"base{number}",
        "headRefOid": f"head{number}",
        "repository": {"nameWithOwner": name_with_owner},
        "labels": {"nodes": [{"name": "performance"}]},
        "files": {
            "totalCount": files_total if files_total is not None else len(file_nodes),
            "nodes": file_nodes,
        },
    }


def _search_payload(
    count: int,
    nodes: list[dict[str, Any]],
    *,
    has_next: bool = False,
    cursor: str | None = None,
) -> dict[str, Any]:
    return {
        "data": {
            "search": {
                "issueCount": count,
                "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                "nodes": nodes,
            }
        }
    }


def _graphql_vars(request: httpx.Request) -> dict[str, Any]:
    body = json.loads(request.content.decode())
    variables: dict[str, Any] = body.get("variables") or {}
    return variables


def _route_by_query(
    responses: dict[str, list[dict[str, Any]]],
    seen: list[str] | None = None,
) -> Any:
    """Return a respx side-effect that answers by the search query string.

    Each key is the exact ``q`` variable of a shard; each value is that
    shard's pages in order, so pagination and bisection can both be driven
    from one fixture.
    """
    cursors: dict[str, int] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        query = str(_graphql_vars(request).get("q", ""))
        if seen is not None:
            seen.append(query)
        if query not in responses:
            raise AssertionError(f"unexpected search query: {query!r}")
        index = cursors.get(query, 0)
        cursors[query] = index + 1
        pages = responses[query]
        return httpx.Response(200, json=pages[min(index, len(pages) - 1)])

    return _handler


def _q(owner: str, repo: str, lo: str, hi: str) -> str:
    return f"repo:{owner}/{repo} is:pr is:merged merged:{lo}..{hi}"


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


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
        assert record.task_id == 16222

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


class TestGetIssueExpanded:
    @respx.mock
    async def test_returns_expanded_with_comments_and_xrefs(self, client: GitHubClient) -> None:
        """get_issue_expanded combines get_issue + get_timeline into IssueExpanded."""
        respx.get("https://api.github.com/repos/org/repo/issues/10").mock(
            return_value=httpx.Response(
                200,
                json={
                    "title": "Slow groupby",
                    "body": "Groupby is too slow on large datasets",
                    "created_at": "2024-03-01T00:00:00Z",
                    "closed_at": "2024-03-10T00:00:00Z",
                },
            )
        )
        respx.get("https://api.github.com/repos/org/repo/issues/10/timeline").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"event": "commented", "body": "I see the same issue"},
                    {"event": "commented", "body": "Confirmed on v2.0"},
                    {"event": "cross-referenced", "source": {"issue": {"body": "Related: see #10"}}},
                    {"event": "closed"},
                    {"event": "commented", "body": ""},  # empty body — should be skipped
                ],
            )
        )

        expanded = await client.get_issue_expanded("org", "repo", 10)
        assert expanded is not None
        assert isinstance(expanded, IssueExpanded)
        assert expanded.number == 10
        assert expanded.title == "Slow groupby"
        assert expanded.description == "Groupby is too slow on large datasets"
        assert expanded.url == "https://github.com/org/repo/issues/10"
        assert expanded.comments == ["I see the same issue", "Confirmed on v2.0"]
        assert expanded.cross_references == ["Related: see #10"]
        assert expanded.created_at is not None
        assert expanded.closed_at is not None

        await client.close()

    @respx.mock
    async def test_returns_none_for_missing_issue(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/org/repo/issues/999").mock(return_value=httpx.Response(404))

        expanded = await client.get_issue_expanded("org", "repo", 999)
        assert expanded is None

        await client.close()

    @respx.mock
    async def test_empty_timeline(self, client: GitHubClient) -> None:
        """Issue with no timeline events still returns valid IssueExpanded."""
        respx.get("https://api.github.com/repos/org/repo/issues/5").mock(
            return_value=httpx.Response(
                200,
                json={
                    "title": "Bug report",
                    "body": "Something is broken",
                    "created_at": "2024-06-01T00:00:00Z",
                    "closed_at": None,
                },
            )
        )
        respx.get("https://api.github.com/repos/org/repo/issues/5/timeline").mock(
            return_value=httpx.Response(200, json=[])
        )

        expanded = await client.get_issue_expanded("org", "repo", 5)
        assert expanded is not None
        assert expanded.comments == []
        assert expanded.cross_references == []
        assert expanded.description == "Something is broken"

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


class TestRetryAfter:
    """``_request`` must honour ``Retry-After``, not only ``X-RateLimit-Reset``."""

    def test_parse_retry_after_delta_seconds(self) -> None:
        assert _parse_retry_after("60") == 60.0
        assert _parse_retry_after(" 30 ") == 30.0

    def test_parse_retry_after_http_date(self) -> None:
        delta = _parse_retry_after("Wed, 21 Oct 2099 07:28:00 GMT")
        assert delta is not None
        assert delta > 0

    def test_parse_retry_after_absent_or_garbage(self) -> None:
        assert _parse_retry_after(None) is None
        assert _parse_retry_after("") is None
        assert _parse_retry_after("soon") is None

    def test_retry_after_wins_over_reset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A secondary rate limit sends Retry-After; it must decide the wait."""
        monkeypatch.setattr(client_mod, "DATASMITH_GH_MAX_RETRY_WAIT_S", 3600.0)
        resp = httpx.Response(403, headers={"Retry-After": "45", "X-RateLimit-Reset": "9999999999"})
        wait, reset_at, source = _rate_limit_wait(resp, attempt=0)
        assert source == "Retry-After"
        assert wait == pytest.approx(45.0, abs=1.0)
        # Absolute epoch, not the delta: TokenPool compares against time.time().
        assert reset_at is not None
        assert reset_at == pytest.approx(time.time() + 45.0, abs=2.0)

    def test_reset_header_used_when_no_retry_after(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(client_mod, "DATASMITH_GH_MAX_RETRY_WAIT_S", 3600.0)
        reset = time.time() + 120.0
        resp = httpx.Response(429, headers={"X-RateLimit-Reset": str(reset)})
        wait, reset_at, source = _rate_limit_wait(resp, attempt=0)
        assert source == "X-RateLimit-Reset"
        assert wait == pytest.approx(120.0, abs=2.0)
        assert reset_at == pytest.approx(reset, abs=0.01)

    def test_backoff_when_no_headers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(client_mod, "DATASMITH_GH_MAX_RETRY_WAIT_S", 3600.0)
        monkeypatch.setattr(client_mod, "DATASMITH_GH_BACKOFF_BASE_S", 1.0)
        wait, reset_at, source = _rate_limit_wait(httpx.Response(403), attempt=2)
        assert source == "backoff"
        assert reset_at is None
        assert wait == 4.0

    def test_wait_is_capped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(client_mod, "DATASMITH_GH_MAX_RETRY_WAIT_S", 10.0)
        resp = httpx.Response(403, headers={"Retry-After": "9000"})
        wait, _, _ = _rate_limit_wait(resp, attempt=0)
        assert wait == 10.0

    @respx.mock
    async def test_request_honours_retry_after_with_no_reset_header(self) -> None:
        """The secondary-limit shape: Retry-After present, reset header absent."""
        pool = _RecordingPool(["ghp_a", "ghp_b"])
        gh = GitHubClient(pool)
        calls = 0

        def _side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(403, headers={"Retry-After": "17"})
            return httpx.Response(200, json=[])

        respx.get("https://api.github.com/repos/o/r/pulls/1/files").mock(side_effect=_side_effect)

        files = await gh.get_files("o", "r", 1)
        assert files == []
        assert calls == 2
        assert len(pool.reports) == 1
        token, remaining, reset_at = pool.reports[0]
        assert token == "ghp_a"
        assert remaining == 0
        assert reset_at == pytest.approx(time.time() + 17.0, abs=3.0)

        await gh.close()


class TestGraphQLErrorHandling:
    """GitHub answers GraphQL failures with HTTP 200 plus an ``errors`` array."""

    @respx.mock
    async def test_not_found_raises(self, client: GitHubClient) -> None:
        respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {"repository": None},
                    "errors": [
                        {
                            "type": "NOT_FOUND",
                            "path": ["repository"],
                            "message": "Could not resolve to a Repository with the name 'ghost/repo'.",
                        }
                    ],
                },
            )
        )

        with pytest.raises(RepositoryNotFoundError) as excinfo:
            await client.graphql("query { repository { name } }")
        assert "repository" in str(excinfo.value)
        assert excinfo.value.errors[0]["type"] == "NOT_FOUND"

        await client.close()

    @respx.mock
    async def test_errors_body_is_not_an_empty_result(self, client: GitHubClient) -> None:
        """An error with no data must raise, not read as 'this repo has no PRs'."""
        respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={"data": None, "errors": [{"message": "Something went wrong"}]},
            )
        )

        with pytest.raises(GitHubGraphQLError):
            await client.graphql("query { viewer { login } }")

        await client.close()

    @respx.mock
    async def test_rate_limited_retries_then_succeeds(self) -> None:
        pool = _RecordingPool(["ghp_a", "ghp_b"])
        gh = GitHubClient(pool)
        calls = 0

        def _side_effect(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(
                    200,
                    json={"data": None, "errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]},
                )
            return httpx.Response(200, json={"data": {"viewer": {"login": "octo"}}})

        respx.post("https://api.github.com/graphql").mock(side_effect=_side_effect)

        result = await gh.graphql("query { viewer { login } }")
        assert result["data"]["viewer"]["login"] == "octo"
        assert calls == 2
        assert pool.reports and pool.reports[0][1] == 0

        await gh.close()

    @respx.mock
    async def test_rate_limited_forever_raises(self, client: GitHubClient) -> None:
        route = respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={"data": None, "errors": [{"type": "RATE_LIMITED", "message": "API rate limit exceeded"}]},
            )
        )

        with pytest.raises(GitHubGraphQLError):
            await client.graphql("query { viewer { login } }", retries=2)
        assert route.call_count == 2

        await client.close()

    @respx.mock
    async def test_scoped_error_keeps_partial_data(self, client: GitHubClient) -> None:
        """A scoped error alongside usable data must not discard the good half."""
        respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {"search": {"issueCount": 1, "nodes": [{"number": 5}]}, "rateLimit": None},
                    "errors": [
                        {
                            "type": "SERVICE_UNAVAILABLE",
                            "path": ["rateLimit"],
                            "message": "timed out",
                        }
                    ],
                },
            )
        )

        result = await client.graphql("query { search { issueCount } rateLimit { cost } }")
        assert result["data"]["search"]["nodes"] == [{"number": 5}]

        await client.close()

    @respx.mock
    async def test_graphql_rotates_token_on_http_403(self) -> None:
        """graphql goes through _request, so it inherits retry and rotation."""
        pool = _RecordingPool(["ghp_a", "ghp_b"])
        gh = GitHubClient(pool)
        seen_tokens: list[str] = []

        def _side_effect(request: httpx.Request) -> httpx.Response:
            seen_tokens.append(request.headers["authorization"])
            if len(seen_tokens) == 1:
                return httpx.Response(403, headers={"X-RateLimit-Reset": str(time.time() + 5)})
            return httpx.Response(200, json={"data": {"viewer": {"login": "octo"}}})

        respx.post("https://api.github.com/graphql").mock(side_effect=_side_effect)

        result = await gh.graphql("query { viewer { login } }")
        assert result["data"]["viewer"]["login"] == "octo"
        assert seen_tokens == ["Bearer ghp_a", "Bearer ghp_b"]

        await gh.close()


class TestFetchMergedPRs:
    """The merged_at-windowed search fetcher (spec 4.2)."""

    @respx.mock
    async def test_window_is_half_open_and_query_shape(self, client: GitHubClient) -> None:
        """A..B is inclusive at both ends, so the exclusive bound loses one second."""
        seen: list[str] = []
        query = _q("octo", "repo", "2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z")
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({query: [_search_payload(1, [_pr_node(7)])]}, seen)
        )

        prs = await client.fetch_merged_prs(
            "octo",
            "repo",
            _dt("2026-08-01T00:00:00"),
            _dt("2026-08-02T00:00:00"),
        )
        assert seen == [query]
        assert [pr["number"] for pr in prs] == [7]

        await client.close()

    @respx.mock
    async def test_node_normalisation(self, client: GitHubClient) -> None:
        """GraphQL says 'path'; every consumer in this project reads 'filename'."""
        query = _q("octo", "repo", "2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z")
        node = _pr_node(
            7,
            files=[
                {"path": "src/core.py", "additions": 10, "deletions": 2},
                {"path": "docs/index.rst", "additions": 1, "deletions": 0},
            ],
        )
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({query: [_search_payload(1, [node])]})
        )

        prs = await client.fetch_merged_prs("octo", "repo", _dt("2026-08-01T00:00:00"), _dt("2026-08-02T00:00:00"))
        pr = prs[0]
        assert pr["merged_at"] == "2026-07-01T12:00:00Z"
        assert pr["created_at"] == "2026-06-01T09:00:00Z"
        assert pr["merge_commit_sha"] == "sha7"
        assert pr["base"]["sha"] == "base7"
        assert pr["head"]["sha"] == "head7"
        assert pr["labels"] == [{"name": "performance"}]
        assert pr["state"] == "closed"
        assert pr["changed_files"] == 2
        assert [f["filename"] for f in pr["file_changes"]] == ["src/core.py", "docs/index.rst"]
        assert pr["file_changes"][0]["additions"] == 10

        await client.close()

    @respx.mock
    async def test_paginates_to_exhaustion(self, client: GitHubClient) -> None:
        query = _q("octo", "repo", "2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z")
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({
                query: [
                    _search_payload(3, [_pr_node(1), _pr_node(2)], has_next=True, cursor="c1"),
                    _search_payload(3, [_pr_node(3)]),
                ]
            })
        )

        stats: dict[str, int] = {}
        prs = await client.fetch_merged_prs(
            "octo", "repo", _dt("2026-08-01T00:00:00"), _dt("2026-08-02T00:00:00"), stats=stats
        )
        assert [pr["number"] for pr in prs] == [1, 2, 3]
        assert stats["queries"] == 2
        assert stats["bisections"] == 0

        await client.close()

    @respx.mock
    async def test_leaf_short_of_issue_count_raises(self, client: GitHubClient) -> None:
        """The invariant that turns a silent truncation into a loud failure."""
        query = _q("octo", "repo", "2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z")
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({query: [_search_payload(9, [_pr_node(1), _pr_node(2)])]})
        )

        with pytest.raises(Truncated) as excinfo:
            await client.fetch_merged_prs("octo", "repo", _dt("2026-08-01T00:00:00"), _dt("2026-08-02T00:00:00"))
        assert "got 2 of 9" in str(excinfo.value)

        await client.close()

    @respx.mock
    async def test_bisects_above_the_cap(self, client: GitHubClient) -> None:
        """issueCount above the cap splits the window at the midpoint."""
        whole = _q("octo", "repo", "2026-07-01T00:00:00Z", "2026-07-02T23:59:59Z")
        left = _q("octo", "repo", "2026-07-01T00:00:00Z", "2026-07-01T23:59:59Z")
        right = _q("octo", "repo", "2026-07-02T00:00:00Z", "2026-07-02T23:59:59Z")
        seen: list[str] = []
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query(
                {
                    whole: [_search_payload(1500, [])],
                    left: [_search_payload(2, [_pr_node(1), _pr_node(2)])],
                    right: [_search_payload(2, [_pr_node(3), _pr_node(4)])],
                },
                seen,
            )
        )

        stats: dict[str, int] = {}
        prs = await client.fetch_merged_prs(
            "octo", "repo", _dt("2026-07-01T00:00:00"), _dt("2026-07-03T00:00:00"), stats=stats
        )
        numbers = [pr["number"] for pr in prs]
        assert numbers == [1, 2, 3, 4]
        # Disjoint shards: the one-second subtraction is what prevents duplicates.
        assert len(numbers) == len(set(numbers))
        assert stats["bisections"] == 1
        assert stats["queries"] == 3
        assert seen[0] == whole
        assert sorted(seen[1:]) == sorted([left, right])

        await client.close()

    @respx.mock
    async def test_bisection_stops_once_a_shard_fits(self, client: GitHubClient) -> None:
        """Only the oversized shard is split again."""
        whole = _q("octo", "repo", "2026-07-01T00:00:00Z", "2026-07-02T23:59:59Z")
        left = _q("octo", "repo", "2026-07-01T00:00:00Z", "2026-07-01T23:59:59Z")
        right = _q("octo", "repo", "2026-07-02T00:00:00Z", "2026-07-02T23:59:59Z")
        right_a = _q("octo", "repo", "2026-07-02T00:00:00Z", "2026-07-02T11:59:59Z")
        right_b = _q("octo", "repo", "2026-07-02T12:00:00Z", "2026-07-02T23:59:59Z")
        seen: list[str] = []
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query(
                {
                    whole: [_search_payload(1500, [])],
                    left: [_search_payload(1, [_pr_node(1)])],
                    right: [_search_payload(1200, [])],
                    right_a: [_search_payload(1, [_pr_node(2)])],
                    right_b: [_search_payload(1, [_pr_node(3)])],
                },
                seen,
            )
        )

        stats: dict[str, int] = {}
        prs = await client.fetch_merged_prs(
            "octo", "repo", _dt("2026-07-01T00:00:00"), _dt("2026-07-03T00:00:00"), stats=stats
        )
        assert [pr["number"] for pr in prs] == [1, 2, 3]
        assert stats["bisections"] == 2
        # The left half fitted, so it was never split.
        assert seen.count(left) == 1

        await client.close()

    @respx.mock
    async def test_recursion_floor_raises(self, client: GitHubClient) -> None:
        """A one-second shard still above the cap cannot be split; it must raise."""
        query = _q("octo", "repo", "2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z")
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({query: [_search_payload(1500, [])]})
        )

        with pytest.raises(Truncated) as excinfo:
            await client.fetch_merged_prs("octo", "repo", _dt("2026-07-01T00:00:00"), _dt("2026-07-01T00:00:01"))
        assert "cannot be split" in str(excinfo.value)

        await client.close()

    @respx.mock
    async def test_not_found_fails_the_repository(self, client: GitHubClient) -> None:
        """A missing repository must not read as 'zero merged PRs'."""
        respx.post("https://api.github.com/graphql").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": {"search": None},
                    "errors": [{"type": "NOT_FOUND", "path": ["search"], "message": "nope"}],
                },
            )
        )

        with pytest.raises(RepositoryNotFoundError):
            await client.fetch_merged_prs("ghost", "repo", _dt("2026-08-01T00:00:00"), _dt("2026-08-02T00:00:00"))

        await client.close()

    async def test_naive_bounds_rejected(self, client: GitHubClient) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            await client.fetch_merged_prs("octo", "repo", datetime(2026, 8, 1), datetime(2026, 8, 2))
        await client.close()

    async def test_empty_window_rejected(self, client: GitHubClient) -> None:
        with pytest.raises(ValueError, match="empty merge window"):
            await client.fetch_merged_prs("octo", "repo", _dt("2026-08-02T00:00:00"), _dt("2026-08-01T00:00:00"))
        await client.close()

    @respx.mock
    async def test_rename_is_warned_about(self, client: GitHubClient, caplog: pytest.LogCaptureFixture) -> None:
        """GitHub follows a rename redirect silently (pymc3 -> pymc)."""
        query = _q("pymc-devs", "pymc3", "2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z")
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({query: [_search_payload(1, [_pr_node(1, name_with_owner="pymc-devs/pymc")])]})
        )

        with caplog.at_level("WARNING"):
            await client.fetch_merged_prs("pymc-devs", "pymc3", _dt("2026-08-01T00:00:00"), _dt("2026-08-02T00:00:00"))
        assert any("pymc-devs/pymc" in record.getMessage() for record in caplog.records)

        await client.close()


class TestFilesTruncationFallback:
    """``files(first: 100)`` truncates; the file-compliance guard must still fire."""

    @respx.mock
    async def test_no_fallback_when_complete(self, client: GitHubClient) -> None:
        query = _q("octo", "repo", "2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z")
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({query: [_search_payload(1, [_pr_node(1)])]})
        )
        rest = respx.get("https://api.github.com/repos/octo/repo/pulls/1/files").mock(
            return_value=httpx.Response(200, json=[])
        )

        stats: dict[str, int] = {}
        await client.fetch_merged_prs(
            "octo", "repo", _dt("2026-08-01T00:00:00"), _dt("2026-08-02T00:00:00"), stats=stats
        )
        assert rest.call_count == 0
        assert stats["files_fallbacks"] == 0

        await client.close()

    @respx.mock
    async def test_truncated_files_fall_back_to_rest_and_flip_the_verdict(self, client: GitHubClient) -> None:
        """The measured case: totalCount far above 100, and the PR must be rejected."""
        query = _q("octo", "repo", "2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z")
        truncated_nodes = [{"path": f"src/mod_{i}.py", "additions": 1, "deletions": 1} for i in range(100)]
        node = _pr_node(1, files=truncated_nodes, files_total=600)
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({query: [_search_payload(1, [node])]})
        )

        # 100 files must survive the truncated screen so the flip is real.
        assert check_file_compliance([{"filename": f["path"], "additions": 1, "deletions": 1} for f in truncated_nodes])

        def _files_page(request: httpx.Request) -> httpx.Response:
            page = int(request.url.params.get("page", "1"))
            per_page = int(request.url.params.get("per_page", "100"))
            start = (page - 1) * per_page
            if start >= 600:
                return httpx.Response(200, json=[])
            entries = [
                {"filename": f"src/mod_{i}.py", "additions": 1, "deletions": 1}
                for i in range(start, min(start + per_page, 600))
            ]
            return httpx.Response(200, json=entries)

        rest = respx.get("https://api.github.com/repos/octo/repo/pulls/1/files").mock(side_effect=_files_page)

        stats: dict[str, int] = {}
        prs = await client.fetch_merged_prs(
            "octo", "repo", _dt("2026-08-01T00:00:00"), _dt("2026-08-02T00:00:00"), stats=stats
        )
        pr = prs[0]
        # 6 full pages of 100, then a 7th that comes back empty and ends the loop.
        assert rest.call_count == 7
        assert stats["files_fallbacks"] == 1
        assert len(pr["file_changes"]) == 600
        assert pr["changed_files"] == 600
        assert pr["file_changes"][0]["filename"] == "src/mod_0.py"
        # 600 >= MAX_FILES_CHANGED, so the guard rejects — it could not on 100.
        assert not check_file_compliance(pr["file_changes"])

        await client.close()

    @respx.mock
    async def test_fallback_keeps_the_pr_when_rest_returns_nothing(self, client: GitHubClient) -> None:
        """A failed fallback degrades to the truncated list rather than dropping the PR."""
        query = _q("octo", "repo", "2026-08-01T00:00:00Z", "2026-08-01T23:59:59Z")
        node = _pr_node(1, files=[{"path": "a.py", "additions": 1, "deletions": 0}], files_total=3000)
        respx.post("https://api.github.com/graphql").mock(
            side_effect=_route_by_query({query: [_search_payload(1, [node])]})
        )
        respx.get("https://api.github.com/repos/octo/repo/pulls/1/files").mock(return_value=httpx.Response(404))

        stats: dict[str, int] = {}
        prs = await client.fetch_merged_prs(
            "octo", "repo", _dt("2026-08-01T00:00:00"), _dt("2026-08-02T00:00:00"), stats=stats
        )
        assert len(prs) == 1
        assert stats["files_fallbacks"] == 0
        assert prs[0]["changed_files"] == 3000

        await client.close()


class TestGetFilesPagination:
    """A single un-paginated page can never reach the 500-file guard."""

    @respx.mock
    async def test_paginates_until_short_page(self, client: GitHubClient) -> None:
        pages: list[int] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            page = int(request.url.params.get("page", "1"))
            pages.append(page)
            if page <= 2:
                return httpx.Response(
                    200,
                    json=[{"filename": f"f{page}_{i}.py", "additions": 1, "deletions": 0} for i in range(100)],
                )
            return httpx.Response(200, json=[{"filename": "last.py", "additions": 1, "deletions": 0}])

        respx.get("https://api.github.com/repos/o/r/pulls/1/files").mock(side_effect=_handler)

        files = await client.get_files("o", "r", 1)
        assert pages == [1, 2, 3]
        assert len(files) == 201

        await client.close()

    @respx.mock
    async def test_stops_at_max_pages(self, client: GitHubClient) -> None:
        route = respx.get("https://api.github.com/repos/o/r/pulls/1/files").mock(
            return_value=httpx.Response(
                200,
                json=[{"filename": f"f{i}.py", "additions": 1, "deletions": 0} for i in range(2)],
            )
        )

        files = await client.get_files("o", "r", 1, per_page=2, max_pages=3)
        assert route.call_count == 3
        assert len(files) == 6

        await client.close()


class TestFetchDiff:
    """A missing diff and a failed request must not be the same value."""

    @respx.mock
    async def test_ok_with_text(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/o/r/pulls/1").mock(
            return_value=httpx.Response(200, text="diff --git a/f.py b/f.py\n+x")
        )

        result = await client.fetch_diff("o", "r", 1)
        assert result.status is DiffStatus.OK
        assert result.ok
        assert not result.empty
        assert "diff --git" in result.text

        await client.close()

    @respx.mock
    async def test_ok_but_genuinely_empty(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/o/r/pulls/1").mock(return_value=httpx.Response(200, text=""))

        result = await client.fetch_diff("o", "r", 1)
        assert result.status is DiffStatus.OK
        assert result.empty
        assert result.status_code == 200

        await client.close()

    @respx.mock
    async def test_not_found(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/o/r/pulls/2").mock(return_value=httpx.Response(404))

        result = await client.fetch_diff("o", "r", 2)
        assert result.status is DiffStatus.NOT_FOUND
        assert not result.ok
        assert result.status_code == 404
        assert result.text == ""

        await client.close()

    @pytest.mark.parametrize("status_code", [406, 410, 451])
    @respx.mock
    async def test_unavailable_statuses(self, client: GitHubClient, status_code: int) -> None:
        respx.get("https://api.github.com/repos/o/r/pulls/3").mock(return_value=httpx.Response(status_code))

        result = await client.fetch_diff("o", "r", 3)
        assert result.status is DiffStatus.UNAVAILABLE
        assert result.status_code == status_code

        await client.close()

    @respx.mock
    async def test_get_diff_wrapper_still_returns_text(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/o/r/pulls/4").mock(return_value=httpx.Response(410))

        assert await client.get_diff("o", "r", 4) == ""

        await client.close()

    @respx.mock
    async def test_transport_failure_raises_rather_than_reporting_absent(self, client: GitHubClient) -> None:
        """An exhausted retry budget is a stage failure, not an empty diff."""
        respx.get("https://api.github.com/repos/o/r/pulls/5").mock(side_effect=httpx.ConnectError("boom"))

        with pytest.raises(httpx.ConnectError):
            await client.fetch_diff("o", "r", 5)

        await client.close()

    @respx.mock
    async def test_other_status_codes_still_raise(self, client: GitHubClient) -> None:
        respx.get("https://api.github.com/repos/o/r/pulls/6").mock(return_value=httpx.Response(500))

        with pytest.raises(httpx.HTTPStatusError):
            await client.fetch_diff("o", "r", 6)

        await client.close()
