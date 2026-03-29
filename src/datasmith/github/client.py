"""Async GitHub client wrapping httpx with token-pool rotation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx

from datasmith.github.models import PR, Issue, IssueExpanded
from datasmith.utils import TokenPool, get_logger

logger = get_logger("github.client")


class GitHubClient:
    """Async GitHub REST + GraphQL client with automatic token rotation."""

    def __init__(self, token_pool: TokenPool) -> None:
        self._pool = token_pool
        self._http: httpx.AsyncClient | None = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url="https://api.github.com",
                timeout=30.0,
                headers={"Accept": "application/vnd.github.v3+json"},
                limits=httpx.Limits(
                    max_connections=200,
                    max_keepalive_connections=40,
                ),
            )
        return self._http

    def _auth_headers(self) -> dict[str, str]:
        token = self._pool.get_token()
        return {"Authorization": f"Bearer {token}"}

    async def _request(self, method: str, path: str, *, _retries: int = 3, **kwargs: Any) -> httpx.Response | None:
        client = await self._client()
        extra_headers = kwargs.pop("headers", {})
        headers = {**self._auth_headers(), **extra_headers}

        last_exc: Exception | None = None
        for attempt in range(_retries):
            try:
                resp = await client.request(method, path, headers=headers, **kwargs)
            except (
                httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.ConnectError,
                httpx.RemoteProtocolError,
            ) as exc:
                last_exc = exc
                logger.warning(
                    "Transient error on %s %s (attempt %d/%d): %s",
                    method,
                    path,
                    attempt + 1,
                    _retries,
                    exc,
                )
                await asyncio.sleep(2**attempt)
                headers = {**self._auth_headers(), **extra_headers}
                continue

            if resp.status_code in (404, 406, 410, 451):
                return None
            if resp.status_code in (429, 403):
                reset = resp.headers.get("X-RateLimit-Reset")
                if reset:
                    token = headers["Authorization"].replace("Bearer ", "")
                    self._pool.report_rate_limit(token, remaining=0, reset_at=float(reset))
                # Retry with a different token
                headers = {**self._auth_headers(), **extra_headers}
                await asyncio.sleep(2**attempt)
                continue
            resp.raise_for_status()
            return resp

        # All retries exhausted
        if last_exc is not None:
            raise last_exc
        # Last response was a rate-limit — raise it
        resp.raise_for_status()
        return resp

    async def get_pr(self, owner: str, repo: str, number: int) -> PR | None:
        """Fetch a pull request by owner/repo/number."""
        resp = await self._request("GET", f"/repos/{owner}/{repo}/pulls/{number}")
        if resp is None:
            return None
        data = resp.json()
        return PR(
            repository=f"{owner}/{repo}",
            issue_number=number,
            title=data.get("title", ""),
            body=data.get("body", "") or "",
            state=data.get("state", ""),
            created_at=data.get("created_at"),
            closed_at=data.get("closed_at"),
            merged_at=data.get("merged_at"),
            merge_commit_sha=data.get("merge_commit_sha", "") or "",
            base_sha=data.get("base", {}).get("sha", ""),
            head_sha=data.get("head", {}).get("sha", ""),
            labels=[label["name"] for label in data.get("labels", [])],
        )

    async def get_issue(self, owner: str, repo: str, number: int) -> Issue | None:
        """Fetch an issue by owner/repo/number."""
        resp = await self._request("GET", f"/repos/{owner}/{repo}/issues/{number}")
        if resp is None:
            return None
        data = resp.json()
        return Issue(
            repository=f"{owner}/{repo}",
            issue_number=number,
            title=data.get("title", ""),
            body=data.get("body", "") or "",
            created_at=data.get("created_at"),
            closed_at=data.get("closed_at"),
        )

    async def get_issue_expanded(self, owner: str, repo: str, number: int) -> IssueExpanded | None:
        """Fetch an issue with timeline comments and cross-references.

        Combines ``get_issue`` + ``get_timeline`` into a single
        ``IssueExpanded`` suitable for ``scrape_links`` and rendering.
        """
        issue = await self.get_issue(owner, repo, number)
        if issue is None:
            return None

        timeline = await self.get_timeline(owner, repo, number)

        comments: list[str] = []
        cross_references: list[str] = []
        for event in timeline:
            evt_type = event.get("event", "")
            if evt_type == "commented":
                body = event.get("body", "")
                if body:
                    comments.append(body)
            elif evt_type == "cross-referenced":
                source_body = event.get("source", {}).get("issue", {}).get("body", "")
                if source_body:
                    cross_references.append(source_body)

        return IssueExpanded(
            number=issue.issue_number,
            title=issue.title,
            url=f"https://github.com/{owner}/{repo}/issues/{number}",
            description=issue.body,
            comments=comments,
            created_at=issue.created_at,
            closed_at=issue.closed_at,
            cross_references=cross_references,
        )

    async def get_timeline(self, owner: str, repo: str, number: int) -> list[dict[str, Any]]:
        """Fetch the timeline for an issue or PR."""
        resp = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/issues/{number}/timeline",
            headers={"Accept": "application/vnd.github.mockingbird-preview+json"},
        )
        if resp is None:
            return []
        return resp.json()  # type: ignore[no-any-return]

    async def get_diff(self, owner: str, repo: str, number: int) -> str:
        """Fetch the diff for a pull request."""
        resp = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/pulls/{number}",
            headers={"Accept": "application/vnd.github.v3.diff"},
        )
        if resp is None:
            return ""
        return resp.text

    async def get_files(self, owner: str, repo: str, number: int) -> list[dict[str, Any]]:
        """Fetch the list of files changed in a pull request."""
        resp = await self._request("GET", f"/repos/{owner}/{repo}/pulls/{number}/files")
        if resp is None:
            return []
        return resp.json()  # type: ignore[no-any-return]

    async def search_code(
        self,
        query: str,
        *,
        per_page: int = 100,
        max_pages: int = 10,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield items from the GitHub Code Search API.

        The search API returns ``{"items": [...], "incomplete_results": bool}``
        rather than a plain list, so ``paginate()`` cannot be reused.  A 2-second
        delay is added between pages to stay within the 30 req/min search rate limit.
        """
        for page_num in range(1, max_pages + 1):
            resp = await self._request(
                "GET",
                "/search/code",
                params={"q": query, "per_page": per_page, "page": page_num},
            )
            if resp is None:
                return
            data = resp.json()
            items = data.get("items", [])
            if not items:
                return
            for item in items:
                yield item
            if len(items) < per_page:
                return
            # Search API has a stricter rate limit (30 req/min)
            await asyncio.sleep(2)

    async def paginate(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        per_page: int = 100,
        max_pages: int = 250,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Yield pages from a GitHub list endpoint."""
        base_params = dict(params or {})
        base_params["per_page"] = per_page
        for page_num in range(1, max_pages + 1):
            resp = await self._request(method, path, params={**base_params, "page": page_num})
            if resp is None:
                return
            data = resp.json()
            if not isinstance(data, list) or not data:
                return
            yield data
            if len(data) < per_page:
                return

    async def graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query against the GitHub API."""
        client = await self._client()
        headers = self._auth_headers()
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        resp = await client.post("https://api.github.com/graphql", json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    _MERGED_PRS_QUERY = """
    query($owner: String!, $repo: String!, $cursor: String) {
      repository(owner: $owner, name: $repo) {
        pullRequests(states: MERGED, first: 100, after: $cursor,
                     orderBy: {field: CREATED_AT, direction: DESC}) {
          nodes {
            number
            title
            body
            state
            createdAt
            mergedAt
            closedAt
            mergeCommit { oid }
            baseRefOid
            headRefOid
            labels(first: 20) { nodes { name } }
          }
          pageInfo { hasNextPage endCursor }
        }
      }
    }
    """

    async def paginate_merged_prs(
        self,
        owner: str,
        repo: str,
        *,
        max_pages: int = 250,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Yield pages of merged PRs using GraphQL (only merged PRs returned)."""
        cursor: str | None = None
        for _ in range(max_pages):
            variables: dict[str, Any] = {"owner": owner, "repo": repo}
            if cursor:
                variables["cursor"] = cursor

            result = await self.graphql(self._MERGED_PRS_QUERY, variables)

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                return
            pr_connection = repo_data.get("pullRequests", {})
            nodes = pr_connection.get("nodes", [])
            if not nodes:
                return

            # Normalize GraphQL shape to match REST field names
            page: list[dict[str, Any]] = []
            for node in nodes:
                pr: dict[str, Any] = {
                    "number": node["number"],
                    "title": node.get("title", ""),
                    "body": node.get("body", "") or "",
                    "state": "closed",
                    "created_at": node.get("createdAt"),
                    "merged_at": node.get("mergedAt"),
                    "closed_at": node.get("closedAt"),
                    "merge_commit_sha": (node.get("mergeCommit") or {}).get("oid", ""),
                    "base": {"sha": node.get("baseRefOid", "")},
                    "head": {"sha": node.get("headRefOid", "")},
                    "labels": [{"name": ln["name"]} for ln in (node.get("labels") or {}).get("nodes", [])],
                }
                page.append(pr)

            yield page

            page_info = pr_connection.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                return
            cursor = page_info.get("endCursor")

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http and not self._http.is_closed:
            await self._http.aclose()
