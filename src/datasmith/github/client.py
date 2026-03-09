"""Async GitHub client wrapping httpx with token-pool rotation."""

from __future__ import annotations

from typing import Any

import httpx

from datasmith.github.models import PR, Issue
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
            )
        return self._http

    def _auth_headers(self) -> dict[str, str]:
        token = self._pool.get_token()
        return {"Authorization": f"Bearer {token}"}

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response | None:
        client = await self._client()
        extra_headers = kwargs.pop("headers", {})
        headers = {**self._auth_headers(), **extra_headers}
        resp = await client.request(method, path, headers=headers, **kwargs)
        if resp.status_code in (404, 410, 451):
            return None
        if resp.status_code in (429, 403):
            reset = resp.headers.get("X-RateLimit-Reset")
            if reset:
                token = headers["Authorization"].replace("Bearer ", "")
                self._pool.report_rate_limit(token, remaining=0, reset_at=float(reset))
            # Retry once with a different token
            headers = {**self._auth_headers(), **extra_headers}
            resp = await client.request(method, path, headers=headers, **kwargs)
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

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http and not self._http.is_closed:
            await self._http.aclose()
