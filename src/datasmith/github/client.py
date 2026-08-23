"""Async GitHub client wrapping httpx with token-pool rotation."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any

import httpx

from datasmith.github.models import PR, Issue, IssueExpanded
from datasmith.utils import TokenPool, get_logger

logger = get_logger("github.client")

# ---------------------------------------------------------------------------
# Tunable knobs.  Every one is env-overridable so an operator on a different
# machine, or against a repository with a different merge volume, edits
# ``tokens.env`` rather than this file.
# ---------------------------------------------------------------------------

# GitHub's search API silently truncates at 1000 results: ``hasNextPage``
# goes false with no error while ``issueCount`` keeps reporting the true
# total.  ``fetch_merged_prs`` therefore bisects the merge window instead of
# trusting pagination to terminate honestly.
DATASMITH_GH_SEARCH_CAP: int = int(os.environ.get("DATASMITH_GH_SEARCH_CAP", "1000"))
DATASMITH_GH_SEARCH_PAGE_SIZE: int = int(os.environ.get("DATASMITH_GH_SEARCH_PAGE_SIZE", "100"))
# 1000 results at 100 per page is 10 pages; the extra headroom only exists so
# a malformed ``pageInfo`` cannot spin forever.
DATASMITH_GH_SEARCH_MAX_PAGES: int = int(os.environ.get("DATASMITH_GH_SEARCH_MAX_PAGES", "20"))
# Recursion floor for the bisection.  ``merged:`` resolves to one second, so a
# shard this small cannot be split again and the fetcher must fail loudly
# rather than recurse forever.
DATASMITH_GH_MIN_SHARD_SECONDS: int = int(os.environ.get("DATASMITH_GH_MIN_SHARD_SECONDS", "1"))
# Ceiling on concurrent search POSTs from one client.  The bisection recurses
# with ``gather`` on both halves, so a repository needing N leaves would
# otherwise issue up to N simultaneous requests -- and stage 2 runs several
# repositories at once on top of that.  A PostHog-scale month is ~16 leaves,
# which against a one-token pool is a burst straight into GitHub's *secondary*
# rate limit.  The cap is held around the POST itself and never around the
# recursion: a parent that held a slot while awaiting its children would
# deadlock any tree deeper than the cap.
DATASMITH_GH_SEARCH_CONCURRENCY: int = int(os.environ.get("DATASMITH_GH_SEARCH_CONCURRENCY", "4"))

DATASMITH_GH_RETRIES: int = int(os.environ.get("DATASMITH_GH_RETRIES", "3"))
# Multiplier on the exponential backoff (base * 2**attempt).  Zero disables
# waiting entirely, which is what the offline tests use.
DATASMITH_GH_BACKOFF_BASE_S: float = float(os.environ.get("DATASMITH_GH_BACKOFF_BASE_S", "1.0"))
# Ceiling on an honoured ``Retry-After`` / ``X-RateLimit-Reset`` wait.  The
# REST core window is an hour, so the default lets a one-token pool wait out a
# full reset -- waiting is a correctness requirement, not an optimisation.
DATASMITH_GH_MAX_RETRY_WAIT_S: float = float(os.environ.get("DATASMITH_GH_MAX_RETRY_WAIT_S", "3600"))

# ``GET /pulls/{n}/files`` is paginated and defaults to 30 per page.  The file
# compliance guard rejects at 500 files, so a single unpaginated page can
# never make it fire; 30 pages of 100 covers GitHub's own 3000-file ceiling.
DATASMITH_GH_FILES_PER_PAGE: int = int(os.environ.get("DATASMITH_GH_FILES_PER_PAGE", "100"))
DATASMITH_GH_FILES_MAX_PAGES: int = int(os.environ.get("DATASMITH_GH_FILES_MAX_PAGES", "30"))
DATASMITH_GH_FILES_FALLBACK_CONCURRENCY: int = int(os.environ.get("DATASMITH_GH_FILES_FALLBACK_CONCURRENCY", "4"))

# Statuses on which ``_request`` reports "GitHub answered, definitively, that
# there is nothing here" rather than raising.
_MISSING_STATUSES: tuple[int, ...] = (404, 406, 410, 451)


class GitHubGraphQLError(RuntimeError):
    """A GraphQL response carried an error that cannot be retried away.

    GitHub answers GraphQL failures with HTTP 200 plus an ``errors`` array, so
    ``raise_for_status`` never sees them and a caller that reads only ``data``
    cannot tell a failure from an empty result.
    """

    def __init__(self, message: str, errors: Iterable[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.errors: list[dict[str, Any]] = list(errors or [])


class RepositoryNotFoundError(GitHubGraphQLError):
    """GitHub answered ``NOT_FOUND`` for a requested repository."""


class Truncated(RuntimeError):
    """A search leaf returned fewer nodes than ``issueCount`` promised.

    Raised per leaf, never on a sum across shards: GitHub's ``A..B`` is
    inclusive at both ends, and the fetcher keeps shards disjoint by
    subtracting one second from the exclusive upper bound.
    """


class DiffStatus(str, Enum):
    """Why a diff request produced the text it did."""

    OK = "ok"
    """GitHub served the diff.  The text may still be empty."""

    NOT_FOUND = "not_found"
    """404 -- the pull request is not visible under this name."""

    UNAVAILABLE = "unavailable"
    """406/410/451 -- GitHub has the PR but will not render its diff."""


@dataclass(frozen=True)
class DiffResult:
    """A diff plus the reason it looks the way it does.

    ``get_diff`` collapses every one of these onto ``""``, which is why the
    stored corpus cannot say whether its 550 empty patches are genuinely empty
    or masked failures.  Stage 3 needs them apart, so it reads this instead.
    """

    status: DiffStatus
    text: str
    status_code: int

    @property
    def ok(self) -> bool:
        """True when GitHub served a diff, empty or not."""
        return self.status is DiffStatus.OK

    @property
    def empty(self) -> bool:
        """True when GitHub served a diff and it had no content."""
        return self.ok and not self.text


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff for retry *attempt* (0-indexed)."""
    return DATASMITH_GH_BACKOFF_BASE_S * (2.0**attempt)


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header into seconds from now.

    The header is either delta-seconds or an HTTP-date; both forms are in use.
    Returns ``None`` when the header is absent or unparseable.
    """
    if not value:
        return None
    stripped = value.strip()
    try:
        return max(0.0, float(stripped))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(stripped)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, when.timestamp() - time.time())


def _rate_limit_wait(response: httpx.Response, attempt: int) -> tuple[float, float | None, str]:
    """Decide how long to wait after a 403/429.

    Returns ``(seconds to sleep, absolute reset epoch or None, which header
    decided)``.  ``Retry-After`` is checked first because GitHub's *secondary*
    rate limit sends it and frequently omits ``X-RateLimit-Reset`` -- reading
    only the reset header burns the retry budget in a few fast attempts and
    then raises.  With a one-token pool that is normal operation, not an
    exceptional path.

    The reset epoch is absolute because :meth:`TokenPool.report_rate_limit`
    compares it against ``time.time()``; handing it a delta marks the token
    immediately available and produces a retry storm.
    """
    now = time.time()
    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
    if retry_after is not None:
        return min(retry_after, DATASMITH_GH_MAX_RETRY_WAIT_S), now + retry_after, "Retry-After"

    reset_raw = response.headers.get("X-RateLimit-Reset")
    if reset_raw:
        try:
            reset_at = float(reset_raw)
        except ValueError:
            reset_at = 0.0
        if reset_at > 0:
            wait = min(max(reset_at - now, 0.0), DATASMITH_GH_MAX_RETRY_WAIT_S)
            return wait, reset_at, "X-RateLimit-Reset"

    return min(_backoff_delay(attempt), DATASMITH_GH_MAX_RETRY_WAIT_S), None, "backoff"


def _iso_z(value: datetime) -> str:
    """Format *value* as the UTC ISO-8601 stamp GitHub's ``merged:`` accepts."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _merged_search_query(owner: str, repo: str, lo: datetime, hi: datetime) -> str:
    """Build the search query for PRs merged in the half-open window ``[lo, hi)``.

    GitHub's ``A..B`` range is inclusive at *both* ends, so the exclusive upper
    bound loses one second.  That subtraction is the whole reason adjacent
    shards stay disjoint.
    """
    return f"repo:{owner}/{repo} is:pr is:merged merged:{_iso_z(lo)}..{_iso_z(hi - timedelta(seconds=1))}"


def _error_path(error: dict[str, Any]) -> str:
    """Render a GraphQL error's ``path`` for logging."""
    path = error.get("path")
    if isinstance(path, list) and path:
        return "/".join(str(part) for part in path)
    return "<response>"


def _error_type(error: dict[str, Any]) -> str:
    return str(error.get("type") or "").upper()


def _has_data(result: dict[str, Any]) -> bool:
    """True when the response carries at least one non-null top-level field."""
    data = result.get("data")
    return isinstance(data, dict) and any(value is not None for value in data.values())


def _files_truncated(node: dict[str, Any]) -> bool:
    """True when a PR's GraphQL ``files`` connection returned fewer nodes than exist."""
    files = node.get("files") or {}
    file_nodes = files.get("nodes") or []
    try:
        total = int(files.get("totalCount", len(file_nodes)))
    except (TypeError, ValueError):
        return False
    return total > len(file_nodes)


def _normalise_pr_node(node: dict[str, Any]) -> dict[str, Any]:
    """Reshape a GraphQL PullRequest node into the REST field names stage 2 expects.

    The GraphQL file connection names the path ``path``; every consumer in this
    project reads ``filename``.
    """
    files = node.get("files") or {}
    file_nodes = files.get("nodes") or []
    file_changes = [
        {
            "filename": entry.get("path", "") or "",
            "additions": int(entry.get("additions", 0) or 0),
            "deletions": int(entry.get("deletions", 0) or 0),
        }
        for entry in file_nodes
    ]
    try:
        changed_files = int(files.get("totalCount", len(file_changes)) or 0)
    except (TypeError, ValueError):
        changed_files = len(file_changes)

    return {
        "number": node["number"],
        "title": node.get("title", "") or "",
        "body": node.get("body", "") or "",
        "state": "closed",
        "created_at": node.get("createdAt"),
        "merged_at": node.get("mergedAt"),
        "closed_at": node.get("closedAt"),
        "merge_commit_sha": (node.get("mergeCommit") or {}).get("oid", "") or "",
        "base": {"sha": node.get("baseRefOid", "") or ""},
        "head": {"sha": node.get("headRefOid", "") or ""},
        "labels": [{"name": label["name"]} for label in ((node.get("labels") or {}).get("nodes") or [])],
        "file_changes": file_changes,
        "changed_files": changed_files,
    }


def _warn_on_rename(owner: str, repo: str, nodes: list[dict[str, Any]]) -> None:
    """Warn when GitHub answered for a repository other than the one requested.

    GitHub follows a rename redirect silently, so a request for
    ``pymc-devs/pymc3`` is answered by ``pymc-devs/pymc`` and the rows land
    under the stale name with nothing in the log to say so.
    """
    requested = f"{owner}/{repo}".lower()
    seen = {(node.get("repository") or {}).get("nameWithOwner", "") for node in nodes}
    for name in sorted(value for value in seen if value and value.lower() != requested):
        logger.warning(
            "Requested %s/%s but GitHub answered for %s: the repository was probably renamed, "
            "and rows will be stored under the stale name",
            owner,
            repo,
            name,
        )


class GitHubClient:
    """Async GitHub REST + GraphQL client with automatic token rotation."""

    def __init__(self, token_pool: TokenPool) -> None:
        self._pool = token_pool
        self._http: httpx.AsyncClient | None = None
        self._last_token: str | None = None
        # Bounds the search POSTs the bisection fans out.  Held around the
        # request, never around the recursion -- see
        # ``DATASMITH_GH_SEARCH_CONCURRENCY``.
        self._search_sem = asyncio.Semaphore(max(1, DATASMITH_GH_SEARCH_CONCURRENCY))

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url="https://api.github.com",
                timeout=30.0,
                follow_redirects=True,
                headers={"Accept": "application/vnd.github.v3+json"},
                limits=httpx.Limits(
                    max_connections=200,
                    max_keepalive_connections=40,
                ),
            )
        return self._http

    def _auth_headers(self) -> dict[str, str]:
        token = self._pool.get_token()
        self._last_token = token
        return {"Authorization": f"Bearer {token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        _retries: int | None = None,
        soft_statuses: tuple[int, ...] = (),
        **kwargs: Any,
    ) -> httpx.Response | None:
        """Issue a request with retry, token rotation and rate-limit pacing.

        Statuses in :data:`_MISSING_STATUSES` normally collapse to ``None``.
        A caller that needs to tell 404 from 410 opts in with *soft_statuses*
        and gets the response back instead; the default keeps every existing
        caller's behaviour unchanged.
        """
        retries = DATASMITH_GH_RETRIES if _retries is None else _retries
        client = await self._client()
        extra_headers = kwargs.pop("headers", {})
        headers = {**self._auth_headers(), **extra_headers}

        last_exc: Exception | None = None
        resp: httpx.Response | None = None
        for attempt in range(retries):
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
                    retries,
                    exc,
                )
                await asyncio.sleep(_backoff_delay(attempt))
                headers = {**self._auth_headers(), **extra_headers}
                continue

            if resp.status_code in _MISSING_STATUSES:
                return resp if resp.status_code in soft_statuses else None
            if resp.status_code in (429, 403):
                # This attempt's outcome supersedes any earlier transport
                # error.  Without the reset, a connect timeout on attempt 0
                # followed by 403s for the rest of the budget would be reported
                # as a timeout -- hiding a rate limit in exactly the one-token
                # run that crosses a reset by design.
                last_exc = None
                delay, reset_at, source = _rate_limit_wait(resp, attempt)
                token = headers["Authorization"].removeprefix("Bearer ")
                if reset_at is not None:
                    self._pool.report_rate_limit(token, remaining=0, reset_at=reset_at)
                logger.warning(
                    "Rate limited (%d) on %s %s (attempt %d/%d); waiting %.1fs per %s",
                    resp.status_code,
                    method,
                    path,
                    attempt + 1,
                    retries,
                    delay,
                    source,
                )
                # Sleep *before* asking for the next token.  ``get_token`` waits
                # with a blocking ``time.sleep`` when every token is marked
                # exhausted, which stalls the whole event loop; sleeping first
                # means the reset has passed by the time we ask.
                await asyncio.sleep(delay)
                headers = {**self._auth_headers(), **extra_headers}
                continue
            resp.raise_for_status()
            return resp

        # All retries exhausted
        if last_exc is not None:
            raise last_exc
        if resp is not None:
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

    async def fetch_diff(self, owner: str, repo: str, number: int) -> DiffResult:
        """Fetch a PR diff, keeping "GitHub has none" apart from "the request failed".

        A transport failure or an exhausted retry budget raises, which
        ``BaseRunner`` records as a ``runner_failures`` row.  Everything GitHub
        answers definitively comes back as a :class:`DiffStatus`, so an empty
        ``text`` with ``status is OK`` means the diff really is empty.
        """
        resp = await self._request(
            "GET",
            f"/repos/{owner}/{repo}/pulls/{number}",
            headers={"Accept": "application/vnd.github.v3.diff"},
            soft_statuses=_MISSING_STATUSES,
        )
        if resp is None:  # pragma: no cover - soft_statuses covers every None path
            return DiffResult(status=DiffStatus.UNAVAILABLE, text="", status_code=0)
        if resp.status_code == 404:
            return DiffResult(status=DiffStatus.NOT_FOUND, text="", status_code=404)
        if resp.status_code in _MISSING_STATUSES:
            return DiffResult(status=DiffStatus.UNAVAILABLE, text="", status_code=resp.status_code)
        return DiffResult(status=DiffStatus.OK, text=resp.text, status_code=resp.status_code)

    async def get_diff(self, owner: str, repo: str, number: int) -> str:
        """Fetch the diff for a pull request as plain text.

        Kept for callers that only want the text.  It cannot distinguish an
        absent diff from an unavailable one -- use :meth:`fetch_diff` where
        that difference matters.
        """
        return (await self.fetch_diff(owner, repo, number)).text

    async def get_files(
        self,
        owner: str,
        repo: str,
        number: int,
        *,
        per_page: int | None = None,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch every file changed in a pull request.

        Paginated deliberately: the endpoint defaults to 30 files per page, and
        ``check_file_compliance`` rejects at 500 files, so a single page can
        never make that guard fire.
        """
        size = DATASMITH_GH_FILES_PER_PAGE if per_page is None else per_page
        pages = DATASMITH_GH_FILES_MAX_PAGES if max_pages is None else max_pages

        out: list[dict[str, Any]] = []
        for page_num in range(1, pages + 1):
            resp = await self._request(
                "GET",
                f"/repos/{owner}/{repo}/pulls/{number}/files",
                params={"per_page": size, "page": page_num},
            )
            if resp is None:
                break
            data = resp.json()
            if not isinstance(data, list) or not data:
                break
            out.extend(data)
            if len(data) < size:
                break
        return out

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

    async def graphql(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        *,
        retries: int | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query, honouring the ``errors`` array GitHub sends with HTTP 200.

        Routed through :meth:`_request` so it inherits retry, token rotation
        and rate-limit pacing.  Errors are classified per ``path``:

        * ``RATE_LIMITED`` -- reported to the token pool and retried with backoff.
        * ``NOT_FOUND`` -- fatal for the named path; raises
          :class:`RepositoryNotFoundError` instead of looking like an empty result.
        * anything else -- logged per path.  The response is still returned when
          it carries data, so one scoped error does not discard good results.
        """
        attempts = DATASMITH_GH_RETRIES if retries is None else retries
        payload: dict[str, Any] = {"query": query}
        if variables is not None:
            payload["variables"] = variables

        last_errors: list[dict[str, Any]] = []
        for attempt in range(max(1, attempts)):
            resp = await self._request("POST", "/graphql", json=payload)
            if resp is None:
                raise GitHubGraphQLError("GraphQL endpoint answered with an unavailable status")
            result: dict[str, Any] = resp.json()

            errors = [error for error in (result.get("errors") or []) if isinstance(error, dict)]
            if not errors:
                return result
            last_errors = errors

            for error in errors:
                logger.warning(
                    "GraphQL error at %s: %s [%s]",
                    _error_path(error),
                    error.get("message", ""),
                    _error_type(error) or "-",
                )

            if any(_error_type(error) == "RATE_LIMITED" for error in errors):
                # The rate-limit headers ride along on the 200, so reuse the
                # same decision ``_request`` makes for a 403.  A bare 1-2-4s
                # backoff would burn every attempt inside seven seconds while
                # the real GraphQL window runs to an hour, making this branch
                # incapable of ever succeeding.
                delay, reset_at, source = _rate_limit_wait(resp, attempt)
                if self._last_token is not None:
                    self._pool.report_rate_limit(
                        self._last_token,
                        remaining=0,
                        reset_at=reset_at if reset_at is not None else time.time() + delay,
                    )
                logger.warning(
                    "GraphQL rate limited (attempt %d/%d); waiting %.1fs per %s",
                    attempt + 1,
                    max(1, attempts),
                    delay,
                    source,
                )
                await asyncio.sleep(delay)
                continue

            # A scoped error alongside usable data is not a failed response --
            # including a scoped NOT_FOUND.  This guard is deliberately *above*
            # the NOT_FOUND raise: GitHub answers a multi-path query with the
            # paths it could resolve plus one error per path it could not, so
            # raising first would throw away good data because some unrelated
            # field was missing (spec section 5).
            #
            # Nothing is swallowed by returning here.  Each error was already
            # logged with its path above, and the consumers read their own
            # path: ``_search_block`` raises when ``data.search`` is not a
            # dict, so a NOT_FOUND on the path the caller actually asked about
            # still fails loudly rather than reading as a healthy zero.
            if _has_data(result):
                return result

            not_found = [error for error in errors if _error_type(error) == "NOT_FOUND"]
            if not_found:
                paths = ", ".join(_error_path(error) for error in not_found)
                raise RepositoryNotFoundError(
                    f"GraphQL NOT_FOUND at {paths}: {not_found[0].get('message', '')}",
                    errors,
                )

            raise GitHubGraphQLError(
                f"GraphQL response carried only errors: {errors[0].get('message', '')}",
                errors,
            )

        raise GitHubGraphQLError(
            f"GraphQL still rate limited after {max(1, attempts)} attempts",
            last_errors,
        )

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
        """Yield pages of merged PRs using GraphQL (only merged PRs returned).

        Deprecated: orders by ``CREATED_AT`` and so cannot express a
        ``merged_at`` window.  Use :meth:`fetch_merged_prs`; this stays only
        until stage 2 is migrated off it.
        """
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

    _SEARCH_MERGED_PRS_QUERY = """
    query($q: String!, $cursor: String, $pageSize: Int!) {
      search(query: $q, type: ISSUE, first: $pageSize, after: $cursor) {
        issueCount
        pageInfo { hasNextPage endCursor }
        nodes {
          ... on PullRequest {
            number
            title
            body
            createdAt
            mergedAt
            closedAt
            mergeCommit { oid }
            baseRefOid
            headRefOid
            repository { nameWithOwner }
            labels(first: 20) { nodes { name } }
            files(first: 100) { totalCount nodes { path additions deletions } }
          }
        }
      }
    }
    """

    async def fetch_merged_prs(
        self,
        owner: str,
        repo: str,
        since: datetime,
        until: datetime,
        *,
        stats: dict[str, int] | None = None,
    ) -> list[dict[str, Any]]:
        """Return every PR of ``owner/repo`` merged in the half-open window ``[since, until)``.

        Asks GitHub the question stage 2 actually means.  ``IssueOrderField``
        has no ``MERGED_AT``, so the repository connection cannot express this
        and the search API is used instead.

        Raises :class:`Truncated` when a leaf disagrees with its own
        ``issueCount``, and :class:`RepositoryNotFoundError` when the
        repository does not resolve -- both surface as a stage failure for that
        repository rather than as an empty result.
        """
        if since.tzinfo is None or until.tzinfo is None:
            raise ValueError(
                "fetch_merged_prs needs timezone-aware bounds; a naive datetime silently shifts the merge window"
            )
        if since.microsecond or until.microsecond:
            # ``merged:`` resolves to one second and ``_iso_z`` formats whole
            # seconds, so a sub-second bound would be truncated into a shard
            # that is short of the requested window -- and the per-leaf
            # ``issueCount`` check would still pass, because it describes the
            # window that was queried rather than the one that was asked for.
            raise ValueError(
                "fetch_merged_prs needs whole-second bounds; GitHub's merged: qualifier has one-second resolution "
                "and a sub-second bound would silently shorten the window"
            )
        if until <= since:
            raise ValueError(f"empty merge window: [{since.isoformat()}, {until.isoformat()})")

        counters = stats if stats is not None else {}
        for key in ("queries", "bisections", "files_fallbacks"):
            counters.setdefault(key, 0)

        nodes = await self._search_merged_range(
            owner,
            repo,
            since.astimezone(UTC),
            until.astimezone(UTC),
            counters,
        )
        await self._resolve_truncated_files(owner, repo, nodes, counters)
        _warn_on_rename(owner, repo, nodes)
        return [_normalise_pr_node(node) for node in nodes]

    async def _search_merged_range(
        self,
        owner: str,
        repo: str,
        lo: datetime,
        hi: datetime,
        counters: dict[str, int],
    ) -> list[dict[str, Any]]:
        """Fetch one shard, bisecting when the search cap would truncate it."""
        query = _merged_search_query(owner, repo, lo, hi)
        variables: dict[str, Any] = {"q": query, "cursor": None, "pageSize": DATASMITH_GH_SEARCH_PAGE_SIZE}
        result = await self._search_once(variables)
        counters["queries"] = counters.get("queries", 0) + 1
        search = self._search_block(result, owner, repo)
        try:
            total = int(search.get("issueCount", 0) or 0)
        except (TypeError, ValueError) as exc:
            raise GitHubGraphQLError(f"{owner}/{repo}: search returned a non-numeric issueCount") from exc

        if total > DATASMITH_GH_SEARCH_CAP:
            span = hi - lo
            mid = lo + timedelta(seconds=int(span.total_seconds() // 2))
            if span <= timedelta(seconds=DATASMITH_GH_MIN_SHARD_SECONDS) or mid <= lo or mid >= hi:
                raise Truncated(
                    f"{owner}/{repo} [{_iso_z(lo)}, {_iso_z(hi)}): {total} PRs merged in "
                    f"{span.total_seconds():.0f}s, above the {DATASMITH_GH_SEARCH_CAP} search cap; "
                    "the window cannot be split any further"
                )
            counters["bisections"] = counters.get("bisections", 0) + 1
            logger.info(
                "%s/%s [%s, %s): %d PRs exceeds the %d search cap; splitting at %s",
                owner,
                repo,
                _iso_z(lo),
                _iso_z(hi),
                total,
                DATASMITH_GH_SEARCH_CAP,
                _iso_z(mid),
            )
            left, right = await asyncio.gather(
                self._search_merged_range(owner, repo, lo, mid, counters),
                self._search_merged_range(owner, repo, mid, hi, counters),
            )
            return left + right

        nodes: list[dict[str, Any]] = [node for node in (search.get("nodes") or []) if node]
        page_info: dict[str, Any] = search.get("pageInfo") or {}
        pages = 1
        while page_info.get("hasNextPage"):
            if pages >= DATASMITH_GH_SEARCH_MAX_PAGES:
                raise Truncated(
                    f"{owner}/{repo} [{_iso_z(lo)}, {_iso_z(hi)}): still paginating after "
                    f"{pages} pages for {total} results"
                )
            variables = {
                "q": query,
                "cursor": page_info.get("endCursor"),
                "pageSize": DATASMITH_GH_SEARCH_PAGE_SIZE,
            }
            result = await self._search_once(variables)
            counters["queries"] = counters.get("queries", 0) + 1
            pages += 1
            search = self._search_block(result, owner, repo)
            nodes.extend(node for node in (search.get("nodes") or []) if node)
            page_info = search.get("pageInfo") or {}

        # The assertion that turns a silent truncation into a loud failure.
        # Per leaf only: shards are disjoint but a sum across them would hide
        # a short leaf behind a long one.
        if len(nodes) != total:
            raise Truncated(f"{owner}/{repo} [{_iso_z(lo)}, {_iso_z(hi)}): got {len(nodes)} of {total} PRs")
        return nodes

    async def _search_once(self, variables: dict[str, Any]) -> dict[str, Any]:
        """Issue one search POST under the fan-out cap.

        The slot covers the request alone.  A slot held across the recursive
        ``gather`` would have every parent waiting on children that cannot
        acquire, so any bisection tree deeper than the cap would deadlock.
        """
        async with self._search_sem:
            return await self.graphql(self._SEARCH_MERGED_PRS_QUERY, variables)

    @staticmethod
    def _search_block(result: dict[str, Any], owner: str, repo: str) -> dict[str, Any]:
        data = result.get("data") or {}
        search = data.get("search")
        if not isinstance(search, dict):
            raise GitHubGraphQLError(
                f"{owner}/{repo}: GraphQL response carried no search block",
                result.get("errors") or [],
            )
        return search

    async def _resolve_truncated_files(
        self,
        owner: str,
        repo: str,
        nodes: list[dict[str, Any]],
        counters: dict[str, int],
    ) -> None:
        """Re-fetch file lists over REST for PRs whose GraphQL ``files`` page truncated.

        ``files(first: 100)`` caps the node list at 100 while ``totalCount``
        reports the truth, so ``check_file_compliance``'s 500-file guard can
        never fire and its 40 000-line guard undercounts.  Roughly 1% of PRs
        are affected, and at least one of them flips its verdict.
        """
        truncated = [node for node in nodes if _files_truncated(node)]
        if not truncated:
            return

        semaphore = asyncio.Semaphore(max(1, DATASMITH_GH_FILES_FALLBACK_CONCURRENCY))
        filled = 0

        async def _fill(node: dict[str, Any]) -> None:
            nonlocal filled
            number = int(node["number"])
            async with semaphore:
                rest_files = await self.get_files(owner, repo, number)
            if not rest_files:
                logger.warning(
                    "%s/%s#%d: files truncated at %d nodes and the REST fallback returned nothing",
                    owner,
                    repo,
                    number,
                    len((node.get("files") or {}).get("nodes") or []),
                )
                return
            existing = node.get("files") or {}
            node["files"] = {
                "totalCount": existing.get("totalCount", len(rest_files)),
                "nodes": [
                    {
                        "path": entry.get("filename", "") or "",
                        "additions": entry.get("additions", 0),
                        "deletions": entry.get("deletions", 0),
                    }
                    for entry in rest_files
                ],
            }
            counters["files_fallbacks"] = counters.get("files_fallbacks", 0) + 1
            filled += 1

        await asyncio.gather(*(_fill(node) for node in truncated))
        logger.info(
            "%s/%s: refetched file lists over REST for %d of %d PRs truncated by files(first: 100)",
            owner,
            repo,
            filled,
            len(truncated),
        )

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http and not self._http.is_closed:
            await self._http.aclose()
