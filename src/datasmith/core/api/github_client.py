"""GitHub API helpers with caching and retry semantics."""

from __future__ import annotations

import random
import time
import typing
from typing import Any, cast

import requests
from requests import HTTPError

from datasmith import logger
from datasmith.core.api.http_utils import build_headers, prepare_url, request_with_backoff
from datasmith.core.cache import CACHE_LOCATION, cache_completion


def _post_with_backoff(
    url: str,
    *,
    payload: dict[str, Any],
    session: requests.Session | None = None,
    rps: int = 2,
    base_delay: float = 1.0,
    max_retries: int = 5,
    max_backoff: float = 60.0,
) -> requests.Response:
    session = session or requests.Session()
    delay = base_delay
    last_exc: requests.RequestException | None = None

    for _ in range(1, max_retries + 1):
        time.sleep(max(0.0, 1 / rps))

        try:
            resp = session.post(
                url,
                headers=build_headers("github"),
                json=payload,
                timeout=15,
            )

            if resp.status_code in (403, 429):
                remaining = resp.headers.get("X-RateLimit-Remaining", "1")
                reset_at = resp.headers.get("X-RateLimit-Reset")
                if remaining == "0" and reset_at:
                    sleep_for = max(0.0, float(reset_at) - time.time())
                else:
                    sleep_for = min(delay, max_backoff)
                resp.close()
                time.sleep(sleep_for + random.uniform(0, 1))  # noqa: S311
                delay *= 2
                continue

            resp.raise_for_status()
            return resp  # noqa: TRY300

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
            last_exc = exc
            time.sleep(min(delay, max_backoff) + random.uniform(0, 1))  # noqa: S311
            delay *= 2

    raise last_exc or RuntimeError("Unknown error calling GitHub GraphQL API")


@cache_completion(CACHE_LOCATION, "github_metadata")
def get_github_metadata(endpoint: str, params: dict[str, str] | None = None) -> dict[str, Any] | None:
    """Call the GitHub REST API for *endpoint* and return parsed JSON."""
    if not endpoint:
        return None
    endpoint = endpoint.lstrip("/")
    header_kwargs = {"diff_api": True} if params and params.get("diff_api", "false").lower() == "true" else {}
    if params and "diff_api" in params:
        params.pop("diff_api")

    api_url = prepare_url(f"https://api.github.com/{endpoint}", params=params)
    try:
        response = request_with_backoff(api_url, site_name="github", header_kwargs=header_kwargs)
    except HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status in (404, 451, 410):
            return None
        logger.error("Failed to fetch %s: %s %s", api_url, status, exc, exc_info=True)
        return None
    except requests.RequestException as exc:
        logger.error("Error fetching %s: %s", api_url, exc, exc_info=True)
        return None
    except RuntimeError as exc:
        logger.error("Runtime error fetching %s: %s", api_url, exc, exc_info=True)
        return None

    if header_kwargs.get("diff_api", False):
        return {"diff": response.text}
    return cast(dict[str, Any], response.json())


@cache_completion(CACHE_LOCATION, "github_metadata_graphql")
def get_github_metadata_graphql(
    query: str,
    variables: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Execute a GraphQL query against the GitHub API."""
    payload = {"query": query, "variables": variables or {}}
    try:
        response = _post_with_backoff(
            url="https://api.github.com/graphql",
            payload=payload,
        )
    except requests.HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status in (404, 451, 410):
            return None
        logger.error("GraphQL HTTP error %s for query %s", status, query, exc_info=True)
        return None
    except requests.RequestException as exc:
        logger.error("GraphQL request error for query %s: %s", query, exc, exc_info=True)
        return None
    except RuntimeError as exc:
        logger.error("GraphQL runtime error: %s", exc, exc_info=True)
        return None

    data = response.json()
    if "errors" in data:
        logger.error("GraphQL errors: %s", data["errors"])
        return None
    return typing.cast(dict[str, Any], data.get("data", {}))


__all__ = ["get_github_metadata", "get_github_metadata_graphql"]
