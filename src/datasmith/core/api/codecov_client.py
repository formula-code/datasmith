"""Codecov API helpers."""

from __future__ import annotations

from typing import Any, cast

import requests
from requests import HTTPError

from datasmith import logger
from datasmith.core.api.http_utils import prepare_url, request_with_backoff
from datasmith.core.cache import CACHE_LOCATION, cache_completion


@cache_completion(CACHE_LOCATION, "codecov_metadata")
def get_codecov_metadata(endpoint: str, params: dict[str, str] | None = None) -> dict[str, Any] | None:
    """Call the Codecov API for *endpoint* and return parsed JSON."""
    if not endpoint:
        return None
    params = params or {}
    params.setdefault("format", "json")

    endpoint = endpoint.lstrip("/")
    api_url = prepare_url(f"https://api.codecov.io/api/v2/gh/{endpoint}", params=params)
    try:
        response = request_with_backoff(api_url, site_name="codecov")
    except HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status in (404, 451, 410):
            return None
        logger.error("Failed to fetch %s: %s %s", api_url, status, exc, exc_info=True)
        return None
    except requests.RequestException as exc:
        logger.error("Error fetching %s: %s", api_url, exc, exc_info=True)
        return None

    return cast(dict[str, Any], response.json())


__all__ = ["get_codecov_metadata"]
