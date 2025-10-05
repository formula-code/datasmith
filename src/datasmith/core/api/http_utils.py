"""HTTP helpers with rate limiting and retry logic."""

from __future__ import annotations

import os
import random
import time
from typing import Any, Callable

import requests
import simple_useragent as sua  # type: ignore[import-untyped]
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException, Timeout

from datasmith import logger

LIST_UA = sua.get_list(shuffle=True, force_cached=True)

RPS: dict[str, int] = {"github": 20, "codecov": 20}
_last_call: dict[str, float] = {"github": 0.0, "codecov": 0.0}


def get_session() -> requests.Session:
    """Return a requests session with connection pooling."""
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _build_github_headers(diff_api: bool = False) -> dict[str, str]:
    if "GH_TOKEN" not in os.environ:
        logger.warning("No GH_TOKEN environment variable found. Rate limits may apply.")
    token = os.environ.get("GH_TOKEN")
    accept = "application/vnd.github.v3.diff" if diff_api else "application/vnd.github+json"
    return {
        "Accept": accept,
        "User-Agent": random.choice(LIST_UA),  # noqa: S311
        **({"Authorization": f"Bearer {token}"} if token else {}),
    }


def _build_codecov_headers() -> dict[str, str]:
    if "CODECOV_TOKEN" not in os.environ:
        logger.warning("No CODECOV_TOKEN environment variable found. Rate limits may apply.")
    token = os.environ.get("CODECOV_TOKEN")
    return {
        "Accept": "application/json",
        "User-Agent": LIST_UA[0],
        **({"Authorization": f"Bearer {token}"} if token else {}),
    }


HeaderBuilder = Callable[..., dict[str, str]]

_configured_headers: dict[str, HeaderBuilder] = {
    "github": _build_github_headers,
    "codecov": _build_codecov_headers,
}


def build_headers(name: str, **kwargs: Any) -> dict[str, str]:
    """Return HTTP headers for the configured API."""
    if name not in _configured_headers:
        raise ValueError(f"Unknown header type: {name}. Available types: {', '.join(_configured_headers.keys())}")
    builder = _configured_headers[name]
    return builder(**kwargs)


def request_with_backoff(
    url: str,
    *,
    site_name: str,
    session: requests.Session | None = None,
    base_delay: float = 1.0,
    max_retries: int = 5,
    max_backoff: float = 60.0,
    header_kwargs: dict[str, Any] | None = None,
) -> requests.Response:
    """GET ``url`` with exponential back-off and basic rate limiting."""
    if session is None:
        session = get_session()
    if header_kwargs is None:
        header_kwargs = {}

    delay = base_delay
    last_exception: RequestException | None = None

    for _ in range(1, max_retries + 1):
        now = time.time()
        min_interval = 1.0 / RPS.get(site_name, 2)
        since = now - _last_call.get(site_name, 0.0)
        if since < min_interval:
            time.sleep(min_interval - since)
        _last_call[site_name] = time.time()

        try:
            resp = session.get(url, headers=build_headers(site_name, **header_kwargs), timeout=15)
            if resp.status_code in (403, 429):
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    sleep_for = float(retry_after)
                else:
                    sleep_for = min(delay, max_backoff)
                    delay *= 2
                resp.close()
                time.sleep(sleep_for)
                continue
            resp.raise_for_status()
        except (Timeout, requests.ConnectionError, HTTPError) as exc:
            last_exception = exc
            time.sleep(min(delay, max_backoff))
            delay *= 2
        else:
            return resp

    raise last_exception or RuntimeError("Unknown error")


def prepare_url(base_url: str, params: dict[str, str] | None = None) -> str:
    """Prepare a URL with optional query parameters."""
    request = requests.Request("GET", base_url, params=params)
    prepared = request.prepare()
    if prepared.url is None:
        raise ValueError(f"Invalid URL: {base_url} with params {params}")
    return prepared.url


__all__ = [
    "RPS",
    "build_headers",
    "get_session",
    "prepare_url",
    "request_with_backoff",
]
