"""HTTP helpers with rate limiting and retry logic.

Enhancements:
- Add long-wait handling for GitHub rate limits (403/429) using X-RateLimit-Reset
  or a conservative 90-minute default with periodic log pings.
- Do not consume retry attempts on rate-limit waits; only increment on real
  transport or HTTP errors to avoid exiting early.
"""

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

RPS: dict[str, int] = {"github": 2, "codecov": 20}
_last_call: dict[str, float] = {"github": 0.0, "codecov": 0.0}


def get_session() -> requests.Session:
    """Return a requests session with connection pooling."""
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_gh_tokens() -> list[str]:
    tokens_str = os.environ.get("GH_TOKENS", "")
    tokens = [token.strip() for token in tokens_str.split(",") if token.strip()]
    return tokens


def _build_github_headers(diff_api: bool = False) -> dict[str, str]:
    if "GH_TOKENS" not in os.environ:
        logger.warning("No GH_TOKEN environment variable found. Rate limits may apply.")
    all_gh_tokens = parse_gh_tokens()
    if not len(all_gh_tokens):
        logger.warning("No GH_TOKENS environment variable found or it's empty. Rate limits may apply.")
    token = random.choice(all_gh_tokens) if all_gh_tokens else None  # noqa: S311
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


def request_with_backoff(  # noqa: C901
    url: str,
    *,
    site_name: str,
    session: requests.Session | None = None,
    base_delay: float = 1.0,
    max_retries: int = 5,
    max_backoff: float = 60.0,
    header_kwargs: dict[str, Any] | None = None,
) -> requests.Response:
    """GET ``url`` with exponential back-off and basic rate limiting.

    Special handling for GitHub rate limits: when we receive 403/429 with
    evidence of rate limiting, we wait until the server-advertised reset time
    (``X-RateLimit-Reset``) or for a conservative 90 minutes, emitting periodic
    log pings so long-running jobs are observable. Rate-limited waits do not
    consume a retry attempt.
    """
    if session is None:
        session = get_session()
    if header_kwargs is None:
        header_kwargs = {}

    delay = base_delay
    last_exception: RequestException | None = None
    attempt = 0

    def _sleep_with_pings(total_seconds: float, *, ping_interval: float = 300.0, reason: str = "") -> None:
        end = time.time() + max(0.0, total_seconds)
        remaining = max(0, int(end - time.time()))
        if reason:
            logger.warning(
                "Waiting %dm %ds (%s). Pinging every %d minutes.",
                remaining // 60,
                remaining % 60,
                reason,
                int(ping_interval // 60) or 1,
            )
        else:
            logger.warning(
                "Waiting %dm %ds. Pinging every %d minutes.",
                remaining // 60,
                remaining % 60,
                int(ping_interval // 60) or 1,
            )

        while True:
            now = time.time()
            if now >= end:
                break
            to_sleep = min(ping_interval, end - now)
            time.sleep(max(0.1, to_sleep))
            remaining = max(0, int(end - time.time()))
            logger.info("Still waiting… %dm %ds remaining", remaining // 60, remaining % 60)

    while attempt < max_retries:
        now = time.time()
        min_interval = 1.0 / RPS.get(site_name, 2)
        since = now - _last_call.get(site_name, 0.0)
        if since < min_interval:
            time.sleep(min_interval - since)
        _last_call[site_name] = time.time()

        try:
            resp = session.get(url, headers=build_headers(site_name, **header_kwargs), timeout=15)
            if resp.status_code in (403, 429):
                # Detect rate limit response. Prefer explicit remaining header
                # or look for a generic hint in the body text.
                body_lower = (resp.text or "").lower()
                is_rate_limited = resp.headers.get("X-RateLimit-Remaining") == "0" or "rate limit" in body_lower
                if is_rate_limited and site_name == "github":
                    # Compute a generous wait time: Retry-After, then X-RateLimit-Reset, else 90 minutes
                    retry_after = resp.headers.get("Retry-After")
                    reset_at = resp.headers.get("X-RateLimit-Reset")

                    wait_seconds = float(30 * 60)  # default to 0.5h
                    try:
                        if retry_after:
                            wait_seconds = max(wait_seconds, float(retry_after))
                        elif reset_at:
                            reset_ts = float(reset_at)
                            delta = max(0.0, reset_ts - time.time())
                            wait_seconds = max(wait_seconds, delta + 30.0)  # tiny buffer
                    except Exception:
                        wait_seconds = float(30 * 60)

                    resp.close()
                    _sleep_with_pings(wait_seconds, reason=f"{site_name} rate limit")
                    # Do NOT increment attempts on rate limits
                    continue

                # Non-rate-limit 403/429: backoff counts as a retry
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    sleep_for = float(retry_after)
                else:
                    sleep_for = min(delay, max_backoff)
                    delay *= 2
                resp.close()
                time.sleep(sleep_for)
                attempt += 1
                continue
            resp.raise_for_status()
        except (Timeout, requests.ConnectionError, HTTPError) as exc:
            last_exception = exc
            time.sleep(min(delay, max_backoff))
            delay *= 2
            attempt += 1
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
