import functools
import logging
import os
import pickle
import random
import re
import sqlite3
import sys
import threading
import time
import typing
from typing import cast

import requests
import simple_useragent as sua  # type: ignore[import-untyped]
from requests.exceptions import HTTPError, RequestException, Timeout

from datasmith import logger

LIST_UA = sua.get_list(shuffle=True, force_cached=True)
CACHE_LOCATION = os.getenv("CACHE_LOCATION")
if not CACHE_LOCATION:
    print("⚠️  Warning: CACHE_LOCATION environment variable not set. Using default 'cache.db'.")
    CACHE_LOCATION = "cache.db"


_cache_lock = threading.Lock()  # Lock for database operations
find_json_block = re.compile(r"```json(.*?)```", re.DOTALL)
_session = requests.Session()


def _build_github_headers() -> dict[str, str]:
    if "GH_TOKEN" not in os.environ:
        sys.stderr.write("⚠️  Warning: No GH_TOKEN environment variable found. Rate limits may apply.\n")
    token = os.environ.get("GH_TOKEN", None)
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": random.choice(LIST_UA),  # noqa: S311
        **({"Authorization": f"Bearer {token}"} if token else {}),
    }


def _build_codecov_headers() -> dict[str, str]:
    """
    Build headers for Codecov API requests.
    """
    if "CODECOV_TOKEN" not in os.environ:
        sys.stderr.write("⚠️  Warning: No CODECOV_TOKEN environment variable found. Rate limits may apply.\n")
    token = os.environ.get("CODECOV_TOKEN", None)
    return {
        "Accept": "application/json",
        "User-Agent": random.choice(LIST_UA),  # noqa: S311
        **({"Authorization": f"Bearer {token}"} if token else {}),
    }


configured_headers = {
    "github": _build_github_headers,
    "codecov": _build_codecov_headers,
}


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level)


def _build_headers(name: str) -> dict[str, str]:
    if name not in configured_headers:
        raise ValueError(f"Unknown header type: {name}. Available types: {', '.join(configured_headers.keys())}")  # noqa: TRY003

    return configured_headers[name]()


@typing.no_type_check
def cache_completion(db_loc: str, table_name: str = "cache"):
    """Decorator to cache function results in a SQLite database.

    Passing `bypass_cache=True` to the wrapped function forces a fresh
    computation *and* overwrites the stored value for the same
    positional / keyword arguments (with `bypass_cache` ignored when
    hashing the arguments).
    """
    # Validate table_name to avoid SQL-injection risks
    if not re.match(r"^\w+$", table_name):
        raise ValueError("table_name must be alphanumeric/underscore only")  # noqa: TRY003

    def decorator(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            # -- Handle bypass flag -----------------------------------------
            bypass = kwargs.pop("bypass_cache", False)
            # We DON'T include the flag in the cache key so that it maps to
            # the same row whether or not the user asks to bypass.
            key_kwargs = kwargs.copy()

            # ----------------------------------------------------------------
            c, conn = get_db_connection(db_loc)  # or (c, conn) if your helper returns that order
            function_name = func.__name__

            create_table_sql = (
                f"CREATE TABLE IF NOT EXISTS {table_name} ("
                "function_name TEXT,"
                "argument_blob BLOB,"
                "result_blob   BLOB,"
                "PRIMARY KEY (function_name, argument_blob)"
                ")"
            )
            with _cache_lock:
                c.execute(create_table_sql)
                conn.commit()

            args_blob = pickle.dumps((function_name, args, key_kwargs))

            # ----------------- Try to read from cache -----------------------
            if not bypass:
                with _cache_lock:
                    c.execute(
                        f"""SELECT result_blob FROM {table_name}
                            WHERE function_name = ? AND argument_blob = ?""",  # noqa: S608
                        (function_name, args_blob),
                    )
                    row = c.fetchone()
                    if row is not None:
                        conn.close()
                        return pickle.loads(row[0])  # noqa: S301

            # ----------------- Compute fresh result -------------------------
            result = (
                func(*args, **kwargs, bypass_cache=bypass)
                if "bypass_cache" in func.__code__.co_varnames
                else func(*args, **kwargs)
            )
            result_blob = pickle.dumps(result)

            # ----------------- Upsert into cache ----------------------------
            with _cache_lock:
                # INSERT OR REPLACE does the “overwrite” when the row exists
                c.execute(
                    f"""INSERT OR REPLACE INTO {table_name}
                        (function_name, argument_blob, result_blob)
                        VALUES (?, ?, ?)""",  # noqa: S608
                    (function_name, args_blob, result_blob),
                )
                conn.commit()
            conn.close()
            return result

        return wrapped

    return decorator


def _request_with_backoff(
    url: str,
    site_name: str,
    session: requests.Session = _session,
    rps: int = 2,
    base_delay: float = 1.0,
    max_retries: int = 5,
    max_backoff: float = 60.0,
) -> requests.Response:
    """
    GET ``url`` with retry, exponential back-off, client-side throttling,
    and GitHub-aware rate-limit handling.
    """
    last_exception: RequestException | None = None
    delay: float = base_delay

    for attempt in range(1, max_retries + 1):
        # ---------- client-side throttle ----------
        throttle = max(0, (1 / rps))
        logger.debug(
            "Attempt %d/%d → %s (site=%s) | throttling %.2fs",
            attempt,
            max_retries,
            url,
            site_name,
            throttle,
        )
        time.sleep(throttle)

        try:
            logger.debug("GET %s (timeout=15s)", url)
            resp = session.get(url, headers=_build_headers(site_name), timeout=15)
            logger.debug("Status %d from %s", resp.status_code, url)

            if resp.status_code in (403, 429):
                # ---------------- rate limited ----------------
                reset = resp.headers.get("X-RateLimit-Reset")
                remaining = resp.headers.get("X-RateLimit-Remaining", "1")

                if remaining == "0" and reset:
                    sleep_for = max(0.0, float(reset) - float(time.time()))
                else:
                    sleep_for = min(delay, max_backoff)

                logger.debug(
                    "Rate-limited (remaining=%s, reset=%s). Sleeping %.2fs then retrying.",
                    remaining,
                    reset,
                    sleep_for,
                )
                resp.close()
                time.sleep(sleep_for + random.uniform(0, 1))  # noqa: S311
                delay *= 2.0
                continue

            resp.raise_for_status()
            logger.debug("Success on attempt %d for %s", attempt, url)
            return resp  # noqa: TRY300

        except (Timeout, requests.ConnectionError, HTTPError) as exc:
            last_exception = exc  # may be re-raised later
            logger.debug(
                "Transient error on attempt %d for %s: %s. Backing off %.2fs",
                attempt,
                url,
                exc,
                delay,
                exc_info=True,
            )
            time.sleep(min(delay, max_backoff) + random.uniform(0, 1))  # noqa: S311
            delay *= 2.0

    # Out of retries
    logger.debug("Exhausted retries for %s; raising %s", url, last_exception)
    raise last_exception or RuntimeError("Unknown error fetching GitHub API")


def get_db_connection(db_loc: str) -> tuple[sqlite3.Cursor, sqlite3.Connection]:
    """Get a SQLite database connection and cursor."""
    conn = sqlite3.connect(db_loc)
    c = conn.cursor()
    return c, conn


def prepare_url(base_url: str, params: dict[str, str] | None = None) -> str:
    """
    Prepare a URL with query parameters.
    """
    r = requests.Request("GET", base_url, params=params)
    prepared = r.prepare()
    if prepared.url is None:
        raise ValueError(f"Invalid URL: {base_url} with params {params}")  # noqa: TRY003
    return prepared.url


@cache_completion(CACHE_LOCATION, "github_metadata")
def _get_github_metadata(endpoint: str, params: dict[str, str] | None = None) -> dict[str, typing.Any] | None:
    """
    Call the GitHub REST API for a specific endpoint and return the JSON.
    Falls back to *None* when the endpoint cannot be reached.

    Examples
    --------
    >>> _get_github_metadata(endpoint="repos/scipy/scipy")
    {'id': 123456, 'name': 'scipy', ...}
    """
    if not endpoint:
        return None
    endpoint = endpoint.lstrip("/")
    api_url = prepare_url(f"https://api.github.com/{endpoint}", params=params)
    try:
        r = _request_with_backoff(api_url, site_name="github")
    except HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (404, 451, 410):
            return None
        # print(f"Failed to fetch {api_url}: {status} {e}")
        logger.error("Failed to fetch %s: %s %s", api_url, status, e, exc_info=True)
        return None
    except RequestException as e:
        # print(f"Error fetching {api_url}: {e}")
        logger.error("Error fetching %s: %s", api_url, e, exc_info=True)
        return None
    except RuntimeError as e:
        logger.error("Runtime error fetching %s: %s", api_url, e, exc_info=True)
        return None

    return cast(dict[str, typing.Any], r.json())


@cache_completion(CACHE_LOCATION, "codecov_metadata")
def _get_codecov_metadata(endpoint: str, params: dict[str, str] | None = None) -> dict[str, typing.Any] | None:
    """
    Call the Codecov API for a specific endpoint and return the JSON.
    Falls back to *None* when the endpoint cannot be reached.
    """
    if not endpoint:
        return None
    endpoint = endpoint.lstrip("/")
    api_url = prepare_url(f"https://api.codecov.io/api/v2/gh/{endpoint}", params=params)
    try:
        r = _request_with_backoff(api_url, site_name="codecov")
    except HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (404, 451, 410):
            return None
        # print(f"Failed to fetch {api_url}: {status} {e}")
        logger.error("Failed to fetch %s: %s %s", api_url, status, e, exc_info=True)
        return None
    except RequestException as e:
        logger.error("Error fetching %s: %s", api_url, e, exc_info=True)
        return None

    return cast(dict[str, typing.Any], r.json())
