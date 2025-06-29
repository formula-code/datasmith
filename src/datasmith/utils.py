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

import requests
import simple_useragent as sua  # type: ignore[import-untyped]
from requests.exceptions import HTTPError, RequestException, Timeout

LIST_UA = sua.get_list(shuffle=True, force_cached=True)
CACHE_LOCATION = os.getenv("CACHE_LOCATION")
if not CACHE_LOCATION:
    print("⚠️  Warning: CACHE_LOCATION environment variable not set. Using default 'cache.db'.")
    CACHE_LOCATION = "cache.db"


_cache_lock = threading.Lock()  # Lock for database operations
find_json_block = re.compile(r"```json(.*?)```", re.DOTALL)
_session = requests.Session()


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level)


def _build_headers() -> dict[str, str]:
    if "GH_TOKEN" not in os.environ:
        sys.stderr.write("⚠️  Warning: No GH_TOKEN environment variable found. Rate limits may apply.\n")
    token = os.environ.get("GH_TOKEN", None)
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": random.choice(LIST_UA),  # noqa: S311
        **({"Authorization": f"Bearer {token}"} if token else {}),
    }


@typing.no_type_check
def cache_completion(db_loc: str, table_name: str = "cache"):
    """Decorator to cache function results in a SQLite database."""

    # Validate table_name to avoid injection risks
    if not re.match(r"^\w+$", table_name):
        raise ValueError

    def decorator(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            c, conn = get_db_connection(db_loc)
            function_name = f.__name__
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    function_name TEXT,
                    argument_blob BLOB,
                    result_blob BLOB,
                    PRIMARY KEY (function_name, argument_blob)
                )
            """
            with _cache_lock:
                c.execute(create_table_query)
                conn.commit()

            args_blob = pickle.dumps((function_name, args, kwargs))
            with _cache_lock:
                c.execute(
                    f"SELECT result_blob FROM {table_name} WHERE function_name = ? AND argument_blob = ?",  # noqa: S608
                    (function_name, args_blob),
                )
                result = c.fetchone()

                if result:
                    return pickle.loads(result[0])  # noqa: S301

            # Compute and cache the result
            result = f(*args, **kwargs)
            result_blob = pickle.dumps(result)

            with _cache_lock:
                c.execute(
                    f"INSERT INTO {table_name} (function_name, argument_blob, result_blob) VALUES (?, ?, ?)",  # noqa: S608
                    (function_name, args_blob, result_blob),
                )
                conn.commit()

            conn.close()
            return result

        return wrapped

    return decorator


def _request_with_backoff(
    url: str,
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
    for _ in range(1, max_retries + 1):
        # ---------- client-side throttle ----------
        time.sleep(max(0, (1 / rps)))
        try:
            resp = session.get(url, headers=_build_headers(), timeout=15)
            if resp.status_code in (403, 429):
                # ---------------- rate limited ----------------
                reset = resp.headers.get("X-RateLimit-Reset")
                remaining = resp.headers.get("X-RateLimit-Remaining", "1")
                if remaining == "0" and reset:
                    sleep_for = max(0.0, float(reset) - float(time.time()))
                else:
                    sleep_for = min(delay, max_backoff)
                resp.close()
                time.sleep(sleep_for + random.uniform(0, 1))  # noqa: S311
                delay *= 2.0
                continue
            resp.raise_for_status()
            return resp  # noqa: TRY300
        except (Timeout, requests.ConnectionError, HTTPError) as exc:
            last_exception = exc  # may be re-raised later
            time.sleep(min(delay, max_backoff) + random.uniform(0, 1))  # noqa: S311
            delay *= 2.0
    # Out of retries
    raise last_exception or RuntimeError("Unknown error fetching GitHub API")


def get_db_connection(db_loc: str) -> tuple[sqlite3.Cursor, sqlite3.Connection]:
    """Get a SQLite database connection and cursor."""
    conn = sqlite3.connect(db_loc)
    c = conn.cursor()
    return c, conn


@cache_completion(CACHE_LOCATION, "benchmark_commits_dates")
def retrieve_commit_info(url: str) -> dict:
    response = _request_with_backoff(url, max_retries=1)
    json_data: dict = response.json()
    return json_data
