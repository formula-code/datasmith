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
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
import simple_useragent as sua  # type: ignore[import-untyped]
from requests.exceptions import HTTPError, RequestException, Timeout

LIST_UA = sua.get_list(shuffle=True, force_cached=True)
SEARCH_URL = "https://api.github.com/search/code"
CACHE_LOCATION = os.getenv("CACHE_LOCATION")
if not CACHE_LOCATION:
    print("⚠️  Warning: CACHE_LOCATION environment variable not set. Using default 'cache.db'.")
    CACHE_LOCATION = "cache.db"


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level)


_cache_lock = threading.Lock()  # Lock for database operations
find_json_block = re.compile(r"```json(.*?)```", re.DOTALL)
_session = requests.Session()


def read_csv(file_path: str) -> pd.DataFrame:
    """
    Read a CSV file and return a DataFrame.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError()
    return pd.read_csv(file_path)


def polite_sleep(seconds: float) -> None:
    until = time.time() + seconds
    while True:
        remaining = until - time.time()
        if remaining <= 0:
            break
        sys.stderr.write(f"\r⏳  Waiting {remaining:4.0f} s …")
        sys.stderr.flush()
        time.sleep(min(remaining, 1))
    sys.stderr.write("\r\033[K")


def get_db_connection(db_loc: str) -> tuple[sqlite3.Cursor, sqlite3.Connection]:
    """Get a SQLite database connection and cursor."""
    conn = sqlite3.connect(db_loc)
    c = conn.cursor()
    return c, conn


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


def _build_headers() -> dict[str, str]:
    if "GH_TOKEN" not in os.environ:
        sys.stderr.write("⚠️  Warning: No GH_TOKEN environment variable found. Rate limits may apply.\n")
    token = os.environ.get("GH_TOKEN", None)
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": random.choice(LIST_UA),  # noqa: S311
        **({"Authorization": f"Bearer {token}"} if token else {}),
    }


def _extract_repo_full_name(url: str) -> str | None:
    """
    Turn a GitHub repo URL into the canonical ``owner/repo`` string.

    Examples
    --------
    >>> _extract_repo_full_name("https://github.com/scipy/scipy")
    'scipy/scipy'
    >>> _extract_repo_full_name("git@github.com:pandas-dev/pandas.git")
    'pandas-dev/pandas'
    """
    if not url:
        return None

    # Strip protocol prefixes that are sometimes stored in CSVs
    path = url.split(":", 1)[-1] if url.startswith(("git@", "ssh://")) else urlparse(url).path.lstrip("/")

    # Remove a possible trailing ".git" or a single trailing "/"
    path = path.rstrip("/").removesuffix(".git")
    if "/" not in path:  # not a repo URL
        return None
    owner, repo = path.split("/", 1)
    return f"{owner}/{repo}"


@cache_completion(CACHE_LOCATION)
def _get_repo_metadata(full_name: str) -> dict[str, typing.Any] | None:
    """
    Call the GitHub REST API for ``owner/repo`` and return the JSON.
    Falls back to *None* when the repo cannot be reached.
    """
    if not full_name:
        return None

    api_url = f"https://api.github.com/repos/{full_name}"
    try:
        r = _request_with_backoff(api_url)
    except HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (404, 451, 410):
            return None
        print(f"Failed to fetch {api_url}: {status} {e}")
        return None
    except RequestException as e:
        print(f"Error fetching {api_url}: {e}")
        return None

    return cast(dict[str, typing.Any], r.json())


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


def dl_and_open(url: str, dl_dir: str, base: str | None = None, force: bool = False) -> str | None:
    rel_path = url[len(base) :].lstrip("/") if base and url.startswith(base) else urlparse(url).path.lstrip("/")

    def clean_component(comp: str) -> str:
        comp = unquote(comp)
        comp = comp.replace(" ", "_").replace("@", "AT")
        comp = comp.replace("(", "").replace(")", "")
        comp = re.sub(r"[^A-Za-z0-9.\-_/]", "_", comp)
        return comp

    clean_parts = [clean_component(c) for c in rel_path.split("/")]
    local_path = os.path.join(dl_dir, *clean_parts)
    local_path = os.path.abspath(local_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    if not os.path.exists(local_path) or force:
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(r.content)
        except requests.RequestException:
            return None
    return local_path
