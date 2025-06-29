import os
import re
import sys
import time
import typing
from typing import cast
from urllib.parse import unquote, urlparse

import requests
from requests.exceptions import HTTPError, RequestException

from datasmith.utils import CACHE_LOCATION, _request_with_backoff, cache_completion

SEARCH_URL = "https://api.github.com/search/code"


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
