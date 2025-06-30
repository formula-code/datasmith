import os
import re
import sys
import time
from urllib.parse import unquote, urlparse

import requests

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


def _parse_commit_url(url: str) -> tuple[str, str, str]:
    """
    Parse a GitHub commit URL and return the owner, repo, and commit SHA.
    """
    m = re.match(r"https://github\.com/([^/]+)/([^/]+)/commit/([0-9a-f]{7,40})", url)
    if not m:
        raise ValueError(f"Not a GitHub commit URL: {url!r}")  # noqa: TRY003
    return m.group(1), m.group(2), m.group(3)


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
