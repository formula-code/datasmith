import re
import shutil
import sys
import time
from pathlib import Path
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
    """
    Fetch *url* into *dl_dir* and return the local filename.
    Works for
      • http/https URLs                 (download)
      • file:// URLs                    (copy)
      • ordinary filesystem paths       (copy/return)
    """
    parsed = urlparse(url)
    is_http = parsed.scheme in ("http", "https")
    is_file = parsed.scheme == "file"

    rel_path = url[len(base) :].lstrip("/") if base and url.startswith(base) else parsed.path.lstrip("/")

    def clean_component(comp: str) -> str:
        comp = unquote(comp)
        comp = comp.replace(" ", "_").replace("@", "AT")
        comp = comp.replace("(", "").replace(")", "")
        return re.sub(r"[^A-Za-z0-9.\-_/]", "_", comp)

    clean_parts = [clean_component(p) for p in Path(rel_path).parts]
    local_path = Path(dl_dir).joinpath(*clean_parts).resolve()
    local_path.parent.mkdir(parents=True, exist_ok=True)

    src_path: Path
    if is_http:
        # Always (re-)download when force=True or target missing
        if force or not local_path.exists():
            try:
                r = requests.get(url, timeout=20)
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                local_path.write_bytes(r.content)
            except requests.RequestException:
                return None
        return str(local_path)

    elif is_file:
        src_path = Path(parsed.path)

    else:  # plain local path
        src_path = Path(url)

    # For file:// and plain local paths we just copy if necessary
    if not src_path.exists():
        return None
    if force or not local_path.exists():
        try:
            shutil.copy2(src_path, local_path)
        except OSError:
            return None
    return str(local_path)
