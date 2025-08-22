import re
import shutil
import time
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

import requests

from datasmith.logging_config import get_logger

logger = get_logger("scrape.utils")

SEARCH_URL = "https://api.github.com/search/code"


def polite_sleep(seconds: float) -> None:
    from datasmith.logging_config import progress_logger

    until = time.time() + seconds
    while True:
        remaining = until - time.time()
        if remaining <= 0:
            break
        progress_logger.update_progress(f"⏳  Waiting {remaining:4.0f} s …")
        time.sleep(min(remaining, 1))
    progress_logger.finish_progress()


_HEX = re.compile(r"[0-9a-fA-F]{7,40}$")


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
    Return (owner, repo, commit_sha) from a GitHub *commit* URL.

    • Accepts http:// or https://, with or without “www.”
    • Ignores trailing slashes, query strings, and fragments
    • Validates that the SHA is 7-40 hexadecimal characters
    """
    parsed = urlparse(url.strip())

    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")

    if parsed.hostname not in {"github.com", "www.github.com"}:
        raise ValueError(f"Not a GitHub URL: {url!r}")

    path = unquote(parsed.path)
    parts = [p for p in PurePosixPath(path).parts if p != "/"]

    if len(parts) < 4 or parts[2] != "commit":
        raise ValueError(f"Not a GitHub commit URL: {url!r}")

    owner, repo, sha = parts[0], parts[1], parts[3]

    if not _HEX.fullmatch(sha):
        raise ValueError(f"Invalid commit SHA: {sha!r}")

    return owner, repo, sha.lower()


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
    raw_parts = [unquote(p) for p in Path(rel_path).parts]
    raw_path = Path(dl_dir).joinpath(*raw_parts).resolve()

    if raw_path.exists():
        local_path = raw_path
    else:

        def clean_component(comp: str) -> str:
            comp = unquote(comp)
            comp = comp.replace(" ", "_").replace("@", "AT")
            comp = comp.replace("(", "").replace(")", "")
            return re.sub(r"[^A-Za-z0-9.\-_/]", "_", comp)

        clean_parts = [clean_component(p) for p in raw_parts]
        local_path = Path(dl_dir).joinpath(*clean_parts).resolve()

    local_path.parent.mkdir(parents=True, exist_ok=True)

    if is_http:
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

    src_path = Path(parsed.path) if is_file else Path(url)
    if not src_path.exists():
        return None
    if force or not local_path.exists():
        try:
            shutil.copy2(src_path, local_path)
        except OSError:
            return None
    return str(local_path)
