import time
from pathlib import Path
from urllib.parse import unquote, urlparse

from datasmith.core.file_utils import (
    dl_and_open as _core_dl_and_open,
)
from datasmith.core.file_utils import (
    extract_repo_full_name as core_extract_repo_full_name,
)
from datasmith.core.file_utils import (
    parse_commit_url as core_parse_commit_url,
)
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


def _parse_pr_url(url: str) -> tuple[str, str, str]:
    parsed = urlparse(url.strip())

    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")

    # Normalize API URLs if needed
    if parsed.hostname == "api.github.com":
        # e.g. https://api.github.com/repos/owner/repo/pulls/123
        path = parsed.path.replace("/repos/", "/").replace("/pulls/", "/pull/")
        parsed = parsed._replace(netloc="github.com", path=path)

    if parsed.hostname not in {"github.com", "www.github.com"}:
        raise ValueError(f"Not a GitHub URL: {url!r}")

    path = unquote(parsed.path)
    parts = [p for p in Path(path).parts if p != "/"]

    # Expected: /owner/repo/pull/<number>
    if len(parts) < 4 or parts[2] != "pull":
        raise ValueError(f"Not a GitHub PR URL: {url!r}")

    owner, repo, pr_num = parts[0], parts[1], parts[3]

    if not pr_num.isdigit() or int(pr_num) <= 0:
        raise ValueError(f"Invalid PR number: {pr_num!r}")

    return owner, repo, pr_num


def _extract_repo_full_name(url: str) -> str | None:
    return core_extract_repo_full_name(url)


def _parse_commit_url(url: str) -> tuple[str, str, str]:
    return core_parse_commit_url(url)


def dl_and_open(url: str, dl_dir: str, base: str | None = None, force: bool = False) -> str | None:
    return _core_dl_and_open(url=url, dl_dir=dl_dir, base=base, force=force)


__all__ = [
    "SEARCH_URL",
    "_extract_repo_full_name",
    "_parse_commit_url",
    "dl_and_open",
    "polite_sleep",
]
