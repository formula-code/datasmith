import time
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from urllib.parse import unquote, urlparse

import pandas as pd

from datasmith.core.file_utils import dl_and_open as _core_dl_and_open
from datasmith.core.file_utils import extract_repo_full_name as core_extract_repo_full_name
from datasmith.core.file_utils import parse_commit_url as core_parse_commit_url
from datasmith.docker.context import Task
from datasmith.logging_config import get_logger
from datasmith.utils import _get_github_metadata

logger = get_logger("scrape.utils")

SEARCH_URL = "https://api.github.com/search/code"


def date_to_unix_timestamp(date_str: str) -> int:
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def make_task(row: pd.Series, tag: str = "run") -> str:
    owner, repo = row["repo_name"].split("/")
    # sha = row["pr_merge_commit_sha"]
    sha = row["pr_base"]["sha"]
    commit_date = date_to_unix_timestamp(row["pr_merged_at"])
    return Task(owner=owner, repo=repo, sha=sha, commit_date=commit_date).with_tag(tag).get_image_name()


def get_patch_from_diff_url(row: pd.Series) -> str | None:
    repo_name = row["repo_name"]
    pull_number = row["pr_number"]
    endpoint = f"/repos/{repo_name}/pulls/{pull_number}"
    diff_text = _get_github_metadata(endpoint=endpoint, params={"diff_api": "true"})
    if not diff_text or "diff" not in diff_text:
        return None
    return cast(str, diff_text["diff"])


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
