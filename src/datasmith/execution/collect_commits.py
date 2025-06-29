"""
Python module for collecting merge commits from a Git repository using the GitHub API.
"""

import random
import sys
import time
import typing
from typing import cast

from requests.exceptions import HTTPError, RequestException

from datasmith.utils import CACHE_LOCATION, _request_with_backoff, cache_completion

PULL_URL = "https://api.github.com/repos/{repository}/pulls"


@cache_completion(CACHE_LOCATION)
def query_commits(
    repo_name: str, page: int, per_page: int, state: str = "closed", **query_args: typing.Any
) -> typing.Optional[dict[str, typing.Any]]:
    url = PULL_URL.format(repository=repo_name) + f"?state={state}&per_page={per_page}&page={page}"
    try:
        r = _request_with_backoff(url, **query_args)
    except HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (404, 451, 410):
            return None
        print(f"Failed to fetch {url}: {status} {e}")
        return None
    except RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

    return cast(dict[str, typing.Any], r.json())


def search_commits(
    repo_name: str,
    state: str = "closed",
    max_pages: int = 100,
    per_page: int = 100,
    base_delay: float = 1.1,
    max_backoff: int = 60,
    max_retries: int = 6,
    jitter: float = 0.3,
) -> list[str]:
    seen: set[str] = set()

    merge_commits = []
    for page in range(1, max_pages + 1):
        data = query_commits(
            repo_name, page, per_page, state, base_delay=base_delay, max_backoff=max_backoff, max_retries=max_retries
        )
        if not data:
            break

        for pr in data:
            if pr.get("merged_at") and pr["merge_commit_sha"] not in seen:
                seen.add(pr["merge_commit_sha"])
                merge_commits.append(pr["merge_commit_sha"])
                if len(merge_commits) % 50 == 0:
                    sys.stderr.write(f"Collected {len(merge_commits)} merge commits so far.\n")

        time.sleep(base_delay + random.random() * jitter)  # noqa: S311
        if len(data) < per_page:
            break

    return merge_commits
