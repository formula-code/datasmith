"""
Python module for collecting merge commits from a Git repository using the GitHub API.
"""

import sys

from datasmith.utils import _get_github_metadata


def search_commits(
    repo_name: str,
    query: str,
    max_pages: int = 100,
    per_page: int = 100,
) -> list[str]:
    seen: set[str] = set()

    merge_commits = []
    for page in range(1, max_pages + 1):
        commit_metadata = _get_github_metadata(
            endpoint=f"/repos/{repo_name}/pulls?{query}&per_page={per_page}&page={page}",
        )
        if not commit_metadata:
            break

        for pr in commit_metadata:
            if pr.get("merged_at") and pr["merge_commit_sha"] not in seen:
                seen.add(pr["merge_commit_sha"])
                merge_commits.append(pr["merge_commit_sha"])
                if len(merge_commits) % 50 == 0:
                    sys.stderr.write(f"Collected {len(merge_commits)} merge commits so far.\n")

        if len(commit_metadata) < per_page:
            break

    return merge_commits
