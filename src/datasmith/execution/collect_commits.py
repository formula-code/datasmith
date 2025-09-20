"""
Python module for collecting merge commits from a Git repository using the GitHub API.
"""

from urllib.parse import quote

from tqdm.auto import tqdm

from datasmith.utils import _get_github_metadata


def search_for_merge_commit(repo_name: str, pr_number: int) -> str | None:
    query_variants = [
        f'repo:{repo_name} "Merge pull request #{pr_number}"',
        f'repo:{repo_name} "(#{pr_number})"',  # squash-merge pattern
    ]
    for q in map(quote, query_variants):
        metadata = _get_github_metadata(endpoint=f"search/commits?q={q}")
        if not metadata:
            continue
        if metadata.get("total_count", 0) > 0:
            commit_id = metadata["items"][0]["sha"]
            return str(commit_id)

    return None


def search_commits(
    repo_name: str,
    query: str,
    max_pages: int = 100,
    per_page: int = 100,
) -> list[str]:
    seen: set[str] = set()

    merge_commits = []
    for page in tqdm(range(1, max_pages + 1), desc="Collecting merge commits"):
        commit_metadata = _get_github_metadata(
            endpoint=f"/repos/{repo_name}/pulls?{query}&per_page={per_page}&page={page}",
        )
        if not commit_metadata:
            break

        for pr in commit_metadata:
            if pr.get("merged_at") and pr["merge_commit_sha"] not in seen:
                merge_commit_sha = pr["merge_commit_sha"]
                is_reachable = _get_github_metadata(
                    endpoint=f"/repos/{repo_name}/commits/{merge_commit_sha}",
                )
                if not is_reachable:
                    merge_commit_sha = search_for_merge_commit(repo_name, pr["number"])
                    if not merge_commit_sha:
                        continue
                seen.add(merge_commit_sha)
                merge_commits.append(merge_commit_sha)

        if len(commit_metadata) < per_page:
            break

    return merge_commits
