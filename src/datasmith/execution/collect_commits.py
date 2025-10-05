"""Collect merge commits from GitHub with defensive typing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import quote

from tqdm.auto import tqdm

from datasmith.core.api.github_client import get_github_metadata


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    """Return value as mapping when possible."""
    return value if isinstance(value, Mapping) else None


def _as_sequence_of_mappings(value: Any) -> list[Mapping[str, Any]]:
    """Return a list of mapping-like entries from value."""
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def search_for_merge_commit(repo_name: str, pr_number: int) -> str | None:
    query_variants = [
        f'repo:{repo_name} "Merge pull request #{pr_number}"',
        f'repo:{repo_name} "(#{pr_number})"',  # squash-merge pattern
    ]
    for q in map(quote, query_variants):
        metadata_raw = get_github_metadata(endpoint=f"search/commits?q={q}")
        metadata = _as_mapping(metadata_raw)
        if not metadata:
            continue
        if metadata.get("total_count", 0) > 0:
            items = metadata.get("items")
            if isinstance(items, Sequence) and items:
                first = items[0]
                if isinstance(first, Mapping):
                    commit_id = first.get("sha")
                    if isinstance(commit_id, str):
                        return commit_id

    return None


def search_commits(
    repo_name: str,
    query: str,
    max_pages: int = 100,
    per_page: int = 100,
) -> list[str]:
    seen: set[str] = set()

    merge_commits: list[str] = []
    for page in tqdm(range(1, max_pages + 1), desc="Collecting merge commits"):
        commit_metadata_raw = get_github_metadata(
            endpoint=f"/repos/{repo_name}/pulls?{query}&per_page={per_page}&page={page}",
        )
        commit_metadata = _as_sequence_of_mappings(commit_metadata_raw)
        if not commit_metadata:
            break

        for pr in commit_metadata:
            merged_at = pr.get("merged_at")
            merge_commit_sha = pr.get("merge_commit_sha")
            pr_number = pr.get("number")
            if not merged_at or not isinstance(merge_commit_sha, str) or not isinstance(pr_number, int):
                continue

            if merge_commit_sha in seen:
                continue

            commit_response = get_github_metadata(endpoint=f"/repos/{repo_name}/commits/{merge_commit_sha}")
            if not commit_response:
                merge_commit_sha = search_for_merge_commit(repo_name, pr_number)
                if not merge_commit_sha:
                    continue

            seen.add(merge_commit_sha)
            merge_commits.append(merge_commit_sha)

        if len(commit_metadata) < per_page:
            break

    return merge_commits
