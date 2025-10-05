"""Commit-centric Git utilities."""

from __future__ import annotations

from typing import Any

from git import BadName, Commit, GitCommandError, Repo
from requests.exceptions import HTTPError

from datasmith import logger
from datasmith.core.api.github_client import get_github_metadata
from datasmith.core.cache import CACHE_LOCATION, cache_completion
from datasmith.core.git.repository import has_asv


def get_change_summary(commit: Commit) -> str:
    """Produce a markdown table summarising file-level stats for *commit*."""
    stats = commit.stats
    lines = [
        "| File | Lines Added | Lines Removed | Total Changes |",
        "|------|-------------|----------------|----------------|",
    ]
    for file_path, file_stats in stats.files.items():
        lines.append(
            f"| {file_path} | {file_stats['insertions']} | {file_stats['deletions']} | {file_stats['lines']} |"
        )
    return "\n".join(lines)


def get_commit_info(repo_name: str, commit_sha: str) -> dict[str, Any]:
    """Fetch commit metadata for ``repo_name``/``commit_sha`` via the GitHub API."""
    try:
        commit_info = get_github_metadata(endpoint=f"/repos/{repo_name}/commits/{commit_sha}")
        if commit_info is None:
            commit_info = get_github_metadata(endpoint=f"/repos/{repo_name}/commits/{commit_sha}")
    except HTTPError:
        logger.exception("Error fetching commit info: %s", commit_sha)
        return {
            "sha": commit_sha,
            "date": None,
            "message": None,
            "total_additions": 0,
            "total_deletions": 0,
            "total_files_changed": 0,
            "files_changed": "",
        }

    if not commit_info:
        return {
            "sha": commit_sha,
            "date": None,
            "message": None,
            "total_additions": 0,
            "total_deletions": 0,
            "total_files_changed": 0,
            "files_changed": "",
        }

    if commit_sha != commit_info["sha"]:
        raise ValueError("Commit SHA mismatch")
    return {
        "sha": commit_info["sha"],
        "date": commit_info["commit"]["committer"]["date"],
        "message": commit_info["commit"]["message"],
        "total_additions": commit_info["stats"]["additions"],
        "total_deletions": commit_info["stats"]["deletions"],
        "total_files_changed": commit_info["stats"]["total"],
        "files_changed": "\n".join([d["filename"] for d in commit_info["files"]]),
    }


@cache_completion(CACHE_LOCATION, "get_commit_info_offline")
def get_commit_info_offline(repo: Repo, commit_sha: str) -> dict[str, Any]:
    """Gather commit info without hitting the GitHub REST API."""
    default = {
        "sha": commit_sha,
        "date": None,
        "message": None,
        "total_additions": 0,
        "total_deletions": 0,
        "total_files_changed": 0,
        "files_changed": "",
        "patch": "",
        "has_asv": False,
        "file_change_summary": "",
    }
    try:
        commit = repo.commit(commit_sha)
    except (BadName, ValueError):
        logger.exception("Maybe commit not found: %s", commit_sha)
        repo.git.fetch("--no-filter", "--quiet", "origin", commit_sha)
        commit = repo.commit(commit_sha)
    except GitCommandError:
        logger.exception("Error fetching commit info: %s", commit_sha)
        return default

    stats = commit.stats
    patch = (
        repo.git.format_patch("--stdout", "-1", commit.hexsha)
        .encode("utf-8", "surrogateescape")
        .decode("utf-8", "backslashreplace")
    )

    return {
        "sha": commit.hexsha,
        "date": commit.committed_datetime.isoformat(),
        "message": commit.message,
        "total_additions": stats.total["insertions"],
        "total_deletions": stats.total["deletions"],
        "total_files_changed": stats.total["files"],
        "files_changed": "\n".join(str(k) for k in stats.files),
        "patch": patch,
        "has_asv": has_asv(repo, commit),
        "file_change_summary": get_change_summary(commit),
    }


__all__ = [
    "get_change_summary",
    "get_commit_info",
    "get_commit_info_offline",
]
