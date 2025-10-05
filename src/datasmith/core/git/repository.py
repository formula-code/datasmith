"""Repository-level Git utilities."""

from __future__ import annotations

import re
from pathlib import Path

from git import Commit, Repo

from datasmith.core.api.github_client import get_github_metadata


def clone_repo(root_path: str | Path, repo_name: str) -> tuple[str, Repo]:
    """Clone ``owner/repo`` into ``root_path`` and return the Repo handle."""
    repo_name = repo_name.strip("/")
    owner, name = repo_name.split("/", 1)
    path = Path(root_path) / f"{owner}__{name}.git"
    repo = Repo.clone_from(
        f"https://github.com/{repo_name}.git",
        path,
        quiet=True,
        allow_unsafe_options=True,
        allow_unsafe_protocols=True,
    )
    return repo_name, repo


def has_asv(repo: Repo, commit: Commit) -> bool:
    """Return True if *commit* contains ASV result blobs."""
    return any(obj.type == "blob" and re.match(r"asv\..*\.json", obj.name) for obj in commit.tree.traverse())  # type: ignore[union-attr]


def find_file_in_tree(repo: str, filename: str, branch: str | None = None) -> list[str] | None:
    """Find files named ``filename`` within ``repo`` (optionally on ``branch``)."""
    if branch is None:
        repo_info = get_github_metadata(f"/repos/{repo}")
        if isinstance(repo_info, list):
            if len(repo_info) == 1:
                repo_info = repo_info[0]
            else:
                raise ValueError(f"Expected one repo info object, got {len(repo_info)}")
        branch = repo_info.get("default_branch") if isinstance(repo_info, dict) else None
        if not branch:
            raise ValueError("Could not determine the default branch for this repository")

    ref = get_github_metadata(f"/repos/{repo}/git/refs/heads/{branch}")
    if isinstance(ref, list):
        if len(ref) == 1:
            ref = ref[0]
        else:
            raise ValueError("Unexpected response while resolving branch ref")
    sha = ref["object"]["sha"]  # type: ignore[index]

    tree = get_github_metadata(f"/repos/{repo}/git/trees/{sha}?recursive=1")
    if not isinstance(tree, dict):
        raise ValueError("Unexpected tree response from GitHub API")  # noqa: TRY004

    entries = tree.get("tree", [])
    matches = [
        entry["path"] for entry in entries if entry.get("type") == "blob" and entry.get("path", "").endswith(filename)
    ]
    matches = [match for match in matches if match.count("/") <= 2]
    return matches or None


__all__ = ["clone_repo", "find_file_in_tree", "has_asv"]
