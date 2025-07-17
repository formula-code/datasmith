from __future__ import annotations

import os
import re
import tempfile
import urllib.parse
from pathlib import Path

from git import GitCommandError, Repo

from datasmith import logger
from datasmith.utils import CACHE_LOCATION, _get_github_metadata, cache_completion

_PR_MERGE_PATTERNS: tuple[re.Pattern[str], ...] = (
    # standard "Merge pull request #123 ..."
    re.compile(r"Merge pull request #(\d+)\b"),
    # squash-merge style "... (#[0-9]+)" on the last line
    re.compile(r"\(#(\d+)\)"),
)


def _default_branch(repo: Repo) -> str:
    """
    Resolve the remote's default branch (origin/HEAD -> "main" / "master" / ...).
    """
    try:
        # “origin/main”
        full_ref: str = repo.git.symbolic_ref("--quiet", "--short", "refs/remotes/origin/HEAD")
        return full_ref.split("/", 1)[1]  # keep text after "origin/"
    except Exception:
        # Fallback if symbolic-ref is missing (rare).
        return repo.head.reference.name


def _is_pr_merge(message: str) -> bool:
    """
    True iff *message* matches one of our PR-closing patterns.
    """
    return any(p.search(message) for p in _PR_MERGE_PATTERNS)


def _is_public(repo_name: str) -> bool:
    """
    Check if a repo is public.
    """
    return _get_github_metadata(f"/repos/{repo_name}") is not None


@cache_completion(CACHE_LOCATION, "search_commits_offline")
def search_commits(
    repo_name: str,
    query: str,
    max_pages: int = 100,  # ignored (kept for compatibility)
    per_page: int = 100,  # ignored (kept for compatibility)
) -> list[str]:
    """
    Return a list of commit SHAs that closed pull requests, **without**
    calling any GitHub API endpoints.  Internally:

        • clones the repo (metadata-only) into a tmp dir
        • walks the commit history
        • selects commits whose message looks like a PR merge

    The only element of *query* we still honour is `base=<branch>`.
    """
    qs = urllib.parse.parse_qs(query, keep_blank_values=True)
    base_branch: str | None = qs.get("base", [None])[0]

    with tempfile.TemporaryDirectory(prefix="gh-history-") as workdir:
        workdir_path = Path(workdir)
        url = f"https://github.com/{repo_name}.git"

        # Clone *just* the commit / tree metadata (no blobs).
        clone_kwargs: dict = {
            "multi_options": ["--filter=tree:0"],
            "no_checkout": True,
        }
        if base_branch:
            clone_kwargs["branch"] = base_branch

        # ignore if repo is not public
        try:
            repo = Repo.clone_from(
                url,
                workdir_path,
                env={"GIT_TERMINAL_PROMPT": "0", **os.environ},
                **clone_kwargs,
            )
        except GitCommandError as e:
            if e.status == 128:
                msg = e.stderr.strip() or "authentication failed or repository not found"
                logger.warning("Cannot clone %s: %s", url, msg)
                return []
            raise

        # Figure out which ref to walk.
        branch = base_branch or _default_branch(repo)
        ref_to_walk = f"origin/{branch}"

        merge_shas: set[str] = set()
        for commit in repo.iter_commits(ref_to_walk):
            if _is_pr_merge(str(commit.message)):
                merge_shas.add(commit.hexsha)

        return sorted(merge_shas)
