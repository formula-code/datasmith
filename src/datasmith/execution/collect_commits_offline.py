from __future__ import annotations

import contextlib
import os
import re
import sys
import tempfile
import urllib.parse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

from git import GitCommandError, Repo
from tqdm.auto import tqdm

from datasmith import logger
from datasmith.agents.perf_judge import PerfClassifier
from datasmith.utils import CACHE_LOCATION, cache_completion

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


def find_parent_commits(repo_name: str, commits: list[str]) -> list[str]:
    """
    Return a list of commit SHAs that are parent commits of the given commits,
    **without** calling any GitHub API endpoints.
    """
    with tempfile.TemporaryDirectory(prefix="gh-history-") as workdir:
        workdir_path = Path(workdir)
        url = f"https://github.com/{repo_name}.git"

        # Clone *just* the commit / tree metadata (no blobs).
        clone_kwargs: dict = {
            "multi_options": ["--filter=tree:0"],
            "no_checkout": True,
        }

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

        parent_commits = set()
        for commit_sha in commits:
            try:
                commit = repo.commit(commit_sha)
                # Add parent commits if they exist
                for parent in commit.parents:
                    parent_commits.add(parent.hexsha)
            except Exception as e:
                logger.warning(f"Could not find commit {commit_sha} in {repo_name}: {e}")

        logger.info(f"Collected {len(parent_commits)} parent commits from {repo_name}.")
        return sorted(parent_commits)


def find_tagged_releases(repo_name: str) -> list[str]:
    """
    Return a list of commit SHAs that are tagged releases, **without**
    calling any GitHub API endpoints.
    """
    with tempfile.TemporaryDirectory(prefix="gh-history-") as workdir:
        workdir_path = Path(workdir)
        url = f"https://github.com/{repo_name}.git"

        # Clone *just* the commit / tree metadata (no blobs).
        clone_kwargs: dict = {
            "multi_options": ["--filter=tree:0"],
            "no_checkout": True,
        }

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

        merge_shas: set[str] = set()
        for tag in repo.tags:
            if tag.commit.hexsha not in merge_shas:
                merge_shas.add(tag.commit.hexsha)

        logger.info(f"Collected {len(merge_shas)} commits from {repo_name}.")

        return sorted(merge_shas)


@cache_completion(CACHE_LOCATION, "find_perf_commits")
def find_perf_commits(  # noqa: C901
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
    Uses an AI Agent to find performance-related commits.
    """
    qs = urllib.parse.parse_qs(query, keep_blank_values=True)
    base_branch: str | None = qs.get("base", [None])[0]
    n_workers = qs.get("n_workers", [1])[0]
    n_workers = int(n_workers) if isinstance(n_workers, str) else 1

    perf_classifier = PerfClassifier()

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

        commits = [(c.hexsha, c.message) for c in repo.iter_commits(ref_to_walk)]

        def process_commit_tuple(t: tuple[str, str | bytes]) -> str | None:
            hexsha, message = t
            full_msg = message.strip()

            if not _is_pr_merge(str(full_msg)):
                logger.debug(f"Skipping commit {hexsha} as it is not a PR merge.")
                return None

            full_msg = re.sub(r"\nSigned-off-by:.*", "", str(full_msg)).replace("\n\n", "\n").strip()
            if len(full_msg.split()) > 2048:
                full_msg = " ".join(full_msg.split()[:2048]) + "..."

            is_perf, agent_trace = perf_classifier.get_response(message=str(full_msg))
            if not is_perf:
                logger.debug(f"Skipping commit {hexsha} as it is not a performance commit.")
                logger.debug(f"Agent trace: {agent_trace}")
                return None

            return hexsha

        merge_shas: set[str] = set()
        max_workers = n_workers
        # keep a small multiple of workers in-flight; adjust if you want more buffering
        window = max_workers * 4

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            pbar = tqdm(
                total=len(commits),
                desc=f"Walking {repo_name} commits",
                unit="commit",
                file=sys.stdout,
                miniters=1,
                mininterval=0.1,
            )

            it = iter(commits)
            pending = set()

            # prime the window
            for _ in range(min(window, len(commits))):
                pending.add(ex.submit(process_commit_tuple, next(it)))

            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)

                for fut in done:
                    try:
                        sha = fut.result()
                        if sha:
                            merge_shas.add(sha)
                    except Exception:
                        # don't let one bad task kill the progress loop
                        logger.exception("Worker failed")
                    finally:
                        pbar.update(1)

                    # backfill one task for each completed, keeping the window steady
                    with contextlib.suppress(StopIteration):
                        pending.add(ex.submit(process_commit_tuple, next(it)))

            pbar.close()

        logger.info(f"Collected {len(merge_shas)} commits from {repo_name}.")
        return sorted(merge_shas)
