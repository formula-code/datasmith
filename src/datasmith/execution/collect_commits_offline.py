from __future__ import annotations

import contextlib
import os
import re
import sys
import tempfile
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from typing import Callable

import pandas as pd
from git import Commit, GitCommandError, Repo
from tqdm.auto import tqdm

from datasmith import logger
from datasmith.agents.perf_judge import PerfClassifier
from datasmith.execution.utils import _get_commit_info_offline, clone_repo, get_change_summary, has_core_file

_PR_MERGE_PATTERNS: tuple[re.Pattern[str], ...] = (
    # standard "Merge pull request #123 ..."
    re.compile(r"Merge pull request #(\d+)\b"),
    # squash-merge style "... (#[0-9]+)" on the last line
    re.compile(r"\(#(\d+)\)"),
    # Refers to an issue/PR number. GH-{number}
    re.compile(r"(?:\b|GH-)(\d+)\b"),
    # Has a hashtag followed by a number. #123
    re.compile(r"#(\d+)\b"),
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


def find_tagged_commits(repo: Repo) -> list[str]:
    merge_shas: set[str] = set()
    for tag in repo.tags:
        if tag.commit.hexsha not in merge_shas:
            merge_shas.add(tag.commit.hexsha)

    logger.debug(f"Collected {len(merge_shas)} commits from {repo.working_dir}.")
    return list(merge_shas)


def find_parent_commits(
    repo: Repo, commits: list[str], add_first: bool = False, incl_datetime: bool = False
) -> list[str] | list[tuple[str, float]]:
    parent_commits = set()
    for commit_sha in commits:
        try:
            commit = repo.commit(commit_sha)
            # Add parent commits if they exist
            parents = commit.parents
            # sort parents by commit date, most recent first
            parents = sorted(parents, key=lambda c: c.committed_datetime, reverse=True)
            if add_first and len(parents):
                # only keep the first parent.
                parents = [parents[0]]

            for parent in parents:
                if incl_datetime:
                    parent_commits.add((parent.hexsha, parent.committed_datetime.timestamp()))
                else:
                    parent_commits.add(parent.hexsha)  # type: ignore[arg-type]
        except Exception as e:
            logger.warning(f"Could not find commit {commit_sha} in {repo.working_dir}: {e}")

    logger.debug(f"Collected {len(parent_commits)} parent commits from {repo.working_dir}.")
    return list(parent_commits)


def collect_commits(repo: Repo) -> list[tuple[str, str]]:
    """
    Collect all commit SHAs from the given bare repository.
    """
    branch = _default_branch(repo)
    ref_to_walk = f"origin/{branch}"
    commits = [c.hexsha for c in repo.iter_commits(ref_to_walk)]
    tagged_commits = find_tagged_commits(repo)
    # parent_commits = find_parent_commits(repo, commits + tagged_commits, add_first=True)

    all_commits = []
    for c in commits:
        all_commits.append(("commit", c))
    for c in tagged_commits:
        all_commits.append(("tag", c))
    # for c in parent_commits:
    #     all_commits.append(("parent", c))

    return all_commits


def _parallel_classify(
    commits: list[tuple[str, str | bytes, str]],
    process_commit_tuple: Callable[[tuple[str, str | bytes, str]], str | None],
    repo_name: str,
    n_workers: int,
) -> set[str]:
    merge_shas: set[str] = set()
    max_workers = n_workers
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
                    logger.exception("Worker failed")
                finally:
                    pbar.update(1)

                with contextlib.suppress(StopIteration):
                    pending.add(ex.submit(process_commit_tuple, next(it)))

        pbar.close()

    logger.info("Collected %d commits from %s.", len(merge_shas), repo_name)
    return merge_shas


def get_offline_commit_info(breakpoints: pd.DataFrame, repo_name: str) -> pd.DataFrame:
    commit2kind = {}
    commit2repo = {}
    commits = []

    for gt_hash in breakpoints["gt_hash"].dropna().unique():
        if gt_hash not in commit2kind:
            commit2kind[gt_hash] = "commit"
            commit2repo[gt_hash] = repo_name
            commits.append(gt_hash)
    for hash_ in breakpoints["hash"].dropna().unique():
        if hash_ not in commit2kind:
            commit2kind[hash_] = "parent"
            commit2repo[hash_] = repo_name
            commits.append(hash_)

    with tempfile.TemporaryDirectory(prefix="gh-repos-") as td:
        repo_name, repo = clone_repo(root_path=td, repo_name=repo_name)
        repo_repeat = [repo] * len(commits)
        with ProcessPoolExecutor(max_workers=4) as pp:
            commit_info = list(
                tqdm(
                    pp.map(_get_commit_info_offline, repo_repeat, commits),
                    total=len(commits),
                    desc="Fetching commit metadata",
                )
            )
        commits_meta = pd.json_normalize(commit_info)  # pyright: ignore[reportArgumentType]
        commits_meta = commits_meta[commits_meta["has_asv"]]  # Take out all commits that don't have asv installed.
        commits_meta["kind"] = commits_meta["sha"].map(commit2kind)

        commits_merged = commits_meta[commits_meta["files_changed"].apply(has_core_file)].reset_index(drop=True)
        commits_merged["repo_name"] = commits_merged["sha"].map(commit2repo)
    return commits_merged


def batch_classify_commits(
    perf_classifier: PerfClassifier, repo_name: str, commits: list[tuple[str, str | bytes, str]], n_workers: int
) -> set[str]:
    def process_commit_tuple(t: tuple[str, str | bytes, str]) -> str | None:
        hexsha, message, changes_summary = t
        full_msg = message.strip()

        if not _is_pr_merge(str(full_msg)):
            logger.debug(f"Skipping commit {hexsha}:{full_msg!s} as it is not a PR merge.")
            return None

        full_msg = re.sub(r"\nSigned-off-by:.*", "", str(full_msg)).replace("\n\n", "\n").strip()
        if len(full_msg.split()) > 2048:
            full_msg = " ".join(full_msg.split()[:2048]) + "..."

        is_perf, agent_trace = perf_classifier.get_response(message=str(full_msg), file_change_summary=changes_summary)
        if not is_perf:
            logger.debug(f"Skipping commit {hexsha} as it is not a performance commit.")
            logger.debug(f"Agent trace: {agent_trace}")
            return None

        return hexsha

    if n_workers < 0:
        merge_shas: set[str] = set()
        for t in tqdm(commits, desc=f"Walking {repo_name} commits", unit="commit", file=sys.stdout):
            sha = process_commit_tuple(t)
            if sha:
                merge_shas.add(sha)
        logger.info(f"Collected {len(merge_shas)} commits from {repo_name}.")
        return merge_shas
    else:
        return _parallel_classify(commits, process_commit_tuple, repo_name, n_workers)


def find_parent_releases(
    repo_name: str, commits: list[str], add_first: bool = False, incl_datetime: bool = False
) -> list[str] | list[tuple[str, float]]:
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

        return find_parent_commits(repo, commits, add_first=add_first, incl_datetime=incl_datetime)


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

        return find_tagged_commits(repo)


def find_perf_commits(
    repo_name: str,
    n_workers: int = -1,
    limit: int | None = None,
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
    perf_classifier = PerfClassifier()

    with tempfile.TemporaryDirectory(prefix="gh-history-") as workdir:
        workdir_path = Path(workdir).absolute()
        url = f"https://github.com/{repo_name}.git"

        try:
            repo = Repo.clone_from(
                url,
                workdir_path,
            )
        except GitCommandError as e:
            if e.status == 128:
                msg = e.stderr.strip() or "authentication failed or repository not found"
                logger.warning("Cannot clone %s: %s", url, msg)
                return []
            raise

        # Figure out which ref to walk.
        branch = _default_branch(repo)
        ref_to_walk = f"origin/{branch}"
        try:
            repo.git.rev_parse(ref_to_walk, verify=True)
        except Exception as e:
            raise RuntimeError(f"Cannot resolve ref {ref_to_walk}: {e}") from e

        commits = list(repo.iter_commits(ref_to_walk))
        # commits = [c for c in commits if has_asv(repo, c)]
        commits = commits[:limit] if limit else commits

        summary_info: dict[Commit, str] = {}

        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            # compute get_change_simmary in parallel
            futures = {ex.submit(get_change_summary, c): c for c in commits}
            for f in tqdm(as_completed(futures), total=len(futures), desc="Computing change summaries"):
                c = futures[f]
                try:
                    summary_info[c] = f.result()
                except Exception:
                    logger.exception("Failed to compute change summary for commit %s", c.hexsha)
                    summary_info[c] = ""
        commit_tuples = [(c.hexsha, c.message, summary_info[c]) for c in commits]
        merge_shas = batch_classify_commits(perf_classifier, repo_name, commit_tuples, n_workers)
        return list(merge_shas)
