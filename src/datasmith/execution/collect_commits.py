"""Collect merge commits from GitHub with defensive typing."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import quote

from tqdm.auto import tqdm

from datasmith import logger
from datasmith.agents.perf_judge import PerfClassifier
from datasmith.core.api.github_client import get_github_metadata
from datasmith.execution.collect_commits_offline import batch_classify_commits


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


def collect_merge_shas(repo: str) -> list[dict]:
    """
    Return (merge_commit_sha, merged_at) for PRs that are closed AND have a non-null merge_commit_sha.
    Optimizations:
      • No per-PR verification or fallback calls
      • Filters by default base branch to shrink result set
    """
    # (Optional) limit to default branch so you don't scan PRs into other bases
    try:
        meta = get_github_metadata(f"repos/{repo}") or {}
        default_base = meta.get("default_branch")
    except Exception:
        default_base = None

    params = {"state": "closed", "sort": "updated", "direction": "desc"}
    if default_base:
        params["base"] = default_base  # reduces pages for repos with many non-default-base PRsd

    prs = _paginate_github(f"repos/{repo}/pulls", params=params, per_page=100, max_pages=100)

    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for pr in prs:
        merged_at = pr.get("merged_at")
        merge_sha = pr.get("merge_commit_sha")
        if merged_at and merge_sha:
            sha = str(merge_sha).strip()
            if sha and sha not in seen:
                out.append(pr)
                seen.add(sha)

    logger.info("Collected %d merged PR SHAs (non-null) from %s.", len(out), repo)
    return out


def _paginate_github(
    endpoint: str, params: dict | None = None, *, per_page: int = 100, max_pages: int = 100
) -> list[dict]:
    """
    Paginate GitHub API endpoint using get_github_metadata.
    Returns all items from all pages as a list.
    """
    all_items: list[dict] = []
    page = 1

    tqdm_iter = tqdm(total=max_pages, desc="Paginating GitHub", unit="page")
    while page <= max_pages:
        merged_params = dict(params or {})
        merged_params.update({"per_page": str(per_page), "page": str(page)})

        data = get_github_metadata(endpoint, params=merged_params)

        if data is None or not isinstance(data, list) or not data:
            break

        all_items.extend(data)

        # If fewer than per_page, we've hit the end
        if len(data) < per_page:
            break

        page += 1
        tqdm_iter.update(1)

    return all_items


def _list_merged_prs(repo_name: str, *, per_page: int = 100, max_pages: int = 100) -> list[dict]:
    """
    Returns a list of PR dicts (subset of fields) for PRs that are actually merged.
    Uses /repos/{repo}/pulls?state=closed and filters merged_at != null.
    """
    endpoint = f"repos/{repo_name}/pulls"
    params = {"state": "closed", "sort": "updated", "direction": "desc"}

    prs = _paginate_github(endpoint, params=params, per_page=per_page, max_pages=max_pages)

    # Filter to only merged PRs
    merged_prs: list[dict] = []
    for pr in prs:
        if pr.get("merged_at"):
            merged_prs.append({
                "number": pr.get("number"),
                "title": pr.get("title") or "",
                "body": pr.get("body") or "",
                "merge_commit_sha": pr.get("merge_commit_sha"),
            })

    return merged_prs


def _ensure_merge_sha(repo_name: str, pr_number: int, merge_sha: str | None) -> str | None:
    """
    Ensure we have a valid merge SHA. If the supplied SHA is missing or 404s,
    fall back to the last commit in the PR.
    """
    if merge_sha:
        data = get_github_metadata(f"repos/{repo_name}/commits/{merge_sha}")
        if data is not None:
            return merge_sha
        logger.debug("merge_commit_sha %s not found for PR #%d; falling back.", merge_sha, pr_number)

    # Fallback: use the last commit from the PR commits list
    commits_endpoint = f"repos/{repo_name}/pulls/{pr_number}/commits"
    commits = _paginate_github(commits_endpoint, params={}, per_page=100, max_pages=100)

    if commits:
        last = commits[-1]
        sha = (last.get("sha") or "").strip() or None
        if sha:
            return sha

    return None


def _pr_change_summary(repo_name: str, pr_number: int) -> str:
    """
    Build a concise change summary from /pulls/{number}/files.
    """
    endpoint = f"repos/{repo_name}/pulls/{pr_number}/files"
    files = _paginate_github(endpoint, params={}, per_page=100, max_pages=100)

    lines: list[str] = []
    for f in files:
        filename = f.get("filename", "")
        status = f.get("status", "")
        additions = f.get("additions", 0)
        deletions = f.get("deletions", 0)
        changes = f.get("changes", additions + deletions)
        lines.append(f"{status}\t{filename}\t+{additions}/-{deletions}\tΔ{changes}")

    return "\n".join(lines)


def find_perf_commits_online(
    repo_name: str,
    n_workers: int = -1,
) -> list[str]:
    """
    Online replacement for find_perf_commits using GitHub REST API:
    - Lists merged PRs from GitHub (public repo assumed).
    - For each PR, ensures a valid merge SHA (falls back to last PR commit if needed).
    - Builds a change summary from PR files.
    - Classifies with PerfClassifier and returns the merge SHAs deemed performance-related.
    """
    perf_classifier = PerfClassifier()

    # 1) Fetch merged PRs
    prs = _list_merged_prs(repo_name)
    if not prs:
        logger.info("No merged PRs found for %s.", repo_name)
        return []

    # 2) Build (merge_sha, message, change_summary) tuples (in parallel for the summaries)
    tuples: list[tuple[str, str, str]] = []
    max_workers = None if n_workers is None or n_workers < 1 else n_workers

    def _build_one(pr: dict) -> tuple[str, str, str] | None:
        number = int(pr["number"])
        merge_sha = _ensure_merge_sha(repo_name, number, pr.get("merge_commit_sha"))
        if not merge_sha:
            return None
        # Prefer PR title/body as the "message" for the classifier (stable across merge strategies)
        msg = (pr.get("title") or "").strip()
        body = (pr.get("body") or "").strip()
        if body:
            msg = f"{msg}\n\n{body}".strip()
        summary = _pr_change_summary(repo_name, number)
        return (merge_sha, msg, summary)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_build_one, pr) for pr in prs]
        for f in tqdm(as_completed(futures), total=len(futures), desc="Preparing PR data"):
            with contextlib.suppress(Exception):
                tup = f.result()
                if tup:
                    tuples.append(tup)

    if not tuples:
        return []

    # 3) Classify
    merge_shas = batch_classify_commits(perf_classifier, repo_name, tuples, n_workers)  # type: ignore[arg-type]

    # Stable, deterministic order (by SHA string)
    return sorted(merge_shas)
