from __future__ import annotations

import typing
import urllib.parse
from collections.abc import Generator

import pandas as pd
import tqdm

from datasmith.execution.utils import _get_commit_info
from datasmith.scrape.utils import _parse_commit_url
from datasmith.utils import _get_codecov_metadata


def codecov_file_coverage(owner: str, repo: str, sha: str, path: str) -> float | None:
    """Return *path*'s line-coverage for *sha*, or *None* if unavailable."""
    base_endpoint = f"/{owner}/repos/{repo}"

    # (0) try the full endpoint first, which is more efficient
    commit_info = _get_codecov_metadata(endpoint=f"{base_endpoint}/totals", params={"sha": sha})
    if commit_info and "totals" in commit_info:
        coverage = commit_info["totals"].get("coverage")
        if coverage is not None:
            return float(coverage)

    # (1) try the totals endpoint first, which is more efficient
    # set logging level to debug
    totals = _get_codecov_metadata(endpoint=f"{base_endpoint}/totals", params={"sha": sha, "path": path})
    if totals and "totals" in totals:
        coverage = totals["totals"].get("coverage")
        if coverage is not None:
            return float(coverage)

    # (2) fallback: dedicated file-report endpoint
    report = _get_codecov_metadata(
        endpoint=f"{base_endpoint}/file_report/{urllib.parse.quote(path, safe='/')}", params={"sha": sha}
    )
    if report and "totals" in report:
        coverage = report["totals"].get("coverage")
        if coverage is not None:
            return float(coverage)

    return None


def _iter_commit_coverage(
    commit_url: str,
    only: list[str] | None = None,
) -> Generator[tuple[str, float | None], None, None]:
    """Yield (path, coverage) pairs for every changed file in *commit_url*."""
    owner, repo, sha = _parse_commit_url(commit_url)
    commit_info = _get_commit_info(repo_name=f"{owner}/{repo}", commit_sha=sha)
    files = commit_info["files_changed"].split("\n")

    if only:
        files = [f for f in files if any(pat in f for pat in only)]
    if not files:
        return  # nothing to yield

    for path in files:
        cov = codecov_file_coverage(owner, repo, sha, path)
        yield path, cov


def generate_coverage_dataframe(
    breakpoints_df: pd.DataFrame,
    index_data: dict[str, typing.Any],
    *,
    only: list[str] | None = None,
) -> pd.DataFrame:
    """Retrieve per-file coverage numbers for **all** commits referenced."""

    base = index_data["show_commit_url"].rstrip("/")

    # Include both ground-truth and observed hashes if present
    url_cols = [c for c in breakpoints_df.columns if c.endswith("hash")]
    all_urls: list[tuple[str, str]] = []
    for col in url_cols:
        urls = (base + "/" + breakpoints_df[col].dropna().astype(str)).tolist()
        all_urls.extend([(col, u) for u in urls])

    # de-duplicate
    seen: set[str] = set()
    filtered = []
    for typ, u in all_urls:
        if u not in seen:
            seen.add(u)
            filtered.append((typ, u))

    outputs = []
    for typ, url in tqdm.tqdm(filtered, desc="Codecov", unit="commit"):
        for path, cov in _iter_commit_coverage(url, only):
            outputs.append([typ, url, path, cov])

    return pd.DataFrame(outputs, columns=["typ", "url", "path", "coverage"])
