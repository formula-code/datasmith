from __future__ import annotations

import concurrent.futures
import typing
import urllib.parse
from collections.abc import Generator

import pandas as pd
import tqdm

from datasmith.execution.utils import _get_commit_info
from datasmith.scrape.utils import _parse_commit_url
from datasmith.utils import _get_codecov_metadata


def _normalize_path(p: str) -> str:
    # Codecov tends to use repo-relative paths; ensure consistent normalization.
    return p.lstrip("./")


def _commit_file_coverages(owner: str, repo: str, sha: str) -> dict[str, float | None]:
    """
    Return a mapping {path -> coverage} for the entire commit using a single totals call.
    Falls back to empty dict if unavailable.
    """
    base_endpoint = f"/{owner}/repos/{repo}"

    totals = _get_codecov_metadata(
        endpoint=f"{base_endpoint}/totals",
        params={"sha": sha},
    )
    if not totals:
        return {}

    # The 'totals' response includes coverage broken down by file.
    # Empirically the shape is either:
    #   {'files': [{'path': 'a/b.py', 'totals': {'coverage': 83.33}}, ...], ...}
    # or similar; we defensively extract common shapes.
    out: dict[str, float | None] = {}

    files = totals.get("files") or totals.get("file_totals") or []
    for item in files:
        path = item.get("path") or item.get("name") or item.get("filename")
        if not path:
            continue
        t = item.get("totals") or item
        cov = t.get("coverage")
        out[_normalize_path(path)] = float(cov) if cov is not None else None

    return out


def _file_coverage_with_fallback(owner: str, repo: str, sha: str, path: str) -> float | None:
    """
    First look up from the per-commit cache; if missing, fall back to the single-file endpoint.
    """
    path_n = _normalize_path(path)
    cached = _commit_file_coverages(owner, repo, sha).get(path_n)
    if cached is not None:
        return cached

    # Fallback: dedicated file report (one call, still much less frequent than before)
    report = _get_codecov_metadata(
        endpoint=f"/{owner}/repos/{repo}/file_report/{urllib.parse.quote(path_n, safe='/')}",
        params={"sha": sha},
    )
    if report and "totals" in report:
        cov = report["totals"].get("coverage")
        if cov is not None:
            return float(cov)
    return None


def _iter_commit_coverage(
    commit_url: str, only: list[str] | None = None
) -> Generator[tuple[str, float | None], None, None]:
    """Yield (path, coverage) for every changed file in *commit_url* with a single Codecov call per commit."""
    owner, repo, sha = _parse_commit_url(commit_url)
    commit_info = _get_commit_info(repo_name=f"{owner}/{repo}", commit_sha=sha)
    files = [f for f in commit_info["files_changed"].split("\n") if f]

    if only:
        files = [f for f in files if any(pat in f for pat in only)]
    if not files:
        return

    # Pull the whole commit's file coverage once:
    coverage_map = _commit_file_coverages(owner, repo, sha)

    for path in files:
        cov = coverage_map.get(_normalize_path(path))
        if cov is None:
            # Rare: path may be missing in totals due to filters/flags; fall back once.
            cov = _file_coverage_with_fallback(owner, repo, sha, path)
        yield path, cov


def generate_coverage_dataframe(
    breakpoints_df: pd.DataFrame,
    index_data: dict[str, typing.Any],
    *,
    commit_urls: dict[str, str] | None = None,
    only: list[str] | None = None,
) -> pd.DataFrame:
    """Retrieve per-file coverage numbers for **all** commits referenced efficiently."""

    base = index_data["show_commit_url"].rstrip("/")
    if base == "#" and (commit_urls is not None) and (index_data["project_url"] in commit_urls):
        base = commit_urls[index_data["project_url"]]
    elif base == "#":
        raise ValueError(
            f"Base URL '{base}' is not set and {index_data['project_url']} is not in commit_urls. Please provide a valid base URL."
        )

    # Include both ground-truth and observed hashes if present
    url_cols = [c for c in breakpoints_df.columns if c.endswith("hash")]

    # Build a de-duplicated list of commit URLs
    seen: set[str] = set()
    filtered: list[tuple[str, str]] = []
    for col in url_cols:
        urls = (base + "/" + breakpoints_df[col].dropna().astype(str)).tolist()
        for u in urls:
            if u and u not in seen:
                seen.add(u)
                filtered.append((col, u))

    outputs = []

    def process_commit(args: tuple[str, str]) -> list[list[typing.Any]]:
        typ, url = args
        result = []
        for path, cov in _iter_commit_coverage(url, only):
            result.append([typ, url, path, cov])
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_commit, item) for item in filtered]
        for future in tqdm.tqdm(
            concurrent.futures.as_completed(futures), total=len(futures), desc="Codecov", unit="commit"
        ):
            outputs.extend(future.result())

    return pd.DataFrame(outputs, columns=["typ", "url", "path", "coverage"])
