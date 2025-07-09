"""
Detect online and offline dashboards. Assumes access to data/bigquery_repos.csv
"""

import typing

import pandas as pd
from tqdm.auto import tqdm

from datasmith.scrape.utils import _extract_repo_full_name
from datasmith.utils import _get_github_metadata


def _get_repo_metadata(full_name: str | None) -> dict[str, typing.Any] | None:
    """
    Call the GitHub REST API for ``owner/repo`` and return the JSON.
    Falls back to *None* when the repo cannot be reached.
    """
    if not full_name:
        return None

    metadata: dict[str, typing.Any] | None = _get_github_metadata(endpoint=f"/repos/{full_name}")
    return metadata


def is_forked(url: str) -> bool:
    """Return *True* if the repository is a fork of another repo."""
    meta = _get_repo_metadata(_extract_repo_full_name(url))
    return bool(meta and meta.get("fork"))


def is_archived(url: str) -> bool:
    """Return *True* if the repository is archived or disabled."""
    meta = _get_repo_metadata(_extract_repo_full_name(url))
    return bool(meta and (meta.get("archived") or meta.get("disabled")))


def is_accessible(url: str) -> bool:
    """Return *True* if the repository exists and the GitHub API returned 200."""
    meta = _get_repo_metadata(_extract_repo_full_name(url))
    return meta is not None


def watchers_count(url: str) -> int | None:
    """Return the number of *watchers* (subscribers) the repository has."""
    meta = _get_repo_metadata(_extract_repo_full_name(url))
    return meta.get("subscribers_count") if meta else None


def stars_count(url: str) -> int | None:
    """Return the number of *stars* (stargazers) the repository has."""
    meta = _get_repo_metadata(_extract_repo_full_name(url))
    return meta.get("stargazers_count") if meta else None


def _repo_summary(url: str) -> dict:
    """Build a small dict with all the new columns for a single repo.

    Keys
    ----
    is_accessible, is_fork, is_archived,
    fork_parent (str|None), forked_at (ISO8601|None),
    watchers (int|None), stars (int|None)
    """
    meta = _get_repo_metadata(_extract_repo_full_name(url))
    if not meta:  # unreachable or HTTP 404, 403, etc.
        return {
            "is_accessible": False,
            "is_fork": None,
            "is_archived": None,
            "fork_parent": None,
            "forked_at": None,
            "watchers": None,
            "stars": None,
        }

    is_fork = bool(meta.get("fork"))
    return {
        "is_accessible": True,
        "is_fork": is_fork,
        "is_archived": bool(meta.get("archived") or meta.get("disabled")),
        "fork_parent": meta["parent"]["full_name"] if is_fork and "parent" in meta else None,
        "forked_at": meta.get("created_at") if is_fork else None,
        # GitHub API semantics: ``watchers_count`` counts *stargazers*,
        # while ``subscribers_count`` counts users who opted-in for notifications.
        # We expose the latter as "watchers" to avoid confusion.
        "watchers": meta.get("subscribers_count"),
        "stars": meta.get("stargazers_count"),
    }


def enrich_repos(df: pd.DataFrame, url_col: str = "repo_name", *, show_progress: bool = True) -> pd.DataFrame:
    """
    Return *df* **plus** five new metadata columns.
    Nothing is filtered out.
    """
    iterable = df[url_col]
    tqdm_iter = tqdm(iterable, desc="Enriching repos", leave=False) if show_progress else iterable
    extra = [_repo_summary(url) for url in tqdm_iter]
    return pd.concat([df.reset_index(drop=True), pd.DataFrame(extra)], axis=1)


def filter_dashboards(df: pd.DataFrame, url_col: str = "repo_name", *, show_progress: bool = True) -> pd.DataFrame:
    """
    Add the extra columns **and** keep only rows that are:
    - accessible,
    - not forks,
    - not archived.
    """
    enriched = enrich_repos(df, url_col=url_col, show_progress=show_progress)
    if not len(enriched):
        raise ValueError("Dataframe empty")  # noqa: TRY003
    return (
        enriched[
            (enriched["is_accessible"].fillna(False))
            & (~enriched["is_fork"].fillna(True))
            & (~enriched["is_archived"].fillna(True))
        ]
        .copy()
        .reset_index(drop=True)
    )
