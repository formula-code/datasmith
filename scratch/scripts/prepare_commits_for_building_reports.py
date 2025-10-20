#!/usr/bin/env python3
"""
Process and filter commit data, analyze installability, enrich with PR/base info,
optionally fetch patches from the GitHub diff API, and save a cleaned parquet.

This is a conversion of an IPython notebook to a CLI script. Notable changes:
- Replaced Dask Delayed with concurrent.futures.ThreadPoolExecutor + tqdm.
- Added argparse for inputs/outputs and tuning.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
import tiktoken
from tqdm.auto import tqdm

from datasmith import setup_environment
from datasmith.docker.context import Task
from datasmith.execution.filter_commits import crude_perf_filter
from datasmith.execution.resolution import analyze_commit
from datasmith.logging_config import configure_logging
from datasmith.utils import _get_github_metadata

logger = configure_logging()


# -----------------------------
# Helpers
# -----------------------------
def date_to_unix_timestamp(date_str: str) -> int:
    from datetime import datetime, timezone

    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def make_task(row: pd.Series, tag: str = "run") -> str:
    owner, repo = row["repo_name"].split("/")
    sha = row["pr_merge_commit_sha"]
    commit_date = date_to_unix_timestamp(row["pr_merged_at"])
    return Task(owner=owner, repo=repo, sha=sha, commit_date=commit_date).with_tag(tag).get_image_name()


def get_patch_from_diff_url(row: pd.Series) -> str | None:
    repo_name = row["repo_name"]
    pull_number = row["pr_number"]
    endpoint = f"/repos/{repo_name}/pulls/{pull_number}"
    diff_text = _get_github_metadata(endpoint=endpoint, params={"diff_api": "true"})
    if not diff_text or "diff" not in diff_text:
        return None
    return diff_text["diff"]


def _thread_map(
    fn,
    items: Sequence[Any],
    max_workers: int,
    desc: str,
    show_progress: bool = True,
) -> list[Any]:
    """
    Run fn(item) over items in a thread pool, returning results in input order.
    Any exceptions are caught and yield None for that item.
    """
    results: list[Any] = [None] * len(items)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fn, item): idx for idx, item in enumerate(items)}
        iterator: Iterable = as_completed(futures)
        if show_progress:
            iterator = tqdm(iterator, total=len(futures), desc=desc)
        for fut in iterator:
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception:
                # You could log/print here if helpful; keep quiet to avoid noisy output.
                results[idx] = None
    return results


def safe_analyze_commit(pair: tuple[str, str]) -> dict[str, Any]:
    """
    Wrapper to call analyze_commit(sha, repo_name) safely.
    Returns {} on error to keep DataFrame construction simple.
    """
    sha, repo_name = pair
    try:
        return analyze_commit(sha, repo_name) or {}
    except Exception:
        return {}


# -----------------------------
# Main pipeline
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Analyze and enrich commit data, then save a filtered parquet.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("scratch/artifacts/pipeflush/merge_commits_filtered.parquet"),
        help="Input parquet with commit data (expects columns like 'patch', 'sha', 'repo_name', etc.).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("scratch/artifacts/pipeflush/merge_commits_filtered_with_patch.parquet"),
        help="Output parquet path.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=84,
        help="Max worker threads for analysis and patch fetching.",
    )
    parser.add_argument(
        "--token-encoding",
        type=str,
        default="o200k_base",
        help="tiktoken encoding to use for patch tokenization (if available).",
    )
    parser.add_argument(
        "--filter-repos",
        action="store_true",
        default=False,
        help="Pass filter_repos=True to crude_perf_filter (default False to mirror the notebook).",
    )
    parser.add_argument(
        "--fetch-patches",
        action="store_true",
        default=False,
        help="Fetch fresh patch text from the GitHub diff API for each PR and overwrite 'patch' column.",
    )
    parser.add_argument(
        "--container-tag",
        type=str,
        default="run",
        help="Tag to apply when generating container image names.",
    )
    args = parser.parse_args()

    # Initialize project env (matches the notebook's setup_environment() call)
    setup_environment()

    # Load commits
    commit_df = pd.read_parquet(args.input)

    # Drop commits with missing patch before tokenization, as in the notebook
    commit_df = commit_df.dropna(subset=["patch"])
    logger.info(f"[load] commits after dropping rows without 'patch': {commit_df.shape}")

    enc = tiktoken.get_encoding(args.token_encoding)
    patch_tokens = enc.encode_batch(
        commit_df["patch"].fillna("").tolist(),
        num_threads=args.max_workers,
    )
    commit_df["n_patch_tokens"] = [len(toks) for toks in patch_tokens]

    # Filter out commits unlikely to be perf-related
    logger.info(f"[filter] pre-filter shape: {commit_df.shape}")
    filtered_df = crude_perf_filter(commit_df, filter_repos=args.filter_repos)
    logger.info(f"[filter] post-filter shape: {filtered_df.shape}")

    # save filtered_df
    filtered_df_path = args.output.parent / "merge_commits_filtered_2.parquet"
    filtered_df_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_df.to_parquet(filtered_df_path, index=False)
    logger.info(f"[save] Saved filtered DataFrame to {filtered_df_path}")

    # Analyze whether dependencies can be installed, resolution strategy, etc.
    # (replaces Dask delayed/analyze_commit with ThreadPool + tqdm)
    pairs: list[tuple[str, str]] = filtered_df[["sha", "repo_name"]].itertuples(index=False, name=None)
    analysis_dicts: list[dict[str, Any]] = _thread_map(
        safe_analyze_commit,
        list(pairs),
        max_workers=args.max_workers,
        desc="Analyzing commits",
    )

    # Combine analysis into DataFrame and merge
    extended_analysis = pd.DataFrame(analysis_dicts)
    filtered_extended_df = pd.concat(
        [filtered_df.reset_index(drop=True), extended_analysis.add_prefix("analysis_")],
        axis=1,
    )

    # Apply the same row filters as notebook
    # - Keep only rows where analysis_can_install is True and pr_base is present
    # - Drop rows whose analysis_resolution_strategy starts with "unresolved"
    filtered_extended_df = filtered_extended_df.dropna(subset=["analysis_can_install", "pr_base"])
    filtered_extended_df = filtered_extended_df.query("analysis_can_install")
    filtered_extended_df = filtered_extended_df[
        ~filtered_extended_df["analysis_resolution_strategy"].fillna("").str.startswith("unresolved")
    ]

    # Expand pr_base info into columns (repo fields + base sha)
    def _repo_info(pr_base: dict[str, Any]) -> dict[str, Any]:
        repo_part = pr_base.get("repo", {}) or {}
        sha_part = {"sha": pr_base.get("sha")}
        return {**repo_part, **sha_part}

    repo_info = filtered_extended_df["pr_base"].apply(lambda d: pd.Series(_repo_info(d)))
    filtered_extended_df = pd.concat([filtered_extended_df, repo_info.add_prefix("pr_base_")], axis=1)

    logger.info(f"[analysis] shape after analysis filters and pr_base expansion: {filtered_extended_df.shape}")

    # Build container names
    filtered_extended_df["container_name"] = filtered_extended_df.apply(
        lambda r: make_task(r, tag=args.container_tag), axis=1
    )

    # Optionally replace/refresh patch content from GitHub diff API
    if args.fetch_patches:
        rows = [row for _, row in filtered_extended_df.iterrows()]
        patches: list[str | None] = _thread_map(
            get_patch_from_diff_url,
            rows,
            max_workers=args.max_workers,
            desc="Fetching patches",
        )
        filtered_extended_df["patch"] = patches

    # Final cleanups: drop rows with missing patch/container
    filtered_extended_df = filtered_extended_df.dropna(subset=["patch", "container_name"])

    # Drop columns that cause parquet issues (keep behavior but ignore if missing)
    to_drop = [
        "analysis_excluded_missing_on_pypi",
        "analysis_excluded_exists_incompatible",
        "analysis_excluded_other",
    ]
    filtered_extended_df = filtered_extended_df.drop(columns=to_drop, errors="ignore")

    # Save parquet
    args.output.parent.mkdir(parents=True, exist_ok=True)
    filtered_extended_df.to_parquet(args.output, index=False)
    logger.info(f"[save] Saved to {args.output} | final shape: {filtered_extended_df.shape}")


if __name__ == "__main__":
    main()
