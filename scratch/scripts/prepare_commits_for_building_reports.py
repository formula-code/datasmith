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
from datasmith.execution.filter_commits import crude_perf_filter
from datasmith.execution.resolution import analyze_commit
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import get_patch_from_diff_url, make_task

logger = configure_logging()


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
    parser.add_argument(
        "--only-filter",
        action="store_true",
        default=False,
        help="Only perform the filtering step and save the filtered parquet; skip analysis and patch fetching.",
    )
    args = parser.parse_args()

    # Initialize project env (matches the notebook's setup_environment() call)
    setup_environment()

    # Load commits
    commit_df = pd.read_parquet(args.input)

    # Drop commits with missing patch before further work
    commit_df = commit_df.dropna(subset=["patch"])  # downstream assumes patch exists
    logger.info(f"[load] commits after dropping rows without 'patch': {commit_df.shape}")

    # Tokenize patches after the early filter to avoid wasted work
    enc = tiktoken.get_encoding(args.token_encoding)
    # remove special filtered words from patches.
    commit_df = commit_df[~commit_df["patch"].str.contains("endoftext")]
    patch_tokens = enc.encode_batch(commit_df["patch"].fillna("").tolist(), num_threads=args.max_workers)
    commit_df["n_patch_tokens"] = [len(toks) for toks in patch_tokens]

    # Filter out commits unlikely to be perf-related
    logger.info(f"[filter] pre-filter shape: {commit_df.shape}")
    filtered_df = crude_perf_filter(commit_df, filter_repos=args.filter_repos)
    logger.info(f"[filter] post-filter shape: {filtered_df.shape}")
    # Filter out commits with missing issues in the timeline.
    # Early filter: keep only PRs that mention at least one issue in their timeline.
    # Uses GitHub issues timeline API via issue_timeline(owner, repo, pr_number).
    # Run in parallel to reduce wall-clock latency.
    # if {"repo_name", "pr_number"}.issubset(filtered_df.columns):
    #     # Build unique (repo_name, pr_number) keys
    #     # key_series = filtered_df[["repo_name", "pr_number"]].dropna()
    #     # Ensure pr_number are ints
    #     # try:
    #     #     key_series = key_series.assign(pr_number=key_series["pr_number"].astype(int))
    #     # except Exception:
    #     #     key_series = key_series[pd.to_numeric(key_series["pr_number"], errors="coerce").notna()]
    #     #     key_series = key_series.assign(pr_number=key_series["pr_number"].astype(int))

    #     # unique_keys: list[tuple[str, int]] = list({(r, int(n)) for r, n in key_series.itertuples(index=False)})

    #     def _mentions_any_issue(row: pd.Series) -> tuple[tuple[str, int], bool]:
    #         repo_full, pr_num = row["repo_name"], row["pr_number"]
    #         try:
    #             owner, repo = repo_full.split("/", 1)
    #         except ValueError:
    #             return (repo_full, pr_num), False
    #         try:
    #             timeline_events = issue_timeline(owner, repo, int(pr_num))
    #             comments_events = list(filter(lambda x: not _is_bot_comment(x), issue_comments(owner, repo, int(pr_num))))
    #             events = timeline_events + comments_events
    #         except Exception:
    #             return (repo_full, pr_num), False
    #         if not events:
    #             return (repo_full, pr_num), False
    #         for ev in events:
    #             src = (ev or {}).get("source") or {}
    #             src_issue = src.get("issue") or {}
    #             src_type = src.get("type")
    #             # Prefer explicit type tag; fallback to absence of pull_request marker
    #             if src_type == "issue":
    #                 return (repo_full, pr_num), True
    #             if isinstance(src_issue, dict) and "pull_request" not in src_issue:
    #                 return (repo_full, pr_num), True
    #         return (repo_full, pr_num), False

    #     logger.info("[filter] Checking PR timelines for issue mentions across %d unique PRs", len(unique_keys))
    #     results: list[Any] = _thread_map(
    #         _mentions_any_issue,
    #         filtered_df.iterrows(),
    #         max_workers=args.max_workers,
    #         desc="Checking PR timelines",
    #         show_progress=True,
    #     )
    #     valid_results: list[tuple[tuple[str, int], bool]] = [
    #         r for r in results if isinstance(r, tuple) and len(r) == 2 and isinstance(r[0], tuple)
    #     ]
    #     mention_map: dict[tuple[str, int], bool] = dict(valid_results)
    #     # Map back to rows; rows without a mapping are treated as False
    #     def _row_keep(row: pd.Series) -> bool:
    #         try:
    #             key = (row["repo_name"], int(row["pr_number"]))
    #         except Exception:
    #             return False
    #         return mention_map.get(key, False)

    #     pre_shape = filtered_df.shape
    #     filtered_df = filtered_df[filtered_df.apply(_row_keep, axis=1)].reset_index(drop=True)
    #     logger.info(
    #         "[filter] PRs mentioning at least one issue in timeline: %d → %d",
    #         pre_shape[0],
    #         filtered_df.shape[0],
    #     )
    # else:
    #     logger.warning(
    #         "[filter] Missing 'repo_name' or 'pr_number' columns; skipping issue timeline filter. Columns: %s",
    #         list(filtered_df.columns),
    #     )

    filtered_df_path = args.input.parent / f"{args.input.stem}_2.parquet"
    filtered_df_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_df.to_parquet(filtered_df_path, index=False)
    logger.info(f"[save] Saved filtered DataFrame to {filtered_df_path}")

    if args.only_filter:
        logger.info("Exiting after filtering step as --only-filter was specified.")
        return

    # Analyze whether dependencies can be installed, resolution strategy, etc.
    pairs: list[tuple[str, str]] = []
    for _, row in filtered_df.iterrows():
        pairs.append((row["pr_base"]["sha"], row["repo_name"]))
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
        filtered_extended_df["diff"] = patches

    # Final cleanups: drop rows with missing diff/container
    filtered_extended_df = filtered_extended_df.dropna(subset=["diff", "container_name"])
    logger.info(f"[final] shape after dropping rows with missing diff/container: {filtered_extended_df.shape}")
    # rename diff -> patch and patch -> original_patch for clarity
    filtered_extended_df = filtered_extended_df.rename(columns={"diff": "patch", "patch": "original_patch"})

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
