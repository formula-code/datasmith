#!/usr/bin/env python3
"""
Collect performance-related commits using the new ReportBuilder workflow.

This updates the older classifier-only script to:
  - read the prepared parquet with PR + patch data (see prepare_commits_for_building_reports.py)
  - run ReportBuilder to detect performance PRs (optionally with classification)
  - emit a parquet filtered to performance commits and a companion .txt of SHAs

Example:
  uv run python scratch/scripts/collect_perf_commits.py \
    --commits scratch/artifacts/pipeflush/merge_commits_filtered_with_patch.parquet \
    --outfile scratch/artifacts/pipeflush/perf_commits
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm.auto import tqdm

from datasmith.agents.problem_extractor import ProblemExtraction
from datasmith.logging_config import configure_logging
from datasmith.scrape.report_builder import ReportBuilder

logger = configure_logging(stream=open(Path(__file__).with_suffix(".log"), "w"), level=20)  # noqa: SIM115


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Collect perf-related commits using ReportBuilder",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--commits",
        type=Path,
        required=True,
        help=(
            "Input file containing commit/PR data. Use the parquet emitted by "
            "prepare_commits_for_building_reports.py (includes 'patch', 'pr_url', etc.)."
        ),
    )
    p.add_argument(
        "--outfile",
        type=Path,
        required=True,
        help=(
            "Output path prefix (no suffix). We create '<outfile>.parquet' with filtered rows and "
            "'<outfile>.txt' with SHAs."
        ),
    )
    p.add_argument("--max-workers", type=int, default=-1, help="Number of parallel workers. -1 = sequential.")

    # ReportBuilder toggles
    p.add_argument("--enable-llm-backends", action="store_true", default=True, help="Enable LLM backends.")
    p.add_argument(
        "--summarize-llm",
        action="store_true",
        default=True,
        help="Use LLM to extract discussion/problem statements (slower).",
    )
    p.add_argument(
        "--add-classification",
        action="store_true",
        default=True,
        help="Add performance classification for detected performance commits.",
    )
    p.add_argument(
        "--filter-performance-only",
        action="store_true",
        default=True,
        help="Filter out non-performance PRs using PerfClassifier.",
    )
    p.add_argument(
        "--include-bot-comments",
        action="store_true",
        default=False,
        help="Include bot comments (codecov/coveralls/etc.) in hints section.",
    )
    p.add_argument("--anonymize-output", action="store_true", default=False, help="Anonymize GitHub entities in text.")
    p.add_argument("--max-links-to-follow", type=int, default=60, help="Max linked resources to follow from comments.")
    p.add_argument(
        "--model-name",
        type=str,
        default="local/meta-llama/Llama-3.3-70B-Instruct",
        help="LLM model name for backends (ReportBuilder).",
    )
    p.add_argument(
        "--sample-size",
        type=int,
        default=-1,
        help="If >0, randomly sample this many rows from input for processing (for testing).",
    )
    return p.parse_args()


def _load_input(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    # Backward-compatibility for older JSONL inputs
    return pd.read_json(path, lines=True)


def _build_single(rb: ReportBuilder, row: pd.Series) -> dict[str, Any]:
    """
    Run ReportBuilder on a single row; return a small dict with classification fields.
    Any exceptions are caught and produce a default negative classification.
    """
    try:
        pr_dict = row.to_dict()
        result = rb.build(pr_dict=pr_dict)
    except Exception as e:  # Defensive: keep the batch running
        logger.warning(f"Report build failed for row sha={row.get('sha')}: {e}")
        return {
            "is_performance_commit": False,
            "classification": "",
            "difficulty": "",
            "classification_reason": f"error: {e}",
            "classification_confidence": None,
        }
    else:
        return result.__dict__


def main(args: argparse.Namespace) -> None:
    df = _load_input(args.commits)
    if args.sample_size and args.sample_size > 0:
        # df = df.sample(n=args.sample_size, random_state=42).reset_index(drop=True)
        df = df.head(args.sample_size).reset_index(drop=True)

    # Expect the prepared parquet with PR metadata
    needed_cols = {"sha", "repo_name", "pr_url", "patch"}
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        logger.warning(
            "Input is missing expected columns %s; proceeding but ReportBuilder may fetch extra metadata via API.",
            missing,
        )

    # Keep rows with a PR URL present (these are PR merges in our prepared parquet)
    pre_rows = len(df)
    df = df.dropna(subset=["pr_url"]) if "pr_url" in df.columns else df
    logger.info("Loaded %d rows; %d have pr_url", pre_rows, len(df))

    # Initialize ReportBuilder (configures LLM backends when enabled)
    rb = ReportBuilder(
        enable_llm_backends=args.enable_llm_backends,
        summarize_llm=args.summarize_llm,
        add_classification=args.add_classification,
        filter_performance_only=args.filter_performance_only,
        include_bot_comments=args.include_bot_comments,
        anonymize_output=args.anonymize_output,
        max_links_to_follow=args.max_links_to_follow,
        model_name=args.model_name,
    )

    # Run builder across rows
    records: list[dict[str, Any]] = []
    if args.max_workers and args.max_workers > 0:
        logger.info("Classifying with %d workers (may be limited by rate limits)", args.max_workers)
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            fut2idx = {ex.submit(_build_single, rb, row): idx for idx, (_, row) in enumerate(df.iterrows())}
            # for fut in tqdm(as_completed(fut2idx), total=len(fut2idx), desc="Building reports"):
            #     records.append(fut.result())
            iterator = tqdm(as_completed(fut2idx), total=len(fut2idx), desc="Building reports")
            n_perf_commits = 0
            for fut in iterator:
                result = fut.result()
                logger.info("Got result: %s", result)
                records.append(result)
                if result.get("is_performance_commit"):
                    n_perf_commits += 1
                iterator.set_postfix({"is_perf": n_perf_commits})
        # Preserve original order
        # We created futures in order; 'records' order is arbitrary, so rebuild by index mapping
        ordered: list[dict[str, Any]] = [None] * len(fut2idx)  # type: ignore[list-item]
        for fut, idx in fut2idx.items():
            try:
                ordered[idx] = fut.result()
            except Exception:
                ordered[idx] = {
                    "is_performance_commit": False,
                    "classification": "",
                    "difficulty": "",
                    "classification_reason": "error",
                    "classification_confidence": None,
                }
        records = ordered  # type: ignore[assignment]
    else:
        logger.info("Classifying sequentially")
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Building reports"):
            records.append(_build_single(rb, row))

    enrich = pd.DataFrame(records)
    enrich["all_data"] = enrich["all_data"].apply(
        lambda d: {k: v.__dict__ if isinstance(v, ProblemExtraction) else v for k, v in d.items()}
        if isinstance(d, dict)
        else d
    )
    enrich["problem_sections"] = enrich["problem_sections"].apply(
        lambda pe: pe.__dict__ if isinstance(pe, ProblemExtraction) else pe
    )
    df_enriched = pd.concat([df.reset_index(drop=True), enrich], axis=1)

    # Save performance-related commits
    raw_out = args.outfile.with_suffix(".raw.parquet")
    perf_out = args.outfile.with_suffix(".parquet")

    df_enriched.to_parquet(raw_out, index=False)
    logger.info("Saved performance commits to %s", raw_out)
    perf_df = df_enriched[df_enriched["is_performance_commit"]].copy()
    logger.info("Detected %d performance commits (from %d total)", len(perf_df), len(df))

    perf_df.to_parquet(perf_out, index=False)
    logger.info("Saved filtered performance commits to %s", perf_out)


if __name__ == "__main__":
    main(parse_args())
