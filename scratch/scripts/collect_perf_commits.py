from __future__ import annotations

import argparse
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from datasmith.core.cache.decorators import cache_completion
from datasmith.core.storage import read_table, resolve_table_name, write_table
from datasmith.logging_config import configure_logging
from datasmith.scrape.report_builder import ReportBuilder

# Cache location for classification results (survives DSPy cache clearing)
_CACHE_DB = os.environ.get("CACHE_LOCATION", "cache.db")

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
        "--summarize-prs",
        action="store_true",
        default=True,
        help="Add LLM-based PR summarization (slower).",
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
        default="local/openai/gpt-oss-120b",
        help="LLM model name for backends (ReportBuilder).",
    )
    p.add_argument(
        "--sample-size",
        type=int,
        default=-1,
        help="If >0, randomly sample this many rows from input for processing (for testing).",
    )
    p.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="Disable SQLite-backed caching (forces fresh LLM classification).",
    )
    p.add_argument("--db", type=str, default=None, help="Pipeline SQLite DB path.")
    return p.parse_args()


def _load_input(path: Path, db_path: str | None = None) -> pd.DataFrame:
    if path.suffix == ".jsonl":
        return pd.read_json(path, lines=True)
    # Try DB table first, fall back to parquet
    return read_table(resolve_table_name(str(path)), db_path=db_path)


def _compute_cache_key(pr_dict: dict[str, Any]) -> str:
    """Compute a stable hash from the pr_dict for caching purposes."""
    # Use sha + repo_name + patch hash as the cache key
    sha = pr_dict.get("sha", "")
    repo_name = pr_dict.get("repo_name", "")
    patch = pr_dict.get("patch", "")
    key_str = f"{sha}:{repo_name}:{patch}"
    return hashlib.sha256(key_str.encode()).hexdigest()[:32]


@cache_completion(_CACHE_DB, "perf_classification")
def _cached_classify(
    cache_key: str,
    pr_dict: dict[str, Any],
    enable_llm_backends: bool,
    summarize_llm: bool,
    add_classification: bool,
    filter_performance_only: bool,
    include_bot_comments: bool,
    anonymize_output: bool,
    max_links_to_follow: int,
    model_name: str,
    bypass_cache: bool = False,
) -> dict[str, Any]:
    """
    Cached classification of a PR.

    The cache_key (hash of sha+repo+patch) ensures we invalidate on data changes.
    The ReportBuilder config args are included so different configs are cached separately.
    """
    rb = ReportBuilder(
        enable_llm_backends=enable_llm_backends,
        summarize_llm=summarize_llm,
        add_classification=add_classification,
        filter_performance_only=filter_performance_only,
        include_bot_comments=include_bot_comments,
        anonymize_output=anonymize_output,
        max_links_to_follow=max_links_to_follow,
        model_name=model_name,
    )
    result = rb.build(pr_dict=pr_dict)
    return result.__dict__


def _prepare_pr_dict(row: pd.Series) -> dict[str, Any]:  # noqa: C901
    """Prepare and sanitize a pr_dict from a DataFrame row."""
    pr_dict = row.to_dict()

    # Sanitize numpy/pandas objects to plain Python types expected by ReportBuilder
    def _sanitize(v: Any) -> Any:
        # Convert NaN to None
        try:
            if pd.isna(v):  # type: ignore[arg-type]
                return None
        except Exception:
            pass
        # Convert numpy arrays to lists (avoid ambiguous truthiness)
        if isinstance(v, np.ndarray):
            return v.tolist()
        # Convert pandas-like Timestamp to ISO string
        if hasattr(v, "isoformat") and callable(v.isoformat):
            try:
                return v.isoformat()
            except Exception:
                return str(v)
        return v

    pr_dict = {k: _sanitize(v) for k, v in pr_dict.items()}

    # Ensure nested structures have the expected shapes
    if not isinstance(pr_dict.get("pr_base"), dict):
        pr_dict["pr_base"] = {}
    lbl = pr_dict.get("pr_labels")
    if lbl is None:
        pr_dict["pr_labels"] = []
    elif isinstance(lbl, dict):
        pr_dict["pr_labels"] = [lbl]
    elif isinstance(lbl, (tuple, set)):
        pr_dict["pr_labels"] = list(lbl)

    # Coerce common text fields to strings when present
    for key in ("pr_url", "pr_body", "pr_title", "patch", "file_change_summary"):
        if key in pr_dict and pr_dict[key] is not None and not isinstance(pr_dict[key], str):
            pr_dict[key] = str(pr_dict[key])

    return pr_dict


def _build_single(rb: ReportBuilder, row: pd.Series, use_cache: bool = True) -> dict[str, Any]:
    """
    Run ReportBuilder on a single row; return a small dict with classification fields.
    Any exceptions are caught and produce a default negative classification.

    If use_cache=True, uses SQLite-backed caching that survives DSPy cache clearing.
    """
    try:
        pr_dict = _prepare_pr_dict(row)

        if use_cache:
            # Use the cached classification function
            cache_key = _compute_cache_key(pr_dict)
            return _cached_classify(
                cache_key=cache_key,
                pr_dict=pr_dict,
                enable_llm_backends=rb.enable_llm_backends,
                summarize_llm=rb.summarize_llm,
                add_classification=rb.add_classification,
                filter_performance_only=rb.filter_performance_only,
                include_bot_comments=rb.include_bot_comments,
                anonymize_output=rb.anonymize_output,
                max_links_to_follow=rb.max_links_to_follow,
                model_name=rb.model_name,
            )
        else:
            # Direct call without caching
            result = rb.build(pr_dict=pr_dict)
            return result.__dict__
    except Exception as e:  # Defensive: keep the batch running
        logger.warning(f"Report build failed for row sha={row.get('sha')}: {e}")
        return {
            "is_performance_commit": False,
            "classification": "",
            "difficulty": "",
            "classification_reason": f"error: {e}",
            "classification_confidence": None,
        }


def main(args: argparse.Namespace) -> None:  # noqa: C901
    df = _load_input(args.commits, db_path=args.db)
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
    use_cache = not args.no_cache
    logger.info("Classification caching: %s", "enabled" if use_cache else "disabled")

    records: list[dict[str, Any]] = []
    if args.max_workers and args.max_workers > 0:
        logger.info("Classifying with %d workers (may be limited by rate limits)", args.max_workers)
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            fut2idx = {ex.submit(_build_single, rb, row, use_cache): idx for idx, (_, row) in enumerate(df.iterrows())}
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
                logger.warning(f"Report build failed for row index={idx}")
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
            records.append(_build_single(rb, row, use_cache))

    enrich = pd.DataFrame(records)

    # Normalize nested dataclasses and complex objects so we can serialize reliably.
    def _normalize(obj):
        if is_dataclass(obj):
            return asdict(obj)
        if isinstance(obj, dict):
            return {k: _normalize(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_normalize(v) for v in obj]
        return obj

    if "all_data" in enrich.columns:
        enrich["all_data"] = enrich["all_data"].apply(_normalize)
    if "problem_sections" in enrich.columns:
        enrich["problem_sections"] = enrich["problem_sections"].apply(_normalize)
    if "final_results" in enrich.columns:
        enrich["final_results"] = enrich["final_results"].apply(_normalize)

    # Convert complex columns to JSON strings to keep the parquet schema simple and stable.
    def _to_json(val: Any) -> str:
        try:
            return json.dumps(val, ensure_ascii=False, default=lambda o: asdict(o) if is_dataclass(o) else str(o))
        except Exception:
            return json.dumps(str(val))

    for _col in ("all_data", "problem_sections", "final_results"):
        if _col in enrich.columns:
            enrich[_col] = enrich[_col].apply(_to_json)
    # drop duplicate columns.
    enrich = enrich.drop(columns=[col for col in df.columns if col in enrich.columns])
    df_enriched = pd.concat([df.reset_index(drop=True), enrich], axis=1)

    # Drop duplicate column names, keeping the original input columns by preference.
    dupes = pd.Series(df_enriched.columns)
    if dupes.duplicated().any():
        # Log which duplicates we drop (we keep the first occurrence)
        logger.warning(
            "Duplicate columns detected; dropping later duplicates: %s",
            sorted(dupes[dupes.duplicated()].unique().tolist()),
        )
        df_enriched = df_enriched.loc[:, ~df_enriched.columns.duplicated(keep="first")]

    # Save performance-related commits
    base_table = resolve_table_name(str(args.outfile))
    write_table(df_enriched, base_table + "_raw", db_path=args.db)
    logger.info("Saved performance commits to table %s_raw", base_table)
    perf_df = df_enriched[df_enriched["is_performance_commit"]].copy()
    logger.info("Detected %d performance commits (from %d total)", len(perf_df), len(df))

    write_table(perf_df, base_table, db_path=args.db)
    logger.info("Saved filtered performance commits to table %s", base_table)


if __name__ == "__main__":
    main(parse_args())
