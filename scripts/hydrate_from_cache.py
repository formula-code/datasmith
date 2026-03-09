#!/usr/bin/env python
"""Hydrate the pull_requests table from the legacy SQLite cache.

The cache at ``scratch/artifacts/cache.db`` contains 219k+ GitHub API responses
from previous pipeline runs.  This script extracts merged PRs from the cached
pull-list pages, applies attribute-compliance filters, and upserts them into
Supabase — avoiding thousands of GitHub API calls and rate-limit issues.

Usage::

    uv run python scripts/hydrate_from_cache.py [--dry-run] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import pickle
import sqlite3
import sys
from pathlib import Path
from typing import Any

# Ensure the package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from datasmith.filters import (
    MAX_FILES_CHANGED,
    MAX_TOTAL_CHANGES,
    check_patch_size,
    has_core_file,
    message_filter,
)
from datasmith.utils import get_client, get_logger

logger = get_logger("hydrate_from_cache")

CACHE_DB = Path(__file__).resolve().parent.parent / "scratch" / "artifacts" / "cache.db"


def _load_cached_prs(db_path: Path) -> list[dict[str, Any]]:
    """Extract all PR data from cached pull-list pages."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    prs: dict[str, dict[str, Any]] = {}  # dedup by owner/repo#number

    for func_name in ("get_github_metadata", "_get_github_metadata"):
        cur.execute(
            "SELECT argument_blob, result_blob FROM github_metadata WHERE function_name=?",
            (func_name,),
        )
        for arg_blob, res_blob in cur.fetchall():
            args = pickle.loads(arg_blob)  # noqa: S301
            url_tuple = args[1]
            kwargs = args[2] if len(args) > 2 else {}
            url = (url_tuple[0] if url_tuple else kwargs.get("endpoint", "")).lstrip("/")

            parts = url.split("/")
            base = parts[3].split("?")[0] if len(parts) >= 4 else ""

            # Pull list pages: repos/owner/repo/pulls?state=closed&...
            if len(parts) >= 4 and base == "pulls" and (len(parts) == 4 or "?" in parts[3]):
                result = pickle.loads(res_blob)  # noqa: S301
                if not isinstance(result, list):
                    continue
                owner, repo = parts[1], parts[2]
                for pr_data in result:
                    if not pr_data.get("merged_at"):
                        continue
                    key = f"{owner}/{repo}#{pr_data['number']}"
                    if key not in prs:
                        prs[key] = {**pr_data, "_owner": owner, "_repo": repo}

    conn.close()
    logger.info("Loaded %d unique merged PRs from cache", len(prs))
    return list(prs.values())


def _load_cached_diffs(db_path: Path) -> dict[str, str]:
    """Extract cached PR diffs (from detail endpoints with diff accept header).

    The cache stores PR detail responses. Some may have been fetched with the
    diff accept header, but the legacy cache doesn't distinguish — so we look
    for PR detail entries that returned a string (diff) rather than a dict.
    """
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    diffs: dict[str, str] = {}

    for func_name in ("get_github_metadata", "_get_github_metadata"):
        cur.execute(
            "SELECT argument_blob, result_blob FROM github_metadata WHERE function_name=?",
            (func_name,),
        )
        for arg_blob, res_blob in cur.fetchall():
            args = pickle.loads(arg_blob)  # noqa: S301
            url_tuple = args[1]
            kwargs = args[2] if len(args) > 2 else {}
            url = (url_tuple[0] if url_tuple else kwargs.get("endpoint", "")).lstrip("/")

            parts = url.split("/")
            # PR detail: repos/owner/repo/pulls/N
            if len(parts) == 5 and parts[3] == "pulls" and parts[4].isdigit():
                result = pickle.loads(res_blob)  # noqa: S301
                if isinstance(result, str) and result.strip():
                    key = f"{parts[1]}/{parts[2]}#{parts[4]}"
                    diffs[key] = result

    conn.close()
    logger.info("Loaded %d cached diffs", len(diffs))
    return diffs


def _load_cached_files(db_path: Path) -> dict[str, list[dict[str, Any]]]:
    """Extract cached PR file lists."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    files_map: dict[str, list[dict[str, Any]]] = {}

    for func_name in ("get_github_metadata", "_get_github_metadata"):
        cur.execute(
            "SELECT argument_blob, result_blob FROM github_metadata WHERE function_name=?",
            (func_name,),
        )
        for arg_blob, res_blob in cur.fetchall():
            args = pickle.loads(arg_blob)  # noqa: S301
            url_tuple = args[1]
            kwargs = args[2] if len(args) > 2 else {}
            url = (url_tuple[0] if url_tuple else kwargs.get("endpoint", "")).lstrip("/")

            parts = url.split("/")
            # PR files: repos/owner/repo/pulls/N/files
            if len(parts) == 6 and parts[3] == "pulls" and parts[4].isdigit() and parts[5].startswith("files"):
                result = pickle.loads(res_blob)  # noqa: S301
                if isinstance(result, list):
                    key = f"{parts[1]}/{parts[2]}#{parts[4]}"
                    files_map[key] = result

    conn.close()
    logger.info("Loaded %d cached file lists", len(files_map))
    return files_map


def hydrate(  # noqa: C901
    db_path: Path,
    dry_run: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Load cached PRs, filter, and upsert into Supabase."""
    all_prs = _load_cached_prs(db_path)
    diffs = _load_cached_diffs(db_path)
    files_map = _load_cached_files(db_path)

    stored = 0
    skipped_date = 0
    skipped_message = 0
    skipped_files = 0
    skipped_patch = 0
    skipped_no_diff = 0

    client = None if dry_run else get_client()

    for pr_data in all_prs:
        owner = pr_data["_owner"]
        repo = pr_data["_repo"]
        number = pr_data["number"]
        merged_at = pr_data["merged_at"]
        title = pr_data.get("title", "")
        key = f"{owner}/{repo}#{number}"

        # Date filter
        if end_date and merged_at > end_date + "T23:59:59Z":
            skipped_date += 1
            continue
        if start_date and merged_at < start_date + "T00:00:00Z":
            skipped_date += 1
            continue

        # Message filter
        if not message_filter(title):
            skipped_message += 1
            continue

        # File-level checks (if cached)
        if key in files_map:
            files = files_map[key]
            if len(files) >= MAX_FILES_CHANGED:
                skipped_files += 1
                continue
            total_changes = sum(f.get("additions", 0) + f.get("deletions", 0) for f in files)
            if total_changes >= MAX_TOTAL_CHANGES:
                skipped_files += 1
                continue
            filenames = [f.get("filename", "") for f in files]
            if filenames and not has_core_file(filenames):
                skipped_files += 1
                continue

        # Patch size check (if cached)
        diff = diffs.get(key, "")
        if diff and not check_patch_size(diff):
            skipped_patch += 1
            continue

        if dry_run:
            logger.info("[DRY RUN] Would store %s/%s#%d: %s", owner, repo, number, title[:60])
            stored += 1
            continue

        record = {
            "owner": owner,
            "repo": repo,
            "issue_number": number,
            "title": title,
            "body": pr_data.get("body", "") or "",
            "state": pr_data.get("state", ""),
            "created_at": pr_data.get("created_at"),
            "merged_at": merged_at,
            "closed_at": pr_data.get("closed_at"),
            "merge_commit_sha": pr_data.get("merge_commit_sha", ""),
            "base_sha": pr_data.get("base", {}).get("sha", ""),
            "head_sha": pr_data.get("head", {}).get("sha", ""),
            "labels": [label["name"] for label in pr_data.get("labels", []) if isinstance(label, dict)],
        }
        if diff:
            record["patch"] = diff

        client.table("pull_requests").upsert(record).execute()  # type: ignore[union-attr]
        stored += 1

        if stored % 100 == 0:
            logger.info("Progress: stored %d PRs so far...", stored)

    logger.info(
        "Done. stored=%d, skipped(date=%d, message=%d, files=%d, patch=%d, no_diff=%d)",
        stored,
        skipped_date,
        skipped_message,
        skipped_files,
        skipped_patch,
        skipped_no_diff,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Hydrate pull_requests from SQLite cache")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be stored without writing")
    parser.add_argument("--start-date", default=None, help="Filter: only PRs merged on or after this date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="Filter: only PRs merged on or before this date (YYYY-MM-DD)")
    parser.add_argument("--cache-db", default=str(CACHE_DB), help="Path to SQLite cache database")
    args = parser.parse_args()

    db_path = Path(args.cache_db)
    if not db_path.exists():
        print(f"Error: cache database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    hydrate(db_path, dry_run=args.dry_run, start_date=args.start_date, end_date=args.end_date)


if __name__ == "__main__":
    main()
