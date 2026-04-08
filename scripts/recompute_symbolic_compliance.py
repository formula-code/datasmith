#!/usr/bin/env python
"""Recompute ``is_performance_commit_symbolic`` for all stored pull requests.

Evaluates the attribute-compliance filters on existing data:

- **message_filter** on ``title`` (always available).
- **check_patch_size** on ``patch`` (when not NULL).
- **check_file_compliance** on ``file_changes`` (when not NULL).

PRs that pass all evaluable filters are marked True; those that fail
at least one are marked False.

Usage::

    uv run python scripts/recompute_symbolic_compliance.py [--dry-run] [--batch-size N]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

# Ensure the package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from datasmith.filters import symbolic_compliance
from datasmith.utils import get_client, get_logger

logger = get_logger("recompute_symbolic")

# Supabase REST API returns at most 1000 rows per request by default.
DEFAULT_BATCH_SIZE = 500


def _fetch_batch(
    client: Any,
    offset: int,
    batch_size: int,
) -> list[dict[str, Any]]:
    """Fetch a batch of PRs ordered by primary key."""
    resp = (
        client.table("pull_requests")
        .select("owner, repo, issue_number, title, patch, file_changes")
        .order("owner")
        .order("repo")
        .order("issue_number")
        .range(offset, offset + batch_size - 1)
        .execute()
    )
    return resp.data or []


def recompute(dry_run: bool = False, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
    """Recompute symbolic compliance for every PR in the database."""
    client = get_client()

    offset = 0
    total_true = 0
    total_false = 0
    total_processed = 0

    while True:
        rows = _fetch_batch(client, offset, batch_size)
        if not rows:
            break

        for row in rows:
            owner = row["owner"]
            repo = row["repo"]
            issue_number = row["issue_number"]
            title = row.get("title") or ""
            patch = row.get("patch")
            file_changes = row.get("file_changes")

            # file_changes is stored as JSONB — may be a list of dicts or None
            fc_list: list[dict[str, Any]] | None = None
            if isinstance(file_changes, list):
                fc_list = file_changes

            passes = symbolic_compliance(
                title=title,
                patch=patch,
                file_changes=fc_list,
            )

            if passes:
                total_true += 1
            else:
                total_false += 1

            if dry_run:
                if not passes:
                    logger.debug(
                        "[DRY RUN] %s/%s#%d -> FAIL: %s",
                        owner,
                        repo,
                        issue_number,
                        title[:60],
                    )
            else:
                client.table("pull_requests").update({
                    "is_performance_commit_symbolic": passes,
                }).eq("owner", owner).eq("repo", repo).eq(
                    "issue_number",
                    issue_number,
                ).execute()

        total_processed += len(rows)
        logger.info(
            "Processed %d PRs (true=%d, false=%d)...",
            total_processed,
            total_true,
            total_false,
        )

        if len(rows) < batch_size:
            break
        offset += batch_size

    logger.info(
        "Done. total=%d, symbolic_pass=%d, symbolic_fail=%d",
        total_processed,
        total_true,
        total_false,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recompute is_performance_commit_symbolic for all stored PRs",
    )
    parser.add_argument("--dry-run", action="store_true", help="Evaluate without writing to DB")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per batch (default: {DEFAULT_BATCH_SIZE})",
    )
    args = parser.parse_args()
    recompute(dry_run=args.dry_run, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
