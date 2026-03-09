#!/usr/bin/env python
"""One-off script: recover repositories and pull_requests from the legacy SQLite cache.

Extracts all relevant GitHub API data from ``scratch/artifacts/cache.db``
(219k+ cached responses) and upserts into Supabase.  This covers:

1. **Repositories** — 70 cached repo metadata entries → ``repositories`` table.
2. **Pull requests** — 44k+ cached PR detail responses + pull-list pages.
   Merged PRs that pass attribute compliance are upserted with
   ``is_performance_commit_symbolic = True``; those that fail are stored
   with ``is_performance_commit_symbolic = False`` (we store all merged PRs
   so future pipeline stages can classify them).
3. **Patches** — 29k+ GitHub API diffs cached in ``github_metadata`` with
   ``params={'diff_api': 'true'}``, stored as ``{"diff": "..."}`` dicts.
4. **File changes** — stored in ``file_changes`` JSONB column when available
   from cached PR detail responses or ``pulls/N/files`` endpoints.

Usage::

    uv run python scripts/recover_from_cache.py [--dry-run] [--all-prs]
    uv run python scripts/recover_from_cache.py              # default: only perf-passing PRs
    uv run python scripts/recover_from_cache.py --all-prs    # store ALL merged PRs
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

from datasmith.filters import symbolic_compliance
from datasmith.utils import get_client, get_logger

logger = get_logger("recover_from_cache")

CACHE_DB = Path(__file__).resolve().parent.parent / "scratch" / "artifacts" / "cache.db"
BATCH_SIZE = 50  # upsert batch size


# ---------------------------------------------------------------------------
# 1. Repositories
# ---------------------------------------------------------------------------


def _extract_repos(db_path: Path) -> list[dict[str, Any]]:
    """Extract repo metadata from cached ``repos/owner/repo`` responses."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    repos: dict[str, dict[str, Any]] = {}

    for func_name in ("get_github_metadata", "_get_github_metadata"):
        cur.execute(
            "SELECT argument_blob, result_blob FROM github_metadata WHERE function_name=?",
            (func_name,),
        )
        for arg_blob, res_blob in cur.fetchall():
            args = pickle.loads(arg_blob)  # noqa: S301
            url = _extract_url(args)
            parts = url.split("/")
            if len(parts) == 3 and parts[0] == "repos":
                result = pickle.loads(res_blob)  # noqa: S301
                if isinstance(result, dict) and "full_name" in result:
                    key = result["full_name"]
                    repos[key] = result

    conn.close()
    logger.info("Extracted %d repo metadata entries from cache", len(repos))
    return list(repos.values())


def _upsert_repos(repos: list[dict[str, Any]], *, dry_run: bool = False) -> int:
    """Upsert repo metadata into the repositories table."""
    if dry_run:
        for r in repos:
            logger.info("[DRY RUN] Would upsert repo: %s", r["full_name"])
        return len(repos)

    client = get_client()
    count = 0
    for r in repos:
        owner, repo = r["full_name"].split("/", 1)
        client.table("repositories").upsert({
            "owner": owner,
            "repo": repo,
            "url": r.get("html_url", ""),
            "language": r.get("language"),
            "stars": r.get("stargazers_count"),
            "topics": r.get("topics"),
            "description": r.get("description", ""),
        }).execute()
        count += 1

    logger.info("Upserted %d repositories", count)
    return count


# ---------------------------------------------------------------------------
# 2. Pull requests (from PR detail endpoints + pull-list pages)
# ---------------------------------------------------------------------------


def _extract_url(args: tuple) -> str:
    """Extract the URL from cached argument tuple."""
    url_tuple = args[1]
    kwargs = args[2] if len(args) > 2 else {}
    return (url_tuple[0] if url_tuple else kwargs.get("endpoint", "")).lstrip("/")


def _extract_prs(db_path: Path) -> dict[str, dict[str, Any]]:  # noqa: C901
    """Extract all merged PR data from cached responses.

    Sources:
    - PR detail endpoints (repos/owner/repo/pulls/N) — 44k+ entries
    - Pull list pages (repos/owner/repo/pulls?state=closed) — 624 entries
    """
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    prs: dict[str, dict[str, Any]] = {}  # key: owner/repo#number

    for func_name in ("get_github_metadata", "_get_github_metadata"):
        cur.execute(
            "SELECT argument_blob, result_blob FROM github_metadata WHERE function_name=?",
            (func_name,),
        )
        for arg_blob, res_blob in cur.fetchall():
            args = pickle.loads(arg_blob)  # noqa: S301
            url = _extract_url(args)
            parts = url.split("/")

            if len(parts) < 4 or parts[0] != "repos":
                continue

            owner, repo = parts[1], parts[2]
            base = parts[3].split("?")[0]

            # PR detail: repos/owner/repo/pulls/N
            if len(parts) == 5 and base == "pulls" and parts[4].split("?")[0].isdigit():
                result = pickle.loads(res_blob)  # noqa: S301
                if isinstance(result, dict) and result.get("merged_at"):
                    number = result["number"]
                    key = f"{owner}/{repo}#{number}"
                    # Detail entries are richer, prefer them over list entries
                    prs[key] = {**result, "_owner": owner, "_repo": repo, "_source": "detail"}

            # Pull list page: repos/owner/repo/pulls?state=closed
            elif (len(parts) == 4 and base == "pulls") or (len(parts) >= 4 and "pulls" in parts[3] and "?" in parts[3]):
                result = pickle.loads(res_blob)  # noqa: S301
                if not isinstance(result, list):
                    continue
                for pr_data in result:
                    if not pr_data.get("merged_at"):
                        continue
                    number = pr_data["number"]
                    key = f"{owner}/{repo}#{number}"
                    # Don't overwrite detail entries with list entries
                    if key not in prs:
                        prs[key] = {**pr_data, "_owner": owner, "_repo": repo, "_source": "list"}

    conn.close()
    logger.info(
        "Extracted %d unique merged PRs from cache (detail + list pages)",
        len(prs),
    )
    return prs


def _extract_diffs(db_path: Path) -> dict[str, str]:
    """Extract GitHub API diffs from cached PR detail responses.

    The legacy pipeline fetched diffs via the PR detail endpoint with
    ``params={'diff_api': 'true'}``.  The result is stored as a dict
    with a ``"diff"`` key containing the unified diff string.
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
            url = _extract_url(args)
            kwargs = args[2] if len(args) > 2 else {}
            params = kwargs.get("params", {})

            # Diff API entries: repos/owner/repo/pulls/N with diff_api=true
            if params.get("diff_api") == "true":
                parts = url.split("/")
                if len(parts) == 5 and parts[3] == "pulls" and parts[4].isdigit():
                    result = pickle.loads(res_blob)  # noqa: S301
                    if isinstance(result, dict) and result.get("diff"):
                        key = f"{parts[1]}/{parts[2]}#{parts[4]}"
                        diffs[key] = result["diff"]

    conn.close()
    logger.info("Extracted %d GitHub API diffs (diff_api=true)", len(diffs))
    return diffs


def _extract_file_lists(db_path: Path) -> dict[str, list[dict[str, Any]]]:
    """Extract cached PR file lists (repos/owner/repo/pulls/N/files)."""
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
            url = _extract_url(args)
            parts = url.split("/")
            if len(parts) == 6 and parts[3] == "pulls" and parts[4].isdigit() and parts[5].startswith("files"):
                result = pickle.loads(res_blob)  # noqa: S301
                if isinstance(result, list):
                    key = f"{parts[1]}/{parts[2]}#{parts[4]}"
                    files_map[key] = result

    conn.close()
    logger.info("Extracted %d cached file lists", len(files_map))
    return files_map


def _build_pr_record(
    pr_data: dict[str, Any],
    diff: str | None,
    file_changes: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Build a pull_requests record dict from cached PR data."""
    owner = pr_data["_owner"]
    repo = pr_data["_repo"]
    title = pr_data.get("title", "")

    # Compute symbolic compliance
    passes = symbolic_compliance(
        title=title,
        patch=diff,
        file_changes=file_changes,
    )

    record: dict[str, Any] = {
        "owner": owner,
        "repo": repo,
        "issue_number": pr_data["number"],
        "title": title,
        "body": (pr_data.get("body") or "")[:50_000],  # cap body size
        "state": pr_data.get("state", ""),
        "created_at": pr_data.get("created_at"),
        "merged_at": pr_data.get("merged_at"),
        "closed_at": pr_data.get("closed_at"),
        "merge_commit_sha": pr_data.get("merge_commit_sha", ""),
        "base_sha": pr_data.get("base", {}).get("sha", ""),
        "head_sha": pr_data.get("head", {}).get("sha", ""),
        "labels": [label["name"] for label in pr_data.get("labels", []) if isinstance(label, dict) and "name" in label],
        "is_performance_commit_symbolic": passes,
    }

    if diff:
        record["patch"] = diff
    if file_changes:
        # Store compact file_changes: just filename, additions, deletions
        record["file_changes"] = [
            {
                "filename": f.get("filename", ""),
                "additions": f.get("additions", 0),
                "deletions": f.get("deletions", 0),
            }
            for f in file_changes
        ]

    return record


def _upsert_prs(
    prs: dict[str, dict[str, Any]],
    diffs: dict[str, str],
    files_map: dict[str, list[dict[str, Any]]],
    *,
    dry_run: bool = False,
    all_prs: bool = False,
) -> tuple[int, int, int]:
    """Upsert PRs into the pull_requests table.

    Returns (stored, symbolic_pass, symbolic_fail).
    """
    client = None if dry_run else get_client()

    stored = 0
    sym_pass = 0
    sym_fail = 0

    for key, pr_data in prs.items():
        diff = diffs.get(key)
        file_changes = files_map.get(key)

        record = _build_pr_record(pr_data, diff, file_changes)
        passes = record["is_performance_commit_symbolic"]

        if passes:
            sym_pass += 1
        else:
            sym_fail += 1

        # By default only store PRs that pass symbolic compliance
        if not all_prs and not passes:
            continue

        if dry_run:
            status = "PASS" if passes else "FAIL"
            logger.debug(
                "[DRY RUN] %s [%s]: %s",
                key,
                status,
                record["title"][:60],
            )
        else:
            client.table("pull_requests").upsert(record).execute()  # type: ignore[union-attr]

        stored += 1
        if stored % 200 == 0:
            logger.info("Progress: %d PRs upserted...", stored)

    return stored, sym_pass, sym_fail


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def recover(db_path: Path, *, dry_run: bool = False, all_prs: bool = False) -> None:
    """Full recovery: repos + pull requests from the SQLite cache."""
    logger.info("=== Phase 1: Repositories ===")
    repos = _extract_repos(db_path)
    _upsert_repos(repos, dry_run=dry_run)

    logger.info("=== Phase 2: Pull Requests ===")
    prs = _extract_prs(db_path)
    diffs = _extract_diffs(db_path)
    files_map = _extract_file_lists(db_path)

    stored, sym_pass, sym_fail = _upsert_prs(
        prs,
        diffs,
        files_map,
        dry_run=dry_run,
        all_prs=all_prs,
    )

    logger.info(
        "=== Done ===\n"
        "  Repos upserted: %d\n"
        "  PRs total in cache: %d\n"
        "  PRs stored: %d\n"
        "  Symbolic compliance: pass=%d, fail=%d\n"
        "  Diffs available: %d\n"
        "  File lists available: %d",
        len(repos),
        len(prs),
        stored,
        sym_pass,
        sym_fail,
        len(diffs),
        len(files_map),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recover repositories and pull_requests from SQLite cache",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be stored without writing to DB",
    )
    parser.add_argument(
        "--all-prs",
        action="store_true",
        help="Store ALL merged PRs, not just those passing symbolic compliance",
    )
    parser.add_argument(
        "--cache-db",
        default=str(CACHE_DB),
        help=f"Path to SQLite cache database (default: {CACHE_DB})",
    )
    args = parser.parse_args()

    db_path = Path(args.cache_db)
    if not db_path.exists():
        logger.error("Cache database not found at %s", db_path)
        sys.exit(1)

    recover(db_path, dry_run=args.dry_run, all_prs=args.all_prs)


if __name__ == "__main__":
    main()
