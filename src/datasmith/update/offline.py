"""Convert rows from an offline parquet source into ``pull_requests`` records."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from datasmith.filters import symbolic_compliance
from datasmith.utils import get_logger

logger = get_logger("update.offline")

# Matches a data row in the markdown file-change table:
#   | filename | additions | deletions | ... |
_TABLE_ROW_RE = re.compile(r"^\|\s*(?P<filename>[^|]+?)\s*\|\s*(?P<additions>\d+)\s*\|\s*(?P<deletions>\d+)\s*\|")


def _sanitize_text(value: object) -> str:
    """Return a clean string, handling NaN/None and Postgres-illegal null bytes."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).replace("\u0000", "")


def parse_file_change_summary(summary: object) -> list[dict[str, Any]] | None:
    """Parse a markdown file-change table into the ``file_changes`` list format.

    Expected input::

        | File | Lines Added | Lines Removed | Total Changes |
        |------|-------------|----------------|----------------|
        | foo.py | 10 | 3 | 13 |

    Returns ``None`` when *summary* is empty/NaN or contains no parseable rows.
    """
    text = _sanitize_text(summary)
    if not text:
        return None
    changes: list[dict[str, Any]] = []
    for line in text.splitlines():
        # Skip header and separator rows
        if line.startswith("|--") or "Lines Added" in line or "File" in line:
            continue
        m = _TABLE_ROW_RE.match(line)
        if m:
            changes.append({
                "filename": m.group("filename").strip(),
                "additions": int(m.group("additions")),
                "deletions": int(m.group("deletions")),
            })
    return changes or None


def _extract_labels(raw_labels: Any) -> list[str]:
    """Extract label name strings from the parquet labels column.

    The column stores a numpy array of label dicts (each with a ``name`` key),
    or an empty array.
    """
    if raw_labels is None:
        return []
    try:
        return [label["name"] for label in raw_labels if isinstance(label, dict) and "name" in label]
    except (TypeError, KeyError):
        return []


def _safe_str(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def _dict_sha(value: object) -> str:
    """Extract ``sha`` from a dict-like value (pr_head / pr_base columns)."""
    if isinstance(value, dict):
        return str(value.get("sha", ""))
    return ""


def load_offline_repo_names(path: str) -> list[tuple[str, str]]:
    """Return unique ``(owner, repo)`` pairs from an offline parquet file."""
    df = pd.read_parquet(path, columns=["repo_name"])
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for name in df["repo_name"].dropna().unique():
        name = str(name).strip()
        if "/" not in name:
            continue
        owner, repo = name.split("/", 1)
        pair = (owner, repo)
        if pair not in seen:
            seen.add(pair)
            pairs.append(pair)
    return pairs


def _row_to_record(row: pd.Series) -> dict[str, Any]:
    """Convert a single parquet row into a ``pull_requests`` upsert record."""
    repo_name = str(row["repo_name"])
    owner, repo = repo_name.split("/", 1)

    title = _sanitize_text(row.get("pr_title"))
    body = _sanitize_text(row.get("pr_body"))
    patch = _sanitize_text(row.get("original_patch"))
    file_changes = parse_file_change_summary(row.get("file_change_summary"))

    record: dict[str, Any] = {
        "owner": owner,
        "repo": repo,
        "issue_number": int(row["pr_number"]),
        "title": title,
        "body": body,
        "state": _safe_str(row.get("pr_state")),
        "created_at": _safe_str(row.get("pr_created_at")) or None,
        "merged_at": _safe_str(row.get("pr_merged_at")) or None,
        "closed_at": _safe_str(row.get("pr_closed_at")) or None,
        "merge_commit_sha": _safe_str(row.get("pr_merge_commit_sha")),
        "base_sha": _dict_sha(row.get("pr_base")),
        "head_sha": _dict_sha(row.get("pr_head")),
        "labels": _extract_labels(row.get("pr_labels")),
        "is_performance_commit_symbolic": symbolic_compliance(
            title=title,
            patch=patch or None,
            file_changes=file_changes,
        ),
    }
    if patch:
        record["patch"] = patch
    if file_changes:
        record["file_changes"] = file_changes
    return record


def load_offline_pull_requests(
    path: str,
    since: str | None = None,
    until: str | None = None,
) -> list[dict[str, Any]]:
    """Load and convert parquet rows into ``pull_requests`` upsert records.

    Filters by ``pr_merged_at`` using the same ``[since, until)`` semantics
    as :class:`~datasmith.runners.scrape_commits.ScrapeCommitsRunner`.
    """
    df = pd.read_parquet(path)
    logger.info("Loaded %d rows from offline source %s", len(df), path)

    # Date filtering on pr_merged_at
    if since or until:
        merged: pd.Series[Any] = pd.to_datetime(df["pr_merged_at"], utc=True, errors="coerce")
        if since:
            df = df[merged >= pd.Timestamp(since, tz="UTC")]
            merged = merged.loc[df.index]
        if until:
            df = df[merged < pd.Timestamp(until, tz="UTC")]
        logger.info("After date filtering: %d rows", len(df))

    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        try:
            records.append(_row_to_record(row))
        except Exception:
            logger.warning(
                "Skipping row %s/%s#%s: conversion error",
                row.get("repo_name", "?"),
                row.get("pr_number", "?"),
                row.get("pr_merge_commit_sha", "?"),
                exc_info=True,
            )
    logger.info("Converted %d records for upsert", len(records))
    return records
