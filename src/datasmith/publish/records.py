from __future__ import annotations

import io
import os
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from datasmith.github.models import FormulaCodeRecord
from datasmith.utils import get_logger
from datasmith.utils.db import fetch_all, window_filters

logger = get_logger("publish.records")

MIN_HARBOR_SPEEDUP = 1.05


# Harbor environments whose runs count for publication, comma-separated.
# Daytona only by default; set DATASMITH_PUBLISH_ENVIRONMENTS=docker,daytona to add local Docker.
def _publish_environments(raw: str) -> tuple[str, ...]:
    """Parse the comma-separated environment list, dropping blanks.

    A function so tests can check the parsing without reloading this module.
    """
    return tuple(e.strip() for e in raw.split(",") if e.strip())


DATASMITH_PUBLISH_ENVIRONMENTS: tuple[str, ...] = _publish_environments(
    os.environ.get("DATASMITH_PUBLISH_ENVIRONMENTS", "daytona")
)

# Only the columns :class:`FormulaCodeRecord` needs, so other large text columns
# are not read.
_RECORD_COLUMNS = (
    "owner, repo, issue_number, merge_commit_sha, base_sha, merged_at, "
    "rendered_problem, classification, difficulty, container_name, patch"
)


def records_to_parquet(records: list[FormulaCodeRecord]) -> bytes:
    """Serialize FormulaCodeRecords to Parquet bytes via pyarrow."""
    if not records:
        return b""

    rows = [r.model_dump(mode="json") for r in records]

    # Build schema from first record
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def records_from_parquet(data: bytes) -> list[FormulaCodeRecord]:
    """Deserialize Parquet bytes back to FormulaCodeRecords."""
    if not data:
        return []
    buf = io.BytesIO(data)
    table = pq.read_table(buf)
    rows = table.to_pylist()
    return [FormulaCodeRecord(**row) for row in rows]


def records_from_supabase(  # noqa: C901
    start_date: str | None = None,
    end_date: str | None = None,
    unpublished_only: bool = True,
) -> list[FormulaCodeRecord]:
    """Query Supabase for FormulaCodeRecords, optionally filtered by date and publish status.

    The date filter is the run window from
    :func:`~datasmith.utils.db.window_filters`: ``merged_at`` in ``[start_date, end_date)``.
    """
    filters: dict[str, Any] = {"is_performance_commit": True}
    is_null: list[str] = []

    if unpublished_only:
        is_null.append("published_at")

    rows = fetch_all(
        "pull_requests",
        select=_RECORD_COLUMNS,
        filters=filters,
        is_null=is_null or None,
        **window_filters(start_date, end_date),
    )

    # Stage 7 check: publish only PRs whose best successful harbor run, in an
    # admitted environment, reaches MIN_HARBOR_SPEEDUP.
    harbor_rows = fetch_all(
        "harbor_runs",
        select="owner, repo, sha, max_speedup, status, environment",
        in_filters={"environment": list(DATASMITH_PUBLISH_ENVIRONMENTS)},
    )
    best_speedup: dict[tuple[str, str, str], float] = {}
    for hr in harbor_rows:
        if hr.get("status") != "success":
            continue
        speedup = hr.get("max_speedup")
        if speedup is None:
            continue
        key = (hr["owner"], hr["repo"], hr["sha"])
        prev = best_speedup.get(key)
        if prev is None or speedup > prev:
            best_speedup[key] = float(speedup)

    # Stage 6 check: the container must also have passed the integrity scan, since a
    # fast harbor run does not show the image was checked for tampering. Not configurable.
    verified_rows = fetch_all(
        "candidate_containers",
        select="owner, repo, sha",
        filters={"verification_state": "verified"},
    )
    verified_keys = {(v.get("owner", ""), v.get("repo", ""), v.get("sha", "")) for v in verified_rows}

    dropped_no_run = 0
    dropped_slow = 0
    dropped_unverified = 0
    records: list[FormulaCodeRecord] = []
    for row in rows:
        sha = row.get("merge_commit_sha", "")
        key = (row.get("owner", ""), row.get("repo", ""), sha)
        best = best_speedup.get(key)
        if best is None:
            dropped_no_run += 1
            continue
        if best < MIN_HARBOR_SPEEDUP:
            dropped_slow += 1
            continue
        if key not in verified_keys:
            dropped_unverified += 1
            continue
        try:
            records.append(
                FormulaCodeRecord(
                    owner=row["owner"],
                    repo=row["repo"],
                    issue_number=row["issue_number"],
                    task_id=int(row["issue_number"]),
                    gt_hash=sha,
                    base_commit=row.get("base_sha", ""),
                    date=row.get("merged_at"),
                    instructions=row.get("rendered_problem", ""),
                    classification=row.get("classification", ""),
                    difficulty=row.get("difficulty", ""),
                    container_name=row.get("container_name", ""),
                    patch=row.get("patch", ""),
                )
            )
        except Exception:
            logger.warning(
                "Failed to create record for %s/%s#%s", row.get("owner"), row.get("repo"), row.get("issue_number")
            )

    if dropped_no_run or dropped_slow or dropped_unverified:
        logger.info(
            "publish: dropped %d records without a successful harbor run in %s, "
            "%d below %.2fx speedup gate, %d whose container is not verification_state='verified' (kept %d)",
            dropped_no_run,
            ",".join(DATASMITH_PUBLISH_ENVIRONMENTS),
            dropped_slow,
            MIN_HARBOR_SPEEDUP,
            dropped_unverified,
            len(records),
        )

    return records
