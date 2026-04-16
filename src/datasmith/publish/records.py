from __future__ import annotations

import io
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from datasmith.github.models import FormulaCodeRecord
from datasmith.utils import get_logger
from datasmith.utils.db import fetch_all

logger = get_logger("publish.records")

MIN_HARBOR_SPEEDUP = 1.05


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
    """Query Supabase for FormulaCodeRecords, optionally filtered by date and publish status."""
    filters: dict[str, Any] = {"is_performance_commit": True}
    is_null: list[str] = []
    gte_filters: dict[str, Any] = {}
    lte_filters: dict[str, Any] = {}

    if unpublished_only:
        is_null.append("published_at")
    if start_date:
        gte_filters["merged_at"] = start_date
    if end_date:
        lte_filters["merged_at"] = end_date

    rows = fetch_all(
        "pull_requests",
        select="*",
        filters=filters,
        is_null=is_null or None,
        gte_filters=gte_filters or None,
        lte_filters=lte_filters or None,
    )

    # Stage 7 (harbor_healthcheck) gate: drop any PR whose best successful
    # harbor run produced a max_speedup below MIN_HARBOR_SPEEDUP, and any PR
    # with no successful harbor run at all. Fail-closed so we never publish
    # a container we haven't positively verified speeds something up.
    #
    # Only Daytona runs count as publishable evidence. Local Docker runs are
    # useful for iterating on the pipeline but can be flaky on the build
    # host (shared CPU, varying load), so we don't let them gate the
    # published dataset.
    harbor_rows = fetch_all(
        "harbor_runs",
        select="owner, repo, sha, max_speedup, status, environment",
        filters={"environment": "daytona"},
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

    dropped_no_run = 0
    dropped_slow = 0
    records: list[FormulaCodeRecord] = []
    for row in rows:
        sha = row.get("merge_commit_sha", "")
        best = best_speedup.get((row.get("owner", ""), row.get("repo", ""), sha))
        if best is None:
            dropped_no_run += 1
            continue
        if best < MIN_HARBOR_SPEEDUP:
            dropped_slow += 1
            continue
        try:
            records.append(
                FormulaCodeRecord(
                    owner=row["owner"],
                    repo=row["repo"],
                    issue_number=row["issue_number"],
                    task_id=f"{row['owner']}__{row['repo']}-{row['issue_number']}",
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

    if dropped_no_run or dropped_slow:
        logger.info(
            "publish: dropped %d records without successful Daytona harbor run, %d below %.2fx speedup gate (kept %d)",
            dropped_no_run,
            dropped_slow,
            MIN_HARBOR_SPEEDUP,
            len(records),
        )

    return records
