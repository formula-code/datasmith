from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq

from datasmith.github.models import FormulaCodeRecord
from datasmith.utils import get_client, get_logger

logger = get_logger("publish.records")


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


def records_from_supabase(
    start_date: str | None = None,
    end_date: str | None = None,
    unpublished_only: bool = True,
) -> list[FormulaCodeRecord]:
    """Query Supabase for FormulaCodeRecords, optionally filtered by date and publish status."""
    client = get_client()
    query = client.table("pull_requests").select("*").eq("is_performance_commit", True)

    if unpublished_only:
        query = query.is_("published_at", "null")
    if start_date:
        query = query.gte("merged_at", start_date)
    if end_date:
        query = query.lte("merged_at", end_date)

    resp = query.execute()

    records: list[FormulaCodeRecord] = []
    for row in resp.data:
        try:
            records.append(
                FormulaCodeRecord(
                    owner=row["owner"],
                    repo=row["repo"],
                    issue_number=row["issue_number"],
                    task_id=f"{row['owner']}__{row['repo']}-{row['issue_number']}",
                    gt_hash=row.get("merge_commit_sha", ""),
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

    return records
