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


# Harbor environments whose runs may gate publication, comma-separated.
# Default is Daytona only. Set DATASMITH_PUBLISH_ENVIRONMENTS=docker,daytona to
# admit local Docker trials as well -- see the comment at the harbor_runs query
# for what that trades away.
def _publish_environments(raw: str) -> tuple[str, ...]:
    """Parse the comma-separated environment list, dropping blanks.

    A function rather than an inline expression so a test can exercise the
    parsing without `importlib.reload`. Reloading this module rebinds its
    globals, which silently breaks every `from datasmith.publish.records
    import ...` reference held elsewhere -- a patch applied to the new module
    then misses the old function object, and unrelated suites start reading
    the real database.
    """
    return tuple(e.strip() for e in raw.split(",") if e.strip())


DATASMITH_PUBLISH_ENVIRONMENTS: tuple[str, ...] = _publish_environments(
    os.environ.get("DATASMITH_PUBLISH_ENVIRONMENTS", "daytona")
)

# Exactly the columns :class:`FormulaCodeRecord` is built from, below.  This
# used to be ``select="*"``, which pulls every column of ``pull_requests`` --
# ``body``, ``rendered_problem``, ``problem_description``, ``patch`` and the
# rest -- for every performance commit in the window.  That is the same shape
# as the read that killed PostgREST with "cannot enlarge string buffer
# containing 1073741822 bytes".  ``patch`` is still here because the record
# requires it; what changes is that the other large text columns no longer
# ride along for free.
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

    The date filter is the pipeline run window, so it comes from
    :func:`~datasmith.utils.db.window_filters` rather than being spelled out
    here: ``merged_at``, half-open ``[start_date, end_date)``, the same as
    every other stage. This read used to close the upper bound, which made a
    PR merged at exactly midnight publishable by two consecutive monthly
    runs; the ``published_at IS NULL`` predicate happened to catch it, but
    that is a second guard covering for a window that meant the wrong thing,
    and ``unpublished_only=False`` removes the cover.
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

    # Stage 7 (harbor_healthcheck) gate: drop any PR whose best successful
    # harbor run produced a max_speedup below MIN_HARBOR_SPEEDUP, and any PR
    # with no successful harbor run at all. Fail-closed so we never publish
    # a container we haven't positively verified speeds something up.
    #
    # Which environments count as publishable evidence.
    #
    # Daytona alone by default: local Docker runs are useful for iterating on
    # the pipeline but can be flaky on the build host (shared CPU, varying
    # load), so they do not gate the published dataset unless an operator says
    # so. This is a knob rather than a hardcoded string because an operator
    # without Daytona access would otherwise have to edit the gate itself, and
    # a gate edited under deadline is a gate that quietly stops gating.
    #
    # Widening it trades measurement quality for reach; it does NOT relax the
    # speedup requirement. MIN_HARBOR_SPEEDUP still applies to whatever
    # environments are admitted, and `status == "success"` still applies.
    # Record in the run notes which environments were admitted, because a
    # published record cannot say so for itself.
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

    # Stage 6 (synthesize_images) gate: the container must also be *honest*.
    #
    # A qualifying harbor run says the container is fast. It does not say the
    # image was ever checked for tampering: ``harbor_runs`` outlives the
    # container generation that produced it, and three rows in the corpus
    # carry a successful trial above MIN_HARBOR_SPEEDUP while their
    # ``candidate_containers`` row is still ``unverified``. Those rows predate
    # the host-side integrity scan -- migration 00029 defaulted every
    # pre-existing row to ``unverified`` for exactly that reason -- so
    # publishing them would ship the generation the honesty gate was built to
    # retire, under a tag no consumer can tell apart from a verified one.
    #
    # Unlike the environment set above this is deliberately NOT a knob. An
    # operator without Daytona has a real need the environment knob answers;
    # nobody has a legitimate need to publish a container that failed, or
    # never faced, the integrity scan.
    #
    # Narrow select: ``candidate_containers`` carries ``build_manifest``,
    # ``dockerfile`` and five shell scripts per row.
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
