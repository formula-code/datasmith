"""Read per-task operator declarations from ``formulacode_task_overrides``.

The table holds facts about a task that cannot be derived from the repo or the
PR. It has no ``anon`` grant, so the host reads it for the build and the trial:

``benchmark_dest``
    Passed into the image build as the ``BENCHMARK_DEST`` build arg, which
    ``docker_build_run.sh`` turns into the ``benchmark_dest_present_post_clean``
    breadcrumb. Input to the FATAL ``benchmark_dest_missing`` invariant.

``expected_n``
    Injected into the trial container as ``FORMULACODE_EXPECTED_N`` via the
    task's ``[verifier.env]`` section. Input to the ``dilution_ratio``
    invariant.

A failed lookup is not fatal: it means "no override", and the matching
invariants skip.
"""

from __future__ import annotations

from typing import Any

from datasmith.utils.core import get_logger
from datasmith.utils.db import fetch_all

logger = get_logger("utils.overrides")

TABLE = "formulacode_task_overrides"

# The task identity: (owner, repo, issue_number).
TaskKey = tuple[str, str, int]


_ALL_CACHE: dict[TaskKey, dict[str, Any]] | None = None


def all_overrides(*, refresh: bool = False) -> dict[TaskKey, dict[str, Any]]:
    """Every override row, read once per process.

    The table is small and callers look up one task at a time, so one read per
    process is enough. A failed read caches ``{}`` too.
    """
    global _ALL_CACHE
    if _ALL_CACHE is not None and not refresh:
        return _ALL_CACHE

    try:
        rows = fetch_all(TABLE, select="owner, repo, issue_number, benchmark_dest, expected_n")
    except Exception:
        logger.warning(
            "Could not read %s; proceeding without overrides. The "
            "benchmark_dest_missing and dilution_ratio invariants will skip.",
            TABLE,
            exc_info=True,
        )
        _ALL_CACHE = {}
        return _ALL_CACHE

    out: dict[TaskKey, dict[str, Any]] = {}
    for row in rows or []:
        try:
            out[(row["owner"], row["repo"], int(row["issue_number"]))] = row
        except (KeyError, TypeError, ValueError):
            continue
    _ALL_CACHE = out
    return out


def fetch_overrides(tasks: list[TaskKey]) -> dict[TaskKey, dict[str, Any]]:
    """Return ``{(owner, repo, issue_number): override_row}`` for *tasks*.

    Tasks with no override row are absent. Any read failure yields ``{}``
    with a warning, which callers treat as "no overrides".
    """
    if not tasks:
        return {}

    wanted = set(tasks)
    return {key: row for key, row in all_overrides().items() if key in wanted}


def benchmark_dest_for(overrides: dict[TaskKey, dict[str, Any]], key: TaskKey) -> str:
    """The declared benchmark path for *key*, or "" when undeclared.

    ``docker_build_run.sh`` writes its breadcrumb only for a non-empty
    ``$BENCHMARK_DEST``, so "" makes the invariant skip.
    """
    row = overrides.get(key) or {}
    return str(row.get("benchmark_dest") or "")


def expected_n_for(overrides: dict[TaskKey, dict[str, Any]], key: TaskKey) -> int | None:
    """The declared benchmark count for *key*, or None when undeclared.

    None, not 0, so the dilution invariant skips instead of comparing against 0.
    """
    row = overrides.get(key) or {}
    raw = row.get("expected_n")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
