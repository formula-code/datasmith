"""Read per-task operator declarations from ``formulacode_task_overrides``.

The overrides table holds facts about a task that cannot be derived from the
repo or the PR. Two of its columns are invariant inputs, and both needed a
host-side reader because the table is RLS-locked with no ``anon`` grant --
neither the trial container nor the image build can query it themselves:

``benchmark_dest``
    Passed into the image build as the ``BENCHMARK_DEST`` build arg, which
    ``docker_build_run.sh`` turns into the ``benchmark_dest_present_post_clean``
    breadcrumb. Input to the FATAL ``benchmark_dest_missing`` invariant.

``expected_n``
    Injected into the trial container as ``FORMULACODE_EXPECTED_N`` via the
    task's ``[verifier.env]`` section. Input to the ``dilution_ratio``
    invariant.

Lookup failure is never fatal. Stage 6 and stage 7 must run on a machine where
the migration has not been applied, and "no override" is a legitimate state for
all but a handful of tasks -- the corresponding invariants then skip, which is
the correct answer rather than a degraded one.
"""

from __future__ import annotations

from typing import Any

from datasmith.utils.core import get_logger
from datasmith.utils.db import fetch_all

logger = get_logger("utils.overrides")

TABLE = "formulacode_task_overrides"

# The canonical task identity, per CLAUDE.md: (owner, repo, issue_number).
TaskKey = tuple[str, str, int]


_ALL_CACHE: dict[TaskKey, dict[str, Any]] | None = None


def all_overrides(*, refresh: bool = False) -> dict[TaskKey, dict[str, Any]]:
    """Every override row, read once per process.

    The table is tiny (5 rows today, and it is hand-maintained), while its
    consumers sit in per-item loops -- stage 6 synthesises one PR at a time and
    enqueues neighbours mid-flight, so there is no single point at which the
    full task set is known. Caching turns what would be one round-trip per
    task into one per run.

    A failed read caches ``{}`` too: if the table is absent, it will still be
    absent on the next call, and retrying per item would be a slow way to
    reach the same answer.
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

    Tasks with no override row are simply absent from the mapping. A missing
    table, an unreachable database, or any other read failure yields ``{}``
    with a warning -- callers treat that identically to "no overrides exist",
    so a fresh checkout behaves the same as a populated one minus the extra
    checks.
    """
    if not tasks:
        return {}

    wanted = set(tasks)
    return {key: row for key, row in all_overrides().items() if key in wanted}


def benchmark_dest_for(overrides: dict[TaskKey, dict[str, Any]], key: TaskKey) -> str:
    """The declared benchmark path for *key*, or "" when undeclared.

    Empty string is deliberate: ``docker_build_run.sh`` gates its breadcrumb on
    a non-empty ``$BENCHMARK_DEST``, so "" keeps the FATAL invariant skipping
    rather than firing against a value nobody declared.
    """
    row = overrides.get(key) or {}
    return str(row.get("benchmark_dest") or "")


def expected_n_for(overrides: dict[TaskKey, dict[str, Any]], key: TaskKey) -> int | None:
    """The declared benchmark count for *key*, or None when undeclared.

    None (not 0) is the "not judged yet" signal: the dilution invariant skips
    on None, and 0 would be a live comparison against a number nobody chose.
    """
    row = overrides.get(key) or {}
    raw = row.get("expected_n")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
