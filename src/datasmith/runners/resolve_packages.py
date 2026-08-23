"""Runner for resolving Python dependencies for classified PRs.

This is the stage that cores scale.  ``analyze_commit`` shells out to ``git
clone`` and ``uv pip compile``, so its parallelism is a property of the
machine — unlike stages 2 and 3, whose ceiling is one GitHub token and does
not care how large the host is.  The two live on separate knobs so raising one
cannot trip the other's rate limits (spec section 6.4).

The work used to run on ``run_in_executor(None, ...)``, the interpreter's
default pool, sized ``min(32, cpu_count + 4)``.  That silently means something
different on every machine, and on a 128-core host it becomes 32 concurrent
clones and compiles — all of them contending for the same disk.  It is also
shared: anything else in the process that reaches for the default executor
queues behind the clones.  The pool here is explicit, bounded, and owned by
the runner.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import functools
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

if TYPE_CHECKING:  # pragma: no cover - import cycle avoidance, typing only
    from datasmith.resolution.orchestrator import ResolutionResult

logger = get_logger("runners.resolve_packages")

# Threads running ``git clone`` plus ``uv pip compile``.  Disk-bound rather
# than CPU-bound, which is why the default is a modest fixed number instead of
# a function of ``cpu_count()``: an operator who has measured their own disk
# raises it in ``tokens.env``.
DATASMITH_RESOLVE_PACKAGES_WORKERS: int = int(os.environ.get("DATASMITH_RESOLVE_PACKAGES_WORKERS", "8"))


@functools.lru_cache(maxsize=1)
def _uv_version() -> str:
    """Report the ``uv`` that compiled the seed, once per process.

    Provenance is the point: two rows resolved months apart differ in what
    resolved them, and until now nothing recorded it.  A version that cannot be
    read is ``unknown`` -- honest, and never a reason to lose the row.
    """
    try:
        proc = subprocess.run(["uv", "--version"], capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    out = (proc.stdout or proc.stderr or "").strip()
    return out or "unknown"


def build_row(owner: str, repo: str, sha: str, result: ResolutionResult) -> dict[str, Any]:
    """Map a resolution result onto a ``packages`` row.

    ``requires_python`` is stored. Its predecessor hardcoded ``None`` here while
    the parsed value was computed upstream and discarded.

    ``build_commands``, ``install_commands`` and ``resolution_strategy`` are gone:
    a reader audit found zero consumers outside this module for all three, and
    the explicit provenance columns say what ``resolution_strategy`` was trying to.

    ``can_install`` is gone too.  It was the probe's verdict under a name that
    claimed "this builds", and stages 5 and 6 read it as a gate; the column is
    retained and nullable for compatibility, and a row this resolver writes
    leaves it null because this resolver never answers that question.
    """
    return {
        "owner": owner,
        "repo": repo,
        "sha": sha,
        "package_name": result.package_name,
        "package_version": result.package_version,
        "primary_root": result.primary_root,
        "requires_python": result.requires_python,
        "python_version": result.python_version,
        "interpreter_source": result.interpreter_source,
        "env_payload": json.dumps(result.env_payload),
        "probe_status": result.probe_status,
        "probe_log": result.probe_log,
        "cutoff_used": result.cutoff_used,
        "dropped_requirements": json.dumps(result.dropped_requirements),
        "resolver_version": result.resolver_version,
        "uv_version": _uv_version(),
        "resolved_at": dt.datetime.now(dt.UTC).isoformat(),
    }


class ResolvePackagesRunner(BaseRunner):
    """Resolve dependencies for classified PRs and persist to the packages table."""

    def __init__(self, n_concurrent: int = 16, max_workers: int | None = None) -> None:
        super().__init__(name="resolve_packages", n_concurrent=n_concurrent)
        self._max_workers = max(1, DATASMITH_RESOLVE_PACKAGES_WORKERS if max_workers is None else max_workers)
        self._executor: ThreadPoolExecutor | None = None

    async def run(self, items: list[Any]) -> None:
        """Run the stage against a pool this runner owns and shuts down.

        ``n_concurrent`` still bounds how many coroutines are in flight; the
        pool bounds how many of them are actually cloning at once.  They are
        separate numbers on purpose — the first is a queue depth, the second
        is what the disk has to survive.
        """
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix="resolve-packages",
        )
        logger.info(
            "Resolving with %d worker thread(s), %d concurrent item(s)",
            self._max_workers,
            self._n_concurrent,
        )
        try:
            await super().run(items)
        finally:
            # Shut down even when the stage raises, so a failed run does not
            # leave clone threads alive behind it.
            self._executor.shutdown(wait=True)
            self._executor = None

    async def _process_item(self, item: Any) -> None:
        """Process an item dict with owner, repo, sha."""
        owner = item["owner"]
        repo = item["repo"]
        sha = item["sha"]

        from datasmith.resolution import analyze_commit

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self._executor, functools.partial(analyze_commit, sha, f"{owner}/{repo}"))

        client = get_client()

        if result is None:
            logger.info("Resolution returned None for %s/%s@%s", owner, repo, sha[:8])
            return

        row = build_row(owner, repo, sha, result)

        client.table("packages").upsert(row).execute()
        logger.info(
            "Resolved %s/%s@%s: python=%s (%s) probe=%s deps=%d",
            owner,
            repo,
            sha[:8],
            result.python_version,
            result.interpreter_source,
            result.probe_status,
            len(result.env_payload),
        )
