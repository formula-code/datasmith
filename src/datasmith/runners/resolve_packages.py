"""Runner for resolving Python dependencies for classified PRs.

``analyze_commit`` runs ``git clone`` and ``uv pip compile``, so this stage is
limited by the machine, not by GitHub. It uses its own bounded thread pool
instead of the default executor, whose size varies by host.
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

# Threads running ``git clone`` and ``uv pip compile``. A fixed default, because
# the work is limited by disk, not CPU.
DATASMITH_RESOLVE_PACKAGES_WORKERS: int = int(os.environ.get("DATASMITH_RESOLVE_PACKAGES_WORKERS", "8"))


@functools.lru_cache(maxsize=1)
def _uv_version() -> str:
    """Return the ``uv`` version that compiled the seed, or ``unknown``.

    Stored on each row as provenance.
    """
    try:
        proc = subprocess.run(["uv", "--version"], capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    out = (proc.stdout or proc.stderr or "").strip()
    return out or "unknown"


def build_row(owner: str, repo: str, sha: str, result: ResolutionResult) -> dict[str, Any]:
    """Map a resolution result onto a ``packages`` row.

    ``can_install`` is left null, because this resolver does not test whether
    the package builds.
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
        """Run the stage on a thread pool that is created here and shut down at the end.

        ``n_concurrent`` limits items in flight; the pool limits concurrent clones.
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
            # Shut down even when the stage raises, so no threads are left running.
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
