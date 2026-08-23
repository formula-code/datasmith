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
import functools
import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.resolve_packages")

# Threads running ``git clone`` plus ``uv pip compile``.  Disk-bound rather
# than CPU-bound, which is why the default is a modest fixed number instead of
# a function of ``cpu_count()``: an operator who has measured their own disk
# raises it in ``tokens.env``.
DATASMITH_RESOLVE_PACKAGES_WORKERS: int = int(os.environ.get("DATASMITH_RESOLVE_PACKAGES_WORKERS", "8"))


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

        env_payload = json.dumps(result.get("final_dependencies", []))

        row = {
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "package_name": result.get("package_name"),
            "package_version": result.get("package_version"),
            "python_version": result.get("python_version", ""),
            "env_payload": env_payload,
            "build_commands": result.get("build_command"),
            "install_commands": result.get("install_command"),
            "primary_root": result.get("primary_root"),
            "resolution_strategy": result.get("resolution_strategy"),
            "can_install": result.get("can_install", False),
            "requires_python": None,
        }

        client.table("packages").upsert(row).execute()
        logger.info(
            "Resolved %s/%s@%s: python=%s can_install=%s deps=%d",
            owner,
            repo,
            sha[:8],
            result.get("python_version"),
            result.get("can_install"),
            len(result.get("final_dependencies", [])),
        )
