"""Runner for resolving Python dependencies for classified PRs."""

from __future__ import annotations

import asyncio
import functools
import json
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.resolve_packages")


class ResolvePackagesRunner(BaseRunner):
    """Resolve dependencies for classified PRs and persist to the packages table."""

    def __init__(self, n_concurrent: int = 16) -> None:
        super().__init__(name="resolve_packages", n_concurrent=n_concurrent)

    async def _process_item(self, item: Any) -> None:
        """Process an item dict with owner, repo, sha."""
        owner = item["owner"]
        repo = item["repo"]
        sha = item["sha"]

        from datasmith.resolution import analyze_commit

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, functools.partial(analyze_commit, sha, f"{owner}/{repo}"))

        client = get_client()

        if result is None:
            logger.info("Resolution returned None for %s/%s@%s", owner, repo, sha[:8])
            return

        env_payload = json.dumps(result.env_payload)

        # The columns are still today's. The provenance the resolver now carries
        # -- probe_status, interpreter_source, cutoff_used, dropped_requirements
        # -- has nowhere to go until the schema migration adds it.
        # ``can_install`` is the probe's verdict under its old name, and it has to
        # keep the old name's meaning while stages 5 and 6 still read it as a
        # gate: "uv could install this". The predecessor tried the commit-date
        # cutoff and then dropped it (``for strict_cutoff in (True, False)``) and
        # set can_install from the dry-run either way, and a seed with nothing to
        # install dry-ran clean. So ``unresolved`` (cutoff relaxed, dry-run clean)
        # and ``empty`` (nothing to install) are both installable; only ``failed``
        # is not. Reading it as ``== "installable"`` would add a cutoff condition
        # the column never had and starve the two stages this redesign exists to
        # unblock.
        row = {
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "package_name": result.package_name,
            "package_version": result.package_version,
            "python_version": result.python_version,
            "env_payload": env_payload,
            "build_commands": None,
            "install_commands": None,
            "primary_root": result.primary_root,
            "resolution_strategy": None,
            "can_install": result.probe_status != "failed",
            "requires_python": None,
        }

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
