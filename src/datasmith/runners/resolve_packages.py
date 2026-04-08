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
