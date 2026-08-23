"""Runner for resolving Python dependencies for classified PRs."""

from __future__ import annotations

import asyncio
import datetime as dt
import functools
import json
import subprocess
from typing import TYPE_CHECKING, Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

if TYPE_CHECKING:  # pragma: no cover - import cycle avoidance, typing only
    from datasmith.resolution.orchestrator import ResolutionResult

logger = get_logger("runners.resolve_packages")


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
