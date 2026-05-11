"""Runner for stage 9: scrape ASV benchmark source code per (owner, repo, sha).

Reads candidate_containers for the date window, checks out each repo at its
SHA via :func:`prepare_repo_checkout`, parses ``benchmarks/*.py`` with the
extractor in :mod:`datasmith.scrape.benchmark_source`, and upserts rows into
``benchmark_codes``.
"""

from __future__ import annotations

import asyncio
import functools
import tempfile
from pathlib import Path
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.scrape.benchmark_source import BenchmarkSource, extract_benchmarks
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.scrape_benchmark_source")


def _scrape_one(owner: str, repo: str, sha: str) -> list[BenchmarkSource]:
    """Synchronous worker — runs in a thread pool because GitPython is blocking."""
    from datasmith.resolution.git_utils import prepare_repo_checkout

    repo_name = f"{owner}/{repo}"
    with tempfile.TemporaryDirectory(prefix="fc-bench-scrape-") as tmp:
        _, repo_dir, cleanup = prepare_repo_checkout(repo_name, sha, Path(tmp))
        try:
            return extract_benchmarks(Path(repo_dir))
        finally:
            try:
                cleanup()
            except Exception:
                logger.debug("worktree cleanup failed for %s@%s", repo_name, sha[:8])


class ScrapeBenchmarkSourceRunner(BaseRunner):
    """Stage 9 — populate ``benchmark_codes`` from candidate containers."""

    def __init__(self, n_concurrent: int = 8) -> None:
        super().__init__(name="scrape_benchmark_source", n_concurrent=n_concurrent)

    async def _process_item(self, item: Any) -> None:
        owner = item["owner"]
        repo = item["repo"]
        sha = item["sha"]

        loop = asyncio.get_running_loop()
        benches = await loop.run_in_executor(None, functools.partial(_scrape_one, owner, repo, sha))

        if not benches:
            logger.info("no ASV benchmarks for %s/%s@%s", owner, repo, sha[:8])
            return

        rows = [
            {
                "owner": owner,
                "repo": repo,
                "benchmark_without_params": b.benchmark_without_params,
                "source": b.source,
                "setup_source": b.setup_source,
                "last_scraped_sha": sha,
            }
            for b in benches
        ]

        client = get_client()
        # Upsert in chunks; PostgREST request size is bounded.
        for start in range(0, len(rows), 200):
            chunk = rows[start : start + 200]
            client.table("benchmark_codes").upsert(chunk, on_conflict="owner,repo,benchmark_without_params").execute()

        logger.info("scraped %d benchmarks for %s/%s@%s", len(rows), owner, repo, sha[:8])
