from __future__ import annotations

import asyncio
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_logger

logger = get_logger("runners.synthesize_images")


class SynthesizeImagesRunner(BaseRunner):
    """Run Synthesizer for each PR to produce Docker build contexts."""

    def __init__(self, synthesizer: Any, verifier: Any, n_concurrent: int = 3) -> None:
        super().__init__(name="synthesize_images", n_concurrent=n_concurrent)
        self._synthesizer = synthesizer
        self._verifier = verifier

    async def _process_item(self, item: Any) -> None:
        """Process a PR dict with owner, repo, issue_number, pr_context."""
        owner = item["owner"]
        repo = item["repo"]
        issue_number = item["issue_number"]
        pr_context = item.get("pr_context", "")

        # Run synthesizer in thread (Docker operations are blocking)
        ctx = await asyncio.to_thread(
            self._synthesizer.run,
            owner,
            repo,
            issue_number,
            pr_context,
            self._verifier,
        )

        if ctx is not None:
            logger.info("Successfully synthesized image for %s/%s#%d", owner, repo, issue_number)
        else:
            raise RuntimeError(f"Synthesis failed for {owner}/{repo}#{issue_number}")
