from __future__ import annotations

from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.classify_prs")


class ClassifyPRsRunner(BaseRunner):
    """Batch classification of PRs via LLM agents."""

    def __init__(self, classifier: Any, judge: Any, n_concurrent: int = 5) -> None:
        super().__init__(name="classify_prs", n_concurrent=n_concurrent)
        self._classifier = classifier
        self._judge = judge

    async def _process_item(self, item: Any) -> None:
        """Process a PR dict with owner, repo, issue_number, description, patch."""
        owner = item["owner"]
        repo = item["repo"]
        issue_number = item["issue_number"]
        description = item.get("description", "")
        patch = item.get("patch", "")
        file_change_summary = item.get("file_change_summary", "")

        is_perf, _reason = self._classifier.classify(description, patch, file_change_summary)

        update: dict[str, Any] = {"is_performance_commit": is_perf}

        if is_perf:
            decision = self._judge.classify(description, patch)
            update["classification"] = decision.category
            update["difficulty"] = decision.difficulty

        client = get_client()
        client.table("pull_requests").update(update).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

        logger.info("Classified %s/%s#%d: perf=%s", owner, repo, issue_number, is_perf)
