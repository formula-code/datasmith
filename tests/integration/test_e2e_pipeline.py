from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datasmith.update.pipeline import Pipeline


@pytest.mark.asyncio
class TestE2EPipeline:
    async def test_full_pipeline_single_stage(self, mock_supabase_client):
        """Run a single pipeline stage end-to-end."""
        pipeline = Pipeline()

        mock_supabase_client.table.return_value.execute.return_value = MagicMock(data=[])

        with patch("datasmith.update.pipeline.get_client", return_value=mock_supabase_client):
            # Run only the publish stage (lightest)
            calls = []

            async def tracked_publish(start, end):
                calls.append(("publish", start, end))

            pipeline._publish = tracked_publish  # type: ignore[assignment]
            await pipeline.run("2024-01-01", "2024-12-31", stage=6)

        assert len(calls) == 1
        assert calls[0] == ("publish", "2024-01-01", "2024-12-31")

    async def test_pipeline_resumption_after_crash(self, mock_supabase_client):
        """Pipeline should resume from last completed stage."""
        pipeline = Pipeline()
        calls = []

        async def mock_run_stage(name, start, end):
            calls.append(name)

        completed_response = MagicMock(
            data=[
                {"runner_name": "scrape_repos", "completed": 10, "total": 10},
                {"runner_name": "scrape_commits", "completed": 5, "total": 5},
            ]
        )

        with patch("datasmith.update.pipeline.get_client", return_value=mock_supabase_client):
            mock_supabase_client.table.return_value.execute.return_value = completed_response
            pipeline._run_stage = mock_run_stage  # type: ignore[assignment]
            await pipeline.run("2024-01-01", "2024-12-31", resume=True)

        assert "scrape_repos" not in calls
        assert "scrape_commits" not in calls
        assert "classify_prs" in calls
