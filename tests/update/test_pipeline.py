from __future__ import annotations

from unittest.mock import patch

import pytest

from datasmith.update.pipeline import STAGES, Pipeline


@pytest.mark.asyncio
class TestPipeline:
    async def test_stages_run_in_order(self):
        pipeline = Pipeline()
        calls = []

        async def mock_stage(name, *args, **kwargs):
            calls.append(name)

        async def mock_run_stage(name, start, end):
            calls.append(name)

        with patch.object(pipeline, "_mark_stage_completed"):
            pipeline._run_stage = mock_run_stage  # type: ignore[assignment]
            await pipeline.run("2024-01-01", "2024-12-31")

        assert calls == STAGES

    async def test_resume_skips_completed(self):
        pipeline = Pipeline()
        calls = []

        async def mock_run_stage(name, start, end):
            calls.append(name)

        with (
            patch.object(pipeline, "_get_completed_stages", return_value=["scrape_repos", "scrape_commits"]),
            patch.object(pipeline, "_mark_stage_completed"),
        ):
            pipeline._run_stage = mock_run_stage  # type: ignore[assignment]
            await pipeline.run("2024-01-01", "2024-12-31", resume=True)

        assert "scrape_repos" not in calls
        assert "scrape_commits" not in calls
        assert "classify_prs" in calls

    async def test_stops_on_failure(self):
        pipeline = Pipeline()
        calls = []

        async def mock_run_stage(name, start, end):
            calls.append(name)
            if name == "scrape_commits":
                raise RuntimeError("boom")

        with patch.object(pipeline, "_mark_stage_completed"):
            pipeline._run_stage = mock_run_stage  # type: ignore[assignment]
            with pytest.raises(RuntimeError, match="boom"):
                await pipeline.run("2024-01-01", "2024-12-31")

        assert "scrape_repos" in calls
        assert "scrape_commits" in calls
        assert "classify_prs" not in calls

    async def test_dry_run_no_stage_completion(self):
        pipeline = Pipeline(dry_run=True)
        calls = []

        async def mock_run_stage(name, start, end):
            calls.append(name)

        mark_calls = []
        original_mark = pipeline._mark_stage_completed

        def tracking_mark(name):
            mark_calls.append(name)

        pipeline._run_stage = mock_run_stage  # type: ignore[assignment]
        pipeline._mark_stage_completed = tracking_mark  # type: ignore[assignment]
        await pipeline.run("2024-01-01", "2024-12-31")

        # Stages are still dispatched (for summary collection) but never marked completed.
        assert calls == STAGES
        assert mark_calls == []
        assert pipeline._completed_stages == []

    async def test_single_stage_execution(self):
        pipeline = Pipeline()
        calls = []

        async def mock_run_stage(name, start, end):
            calls.append(name)

        with patch.object(pipeline, "_mark_stage_completed"):
            pipeline._run_stage = mock_run_stage  # type: ignore[assignment]
            await pipeline.run("2024-01-01", "2024-12-31", stage=6)

        assert calls == ["synthesize_images"]

    async def test_invalid_stage_raises(self):
        pipeline = Pipeline()
        with pytest.raises(ValueError, match="Stage must be"):
            await pipeline.run("2024-01-01", "2024-12-31", stage=99)

    async def test_date_filtering_passed_through(self):
        pipeline = Pipeline()
        received_dates = []

        async def mock_run_stage(name, start, end):
            received_dates.append((start, end))

        with patch.object(pipeline, "_mark_stage_completed"):
            pipeline._run_stage = mock_run_stage  # type: ignore[assignment]
            await pipeline.run("2024-06-01", "2024-06-30")

        for start, end in received_dates:
            assert start == "2024-06-01"
            assert end == "2024-06-30"
