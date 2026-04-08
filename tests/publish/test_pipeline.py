from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datasmith.publish.pipeline import publish_pipeline


@pytest.mark.asyncio
class TestPublishPipeline:
    async def test_marks_published(self):
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.update.return_value = mock_table
        mock_table.eq.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        mock_record = MagicMock()
        mock_record.owner = "org"
        mock_record.repo = "repo"
        mock_record.issue_number = 1
        mock_record.task_id = "org__repo-1"
        mock_record.container_name = ""

        with (
            patch("datasmith.publish.pipeline.records_from_supabase", return_value=[mock_record]),
            patch("datasmith.publish.pipeline.get_client", return_value=mock_client),
        ):
            count = await publish_pipeline("2024-01-01", "2024-12-31", dockerhub_push=False, hf_publish=False)

        assert count == 1
        mock_table.update.assert_called_once()

    async def test_skips_already_published(self):
        with patch("datasmith.publish.pipeline.records_from_supabase", return_value=[]):
            count = await publish_pipeline("2024-01-01", "2024-12-31")

        assert count == 0

    async def test_returns_record_count(self):
        records = [
            MagicMock(owner="o", repo="r", issue_number=i, task_id=f"o__r-{i}", container_name="") for i in range(5)
        ]

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.update.return_value = mock_table
        mock_table.eq.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        with (
            patch("datasmith.publish.pipeline.records_from_supabase", return_value=records),
            patch("datasmith.publish.pipeline.get_client", return_value=mock_client),
        ):
            count = await publish_pipeline("2024-01-01", "2024-12-31", dockerhub_push=False, hf_publish=False)

        assert count == 5
