from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datasmith.github.models import FormulaCodeRecord
from datasmith.publish.records import records_from_parquet, records_from_supabase, records_to_parquet


def _make_record(**kwargs):
    defaults = {
        "owner": "test-org",
        "repo": "test-repo",
        "issue_number": 42,
        "task_id": "test-org__test-repo-42",
        "gt_hash": "abc123",
        "base_commit": "def456",
    }
    defaults.update(kwargs)
    return FormulaCodeRecord(**defaults)


class TestRecordsToParquet:
    def test_roundtrip(self):
        records = [_make_record(), _make_record(issue_number=43, task_id="test-org__test-repo-43")]
        data = records_to_parquet(records)
        assert len(data) > 0
        restored = records_from_parquet(data)
        assert len(restored) == 2
        assert restored[0].owner == "test-org"
        assert restored[1].issue_number == 43

    def test_empty_records(self):
        assert records_to_parquet([]) == b""
        assert records_from_parquet(b"") == []

    def test_parquet_bytes_valid(self):
        import io

        import pyarrow.parquet as pq

        records = [_make_record()]
        data = records_to_parquet(records)
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == 1

    def test_task_id_format(self):
        r = _make_record()
        assert r.task_id == "test-org__test-repo-42"

    def test_required_fields_validation(self):
        with pytest.raises(TypeError):
            FormulaCodeRecord()  # missing required fields


class TestRecordsFromSupabase:
    def test_queries_supabase(self):
        mock_client = MagicMock()
        mock_query = MagicMock()
        mock_client.table.return_value = mock_query
        mock_query.select.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_query.is_.return_value = mock_query
        mock_query.gte.return_value = mock_query
        mock_query.lte.return_value = mock_query
        mock_query.execute.return_value = MagicMock(
            data=[
                {
                    "owner": "org",
                    "repo": "repo",
                    "issue_number": 1,
                    "merge_commit_sha": "abc",
                    "base_sha": "def",
                    "merged_at": "2024-01-01T00:00:00Z",
                },
            ]
        )

        with patch("datasmith.publish.records.get_client", return_value=mock_client):
            records = records_from_supabase(start_date="2024-01-01", end_date="2024-12-31")

        assert len(records) == 1
        assert records[0].owner == "org"
        assert records[0].task_id == "org__repo-1"
