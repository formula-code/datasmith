from __future__ import annotations

from unittest.mock import patch

import pytest

from datasmith.github.models import FormulaCodeRecord
from datasmith.publish.records import records_from_parquet, records_from_supabase, records_to_parquet


def _make_record(**kwargs):
    defaults = {
        "owner": "test-org",
        "repo": "test-repo",
        "issue_number": 42,
        "task_id": 42,
        "gt_hash": "abc123",
        "base_commit": "def456",
    }
    defaults.update(kwargs)
    return FormulaCodeRecord(**defaults)


class TestRecordsToParquet:
    def test_roundtrip(self):
        records = [_make_record(), _make_record(issue_number=43, task_id=43)]
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

    def test_task_id_is_issue_number(self):
        r = _make_record()
        assert r.task_id == r.issue_number == 42

    def test_required_fields_validation(self):
        with pytest.raises((TypeError, Exception)):
            FormulaCodeRecord()  # missing required fields


class TestRecordsFromSupabase:
    def test_queries_supabase(self):
        fake_rows = [
            {
                "owner": "org",
                "repo": "repo",
                "issue_number": 1,
                "merge_commit_sha": "abc",
                "base_sha": "def",
                "merged_at": "2024-01-01T00:00:00Z",
            },
        ]
        fake_harbor_rows = [
            {
                "owner": "org",
                "repo": "repo",
                "sha": "abc",
                "max_speedup": 2.0,
                "status": "success",
                "environment": "daytona",
            },
        ]

        with patch("datasmith.publish.records.fetch_all", side_effect=[fake_rows, fake_harbor_rows]) as mock_fetch:
            records = records_from_supabase(start_date="2024-01-01", end_date="2024-12-31")

        assert len(records) == 1
        assert records[0].owner == "org"
        assert records[0].task_id == 1

        # Verify fetch_all was called with correct filters (first call = pull_requests)
        first_call_kwargs = mock_fetch.call_args_list[0]
        assert first_call_kwargs[1]["filters"] == {"is_performance_commit": True}
        assert first_call_kwargs[1]["gte_filters"] == {"merged_at": "2024-01-01"}
        assert first_call_kwargs[1]["lte_filters"] == {"merged_at": "2024-12-31"}
