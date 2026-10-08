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

        fake_verified_rows = [{"owner": "org", "repo": "repo", "sha": "abc"}]

        with patch(
            "datasmith.publish.records.fetch_all",
            side_effect=[fake_rows, fake_harbor_rows, fake_verified_rows],
        ) as mock_fetch:
            records = records_from_supabase(start_date="2024-01-01", end_date="2024-12-31")

        assert len(records) == 1
        assert records[0].owner == "org"
        assert records[0].task_id == 1

        # Verify fetch_all was called with correct filters (first call = pull_requests).
        # The upper bound is strict: the window is half-open, so ``lte_filters``
        # must not appear at all. See ``datasmith.utils.db.window_filters``.
        first_call_kwargs = mock_fetch.call_args_list[0]
        assert first_call_kwargs[1]["filters"] == {"is_performance_commit": True}
        assert first_call_kwargs[1]["gte_filters"] == {"merged_at": "2024-01-01"}
        assert first_call_kwargs[1]["lt_filters"] == {"merged_at": "2024-12-31"}
        assert "lte_filters" not in first_call_kwargs[1]

    def test_select_is_narrowed_to_the_columns_the_record_uses(self):
        """``select="*"`` streamed every text column of every matching row.

        That is the shape of the read that killed PostgREST with "cannot
        enlarge string buffer containing 1073741822 bytes".  ``patch`` is
        still selected because the record requires it; the point is that
        ``body``, ``problem_description`` and the rest no longer ride along.
        """
        with patch("datasmith.publish.records.fetch_all", side_effect=[[], [], []]) as mock_fetch:
            records_from_supabase(start_date="2024-01-01", end_date="2024-12-31")

        select = mock_fetch.call_args_list[0][1]["select"]
        assert select != "*"
        columns = {c.strip() for c in select.split(",")}
        # Every column the record constructor actually reads.
        assert columns == {
            "owner",
            "repo",
            "issue_number",
            "merge_commit_sha",
            "base_sha",
            "merged_at",
            "rendered_problem",
            "classification",
            "difficulty",
            "container_name",
            "patch",
        }


class TestWhichHarborEnvironmentsMayGatePublication:
    """The gate is a knob, not a hardcoded string — but a narrow one.

    Default stays Daytona-only: local Docker trials share the build host with
    everything else and their timings move with load. An operator without
    Daytona access would otherwise have to edit the gate itself, and a gate
    edited under deadline is a gate that quietly stops gating.

    No `importlib.reload` here. Reloading rebinds the module's globals and
    breaks every `from datasmith.publish.records import ...` reference held
    elsewhere, so a patch lands on the new module while the stale function
    object keeps the real `fetch_all` — which is exactly how an earlier
    version of these tests made `tests/update/test_window_contract.py` read
    the live database and fail.
    """

    def _captured_filters(self, monkeypatch, envs: tuple[str, ...]):
        from unittest.mock import patch

        import datasmith.publish.records as rec

        monkeypatch.setattr(rec, "DATASMITH_PUBLISH_ENVIRONMENTS", envs)
        captured: dict = {}

        def fake_fetch_all(table, **kwargs):
            if table == "harbor_runs":
                captured["in_filters"] = kwargs.get("in_filters")
            return []

        with patch.object(rec, "fetch_all", side_effect=fake_fetch_all):
            rec.records_from_supabase("2017-01-01", "2030-12-31")
        return captured

    def test_the_default_is_daytona_only(self) -> None:
        import datasmith.publish.records as rec

        assert rec._publish_environments("daytona") == ("daytona",)

    def test_the_query_uses_whatever_is_configured(self, monkeypatch) -> None:
        captured = self._captured_filters(monkeypatch, ("docker", "daytona"))
        assert captured["in_filters"] == {"environment": ["docker", "daytona"]}

    def test_daytona_only_is_the_shipped_behaviour(self, monkeypatch) -> None:
        captured = self._captured_filters(monkeypatch, ("daytona",))
        assert captured["in_filters"] == {"environment": ["daytona"]}

    def test_whitespace_and_blanks_are_tolerated(self) -> None:
        import datasmith.publish.records as rec

        assert rec._publish_environments(" docker , , daytona ") == ("docker", "daytona")

    def test_widening_the_environment_does_not_relax_the_speedup_floor(self, monkeypatch) -> None:
        """The whole point of the gate survives the knob."""
        import datasmith.publish.records as rec

        monkeypatch.setattr(rec, "DATASMITH_PUBLISH_ENVIRONMENTS", ("docker",))
        assert rec.MIN_HARBOR_SPEEDUP == 1.05
