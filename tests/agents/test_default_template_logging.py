"""The no-agent build path must record its own outcome.

TRY_DEFAULT is the only path that can build without an agent. Its success rate
is the number that decides how much agent work the pipeline needs, and nothing
recorded it. Rows use agent_name="default_template" so the rate is one query.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.agents.synthesizer import Synthesizer


def _rows_written(mock_client: MagicMock) -> list[dict]:
    table = mock_client.return_value.table
    return [call.args[0] for call in table.return_value.insert.call_args_list]


class TestDefaultTemplateLogging:
    def test_success_writes_a_row_marked_default_template(self):
        synth = Synthesizer(agent="codex")
        with patch("datasmith.agents.synthesizer.get_client") as client:
            synth._log_default_attempt(
                owner="pandas-dev",
                repo="pandas",
                sha="a" * 40,
                issue_number=43524,
                success=True,
                duration_s=446.0,
                error_message=None,
            )
        rows = _rows_written(client)
        assert len(rows) == 1
        assert rows[0]["agent_name"] == "default_template"
        assert rows[0]["success"] is True
        assert rows[0]["issue_number"] == 43524

    def test_failure_records_the_message(self):
        synth = Synthesizer(agent="codex")
        with patch("datasmith.agents.synthesizer.get_client") as client:
            synth._log_default_attempt(
                owner="apache",
                repo="arrow",
                sha="b" * 40,
                issue_number=44236,
                success=False,
                duration_s=1606.0,
                error_message="pkg stage failed",
            )
        rows = _rows_written(client)
        assert rows[0]["success"] is False
        assert rows[0]["failure_stage"] == "default_template"
        assert "pkg stage failed" in rows[0]["error_message"]

    def test_a_supabase_outage_does_not_raise(self):
        """Logging is never allowed to fail a build."""
        synth = Synthesizer(agent="codex")
        with patch("datasmith.agents.synthesizer.get_client", side_effect=RuntimeError("down")):
            synth._log_default_attempt(
                owner="networkx",
                repo="networkx",
                sha="c" * 40,
                issue_number=8148,
                success=True,
                duration_s=1.0,
                error_message=None,
            )
