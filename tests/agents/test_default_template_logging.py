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


class TestTheCallSiteIsActuallyWired:
    """The tests above call `_log_default_attempt` directly, so they pass even if
    the TRY_DEFAULT block never calls it.

    That was proved by mutation: deleting the whole call site from the
    TRY_DEFAULT block left the direct-call tests green. Task 5 reads this rate,
    so a removed call site would make the trial silently measure zero with no
    test objecting. These tests exercise the wiring instead of the method.
    """

    @patch("datasmith.agents.synthesizer.verify_context")
    def test_a_failed_default_build_logs_a_row(self, mock_verify: MagicMock) -> None:
        from datasmith.agents.sandbox import SandboxResult
        from datasmith.agents.synthesizer import Synthesizer

        mock_verify.return_value = SandboxResult(
            success=False,
            failure_json={"stage": "run", "return_code": 1, "error_message": "asv: command not found"},
        )
        synth = Synthesizer()
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[]),
            patch.object(synth, "_sandbox_generate", return_value=(None, {}, None, False)),
            patch.object(synth, "_log_attempt"),
            patch.object(synth, "_log_default_attempt") as log_default,
        ):
            synth.run("networkx", "networkx", 8148, "ctx", sha="a" * 40)

        assert log_default.called, (
            "the TRY_DEFAULT block did not call _log_default_attempt. "
            "Task 5's trial reads these rows, so it would measure zero silently."
        )
        kwargs = log_default.call_args.kwargs
        assert kwargs["success"] is False
        assert kwargs["owner"] == "networkx"
        assert kwargs["issue_number"] == 8148

    @patch("datasmith.agents.synthesizer.verify_context")
    def test_a_successful_default_build_logs_a_row(self, mock_verify: MagicMock) -> None:
        from datasmith.agents.sandbox import SandboxResult
        from datasmith.agents.synthesizer import Synthesizer
        from datasmith.docker.context import DockerContext

        # image_tag is not decoration. TRY_DEFAULT now runs the host image scan
        # before it saves, and an image it cannot name is not clean -- so a
        # stub that reports success without a tag is refused, falls through to
        # LLM_GENERATE, and spawns a real agent. The real `verify_context`
        # always extracts a tag from verification_success.json on success.
        mock_verify.return_value = SandboxResult(
            success=True,
            docker_context=DockerContext(build_pkg_sh="#!/bin/bash\ntrue"),
            image_tag="formulacode/networkx-networkx:8148",
        )
        synth = Synthesizer()
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[]),
            patch.object(synth, "_save_context"),
            patch.object(synth, "_log_default_attempt") as log_default,
            # The scan itself is covered by tests/agents/test_try_default_host_scan.py.
            # Here it is stubbed clean so this test measures what it names --
            # that a successful default build logs a row -- without needing docker.
            patch("datasmith.agents.synthesizer._host_scan_findings", return_value=[]),
        ):
            synth.run("networkx", "networkx", 8148, "ctx", sha="a" * 40)

        assert log_default.called
        assert log_default.call_args.kwargs["success"] is True
