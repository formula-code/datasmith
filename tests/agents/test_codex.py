"""Tests for datasmith.agents.codex."""

from __future__ import annotations

import json
import subprocess
from unittest.mock import MagicMock, patch

from datasmith.agents.codex import CodexResult, codex_exec


class TestCodexResult:
    def test_codex_result_defaults(self) -> None:
        """Defaults are correct."""
        result = CodexResult(success=False)
        assert result.success is False
        assert result.output == ""
        assert result.files_changed == []
        assert result.duration_s == 0.0
        assert result.error == ""

    def test_codex_result_with_values(self) -> None:
        result = CodexResult(
            success=True,
            output="done",
            files_changed=["a.py", "b.py"],
            duration_s=1.5,
            error="",
        )
        assert result.success is True
        assert result.output == "done"
        assert len(result.files_changed) == 2
        assert result.duration_s == 1.5


class TestCodexExec:
    @patch("datasmith.agents.codex.subprocess.run")
    def test_codex_exec_success(self, mock_run: MagicMock) -> None:
        """Mock subprocess returncode 0 produces CodexResult(success=True)."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"output": "build complete"}\n',
            stderr="",
        )
        result = codex_exec("build the project")
        assert result.success is True
        assert "build complete" in result.output
        assert result.error == ""

    @patch("datasmith.agents.codex.subprocess.run")
    def test_codex_exec_timeout(self, mock_run: MagicMock) -> None:
        """Mock TimeoutExpired produces appropriate error."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="codex", timeout=900)
        result = codex_exec("slow prompt", timeout=900)
        assert result.success is False
        assert "timed out" in result.error

    @patch("datasmith.agents.codex.subprocess.run")
    def test_codex_exec_captures_output(self, mock_run: MagicMock) -> None:
        """Verify stdout is captured and parsed."""
        lines = [
            json.dumps({"output": "line1"}),
            json.dumps({"file": "test.py"}),
            json.dumps({"message": "line2"}),
        ]
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="\n".join(lines) + "\n",
            stderr="",
        )
        result = codex_exec("do something")
        assert result.success is True
        assert "line1" in result.output
        assert "line2" in result.output
        assert "test.py" in result.files_changed

    @patch("datasmith.agents.codex.subprocess.run")
    def test_codex_exec_not_found(self, mock_run: MagicMock) -> None:
        """Mock FileNotFoundError when codex CLI is missing."""
        mock_run.side_effect = FileNotFoundError("codex not found")
        result = codex_exec("prompt")
        assert result.success is False
        assert "not found" in result.error

    @patch("datasmith.agents.codex.subprocess.run")
    def test_codex_exec_working_directory(self, mock_run: MagicMock) -> None:
        """Verify cwd is passed to subprocess."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="plain text output\n",
            stderr="",
        )
        test_dir = "/tmp/mydir"  # noqa: S108
        codex_exec("prompt", workdir=test_dir)
        call_kwargs = mock_run.call_args
        assert call_kwargs.kwargs["cwd"] == test_dir

    @patch("datasmith.agents.codex.subprocess.run")
    def test_codex_exec_nonzero_returncode(self, mock_run: MagicMock) -> None:
        """Non-zero returncode means success=False and error captured."""
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="something went wrong",
        )
        result = codex_exec("bad prompt")
        assert result.success is False
        assert result.error == "something went wrong"

    @patch("datasmith.agents.codex.subprocess.run")
    def test_codex_exec_generic_exception(self, mock_run: MagicMock) -> None:
        """Generic exception is caught and reported."""
        mock_run.side_effect = OSError("disk full")
        result = codex_exec("prompt")
        assert result.success is False
        assert "disk full" in result.error

    @patch("datasmith.agents.codex.subprocess.run")
    def test_codex_exec_non_json_stdout(self, mock_run: MagicMock) -> None:
        """Non-JSON stdout lines are preserved as output."""
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="plain text line\nanother line\n",
            stderr="",
        )
        result = codex_exec("prompt")
        assert result.success is True
        assert "plain text line" in result.output
        assert "another line" in result.output
