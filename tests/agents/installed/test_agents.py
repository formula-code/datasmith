"""Tests for datasmith.agents.installed — InstalledAgent abstraction."""

from __future__ import annotations

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from datasmith.agents.installed import (
    AgentResult,
    ClaudeAgent,
    CodexAgent,
    CodexResult,
    GeminiAgent,
    get_agent,
)
from datasmith.agents.installed.claude import _parse_claude_stdout
from datasmith.agents.installed.codex import _parse_codex_stdout
from datasmith.agents.installed.gemini import _parse_gemini_stdout

# ---- AgentResult ----


class TestAgentResult:
    def test_defaults(self) -> None:
        r = AgentResult(success=False)
        assert r.success is False
        assert r.output == ""
        assert r.files_changed == []
        assert r.duration_s == 0.0
        assert r.error == ""

    def test_codex_result_alias(self) -> None:
        """CodexResult is the same class as AgentResult."""
        assert CodexResult is AgentResult

    def test_with_values(self) -> None:
        r = AgentResult(success=True, output="ok", files_changed=["a.py"], duration_s=1.5)
        assert r.success is True
        assert r.files_changed == ["a.py"]
        assert r.duration_s == 1.5


# ---- get_agent auto-detection ----


class TestGetAgent:
    @patch("shutil.which")
    def test_default_prefers_claude(self, mock_which: MagicMock) -> None:
        mock_which.side_effect = lambda b: "/usr/bin/claude" if b == "claude" else None
        agent = get_agent()
        assert agent.name() == "claude"

    @patch("shutil.which")
    def test_falls_back_to_codex(self, mock_which: MagicMock) -> None:
        mock_which.side_effect = lambda b: "/usr/bin/codex" if b == "codex" else None
        agent = get_agent()
        assert agent.name() == "codex"

    @patch("shutil.which")
    def test_falls_back_to_gemini(self, mock_which: MagicMock) -> None:
        mock_which.side_effect = lambda b: "/usr/bin/gemini" if b == "gemini" else None
        agent = get_agent()
        assert agent.name() == "gemini"

    @patch("shutil.which", return_value=None)
    def test_raises_when_none_available(self, _mock: MagicMock) -> None:
        with pytest.raises(RuntimeError, match="No installed CLI agent found"):
            get_agent()

    @patch("shutil.which")
    def test_custom_preference(self, mock_which: MagicMock) -> None:
        mock_which.side_effect = lambda b: f"/usr/bin/{b}" if b in ("claude", "gemini") else None
        agent = get_agent(preference=["gemini", "claude"])
        assert agent.name() == "gemini"


# ---- is_available ----


class TestIsAvailable:
    @patch("shutil.which", return_value="/usr/bin/codex")
    def test_codex_available(self, _mock: MagicMock) -> None:
        assert CodexAgent().is_available() is True

    @patch("shutil.which", return_value=None)
    def test_codex_unavailable(self, _mock: MagicMock) -> None:
        assert CodexAgent().is_available() is False

    @patch("shutil.which", return_value="/usr/bin/claude")
    def test_claude_available(self, _mock: MagicMock) -> None:
        assert ClaudeAgent().is_available() is True

    @patch("shutil.which", return_value=None)
    def test_claude_unavailable(self, _mock: MagicMock) -> None:
        assert ClaudeAgent().is_available() is False

    @patch("shutil.which", return_value="/usr/bin/gemini")
    def test_gemini_available(self, _mock: MagicMock) -> None:
        assert GeminiAgent().is_available() is True

    @patch("shutil.which", return_value=None)
    def test_gemini_unavailable(self, _mock: MagicMock) -> None:
        assert GeminiAgent().is_available() is False


# ---- CodexAgent.exec ----


class TestCodexAgent:
    @patch("datasmith.agents.installed.codex.subprocess.run")
    def test_exec_success(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout='{"output": "build complete"}\n',
            stderr="",
        )
        result = CodexAgent().exec("build it")
        assert result.success is True
        assert "build complete" in result.output

    @patch("datasmith.agents.installed.codex.subprocess.run")
    def test_exec_timeout(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="codex", timeout=900)
        result = CodexAgent().exec("slow", timeout=900)
        assert result.success is False
        assert "timed out" in result.error

    @patch("datasmith.agents.installed.codex.subprocess.run")
    def test_exec_not_found(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = FileNotFoundError()
        result = CodexAgent().exec("prompt")
        assert result.success is False
        assert "not found" in result.error

    @patch("datasmith.agents.installed.codex.subprocess.run")
    def test_exec_workdir(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        CodexAgent().exec("prompt", workdir="/tmp/test")
        assert mock_run.call_args.kwargs["cwd"] == "/tmp/test"

    @patch("datasmith.agents.installed.codex.subprocess.run")
    def test_full_auto_sandbox_flags(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        CodexAgent(full_auto=True, sandbox="danger-full-access").exec("prompt")
        cmd = mock_run.call_args[0][0]
        assert "--full-auto" in cmd
        assert "--sandbox" in cmd
        assert "--dangerously-bypass-approvals-and-sandbox" not in cmd


# ---- ClaudeAgent.exec ----


class TestClaudeAgent:
    @patch("datasmith.agents.installed.claude.subprocess.run")
    def test_exec_success(self, mock_run: MagicMock) -> None:
        stdout_lines = [
            json.dumps({"type": "assistant", "message": "I fixed the build"}),
            json.dumps({"type": "tool_use", "name": "Edit", "input": {"file_path": "fix.py"}}),
            json.dumps({"type": "result", "result": "Done"}),
        ]
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="\n".join(stdout_lines) + "\n",
            stderr="",
        )
        result = ClaudeAgent().exec("fix the build")
        assert result.success is True
        assert "I fixed the build" in result.output
        assert "fix.py" in result.files_changed

    @patch("datasmith.agents.installed.claude.subprocess.run")
    def test_exec_timeout(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="claude", timeout=600)
        result = ClaudeAgent().exec("slow", timeout=600)
        assert result.success is False
        assert "timed out" in result.error

    @patch("datasmith.agents.installed.claude.subprocess.run")
    def test_exec_not_found(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = FileNotFoundError()
        result = ClaudeAgent().exec("prompt")
        assert result.success is False
        assert "not found" in result.error

    @patch("datasmith.agents.installed.claude.subprocess.run")
    def test_nesting_guard(self, mock_run: MagicMock) -> None:
        """CLAUDE_CODE_ENTRYPOINT and CLAUDECODE should be removed from subprocess env."""
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        import os

        with patch.dict(os.environ, {"CLAUDE_CODE_ENTRYPOINT": "cli", "CLAUDECODE": "1"}):
            ClaudeAgent().exec("prompt")
        call_env = mock_run.call_args.kwargs["env"]
        assert "CLAUDE_CODE_ENTRYPOINT" not in call_env
        assert "CLAUDECODE" not in call_env

    @patch("datasmith.agents.installed.claude.subprocess.run")
    def test_command_flags(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        ClaudeAgent().exec("prompt")
        cmd = mock_run.call_args[0][0]
        assert "--dangerously-skip-permissions" in cmd
        assert "--output-format" in cmd
        assert "stream-json" in cmd
        assert "--no-session-persistence" in cmd


# ---- GeminiAgent.exec ----


class TestGeminiAgent:
    @patch("datasmith.agents.installed.gemini.subprocess.run")
    def test_exec_success(self, mock_run: MagicMock) -> None:
        stdout_lines = [
            json.dumps({"type": "response", "text": "Fixed!"}),
            json.dumps({"output": "extra info"}),
        ]
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="\n".join(stdout_lines) + "\n",
            stderr="",
        )
        result = GeminiAgent().exec("fix it")
        assert result.success is True
        assert "Fixed!" in result.output

    @patch("datasmith.agents.installed.gemini.subprocess.run")
    def test_exec_timeout(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="gemini", timeout=600)
        result = GeminiAgent().exec("slow", timeout=600)
        assert result.success is False
        assert "timed out" in result.error

    @patch("datasmith.agents.installed.gemini.subprocess.run")
    def test_exec_not_found(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = FileNotFoundError()
        result = GeminiAgent().exec("prompt")
        assert result.success is False
        assert "not found" in result.error

    @patch("datasmith.agents.installed.gemini.subprocess.run")
    def test_command_flags(self, mock_run: MagicMock) -> None:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        GeminiAgent().exec("prompt")
        cmd = mock_run.call_args[0][0]
        assert "--yolo" in cmd


# ---- Dry-run mode ----


class TestDryRun:
    def test_exec_or_dry_run(self) -> None:
        agent = CodexAgent()
        result = agent.exec_or_dry_run("prompt", dry_run=True)
        assert result.success is True
        assert "dry run" in result.output
        assert result.duration_s == 0.0


# ---- Parser unit tests ----


class TestParseCodexStdout:
    def test_agent_message(self) -> None:
        line = json.dumps({"item": {"type": "agent_message", "text": "hello"}})
        out, files = _parse_codex_stdout(line)
        assert out == ["hello"]
        assert files == []

    def test_file_change(self) -> None:
        line = json.dumps({"file": "src/main.py"})
        _out, files = _parse_codex_stdout(line)
        assert files == ["src/main.py"]

    def test_non_json(self) -> None:
        out, files = _parse_codex_stdout("plain text\n")
        assert out == ["plain text"]
        assert files == []


class TestParseClaudeStdout:
    def test_assistant_string_message(self) -> None:
        line = json.dumps({"type": "assistant", "message": "hello"})
        out, _files = _parse_claude_stdout(line)
        assert out == ["hello"]

    def test_tool_use_file(self) -> None:
        line = json.dumps({"type": "tool_use", "name": "Edit", "input": {"file_path": "a.py"}})
        _out, files = _parse_claude_stdout(line)
        assert files == ["a.py"]

    def test_result(self) -> None:
        line = json.dumps({"type": "result", "result": "All done"})
        out, _files = _parse_claude_stdout(line)
        assert "All done" in out


class TestParseGeminiStdout:
    def test_response(self) -> None:
        line = json.dumps({"type": "response", "text": "done"})
        out, _files = _parse_gemini_stdout(line)
        assert out == ["done"]

    def test_output(self) -> None:
        line = json.dumps({"output": "info"})
        out, _files = _parse_gemini_stdout(line)
        assert out == ["info"]
