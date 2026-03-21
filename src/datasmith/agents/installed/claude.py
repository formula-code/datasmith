"""Claude Code CLI agent implementation."""

from __future__ import annotations

import json
import os
import subprocess
import time

from datasmith.agents.installed.base import AgentResult, InstalledAgent
from datasmith.utils import get_logger

logger = get_logger("agents.installed.claude")


def _extract_assistant_text(message: object) -> str:
    """Extract text from a Claude assistant message payload."""
    if isinstance(message, str):
        return message
    if isinstance(message, dict):
        content = message.get("content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block.get("text", ""))
            return "\n".join(parts)
    return ""


def _parse_claude_stdout(stdout: str) -> tuple[list[str], list[str]]:
    """Parse Claude Code stream-json output into (output_lines, files_changed)."""
    files_changed: list[str] = []
    output_lines: list[str] = []

    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if not isinstance(obj, dict):
                continue

            msg_type = obj.get("type", "")

            if msg_type == "assistant" and "message" in obj:
                text = _extract_assistant_text(obj["message"])
                if text:
                    output_lines.append(text)
            elif msg_type == "result":
                result_text = obj.get("result", "")
                if isinstance(result_text, str) and result_text:
                    output_lines.append(result_text)
            elif msg_type == "tool_use":
                _collect_file_change(obj, files_changed)

        except json.JSONDecodeError:
            output_lines.append(line)

    return output_lines, files_changed


_FILE_TOOL_NAMES = {"Write", "Edit", "write_file", "edit_file"}


def _collect_file_change(obj: dict, files_changed: list[str]) -> None:
    """Extract file path from a tool_use event if it's a file-editing tool."""
    tool_name = obj.get("name", "")
    tool_input = obj.get("input", {})
    if tool_name in _FILE_TOOL_NAMES and isinstance(tool_input, dict):
        path = tool_input.get("file_path") or tool_input.get("path", "")
        if path:
            files_changed.append(path)


class ClaudeAgent(InstalledAgent):
    """Claude Code CLI agent."""

    def name(self) -> str:
        return "claude"

    def is_available(self) -> bool:
        return self._which("claude")

    def exec(
        self,
        prompt: str,
        timeout: int = 900,
        workdir: str | None = None,
    ) -> AgentResult:
        cmd = [
            "claude",
            "-p",
            prompt,
            "--dangerously-skip-permissions",
            "--output-format",
            "stream-json",
            "--no-session-persistence",
            "--verbose",
        ]

        logger.debug("claude command: %s", " ".join(cmd))

        # Nesting guard: unset Claude Code env vars to avoid
        # "cannot be launched inside another Claude Code session" error.
        env = os.environ.copy()
        env.pop("CLAUDE_CODE_ENTRYPOINT", None)
        env.pop("CLAUDECODE", None)

        start = time.time()
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=workdir,
                env=env,
            )
            duration = time.time() - start
            output_lines, files_changed = _parse_claude_stdout(result.stdout)

            return AgentResult(
                success=result.returncode == 0,
                output="\n".join(output_lines) if output_lines else result.stdout,
                files_changed=files_changed,
                duration_s=duration,
                error=result.stderr if result.returncode != 0 else "",
            )
        except subprocess.TimeoutExpired:
            return AgentResult(
                success=False,
                duration_s=time.time() - start,
                error=f"Claude Code execution timed out after {timeout}s",
            )
        except FileNotFoundError:
            return AgentResult(
                success=False,
                duration_s=time.time() - start,
                error="claude CLI not found. Install with: npm install -g @anthropic-ai/claude-code",
            )
        except Exception as e:
            return AgentResult(
                success=False,
                duration_s=time.time() - start,
                error=str(e),
            )
