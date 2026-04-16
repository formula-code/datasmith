"""Qwen Code CLI agent implementation."""

from __future__ import annotations

import json

from datasmith.agents.installed.base import AgentResult, InstalledAgent, run_agent_subprocess
from datasmith.utils import get_logger

logger = get_logger("agents.installed.qwen")

_FILE_TOOL_NAMES = {"write_file", "edit", "Write", "Edit", "edit_file", "create_file", "update_file"}


def _extract_assistant_text(message: object) -> str:
    """Extract text from a Qwen assistant message payload."""
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
                    text = block.get("text", "")
                    # Strip <think>...</think> reasoning blocks from output
                    if "</think>" in text:
                        text = text.split("</think>", 1)[-1].strip()
                    if text:
                        parts.append(text)
            return "\n".join(parts)
    return ""


def _parse_qwen_stdout(stdout: str) -> tuple[list[str], list[str]]:  # noqa: C901
    """Parse Qwen Code stream-json output into (output_lines, files_changed)."""
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
                    # Strip <think>...</think> from result text too
                    if "</think>" in result_text:
                        result_text = result_text.split("</think>", 1)[-1].strip()
                    if result_text:
                        output_lines.append(result_text)
            elif msg_type == "tool_use":
                _collect_file_change(obj, files_changed)

        except json.JSONDecodeError:
            output_lines.append(line)

    return output_lines, files_changed


def _collect_file_change(obj: dict, files_changed: list[str]) -> None:
    """Extract file path from a tool_use event if it's a file-editing tool."""
    tool_name = obj.get("name", "")
    tool_input = obj.get("input") or obj.get("args", {})
    if tool_name in _FILE_TOOL_NAMES and isinstance(tool_input, dict):
        path = tool_input.get("file_path") or tool_input.get("path", "")
        if path:
            files_changed.append(path)


class QwenAgent(InstalledAgent):
    """Qwen Code CLI agent."""

    def name(self) -> str:
        return "qwen"

    def is_available(self) -> bool:
        return self._which("qwen")

    def exec(
        self,
        prompt: str,
        timeout: int = 3600,
        workdir: str | None = None,
    ) -> AgentResult:
        cmd = [
            "qwen",
            "-p",
            prompt,
            "--yolo",
            "-o",
            "stream-json",
        ]

        logger.debug("qwen command: %s", " ".join(cmd))

        try:
            returncode, stdout, stderr, duration = run_agent_subprocess(
                cmd, timeout=timeout, cwd=workdir, agent_name="qwen"
            )
            output_lines, files_changed = _parse_qwen_stdout(stdout)

            return AgentResult(
                success=returncode == 0,
                output="\n".join(output_lines) if output_lines else stdout,
                raw_output=stdout,
                files_changed=files_changed,
                duration_s=duration,
                error=stderr if returncode != 0 else "",
            )
        except FileNotFoundError:
            return AgentResult(
                success=False,
                duration_s=0.0,
                error="qwen CLI not found. Install with: npm install -g @qwen-code/qwen-code",
            )
