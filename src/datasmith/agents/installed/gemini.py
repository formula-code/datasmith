"""Gemini CLI agent implementation."""

from __future__ import annotations

import json
import subprocess
import time

from datasmith.agents.installed.base import AgentResult, InstalledAgent
from datasmith.utils import get_logger

logger = get_logger("agents.installed.gemini")


_TEXT_TYPES = {"assistant", "response", "text"}
_FILE_TOOL_NAMES = {"write_file", "edit_file", "Write", "Edit", "create_file", "update_file"}


def _parse_gemini_stdout(stdout: str) -> tuple[list[str], list[str]]:
    """Parse Gemini CLI stream-json output into (output_lines, files_changed)."""
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

            if msg_type in _TEXT_TYPES:
                _append_text(obj, output_lines)
            elif msg_type == "result":
                _append_result(obj, output_lines)
            elif msg_type in ("tool_use", "action"):
                _collect_gemini_file(obj, files_changed)
            elif "output" in obj:
                output_lines.append(obj["output"])
            elif "message" in obj:
                output_lines.append(obj["message"])

        except json.JSONDecodeError:
            output_lines.append(line)

    return output_lines, files_changed


def _append_text(obj: dict, output_lines: list[str]) -> None:
    text = obj.get("text") or obj.get("message") or obj.get("content", "")
    if isinstance(text, str) and text:
        output_lines.append(text)


def _append_result(obj: dict, output_lines: list[str]) -> None:
    result_text = obj.get("result") or obj.get("text", "")
    if isinstance(result_text, str) and result_text:
        output_lines.append(result_text)


def _collect_gemini_file(obj: dict, files_changed: list[str]) -> None:
    tool_name = obj.get("name") or obj.get("tool", "")
    tool_input = obj.get("input") or obj.get("args", {})
    if isinstance(tool_input, dict) and tool_name in _FILE_TOOL_NAMES:
        path = tool_input.get("file_path") or tool_input.get("path", "")
        if path:
            files_changed.append(path)


class GeminiAgent(InstalledAgent):
    """Gemini CLI agent."""

    def name(self) -> str:
        return "gemini"

    def is_available(self) -> bool:
        return self._which("gemini")

    def exec(
        self,
        prompt: str,
        timeout: int = 900,
        workdir: str | None = None,
    ) -> AgentResult:
        cmd = [
            "gemini",
            "-p",
            prompt,
            "--yolo",
            "-o",
            "json",
        ]

        logger.debug("gemini command: %s", " ".join(cmd))

        start = time.time()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=workdir)
            duration = time.time() - start
            output_lines, files_changed = _parse_gemini_stdout(result.stdout)

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
                error=f"Gemini CLI execution timed out after {timeout}s",
            )
        except FileNotFoundError:
            return AgentResult(
                success=False,
                duration_s=time.time() - start,
                error="gemini CLI not found. Install with: npm install -g @anthropic-ai/gemini-cli",
            )
        except Exception as e:
            return AgentResult(
                success=False,
                duration_s=time.time() - start,
                error=str(e),
            )
