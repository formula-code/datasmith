"""Codex CLI agent implementation."""

from __future__ import annotations

import json

from datasmith.agents.installed.base import AgentResult, InstalledAgent, run_agent_subprocess
from datasmith.utils import get_logger

logger = get_logger("agents.installed.codex")


def _parse_codex_stdout(stdout: str) -> tuple[list[str], list[str]]:
    """Parse JSON stream from Codex stdout into (output_lines, files_changed)."""
    files_changed: list[str] = []
    output_lines: list[str] = []

    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                if "file" in obj:
                    files_changed.append(obj["file"])
                # codex >=0.114 item.completed format
                item = obj.get("item")
                if isinstance(item, dict) and item.get("type") == "agent_message":
                    text = item.get("text", "")
                    if text:
                        output_lines.append(text)
                elif "output" in obj:
                    output_lines.append(obj["output"])
                elif "message" in obj:
                    output_lines.append(obj["message"])
        except json.JSONDecodeError:
            output_lines.append(line)

    return output_lines, files_changed


class CodexAgent(InstalledAgent):
    """Codex CLI (``codex exec``) agent."""

    def __init__(self, full_auto: bool = False, sandbox: str = "") -> None:
        self._full_auto = full_auto
        self._sandbox = sandbox

    def name(self) -> str:
        return "codex"

    def is_available(self) -> bool:
        return self._which("codex")

    def exec(
        self,
        prompt: str,
        timeout: int = 3600,
        workdir: str | None = None,
    ) -> AgentResult:
        cmd = ["codex", "exec", "--model", "gpt-5.4-mini", "-c", "model_reasoning_effort=medium"]
        if self._full_auto and self._sandbox:
            cmd.extend(["--full-auto", "--sandbox", self._sandbox])
        else:
            cmd.append("--dangerously-bypass-approvals-and-sandbox")
        cmd.extend(["--json", "--ephemeral"])
        cmd.append(prompt)

        logger.debug("codex command: %s", " ".join(cmd))

        try:
            returncode, stdout, stderr, duration = run_agent_subprocess(
                cmd, timeout=timeout, cwd=workdir, agent_name="codex"
            )
            output_lines, files_changed = _parse_codex_stdout(stdout)

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
                error="codex CLI not found. Install with: npm install -g @openai/codex",
            )
