"""Codex CLI agent implementation."""

from __future__ import annotations

import json
import os

from datasmith.agents.installed.base import AgentResult, InstalledAgent, run_agent_subprocess
from datasmith.utils import get_logger

logger = get_logger("agents.installed.codex")

# Codex polls a backgrounded shell call for only 5 minutes by default, but local_ci.py
# runs 15 to 40 minutes; match SYNTHESIS_TIMEOUT_S so one verifier run fits in one tool call.
_SYNTHESIS_TIMEOUT_S = int(os.environ.get("SYNTHESIS_TIMEOUT_S", "14400"))
_BG_TERMINAL_MAX_TIMEOUT_MS = _SYNTHESIS_TIMEOUT_S * 1000

# Output limit per tool call, high so long docker build logs and failure.json reads stay intact.
_TOOL_OUTPUT_TOKEN_LIMIT = int(os.environ.get("CODEX_TOOL_OUTPUT_TOKEN_LIMIT", "100000"))

# Compaction can drop the launch prompt's rules and earlier failures, so defer it past most
# sessions; _COMPACT_PROMPT restates the rules when it does happen.
_AUTO_COMPACT_TOKEN_LIMIT = int(os.environ.get("CODEX_AUTO_COMPACT_TOKEN_LIMIT", "200000"))

# Model and reasoning effort, read from the environment with a literal default.
DATASMITH_CODEX_MODEL: str = os.environ.get("DATASMITH_CODEX_MODEL", "gpt-5.6-luna")
DATASMITH_CODEX_REASONING_EFFORT: str = os.environ.get("DATASMITH_CODEX_REASONING_EFFORT", "medium")

# Replaces codex's default compaction instructions, so the summary keeps the hard
# requirements, the current docker_build_*.sh contents and the latest failure.json.
_COMPACT_PROMPT = """\
You are compacting the history of a long synthesis session for the FormulaCode
Docker build verifier. Produce a concise summary that REPLACES older turns,
but you MUST preserve the following items verbatim or near-verbatim — they
are load-bearing and the agent will fail without them:

1. The hard requirements from the original developer/user prompt:
   - The agent MUST run `python3 local_ci.py` from the workspace root.
     This is the ONLY accepted verifier. A `bash -n` syntax check, a manual
     `pip install`, a manual `docker build`, or any "local reproduction"
     outside this script does NOT count and will be ignored.
   - The ONLY accepted success state is `task/verification_success.json`
     existing on disk when the agent exits.
   - The agent must NEVER run `pkill`, `kill`, `killall`, or `pgrep` against
     `local_ci`, `docker`, `asv`, `pytest`, `python`, or any process it
     did not start itself. Peer worker processes visible in `ps` belong to
     other tasks and must be left alone.
   - A full verifier run typically takes 15-40 minutes; the agent must wait
     for it to finish and not exit early.

2. The current contents of any files the agent has modified:
   `task/docker_build_pkg.sh`, `task/docker_build_run.sh`,
   `task/docker_build_env.sh`, and `task/env_payload_override.json`.
   Quote them in full inside ```bash code fences.

3. The most recent `task/failure.json`: `stage`, `return_code`,
   `error_message`, and the last 50 lines of `stderr`.

4. A bullet list of approaches already tried and why they failed, so the
   agent does not repeat them.

After preserving the above, you may compress earlier reasoning, tool calls,
and intermediate file reads into a brief recap.
"""


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
        # `compact_prompt` is a TOML string value; pass it as a JSON-encoded
        # string literal so embedded newlines and quotes survive `-c` parsing.
        compact_prompt_toml = json.dumps(_COMPACT_PROMPT)
        cmd = [
            "codex",
            "exec",
            "--model",
            DATASMITH_CODEX_MODEL,
            "-c",
            f"model_reasoning_effort={DATASMITH_CODEX_REASONING_EFFORT}",
            "-c",
            f"background_terminal_max_timeout={_BG_TERMINAL_MAX_TIMEOUT_MS}",
            "-c",
            f"tool_output_token_limit={_TOOL_OUTPUT_TOKEN_LIMIT}",
            "-c",
            f"model_auto_compact_token_limit={_AUTO_COMPACT_TOKEN_LIMIT}",
            "-c",
            f"compact_prompt={compact_prompt_toml}",
        ]
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
