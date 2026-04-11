"""Codex CLI agent implementation."""

from __future__ import annotations

import json
import os

from datasmith.agents.installed.base import AgentResult, InstalledAgent, run_agent_subprocess
from datasmith.utils import get_logger

logger = get_logger("agents.installed.codex")

# Codex CLI's `background_terminal_max_timeout` defaults to 300_000 ms (5 min).
# That's the polling window for backgrounded shell tool calls; if the model
# launches `python3 local_ci.py` (which runs a multi-stage docker build
# typically lasting 15-40 minutes), the polling window expires long before
# the verifier finishes, codex returns control to the model with no completion
# event, and the agent eventually exits leaving the verifier orphaned and no
# result file on disk. We raise the cap to match SYNTHESIS_TIMEOUT_S so a full
# verifier run can complete inside one tool call.
_SYNTHESIS_TIMEOUT_S = int(os.environ.get("SYNTHESIS_TIMEOUT_S", "14400"))
_BG_TERMINAL_MAX_TIMEOUT_MS = _SYNTHESIS_TIMEOUT_S * 1000

# Token budget for storing individual tool/function outputs in history.
# Default is small; bumping this prevents codex from truncating long docker
# build logs and `failure.json` reads which the model needs intact to diagnose
# failures across turns. 100k is generous (each tool call is capped here, not
# the whole session) but small enough that one call can't blow past the
# model's context window on its own.
_TOOL_OUTPUT_TOKEN_LIMIT = int(os.environ.get("CODEX_TOOL_OUTPUT_TOKEN_LIMIT", "100000"))

# Token threshold at which codex auto-compacts history. Codex's compactor
# summarizes older turns into a recap, and on overflow falls back to dropping
# the oldest history items entirely (the binary literally logs
# "Context window exceeded while compacting; removing oldest history item.").
# Both behaviors risk silently losing the launch prompt's hard requirements
# and earlier failure context — i.e. the agent forgets the rules halfway
# through a long synthesis session and starts skipping local_ci.py
# again. We push the threshold high to defer compaction past most sessions,
# and override compact_prompt below so that when compaction *does* happen,
# the rules survive into the post-compaction state.
_AUTO_COMPACT_TOKEN_LIMIT = int(os.environ.get("CODEX_AUTO_COMPACT_TOKEN_LIMIT", "200000"))

# Custom compaction prompt. Replaces codex's default summarization instructions.
# Codex sends this to the model along with the history to be compacted; the
# model returns a summary that replaces the older turns. We use it to re-state
# the hard requirements verbatim so they survive into the post-compaction
# state, and to require preservation of the current docker_build_*.sh contents
# and the most recent failure.json so the agent doesn't lose its place.
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
            "gpt-5.4-mini",
            "-c",
            "model_reasoning_effort=xhigh",
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
