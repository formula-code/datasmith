"""Backward-compatibility shim — real logic lives in agents.installed.codex."""

from __future__ import annotations

from datasmith.agents.installed import AgentResult as CodexResult
from datasmith.agents.installed import CodexAgent
from datasmith.agents.installed.codex import _parse_codex_stdout

__all__ = ["CodexResult", "_parse_codex_stdout", "codex_exec"]


def codex_exec(
    prompt: str,
    timeout: int = 900,
    workdir: str | None = None,
    dry_run: bool = False,
    full_auto: bool = False,
    sandbox: str = "",
) -> CodexResult:
    """Execute a prompt via the Codex CLI.

    Thin wrapper around :class:`~datasmith.agents.installed.codex.CodexAgent`.
    """
    agent = CodexAgent(full_auto=full_auto, sandbox=sandbox)
    return agent.exec_or_dry_run(prompt, timeout=timeout, workdir=workdir, dry_run=dry_run)
