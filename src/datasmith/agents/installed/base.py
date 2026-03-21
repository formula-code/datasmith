"""Abstract interface for CLI-based coding agents."""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from datasmith.utils import get_logger

logger = get_logger("agents.installed")


@dataclass
class AgentResult:
    """Unified result from any installed CLI agent."""

    success: bool
    output: str = ""
    files_changed: list[str] = field(default_factory=list)
    duration_s: float = 0.0
    error: str = ""


# Backward-compat alias
CodexResult = AgentResult


class InstalledAgent(ABC):
    """Abstract interface for CLI-based coding agents.

    Each subclass wraps a specific CLI tool (Codex, Claude Code, Gemini CLI)
    and normalises its output into an ``AgentResult``.
    """

    @abstractmethod
    def name(self) -> str:
        """Human-readable agent name."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the CLI binary is on PATH."""

    @abstractmethod
    def exec(
        self,
        prompt: str,
        timeout: int = 900,
        workdir: str | None = None,
    ) -> AgentResult:
        """Run a prompt non-interactively. Returns AgentResult."""

    def exec_or_dry_run(
        self,
        prompt: str,
        timeout: int = 900,
        workdir: str | None = None,
        dry_run: bool = False,
    ) -> AgentResult:
        """Shared dry-run wrapper around :meth:`exec`."""
        if dry_run:
            logger.info("[DRY RUN] %s command for prompt (%d chars): %.500s", self.name(), len(prompt), prompt)
            return AgentResult(
                success=True,
                output="[dry run — no execution]",
                duration_s=0.0,
            )
        return self.exec(prompt, timeout=timeout, workdir=workdir)

    @staticmethod
    def _which(binary: str) -> bool:
        return shutil.which(binary) is not None


# Registry of concrete agents in preference order.
# Populated by __init__.py after all subclasses are importable.
_AGENT_CLASSES: list[type[InstalledAgent]] = []


def get_agent(preference: list[str] | None = None) -> InstalledAgent:
    """Auto-detect and return the first available agent.

    *preference* is a list of agent names (lowercase) to try in order.
    Default: ``["claude", "codex", "gemini"]``.

    Raises ``RuntimeError`` if none are available.
    """
    from datasmith.agents.installed.claude import ClaudeAgent
    from datasmith.agents.installed.codex import CodexAgent
    from datasmith.agents.installed.gemini import GeminiAgent

    registry: dict[str, type[InstalledAgent]] = {
        "claude": ClaudeAgent,
        "codex": CodexAgent,
        "gemini": GeminiAgent,
    }

    order = preference or ["claude", "codex", "gemini"]
    for name in order:
        cls = registry.get(name)
        if cls is None:
            continue
        agent = cls()
        if agent.is_available():
            logger.info("Auto-detected agent: %s", agent.name())
            return agent

    available = list(registry.keys())
    raise RuntimeError(f"No installed CLI agent found. Tried: {order}. Install one of: {available}")
