"""No-op agent that skips LLM generation, relying solely on similar-context matching."""

from __future__ import annotations

from datasmith.agents.installed.base import AgentResult, InstalledAgent


class NoneAgent(InstalledAgent):
    """A no-op agent that is always available and never executes."""

    def name(self) -> str:
        return "none"

    def is_available(self) -> bool:
        return True

    def exec(
        self,
        prompt: str,
        timeout: int = 3600,
        workdir: str | None = None,
    ) -> AgentResult:
        return AgentResult(
            success=False,
            output="[none agent — no LLM execution]",
            error="NoneAgent does not execute",
        )
