"""Installed CLI agent abstraction.

Provides a unified interface for CLI-based coding agents (Codex, Claude Code,
Gemini CLI) with auto-detection of whichever is available on the host.
"""

from datasmith.agents.installed.base import AgentResult, CodexResult, InstalledAgent, get_agent
from datasmith.agents.installed.claude import ClaudeAgent
from datasmith.agents.installed.codex import CodexAgent
from datasmith.agents.installed.gemini import GeminiAgent

__all__ = [
    "AgentResult",
    "ClaudeAgent",
    "CodexAgent",
    "CodexResult",
    "GeminiAgent",
    "InstalledAgent",
    "get_agent",
]
