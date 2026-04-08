"""DEPRECATED: This module has been refactored into multiple modules.

Use:
- datasmith.agents.build for agent-based build logic
- datasmith.docker.validation for validation functions
- datasmith.docker.cleanup for cleanup functions

This module provides backward-compatible imports.
"""

from __future__ import annotations

# Re-export from new modules for backward compatibility
from datasmith.agents.build import (
    AttemptRecord,
    BuildScriptAgentStep,
    BuildScriptProgram,
    agent_build_and_validate,
    build_once_with_context,
    synthesize_script,
)
from datasmith.docker.cleanup import fast_cleanup_run_artifacts, remove_containers_by_label

__all__ = [
    "AttemptRecord",
    "BuildScriptAgentStep",
    "BuildScriptProgram",
    "agent_build_and_validate",
    "build_once_with_context",
    "fast_cleanup_run_artifacts",
    "remove_containers_by_label",
    "synthesize_script",
]
