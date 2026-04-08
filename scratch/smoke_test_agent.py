#!/usr/bin/env python3
"""Smoke test: run Synthesizer API with auto-detected agent on pandas-dev/pandas.

Usage:
    uv run python scratch/smoke_test_agent.py
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from datasmith.agents import Synthesizer
from datasmith.docker.context import DockerContext
from datasmith.docker.verifiers import Verifier, VerifyResult

TASK_DIR = "dataset/formulacode_verified_new/pandas-dev_pandas/2b568ce82618c5e6dd2fc6f6f1d22e84c4883546"
OWNER = "pandas-dev"
REPO = "pandas"
SHA = "2b568ce82618c5e6dd2fc6f6f1d22e84c4883546"
PYTHON_VERSION = "3.12"
ISSUE_NUMBER = 0


class NoOpVerifier(Verifier):
    """Always-fail verifier — skips TRY_SIMILAR so we exercise the agent path."""

    def verify(self, image: str) -> VerifyResult:
        return VerifyResult(ok=False, rc=1, stdout="", stderr="no-op verifier", duration_s=0.0)


def _extract_env_payload(task_dir: str) -> str:
    """Extract env_payload from task.txt by evaluating the Task() expression."""
    from datasmith.agents.templates.sandbox_verify import Task

    text = Path(task_dir, "task.txt").read_text().strip()
    task = eval(text, {"__builtins__": {}}, {"Task": Task})  # noqa: S307
    return task.env_payload


def main() -> None:
    from datasmith.agents.installed import get_agent

    agent = get_agent()
    print(f"Auto-detected agent: {agent.name()} (available={agent.is_available()})")

    base_context = DockerContext.from_directory(TASK_DIR)
    env_payload = _extract_env_payload(TASK_DIR)
    print(f"Loaded base context from {TASK_DIR}")
    print(f"env_payload: {len(env_payload)} chars, contains odfpy: {'odfpy' in env_payload}")

    synth = Synthesizer(max_attempts=1)
    ctx = synth.run(
        owner=OWNER,
        repo=REPO,
        issue_number=ISSUE_NUMBER,
        pr_context="pandas is a data analysis library. This commit improves performance.",
        verifier=NoOpVerifier(),
        sha=SHA,
        base_context=base_context,
        env_payload=env_payload,
        python_version=PYTHON_VERSION,
    )

    print(f"\nSynthesizer trace: {synth.trace}")
    if ctx is not None:
        print("SUCCESS: Synthesizer returned a DockerContext")
        print(f"  build_pkg_sh length: {len(ctx.build_pkg_sh)} chars")
        print(f"  dockerfile length:   {len(ctx.dockerfile)} chars")
    else:
        print("FAILED: Synthesizer returned None")
        sys.exit(1)


if __name__ == "__main__":
    main()
