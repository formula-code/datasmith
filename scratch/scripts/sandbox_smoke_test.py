#!/usr/bin/env python3
"""Smoke test: run SandboxRunner on a known-good task.

Usage:
    python scratch/scripts/sandbox_smoke_test.py <task_dir> [--timeout SECONDS]

Example:
    python scratch/scripts/sandbox_smoke_test.py \
        dataset/formulacode_verified_new/optuna_optuna/1ab5c2e3fdc99b7eb3f4cae5c092cbf20b8985b7
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Ensure the project is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from datasmith.agents.sandbox import SandboxConfig, SandboxRunner
from datasmith.docker.context import DockerContext


def main() -> None:
    parser = argparse.ArgumentParser(description="Sandbox synthesis smoke test")
    parser.add_argument("task_dir", type=Path, help="Path to a task directory with all context files")
    parser.add_argument("--timeout", type=int, default=3600, help="Codex timeout in seconds (default: 3600)")
    parser.add_argument("--skip-tests", action="store_true", help="Pass --skip-tests to verify")
    parser.add_argument("--dry-run", action="store_true", help="Prepare workspace only, don't launch Codex")
    args = parser.parse_args()

    task_dir = args.task_dir
    if not task_dir.exists():
        print(f"ERROR: Task dir not found: {task_dir}")
        sys.exit(1)

    task_txt_path = task_dir / "task.txt"
    if not task_txt_path.exists():
        print(f"ERROR: No task.txt in {task_dir}")
        sys.exit(1)

    # Parse task.txt to extract fields
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class Task:
        owner: str = ""
        repo: str = ""
        sha: str | None = None
        commit_date: float = 0.0
        env_payload: str = ""
        python_version: str = ""
        tag: str = "pkg"
        benchmarks: str = ""

    task_txt = task_txt_path.read_text().strip()
    task = eval(task_txt, {"__builtins__": {}}, {"Task": Task})  # noqa: S307

    print(f"Task: {task.owner}/{task.repo}@{(task.sha or 'unknown')[:12]}")
    print(f"Python: {task.python_version}")
    print(f"Timeout: {args.timeout}s")
    print()

    # Load context
    ctx = DockerContext.from_directory(str(task_dir))

    # Configure and run
    config = SandboxConfig(
        timeout_s=args.timeout,
        codex_timeout_s=args.timeout,
        skip_tests=args.skip_tests,
    )
    runner = SandboxRunner(config=config)

    print(f"Starting sandbox run at {time.strftime('%H:%M:%S')}...")
    start = time.time()
    result = runner.run(
        owner=task.owner,
        repo=task.repo,
        sha=task.sha or "",
        base_context=ctx,
        env_payload=task.env_payload,
        python_version=task.python_version,
        pr_context=f"Verifying build for {task.owner}/{task.repo}@{(task.sha or '')[:12]}",
        dry_run=args.dry_run,
    )
    elapsed = time.time() - start

    print()
    print("=" * 60)
    print(f"Result: {'SUCCESS' if result.success else 'FAILURE'}")
    print(f"Duration: {elapsed:.1f}s ({elapsed/60:.1f}m)")
    if result.failure_json:
        print(f"Failure stage: {result.failure_json.get('stage', 'unknown')}")
        print(f"Error: {result.failure_json.get('error_message', '')[:500]}")
    if result.agent_output:
        print(f"Agent output (last 500 chars): ...{result.agent_output[-500:]}")
    print("=" * 60)

    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
