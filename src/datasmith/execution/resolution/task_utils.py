from __future__ import annotations

import json
import re

from datasmith.core.models import Task
from datasmith.execution.resolution import analyze_commit
from datasmith.logging_config import configure_logging

logger = configure_logging()

RE_PY_EXTRACT = re.compile(r"python[=<>!~]*([\d\.]+)")


def resolve_task(task: Task, bypass_cache: bool = False) -> tuple[dict, Task]:
    if task.sha is None:
        return {
            "dry_run_log": "No SHA provided",
            "can_install": False,
        }, task
    sha: str = task.sha
    task_analysis = analyze_commit(sha, f"{task.owner}/{task.repo}", bypass_cache=bypass_cache) or {}
    if not task_analysis or not task_analysis.get("can_install", False):
        logger.warning("agent_build_and_validate: task cannot be installed")
        return {
            "ok": False,
            "rc": 1,
            "stage": "analysis",
            "owner": task.owner,
            "repo": task.repo,
            "sha": task.sha,
            "image_name": task.with_tag("pkg").get_image_name(),
            "duration_s": 0.0,
            "stderr_tail": json.dumps(task_analysis) if task_analysis else "No analysis",
            "stdout_tail": task_analysis.get("dry_run_log", ""),
            "attempts": [],
            "context_pickle": None,
        }, task

    python_version = ""
    if task_analysis.get("resolution_strategy"):
        m = RE_PY_EXTRACT.search(task_analysis["resolution_strategy"])
        if m:
            python_version = m.group(1)
    if not python_version:
        python_version = task_analysis.get("python_version", "")
    if not python_version and task.python_version:
        python_version = task.python_version

    logger.info(
        "agent_build_and_validate: task analysis: python_versions=%s, final_dependencies=%s",
        task_analysis.get("python_version"),
        task_analysis.get("final_dependencies"),
    )

    task = Task(
        owner=task.owner,
        repo=task.repo,
        sha=task.sha,
        commit_date=task.commit_date,
        python_version=python_version,
        env_payload=json.dumps({"dependencies": task_analysis.get("final_dependencies", "")}) or task.env_payload,
    )

    return task_analysis, task
