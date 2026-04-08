#!/usr/bin/env python3
"""Seed the Supabase `build_attempts` table with legacy registry scripts.

This makes proven-working build scripts discoverable by the Synthesizer's
``_find_similar()`` lookup (which queries ``build_attempts`` by owner/repo/ok=True).

Usage:
    python scratch/scripts/seed_synthesizer_cache.py [--dry-run]
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass

# Allow running as a script from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from datasmith.utils.db import batch_upsert, get_client  # noqa: E402

REGISTRY_PATH = "scratch/artifacts/pipeflush/context_registry_final_filtered.json"


@dataclass(frozen=True)
class Task:
    """Mirrors the Task dataclass used to stringify registry keys."""

    owner: str = ""
    repo: str = ""
    sha: str | None = None
    commit_date: float = 0.0
    env_payload: str = ""
    python_version: str = ""
    tag: str = "pkg"
    benchmarks: str = ""


def _parse_task_key(key: str) -> Task | None:
    try:
        task = eval(key, {"__builtins__": {}}, {"Task": Task})  # noqa: S307
        if isinstance(task, Task):
            return task
    except Exception:
        pass
    return None


def main(dry_run: bool = False) -> None:
    with open(REGISTRY_PATH) as f:
        registry = json.load(f)

    contexts = registry["contexts"]
    rows: list[dict[str, object]] = []

    for key, entry_data in contexts.items():
        task = _parse_task_key(key)
        if task is None or task.owner == "default" or task.sha is None:
            continue

        script = entry_data.get("building_data", "")
        if not script:
            continue

        rows.append({
            "owner": task.owner,
            "repo": task.repo,
            "issue_number": 0,  # sentinel for legacy entries
            "attempt_idx": 0,
            "model": "legacy-registry",
            "script": script,
            "ok": True,
            "rc": 0,
            "stderr_tail": "",
            "stdout_tail": f"seeded from legacy registry, sha={task.sha}",
        })

    if dry_run:
        print(f"[dry-run] Would insert {len(rows)} rows into build_attempts")
        for r in rows[:3]:
            print(f"  {r['owner']}/{r['repo']} sha={r['stdout_tail']}")
        return

    inserted = batch_upsert("build_attempts", rows)
    print(f"Inserted {inserted} rows into build_attempts")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    main(dry_run=dry_run)
