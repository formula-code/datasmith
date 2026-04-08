#!/usr/bin/env python3
"""Migrate legacy Docker registry entries to new dataset/formulacode_verified_new/ directory structure.

Reads context_registry_final_filtered.json (77 pre-synthesized build contexts from the old
pipeline) and writes each valid, non-duplicate entry as a task directory with all Docker
context files + a task.txt metadata file.

Usage:
    python scratch/scripts/migrate_legacy_registry.py [--dry-run]
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass

# Allow running as a script from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from datasmith.docker.context import DockerContext  # noqa: E402

REGISTRY_PATH = "scratch/artifacts/pipeflush/context_registry.json"
VERIFIED_DIR = "dataset/formulacode_verified_new/"
VERIFIED_NEW_DIR = "dataset/formulacode/"


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
    """Parse a stringified Task(...) key back into a Task object."""
    try:
        task = eval(key, {"__builtins__": {}}, {"Task": Task})  # noqa: S307
        if isinstance(task, Task):
            return task
    except Exception:
        pass
    return None


def _task_dir_exists(owner: str, repo: str, sha: str) -> bool:
    """Check if a task directory already exists in either verified dir."""
    slug = f"{owner}_{repo}"
    for base in (VERIFIED_DIR, VERIFIED_NEW_DIR):
        if os.path.isdir(os.path.join(base, slug, sha)):
            return True
    return False


def main(dry_run: bool = False) -> None:
    with open(REGISTRY_PATH) as f:
        registry = json.load(f)

    contexts = registry["contexts"]
    created = 0
    skipped_existing = 0
    skipped_invalid = 0

    for key, entry_data in contexts.items():
        task = _parse_task_key(key)
        if task is None or task.owner == "default" or task.sha is None:
            skipped_invalid += 1
            continue

        if _task_dir_exists(task.owner, task.repo, task.sha):
            skipped_existing += 1
            continue

        slug = f"{task.owner}_{task.repo}"
        out_dir = os.path.join(VERIFIED_NEW_DIR, slug, task.sha)

        if dry_run:
            print(f"  [dry-run] would create {out_dir}")
        else:
            ctx = DockerContext.from_legacy_dict(entry_data)
            ctx.to_directory(out_dir)
            # Write the raw Task key as metadata.
            with open(os.path.join(out_dir, "task.txt"), "w") as f:
                f.write(key)

        created += 1

    print(f"Created: {created}")
    print(f"Skipped (already exist): {skipped_existing}")
    print(f"Skipped (invalid/default): {skipped_invalid}")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    main(dry_run=dry_run)
