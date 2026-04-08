#!/usr/bin/env python3
"""Transform legacy context_registry into the new per-SHA format.

Reads ``context_registry_final_filtered.json`` (77 entries, old pipeline) and
produces a single JSON file with one entry per ``(owner, repo, sha)``, using
DockerContext field names instead of legacy names.

Legacy field mapping (registry -> DockerContext -> file on disk):

    building_data       -> build_pkg_sh       -> docker_build_pkg.sh
    dockerfile_data     -> dockerfile          -> Dockerfile
    base_building_data  -> build_base_sh       -> docker_build_base.sh
    env_building_data   -> build_env_sh        -> docker_build_env.sh
    run_building_data   -> build_run_sh        -> docker_build_run.sh
    final_building_data -> build_final_sh      -> docker_build_final.sh
    profile_data        -> profile_sh          -> profile.sh
    run_tests_data      -> run_tests_sh        -> run-tests.sh
    entrypoint_data     -> entrypoint_sh       -> entrypoint.sh

Usage:
    python scratch/scripts/prefill/transform_registry.py [--dry-run]
    python scratch/scripts/prefill/transform_registry.py --out /tmp/contexts.json
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass

REGISTRY_PATH = "scratch/artifacts/pipeflush/context_registry_final_filtered.json"
DEFAULT_OUT = "scratch/artifacts/pipeflush/contexts_by_sha.json"

# Maps legacy registry field names to DockerContext field names.
LEGACY_MAP: dict[str, str] = {
    "dockerfile_data": "dockerfile",
    "base_building_data": "build_base_sh",
    "env_building_data": "build_env_sh",
    "building_data": "build_pkg_sh",
    "run_building_data": "build_run_sh",
    "final_building_data": "build_final_sh",
    "profile_data": "profile_sh",
    "run_tests_data": "run_tests_sh",
    "entrypoint_data": "entrypoint_sh",
}


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


def _remap_entry(entry: dict[str, object]) -> dict[str, str]:
    """Rename legacy keys to DockerContext field names, drop non-string fields."""
    out: dict[str, str] = {}
    for legacy_key, new_key in LEGACY_MAP.items():
        val = entry.get(legacy_key, "")
        if isinstance(val, str) and val:
            out[new_key] = val
    return out


def main(dry_run: bool = False, out_path: str = DEFAULT_OUT) -> None:
    with open(REGISTRY_PATH) as f:
        registry = json.load(f)

    contexts = registry["contexts"]

    entries: list[dict[str, object]] = []
    skipped = 0

    for key, entry_data in contexts.items():
        task = _parse_task_key(key)
        if task is None or task.owner == "default" or task.sha is None:
            skipped += 1
            continue

        remapped = _remap_entry(entry_data)
        if not remapped.get("build_pkg_sh"):
            skipped += 1
            continue

        entries.append({
            "owner": task.owner,
            "repo": task.repo,
            "sha": task.sha,
            "commit_date": task.commit_date,
            "python_version": task.python_version,
            "context": remapped,
        })

    # Summary stats
    repos = {(e["owner"], e["repo"]) for e in entries}
    print(f"Entries: {len(entries)} (from {len(repos)} repos)")
    print(f"Skipped: {skipped}")
    print()

    by_repo: dict[str, list[str]] = {}
    for e in entries:
        key = f"{e['owner']}/{e['repo']}"
        by_repo.setdefault(key, []).append(str(e["sha"])[:12])
    for repo_key in sorted(by_repo):
        shas = by_repo[repo_key]
        print(f"  {repo_key}: {len(shas)} sha(s)")

    output = {
        "description": (
            "Per-SHA contexts transformed from context_registry_final_filtered.json. "
            "Field names match DockerContext (not legacy names)."
        ),
        "entries": entries,
    }

    if dry_run:
        print(f"\n[dry-run] Would write to {out_path}")
        return

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    out = DEFAULT_OUT
    for i, arg in enumerate(sys.argv):
        if arg == "--out" and i + 1 < len(sys.argv):
            out = sys.argv[i + 1]
    main(dry_run=dry_run, out_path=out)
