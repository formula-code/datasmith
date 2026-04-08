#!/usr/bin/env python3
"""Prefill the Supabase ``build_attempts`` table with proven-good build scripts.

Reads the transformed ``contexts_by_sha.json`` (produced by ``transform_registry.py``)
and inserts one row per ``(owner, repo, sha)`` with ``ok=True``.  This makes the
scripts discoverable by the Synthesizer's ``_find_similar()`` lookup, which queries:

    SELECT script FROM build_attempts WHERE owner=? AND repo=? AND ok=True LIMIT 5

Each SHA gets its own row even if the script content is identical to another SHA
in the same repo — different commits can diverge in the future.

Rows inserted by this script are distinguishable from live synthesis attempts by:
  - ``issue_number = 0``   (sentinel — real PRs have issue_number >= 1)
  - ``stdout_tail``        contains "prefilled from legacy registry"

Usage:
    # Preview what would be inserted:
    python scratch/scripts/prefill/prefill_build_attempts.py --dry-run

    # Insert into Supabase:
    python scratch/scripts/prefill/prefill_build_attempts.py

    # Use a custom transformed file:
    python scratch/scripts/prefill/prefill_build_attempts.py --input /tmp/contexts.json

    # Clear previously prefilled rows before inserting (idempotent reruns):
    python scratch/scripts/prefill/prefill_build_attempts.py --replace
"""

from __future__ import annotations

import json
import os
import sys

# Allow running as a script from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from datasmith.utils.db import batch_upsert, get_client  # noqa: E402

DEFAULT_INPUT = "scratch/artifacts/pipeflush/contexts_by_sha.json"
PREFILL_MARKER = "prefilled from legacy registry"


def _load_entries(path: str) -> list[dict[str, object]]:
    """Load the transformed contexts_by_sha.json."""
    with open(path) as f:
        data = json.load(f)
    return data["entries"]


def _build_rows(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    """Convert transformed entries into build_attempts rows."""
    rows: list[dict[str, object]] = []
    for entry in entries:
        ctx = entry.get("context", {})
        if not isinstance(ctx, dict):
            continue
        script = ctx.get("build_pkg_sh", "")
        if not script:
            continue
        rows.append({
            "owner": entry["owner"],
            "repo": entry["repo"],
            "sha": entry["sha"],
            "issue_number": 0,  # sentinel for legacy entries
            "attempt_idx": 0,
            "script": script,
            "ok": True,
            "rc": 0,
            "duration_s": 0.0,
            "stderr_tail": "",
            "stdout_tail": PREFILL_MARKER,
        })
    return rows


def _delete_existing_prefills() -> int:
    """Remove previously prefilled rows (issue_number=0 with our marker)."""
    client = get_client()
    resp = (
        client.table("build_attempts")
        .delete()
        .eq("issue_number", 0)
        .eq("stdout_tail", PREFILL_MARKER)
        .execute()
    )
    return len(resp.data) if resp.data else 0


def main(
    dry_run: bool = False,
    input_path: str = DEFAULT_INPUT,
    replace: bool = False,
) -> None:
    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}")
        print("Run transform_registry.py first to produce it.")
        sys.exit(1)

    entries = _load_entries(input_path)
    rows = _build_rows(entries)

    # Summary
    repos: dict[str, list[str]] = {}
    for r in rows:
        key = f"{r['owner']}/{r['repo']}"
        repos.setdefault(key, []).append(str(r["sha"])[:12])

    print(f"Total rows: {len(rows)} across {len(repos)} repos")
    print()
    for repo_key in sorted(repos):
        shas = repos[repo_key]
        print(f"  {repo_key}: {len(shas)} sha(s)")

    if dry_run:
        print(f"\n[dry-run] Would insert {len(rows)} rows into build_attempts")
        return

    if replace:
        deleted = _delete_existing_prefills()
        print(f"\nDeleted {deleted} existing prefill rows")

    inserted = batch_upsert("build_attempts", rows)
    print(f"\nInserted {inserted} rows into build_attempts")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    replace = "--replace" in sys.argv
    input_path = DEFAULT_INPUT
    for i, arg in enumerate(sys.argv):
        if arg == "--input" and i + 1 < len(sys.argv):
            input_path = sys.argv[i + 1]
    main(dry_run=dry_run, input_path=input_path, replace=replace)
