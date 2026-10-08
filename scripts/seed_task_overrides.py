"""Seed formulacode_task_overrides from the rl-prep override records.

The override record is the operator-declared half of a task: facts that cannot
be derived from the repo or the PR. Its canonical source today is
``rl-prep/local-overrides/formulacode_task_overrides.json`` (5 tasks), which is
UNTRACKED in this repo -- unpacked from rl-prep.zip -- so this script must
tolerate its absence rather than assuming it.

The most consequential field is ``benchmark_dest``. It is the producer for the
FATAL ``benchmark_dest_missing`` invariant, which has been structurally inert
since it shipped because nothing in the tree ever set ``$BENCHMARK_DEST``.

``expected_n`` is deliberately NOT seeded. It is not present in the source
record (verified: the real record has ten keys and none is expected_n), and it
is a human judgement about how many benchmarks a PR *should* touch. Rows land
with it NULL, and the dilution invariant skips until someone fills it in.

Usage:
  uv run python scripts/seed_task_overrides.py            # dry run
  uv run python scripts/seed_task_overrides.py --apply    # actually upsert
  uv run python scripts/seed_task_overrides.py --source path/to/overrides.json

Idempotent: upserts on (owner, repo, issue_number), so re-running is a no-op
for unchanged records. Never deletes.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from datasmith.utils.db import get_client

load_dotenv("tokens.env")

DEFAULT_SOURCE = Path("rl-prep/local-overrides/formulacode_task_overrides.json")
TABLE = "formulacode_task_overrides"

# Fields copied straight from the source record. expected_n and notes are
# absent by design -- see the module docstring.
_PASSTHROUGH = (
    "benchmark_dest",
    "benchmark_storage_key",
    "extra_dockerfile_commands",
    "extra_entrypoint_commands",
    "pip_pins",
    "restore_regex",
    "oracle_h",
)


def load_records(source: Path) -> list[dict[str, Any]]:
    """Read override records, tolerating an absent or malformed source.

    Returns [] rather than raising: the source is untracked, so a clean
    checkout legitimately does not have it.
    """
    if not source.exists():
        print(f"[seed] source not found: {source}")
        print("[seed] rl-prep/ is untracked (unpacked from rl-prep.zip). Nothing to seed.")
        return []
    try:
        data = json.loads(source.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[seed] could not parse {source}: {exc}")
        return []
    if not isinstance(data, list):
        print(f"[seed] expected a JSON list, got {type(data).__name__}")
        return []
    return [r for r in data if isinstance(r, dict)]


def to_row(rec: dict[str, Any]) -> dict[str, Any] | None:
    """Map one override record onto a table row, or None if unusable."""
    owner, repo = rec.get("owner"), rec.get("repo")
    issue = rec.get("issue_number")
    if not owner or not repo or issue is None:
        return None
    try:
        issue_number = int(issue)
    except (TypeError, ValueError):
        return None

    row: dict[str, Any] = {"owner": owner, "repo": repo, "issue_number": issue_number}
    for key in _PASSTHROUGH:
        if key in rec:
            row[key] = rec[key]
    return row


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Seed formulacode_task_overrides")
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--apply", action="store_true", help="Actually write (default: dry run)")
    args = ap.parse_args(argv)

    records = load_records(args.source)
    if not records:
        return 0

    rows = [r for r in (to_row(rec) for rec in records) if r is not None]
    skipped = len(records) - len(rows)

    print(f"[seed] {len(rows)} usable record(s) from {args.source}" + (f" ({skipped} skipped)" if skipped else ""))
    for row in rows:
        dest = row.get("benchmark_dest") or "(none)"
        print(f"  {row['owner']}/{row['repo']}#{row['issue_number']:<6} benchmark_dest={dest}")

    if not args.apply:
        print("\n[seed] DRY RUN — pass --apply to write. expected_n is never seeded;")
        print("[seed] it is a hand-declared judgement and stays NULL until set.")
        return 0

    client = get_client()
    client.table(TABLE).upsert(rows, on_conflict="owner,repo,issue_number").execute()
    print(f"\n[seed] upserted {len(rows)} row(s) into {TABLE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
