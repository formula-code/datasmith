#!/usr/bin/env python3
"""Seal build breadcrumbs into /opt/formulacode/build_manifest.json.

Runs INSIDE the task image, after docker_build_final.sh, invoked from the
Dockerfile so it executes regardless of what build_final_sh contains.

STDLIB ONLY — the task image has no datasmith installed.

Contract: a missing breadcrumb becomes ``null``.  Sealing must never fail a
build that would otherwise succeed, so every read is defensive and the exit
code is 0 unless the output path is unwritable.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess  # noqa: S404
import sys

SCHEMA_VERSION = 1
DEFAULT_NOTES = "/opt/formulacode/notes.jsonl"
DEFAULT_OUT = "/opt/formulacode/build_manifest.json"

# Fields whose value is coerced out of the raw breadcrumb string.
_INT_FIELDS = ("issue_number", "discovered_n", "expected_n", "cpu_cap", "nproc", "rounds")
_BOOL_FIELDS = (
    "benchmark_dir_init_present",
    "benchmark_dest_present_post_clean",
    "discovery_fallback_used",
    "secrets_scan_clean",
)
_LIST_FIELDS = ("pins_requested", "pins_resolved")
_STR_FIELDS = (
    "owner", "repo", "declared_commit", "head_at_seal",
    "image_digest", "lsv_sha", "reward_formula_id",
    "benchmark_dest", "benchmark_dir",
)


def parse_notes(lines: list[str]) -> dict[str, str]:
    """Parse breadcrumb lines into a flat dict. Last write wins.

    Malformed lines are skipped rather than raising — a corrupt breadcrumb
    must not cost us the whole manifest.
    """
    notes: dict[str, str] = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except (ValueError, TypeError):
            continue
        if isinstance(rec, dict) and "k" in rec:
            notes[str(rec["k"])] = "" if rec.get("v") is None else str(rec["v"])
    return notes


def _as_int(raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(float(raw))
    except (ValueError, TypeError):
        return None


def _as_bool(raw: str | None) -> bool | None:
    if raw is None or raw == "":
        return None
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _as_list(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    return [tok for tok in raw.split() if tok]


def build_block(notes: dict[str, str], introspected: dict[str, object]) -> dict:
    """Assemble the sealed ``build`` block.

    ``introspected`` values take precedence over breadcrumbs — they are read
    directly from the image at seal time and cannot be stale.
    """
    block: dict[str, object] = {}
    for key in _STR_FIELDS:
        block[key] = notes.get(key) or None
    for key in _INT_FIELDS:
        block[key] = _as_int(notes.get(key))
    for key in _BOOL_FIELDS:
        block[key] = _as_bool(notes.get(key))
    for key in _LIST_FIELDS:
        block[key] = _as_list(notes.get(key))
    for key, value in introspected.items():
        if value is not None:
            block[key] = value
    return block


def _introspect() -> dict[str, object]:
    """Read facts directly off the image at seal time."""
    found: dict[str, object] = {}
    try:
        head = subprocess.run(  # noqa: S603
            ["git", "-C", "/workspace/repo", "rev-parse", "HEAD"],  # noqa: S607
            capture_output=True, text=True, timeout=30,
        )
        if head.returncode == 0 and head.stdout.strip():
            found["head_at_seal"] = head.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        found["nproc"] = os.cpu_count()
    except NotImplementedError:
        pass
    return found


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Seal build breadcrumbs into a manifest")
    ap.add_argument("--notes", default=DEFAULT_NOTES)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args(argv)

    try:
        with open(args.notes, encoding="utf-8") as fh:
            lines = fh.readlines()
    except OSError:
        lines = []

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "build": build_block(parse_notes(lines), _introspect()),
        "verify": {},
    }

    try:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2, sort_keys=True)
    except OSError as exc:
        print(f"[emit_manifest] failed to write {args.out}: {exc}", file=sys.stderr)
        return 1

    print(f"[emit_manifest] sealed {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
