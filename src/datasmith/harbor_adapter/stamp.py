"""Render provenance stamp for Harbor task directories.

Every ``FormulaCodeAdapter.generate_task`` call writes ``render_stamp.json`` at
the task root recording WHICH render source produced the directory: the
datasmith commit, a digest over the template payload plus the render code, and
the render parameters (rounds, cpus, ...). ``check`` recomputes the digest from
the currently installed datasmith and reports every task dir whose stamp is
missing or whose digest differs, i.e. rendered dirs that have drifted from the
committed source (hot patches, stale renders, a different datasmith checkout).

    python -m datasmith.harbor_adapter.stamp check /path/to/tasks/*

Exit status is 1 when any directory is unstamped or drifted.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import subprocess
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STAMP_NAME = "render_stamp.json"

_HERE = Path(__file__).resolve().parent
_TEMPLATE_DIR = _HERE / "template"
# Render code whose changes alter the rendered output (jinja context, task.toml
# renderer, static-file list). Template files are covered by _TEMPLATE_DIR.
_RENDER_SOURCES = ("adapter.py", "utils.py", "records.py")


def _iter_digest_files() -> Iterable[tuple[str, Path]]:
    for path in sorted(_TEMPLATE_DIR.rglob("*")):
        if path.is_file() and path.suffix not in (".pyc", ".pyo") and "__pycache__" not in path.parts:
            yield path.relative_to(_HERE).as_posix(), path
    for name in _RENDER_SOURCES:
        yield name, _HERE / name


def render_source_digest() -> str:
    """sha256 over the template payload and the render code, in a fixed order."""
    h = hashlib.sha256()
    for rel, path in _iter_digest_files():
        h.update(rel.encode())
        h.update(b"\0")
        h.update(path.read_bytes())
        h.update(b"\0")
    return h.hexdigest()


def source_commit() -> dict[str, Any]:
    """Best-effort git identity of the datasmith checkout rendering the task."""
    info: dict[str, Any] = {"commit": None, "dirty": None}
    # Not a git checkout (wheel install) or no git binary: the digest alone still
    # identifies the render source, so a missing commit is not an error.
    with contextlib.suppress(OSError, subprocess.CalledProcessError):
        commit = subprocess.check_output(
            ["git", "-C", str(_HERE), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
        status = subprocess.check_output(
            ["git", "-C", str(_HERE), "status", "--porcelain", "--", str(_HERE)],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        info["commit"] = commit
        info["dirty"] = bool(status.strip())
    return info


def write_stamp(task_dir: Path, **params: Any) -> Path:
    stamp = {
        "datasmith": source_commit(),
        "render_source_sha256": render_source_digest(),
        "rendered_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "params": params,
    }
    path = Path(task_dir) / STAMP_NAME
    path.write_text(json.dumps(stamp, indent=2, sort_keys=True) + "\n")
    return path


def read_stamp(task_dir: Path) -> dict[str, Any] | None:
    path = Path(task_dir) / STAMP_NAME
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def check_task_dirs(task_dirs: Iterable[Path]) -> list[str]:
    """Return one problem line per unstamped or drifted task dir (empty = all good)."""
    expected = render_source_digest()
    problems: list[str] = []
    for task_dir in task_dirs:
        task_dir = Path(task_dir)
        if not (task_dir / "task.toml").is_file():
            problems.append(f"{task_dir}: not a harbor task dir (no task.toml)")
            continue
        stamp = read_stamp(task_dir)
        if stamp is None:
            problems.append(f"{task_dir}: UNSTAMPED (rendered before stamps existed, or hand-written)")
            continue
        got = stamp.get("render_source_sha256")
        if got != expected:
            commit = (stamp.get("datasmith") or {}).get("commit")
            problems.append(
                f"{task_dir}: DRIFT render_source_sha256 {str(got)[:12]} (datasmith {str(commit)[:9]}) "
                f"!= installed {expected[:12]}"
            )
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m datasmith.harbor_adapter.stamp", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    chk = sub.add_parser("check", help="verify rendered task dirs match the installed render source")
    chk.add_argument("task_dirs", nargs="+", type=Path)
    sub.add_parser("digest", help="print the installed render-source digest and git identity")
    args = ap.parse_args(argv)

    if args.cmd == "digest":
        print(json.dumps({"datasmith": source_commit(), "render_source_sha256": render_source_digest()}, indent=2))
        return 0

    problems = check_task_dirs(args.task_dirs)
    n = len(list(args.task_dirs))
    for line in problems:
        print(line)
    print(f"[stamp] checked {n} task dir(s): {n - len(problems)} ok, {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
