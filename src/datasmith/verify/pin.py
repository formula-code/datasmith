"""Pin a built image into a task's task.toml (`[environment] docker_image`).

A row is pinned only when the task's environment/ still has the digest the image was built from.
"""

from __future__ import annotations

import re
from pathlib import Path

from datasmith.verify.images import env_digest

SECTION = re.compile(r"^\s*\[environment\]\s*$")
PIN = re.compile(r"""^\s*docker_image\s*=\s*["']([^"']*)["']""")


def _env_span(lines: list[str]) -> tuple[int, int] | None:
    start = next((i for i, line in enumerate(lines) if SECTION.match(line)), None)
    if start is None:
        return None
    end = next((i for i in range(start + 1, len(lines)) if lines[i].lstrip().startswith("[")), len(lines))
    return start, end


def read_pin(toml_text: str) -> str | None:
    lines = toml_text.splitlines()
    start, end = _env_span(lines) or (0, 0)
    return next((m[1] or None for line in lines[start + 1 : end] if (m := PIN.match(line))), None)


def set_pin(toml_text: str, tag: str) -> str:
    lines, new = toml_text.splitlines(), f'docker_image = "{tag}"'
    span = _env_span(lines)
    if span is None:
        return toml_text.rstrip("\n") + f"\n\n[environment]\n{new}\n"
    i = next((i for i in range(span[0] + 1, span[1]) if PIN.match(lines[i])), None)
    if i is None:
        lines.insert(span[0] + 1, new)
    else:
        lines[i] = new
    return "\n".join(lines) + "\n"


def plan(task_dir: Path, row: dict | None) -> tuple[str, str | None]:
    """(status, tag to pin or None) for one task and its newest manifest row."""
    pin = read_pin((task_dir / "task.toml").read_text())
    if row is None:
        return ("unbuilt" if pin is None else "foreign-pin"), None
    if not row.get("verified"):
        return ("unverified-pinned" if pin == row["tag"] else "unverified"), None
    if env_digest(task_dir / "environment") != row.get("env_digest"):
        return "env-mismatch", None
    if pin == row["tag"]:
        return "ok", None
    return ("unpinned" if pin is None else "drift"), row["tag"]


def write_pin(task_dir: Path, tag: str) -> None:
    toml = task_dir / "task.toml"
    tmp = toml.with_suffix(".toml.tmp")
    tmp.write_text(set_pin(toml.read_text(), tag))
    # Rename, so a hard-linked copy never writes through to the render.
    tmp.replace(toml)
