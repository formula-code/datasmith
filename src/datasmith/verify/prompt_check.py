"""Check that a task prompt describes only the symptom: no ASV benchmarks, no code the patch changes, no code or diff,
no PR or issue links, no Relevant Issues section. Returns the findings; an empty list passes."""

from __future__ import annotations

import re
from pathlib import Path

from datasmith.verify.prompts import ENDS, START

ASV = re.compile(r"\bASV\b|\basv\b|\b[Bb]enchmarks?\b|\btime_\w+|\bpeakmem_\w+")
LINK = re.compile(r"https?://github\.com/\S+/(pull|issues)/\d+|(?<![\w/])#\d{2,}\b")
CODE = re.compile(r"```|^\+\+\+ |^--- a/|^@@ ", re.M)
DEF = re.compile(r"^[+-]?\s*(?:async\s+)?(?:def|class|cdef|cpdef)\s+([A-Za-z_]\w{3,})", re.M)
HUNK = re.compile(r"^@@ .*@@\s*(?:async\s+)?(?:def|class|cdef|cpdef)\s+([A-Za-z_]\w{3,})", re.M)


PUBLIC_HOOK = re.compile(r"_(repr_\w+|ipython_\w+)_")


def task_text(instruction: str) -> str:
    """The text between the task description lead-in and Task Requirements."""
    i = instruction.find(START)
    i = i + len(START) if i >= 0 else 0
    j = instruction.find("\n## Task Requirements", i)
    return instruction[i : j if j >= 0 else len(instruction)]


def changed_names(patch: str) -> set[str]:
    """Functions and classes the patch defines or changes (from added/removed lines and hunk headers)."""
    names = set(DEF.findall("\n".join(line for line in patch.splitlines() if line[:1] in "+-")))
    names |= set(HUNK.findall(patch))
    return {n for n in names if not n.startswith("test")}


def check(instruction: str, patch: str = "", benchmarks: set[str] | None = None) -> list[str]:
    text = task_text(instruction)
    found = []
    if ENDS[0].strip() in text:
        found.append("relevant issues section")
    if CODE.search(text):
        found.append("code or diff")
    if m := LINK.search(text):
        found.append(f"link {m.group(0)}")
    if m := ASV.search(text):
        found.append(f"names benchmarks: {m.group(0)}")
    for b in sorted(benchmarks or ()):
        parts = [p for p in b.split(".") if len(p) > 3 and not p.startswith("time_")]
        if any(re.search(rf"\b{re.escape(p)}\b", text) for p in parts[1:2]) or b in text:
            found.append(f"benchmark {b}")
            break
    # A public entry point may be named (giveaway score 1); an internal function or class may not.
    # IPython display hooks such as _repr_html_ are public.
    for name in sorted(n for n in changed_names(patch) if n.startswith("_") and not PUBLIC_HOOK.fullmatch(n)):
        if re.search(rf"\b{re.escape(name)}\b", text):
            found.append(f"changed internal code {name}")
    return found


def check_task(task_dir: Path, benchmarks: set[str] | None = None) -> list[str]:
    solve = task_dir / "solution" / "solve.sh"
    m = re.search(r"<< '__SOLUTION__'\n(.*?)\n__SOLUTION__", solve.read_text(), re.S) if solve.exists() else None
    return check((task_dir / "instruction.md").read_text(), m.group(1) if m else "", benchmarks)
