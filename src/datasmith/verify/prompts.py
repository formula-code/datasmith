"""Replace the task description of rendered tasks with approved prompt text; nothing else in the task changes.

A prompts folder holds versions.json ({task: {"version", "approved", "relevant_issues"}}), <task>.md with the new
Initial Observations, and optionally <task>.issues.md for the Relevant Issues section. Each applied task gets
PROMPT_VERSION.json, and its old instruction.md is kept outside the task folder, so harbor does not upload it.
"""

from __future__ import annotations

import datetime
import json
import shutil
from pathlib import Path

START = "understand the problem:\n\n"
ENDS = ("\n**Relevant Issues**", "\n## Task Requirements")


def rewrite(text: str, observations: str, issues: str | None) -> str:
    """The instruction with the problem text replaced; Relevant Issues replaced too when `issues` is given, else kept."""
    i = text.index(START) + len(START)
    ends = ENDS[1:] if issues is not None else ENDS
    j = min(k for k in (text.find(e, i) for e in ends) if k >= 0)
    section = f"\n**Relevant Issues**\n\n{issues.strip()}\n" if issues and issues.strip() else ""
    return text[:i] + "**Initial Observations**\n\n" + observations.strip() + "\n" + section + text[j:]


def apply(tasks_root: Path, prompts: Path, tasks: list[str] | None = None, dry_run: bool = False) -> list[str]:
    """Apply the approved prompts to the tasks that have one (all of versions.json by default); returns the tasks changed."""
    versions = json.loads((prompts / "versions.json").read_text())
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    done = []
    for task in tasks or sorted(versions):
        ins = tasks_root / task / "instruction.md"
        if task not in versions or not ins.exists():
            continue
        v = versions[task]
        issues = (prompts / v["relevant_issues"]).read_text() if v.get("relevant_issues") else None
        new = rewrite(ins.read_text(), (prompts / f"{task}.md").read_text(), issues)
        done.append(task)
        if dry_run:
            continue
        backup = tasks_root.parent / "prompt-backups" / task / f"instruction.md.{stamp}"
        backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ins, backup)
        ins.write_text(new)
        (tasks_root / task / "PROMPT_VERSION.json").write_text(json.dumps({**v, "task": task, "applied": stamp}) + "\n")
    return done
