"""Replace the task description of rendered tasks with approved prompt text; nothing else in the task changes.

A prompts folder holds versions.json ({task: {"version", "approved", "relevant_issues"}}), <task>.md with the new
Initial Observations, and optionally a file for the Relevant Issues section. Only entries whose "approved" is a review date
(YYYY-MM-DD) are applied. Each applied task gets
PROMPT_VERSION.json, and its old instruction.md is kept outside the task folder, so harbor does not upload it.
"""

from __future__ import annotations

import datetime
import json
import re
import shutil
from pathlib import Path

START = "understand the problem:\n\n"
ENDS = ("\n**Relevant Issues**", "\n## Task Requirements")
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def rewrite(text: str, observations: str, issues: str | None) -> str:
    """The instruction with the problem text replaced; Relevant Issues replaced too when `issues` is given, else kept."""
    i = text.index(START) + len(START)
    ends = ENDS[1:] if issues is not None else ENDS
    # Start one character back, so an empty problem text still finds the next section.
    j = min(k for k in (text.find(e, i - 1) for e in ends) if k >= 0)
    section = f"\n**Relevant Issues**\n\n{issues.strip()}\n" if issues and issues.strip() else ""
    return text[:i] + "**Initial Observations**\n\n" + observations.strip() + "\n" + section + text[j:]


def apply(
    tasks_root: Path, prompts: Path, tasks: list[str] | None = None, dry_run: bool = False, drafts: bool = False
) -> list[str]:
    """Apply the approved prompts (with drafts=True also unapproved ones, for a preview copy); returns the tasks changed."""
    versions = json.loads((prompts / "versions.json").read_text())
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    done = []
    for task in tasks or sorted(versions):
        ins = tasks_root / task / "instruction.md"
        if task not in versions or not ins.exists() or not (prompts / f"{task}.md").exists():
            continue
        v = versions[task]
        if not (isinstance(v.get("approved"), str) and DATE.fullmatch(v["approved"])) and not drafts:
            continue
        # relevant_issues: a file name replaces the section, "" removes it, null keeps it.
        rel = v.get("relevant_issues")
        if rel and ("\n" in rel or not (prompts / rel).is_file()):
            raise ValueError(f'{task}: relevant_issues must be a file name in {prompts}, "" or null')
        issues = (prompts / rel).read_text() if rel else ("" if rel == "" else None)
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
