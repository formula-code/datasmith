"""Survey counts: task phases from the state files and decisions from the review file."""

from __future__ import annotations

import collections
from typing import Any

from datasmith.verify.survey import Survey, task_id


def counts(survey: Survey) -> dict[str, Any]:
    states = survey.states()
    reviews = survey.reviews()
    phases = collections.Counter(s.get("phase") or "not_started" for s in states.values())
    reviewed = {task_id(r["task"]) for r in reviews}
    if reviewed - set(states):
        phases["not_started"] += len(reviewed - set(states))
    return {
        "tasks": len(reviewed | set(states)),
        "phases": dict(phases.most_common()),
        "review": dict(collections.Counter(r["decision"] or "pending" for r in reviews).most_common()),
        "nodes": dict(collections.Counter(str(s.get("node") or "unknown") for s in states.values()).most_common()),
    }
