"""Review decisions in the survey's manual_review.csv, and the automatic not_approved rules.

Automatic rules only turn a pending task into not_approved; they never approve and never change a decided row.
"""

from __future__ import annotations

import datetime
from typing import Any

from datasmith.verify.noise import classify_all, load_rule
from datasmith.verify.survey import DECISIONS, UNMEASURED, Survey, display_id, task_id

NOTE_UP_ZERO = "auto: no oracle benchmark above noise (oracle up 0)"
NOTE_UNMEASURED = {
    "uncovered": "uncovered: no benchmark runs the code the patch changes",
    "base_edited": "base edited: the patch changes the benchmark files",
    "patch_failed": "patch failed: the patch did not apply in the image",
    "no_benchmarks": "no benchmarks: benchmark suite empty or failed to load at the base commit",
    "image_failed": "image build failed",
    "error": "build failed (state error)",
    "too_slow": "too slow: the oracle measurement exceeded the time limit",
}


def set_decision(
    survey: Survey, task: str, decision: str, note: str = "", data: str = "phase 1", reviewer: str = "user"
) -> dict[str, str]:
    """Record one decision; `task` is owner/repo#N or owner__repo__N."""
    if decision not in DECISIONS:
        raise ValueError(f"decision must be one of {DECISIONS}")
    key = (display_id(task) if "__" in task else task).lower()
    rows = survey.reviews()
    hit = [r for r in rows if r["task"].lower() == key]
    if not hit:
        raise KeyError(f"{task}: not in {survey.review_path}")
    today = datetime.date.today().isoformat()
    hit[0].update(
        decision=decision, reviewer=reviewer, date=today, data=data, note=note or f"{reviewer} review {today}"
    )
    survey.write_reviews(rows)
    return hit[0]


def add_pending(survey: Survey, tasks: list[str]) -> list[str]:
    """Add a pending row for every task (owner__repo__N) that has none; returns the added display ids."""
    rows = survey.reviews()
    have = {r["task"].lower() for r in rows}
    added = [display_id(t) for t in tasks if display_id(t).lower() not in have]
    if added:
        rows += [{"task": t, "decision": "pending", "reviewer": "", "date": "", "data": "", "note": ""} for t in added]
        survey.write_reviews(rows)
    return added


def auto_decisions(survey: Survey) -> list[dict[str, Any]]:
    """Pending tasks that an automatic rule marks not_approved, with the rule's note.

    Rules: a measured task with no oracle benchmark up beyond noise; a task in an unmeasured end state.
    """
    states = survey.states()
    records = survey.records()
    stats = classify_all(records, load_rule(survey.rule_path)) if records and survey.rule_path.exists() else {}
    out = []
    for row in survey.reviews():
        if row["decision"] not in ("", "pending"):
            continue
        task = task_id(row["task"])
        phase = states.get(task, {}).get("phase")
        if phase == "measured" and task in stats and stats[task]["oracle_up"] == 0:
            s = stats[task]
            out.append({
                "task": task,
                "rule": "oracle_up_0",
                "note": NOTE_UP_ZERO,
                "detail": f"oracle up 0, down {s['oracle_down']}, benchmarks {s['n_benchmarks']}",
            })
        elif phase in UNMEASURED:
            out.append({
                "task": task,
                "rule": phase,
                "note": f"auto: no usable measurement; {NOTE_UNMEASURED[phase]}",
                "detail": phase,
            })
    return out


def apply_auto(survey: Survey, dry_run: bool = False) -> list[dict[str, Any]]:
    picked = auto_decisions(survey)
    if picked and not dry_run:
        notes = {display_id(d["task"]).lower(): d["note"] for d in picked}
        today = datetime.date.today().isoformat()
        rows = survey.reviews()
        for r in rows:
            if r["task"].lower() in notes:
                r.update(
                    decision="not_approved", reviewer="auto", date=today, data="phase 1", note=notes[r["task"].lower()]
                )
        survey.write_reviews(rows)
    return picked
