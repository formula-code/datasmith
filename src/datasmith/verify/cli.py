"""fc-verify: per-task approval and repair stages on a survey folder.

fc-verify status   SURVEY
fc-verify classify SURVEY [TASK ...]             oracle up/down per task on the noise rule
fc-verify report   SURVEY [TASK ...]             review summaries (default: measured tasks pending review)
fc-verify review   SURVEY TASK DECISION [--note]
fc-verify auto     SURVEY [--dry-run]            automatic not_approved rules
fc-verify add      SURVEY TASK ...               pending rows for new tasks
fc-verify precheck SURVEY TASK ... [--measure-uncovered always_affected,compiled] [--rerun]
fc-verify failure  LOG ...                       error class of failed image builds
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from datasmith.verify import precheck, report, review, status
from datasmith.verify.noise import classify_all, load_rule
from datasmith.verify.survey import DECISIONS, Survey, task_id


def _tasks(ids: list[str]) -> list[str]:
    return [task_id(t) if "#" in t else t for t in ids]


def _status(survey: Survey, a: argparse.Namespace) -> None:
    print(json.dumps(status.counts(survey), indent=1))


def _classify(survey: Survey, a: argparse.Namespace) -> None:
    stats = classify_all(survey.records(), load_rule(survey.rule_path))
    for t in _tasks(a.tasks) or sorted(stats):
        if t in stats:
            print(json.dumps({k: v for k, v in stats[t].items() if k not in ("up", "down")}))


def _report(survey: Survey, a: argparse.Namespace) -> None:
    for s in report.summaries(survey, load_rule(survey.rule_path), _tasks(a.tasks) or None):
        print(report.render(s), end="\n\n")


def _review(survey: Survey, a: argparse.Namespace) -> None:
    print(",".join(review.set_decision(survey, a.task, a.decision, a.note, a.data).values()))


def _auto(survey: Survey, a: argparse.Namespace) -> None:
    picked = review.apply_auto(survey, dry_run=a.dry_run)
    for d in picked:
        print(f"{d['task']}: {d['detail']}")
    print(f"{'would mark' if a.dry_run else 'marked'} {len(picked)} not_approved")


def _add(survey: Survey, a: argparse.Namespace) -> None:
    print("\n".join(review.add_pending(survey, _tasks(a.tasks))))


def _precheck(survey: Survey, a: argparse.Namespace) -> None:
    kinds = set(filter(None, a.measure_uncovered.split(",")))
    for t in _tasks(a.tasks):
        st = survey.state(t)
        if not st.get("tag"):
            print(json.dumps({"task": t, "error": "no image tag in state"}))
            continue
        end, result = precheck.precheck(survey, t, st["tag"], kinds, rerun=a.rerun)
        if end:
            survey.save_state(t, {**st, "phase": end})
        print(json.dumps({"task": t, "end": end, **{k: result.get(k) for k in ("by_deps", "patch_failed", "error")}}))


COMMANDS = {
    "status": _status,
    "classify": _classify,
    "report": _report,
    "review": _review,
    "auto": _auto,
    "add": _add,
    "precheck": _precheck,
}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="fc-verify", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("status", "classify", "report", "review", "auto", "add", "precheck"):
        p = sub.add_parser(name)
        p.add_argument("survey", type=Path)
        if name in ("classify", "report", "add", "precheck"):
            p.add_argument("tasks", nargs="*")
    sub.choices["review"].add_argument("task")
    sub.choices["review"].add_argument("decision", choices=DECISIONS)
    sub.choices["review"].add_argument("--note", default="")
    sub.choices["review"].add_argument("--data", default="phase 1")
    sub.choices["auto"].add_argument("--dry-run", action="store_true")
    sub.choices["precheck"].add_argument(
        "--measure-uncovered", default="", help=",".join(precheck.MEASURE_UNCOVERED_KINDS)
    )
    sub.choices["precheck"].add_argument("--rerun", action="store_true")
    sub.add_parser("failure").add_argument("logs", nargs="+", type=Path)
    a = ap.parse_args(argv)

    if a.cmd == "failure":
        for log in a.logs:
            print(json.dumps({"log": str(log), **report.failure_reason(log)}))
        return 0
    COMMANDS[a.cmd](Survey(a.survey), a)
    return 0


if __name__ == "__main__":
    sys.exit(main())
