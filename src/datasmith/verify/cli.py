"""fc-verify: per-task approval and repair stages on a survey folder.

fc-verify status   SURVEY
fc-verify classify SURVEY [TASK ...]             oracle up/down per task on the noise rule
fc-verify report   SURVEY [TASK ...]             review summaries (default: measured tasks pending review)
fc-verify review   SURVEY TASK DECISION [--note]
fc-verify auto     SURVEY [--dry-run]            automatic not_approved rules
fc-verify add      SURVEY TASK ...               pending rows for new tasks
fc-verify precheck SURVEY TASK ... [--measure-uncovered always_affected,compiled] [--rerun]
fc-verify build    TASKS_ROOT OUT [--ids FILE ...] [--workers N] [--retry-failed]
fc-verify publish  TASKS_ROOT MANIFEST OUT [--region R] [--repo NAME] [--until-idle]
fc-verify push     MANIFEST OUT_JSONL [--region R] [--repo NAME]  push verified rows as they are (no scrub)
fc-verify pins     TASKS_ROOT MANIFEST                    pin status of every task folder
fc-verify measure  SURVEY TASK ... [--manifest M] [--workers N] [--oracle-runs 3] [--nop-runs 3] [--gate URL]
fc-verify capture  SURVEY TASK ... [--manifest M]    snapshot reference and its no-change check
fc-verify release-leases JOB_DIR [--gate URL]      free the gate slots of a stopped job
fc-verify readiness SURVEY OUT_CSV [--registry-tags FILE] [--fetch-github] [--giveaway JSONL]
fc-verify failure  LOG ...                       error class of failed image builds
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from datasmith.verify import capture, images, measure, pin, precheck, registry, release, report, review, status
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


def _measure(survey: Survey, a: argparse.Namespace) -> None:
    s = measure.Settings(
        n_oracle=a.oracle_runs,
        n_nop=a.nop_runs,
        gate_url=a.gate,
        confirm_rounds=a.confirm_rounds,
        max_heavy=a.max_heavy,
        harbor=a.harbor,
    )
    for task, phase in measure.measure_many(survey, _tasks(a.tasks), s, a.workers, a.manifest).items():
        print(f"{task}: {phase}")


def _readiness(survey: Survey, a: argparse.Namespace) -> None:
    tags = set(a.registry_tags.read_text().split()) if a.registry_tags else None
    giveaway = {}
    if a.giveaway:
        for line in a.giveaway.read_text().splitlines():
            if line.strip():
                g = json.loads(line)
                giveaway[g["task"]] = g.get("score", "")
    rows = release.readiness(survey, registry=tags, fetch=a.fetch_github, giveaway=giveaway)
    release.write_csv(rows, a.out_csv)
    print(json.dumps(release.summary(rows), indent=1))


def _capture(survey: Survey, a: argparse.Namespace) -> None:
    s = measure.Settings(gate_url=a.gate, harbor=a.harbor)
    for t in _tasks(a.tasks):
        print(f"{t}: {capture.capture(survey, t, s, a.manifest)}")


COMMANDS = {
    "status": _status,
    "classify": _classify,
    "report": _report,
    "review": _review,
    "auto": _auto,
    "add": _add,
    "precheck": _precheck,
    "measure": _measure,
    "readiness": _readiness,
    "capture": _capture,
}


def _ids(files: list[Path], root: Path) -> list[str]:
    out = []
    for f in files:
        for line in f.read_text().splitlines():
            if m := re.search(r"([\w.-]+)/([\w.-]+)#(\d+)", line):
                out.append("__".join(m.groups()))
            elif re.fullmatch(r"[\w.-]+__[\w.-]+__\d+", line.strip()):
                out.append(line.strip())
    return out or sorted(p.name for p in root.iterdir() if (p / "environment").is_dir())


def _build(a: argparse.Namespace) -> int:
    images.build(
        a.tasks_root,
        _ids(a.ids, a.tasks_root),
        a.out,
        a.workers,
        images.Limits(keep_failed=a.keep_failed),
        retry_failed=a.retry_failed,
    )
    return 0


def _publish(a: argparse.Namespace) -> int:
    def building() -> bool:
        return images.sh(["pgrep", "-f", "[f]c-verify build"]).returncode == 0

    registry.publish(a.tasks_root, a.manifest, a.out, registry.EcrRegistry(a.region, a.repo), a.until_idle, building)
    return 0


def _push(a: argparse.Namespace) -> int:
    registry.push_rows(a.manifest, a.out, registry.EcrRegistry(a.region, a.repo))
    return 0


def _pins(a: argparse.Namespace) -> int:
    rows, bad = images.rows_by_task(a.manifest), 0
    for task_dir in sorted(p for p in a.tasks_root.iterdir() if (p / "task.toml").is_file()):
        state, tag = pin.plan(task_dir, rows.get(task_dir.name))
        if state not in ("ok", "unbuilt"):
            bad += state in ("drift", "env-mismatch", "unverified-pinned")
            print(f"{task_dir.name}: {state}" + (f" -> {tag}" if tag else ""))
    return 1 if bad else 0


def _release(a: argparse.Namespace) -> int:
    print("\n".join(measure.release_leases(a.job_dir, a.gate)))
    return 0


def _failure(a: argparse.Namespace) -> int:
    for log in a.logs:
        print(json.dumps({"log": str(log), **report.failure_reason(log)}))
    return 0


NO_SURVEY = {
    "push": _push,
    "build": _build,
    "publish": _publish,
    "pins": _pins,
    "failure": _failure,
    "release-leases": _release,
}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="fc-verify", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in (
        "status",
        "classify",
        "report",
        "review",
        "auto",
        "add",
        "precheck",
        "measure",
        "readiness",
        "capture",
    ):
        p = sub.add_parser(name)
        p.add_argument("survey", type=Path)
        if name in ("classify", "report", "add", "precheck", "measure", "capture"):
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
    b = sub.add_parser("build")
    b.add_argument("tasks_root", type=Path)
    b.add_argument("out", type=Path)
    b.add_argument(
        "--ids", type=Path, action="append", default=[], help="files of owner/repo#N or owner__repo__N lines"
    )
    b.add_argument("--workers", type=int, default=1)
    b.add_argument("--retry-failed", action="store_true")
    b.add_argument("--keep-failed", action="store_true")
    pb = sub.add_parser("publish")
    pb.add_argument("tasks_root", type=Path)
    pb.add_argument("manifest", type=Path)
    pb.add_argument("out", type=Path)
    pb.add_argument("--region", default="us-east-1")
    pb.add_argument("--repo", default="formulacode")
    pb.add_argument("--until-idle", action="store_true", help="keep publishing while a build runs")
    m = sub.choices["measure"]
    m.add_argument("--manifest", type=Path, help="image manifest; images are loaded before measuring")
    m.add_argument("--workers", type=int, default=1)
    m.add_argument("--oracle-runs", type=int, default=3)
    m.add_argument("--nop-runs", type=int, default=3)
    m.add_argument("--gate", default="http://127.0.0.1:8267")
    m.add_argument("--confirm-rounds", type=int, default=0)
    m.add_argument("--max-heavy", type=int, default=1)
    m.add_argument("--harbor", default="harbor")
    c = sub.choices["capture"]
    c.add_argument("--manifest", type=Path)
    c.add_argument("--gate", default="http://127.0.0.1:8267")
    c.add_argument("--harbor", default="harbor")
    rd = sub.choices["readiness"]
    rd.add_argument("out_csv", type=Path)
    rd.add_argument("--registry-tags", type=Path, help="file with one registry tag per line")
    rd.add_argument("--fetch-github", action="store_true", help="look up repo licenses and PR authors")
    rd.add_argument("--giveaway", type=Path, help="JSON lines with task and score from a prompt audit")
    rl = sub.add_parser("release-leases")
    rl.add_argument("job_dir", type=Path)
    rl.add_argument("--gate", default="http://127.0.0.1:8267")
    pu = sub.add_parser("push")
    pu.add_argument("manifest", type=Path)
    pu.add_argument("out", type=Path)
    pu.add_argument("--region", default="us-east-1")
    pu.add_argument("--repo", default="formulacode")
    pp = sub.add_parser("pins")
    pp.add_argument("tasks_root", type=Path)
    pp.add_argument("manifest", type=Path)
    a = ap.parse_args(argv)
    if a.cmd in NO_SURVEY:
        return NO_SURVEY[a.cmd](a)

    COMMANDS[a.cmd](Survey(a.survey), a)
    return 0


if __name__ == "__main__":
    sys.exit(main())
