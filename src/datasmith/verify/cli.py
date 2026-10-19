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
fc-verify readiness SURVEY OUT_CSV [--registry-tags FILE] [--fetch-github] [--giveaway JSONL]
fc-verify records  IDS_MD OUT_DIR                  render records for chosen owner/repo#N ids (database)
fc-verify select   OUT_DIR --hub TSV [--n 130] [--seed S] [--skip IDS_MD ...] [--exclude-repo owner/repo ...]
fc-verify env-image owner/repo#N ... [--registry URI]   environment image rebuilt at the base commit
fc-verify prompt   TASKS_ROOT PROMPTS_DIR [TASK ...] [--dry-run] [--drafts]   approved prompt text into rendered tasks
fc-verify prompt-check TASKS_ROOT [TASK ...] [--model M --api-base URL]   symptom-only check, optional model score
fc-verify prompt-draft TASKS_ROOT OUT_DIR TASK ... --model M [--api-base URL]   short prompt drafts for review
fc-verify failure  LOG ...                       error class of failed image builds
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from datasmith.measurement.noise import classify_all, load_rule
from datasmith.verify import images, pin, precheck, registry, release, report, review, status
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


COMMANDS = {
    "status": _status,
    "classify": _classify,
    "report": _report,
    "review": _review,
    "auto": _auto,
    "add": _add,
    "precheck": _precheck,
    "readiness": _readiness,
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


def _records(a: argparse.Namespace) -> int:
    from datasmith.utils.db import get_client
    from datasmith.verify import selection

    ids = re.findall(r"[\w.-]+/[\w.-]+#\d+", a.ids.read_text())
    recs, missing = selection.records_for(get_client(), ids)
    rows = [{"task": f"{r['repo_name']}#{r['issue_number']}", "record": r} for r in recs]
    selection.write_selection(rows, a.out)
    print(f"{len(recs)} records in {a.out}; missing or incomplete: {missing or 'none'}")
    return 0


def _select(a: argparse.Namespace) -> int:
    from datasmith.utils.db import get_client
    from datasmith.verify import selection

    if a.refresh_hub:
        selection.refresh_hub(a.hub)
    hub = selection.hub_tags(a.hub)
    known = {t for f in a.skip for t in re.findall(r"[\w.-]+/[\w.-]+#\d+", f.read_text())}
    exclude = {selection.image_name(*r.split("/")) for r in a.exclude_repo}
    pool = selection.filter_pool(
        selection.db_pool(get_client(), a.counts), hub, known, exclude, tuple(a.tier1), a.built_from, a.counts
    )
    a.out.mkdir(parents=True, exist_ok=True)

    def at_base(c: dict) -> bool:
        image, tag = c["record"]["container_name"].split("/", 1)[1].split(":")
        return selection.image_ref(image, int(tag), a.out / "image_refs.tsv") == c["record"]["base_commit"]

    rows = selection.sample([], pool, a.n, a.seed, lambda repo: a.cap, at_base)
    selection.write_selection(rows, a.out)
    print(f"wrote {len(rows)} rows to {a.out}")
    return 0


def _env_image(a: argparse.Namespace) -> int:
    from datasmith.utils.db import get_client
    from datasmith.verify import env_image

    client = get_client()
    for t in a.tasks:
        owner, repo, num = re.fullmatch(r"([\w.-]+)/([\w.-]+)#(\d+)", t).groups()
        print(json.dumps({"task": t, **env_image.build(client, owner, repo, int(num), a.registry)}))
    return 0


def _prompt(a: argparse.Namespace) -> int:
    from datasmith.verify import prompts

    for t in prompts.apply(a.tasks_root, a.prompts, a.tasks or None, a.dry_run, a.drafts):
        print(f"{t}: prompt applied" + (" (dry run)" if a.dry_run else ""))
    return 0


def _model(a: argparse.Namespace):
    import os

    from datasmith.verify import short_prompt

    return short_prompt.lm(a.model, a.api_base, os.environ.get(a.api_key_env))


def _prompt_check(a: argparse.Namespace) -> int:
    from datasmith.verify import prompt_check, short_prompt

    model = _model(a) if a.model else None
    tasks = a.tasks or sorted(p.name for p in a.tasks_root.iterdir() if (p / "instruction.md").exists())
    failed = 0
    for t in tasks:
        d = a.tasks_root / t
        row = {"task": t, "findings": prompt_check.check_task(d)}
        if model:
            row.update(
                short_prompt.score(
                    prompt_check.task_text((d / "instruction.md").read_text()), short_prompt._patch(d), model
                )
            )
        failed += bool(row["findings"]) or row.get("score", 0) > short_prompt.TARGET_SCORE
        print(json.dumps(row))
    print(f"{failed} of {len(tasks)} prompts are not symptom-only", file=sys.stderr)
    return 1 if failed else 0


def _prompt_draft(a: argparse.Namespace) -> int:
    from datasmith.verify import short_prompt

    for t, v in short_prompt.draft_tasks(a.tasks_root, a.out, _model(a), a.tasks).items():
        if t in a.tasks:
            print(json.dumps({"task": t, "score": v["score"], "findings": v["findings"]}))
    return 0


def _failure(a: argparse.Namespace) -> int:
    for log in a.logs:
        print(json.dumps({"log": str(log), **report.failure_reason(log)}))
    return 0


NO_SURVEY = {
    "prompt-check": _prompt_check,
    "prompt-draft": _prompt_draft,
    "prompt": _prompt,
    "records": _records,
    "select": _select,
    "env-image": _env_image,
    "push": _push,
    "build": _build,
    "publish": _publish,
    "pins": _pins,
    "failure": _failure,
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
        "readiness",
    ):
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
    rd = sub.choices["readiness"]
    rd.add_argument("out_csv", type=Path)
    rd.add_argument("--registry-tags", type=Path, help="file with one registry tag per line")
    rd.add_argument("--fetch-github", action="store_true", help="look up repo licenses and PR authors")
    rd.add_argument("--giveaway", type=Path, help="JSON lines with task and score from a prompt audit")
    rc = sub.add_parser("records")
    rc.add_argument("ids", type=Path)
    rc.add_argument("out", type=Path)
    se = sub.add_parser("select")
    se.add_argument("out", type=Path)
    se.add_argument("--hub", type=Path, required=True, help="Docker Hub tag list (tsv)")
    se.add_argument("--refresh-hub", action="store_true")
    se.add_argument("--n", type=int, default=130)
    se.add_argument("--seed", default="fc-expand")
    se.add_argument("--cap", type=int, default=5, help="tasks per repo")
    se.add_argument("--skip", type=Path, nargs="*", default=[], help="lists of owner/repo#N already chosen")
    se.add_argument("--exclude-repo", action="append", default=[])
    se.add_argument("--tier1", nargs=2, default=["2026-08-18", "2026-09-01"], help="push dates of tier 1 images")
    se.add_argument(
        "--built-from", default="2026-04-12T19", help="images pushed earlier were built at the merge commit"
    )
    se.add_argument("--counts", action="store_true")
    ei = sub.add_parser("env-image")
    ei.add_argument("tasks", nargs="+")
    ei.add_argument("--registry", help="repository URI to push the result to")
    pr = sub.add_parser("prompt")
    pr.add_argument("tasks_root", type=Path)
    pr.add_argument("prompts", type=Path)
    pr.add_argument("tasks", nargs="*")
    pr.add_argument("--dry-run", action="store_true")
    pr.add_argument("--drafts", action="store_true", help="also apply unapproved drafts (use on a preview copy)")
    for name in ("prompt-check", "prompt-draft"):
        q = sub.add_parser(name)
        q.add_argument("tasks_root", type=Path)
        if name == "prompt-draft":
            q.add_argument("out", type=Path)
        q.add_argument("tasks", nargs="*" if name == "prompt-check" else "+")
        q.add_argument("--model", required=name == "prompt-draft", help="model name, e.g. openai/<served name>")
        q.add_argument("--api-base")
        q.add_argument("--api-key-env", default="OPENAI_API_KEY", help="environment variable holding the API key")
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
