"""Render (or re-render) Harbor task directories from a records file.

The datasmith pipeline (``fc-data --stage 7``) renders task dirs from live
Supabase rows and immediately runs them through Harbor. For RL we need a
render-only path that works on any box, without database access, from a
committed input. That input is a JSON-lines file of ``FormulaCodeRecord``
dicts, which is exactly what every rendered task dir already carries in
``tests/config.json``.

    # 1. one-time export from an existing rendered tree (records travel with the repo)
    python -m datasmith.harbor_adapter.regen export \
        --tasks-root initial_survey/tasks --ids-file rl_train_ids_gated.md \
        --ids-file rl_eval_ids_gated.md --out initial_survey/rl_task_records.jsonl

    # 2. reproducible render into a scratch dir (never over a live tree)
    python -m datasmith.harbor_adapter.regen render \
        --records initial_survey/rl_task_records.jsonl --out /scratch/tasks \
        --cpus 2 --memory 16G --storage 20G

    # 3. drift check (see stamp.py)
    python -m datasmith.harbor_adapter.regen check /scratch/tasks/*

Id files accept ``owner/repo#N`` anywhere on a line (the ``- owner/repo#N (v3=..)``
markdown lists, the ``owner_repo_issue`` tsv column) or bare ``owner__repo__N``
task dir names. Lines without an id are ignored, so headers and comments are fine.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable
from pathlib import Path

from datasmith.harbor_adapter.adapter import FormulaCodeAdapter, FormulaCodeRecord
from datasmith.harbor_adapter.stamp import check_task_dirs
from datasmith.harbor_adapter.utils import DATASMITH_LSV_ROUNDS

_ID_SLASH = re.compile(r"([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)#(\d+)")
_ID_DIR = re.compile(
    r"(?<![A-Za-z0-9_.-])([A-Za-z0-9.-]+(?:_[A-Za-z0-9.-]+)*)__([A-Za-z0-9.-]+(?:_[A-Za-z0-9.-]+)*)__(\d+)(?![A-Za-z0-9_])"
)

# FormulaCodeRecord fields, in declaration order. ``task_id`` (a derived property
# that config.json also surfaces) is intentionally not a record field.
_RECORD_FIELDS = (
    "container_name",
    "patch",
    "owner",
    "repo",
    "issue_number",
    "gt_hash",
    "base_commit",
    "instructions",
    "date",
    "classification",
    "difficulty",
    "repo_name",
)


def parse_ids(text: str) -> list[str]:
    """Task dir names (owner__repo__N) found in ``text``, first-seen order, de-duplicated."""
    seen: dict[str, None] = {}
    for line in text.splitlines():
        for m in _ID_SLASH.finditer(line):
            seen.setdefault(f"{m.group(1)}__{m.group(2)}__{m.group(3)}", None)
        if not _ID_SLASH.search(line):
            for m in _ID_DIR.finditer(line):
                seen.setdefault(f"{m.group(1)}__{m.group(2)}__{m.group(3)}", None)
    return list(seen)


def load_ids(files: Iterable[Path]) -> list[str]:
    ids: dict[str, None] = {}
    for f in files:
        for tid in parse_ids(Path(f).read_text()):
            ids.setdefault(tid, None)
    return list(ids)


def record_from_config(cfg: dict) -> FormulaCodeRecord:
    missing = [k for k in ("owner", "repo", "issue_number", "base_commit", "container_name") if not cfg.get(k)]
    if missing:
        raise ValueError(f"record missing required fields {missing}")
    fields = {k: cfg[k] for k in _RECORD_FIELDS if k in cfg}
    # verified-rl rows keep datasmith's label in difficulty_datasmith; difficulty may be a curriculum bucket.
    if cfg.get("difficulty_datasmith"):
        fields["difficulty"] = cfg["difficulty_datasmith"]
    elif cfg.get("difficulty_source"):
        fields.pop("difficulty", None)
    return FormulaCodeRecord(**fields)


def _load_records(path: Path) -> list[FormulaCodeRecord]:
    recs = []
    for n, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            recs.append(record_from_config(json.loads(line)))
        except Exception as exc:
            raise SystemExit(f"{path}:{n}: bad record: {exc}") from exc
    return recs


def cmd_export(args: argparse.Namespace) -> int:
    root = Path(args.tasks_root)
    ids = (
        load_ids(args.ids_file)
        if args.ids_file
        else sorted(p.parent.parent.name for p in root.glob("*/tests/config.json"))
    )
    out_lines, missing = [], []
    for tid in ids:
        cfg_path = root / tid / "tests" / "config.json"
        if not cfg_path.is_file():
            missing.append(tid)
            continue
        cfg = json.loads(cfg_path.read_text())
        rec = record_from_config(cfg)
        if rec.task_dir_name != tid:
            raise SystemExit(f"{cfg_path}: record names {rec.task_dir_name}, dir is {tid}")
        out_lines.append(json.dumps({k: getattr(rec, k) for k in _RECORD_FIELDS}, sort_keys=True))
    if missing:
        print(f"[export] MISSING {len(missing)} task dir(s) under {root}: {', '.join(missing)}", file=sys.stderr)
        if not args.allow_missing:
            return 1
    Path(args.out).write_text("\n".join(out_lines) + ("\n" if out_lines else ""))
    print(f"[export] wrote {len(out_lines)} record(s) to {args.out}")
    return 0


def cmd_render(args: argparse.Namespace) -> int:
    records = _load_records(Path(args.records))
    if args.ids_file:
        wanted = set(load_ids(args.ids_file))
        have = {r.task_dir_name for r in records}
        absent = sorted(wanted - have)
        if absent:
            print(f"[render] {len(absent)} id(s) not in {args.records}: {', '.join(absent)}", file=sys.stderr)
            if not args.allow_missing:
                return 1
        records = [r for r in records if r.task_dir_name in wanted]
    verifier_env = dict(kv.split("=", 1) for kv in args.verifier_env) or None
    out_root = Path(args.out)
    if out_root.exists() and any(out_root.iterdir()) and not args.force:
        raise SystemExit(f"{out_root} is not empty; pass --force to re-render into it")
    adapter = FormulaCodeAdapter(harbor_tasks_root=out_root, force=False)
    for rec in records:
        out_dir = adapter.generate_task(
            rec,
            run_pytest=not args.no_pytest,
            timeout_sec=args.timeout,
            cpus=args.cpus,
            memory=args.memory,
            storage=args.storage,
            rounds=args.rounds,
            verifier_env=verifier_env,
        )
        print(f"[render] {out_dir}")
    print(
        f"[render] {len(records)} task dir(s) -> {out_root} "
        f"(rounds={args.rounds} cpus={args.cpus} memory={args.memory} storage={args.storage})"
    )
    problems = check_task_dirs(out_root / r.task_dir_name for r in records)
    for line in problems:
        print(line, file=sys.stderr)
    return 1 if problems else 0


def cmd_check(args: argparse.Namespace) -> int:
    problems = check_task_dirs(args.task_dirs)
    for line in problems:
        print(line)
    print(
        f"[check] {len(args.task_dirs)} task dir(s): {len(args.task_dirs) - len(problems)} ok, {len(problems)} problem(s)"
    )
    return 1 if problems else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="fc-render-tasks", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    ex = sub.add_parser("export", help="collect tests/config.json records from a rendered tree into a jsonl")
    ex.add_argument("--tasks-root", required=True, type=Path)
    ex.add_argument(
        "--ids-file", action="append", type=Path, default=[], help="restrict to ids listed in this file (repeatable)"
    )
    ex.add_argument("--out", required=True, type=Path)
    ex.add_argument("--allow-missing", action="store_true")
    ex.set_defaults(fn=cmd_export)

    rn = sub.add_parser("render", help="render task dirs from a records jsonl")
    rn.add_argument("--records", required=True, type=Path)
    rn.add_argument("--out", required=True, type=Path, help="output tasks root (must be empty unless --force)")
    rn.add_argument(
        "--ids-file", action="append", type=Path, default=[], help="render only ids listed here (repeatable)"
    )
    rn.add_argument(
        "--rounds",
        type=int,
        default=DATASMITH_LSV_ROUNDS,
        help=f"LSV rounds (default $DATASMITH_LSV_ROUNDS or {DATASMITH_LSV_ROUNDS}); must equal the image's baked baseline rounds",
    )
    rn.add_argument("--cpus", type=int, default=2, help="trial cpu quota; also pins NUMBA_NUM_THREADS (default 2)")
    rn.add_argument("--memory", default="16G")
    rn.add_argument("--storage", default="20G")
    rn.add_argument("--timeout", type=float, default=21600.0)
    rn.add_argument(
        "--verifier-env",
        action="append",
        default=[],
        metavar="KEY=VAL",
        help="[verifier.env] entries for task.toml (repeatable)",
    )
    rn.add_argument("--no-pytest", action="store_true")
    rn.add_argument(
        "--force", action="store_true", help="render into a non-empty output root (overwrites per task dir)"
    )
    rn.add_argument("--allow-missing", action="store_true")
    rn.set_defaults(fn=cmd_render)

    ck = sub.add_parser("check", help="drift check: rendered dirs vs installed render source")
    ck.add_argument("task_dirs", nargs="+", type=Path)
    ck.set_defaults(fn=cmd_check)

    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
