#!/usr/bin/env python3
"""Judge whether Harbor runs properly against a container.

`scripts/container_honesty.py` answers "is this image sound and untampered".
This answers the other half of the bar: does the trial harness that scores the
benchmark actually work on it.

They are different questions, and a container can pass one and fail the other.
`pysal/momepy#237` built cleanly, collected tests, and passed them, and then
measured zero benchmarks. `dynamicslab/pysindy#139` measured two and reported a
speedup of exactly 1.0 that its own tampered library had fabricated.

Stage 7 already runs a Harbor trial and writes one `harbor_runs` row. This
script runs it for one task and judges the row, rather than reimplementing the
trial.

Checks are THREE-VALUED. A check whose input is absent is SKIPPED, never
passed.

Usage:

    python scripts/harbor_honesty.py --task networkx/networkx#8148
    python scripts/harbor_honesty.py --task <spec> --no-run    # judge the last row

Exit code is 0 when no FATAL check failed, and 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
_SPEC = re.compile(r"^(?P<owner>[^/]+)/(?P<repo>[^#/]+)[#/](?P<number>\d+)$")


def parse_spec(spec: str) -> tuple[str, str, int]:
    match = _SPEC.match(spec.strip())
    if not match:
        raise SystemExit(f"unparseable task spec: {spec!r} (expected owner/repo#123)")
    return match["owner"], match["repo"], int(match["number"])


def run_trial(spec: str, timeout: int) -> int:
    """Run stage 7 for one task, locally, under Docker."""
    env = dict(os.environ)
    env.setdefault("SUPABASE_URL", "http://127.0.0.1:54321")
    env["DATASMITH_DISABLE_DOCKER_PRUNE"] = "1"
    cmd = [
        "uv",
        "run",
        "fc-data",
        "--start-date",
        "2017-01-01",
        "--end-date",
        "2030-12-31",
        "--stage",
        "7",
        "--harbor-environment",
        "docker",
        "--n-concurrent",
        "1",
        "--tasks",
        spec,
    ]
    print(f"[harbor] {' '.join(cmd)}", flush=True)
    try:
        proc = subprocess.run(cmd, cwd=_ROOT, env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"[harbor] trial exceeded {timeout}s", file=sys.stderr)
        return 124
    return proc.returncode


def latest_row(owner: str, repo: str, number: int) -> dict[str, Any] | None:
    sys.path.insert(0, str(_ROOT / "src"))
    from datasmith.utils.db import fetch_all

    rows = fetch_all(
        "harbor_runs",
        select=(
            "owner,repo,issue_number,status,environment,n_benchmarks,max_speedup,"
            "geomean_speedup,wallclock_sec,pytest_success_ratio,error_message,"
            "reward_payload,ran_at"
        ),
        filters={"owner": owner, "repo": repo, "issue_number": number},
    )
    if not rows:
        return None
    return sorted(rows, key=lambda r: r.get("ran_at") or "")[-1]


# ------------------------------------------------------------------ policy


def _payload(row: dict) -> dict:
    value = row.get("reward_payload")
    return value if isinstance(value, dict) else {}


def _c_status_success(row: dict) -> bool | None:
    status = row.get("status")
    return None if status is None else status == "success"


def _c_setup_succeeded(row: dict) -> bool | None:
    """setup.sh is where lsv_init runs. A failed setup means no baseline exists."""
    setup = _payload(row).get("setup")
    if not isinstance(setup, dict) or "succeeded" not in setup:
        return None
    return bool(setup["succeeded"])


def _c_no_lsv_error(row: dict) -> bool | None:
    payload = _payload(row)
    if "lsv_error" not in payload:
        return None
    return payload["lsv_error"] in (None, "")


def _c_benchmarks_measured(row: dict) -> bool | None:
    """momepy#237 built, tested and measured ZERO. This is the check it fails."""
    n = _payload(row).get("num_valid_benchmarks")
    if n is None:
        n = row.get("n_benchmarks")
    return None if n is None else int(n) > 0


def _c_tests_passed(row: dict) -> bool | None:
    value = _payload(row).get("tests_passed")
    return None if value is None else bool(value)


def _c_pytest_ran(row: dict) -> bool | None:
    pytest_block = _payload(row).get("pytest")
    if not isinstance(pytest_block, dict) or "total" not in pytest_block:
        return None
    return int(pytest_block["total"]) > 0


def _c_speedups_not_degenerate(row: dict) -> bool | None:
    """Every measured benchmark being exactly 1.0 is the fabrication signature.

    pysindy#139 reported max and geomean of exactly 1.0 across two benchmarks,
    because its replaced library set baseline = current when no baseline
    existed. Real timings do not land on 1.0 exactly.
    """
    speedups = _payload(row).get("per_benchmark_speedups")
    if not isinstance(speedups, dict) or not speedups:
        return None
    values = [v for v in speedups.values() if isinstance(v, int | float)]
    if not values:
        return None
    return not all(v == 1.0 for v in values)


CHECKS: list[tuple[str, str, Any]] = [
    ("harbor_status_success", "fatal", _c_status_success),
    ("setup_succeeded", "fatal", _c_setup_succeeded),
    ("no_lsv_error", "fatal", _c_no_lsv_error),
    ("benchmarks_measured", "fatal", _c_benchmarks_measured),
    ("pytest_ran", "fatal", _c_pytest_ran),
    ("tests_passed", "warn", _c_tests_passed),
    ("speedups_not_degenerate", "fatal", _c_speedups_not_degenerate),
]


def evaluate(row: dict) -> dict:
    passed, failed, warned, skipped = [], [], [], []
    for name, severity, fn in CHECKS:
        try:
            verdict = fn(row)
        # A check must never crash the gate. A raising check is a FAILED check.
        except Exception as exc:
            failed.append(f"{name} (raised {type(exc).__name__}: {exc})")
            continue
        if verdict is None:
            skipped.append(name)
        elif verdict:
            passed.append(name)
        elif severity == "fatal":
            failed.append(name)
        else:
            warned.append(name)
    return {"harbor_ok": not failed, "passed": passed, "failed": failed, "warned": warned, "skipped": skipped}


def summarise(spec: str, row: dict, verdict: dict) -> str:
    payload = _payload(row)
    return "\n".join([
        f"task             {spec}",
        f"verdict          {'HARBOR OK' if verdict['harbor_ok'] else 'HARBOR NOT OK'}",
        "",
        f"  status         {row.get('status')}   environment={row.get('environment')}",
        f"  wallclock      {row.get('wallclock_sec')}s",
        f"  setup          {payload.get('setup')}",
        f"  lsv_error      {payload.get('lsv_error')}",
        f"  benchmarks     {payload.get('num_valid_benchmarks', row.get('n_benchmarks'))}",
        f"  pytest         {payload.get('pytest')}",
        f"  max/geomean    {row.get('max_speedup')} / {row.get('geomean_speedup')}",
        f"  error          {row.get('error_message')}",
        "",
        f"  passed   {', '.join(verdict['passed']) or '-'}",
        f"  FAILED   {', '.join(verdict['failed']) or '-'}",
        f"  warned   {', '.join(verdict['warned']) or '-'}",
        f"  skipped  {', '.join(verdict['skipped']) or '-'}",
    ])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--task", required=True, help="owner/repo#123")
    ap.add_argument("--no-run", action="store_true", help="judge the last row instead of running a trial")
    ap.add_argument("--timeout", type=int, default=7200)
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    owner, repo, number = parse_spec(args.task)

    before = latest_row(owner, repo, number)
    if not args.no_run:
        rc = run_trial(args.task, args.timeout)
        print(f"[harbor] stage 7 exited {rc}", flush=True)

    row = latest_row(owner, repo, number)
    if row is None:
        print(f"task             {args.task}")
        print("verdict          HARBOR NOT OK (no harbor_runs row was produced)")
        return 1
    if not args.no_run and before is not None and row.get("ran_at") == before.get("ran_at"):
        print(f"task             {args.task}")
        print("verdict          HARBOR NOT OK (the trial wrote no new row)")
        return 1

    verdict = evaluate(row)
    print(summarise(args.task, row, verdict))
    if args.json:
        Path(args.json).write_text(
            json.dumps({"task": args.task, "row": row, "verdict": verdict}, indent=2, default=str)
        )
        print(f"\nwrote {args.json}")
    return 0 if verdict["harbor_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
