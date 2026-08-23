#!/usr/bin/env python3
"""Measure how many repositories build with no agent, and how many are honest.

Runs stage 6 with `--agent none` and `DATASMITH_SKIP_SIMILAR_CONTEXTS=1`, so
only the stock template can succeed. Then runs the honesty gate on every
container that built.

Two numbers come out, and they answer different questions:

  build rate    how often the stock template alone produces a container
  honest rate   how often that container passes the honesty gate

The second is not a security claim. `scripts/container_honesty.py` says why in
its own docstring: a builder that wants to pass it can. With `--agent none`
nothing had motive or opportunity, so the gate is measuring breakage, not
resisting an adversary.

Selection is seeded and printed, so a run is repeatable and the sample can be
audited. ONE task per repository, because the number under measurement is a
per-repository property.

    python scripts/prepass_trial.py --sample 6 --seed 20260823
    python scripts/prepass_trial.py --tasks pydata/bottleneck#468,apache/arrow#1646

Environment, and why each is needed:

    SUPABASE_URL=http://127.0.0.1:54321   local only, never the tunnel
    DATASMITH_DISABLE_DOCKER_PRUNE=1      keep BuildKit cache during the run
    TMPDIR=/mnt/sdd2/tmp-prepass          / is 98% full; Docker lives on sdd2

Results are appended to the output file AS EACH TASK FINISHES, not at the end,
so a run that dies half way still leaves what it learned.
"""

from __future__ import annotations

import argparse
import os
import random
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sample", type=int, default=6, help="repositories to try (ignored with --tasks)")
    p.add_argument("--seed", type=int, default=20260823)
    p.add_argument("--tasks", default="", help="explicit owner/repo#N specs, comma separated")
    p.add_argument("--build-timeout", type=int, default=7200)
    p.add_argument("--honesty-timeout", type=int, default=2400)
    p.add_argument("--cpuset", default=None, help="cpuset for the honesty probe, e.g. 32-47")
    p.add_argument("--out", default="docs/superpowers/plans/2026-08-23-prepass-trial-results.md")
    return p.parse_args()


def _env() -> dict[str, str]:
    env = dict(os.environ)
    env["SUPABASE_URL"] = "http://127.0.0.1:54321"
    env["DATASMITH_DISABLE_DOCKER_PRUNE"] = "1"
    env["DATASMITH_SKIP_SIMILAR_CONTEXTS"] = "1"
    env.setdefault("TMPDIR", "/mnt/sdd2/tmp-prepass")
    return env


def choose_tasks(sample: int, seed: int) -> list[tuple[str, str, int]]:
    """One buildable task per repository, drawn reproducibly.

    A task is buildable when its merge commit has a `can_install` packages row.
    Without that, stage 6 drops it before any build starts, and the repository
    would silently leave the denominator.
    """
    sys.path.insert(0, str(_ROOT / "src"))
    from datasmith.utils.db import fetch_all

    installable = {
        (r["owner"], r["repo"], r["sha"])
        for r in fetch_all("packages", select="owner,repo,sha", filters={"can_install": True})
    }
    prs = fetch_all(
        "pull_requests",
        select="owner,repo,issue_number,merge_commit_sha,created_at",
        filters={"is_performance_commit": True, "is_performance_commit_symbolic": True},
    )

    by_repo: dict[tuple[str, str], list[tuple[str, str, int]]] = {}
    for r in prs:
        key = (r["owner"], r["repo"])
        if (r["owner"], r["repo"], r["merge_commit_sha"]) in installable:
            by_repo.setdefault(key, []).append((r["owner"], r["repo"], int(r["issue_number"])))

    repos = sorted(by_repo)
    # S311: a seeded generator is the point. The sample must be reproducible
    # so a run can be audited and repeated. This is not a security context.
    picked = random.Random(seed).sample(repos, min(sample, len(repos)))  # noqa: S311
    # Lowest issue number per repo, so the choice does not drift between runs.
    return [sorted(by_repo[repo])[0] for repo in sorted(picked)]


def run_build(spec: str, timeout: int) -> tuple[bool, float]:
    cmd = [
        "uv",
        "run",
        "fc-data",
        "--start-date",
        "2017-01-01",
        "--end-date",
        "2030-12-31",
        "--stage",
        "6",
        "--agent",
        "none",
        "--force",
        "--n-concurrent",
        "1",
        "--tasks",
        spec,
    ]
    started = time.time()
    try:
        subprocess.run(cmd, cwd=_ROOT, env=_env(), timeout=timeout, capture_output=True, text=True)
    except subprocess.TimeoutExpired:
        return False, time.time() - started
    return True, time.time() - started


def build_outcome(owner: str, repo: str, number: int) -> dict | None:
    """The TRY_DEFAULT row for this task, or None if the stage never reached it."""
    sys.path.insert(0, str(_ROOT / "src"))
    from datasmith.utils.db import fetch_all

    rows = [
        r
        for r in fetch_all(
            "error_logs",
            select="owner,repo,issue_number,success,duration_s,error_message,created_at",
            filters={"agent_name": "default_template"},
        )
        if (r["owner"], r["repo"], r["issue_number"]) == (owner, repo, number)
    ]
    return sorted(rows, key=lambda r: r["created_at"])[-1] if rows else None


def run_honesty(image: str, cpuset: str | None, timeout: int) -> tuple[bool, str]:
    cmd = [sys.executable, str(_ROOT / "scripts" / "container_honesty.py"), "--image", image]
    if cpuset:
        cmd += ["--cpuset", cpuset]
    try:
        proc = subprocess.run(
            ["uv", "run", *cmd[1:]] if cmd[0] == sys.executable else cmd,
            cwd=_ROOT,
            env=_env(),
            timeout=timeout,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        return False, f"honesty gate exceeded {timeout}s"
    return proc.returncode == 0, proc.stdout


def image_tag(owner: str, repo: str, number: int) -> str:
    sys.path.insert(0, str(_ROOT / "src"))
    from datasmith.docker.images import get_pr_image_name

    return get_pr_image_name(owner, repo, number)


def append(out: Path, text: str) -> None:
    """Write as we go. A run that dies half way still leaves what it learned."""
    with out.open("a", encoding="utf-8") as fh:
        fh.write(text + "\n")
        fh.flush()


def main() -> int:
    args = parse_args()
    if os.environ.get("SUPABASE_URL", "http://127.0.0.1:54321") not in ("", "http://127.0.0.1:54321"):
        print("refusing to run against a non-local SUPABASE_URL", file=sys.stderr)
        return 2

    if args.tasks:
        tasks = []
        for raw in args.tasks.split(","):
            raw = raw.strip()
            if not raw:
                continue
            owner_repo, _, number = raw.replace("#", "/").rpartition("/")
            owner, _, repo = owner_repo.partition("/")
            tasks.append((owner, repo, int(number)))
    else:
        tasks = choose_tasks(args.sample, args.seed)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    append(out, f"\n# Pre-pass trial, seed {args.seed}, {len(tasks)} repositories\n")
    append(out, "Stage 6 with `--agent none` and TRY_SIMILAR disabled, so only the stock")
    append(out, "template can succeed. The honesty gate is not a security check.\n")
    append(out, "| repository | issue | built | build s | honest | note |")
    append(out, "|---|---|---|---|---|---|")

    for owner, repo, number in tasks:
        spec = f"{owner}/{repo}#{number}"
        print(f"[trial] {spec}", flush=True)
        _, wall = run_build(spec, args.build_timeout)
        row = build_outcome(owner, repo, number)

        if row is None:
            append(out, f"| {owner}/{repo} | {number} | no row | {wall:.0f} | - | stage never reached TRY_DEFAULT |")
            continue
        if not row["success"]:
            first = (row.get("error_message") or "").split("\n")[0][:70]
            append(out, f"| {owner}/{repo} | {number} | no | {row['duration_s']:.0f} | - | {first} |")
            continue

        honest, report = run_honesty(image_tag(owner, repo, number), args.cpuset, args.honesty_timeout)
        failed = ""
        for line in report.splitlines():
            if line.strip().startswith("FAILED"):
                failed = line.split("FAILED", 1)[1].strip()
        append(
            out,
            f"| {owner}/{repo} | {number} | yes | {row['duration_s']:.0f} | "
            f"{'yes' if honest else 'no'} | {failed or 'all checks pass'} |",
        )
        (out.parent / f"honesty-{owner}-{repo}-{number}.txt").write_text(report)

    append(out, "\nRun finished.\n")
    print(f"[trial] wrote {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
