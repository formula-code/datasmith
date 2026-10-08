#!/usr/bin/env python3
"""Drive stage 6 toward the goal's 100 verified containers, diversity first.

Ordering is deliberate. Phase A re-runs PRs that ALREADY have a
`candidate_containers` row: those built at least once under an older
generation, their Docker layers are cached, and the goal explicitly sanctions
re-running an earned pass rather than backfilling the flag. Phase B falls
through to fresh perf PRs when a repo's existing rows are exhausted.

Repos are visited round-robin, not depth-first, because the goal caps a repo at
10 counted rows -- grinding one repo to 40 buys nothing after the tenth.

    python scripts/grind.py --dry-run          # print the plan, run nothing
    python scripts/grind.py --batch 12         # grind until the cap is met
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

DATASMITH_GRIND_REPO_CAP: int = int(os.environ.get("DATASMITH_GRIND_REPO_CAP", "10"))
DATASMITH_GRIND_TARGET: int = int(os.environ.get("DATASMITH_GRIND_TARGET", "100"))
DATASMITH_GRIND_BATCH: int = int(os.environ.get("DATASMITH_GRIND_BATCH", "12"))
DATASMITH_GRIND_CONCURRENCY: int = int(os.environ.get("DATASMITH_GRIND_CONCURRENCY", "4"))
DATASMITH_GRIND_MAX_ROUNDS: int = int(os.environ.get("DATASMITH_GRIND_MAX_ROUNDS", "8"))
DATASMITH_GRIND_MAX_ATTEMPTS: int = int(os.environ.get("DATASMITH_GRIND_MAX_ATTEMPTS", "2"))
# How many tasks from any one repo a single batch may run. Passed to
# `--tasks-per-repo` as well as used when planning, so the pipeline enforces
# the same bound the planner intends -- and enforces it by RANDOM sample,
# which the planner does not do. `candidates()` returns rows in database
# order, so without the flag a repo's retries keep landing on the same few
# PRs; the goal counts at most 10 rows per repo, and ten near-identical
# containers from one corner of a repo is not the diversity it is after.
DATASMITH_GRIND_TASKS_PER_REPO: int = int(os.environ.get("DATASMITH_GRIND_TASKS_PER_REPO", "3"))

# Ordered by expected yield: repos with a labelled accept or a recent success
# first, so the early rounds prove the harness before the hard repos consume it.
REPOS: tuple[tuple[str, str], ...] = (
    ("networkx", "networkx"),
    ("pydata", "bottleneck"),
    ("xdslproject", "xdsl"),
    ("xarray-contrib", "flox"),
    ("UXARRAY", "uxarray"),
    ("shapely", "shapely"),
    ("pybop-team", "PyBOP"),
    ("TileDB-Inc", "TileDB-Py"),
    ("pybamm-team", "PyBaMM"),
    ("optuna", "optuna"),
    ("pydata", "xarray"),
    ("scikit-image", "scikit-image"),
    ("modin-project", "modin"),
    ("pymc-devs", "pymc3"),
    ("Qiskit", "qiskit"),
    # Outside the proven-measurable 15, but each already holds a container
    # this pipeline built -- cheap diversity toward the 10-repo floor.
    ("numpy", "numpy-financial"),
    ("bluesky", "tiled"),
    # Reserve depth. 134 repos already hold a container row and 50 hold ten or
    # more, so the 10-repo floor is not supply-limited -- it is limited by how
    # many repos actually re-earn a pass. These come last: the round-robin
    # sorts by verified count and `sorted` is stable, so they are reached only
    # once the repos above have been served.
    #
    # apache/arrow is deliberately absent: the host scan found a /usr/local/bin
    # grep wrapper in its image, so its stored contexts are suspect. pandas is
    # absent for cost -- 232 rows of the slowest builds in the corpus.
    ("dask", "dask"),
    ("astropy", "astropy"),
    ("scikit-learn", "scikit-learn"),
    ("pymc-devs", "pymc"),
    ("scipy", "scipy"),
    ("geopandas", "geopandas"),
    ("napari", "napari"),
    ("dipy", "dipy"),
    ("dask", "distributed"),
    ("h5py", "h5py"),
    ("holoviz", "datashader"),
    ("microsoft", "Qcodes"),
    ("datalad", "datalad"),
    ("WagnerGroup", "pyqmc"),
)


_LEDGER_NAME = "grind_attempts.json"


def _ledger_path() -> Path:
    return _ROOT / "logs" / _LEDGER_NAME


def load_ledger() -> dict[str, int]:
    """How many times each task has been attempted, across driver restarts.

    Without this the planner re-picks the same failures every batch: a task
    that cannot build is not removed by anything, so a 200-batch run would
    spend itself on the same handful of repos. Two attempts, then the task is
    retired -- a third pass with the same agent on the same error has not once
    converged in this session's traces.
    """
    path = _ledger_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def record_attempts(tasks: list[str]) -> None:
    ledger = load_ledger()
    for task in tasks:
        ledger[task] = ledger.get(task, 0) + 1
    path = _ledger_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledger, indent=1, sort_keys=True))


def protected_tasks() -> set[str]:
    """Tasks the grind must never rebuild.

    `pv_validate.py` pins its 16 cases BY DIGEST and skips a case whose tag has
    moved, which turns the pass criterion into FAIL. A grind `--force` rebuild
    moves exactly that tag: it happened once already when networkx:8148 was
    rebuilt. Condition 1 would gain a row while condition 2 quietly broke, and
    the damage would surface only in the definitive run at stop time.

    Derived from the module rather than hardcoded, so a case added to the
    labelled set is protected without editing this file.
    """
    try:
        sys.path.insert(0, str(_ROOT / "scripts"))
        import pv_validate

        return {case.task for case in pv_validate.CASES}
    except Exception as exc:
        # Fail CLOSED on the repos the labelled set is drawn from would be too
        # blunt; failing loud is right -- the caller decides.
        print(f"[grind] WARNING: could not read pv_validate cases ({exc}); pinned images are UNPROTECTED")
        return set()


def reap_leaked_containers(max_age_min: int = 90) -> int:
    """Force-remove task containers that outlived any legitimate timeout.

    `battery.py` now kills its own container when the client times out, but it
    is not the only thing that runs one -- `local_ci.py` starts containers too,
    and a killed driver leaves whatever it had running. A leaked container is
    not idle: on 2026-08-25 thirteen of them, up to 274 minutes old, held the
    host at load 372 on 128 cores doing work nothing was waiting for, and the
    grind's throughput collapsed until they were reaped.

    90 minutes is well past the 40-minute battery timeout and past the longest
    honest measurement seen (networkx, 140 benchmarks, 28 minutes).
    """
    try:
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.ID}} {{.Names}} {{.RunningFor}}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        ).stdout
    except Exception:
        return 0
    reaped = 0
    for line in out.splitlines():
        parts = line.split(None, 2)
        if len(parts) < 3 or not parts[1].startswith("fc-"):
            continue
        age = parts[2]
        hours = "hour" in age or "day" in age
        minutes = 0
        if "minute" in age:
            head = age.split()[0]
            minutes = int(head) if head.isdigit() else 0
        if not hours and minutes <= max_age_min:
            continue
        if subprocess.run(["docker", "rm", "-f", parts[0]], capture_output=True, check=False).returncode == 0:
            print(f"[grind] reaped leaked container {parts[1]} ({age})")
            reaped += 1
    return reaped


def _client():
    from datasmith.utils.db import get_client

    return get_client()


def _ensure_db() -> None:
    """The Supabase stack vanished once mid-session. Bring it back, don't stall."""
    import httpx

    url = os.environ.get("SUPABASE_URL", "http://127.0.0.1:54321")
    for attempt in range(3):
        try:
            httpx.get(f"{url}/rest/v1/", timeout=10)
            return
        except Exception as exc:  # any transport failure means "down"
            print(f"[grind] database unreachable ({exc}); starting supabase (try {attempt + 1}/3)")
            subprocess.run(["npx", "supabase", "start"], cwd=_ROOT, check=False)
            time.sleep(20)
    print("[grind] database still unreachable; continuing anyway")


def verified_counts() -> dict[tuple[str, str], int]:
    client = _client()
    rows = (
        client.table("candidate_containers").select("owner,repo").eq("verification_state", "verified").execute()
    ).data
    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        key = (row["owner"], row["repo"])
        counts[key] = counts.get(key, 0) + 1
    return counts


def candidates(owner: str, repo: str, want: int, ledger: dict[str, int]) -> list[int]:
    """Phase A rows first, then phase B, minus verified and retired tasks."""
    client = _client()
    built = (
        client.table("candidate_containers")
        .select("task_id,verification_state")
        .eq("owner", owner)
        .eq("repo", repo)
        .execute()
    ).data
    done = {r["task_id"] for r in built if r["verification_state"] == "verified"}
    phase_a = [r["task_id"] for r in built if r["task_id"] not in done]

    perf = (
        client.table("pull_requests")
        .select("issue_number")
        .eq("owner", owner)
        .eq("repo", repo)
        .eq("is_performance_commit", True)
        .execute()
    ).data
    seen = done | set(phase_a)
    phase_b = [r["issue_number"] for r in perf if r["issue_number"] not in seen]

    protected = protected_tasks()

    def skip(issue: int) -> bool:
        task = f"{owner}/{repo}#{issue}"
        return ledger.get(task, 0) >= DATASMITH_GRIND_MAX_ATTEMPTS or task in protected

    return [i for i in phase_a + phase_b if not skip(i)][:want]


def run_batch(tasks: list[str], log: Path) -> int:
    env = {
        **os.environ,
        "DATASMITH_PV_ENABLED": "1",
        "DATASMITH_PV_MAX_ROUNDS": str(DATASMITH_GRIND_MAX_ROUNDS),
        "DATASMITH_DISABLE_DOCKER_PRUNE": "1",
        "DATASMITH_SKIP_IMAGE_PUSH": "1",
        # TRY_SIMILAR saves a context without running the verifier, so a hit
        # there produces an `unverified` row and burns the task. Force the
        # producer/verifier path, which is the only one that can earn a pass.
        "DATASMITH_SKIP_SIMILAR_CONTEXTS": "1",
        "DATASMITH_PV_BATTERY_TIMEOUT_S": "2400",
        # Neighbour enqueueing predates this driver and now fights it: the
        # neighbours bypass the attempt ledger, over-produce in hot repos
        # past the goal's cap of 10, and -- because `--force` applies
        # run-wide -- can re-run an ALREADY-VERIFIED task, overwriting the
        # stored scripts and manifest of a row the goal counts. The cap is
        # checked as `added >= CAP` before each enqueue, so 0 disables it.
        "DATASMITH_NEIGHBOR_CAP": "0",
        "PYTHONUNBUFFERED": "1",
    }
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
        "codex",
        "--force",
        "--n-concurrent",
        str(DATASMITH_GRIND_CONCURRENCY),
        "--tasks-per-repo",
        str(DATASMITH_GRIND_TASKS_PER_REPO),
        "--tasks",
        ",".join(tasks),
    ]
    with log.open("a") as fh:
        fh.write(f"\n===== batch {time.strftime('%H:%M:%S')}: {len(tasks)} task(s) =====\n")
        fh.flush()
        return subprocess.run(cmd, cwd=_ROOT, env=env, stdout=fh, stderr=subprocess.STDOUT, check=False).returncode


def plan_batch(
    rotation: tuple[tuple[str, str], ...],
    counts: dict[tuple[str, str], int],
    ledger: dict[str, int],
    batch: int,
) -> list[str]:
    """Spend most of a batch on repos that have proven they yield, some on new ones.

    The goal wants 100 rows across at least 10 repos, capped at 10 per repo.
    Those two numbers pull in opposite directions and the right balance moves
    as the corpus fills.

    Ordering repos by verified-count ASCENDING -- always serving the emptiest
    first -- is correct while the repo floor is unmet, and it got the corpus to
    8 distinct repos quickly. It then became actively wrong: measured over the
    night of 2026-08-25/26, 8 repos produced every verified container and
    roughly 20 others produced nothing at all, while a batch drawn from the
    empty ones returned 0 accepts and 12 failures in two and a half hours.
    Each of those failures can burn an hour, because a verification timeout is
    3600 s and the same timeout signature repeats until the stall detector
    fires.

    Eight proven repos at the cap would be 80 rows. Twenty unproven ones have
    yet to produce a single one. So spend the batch mostly on repos with a
    verified row and room left, and reserve a slice for unproven repos --
    which is what still finds the 9th and 10th repo, and what stops this from
    collapsing into a monoculture the cap exists to prevent.
    """
    proven = [r for r in rotation if 0 < counts.get(r, 0) < DATASMITH_GRIND_REPO_CAP]
    unproven = [r for r in rotation if counts.get(r, 0) == 0]

    # Reserve at least one repo's worth of the batch for exploration, and all
    # of it while no repo has proven anything yet.
    explore = max(DATASMITH_GRIND_TASKS_PER_REPO, batch // 4) if proven else batch

    plan: list[str] = []
    for group, budget in ((unproven, explore), (proven, batch - explore)):
        taken = 0
        for owner, repo in sorted(group, key=lambda kv: counts.get(kv, 0), reverse=group is proven):
            if taken >= budget:
                break
            need = DATASMITH_GRIND_REPO_CAP - counts.get((owner, repo), 0)
            if need <= 0:
                continue
            take = max(1, min(need, DATASMITH_GRIND_TASKS_PER_REPO, budget - taken))
            for issue in candidates(owner, repo, take, ledger):
                plan.append(f"{owner}/{repo}#{issue}")
                taken += 1
    return plan[:batch]


def _rotation(spec: str) -> tuple[tuple[str, str], ...]:
    """The repos this driver owns. Empty spec means all of them.

    Partitioning exists so two drivers can run at once: they share the cap
    arithmetic (both read `verified_counts()`) but never plan the same task,
    because a task belongs to exactly one repo.
    """
    if not spec:
        return REPOS
    wanted = {tuple(r.split("/", 1)) for r in spec.split(",") if "/" in r}
    unknown = wanted - set(REPOS)
    if unknown:
        print(f"[grind] WARNING: not in REPOS, ignored: {sorted(unknown)}")
    return tuple(r for r in REPOS if r in wanted)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--batch", type=int, default=DATASMITH_GRIND_BATCH)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--log", default="")
    ap.add_argument("--max-batches", type=int, default=200)
    ap.add_argument(
        "--repos",
        default="",
        help="comma-separated owner/repo to restrict this driver to; lets two drivers "
        "work disjoint partitions concurrently without ever planning the same task",
    )
    ap.add_argument("--ledger", default="", help="attempt-ledger filename (two drivers must not share one)")
    args = ap.parse_args()

    global _LEDGER_NAME
    if args.ledger:
        _LEDGER_NAME = args.ledger

    rotation = _rotation(args.repos)
    if not rotation:
        print("[grind] no known repos selected")
        return 1

    log = Path(args.log) if args.log else _ROOT / "logs" / "grind.log"
    log.parent.mkdir(parents=True, exist_ok=True)

    for batch_no in range(args.max_batches):
        _ensure_db()
        reap_leaked_containers()
        counts = verified_counts()
        ledger = load_ledger()
        capped = sum(min(n, DATASMITH_GRIND_REPO_CAP) for n in counts.values())
        print(f"[grind] batch {batch_no}: capped {capped}/{DATASMITH_GRIND_TARGET}, {len(counts)} repo(s)")
        if capped >= DATASMITH_GRIND_TARGET:
            print("[grind] target met")
            return 0

        plan = plan_batch(rotation, counts, ledger, args.batch)
        if not plan:
            print("[grind] no candidates left")
            return 1
        print(f"[grind] plan: {' '.join(plan)}")
        if args.dry_run:
            return 0
        record_attempts(plan)
        rc = run_batch(plan, log)
        print(f"[grind] batch {batch_no} rc={rc}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
