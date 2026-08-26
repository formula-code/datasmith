#!/usr/bin/env python3
"""Report PASS/FAIL for each of the four stop conditions of the 100-container goal.

The goal's evidence clause names this script, so its output is what a stop is
argued from. Two rules shape the implementation:

* **Narrow selects only.** ``fetch_all("candidate_containers", select="build_manifest")``
  streams a multi-kilobyte JSONB blob per row and has already killed PostgREST
  once this session. Manifest presence is therefore asked as a *filter*
  (``not.is null``) and counted server-side; the column is never transferred.
* **Cheap by default, honest about it.** Conditions 2 and 3 cost minutes
  (two ~100 s image scans, a 16-case verifier run, the test suite). Without
  ``--full`` they report NOT RUN rather than a stale cached verdict. A stop is
  only evidenced by a ``--full`` run.

    python scripts/goal_check.py            # conditions 1 and 3a, seconds
    python scripts/goal_check.py --full     # everything, minutes
"""

from __future__ import annotations

import argparse
import collections
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]

# The goal's own numbers, as tunables so a re-scoped goal does not need a code
# edit (CLAUDE.md's convention: read at module scope, literal default).
DATASMITH_GOAL_TARGET: int = int(os.environ.get("DATASMITH_GOAL_TARGET", "100"))
DATASMITH_GOAL_REPO_CAP: int = int(os.environ.get("DATASMITH_GOAL_REPO_CAP", "10"))
DATASMITH_GOAL_MIN_REPOS: int = int(os.environ.get("DATASMITH_GOAL_MIN_REPOS", "10"))
# Reported, never gated: what the pipeline itself would call publishable
# (`MIN_HARBOR_SPEEDUP`), and enough benchmarks that the number rests on
# more than a single measurement.
DATASMITH_GOAL_MIN_SPEEDUP: float = float(os.environ.get("DATASMITH_GOAL_MIN_SPEEDUP", "1.05"))
DATASMITH_GOAL_MIN_BENCHMARKS: int = int(os.environ.get("DATASMITH_GOAL_MIN_BENCHMARKS", "5"))

_CONTROL_IMAGES: tuple[str, ...] = (
    "formulacode/attack-demo:1",
    "formulacode/dynamicslab-pysindy:139",
)


@dataclass
class Condition:
    key: str
    title: str
    state: str  # "PASS" | "FAIL" | "NOT RUN"
    lines: list[str]


def _hdr(c: Condition) -> str:
    return f"[{c.state:^7}] {c.key}. {c.title}"


# --------------------------------------------------------------- condition 1


def condition_corpus() -> Condition:
    from datasmith.utils.db import get_client

    lines: list[str] = []
    client = get_client()

    # Only small columns cross the wire. `build_manifest` is asked about with a
    # filter so PostgREST never serialises it.
    verified = (
        client.table("candidate_containers")
        .select("owner,repo,task_id,verified_at,build_manifest->verify")
        .eq("verification_state", "verified")
        .execute()
    ).data

    sealed = (
        client.table("candidate_containers")
        .select("task_id", count="exact")
        .eq("verification_state", "verified")
        .not_.is_("build_manifest", "null")
        .execute()
    ).count or 0

    total = (client.table("candidate_containers").select("task_id", count="exact").execute()).count or 0

    # A row that claims `verified` without the facts the accept path always
    # writes did not come from the accept path. The goal voids it.
    no_stamp = [r for r in verified if not r.get("verified_at")]
    voided = len(no_stamp) + (len(verified) - sealed)

    by_repo: dict[tuple[str, str], int] = collections.Counter((r["owner"], r["repo"]) for r in verified)
    capped = sum(min(n, DATASMITH_GOAL_REPO_CAP) for n in by_repo.values())
    repos_at_cap = sum(1 for n in by_repo.values() if n >= DATASMITH_GOAL_REPO_CAP)

    lines.append(
        f"verified rows {len(verified)} of {total} total; "
        f"sealed manifest {sealed}; missing verified_at {len(no_stamp)}; voided {voided}"
    )
    lines.append(
        f"capped count {capped}/{DATASMITH_GOAL_TARGET} "
        f"(cap {DATASMITH_GOAL_REPO_CAP}/repo, {repos_at_cap} repo(s) at cap); "
        f"distinct repos {len(by_repo)}/{DATASMITH_GOAL_MIN_REPOS}"
    )
    if by_repo:
        lines.append("per-repo breakdown:")
        for (owner, repo), n in sorted(by_repo.items(), key=lambda kv: (-kv[1], kv[0])):
            counted = min(n, DATASMITH_GOAL_REPO_CAP)
            over = f"  ({n - counted} over cap, not counted)" if n > counted else ""
            lines.append(f"    {owner}/{repo:<28} {n:>3} verified, {counted:>3} counted{over}")
    else:
        lines.append("per-repo breakdown: (none)")

    # Quality alongside count.
    #
    # `verified` means the container is HONEST -- it built, scanned clean,
    # passed the battery, and measurement ran. It does not mean a speedup was
    # shown. Depth on small repos converts fast and yields thin tasks: three
    # xdsl rows verified on 2026-08-26 each measured a SINGLE benchmark, at
    # 0.95, 0.99 and 1.00. Reporting only the count hides that.
    #
    # None of this gates anything. Condition 1 counts verified rows because
    # that is what the goal says; changing the target is the operator's call.
    speedy = well_measured = both = 0
    for row in verified:
        v = row.get("verify") or {}
        fast = (v.get("max_speedup") or 0) >= DATASMITH_GOAL_MIN_SPEEDUP
        thick = (v.get("benchmarks_measured_n") or 0) >= DATASMITH_GOAL_MIN_BENCHMARKS
        speedy += fast
        well_measured += thick
        both += fast and thick
    lines.append(
        f"quality (not gating): {speedy} with max_speedup >= {DATASMITH_GOAL_MIN_SPEEDUP}, "
        f"{well_measured} measuring >= {DATASMITH_GOAL_MIN_BENCHMARKS} benchmarks, {both} both"
    )

    ok = capped >= DATASMITH_GOAL_TARGET and len(by_repo) >= DATASMITH_GOAL_MIN_REPOS and voided == 0
    if not ok:
        short = max(0, DATASMITH_GOAL_TARGET - capped)
        lines.append(
            f"remaining: {short} more counted row(s), {max(0, DATASMITH_GOAL_MIN_REPOS - len(by_repo))} more repo(s)"
        )
    return Condition("1", "CORPUS", "PASS" if ok else "FAIL", lines)


# --------------------------------------------------------------- condition 2


def _pins_intact() -> tuple[bool, list[str]]:
    """Are pv_validate's 16 digest pins still resolvable to the pinned image?

    A pin drifts two ways: a rebuild moves the tag (the grind does this on
    `--force`), or something outside this session prunes the image. Either way
    `pv_validate` SKIPS the case and reports its criterion as FAIL -- and that
    was previously only discoverable during the definitive run, which costs
    hours. Sixteen `docker inspect` calls cost about a second.
    """
    sys.path.insert(0, str(_ROOT / "scripts"))
    try:
        import pv_validate
    except Exception as exc:
        return False, [f"could not import pv_validate: {exc}"]

    drift: list[str] = []
    for case in pv_validate.CASES:
        proc = subprocess.run(
            ["docker", "inspect", "--format", "{{.Id}}", case.image],
            capture_output=True,
            text=True,
            check=False,
        )
        actual = proc.stdout.strip() if proc.returncode == 0 else ""
        if not actual:
            drift.append(f"{case.task}: image {case.image} is GONE")
        elif actual != case.digest:
            drift.append(f"{case.task}: tag moved — pinned {case.digest[:19]}, local {actual[:19]}")
    if drift:
        return False, [f"{len(drift)} of {len(pv_validate.CASES)} pins broken:", *[f"  {d}" for d in drift]]
    return True, [f"all {len(pv_validate.CASES)} digest pins intact"]


def condition_controls(full: bool) -> Condition:
    pins_ok, pin_lines = _pins_intact()
    if not full:
        state = "FAIL" if not pins_ok else "NOT RUN"
        return Condition(
            "2", "CONTROLS", state, [*pin_lines, "pass --full to scan the controls and run the labelled set"]
        )

    from datasmith.agents.reflexive.image_integrity import collect_and_evaluate

    lines: list[str] = list(pin_lines)
    ok = pins_ok
    for image in _CONTROL_IMAGES:
        integrity = collect_and_evaluate(image)
        if not integrity.collected:
            # An unscannable control is not a clean control.
            lines.append(f"{image}: SCAN FAILED ({integrity.error[:120]}) -> treated as FAIL")
            ok = False
            continue
        verdict = "TAMPERED" if integrity.findings else "clean"
        lines.append(
            f"{image}: {verdict} ({len(integrity.findings)} finding(s), "
            f"{integrity.members_scanned} members, {integrity.duration_s:.1f}s)"
        )
        for finding in integrity.findings:
            lines.append(f"    - {finding.detail[:150]}")
        if not integrity.findings:
            ok = False

    proc = subprocess.run(
        [sys.executable, str(_ROOT / "scripts" / "pv_validate.py")],
        capture_output=True,
        text=True,
        cwd=_ROOT,
        check=False,
    )
    tail = (proc.stdout or proc.stderr or "").strip().splitlines()
    lines.append(f"pv_validate.py rc={proc.returncode}")
    for line in tail[-6:]:
        lines.append(f"    {line}")
    if proc.returncode != 0 or "criterion PASS" not in (proc.stdout or ""):
        ok = False

    return Condition("2", "CONTROLS", "PASS" if ok else "FAIL", lines)


# --------------------------------------------------------------- condition 3


def _pv_flag_is_off() -> tuple[bool, str]:
    """`DATASMITH_PV_ENABLED` must not be enabled in tokens.env."""
    path = _ROOT / "tokens.env"
    if not path.exists():
        return True, "tokens.env absent"
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key.strip() == "DATASMITH_PV_ENABLED":
            value = value.strip().strip("'\"")
            on = value not in ("", "0", "false", "False", "no")
            return (not on), f"tokens.env sets DATASMITH_PV_ENABLED={value!r}"
    return True, "tokens.env does not set DATASMITH_PV_ENABLED (defaults off)"


def condition_invariants(full: bool) -> Condition:
    lines: list[str] = []
    flag_ok, detail = _pv_flag_is_off()
    lines.append(f"PV flag: {'ok' if flag_ok else 'ENABLED — must stay off'} — {detail}")
    ok = flag_ok

    if not full:
        lines.append("suite/ruff/mypy: NOT RUN (pass --full)")
        return Condition("3", "INVARIANTS", "NOT RUN" if flag_ok else "FAIL", lines)

    gates = (
        ("pytest", ["uv", "run", "pytest", "-q", "-m", "not slow"]),
        ("ruff", ["uv", "run", "ruff", "check", "--no-fix", "."]),
        ("mypy", ["uv", "run", "mypy"]),
    )
    for name, cmd in gates:
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=_ROOT, check=False)
        good = proc.returncode == 0
        ok = ok and good
        summary = (proc.stdout or proc.stderr or "").strip().splitlines()
        lines.append(f"{name}: {'green' if good else f'FAIL rc={proc.returncode}'}")
        for line in summary[-3:]:
            lines.append(f"    {line}")
    return Condition("3", "INVARIANTS", "PASS" if ok else "FAIL", lines)


# ------------------------------------------------------------------- driver


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--full", action="store_true", help="run the expensive conditions (2 and 3)")
    args = ap.parse_args()

    conditions = [
        condition_corpus(),
        condition_controls(args.full),
        condition_invariants(args.full),
        Condition("4", "EVIDENCE", "PASS", ["this report is the evidence"]),
    ]

    print("=" * 78)
    print(f"GOAL CHECK — 100 verified containers ({'full' if args.full else 'quick'} run)")
    print("=" * 78)
    for c in conditions:
        print()
        print(_hdr(c))
        for line in c.lines:
            print(f"        {line}")

    states = {c.state for c in conditions}
    overall = "PASS" if states == {"PASS"} else "FAIL" if "FAIL" in states else "INCOMPLETE"
    print()
    print("-" * 78)
    print(f"OVERALL: {overall}")
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
