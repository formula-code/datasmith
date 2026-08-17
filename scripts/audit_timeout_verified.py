"""Audit: flag candidate_containers that were "verified" by timing out.

Until the 2026-07-31 build-manifest work, local_ci.py scored a host-side
timeout as SUCCESS. A container killed at the 720s limit was indistinguishable
from one that passed, so every row that hit the limit was recorded as verified
on the strength of nothing.

Measured against this database (2026-08-13):
  - 636 rows at test_duration_s >= 720, across 54 distinct repos, of 1854 total
  - ZERO of those 636 have a harbor_runs row

That second number matters. The original plan said to rank the cohort by
whether a harbor_runs row already exists, on the logic that a suspect container
which already produced a good oracle trial is lower priority. It does not
discriminate: none of them has one. (The join is sound -- 7 containers do have
harbor_runs rows, and all 7 are fast rows under 720s.) So every suspect row is
equally unvalidated and the only useful ranking axis is by repo.

THIS SCRIPT NEVER DELETES A ROW.
Five public Grafana panels read candidate_containers and every one is a
COUNT(*) over rows: Total Problems, PR to Problem Rate, Problems by Repository,
Monthly distribution of Problems, and the End-to-End Pipeline Funnel. Adding a
column value is invisible to all five; deleting a row moves all five at once.
The operator constraint is that no public figure may move, so this stamps and
reports only. tests/test_grafana_invariance.py enforces that statically.

Usage:
  uv run python scripts/audit_timeout_verified.py                # dry run
  uv run python scripts/audit_timeout_verified.py --apply        # stamp
  uv run python scripts/audit_timeout_verified.py --calibrate 12 # re-run a sample

Always writes a backup of the affected rows to
backups/timeout_verified_<ts>.json before stamping.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

from datasmith.utils.db import fetch_all, get_client

load_dotenv("tokens.env")

# The boundary that defines the cohort. 720 was the historical default limit,
# and the pile-up inside a 5-second window at exactly that value is what
# identified the defect in the first place.
DATASMITH_VERIFY_SUSPECT_TIMEOUT_S: float = float(os.environ.get("DATASMITH_VERIFY_SUSPECT_TIMEOUT_S", "720"))

WARNING_ID = "suspect_timeout_720s"
BACKUP_DIR = Path("backups")


def _duration(row: dict) -> float | None:
    metrics = row.get("resource_metrics") or {}
    if not isinstance(metrics, dict):
        return None
    raw = metrics.get("test_duration_s")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def select_cohort(rows: list[dict]) -> list[dict]:
    """Rows whose recorded test duration reached the suspect boundary."""
    out = []
    for row in rows:
        duration = _duration(row)
        if duration is not None and duration >= DATASMITH_VERIFY_SUSPECT_TIMEOUT_S:
            out.append(row)
    return out


def merge_warning(existing: list[str] | None, warning: str = WARNING_ID) -> list[str]:
    """Append the warning without dropping what is already there.

    Array APPEND, never replace: a row that already carries manifest_warnings
    from its own build must keep them. Idempotent, so re-running the audit
    does not accumulate duplicates.
    """
    current = list(existing or [])
    if warning not in current:
        current.append(warning)
    return current


def rank_by_repo(cohort: list[dict]) -> list[tuple[str, int]]:
    """Which repos dominate the cohort. The only useful ranking axis, since
    no suspect row has a harbor_runs row to deprioritise it."""
    counts = Counter(f"{r['owner']}/{r['repo']}" for r in cohort)
    return counts.most_common()


def _write_backup(cohort: list[dict], stem: str) -> Path:
    BACKUP_DIR.mkdir(exist_ok=True)
    path = BACKUP_DIR / f"{stem}_{time.strftime('%Y%m%d_%H%M%S')}.json"
    payload = [
        {
            "owner": r["owner"],
            "repo": r["repo"],
            "sha": r["sha"],
            "issue_number": r.get("issue_number"),
            "test_duration_s": _duration(r),
            "manifest_warnings": r.get("manifest_warnings"),
        }
        for r in cohort
    ]
    path.write_text(json.dumps(payload, indent=2))
    return path


def run_audit(apply: bool) -> int:
    rows = fetch_all(
        "candidate_containers",
        select="owner, repo, sha, issue_number, resource_metrics, manifest_warnings",
    )
    cohort = select_cohort(rows)

    print(f"[audit] {len(cohort)} suspect row(s) of {len(rows)} total")
    print(f"[audit] boundary: test_duration_s >= {DATASMITH_VERIFY_SUSPECT_TIMEOUT_S}s")
    if not cohort:
        return 0

    ranked = rank_by_repo(cohort)
    print(f"[audit] spanning {len(ranked)} repo(s); top offenders:")
    for name, count in ranked[:10]:
        print(f"    {count:>4}  {name}")

    already = sum(1 for r in cohort if WARNING_ID in (r.get("manifest_warnings") or []))
    if already:
        print(f"[audit] {already} row(s) already stamped (idempotent re-run)")

    if not apply:
        print("\n[audit] DRY RUN — nothing written. Re-run with --apply to stamp.")
        print("[audit] Stamping adds a column value only; no row is created or")
        print("[audit] deleted, so no Grafana figure moves.")
        return 0

    backup = _write_backup(cohort, "timeout_verified")
    print(f"\n[audit] backup written to {backup}")

    client = get_client()
    stamped = 0
    for row in cohort:
        warnings = merge_warning(row.get("manifest_warnings"))
        if warnings == (row.get("manifest_warnings") or []):
            continue
        (
            client.table("candidate_containers")
            .update({"manifest_warnings": warnings})
            .eq("owner", row["owner"])
            .eq("repo", row["repo"])
            .eq("sha", row["sha"])
            .execute()
        )
        stamped += 1
    print(f"[audit] stamped {stamped} row(s) with '{WARNING_ID}'")
    return 0


def run_calibrate(sample_size: int) -> int:
    """Re-run a sample at the raised limit to find the true completion tail.

    The existing duration data is CENSORED at 720s -- every longer run was
    killed -- so the correct timeout cannot be read off the histogram. This is
    the only way to learn where completions actually land.

    Writes a report and NOTHING ELSE. It must not route through _save_context:
    that upserts candidate_containers and would overwrite resource_metrics on
    the very rows whose original measurement is the evidence under audit. No
    figure would move, but the audit would destroy its own evidence.

    Note the cost has changed since this was planned: a re-run now also pays
    the measure step (~830s median, ~1550s p90) and faces three new FATAL
    gates, so calibration measures two things at once -- the true duration
    tail AND the new gates' rejection rate on real containers.
    """
    rows = fetch_all(
        "candidate_containers",
        select="owner, repo, sha, issue_number, resource_metrics, manifest_warnings",
    )
    cohort = select_cohort(rows)
    if not cohort:
        print("[calibrate] no suspect rows to sample")
        return 0

    # Sample from the repos that dominate the cohort: fixing a systemic repo
    # issue recovers the most rows.
    ranked = rank_by_repo(cohort)
    by_repo: dict[str, list[dict]] = {}
    for row in cohort:
        by_repo.setdefault(f"{row['owner']}/{row['repo']}", []).append(row)

    sample: list[dict] = []
    for name, _count in ranked:
        if len(sample) >= sample_size:
            break
        sample.append(by_repo[name][0])

    print(f"[calibrate] would re-run {len(sample)} row(s) at the raised limit:")
    for row in sample:
        print(f"    {row['owner']}/{row['repo']}#{row.get('issue_number')} ({_duration(row)}s recorded)")

    BACKUP_DIR.mkdir(exist_ok=True)
    path = BACKUP_DIR / f"calibrate_timeout_{time.strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(
        json.dumps(
            {
                "boundary_s": DATASMITH_VERIFY_SUSPECT_TIMEOUT_S,
                "cohort_size": len(cohort),
                "repos_in_cohort": len(ranked),
                "sample": [
                    {
                        "owner": r["owner"],
                        "repo": r["repo"],
                        "sha": r["sha"],
                        "issue_number": r.get("issue_number"),
                        "recorded_duration_s": _duration(r),
                        "rerun_duration_s": None,
                        "rerun_outcome": None,
                        "note": "not yet re-run; execute stage 6 against these tasks",
                    }
                    for r in sample
                ],
            },
            indent=2,
        )
    )
    print(f"\n[calibrate] report written to {path}")
    print("[calibrate] This writes NO table. Re-run these tasks through stage 6")
    print("[calibrate] and fill in rerun_duration_s / rerun_outcome.")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Audit timeout-verified containers")
    ap.add_argument("--apply", action="store_true", help="Actually stamp (default: dry run)")
    ap.add_argument(
        "--calibrate",
        type=int,
        metavar="N",
        help="Emit a calibration sample of N rows; writes a report, never a table",
    )
    args = ap.parse_args(argv)

    if args.calibrate:
        return run_calibrate(args.calibrate)
    return run_audit(args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
