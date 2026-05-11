"""Load per-benchmark speedups from terminal-bench runs into ``benchmark_information``.

Scope: only the tasks listed in ``analysis/tasks.txt`` (one ``runs/<run_id>/<task_id>``
path per line). For each line we read the run-level ``run_metadata.json`` and the
per-task ``tests/config.json`` from the dataset the run consumed, look up the
canonical owner/repo/issue_number in ``pull_requests`` via ``merge_commit_sha = gt_hash``,
then explode ``parser_extra_metrics.per_benchmark_speedups_by_agent`` into one row
per (agent, model, benchmark).

The per-benchmark dict is identical across trials within a task, so we only
process the first trial entry that carries a non-empty dict.

speedup = (agent/nop) / (oracle/nop) — i.e. oracle_time / agent_time, so 1.0 = parity
with the human expert and >1.0 means the agent beat the human.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from functools import cache
from pathlib import Path
from typing import Any

from datasmith.utils.db import get_client

logger = logging.getLogger("load_benchmark_information")

DEFAULT_TB_ROOT = Path("/mnt/sdd1/atharvas/formulacode/eval_frameworks/terminal-bench")
DEFAULT_TASKS_FILE = DEFAULT_TB_ROOT / "analysis" / "tasks.txt"


def parse_agent_key(key: str) -> tuple[str, str | None]:
    """``"terminus-2:anthropic-claude-sonnet-4-20250514"`` -> ("terminus-2", "...")."""
    if ":" not in key:
        return key, None
    agent, model = key.split(":", 1)
    if agent == "oracle" and model == "oracle":
        return "oracle", None
    return agent, model


@cache
def load_run_metadata(tb_root: Path, run_id: str) -> dict[str, Any]:
    p = tb_root / "runs" / run_id / "run_metadata.json"
    return json.loads(p.read_text())


@cache
def load_results(tb_root: Path, run_id: str) -> dict[str, Any]:
    p = tb_root / "runs" / run_id / "results.json"
    return json.loads(p.read_text())


def load_task_config(tb_root: Path, dataset_path: Path, task_id: str) -> dict[str, Any] | None:
    """Read ``<dataset>/<task_id>/tests/config.json``.

    ``run_metadata.dataset_path`` may point to a host path that doesn't exist on
    this machine; if so we fall back to scanning ``<tb_root>/dataset/*`` for a
    matching task directory.
    """
    candidates: list[Path] = []
    if dataset_path.exists():
        candidates.append(dataset_path / task_id / "tests" / "config.json")
    for ds in (tb_root / "dataset").glob("*"):
        candidates.append(ds / task_id / "tests" / "config.json")
    for cand in candidates:
        if cand.exists():
            return json.loads(cand.read_text())
    return None


@cache
def lookup_pr_by_sha(gt_hash: str) -> tuple[str, str, int] | None:
    client = get_client()
    resp = (
        client.table("pull_requests")
        .select("owner,repo,issue_number")
        .eq("merge_commit_sha", gt_hash)
        .limit(1)
        .execute()
    )
    if not resp.data:
        return None
    row = resp.data[0]
    return row["owner"], row["repo"], int(row["issue_number"])


def find_per_benchmark_dict(results: dict[str, Any], task_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Return (per_benchmark_speedups_by_agent, sample_trial) for the task.

    Picks the first trial whose ``per_benchmark_speedups_by_agent`` is non-empty,
    since the dict is identical across trials within a task.
    """
    sample: dict[str, Any] | None = None
    for trial in results.get("results", []):
        if trial.get("task_id") != task_id:
            continue
        sample = sample or trial
        pem = trial.get("parser_extra_metrics") or {}
        pbs = pem.get("per_benchmark_speedups_by_agent") or {}
        if pbs:
            return pbs, trial
    return ({}, sample) if sample is not None else None


def build_rows(
    *,
    run_id: str,
    measured_at: str,
    commit_hash: str | None,
    owner: str,
    repo: str,
    issue_number: int,
    pbs: dict[str, dict[str, dict[str, Any]]],
    sample_trial: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    trial_started = sample_trial.get("trial_started_at")
    trial_ended = sample_trial.get("trial_ended_at")
    for agent_key, benchmarks in pbs.items():
        agent_name, model_name = parse_agent_key(agent_key)
        for bm_name, bm in benchmarks.items():
            agent_vs_nop = bm.get("agent/nop")
            oracle_vs_nop = bm.get("oracle/nop")
            if agent_vs_nop is None or oracle_vs_nop in (None, 0):
                continue
            speedup = agent_vs_nop / oracle_vs_nop
            rows.append({
                "measured_at": measured_at,
                "run_id": run_id,
                "owner": owner,
                "repo": repo,
                "issue_number": issue_number,
                "benchmark_name": bm_name,
                "agent_name": agent_name,
                "model_name": model_name,
                "speedup": speedup,
                "agent_speedup_vs_nop": agent_vs_nop,
                "oracle_speedup_vs_nop": oracle_vs_nop,
                "advantage": bm.get("advantage"),
                "significant": bm.get("significant"),
                "commit_hash": commit_hash,
                "trial_started_at": trial_started,
                "trial_ended_at": trial_ended,
                "raw_payload": bm,
            })
    return rows


def upsert_rows(rows: list[dict[str, Any]], batch: int = 500) -> None:
    if not rows:
        return
    client = get_client()
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        client.table("benchmark_information").upsert(
            chunk,
            on_conflict="run_id,owner,repo,issue_number,benchmark_name,agent_name,model_name",
        ).execute()
        logger.info("upserted %d rows (%d/%d)", len(chunk), i + len(chunk), len(rows))


def process_task_line(tb_root: Path, line: str) -> list[dict[str, Any]]:
    parts = Path(line).parts
    if len(parts) < 3 or parts[0] != "runs":
        logger.warning("skip malformed line: %s", line)
        return []
    run_id, task_id = parts[1], parts[2]

    meta = load_run_metadata(tb_root, run_id)
    measured_at = meta.get("start_time") or meta.get("end_time")
    commit_hash = meta.get("commit_hash")
    dataset_path = Path(meta.get("dataset_path", ""))

    config = load_task_config(tb_root, dataset_path, task_id)
    if config is None:
        logger.warning("no tests/config.json found for %s/%s", run_id, task_id)
        return []
    gt_hash = config.get("gt_hash")
    if not gt_hash:
        logger.warning("no gt_hash for %s/%s", run_id, task_id)
        return []

    pr = lookup_pr_by_sha(gt_hash)
    if pr is None:
        logger.warning("no pull_requests row for gt_hash=%s (%s/%s)", gt_hash, run_id, task_id)
        return []
    owner, repo, issue_number = pr

    results = load_results(tb_root, run_id)
    found = find_per_benchmark_dict(results, task_id)
    if found is None:
        logger.warning("no trial found for %s/%s", run_id, task_id)
        return []
    pbs, sample_trial = found
    if not pbs:
        logger.info("no per_benchmark_speedups_by_agent for %s/%s", run_id, task_id)
        return []

    return build_rows(
        run_id=run_id,
        measured_at=measured_at,
        commit_hash=commit_hash,
        owner=owner,
        repo=repo,
        issue_number=issue_number,
        pbs=pbs,
        sample_trial=sample_trial,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tb-root", type=Path, default=DEFAULT_TB_ROOT)
    ap.add_argument("--tasks-file", type=Path, default=DEFAULT_TASKS_FILE)
    ap.add_argument("--dry-run", action="store_true", help="Build rows but don't upsert.")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N task lines.")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    lines = [ln.strip() for ln in args.tasks_file.read_text().splitlines() if ln.strip()]
    if args.limit:
        lines = lines[: args.limit]
    logger.info("processing %d task lines from %s", len(lines), args.tasks_file)

    all_rows: list[dict[str, Any]] = []
    for ln in lines:
        try:
            all_rows.extend(process_task_line(args.tb_root, ln))
        except Exception:
            logger.exception("failed on %s", ln)

    logger.info("built %d rows", len(all_rows))
    if args.dry_run:
        if all_rows:
            logger.info("sample row: %s", json.dumps(all_rows[0], default=str, indent=2)[:500])
        return 0

    upsert_rows(all_rows)
    logger.info("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
