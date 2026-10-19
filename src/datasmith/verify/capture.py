"""Snapshot capture: one oracle trial records the snapshot reference, then one no-change trial is checked against it.

Capture trials run a copy of the task whose test.sh skips LSV timing and pytest, so they time nothing. A reference with
at least one real entry goes to `<survey>/snapshots/<task>/` and the record gets `snapshot_source = "fresh"`. The
no-change trial's mismatches are false alarms of the snapshot check and are stored as `snapshot_nop_mismatch`.
"""

from __future__ import annotations

import collections
import json
import re
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from datasmith.verify import images
from datasmith.verify.measure import Settings, job_config, outputs_dir, patch_failures, read_json, run_harbor, utc
from datasmith.verify.survey import Survey

CAPTURE_SKIPS = re.compile(r"python /tests/(lsv_measure|pytest_runner)\.py[^;\n]*")


def real_snapshots(baseline: Any) -> bool:
    """True when a snapshot baseline has at least one entry that is not a skip."""
    entries = (baseline or {}).get("entries") if isinstance(baseline, dict) else None
    return isinstance(entries, dict) and any(v != "skip" for v in entries.values())


def capture_task_dir(survey: Survey, task: str) -> Path:
    """A copy of the pinned task whose test.sh runs no LSV timing and no pytest; returns its tasks root."""
    root = survey.root / "tasks-capture"
    shutil.copytree(survey.task_dir(task), root / task, symlinks=True, dirs_exist_ok=True)
    test = root / task / "tests" / "test.sh"
    text, n = CAPTURE_SKIPS.subn("true", (survey.task_dir(task) / "tests" / "test.sh").read_text())
    if n < 2:
        raise RuntimeError(f"{task}: expected the LSV measure and pytest calls in test.sh, found {n}")
    test.unlink()
    test.write_text(text)
    return root


def capture_config(survey: Survey, task: str, job: str, s: Settings, nop: bool) -> dict:
    cfg = job_config(
        survey,
        task,
        job,
        Settings(**{**s.__dict__, "n_oracle": 0 if nop else 1, "n_nop": 1 if nop else 0}),
        capture_task_dir(survey, task),
    )
    if not nop:
        # Without FC_AGENT_TAG the oracle trial records the reference at setup.
        del cfg["environment"]["env"]["FC_AGENT_TAG"]
    return cfg


def capture_trial(survey: Survey, task: str, s: Settings, nop: bool, run: Callable[..., Any]) -> Path | None:
    st = survey.state(task)
    kind = "capture_nop" if nop else "capture"
    n = st.get(f"{kind}_jobs", 0)
    job = f"{task}__{kind}" + (f"-{n}" if n else "")
    config = survey.root / "configs" / f"{job}.yaml"
    if not config.exists():
        config.parent.mkdir(parents=True, exist_ok=True)
        config.write_text(yaml.safe_dump(capture_config(survey, task, job, s, nop), sort_keys=False))
    trials = run_harbor(survey, job, config, s, run)
    want = "summary_*.json" if nop else ".snapshots/baseline.json"
    done = [t for t in trials if any(outputs_dir(t).glob(want))]
    if not done:
        survey.save_state(
            task, {**st, f"{kind}_jobs": n + 1, "last_error": f"{job}: no {want} in {len(trials)} trial(s)"}
        )
        return None
    return done[0]


def update_record(survey: Survey, task: str, **fields: Any) -> None:
    path = survey.root / "oracle" / f"{task}.json"
    path.write_text(json.dumps({**(read_json(path) or {}), **fields}, indent=2, sort_keys=True) + "\n")


def capture(
    survey: Survey, task: str, s: Settings, manifest: Path | None = None, run: Callable[..., Any] = subprocess.run
) -> str:
    """Capture the reference and check a no-change run against it; returns the task's capture result."""
    if manifest is not None and (row := images.rows_by_task(manifest).get(task)):
        images.load(row)
    record = read_json(survey.root / "oracle" / f"{task}.json") or {}
    have = record.get("snapshot_source") == "fresh" and real_snapshots(
        read_json(survey.root / "snapshots" / task / "baseline.json")
    )
    if not have:
        trial = capture_trial(survey, task, s, nop=False, run=run)
        if trial is None:
            return "capture_failed"
        snap = outputs_dir(trial) / ".snapshots"
        baseline = read_json(snap / "baseline.json")
        info = {
            "job": trial.parent.name,
            "trial_dir": str(trial),
            "captured_at": utc(),
            "patch_failed": patch_failures(trial),
            "task_checksum": record.get("task_checksum"),
            "counts": dict(collections.Counter(((baseline or {}).get("entries") or {}).values())),
        }
        if not real_snapshots(baseline) or info["patch_failed"]:
            update_record(survey, task, snapshot_capture=info)
            return "no_snapshots"
        shutil.copytree(snap, survey.root / "snapshots" / task, dirs_exist_ok=True)
        update_record(survey, task, snapshot_source="fresh", snapshot_capture=info)
    trial = capture_trial(survey, task, s, nop=True, run=run)
    if trial is None:
        return "capture_nop_failed"
    summary = read_json(sorted(outputs_dir(trial).glob("summary_*.json"))[0]) or {}
    transitions = {k: v for k, v in summary.items() if "-to-" in k and isinstance(v, int)}
    mismatch = (summary.get("pass-to-fail", 0) + summary.get("skip-to-fail", 0)) if transitions else None
    update_record(
        survey,
        task,
        snapshot_nop_mismatch=mismatch,
        snapshot_nop={
            "job": trial.parent.name,
            "trial_dir": str(trial),
            "transitions": transitions,
            "mismatch_ids": sorted(summary.get("pass-to-fail-ids", []) + summary.get("skip-to-fail-ids", [])),
        },
    )
    return "captured"
