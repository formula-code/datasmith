"""Measure stage: one harbor job per task with the oracle runs and the no-change runs, written to oracle/<task>.json.

Each run is a paired A-B-B-A timing inside one container (template tests/lsv_measure.py); the host measure gate limits
how many timings run at once. A job whose trials did not all finish, or that has infrastructure failures, is counted
in the state (`measure_jobs`) and runs again under a new job name, so harbor never resumes it.
"""

from __future__ import annotations

import json
import math
import os
import re
import statistics
import subprocess
import time
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from datasmith.verify import images
from datasmith.verify.noise import load_rule, repo_spreads, rule_for, thetas
from datasmith.verify.survey import Survey, repo_of

NOP_AGENT = "datasmith.verify.agents:NopTouch"
# Container temp dir inside the mounted artifacts dir, so benchmark leftovers land on the jobs disk.
TRIAL_TMPDIR = "/logs/artifacts/.tmp"
INFRA_EXCEPTIONS = {
    "AgentSetupTimeoutError",
    "EnvironmentStartTimeoutError",
    "SetupScriptError",
    "HealthcheckError",
    "AddTestsDirError",
    "DownloadVerifierDirError",
}
SECRET_ENV = re.compile(r"KEY|TOKEN|SECRET|PASSWORD")


@dataclass
class Settings:
    n_oracle: int = 3
    n_nop: int = 3
    gate_url: str = "http://127.0.0.1:8267"
    gateway_url: str = "http://127.0.0.1:8268"
    gate_acquire_wait: int = 7200
    gate_secret_file: str | None = None
    trial_cpus: int = 2
    trial_memory_mb: int = 12288
    timeout_s: int = 3600
    lsv_rounds: int = 5
    confirm_rounds: int = 0
    snapshot_workers: int = 1
    verifier_timeout_multiplier: float = 1.0
    max_heavy: int = 1
    heavy_benchmarks: int = 200
    harbor: str = "harbor"
    # Per task: {"mounts": {host: container}, "env": {...}} for tasks whose benchmarks read files at run time.
    offline: dict[str, dict] = field(default_factory=dict)


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def when(stamp: str | None) -> float | None:
    try:
        return datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
    except (AttributeError, ValueError):
        return None


def seconds(span: dict | None) -> float | None:
    start, end = when((span or {}).get("started_at")), when((span or {}).get("finished_at"))
    return None if start is None or end is None else end - start


def outputs_dir(trial: Path) -> Path:
    """<trial>/verifier/artifacts with a separate verifier container, else <trial>/artifacts."""
    sep = trial / "verifier" / "artifacts"
    return sep if sep.is_dir() else trial / "artifacts"


def patch_files(solve_sh: Path) -> list[str]:
    return sorted(set(re.findall(r"^\+\+\+ b/(\S+)", solve_sh.read_text(), re.M))) if solve_sh.exists() else []


def confirmed_speedups(trial: Path) -> dict[str, float]:
    """Benchmarks the confirmation pass timed again: median over both passes' paired log ratios."""
    lsv = outputs_dir(trial) / "lsv"
    first = (read_json(lsv / "lsv_measure_results.json") or {}).get("benchmarks") or {}
    out = {}
    for name, b in ((read_json(lsv / "lsv_measure_confirm.json") or {}).get("benchmarks") or {}).items():
        logs = [x for x in ((first.get(name) or {}).get("paired") or {}).get("log_ratios") or [] if x is not None]
        logs += [x for x in (b.get("paired") or {}).get("log_ratios") or [] if x is not None]
        if logs:
            out[name] = math.exp(statistics.median(logs))
    return out


def log_speedups(trial: Path) -> dict[str, float]:
    """ln speedup per benchmark: reward.json's, with the confirmation pass's medians in place."""
    per = (read_json(trial / "verifier" / "reward.json") or {}).get("per_benchmark_speedups")
    if not isinstance(per, dict):
        return {}
    out = {}
    for name, value in {**per, **confirmed_speedups(trial)}.items():
        try:
            g = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(g) and g > 0:
            out[name] = math.log(g)
    return out


def test_outcomes(trial: Path) -> dict[str, Any]:
    tr = read_json(outputs_dir(trial) / "test_results.json") or {}
    reg = tr.get("regression") or {}
    failing = {
        t["nodeid"] for t in (tr.get("results") or {}).get("tests") or [] if t.get("outcome") in ("failed", "error")
    }
    return {
        "ran": bool(reg.get("ran")),
        "n_base_pass": reg.get("n_base_pass") or 0,
        "n_tests": reg.get("n_agent_tests") or 0,
        "n_regressed": reg.get("n_regressed") or 0,
        "failing": sorted(failing),
    }


def patch_failures(trial: Path) -> list[str]:
    """Files with a hunk the oracle's `patch` could not apply."""
    log = trial / "agent" / "oracle.txt"
    failed, current = [], None
    for line in log.read_text(errors="replace").splitlines() if log.exists() else []:
        if line.startswith("patching file "):
            current = line[len("patching file ") :].strip()
        elif line.startswith("Hunk #") and "FAILED" in line and current and current not in failed:
            failed.append(current)
    return failed


def ungated(trial: Path, timeouts: list[dict]) -> bool:
    """True when a gate acquire of this trial failed, so a timing may have run beside others."""
    acquires = outputs_dir(trial) / "measure_gate.txt"
    if acquires.is_file():
        return any(line.split()[-1] != "1" for line in acquires.read_text().splitlines() if line.strip())
    result = read_json(trial / "result.json") or {}
    start, end = when(result.get("started_at")), when(result.get("finished_at"))
    if start is None:
        return bool(timeouts)
    end = end or time.time()
    return any(t["requested_at"] <= end and t["timed_out_at"] >= start for t in timeouts)


def trial_run(trial: Path, timeouts: list[dict]) -> dict[str, Any]:
    """One measure trial as a record run. `infra` marks a trial without data (setup failure, ungated, no reward file)."""
    rj = read_json(trial / "verifier" / "reward.json")
    agent = (read_json(trial / "config.json") or {}).get("agent") or {}
    role = (
        "nop"
        if str(agent.get("import_path") or "").endswith(":NopTouch")
        else ("oracle_baseline" if not agent.get("model_name") else "oracle")
    )
    reuse = (read_json(outputs_dir(trial) / "lsv" / "lsv_init_results.json") or {}).get("baseline_reuse") or {}
    result = read_json(trial / "result.json") or {}
    exc = (result.get("exception_info") or {}).get("exception_type")
    gated = not ungated(trial, timeouts)
    return {
        "trial": trial.name,
        "trial_dir": str(trial),
        "role": role,
        "run": int((agent.get("model_name") or "run1")[3:]),
        "exception": exc,
        "cpu_model": (reuse.get("trial_fingerprint") or {}).get("cpu_model"),
        "gate_acquired": gated,
        "baseline_reused": reuse.get("reused"),
        "lsv_mean_speedup": (rj or {}).get("lsv_mean_speedup"),
        "tests_passed": (rj or {}).get("tests_passed"),
        "snapshots_passed": (rj or {}).get("snapshots_passed"),
        "snapshot_verify_ran": (rj or {}).get("snapshot_verify_ran"),
        "project_shadowed": (rj or {}).get("project_shadowed"),
        "log_speedups": log_speedups(trial),
        "tests": test_outcomes(trial),
        "patch_failed": patch_failures(trial),
        "timing": {
            "started_at": result.get("started_at"),
            "finished_at": result.get("finished_at"),
            "trial_s": seconds(result),
            **{
                f"{k}_s": seconds(result.get(k))
                for k in ("environment_setup", "agent_setup", "agent_execution", "verifier")
            },
        },
        "infra": exc in INFRA_EXCEPTIONS or not gated or rj is None,
    }


def job_leases(job_dir: Path) -> list[str]:
    """Gate lease ids its trials logged; lines without an id (older templates) are skipped."""
    sids = []
    for log in job_dir.glob("*/**/measure_gate.txt"):
        sids += [parts[1] for line in log.read_text().splitlines() if len(parts := line.split()) == 3]
    return sorted(set(sids))


def release_leases(job_dir: Path, gate_url: str) -> list[str]:
    """Release the leases of a stopped job; the gate ignores ids it does not hold."""
    released = []
    for sid in job_leases(job_dir):
        req = urllib.request.Request(f"{gate_url}/release?sid={sid}", method="POST")  # noqa: S310
        try:
            with urllib.request.urlopen(req, timeout=10):  # noqa: S310
                released.append(sid)
        except OSError:
            continue
    return released


def gate_token(secret_file: str, label: str) -> str:
    from datasmith.verify.services.measure_gate import token_for

    return token_for(Path(secret_file).read_bytes().strip(), label)


def confirm_spec(survey: Survey, task: str, rule: dict, rounds: int) -> dict:
    """tests/confirm.json: theta per benchmark from the task's existing record, else theta_min for every benchmark."""
    r = rule_for(rule, repo_of(task))
    out = {
        "rounds": rounds,
        "theta_default": r.get("theta_min", r.get("theta")),
        "theta": {},
        "oracle_up": [],
        "theta_source": "floor",
    }
    record = read_json(survey.root / "oracle" / f"{task}.json")
    if record and record.get("runs"):
        spreads = repo_spreads(list(survey.records().values()))
        out.update(theta=thetas(rule, record, spreads), theta_source="record")
    return out


def job_config(survey: Survey, task: str, job: str, s: Settings, tasks_root: Path) -> dict[str, Any]:
    """Harbor job: the oracle once per oracle run and NopTouch once per no-change run, all concurrent."""
    timeout = s.timeout_s
    agents = [{"name": "oracle", "override_timeout_sec": timeout}]
    agents += [
        {"name": "oracle", "model_name": f"run{i}", "override_timeout_sec": timeout} for i in range(2, s.n_oracle + 1)
    ]
    files = patch_files(survey.task_dir(task) / "solution" / "solve.sh")
    agents += [
        {"import_path": NOP_AGENT, "model_name": f"run{i}", "kwargs": {"files": files}} for i in range(1, s.n_nop + 1)
    ]
    env = {
        "MEASURE_GATE_URL": s.gate_url,
        "TMPDIR": TRIAL_TMPDIR,
        "MEASURE_GATE_ACQUIRE_WAIT": str(s.gate_acquire_wait),
        "FC_AGENT_TAG": "agent",
        "FORMULACODE_SNAPSHOT_WORKERS": str(s.snapshot_workers),
    }
    cfg: dict[str, Any] = {
        "job_name": job,
        "jobs_dir": str(survey.root / "jobs"),
        "n_attempts": 1,
        "n_concurrent_trials": s.n_oracle + s.n_nop,
        "quiet": False,
        "environment": {
            "type": "docker",
            "force_build": False,
            "delete": True,
            "override_cpus": s.trial_cpus,
            "override_memory_mb": s.trial_memory_mb,
            "env": env,
        },
        "verifier": {
            "env": {
                "SUPABASE_URL": s.gateway_url,
                "SUPABASE_ANON_KEY": "local",
                "FORMULACODE_SNAPSHOT_FILTER": ".*",
                "FORMULACODE_NO_UPLOAD": "",
                "DATASMITH_LSV_ROUNDS": str(s.lsv_rounds),
            }
        },
        "agents": agents,
        "datasets": [{"path": str(tasks_root), "task_names": [task]}],
        "verifier_timeout_multiplier": s.verifier_timeout_multiplier,
    }
    if offline := s.offline.get(task):
        cfg["environment"]["mounts_json"] = [
            {"type": "bind", "source": src, "target": dst, "read_only": True, "bind": {"create_host_path": False}}
            for src, dst in offline.get("mounts", {}).items()
        ]
        env.update(offline.get("env", {}))
    if s.gate_secret_file:
        cfg["verifier"]["env"]["MEASURE_GATE_TOKEN"] = gate_token(s.gate_secret_file, job)
    return cfg


def tasks_root_for(survey: Survey, task: str, job: str, confirm: dict | None) -> Path:
    """The pinned tasks root, or a copy for this job with tests/confirm.json for the confirmation pass."""
    if not confirm:
        return survey.root / "pinned"
    import shutil

    dst = survey.root / "tasks-confirm" / job
    shutil.copytree(survey.task_dir(task), dst / task, symlinks=True, dirs_exist_ok=True)
    (dst / task / "tests" / "confirm.json").write_text(json.dumps(confirm))
    return dst


def run_harbor(
    survey: Survey, job: str, config: Path, s: Settings, run: Callable[..., Any] = subprocess.run
) -> list[Path]:
    env = {k: v for k, v in os.environ.items() if not SECRET_ENV.search(k)}
    env.update(HARBOR_DOCKER_HOST_NET="1", COMPOSE_BAKE="false", DOCKER_BUILDKIT="0")
    (survey.root / "logs").mkdir(parents=True, exist_ok=True)
    with (survey.root / "logs" / f"{job}.log").open("a") as log:
        run(
            [s.harbor, "run", "--config", str(config)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    return sorted(p.parent for p in (survey.root / "jobs" / job).glob("*/result.json"))


def gate_timeouts(s: Settings) -> list[dict]:
    try:
        with urllib.request.urlopen(s.gate_url + "/timeouts", timeout=10) as r:  # noqa: S310
            return json.load(r)["timeouts"]
    except OSError:
        return []


def is_heavy(survey: Survey, task: str, s: Settings) -> bool:
    cov = read_json(survey.root / "coverage" / f"{task}.json") or {}
    return (cov.get("selected") or 0) > s.heavy_benchmarks


def measure_task(
    survey: Survey,
    task: str,
    s: Settings,
    manifest: Path | None = None,
    grade: Callable[[Path, dict], dict] | None = None,
    run: Callable[..., Any] = subprocess.run,
) -> str:
    """Measure one task and return its new phase: measured, no_benchmarks or error.

    `grade(trial, run_row)` may add fields to each run (for example a correctness verdict from the training grader).
    """
    st = survey.state(task)
    n = st.get("measure_jobs", 0)
    job = f"{task}__oracle" + (f"-{n}" if n else "")
    if manifest is not None and (row := images.rows_by_task(manifest).get(task)):
        images.load(row)
    rule = load_rule(survey.rule_path) if s.confirm_rounds > 0 else None
    confirm = confirm_spec(survey, task, rule, s.confirm_rounds) if rule else None
    config = survey.root / "configs" / f"{job}.yaml"
    if not config.exists():
        config.parent.mkdir(parents=True, exist_ok=True)
        config.write_text(
            yaml.safe_dump(
                job_config(survey, task, job, s, tasks_root_for(survey, task, job, confirm)), sort_keys=False
            )
        )
    trials = run_harbor(survey, job, config, s, run)
    timeouts = gate_timeouts(s)
    runs = [trial_run(t, timeouts) for t in trials]
    for r, t in zip(runs, trials, strict=True):
        if grade:
            r.update(grade(t, r))
    infra = [r["trial"] for r in runs if r.pop("infra")]
    want = s.n_oracle + s.n_nop
    if len(runs) < want or infra:
        survey.save_state(
            task,
            {
                **st,
                "phase": "error",
                "measure_jobs": n + 1,
                "last_error": f"{job}: {len(runs)}/{want} trials, infra failures {infra}",
            },
        )
        return "error"
    checksum = (read_json(trials[0] / "result.json") or {}).get("task_checksum")
    record = {
        "task": task,
        "job": job,
        "tag": st.get("tag"),
        "n_oracle": s.n_oracle,
        "n_nop": s.n_nop,
        "task_checksum": checksum,
        "measured_at": utc(),
        "cpu_models": sorted({r["cpu_model"] for r in runs if r.get("cpu_model")}),
        "runs": sorted(runs, key=lambda r: (r["role"] == "nop", r["run"])),
    }
    out = survey.root / "oracle" / f"{task}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    phase = "measured" if any(r["log_speedups"] for r in runs) else "no_benchmarks"
    survey.save_state(task, {**st, "phase": phase, "measure_jobs": n + 1, "cpu_model": ",".join(record["cpu_models"])})
    return phase


def measure_many(
    survey: Survey,
    tasks: list[str],
    s: Settings,
    workers: int = 1,
    manifest: Path | None = None,
    grade: Callable[[Path, dict], dict] | None = None,
) -> dict[str, str]:
    """Measure tasks `workers` at a time, with at most `s.max_heavy` heavy tasks at once (0 = no limit)."""
    import threading

    heavy = threading.Semaphore(s.max_heavy) if s.max_heavy else None
    results: dict[str, str] = {}

    def one(task: str) -> None:
        if (survey.root / "STOP").exists():
            return
        if heavy and is_heavy(survey, task, s):
            with heavy:
                results[task] = measure_task(survey, task, s, manifest, grade)
        else:
            results[task] = measure_task(survey, task, s, manifest, grade)

    with ThreadPoolExecutor(max(1, workers)) as pool:
        list(pool.map(one, tasks))
    return results
