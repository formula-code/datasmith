"""Stage 7 runner: execute synthesized containers through Harbor's oracle agent.

Unlike the other stages, this one dispatches the whole batch to Harbor in a
single ``Job.run()`` call and lets Harbor's ``LocalOrchestrator`` handle
per-trial concurrency via ``OrchestratorConfig.n_concurrent_trials``. We then
walk the returned ``JobResult`` and persist one ``harbor_runs`` row per
trial (including failures).

The only per-PR work we do is materializing a Harbor task directory via the
vendored ``datasmith.harbor_adapter`` package.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from datasmith.harbor_adapter import FormulaCodeAdapter, to_record
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.harbor_healthcheck")

MIN_SPEEDUP_GATE = 1.05  # mirrored by publish/records.py


def _patch_harbor_trial_name() -> None:
    """Suppress the 7-char random suffix Harbor appends to every trial_name.

    Harbor's ``TrialConfig.set_default_trial_name`` generates
    ``f"{task_name[:32]}__{ShortUUID().random(length=7)}"`` (see
    ``harbor/models/trial/config.py``), which makes trial directories
    non-deterministic across runs and defeats simple re-triage. We want the
    directory under ``jobs/<job_name>/`` to be exactly our ``task_id``
    (``owner_repo_prnumber``) so a second run of the same task lands in the
    same path. Override at import time — only affects this process.
    """
    from harbor.models.trial.config import TrialConfig

    if getattr(TrialConfig, "_fc_datasmith_patched", False):
        return

    def _deterministic_trial_name(self):  # type: ignore[no-untyped-def]
        return self.task.get_task_id().get_name()

    TrialConfig.generate_trial_name = _deterministic_trial_name
    TrialConfig._fc_datasmith_patched = True


def _build_verifier_env() -> dict[str, str]:
    """Emit SUPABASE_* entries for Harbor's own Supabase project, read from
    the HARBOR_-prefixed env vars in tokens.env.

    Unlike oracle_run.py (which emits ``${VAR:-}`` shell-expansion refs that
    resolve from the host env at trial launch), we inline literal values.
    The reason: datasmith's own ``SUPABASE_URL`` / ``SUPABASE_KEY`` point at
    its local Supabase instance and must stay pointed there for the rest of
    the pipeline. Baking Harbor's creds directly into each task.toml keeps
    the two projects cleanly separated and avoids host env swapping around
    ``Job.run()``.

    The resulting values land in the per-task ``[verifier.env]`` section,
    which Harbor copies into each trial container as ``SUPABASE_URL`` /
    ``SUPABASE_ANON_KEY`` / ``SUPABASE_SERVICE_KEY`` — the names
    ``upload.py`` inside the container expects.
    """
    mapping = {
        "SUPABASE_URL": "HARBOR_SUPABASE_URL",
        "SUPABASE_ANON_KEY": "HARBOR_SUPABASE_ANON_KEY",
        "SUPABASE_SERVICE_KEY": "HARBOR_SUPABASE_SERVICE_KEY",
    }
    env: dict[str, str] = {}
    for container_key, host_key in mapping.items():
        val = os.environ.get(host_key)
        if val:
            env[container_key] = val
    # lsv_init.py gates snapshot capture on HARBOR_AGENT_NAME=="oracle"; Harbor
    # itself never sets this. Inline a literal so the two scripts (lsv_init +
    # test.sh) agree on the agent identity the trial is running.
    env["HARBOR_AGENT_NAME"] = "oracle"
    return env


def _materialize_tasks(
    items: list[dict[str, Any]],
    task_dir: Path,
    *,
    rounds: int,
) -> dict[str, dict[str, Any]]:
    """Write one Harbor task directory per PR. Returns a mapping from
    ``task_id`` (the directory name Harbor sees) back to the datasmith row
    metadata we need when inserting harbor_runs."""
    adapter = FormulaCodeAdapter(harbor_tasks_root=task_dir, force=True)
    verifier_env = _build_verifier_env() or None

    task_id_map: dict[str, dict[str, Any]] = {}
    for pr in items:
        try:
            rec = to_record(pr)
        except Exception:
            logger.exception(
                "Failed to build record for %s/%s#%s",
                pr.get("owner"),
                pr.get("repo"),
                pr.get("issue_number"),
            )
            continue
        try:
            adapter.generate_task(
                rec,
                run_pytest=True,
                rounds=rounds,
                verifier_env=verifier_env,
            )
        except Exception:
            logger.exception("generate_task failed for %s", rec.task_id)
            continue
        task_id_map[rec.task_id] = {
            "owner": pr["owner"],
            "repo": pr["repo"],
            "sha": pr["merge_commit_sha"],
            "issue_number": pr["issue_number"],
            "container_name": pr.get("container_name"),
        }
    return task_id_map


def _build_job_config(
    task_dir: Path,
    *,
    use_daytona: bool,
    n_concurrent_trials: int,
    job_name: str,
) -> Any:
    """Construct a Harbor JobConfig that runs the built-in oracle agent on
    every task materialized under ``task_dir``."""
    from harbor.models.environment_type import EnvironmentType
    from harbor.models.job.config import (
        JobConfig,
        LocalDatasetConfig,
        OrchestratorConfig,
    )
    from harbor.models.orchestrator_type import OrchestratorType
    from harbor.models.trial.config import AgentConfig, EnvironmentConfig

    # Harbor defaults to 4 GB per trial container (per the task.toml template)
    # which is too tight for lsv_init on mid/large Python repos — sklearn's
    # dep-graph walk alone exceeds 4 GB and gets OOM-killed with exit 137.
    # Bump to 32 GB across the board; the host has 500 GB so there's plenty
    # of headroom, and smaller repos won't actually use more than they need.
    MEMORY_MB = 32 * 1024
    if use_daytona:
        environment = EnvironmentConfig(
            type=EnvironmentType.DAYTONA,
            force_build=True,
            delete=True,
            override_memory_mb=MEMORY_MB,
            kwargs={
                "auto_stop_interval_mins": 0,
                "auto_delete_interval_mins": 0,
            },
        )
    else:
        environment = EnvironmentConfig(
            type=EnvironmentType.DOCKER,
            force_build=True,
            delete=True,
            override_memory_mb=MEMORY_MB,
        )

    return JobConfig(
        job_name=job_name,
        jobs_dir=Path("jobs"),
        n_attempts=1,
        orchestrator=OrchestratorConfig(
            type=OrchestratorType.LOCAL,
            n_concurrent_trials=n_concurrent_trials,
            quiet=False,
        ),
        environment=environment,
        agents=[AgentConfig(name="oracle")],
        datasets=[LocalDatasetConfig(path=task_dir)],
    )


def _trial_dir_from_uri(trial_uri: str) -> Path:
    """Harbor writes trial_uri as ``file:///.../jobs/<name>/<trial>``."""
    return Path(urlparse(trial_uri).path)


def _row_from_trial(  # noqa: C901
    trial: Any,
    task_id_map: dict[str, dict[str, Any]],
    *,
    environment: str,
) -> dict[str, Any] | None:
    """Build one harbor_runs row from a TrialResult. Returns None if the
    trial can't be mapped back to a datasmith PR."""
    from harbor.models.trial.paths import TrialPaths

    task_id = trial.task_id.get_name()
    meta = task_id_map.get(task_id)
    if meta is None:
        logger.warning("Trial %s has no matching datasmith PR — skipping row", task_id)
        return None

    trial_dir = _trial_dir_from_uri(trial.trial_uri)
    paths = TrialPaths(trial_dir=trial_dir)

    wallclock: float | None = None
    if trial.started_at and trial.finished_at:
        wallclock = (trial.finished_at - trial.started_at).total_seconds()

    reward_payload: dict[str, Any] | None = None
    max_speedup: float | None = None
    geomean: float | None = None
    n_benchmarks: int | None = None
    status = "failed"
    error_message: str | None = None
    harbor_exception: str | None = None

    # Harbor can raise ``VerifierTimeoutError`` *after* test.sh has already
    # written a valid reward.json — the verifier wrapper enforces a
    # wall-clock budget that includes file-upload/stdout-drain overhead, so
    # a trial can land a perfectly good reward.json on disk and still get
    # flagged. In that case we want the success, not the wrapper timeout.
    # → Check reward.json FIRST and treat trial.exception_info as
    # decoration (stored as ``harbor_exception`` for post-mortem triage).
    if trial.exception_info is not None:
        harbor_exception = str(getattr(trial.exception_info, "message", None) or trial.exception_info)

    if paths.reward_json_path.exists():
        try:
            reward_payload = json.loads(paths.reward_json_path.read_text())
        except Exception as exc:
            error_message = f"reward.json parse error: {exc}"
            if harbor_exception:
                error_message = f"{error_message}; harbor_exception: {harbor_exception}"
        else:
            payload = reward_payload or {}
            speedups = payload.get("per_benchmark_speedups") or {}
            if speedups:
                try:
                    max_speedup = max(float(v) for v in speedups.values())
                except (TypeError, ValueError) as exc:
                    error_message = f"non-numeric speedup values: {exc}"
            geomean = payload.get("lsv_mean_speedup")
            n_benchmarks = payload.get("num_valid_benchmarks") or len(speedups) or None

            patch_info = payload.get("patch") or {}
            patch_applied = patch_info.get("applied")
            lsv_block = payload.get("lsv") or {}
            lsv_init = lsv_block.get("init") or {}
            lsv_init_populated = bool(lsv_init)
            impactable = len(lsv_init.get("benchmarks_impactable") or [])
            source_files_covered = lsv_init.get("source_files_covered")
            lsv_error = payload.get("lsv_error")
            tests_passed = payload.get("tests_passed")
            setup = payload.get("setup") or {}
            setup_exit = setup.get("exit_code")
            setup_phase = setup.get("failed_phase")

            # Priority order for status classification — most specific wins.
            # Check setup failures FIRST because a failed setup cascades into
            # every other failure mode (no dep DB → lsv_measure crashes →
            # empty benchmarks → reward.json looks like a run produced no
            # signal). We want the report to name the real root cause.
            if setup_exit not in (None, 0):
                if setup_phase == "lsv_init":
                    status = "lsv_init_failed"
                    error_message = (
                        f"lsv_init.py crashed in setup.sh (exit={setup_exit}). "
                        "Benchmark discovery never completed, so no dep DB was "
                        "written. Check setup.txt in the trial dir for the "
                        "underlying ASV/LSV traceback."
                    )
                else:
                    status = "setup_failed"
                    error_message = f"setup.sh failed in phase '{setup_phase}' (exit={setup_exit})."
            elif patch_applied is False:
                status = "patch_failed"
                error_message = "solve.sh produced no diff vs base_commit"
            elif lsv_init_populated and impactable == 0 and source_files_covered == 0:
                # lsv_init ran TO COMPLETION but found zero coverage — a
                # real repo/LSV compatibility issue, not a crash.
                status = "lsv_init_empty"
                error_message = (
                    "LSV init mapped 0 source files and 0 impactable benchmarks — "
                    "ASV/LSV could not trace imports into this repo's benchmark suite."
                )
            elif max_speedup is not None:
                status = "success"
            elif lsv_error:
                status = "lsv_measure_failed"
                error_message = lsv_error
            elif not speedups:
                status = "no_benchmarks"

            if tests_passed is False and status == "success":
                # Benchmarks ran but tests failed — degrade so publish
                # doesn't silently gate on a broken suite.
                status = "tests_failed"
                error_message = error_message or "tests_passed=False in reward.json"

            # If we used reward.json but Harbor also raised an exception,
            # keep the reward-derived status as authoritative and tack the
            # exception onto error_message so it's still visible in triage.
            if harbor_exception and status != "success":
                suffix = f" (harbor_exception: {harbor_exception})"
                error_message = (error_message or "") + suffix
    else:
        # No reward.json means the trial never completed a verifier pass.
        # Harbor's exception (if any) is now the only signal we have.
        if harbor_exception:
            error_message = f"reward.json missing; harbor_exception: {harbor_exception}"
            if "VerifierTimeoutError" in harbor_exception or "timeout" in harbor_exception.lower():
                status = "verifier_timeout"
            else:
                status = "harbor_exception"
        else:
            error_message = "reward.json missing (no harbor exception recorded)"

    return {
        "owner": meta["owner"],
        "repo": meta["repo"],
        "sha": meta["sha"],
        "issue_number": meta["issue_number"],
        "container_name": meta["container_name"],
        "environment": environment,
        "agent_name": "oracle",
        "status": status,
        "max_speedup": max_speedup,
        "geomean_speedup": geomean,
        "n_benchmarks": n_benchmarks,
        "wallclock_sec": wallclock,
        "reward_payload": reward_payload,
        "error_message": error_message,
    }


def _insert_harbor_runs(rows: list[dict[str, Any]], chunk_size: int = 100) -> int:
    """Insert (not upsert) into harbor_runs. Each row is a fresh run with an
    auto-generated run_id, so upsert semantics don't apply here.

    Failure handling: harbor_runs has a foreign key on
    ``candidate_containers(owner, repo, sha)``. If a PR's container row
    has been deleted out from under us (e.g. stage 6 rebuilt and the old
    sha was dropped), the chunk insert raises ``23503`` and we'd lose
    every other row in the chunk. Retry row-by-row on chunk failure so
    orphan rows are logged and skipped instead of blowing up the whole
    stage.
    """
    if not rows:
        return 0
    client = get_client()
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        try:
            client.table("harbor_runs").insert(chunk).execute()
            total += len(chunk)
            continue
        except Exception as chunk_exc:
            logger.warning(
                "harbor_runs chunk insert failed (%s); retrying row-by-row",
                chunk_exc,
            )
        # Fall back to per-row inserts so a single FK violation or other
        # row-scoped error doesn't drop the rest of the batch.
        for row in chunk:
            try:
                client.table("harbor_runs").insert(row).execute()
                total += 1
            except Exception as row_exc:
                logger.warning(
                    "harbor_runs orphan row skipped: %s/%s@%s status=%s — %s",
                    row.get("owner"),
                    row.get("repo"),
                    (row.get("sha") or "")[:12],
                    row.get("status"),
                    row_exc,
                )
    return total


async def run_harbor_healthcheck(
    items: list[dict[str, Any]],
    *,
    task_dir: Path,
    use_daytona: bool = False,
    n_concurrent_trials: int = 4,
    rounds: int = 2,
    job_name: str | None = None,
) -> list[dict[str, Any]]:
    """Materialize *items* into *task_dir*, run Harbor's oracle agent on the
    whole batch, and persist one ``harbor_runs`` row per trial.

    Returns the list of row dicts that were inserted (useful for tests).
    """
    from harbor.job import Job

    _patch_harbor_trial_name()

    if not items:
        logger.info("run_harbor_healthcheck: nothing to do")
        return []

    if job_name is None:
        job_name = f"fc-healthcheck-{uuid.uuid4().hex[:8]}"

    task_id_map = _materialize_tasks(items, task_dir, rounds=rounds)
    if not task_id_map:
        logger.warning("No tasks materialized — skipping Harbor dispatch")
        return []
    logger.info("Materialized %d tasks under %s", len(task_id_map), task_dir)

    environment = "daytona" if use_daytona else "docker"
    config = _build_job_config(
        task_dir,
        use_daytona=use_daytona,
        n_concurrent_trials=n_concurrent_trials,
        job_name=job_name,
    )
    logger.info(
        "Dispatching Harbor job '%s' on %s (n_concurrent_trials=%d)",
        job_name,
        environment,
        n_concurrent_trials,
    )

    await Job(config).run()

    # Harbor's Job.run() returns a JobResult whose `trial_results` field is
    # never populated (job.py:380-435 — only the stats summary is bubbled up;
    # per-trial TrialResult objects are written to per-trial result.json files
    # on disk and otherwise dropped). Walk those files ourselves.
    from harbor.models.trial.result import TrialResult

    rows: list[dict[str, Any]] = []
    job_dir = config.jobs_dir / job_name
    trial_result_paths = sorted(job_dir.glob("*/result.json"))
    logger.info("Walking %d per-trial result.json files under %s", len(trial_result_paths), job_dir)
    for path in trial_result_paths:
        try:
            trial = TrialResult.model_validate_json(path.read_text())
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", path, exc)
            continue
        row = _row_from_trial(trial, task_id_map, environment=environment)
        if row is not None:
            rows.append(row)

    n_success = sum(1 for r in rows if r["status"] == "success")
    n_fast = sum(
        1
        for r in rows
        if r["status"] == "success" and r["max_speedup"] is not None and r["max_speedup"] >= MIN_SPEEDUP_GATE
    )
    logger.info(
        "Harbor job '%s' done: %d/%d trials succeeded, %d >= %.2fx gate",
        job_name,
        n_success,
        len(rows),
        n_fast,
        MIN_SPEEDUP_GATE,
    )

    _insert_harbor_runs(rows)
    return rows
