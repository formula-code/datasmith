"""Stage 7 runner: execute synthesized containers through Harbor's oracle agent.

The whole batch goes to Harbor in one ``Job.run()`` call, and Harbor sets
per-trial concurrency. Each trial, failed or not, becomes one ``harbor_runs`` row.
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
from datasmith.utils.overrides import expected_n_for, fetch_overrides

logger = get_logger("runners.harbor_healthcheck")

MIN_SPEEDUP_GATE = 1.05  # mirrored by publish/records.py


def _patch_harbor_trial_name() -> None:
    """Remove the random suffix Harbor appends to every trial name.

    The trial directory is then exactly the task name, so a rerun lands in the
    same path. The patch affects only this process.
    """
    from harbor.models.trial.config import TrialConfig

    if getattr(TrialConfig, "_fc_datasmith_patched", False):
        return

    def _deterministic_trial_name(self):  # type: ignore[no-untyped-def]
        return self.task.get_task_id().get_name()

    TrialConfig.generate_trial_name = _deterministic_trial_name
    TrialConfig._fc_datasmith_patched = True


def _build_verifier_env() -> dict[str, str]:
    """Return ``[verifier.env]`` entries for Harbor's Supabase project.

    Values come from the ``HARBOR_SUPABASE_*`` variables and are written as
    literals, because datasmith's own ``SUPABASE_URL`` points at a different project.
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
    # lsv_init.py captures snapshots only for the oracle, and Harbor does not set this.
    env["HARBOR_AGENT_NAME"] = "oracle"
    return env


def _materialize_tasks(
    items: list[dict[str, Any]],
    task_dir: Path,
    *,
    rounds: int,
) -> dict[str, dict[str, Any]]:
    """Write one Harbor task directory per PR.

    Returns a map from task directory name to the row fields that harbor_runs needs.
    """
    adapter = FormulaCodeAdapter(harbor_tasks_root=task_dir, force=True)
    verifier_env = _build_verifier_env() or None

    # The container cannot read the overrides table, so expected_n goes in per task;
    # tasks without one skip the dilution_ratio check.
    overrides = fetch_overrides([
        (pr["owner"], pr["repo"], int(pr["issue_number"]))
        for pr in items
        if pr.get("owner") and pr.get("repo") and pr.get("issue_number") is not None
    ])
    if overrides:
        n_expected = sum(1 for row in overrides.values() if row.get("expected_n") is not None)
        logger.info("Loaded %d task override(s); %d declare expected_n", len(overrides), n_expected)

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
                expected_n=expected_n_for(overrides, (rec.owner, rec.repo, rec.issue_number)),
            )
        except Exception:
            logger.exception("generate_task failed for %s/%s#%d", rec.owner, rec.repo, rec.issue_number)
            continue
        task_id_map[rec.task_dir_name] = {
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
    """Build a Harbor JobConfig that runs the oracle agent on every task in ``task_dir``."""
    from harbor.models.environment_type import EnvironmentType
    from harbor.models.job.config import (
        JobConfig,
        LocalDatasetConfig,
        OrchestratorConfig,
    )
    from harbor.models.orchestrator_type import OrchestratorType
    from harbor.models.trial.config import AgentConfig, EnvironmentConfig

    # The 4 GB default is too small for lsv_init on large repos such as sklearn.
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
    """Build one harbor_runs row from a TrialResult, or None if no PR matches."""
    from harbor.models.trial.paths import TrialPaths

    task_name = trial.task_id.get_name()
    meta = task_id_map.get(task_name)
    if meta is None:
        logger.warning("Trial %s has no matching datasmith PR — skipping row", task_name)
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

    # Harbor can time out after a valid reward.json is written, so reward.json
    # decides the status and the exception is only kept for triage.
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

            # Most specific status first. Setup failures come first because they
            # cause every later failure.
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
                # lsv_init finished but found no coverage; not a crash.
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
                # Benchmarks ran but tests failed, so publish must not accept it.
                status = "tests_failed"
                error_message = error_message or "tests_passed=False in reward.json"

            # Keep the status from reward.json and append Harbor's exception.
            if harbor_exception and status != "success":
                suffix = f" (harbor_exception: {harbor_exception})"
                error_message = (error_message or "") + suffix
    else:
        # No reward.json: the verifier never finished, so use Harbor's exception.
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


def _publish_oracle_snapshots(trial: Any, owner: str, repo: str, issue_number: int) -> None:
    """Upload an oracle trial's ``.snapshots/`` baseline to the ``snapshots`` bucket.

    The key is ``{owner}__{repo}__{issue}/oracle.tar.gz``, without ``.pkl`` files;
    agent trials download it for correctness checks. Never raises.
    """
    import io
    import tarfile
    import urllib.error
    import urllib.request

    try:
        snap = _trial_dir_from_uri(trial.trial_uri) / "artifacts" / ".snapshots"
        if not (snap / "baseline.json").exists():
            return
        base = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")
        if not (base and key):
            return
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            tar.add(
                str(snap),
                arcname=".snapshots",
                filter=lambda i: None if i.name.endswith((".pkl", ".pkl.gz")) else i,
            )
        object_key = f"{owner}__{repo}__{issue_number}/oracle.tar.gz"
        url = f"{base}/storage/v1/object/snapshots/{object_key}"
        headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/gzip",
            "x-upsert": "true",
            "User-Agent": "Mozilla/5.0",
        }
        cid = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_ID")
        csec = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_SECRET")
        if cid and csec:
            headers["CF-Access-Client-Id"] = cid
            headers["CF-Access-Client-Secret"] = csec
        data = buf.getvalue()
        for method in ("POST", "PUT"):
            try:
                req = urllib.request.Request(url, data=data, headers=headers, method=method)  # noqa: S310
                with urllib.request.urlopen(req, timeout=180):  # noqa: S310
                    get_logger(__name__).info("Published oracle snapshots: %s", object_key)
                    return
            except urllib.error.HTTPError as e:
                if method == "POST" and e.code in (400, 409):
                    continue  # exists -> overwrite
                get_logger(__name__).warning(
                    "Snapshot publish failed for %s (%s)", object_key, e.code
                )
                return
    except Exception as exc:  # publishing must never stop the healthcheck
        get_logger(__name__).warning("Snapshot publish error: %s", exc)


def _keep_oracle_snapshots(trial: Any, snapshots_dir: Path) -> None:
    """Copy the trial's ``.snapshots/`` to ``<snapshots_dir>/<task>/`` when it has an entry that is not a skip."""
    import shutil

    snap = _trial_dir_from_uri(trial.trial_uri) / "artifacts" / ".snapshots"
    try:
        real = any(v != "skip" for v in json.loads((snap / "baseline.json").read_text())["entries"].values())
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return
    if real:
        shutil.copytree(snap, snapshots_dir / trial.task_id.get_name(), dirs_exist_ok=True)


def _insert_harbor_runs(rows: list[dict[str, Any]], chunk_size: int = 100) -> int:
    """Insert new rows into harbor_runs; each row is a new run.

    If a chunk fails, for example on a foreign key error for a deleted container
    row, retry row by row so only the bad rows are skipped.
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
    snapshots_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """Write tasks for *items*, run the oracle agent on them, and store one row per trial.

    ``snapshots_dir`` also keeps each oracle snapshot reference with a real entry in ``<snapshots_dir>/<task>/``.
    Returns the inserted row dicts.
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

    # JobResult does not include trial results, so read each trial's result.json.
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
            # Store the oracle baseline that agent trials check against.
            if row.get("agent_name") == "oracle":
                _publish_oracle_snapshots(
                    trial, row["owner"], row["repo"], row["issue_number"]
                )
                if snapshots_dir is not None:
                    _keep_oracle_snapshots(trial, snapshots_dir)

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
