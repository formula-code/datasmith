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
import socket
import subprocess
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from datasmith.harbor_adapter import FormulaCodeAdapter, to_record
from datasmith.utils import get_client, get_logger
from datasmith.utils.overrides import expected_n_for, fetch_overrides

logger = get_logger("runners.harbor_healthcheck")

MIN_SPEEDUP_GATE = 1.05  # mirrored by publish/records.py

# ── LSV baseline cache knobs ────────────────────────────────────────────────
# Master switch. Off => the runner injects no cache creds/key, so lsv_init falls
# back to force=True and behaves exactly as before the cache existed.
DATASMITH_LSV_CACHE_ENABLED: bool = os.environ.get("DATASMITH_LSV_CACHE_ENABLED", "1").strip().lower() not in (
    "0",
    "false",
    "no",
)

# Cache-key fields for where a trial ran. docker keys on the host id (baselines
# don't transfer between dev hosts); daytona keys on the machine_class (plus the
# in-sandbox detected_cpu_model, since one class spans several EPYC SKUs).
DATASMITH_DOCKER_HOST_ID: str = os.environ.get("DATASMITH_DOCKER_HOST_ID", socket.gethostname())
DATASMITH_DAYTONA_MACHINE_CLASS: str = os.environ.get("DATASMITH_DAYTONA_MACHINE_CLASS", "default")

# Trial cgroup pins. These are BOTH written to the trial (task.toml cpus +
# EnvironmentConfig override_memory_mb) AND recorded as the cache-key
# cpu_count/mem_bytes, so the key describes the hardware the trial actually got.
# Memory defaults to 32 GB on both environments -- matching the pre-cache
# hardcoded pin (lsv_init OOMs large repos like sklearn under 4 GB); lower it
# deliberately per environment if needed.
DATASMITH_HARBOR_TRIAL_CPUS_DOCKER: int = int(os.environ.get("DATASMITH_HARBOR_TRIAL_CPUS_DOCKER", "2"))
DATASMITH_HARBOR_TRIAL_CPUS_DAYTONA: int = int(os.environ.get("DATASMITH_HARBOR_TRIAL_CPUS_DAYTONA", "2"))
DATASMITH_HARBOR_TRIAL_MEMORY_MB_DOCKER: int = int(os.environ.get("DATASMITH_HARBOR_TRIAL_MEMORY_MB_DOCKER", "32768"))
DATASMITH_HARBOR_TRIAL_MEMORY_MB_DAYTONA: int = int(os.environ.get("DATASMITH_HARBOR_TRIAL_MEMORY_MB_DAYTONA", "32768"))


def _trial_pins(use_daytona: bool) -> tuple[int, int]:
    """(cpu_count, mem_mb) the runner pins for a trial in this environment. The
    single authority for both the actual pin and the cache key, so they cannot
    drift apart."""
    if use_daytona:
        return DATASMITH_HARBOR_TRIAL_CPUS_DAYTONA, DATASMITH_HARBOR_TRIAL_MEMORY_MB_DAYTONA
    return DATASMITH_HARBOR_TRIAL_CPUS_DOCKER, DATASMITH_HARBOR_TRIAL_MEMORY_MB_DOCKER


def _patch_harbor_trial_name() -> None:
    """Suppress the 7-char random suffix Harbor appends to every trial_name.

    Harbor's ``TrialConfig.set_default_trial_name`` generates
    ``f"{task_name[:32]}__{ShortUUID().random(length=7)}"`` (see
    ``harbor/models/trial/config.py``), which makes trial directories
    non-deterministic across runs and defeats simple re-triage. We want the
    directory under ``jobs/<job_name>/`` to be exactly the task name
    (``owner__repo__issue_number``) so a second run of the same task lands
    in the same path. Override at import time — only affects this process.
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


def _build_base_verifier_env() -> dict[str, str]:
    """Creds for datasmith's OWN Supabase (the LSV cache's home), prefixed
    ``DATASMITH_`` so they never collide with the Harbor ``SUPABASE_*`` names the
    container already carries for upload.py.

    Returns ``{}`` when the service key is absent, which disables the cache. The
    URL must be reachable from inside the trial container: a Daytona (or remote
    docker) sandbox cannot reach a ``127.0.0.1`` instance, so the cache only
    works when ``SUPABASE_URL`` is the ``db.formulacode.org`` tunnel (see
    CLAUDE.md remote-access); a localhost URL simply yields cache misses.
    """
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        return {}
    env = {
        "DATASMITH_SUPABASE_URL": url,
        "DATASMITH_SUPABASE_SERVICE_KEY": key,
    }
    for k in ("DATASMITH_CF_ACCESS_CLIENT_ID", "DATASMITH_CF_ACCESS_CLIENT_SECRET"):
        v = os.environ.get(k)
        if v:
            env[k] = v
    return env


def _resolve_image_digest(container_name: str | None) -> str:
    """Best-effort stable digest for the task image, so a rebuild under the same
    name (different deps -> different timings) does not reuse a stale baseline.
    Falls back to the container_name (still stable per image); never raises."""
    if not container_name:
        return ""
    try:
        out = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", container_name],
            capture_output=True,
            text=True,
            timeout=30,
        )
        digest = (out.stdout or "").strip()
        if "@" in digest:
            return "manifest:" + digest.split("@", 1)[1][:32]
    except Exception as exc:
        logger.debug("image digest lookup failed for %s (%s); using container_name", container_name, exc)
    return container_name


def _compute_resource_attrs(
    *,
    use_daytona: bool,
    container_name: str | None,
    image_digest: str,
    cpu_count: int,
    mem_mb: int,
) -> dict[str, str]:
    """The runner-supplied portion of the lsv_baseline_cache key -- every column
    except ``detected_cpu_model``, which lsv_init reads from /proc inside the
    sandbox. Values are strings for baking into shell exports; the in-container
    code coerces cpu_count/mem_bytes back to int."""
    return {
        "env": "daytona" if use_daytona else "docker",
        "container_name": container_name or "",
        "image_digest": image_digest,
        "machine_class": DATASMITH_DAYTONA_MACHINE_CLASS if use_daytona else "",
        "docker_host_id": "" if use_daytona else DATASMITH_DOCKER_HOST_ID,
        "cpu_count": str(cpu_count),
        "mem_bytes": str(mem_mb * 1024 * 1024),
    }


def _decode_bytea(value: Any) -> bytes | None:
    """Decode a PostgREST bytea payload (PostgreSQL hex form ``\\x...``) to raw
    bytes. Returns None on anything unexpected -- the caller degrades to a miss."""
    if not isinstance(value, str) or not value.startswith("\\x"):
        return None
    try:
        return bytes.fromhex(value[2:])
    except ValueError:
        return None


def _fetch_deps_db(owner: str, repo: str, issue_number: int) -> bytes | None:
    """Fetch the pre-surveyed LSV deps DB for a task from ``lsv_deps_cache`` on
    datasmith's own Supabase, or None on miss / any error.

    Host-side over ``get_client()`` (not the in-container urllib path): the survey
    is resource-independent, so it is fetched once here and baked into the image,
    which works even when SUPABASE_URL is localhost (the container's baseline
    fetch still needs the tunnel, but that is a separate layer). Best-effort:
    every failure returns None and the trial re-runs the survey under force=True.
    """
    try:
        resp = (
            get_client()
            .table("lsv_deps_cache")
            .select("deps_db")
            .eq("owner", owner)
            .eq("repo", repo)
            .eq("issue_number", issue_number)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        logger.debug("lsv_deps_cache lookup failed for %s/%s#%d (%s)", owner, repo, issue_number, exc)
        return None
    rows = resp.data or []
    if not rows or not isinstance(rows[0], dict):
        return None
    return _decode_bytea(rows[0].get("deps_db"))


def _lsv_render_env(base_env: dict[str, str], *, task_id: str, attrs: dict[str, str]) -> dict[str, str]:
    """Combine the datasmith creds with the LSV_* cache key into the dict baked
    into setup.sh/test.sh. lsv_init reads LSV_* + creds to look baselines up;
    lsv_cache_writeback reads them to upsert."""
    return {
        **base_env,
        "LSV_TASK_ID": task_id,
        "LSV_ENV": attrs["env"],
        "LSV_CONTAINER_NAME": attrs["container_name"],
        "LSV_IMAGE_DIGEST": attrs["image_digest"],
        "LSV_MACHINE_CLASS": attrs["machine_class"],
        "LSV_DOCKER_HOST_ID": attrs["docker_host_id"],
        "LSV_CPU_COUNT": attrs["cpu_count"],
        "LSV_MEM_BYTES": attrs["mem_bytes"],
    }


def _materialize_tasks(
    items: list[dict[str, Any]],
    task_dir: Path,
    *,
    rounds: int,
    use_daytona: bool,
) -> dict[str, dict[str, Any]]:
    """Write one Harbor task directory per PR. Returns a mapping from the
    Harbor task directory name back to the datasmith row metadata we need
    when inserting harbor_runs."""
    adapter = FormulaCodeAdapter(harbor_tasks_root=task_dir, force=True)
    verifier_env = _build_verifier_env() or None

    # LSV baseline cache: on only when enabled AND datasmith creds are present.
    # cpu_count/mem_mb are the pins the trial gets (see _build_job_config) and
    # double as the cache-key hardware fields.
    base_env = _build_base_verifier_env()
    cache_on = DATASMITH_LSV_CACHE_ENABLED and bool(base_env)
    cpu_count, mem_mb = _trial_pins(use_daytona)
    if cache_on:
        logger.info("LSV baseline cache enabled (env=%s)", "daytona" if use_daytona else "docker")
    elif DATASMITH_LSV_CACHE_ENABLED:
        logger.info("LSV baseline cache idle: SUPABASE_URL/SUPABASE_KEY not set")

    # Per-task operator declarations. expected_n is the producer for the
    # dilution_ratio invariant; the trial container cannot read the table
    # itself (RLS-locked, no anon grant), so it is injected per task via
    # [verifier.env]. Tasks without a declaration get None and the invariant
    # skips, which is the common case.
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
        render_env: dict[str, str] | None = None
        deps_db: bytes | None = None
        if cache_on:
            attrs = _compute_resource_attrs(
                use_daytona=use_daytona,
                container_name=pr.get("container_name"),
                image_digest=_resolve_image_digest(pr.get("container_name")),
                cpu_count=cpu_count,
                mem_mb=mem_mb,
            )
            render_env = _lsv_render_env(base_env, task_id=rec.task_dir_name, attrs=attrs)
            deps_db = _fetch_deps_db(rec.owner, rec.repo, rec.issue_number)
            if deps_db:
                logger.info(
                    "LSV survey cache hit for %s/%s#%d (%d bytes)",
                    rec.owner,
                    rec.repo,
                    rec.issue_number,
                    len(deps_db),
                )
        try:
            adapter.generate_task(
                rec,
                run_pytest=True,
                rounds=rounds,
                cpus=cpu_count,
                memory=f"{mem_mb}M",
                verifier_env=verifier_env,
                expected_n=expected_n_for(overrides, (rec.owner, rec.repo, rec.issue_number)),
                render_env=render_env,
                deps_db=deps_db,
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
    # dep-graph walk alone exceeds 4 GB and gets OOM-killed with exit 137. The
    # pin comes from _trial_pins so it stays identical to the cache-key
    # mem_bytes; the default is still 32 GB.
    _, MEMORY_MB = _trial_pins(use_daytona)
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

    task_id_map = _materialize_tasks(items, task_dir, rounds=rounds, use_daytona=use_daytona)
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
