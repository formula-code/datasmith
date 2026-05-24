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

import asyncio
import contextlib
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
from datasmith.utils.db import stable_hash

logger = get_logger("runners.harbor_healthcheck")

MIN_SPEEDUP_GATE = 1.05  # mirrored by publish/records.py

# ── Tunable constants (overridable via tokens.env per CLAUDE.md) ────────────
# Relative path inside each task's environment/ directory where cached LSV
# artifacts get staged. The Dockerfile's `COPY cache/ /opt/lsv/cache/`
# directive bakes this into the image so lsv_init.py can read from
# /opt/lsv/cache/ at setup-time (Harbor's verifier mount of /tests/ doesn't
# exist yet during setup.sh).
DATASMITH_LSV_CACHE_DIRNAME: str = os.environ.get("DATASMITH_LSV_CACHE_DIRNAME", "cache")
# Stable identifier for the docker host running stage 7 trials. Without an
# override, falls back to socket.gethostname() — fine on a single dev box,
# but a CI worker pool should set this explicitly so cache invalidation
# isn't keyed on whichever runner happened to pick up the job.
DATASMITH_DOCKER_HOST_ID: str = os.environ.get("DATASMITH_DOCKER_HOST_ID", socket.gethostname())
# Daytona machine class string baked into the resource_signature for daytona
# runs. The Harbor SDK doesn't expose a "current machine class" attribute, so
# we treat this as an operator-supplied tag.
DATASMITH_DAYTONA_MACHINE_CLASS: str = os.environ.get("DATASMITH_DAYTONA_MACHINE_CLASS", "default")
# Supabase Storage bucket holding per-PR `.lightspeed_deps.db` files.
DATASMITH_LSV_DEPS_BUCKET: str = os.environ.get("DATASMITH_LSV_DEPS_BUCKET", "lsv-deps")
# Supabase Storage bucket holding per-PR oracle snapshot tarballs (the runner
# uploads these after a successful daytona oracle trial; the URL is recorded
# at ``pull_requests.snapshot_storage_url``). Snapshots are produced by
# snapshot-tester (independent of LSV), so the bucket is named accordingly.
DATASMITH_SNAPSHOTS_BUCKET: str = os.environ.get("DATASMITH_SNAPSHOTS_BUCKET", "snapshots")
# Supabase Storage bucket holding per-trial run-artifacts tarballs
# (agent/, verifier/, artifacts/ — staged in-container then uploaded by the
# runner). URL recorded at ``harbor_runs.artifacts_storage_url``.
DATASMITH_RUNS_BUCKET: str = os.environ.get("DATASMITH_RUNS_BUCKET", "runs")
# Hard wall-clock cap (seconds) per Harbor trial — both verifier and agent
# phases. Default 12h. Bumped from 4h after observing legitimate multi-hour
# `lsv_measure` work on large benchmark suites (contourpy#368: 2894
# benchmarks x 2 rounds = 5+ hours just for the measurement pass). Note that
# Harbor's `_verify_with_retry` has @retry(stop_after_attempt(2), retry on
# VerifierTimeoutError), which doubles the effective per-trial cap to 24h —
# our `DATASMITH_HARBOR_JOB_TIMEOUT_S` wrapper below puts a real ceiling on
# that.
DATASMITH_HARBOR_TRIAL_TIMEOUT_S: float = float(os.environ.get("DATASMITH_HARBOR_TRIAL_TIMEOUT_S", "43200"))
# Hard wall-clock cap (seconds) for the entire `Job.run()` call. Above this
# the runner cancels the asyncio task, gives a brief cleanup grace, and
# falls through to harvest whatever per-trial result.json files made it to
# disk. Default = 1.1x trial timeout so a single retry by Harbor's verifier
# decorator gets terminated cleanly. n_concurrent_trials > 1 doesn't widen
# this — wall-clock is bounded by the slowest trial, not the sum.
DATASMITH_HARBOR_JOB_TIMEOUT_S: float = float(
    os.environ.get("DATASMITH_HARBOR_JOB_TIMEOUT_S", str(DATASMITH_HARBOR_TRIAL_TIMEOUT_S * 1.1))
)
# Memory cap (MB) per Harbor trial. Daytona accounts cap each sandbox at
# 8 GB (DaytonaAuthorizationError otherwise); the docker host typically has
# enough headroom to run lsv_init on large Python repos, so docker defaults
# to 32 GB. Tune via env vars when scaling up.
DATASMITH_HARBOR_TRIAL_MEMORY_MB_DAYTONA: int = int(os.environ.get("DATASMITH_HARBOR_TRIAL_MEMORY_MB_DAYTONA", "8192"))
DATASMITH_HARBOR_TRIAL_MEMORY_MB_DOCKER: int = int(os.environ.get("DATASMITH_HARBOR_TRIAL_MEMORY_MB_DOCKER", "32768"))
# CPU cap per Harbor trial. Daytona pins this exactly (cgroup quota). Docker
# trials inherit the same cgroup limit via docker-compose deploy.resources.
# Used both as task.toml's `cpus` value AND as `LSV_CPU_COUNT` in the cache
# key, so all trials within a machine_class hit the same baseline row.
DATASMITH_HARBOR_TRIAL_CPUS_DAYTONA: int = int(os.environ.get("DATASMITH_HARBOR_TRIAL_CPUS_DAYTONA", "2"))
DATASMITH_HARBOR_TRIAL_CPUS_DOCKER: int = int(os.environ.get("DATASMITH_HARBOR_TRIAL_CPUS_DOCKER", "2"))


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


def _build_base_verifier_env() -> dict[str, str]:
    """Emit datasmith-Supabase creds for the in-container cache writeback path.

    ``lsv_cache_writeback.py`` and ``parser.py:fetch_oracle_benchmarks``
    both target datasmith's Supabase (``pull_requests``, ``harbor_runs``,
    ``lsv_baseline_cache``). The legacy Harbor-Supabase upload path
    (``upload.py``) was removed — see migration 00016 + the harbor_adapter
    refactor — so we no longer forward ``HARBOR_SUPABASE_*`` here.

    Service-role key bypasses RLS, which is required for upserts.
    ``db.formulacode.org`` is gated by Cloudflare Access, so we also forward
    the service-token headers when set (see CLAUDE.md remote-access).

    The ``HARBOR_AGENT_NAME`` env var is no longer set here — the adapter
    bakes it into setup.sh and test.sh's render_env at task-materialization
    time, sourced from ``FormulaCodeRecord.harbor_agent_name``.
    """
    env: dict[str, str] = {}
    ds_url = os.environ.get("SUPABASE_URL")
    ds_key = os.environ.get("SUPABASE_KEY")
    if ds_url:
        env["DATASMITH_SUPABASE_URL"] = ds_url
    if ds_key:
        env["DATASMITH_SUPABASE_SERVICE_KEY"] = ds_key
    for k in ("DATASMITH_CF_ACCESS_CLIENT_ID", "DATASMITH_CF_ACCESS_CLIENT_SECRET"):
        v = os.environ.get(k)
        if v:
            env[k] = v
    return env


def _resolve_image_digest(container_name: str | None) -> str:
    """Best-effort resolve the registry digest for ``container_name``.

    Returns the digest string (``sha256:…``) on success, or the literal
    ``container_name`` (or ``""``) on failure. The digest is purely an input
    to ``resource_signature`` — failure to resolve just makes the signature
    coarser, not wrong, so we never raise.
    """
    if not container_name:
        return ""
    # ``docker manifest inspect`` works against the local docker daemon's
    # registry credentials and prints JSON containing per-arch digests; for
    # signature stability we just hash the whole blob.
    try:
        out = subprocess.run(
            ["docker", "manifest", "inspect", container_name],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if out.returncode == 0 and out.stdout.strip():
            return f"manifest:{stable_hash(out.stdout.strip())[:16]}"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return container_name


def _compute_resource_attrs(
    *,
    use_daytona: bool,
    container_name: str | None,
    image_digest: str,
    cpus: int,
    memory_mb: int,
) -> dict[str, Any]:
    """Build the runner-supplied portion of the ``lsv_baseline_cache`` key.

    All seven values come from the runner — what we *ask* Harbor to pin via
    task.toml. cpu_count + mem_bytes deliberately mirror the cgroup limits
    we configure (not what /proc inside the sandbox reports — empirically
    /proc shows the underlying Daytona host's full hardware, not the
    sandbox's pinned 2/8). Identical reasoning for docker, where the
    runner is the host so it knows what it pinned.

    The runner ships these via setup.sh's render_env so lsv_init.py sees
    identical values inside the container. lsv_init.py adds an eighth
    attr — ``detected_cpu_model`` — read from /proc/cpuinfo inside the
    sandbox, which captures the physical-host SKU Daytona scheduled this
    trial onto. We deliberately do not compute that here because the
    runner has no visibility into which physical host Daytona will pick.
    """
    return {
        "env": "daytona" if use_daytona else "docker",
        "container_name": container_name or "",
        "image_digest": image_digest,
        "machine_class": DATASMITH_DAYTONA_MACHINE_CLASS if use_daytona else "",
        "docker_host_id": "" if use_daytona else DATASMITH_DOCKER_HOST_ID,
        "cpu_count": cpus,
        "mem_bytes": memory_mb * 1024 * 1024,
    }


def _fetch_pr_deps_db_url(
    client: Any,
    *,
    owner: str,
    repo: str,
    issue_number: int,
) -> str | None:
    """Look up the cached deps DB Storage URL for this PR.

    The deps DB lives on ``pull_requests.lsv_deps_db_url`` and is
    resource-independent (it's the asv-derived test-to-source-file map),
    so the runner can pre-stage it via the Dockerfile's ``COPY cache/``
    directive. Baseline timings have moved to an in-container fetch
    (lsv_init.py queries lsv_baseline_cache once cpu_count and mem_bytes
    are known from inside the sandbox).
    """
    try:
        resp = (
            client.table("pull_requests")
            .select("lsv_deps_db_url")
            .eq("owner", owner)
            .eq("repo", repo)
            .eq("issue_number", issue_number)
            .limit(1)
            .execute()
        )
        if resp.data:
            url: str | None = resp.data[0].get("lsv_deps_db_url") or None
            return url
    except Exception as exc:
        logger.warning(
            "pull_requests.lsv_deps_db_url lookup failed for %s/%s#%s: %s",
            owner,
            repo,
            issue_number,
            exc,
        )
    return None


def _stage_lsv_deps_db(
    client: Any,
    cache_dir: Path,
    *,
    deps_db_url: str | None,
) -> bool:
    """Drop the cached deps DB into ``cache_dir`` (resource-independent).

    The Dockerfile's ``COPY cache/ /opt/lsv/cache/`` then bakes it into
    the image at build time. Returns whether the file was staged.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    if not deps_db_url:
        return False
    try:
        blob = client.storage.from_(DATASMITH_LSV_DEPS_BUCKET).download(deps_db_url)
        (cache_dir / "lightspeed_deps.db").write_bytes(blob)
        return True
    except Exception as exc:
        logger.warning("LSV deps DB download failed (key=%s): %s", deps_db_url, exc)
        return False


def _materialize_tasks(
    items: list[dict[str, Any]],
    task_dir: Path,
    *,
    rounds: int,
    use_daytona: bool,
) -> dict[str, dict[str, Any]]:
    """Write one Harbor task directory per PR. Returns a mapping from
    ``task_id`` (the directory name Harbor sees) back to the datasmith row
    metadata we need when inserting harbor_runs.

    Per-task work also stages any cached LSV deps DB / baselines into
    ``<task>/environment/cache/`` so lsv_init can short-circuit
    ``initialize_diffcheck``, and bakes the resource signature into the
    verifier env so the in-container writeback knows which cache row to
    upsert when the cache misses.
    """
    adapter = FormulaCodeAdapter(harbor_tasks_root=task_dir, force=True)
    base_env = _build_base_verifier_env()
    client = get_client()

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

        owner = pr["owner"]
        repo = pr["repo"]
        issue_number = pr["issue_number"]
        container_name = pr.get("container_name")

        image_digest = _resolve_image_digest(container_name)
        # Per-task resource pinning. Defaults uniform per env; future per-task
        # overrides (e.g. apache/arrow needing 8 cpus / 32 GB) can branch on
        # owner/repo/issue here without breaking the cache-key invariant —
        # different attrs deliberately produce a different cache row.
        cpus = DATASMITH_HARBOR_TRIAL_CPUS_DAYTONA if use_daytona else DATASMITH_HARBOR_TRIAL_CPUS_DOCKER
        memory_mb = DATASMITH_HARBOR_TRIAL_MEMORY_MB_DAYTONA if use_daytona else DATASMITH_HARBOR_TRIAL_MEMORY_MB_DOCKER
        attrs = _compute_resource_attrs(
            use_daytona=use_daytona,
            container_name=container_name,
            image_digest=image_digest,
            cpus=cpus,
            memory_mb=memory_mb,
        )
        deps_db_url = _fetch_pr_deps_db_url(
            client,
            owner=owner,
            repo=repo,
            issue_number=issue_number,
        )

        # Per-task verifier_env: base creds + full cache identity. We thread
        # cpu_count/mem_bytes through here too so lsv_init.py and
        # lsv_cache_writeback.py read identical values via setup.sh / test.sh
        # render_env (Harbor's [verifier.env] does not reach setup.sh's
        # process env on daytona — see render_env workaround below).
        verifier_env = dict(base_env)
        verifier_env["LSV_TASK_ID"] = rec.task_id
        verifier_env["LSV_DEPS_BUCKET"] = DATASMITH_LSV_DEPS_BUCKET
        verifier_env["LSV_ENV"] = attrs["env"]
        verifier_env["LSV_CONTAINER_NAME"] = attrs["container_name"]
        verifier_env["LSV_IMAGE_DIGEST"] = attrs["image_digest"]
        verifier_env["LSV_MACHINE_CLASS"] = attrs["machine_class"]
        verifier_env["LSV_DOCKER_HOST_ID"] = attrs["docker_host_id"]
        verifier_env["LSV_CPU_COUNT"] = str(attrs["cpu_count"])
        verifier_env["LSV_MEM_BYTES"] = str(attrs["mem_bytes"])

        # Vars that test.sh + setup.sh actually need at runtime. We inline
        # these as `export` lines in the rendered scripts because Harbor's
        # daytona exec does not propagate task.toml [verifier.env] to the
        # process env (verified empirically — even HARBOR_AGENT_NAME and
        # SUPABASE_URL come through unset). The verifier_env above stays in
        # task.toml for any Harbor-internal consumer that does honor it.
        # Filter to non-empty values so empty docker-only fields on daytona
        # runs don't pollute the rendered script with `export X=''` lines.
        test_render_env = {k: v for k, v in verifier_env.items() if v}
        # setup.sh runs lsv_init.py, which needs the cache identity + the
        # datasmith Supabase creds to query lsv_baseline_cache. Same dict;
        # rendered at task-generation time.
        setup_render_env = dict(test_render_env)

        try:
            adapter.generate_task(
                rec,
                run_pytest=True,
                rounds=rounds,
                cpus=cpus,
                memory=f"{memory_mb}M",
                verifier_env=verifier_env,
                test_render_env=test_render_env,
                setup_render_env=setup_render_env,
                timeout_sec=DATASMITH_HARBOR_TRIAL_TIMEOUT_S,
            )
        except Exception:
            logger.exception("generate_task failed for %s/%s#%d", rec.owner, rec.repo, rec.issue_number)
            continue

        # Stage cached files AFTER generate_task creates the directory tree.
        # The adapter pre-creates an empty environment/cache/ (so the Dockerfile
        # COPY directive always has something to copy); we drop the deps DB
        # into that same directory. Baselines are NOT pre-staged — they're
        # resource-keyed and the runner doesn't know the sandbox specs;
        # lsv_init.py fetches them at runtime via PostgREST.
        cache_dir = task_dir / rec.task_id / "environment" / DATASMITH_LSV_CACHE_DIRNAME
        deps_staged = _stage_lsv_deps_db(client, cache_dir, deps_db_url=deps_db_url)
        if deps_staged:
            logger.info("LSV cache stage hit for %s: deps_db", rec.task_id)

        task_id_map[rec.task_id] = {
            "owner": owner,
            "repo": repo,
            "sha": pr["merge_commit_sha"],
            "issue_number": issue_number,
            "container_name": container_name,
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
    # Daytona caps each sandbox at 8 GB on standard accounts (any larger
    # request fails fast with DaytonaAuthorizationError), so the two
    # environments need different headroom — tune via the
    # DATASMITH_HARBOR_TRIAL_MEMORY_MB_* env vars.
    if use_daytona:
        environment = EnvironmentConfig(
            type=EnvironmentType.DAYTONA,
            force_build=True,
            delete=True,
            override_memory_mb=DATASMITH_HARBOR_TRIAL_MEMORY_MB_DAYTONA,
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
            override_memory_mb=DATASMITH_HARBOR_TRIAL_MEMORY_MB_DOCKER,
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

    # Compute pytest_success_ratio from the in-container reward payload, if
    # the verifier ran pytest. parser.py writes the structured counters under
    # the top-level ``"pytest"`` key (parser.summarize_pytest); fields:
    # total / passed / failed / skipped / error.
    pytest_success_ratio: float | None = None
    if reward_payload:
        pytest_block = reward_payload.get("pytest") or {}
        try:
            total = int(pytest_block.get("total", 0) or 0)
            failed = int(pytest_block.get("failed", 0) or 0)
            err = int(pytest_block.get("error", 0) or 0)
            if total > 0:
                pytest_success_ratio = (total - failed - err) / total
        except (TypeError, ValueError):
            pytest_success_ratio = None

    return {
        # Generate run_id client-side so we can update pull_requests.baseline_run_id
        # without re-fetching after insert. The schema's gen_random_uuid() default
        # is preserved as a fallback for any caller that doesn't pre-set this.
        "run_id": str(uuid.uuid4()),
        "owner": meta["owner"],
        "repo": meta["repo"],
        "sha": meta["sha"],
        "issue_number": meta["issue_number"],
        "container_name": meta["container_name"],
        "environment": environment,
        "agent_name": "oracle",
        # Match the legacy `runs` schema where the oracle is its own model.
        "model_name": "oracle",
        "model_agent_signature": "oracle:oracle",
        "status": status,
        "max_speedup": max_speedup,
        "geomean_speedup": geomean,
        "n_benchmarks": n_benchmarks,
        "wallclock_sec": wallclock,
        "reward_payload": reward_payload,
        "pytest_success_ratio": pytest_success_ratio,
        "error_message": error_message,
    }


def _upload_run_artifacts_for_row(
    client: Any,
    row: dict[str, Any],
    *,
    job_dir: Path,
) -> str | None:
    """If this trial produced a `verifier/run_artifacts.tar.gz` (staged by
    test.sh), upload it to the ``runs`` Storage bucket and return the object
    key. Caller writes the key to ``harbor_runs.artifacts_storage_url``.

    Returns ``None`` on miss / failure — never raises. Same plumbing as
    ``_upload_snapshot_for_row``. The tarball staging happens in test.sh
    after parser.py runs; if test.sh never reached that point (e.g.
    lsv_init_failed), no tarball exists and we skip cleanly."""
    task_id = f"{row['owner']}_{row['repo']}_{row['issue_number']}"
    candidate = job_dir / task_id / "verifier" / "run_artifacts.tar.gz"
    if not candidate.exists() or candidate.stat().st_size == 0:
        return None

    # Object key includes run_id so re-runs get new keys (vs. snapshot upload
    # which is per-PR; snapshots are oracle-deterministic, run artifacts are
    # not — preserving each run's logs is the whole point).
    run_id = row.get("run_id") or uuid.uuid4().hex
    object_key = f"{row['owner']}__{row['repo']}__{row['issue_number']}/{run_id}.tar.gz"
    try:
        blob = candidate.read_bytes()
        client.storage.from_(DATASMITH_RUNS_BUCKET).upload(
            path=object_key,
            file=blob,
            file_options={"content-type": "application/gzip", "upsert": "true"},
        )
        logger.info(
            "Uploaded run artifacts for %s/%s#%s (%d bytes) → %s/%s",
            row["owner"],
            row["repo"],
            row["issue_number"],
            len(blob),
            DATASMITH_RUNS_BUCKET,
            object_key,
        )
        return object_key
    except Exception as exc:
        logger.warning(
            "Run-artifacts upload failed for %s/%s#%s: %s",
            row["owner"],
            row["repo"],
            row["issue_number"],
            exc,
        )
        return None


def _upload_snapshot_for_row(
    client: Any,
    row: dict[str, Any],
    *,
    job_dir: Path,
) -> str | None:
    """If this trial produced a `verifier/oracle_snapshots.tar.gz`, upload it
    to Supabase Storage and return the object key (caller then writes it to
    ``pull_requests.snapshot_storage_url``).

    Returns ``None`` on miss / failure — never raises. The runner-side path
    only works if Harbor exfiltrated the verifier dir (the trial's mounted
    /logs/verifier/). When sandbox setup fails before test.sh runs (common
    when the upstream image isn't published yet), no tarball gets written
    and we just skip."""
    task_id = f"{row['owner']}_{row['repo']}_{row['issue_number']}"
    candidate = job_dir / task_id / "verifier" / "oracle_snapshots.tar.gz"
    if not candidate.exists() or candidate.stat().st_size == 0:
        return None

    object_key = f"{row['owner']}__{row['repo']}__{row['issue_number']}/oracle.tar.gz"
    try:
        blob = candidate.read_bytes()
        # supabase-py's storage.from_(bucket).upload doesn't support upsert
        # by default; pass file_options to overwrite an existing object so a
        # re-run of the same PR replaces the prior tarball.
        client.storage.from_(DATASMITH_SNAPSHOTS_BUCKET).upload(
            path=object_key,
            file=blob,
            file_options={"content-type": "application/gzip", "upsert": "true"},
        )
        logger.info(
            "Uploaded oracle snapshots for %s/%s#%s (%d bytes) → %s/%s",
            row["owner"],
            row["repo"],
            row["issue_number"],
            len(blob),
            DATASMITH_SNAPSHOTS_BUCKET,
            object_key,
        )
        return object_key
    except Exception as exc:
        logger.warning(
            "Snapshot upload failed for %s/%s#%s: %s",
            row["owner"],
            row["repo"],
            row["issue_number"],
            exc,
        )
        return None


def _update_baseline_pointers(
    rows: list[dict[str, Any]],
    *,
    environment: str,
    job_dir: Path,
) -> None:
    """For every successful daytona oracle run, point its PR's
    ``pull_requests.baseline_run_id`` at this run, and (best-effort) upload
    the snapshot tarball + populate ``snapshot_storage_url``.

    Docker rows are intentionally *not* eligible — the publish gate (stage 8)
    runs against daytona only, and we don't want a flaky local docker
    iteration to overwrite a leaderboard pointer.
    """
    if environment != "daytona":
        return
    successful = [r for r in rows if r.get("status") == "success" and r.get("run_id")]
    if not successful:
        return

    client = get_client()
    targets: list[dict[str, Any]] = []
    for r in successful:
        target: dict[str, Any] = {
            "owner": r["owner"],
            "repo": r["repo"],
            "issue_number": r["issue_number"],
            "baseline_run_id": r["run_id"],
        }
        snapshot_key = _upload_snapshot_for_row(client, r, job_dir=job_dir)
        if snapshot_key:
            target["snapshot_storage_url"] = snapshot_key
        targets.append(target)

    # ``upsert`` with the natural PK does a partial update because PostgREST
    # treats absent columns as "do not modify" — exactly what we want.
    try:
        client.table("pull_requests").upsert(targets, on_conflict="owner,repo,issue_number").execute()
        snap_n = sum(1 for t in targets if "snapshot_storage_url" in t)
        logger.info(
            "Updated pull_requests for %d oracle runs (%d with snapshot URL)",
            len(targets),
            snap_n,
        )
    except Exception as exc:
        logger.warning("Baseline pointer upsert failed: %s", exc)


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

    # Wrap Job.run() with a hard wall-clock cap. Harbor's per-trial verifier
    # has its own asyncio.wait_for(timeout=verifier_timeout_sec), but
    # `_verify_with_retry` is decorated `@retry(stop_after_attempt(2),
    # retry_if_exception_type(VerifierTimeoutError))`, doubling the effective
    # cap to 2x per-trial. There is no upstream lever for that retry, so we
    # bound the whole thing here. asyncio.shield prevents the inner task from
    # being cancelled until we actually decide to cancel it (otherwise
    # wait_for would re-raise CancelledError synchronously without giving
    # trials a chance to write result.json). After cancellation we give a
    # short cleanup grace so per-trial _cleanup_and_finalize blocks can run.
    job_task = asyncio.create_task(Job(config).run())
    timed_out = False
    try:
        await asyncio.wait_for(asyncio.shield(job_task), timeout=DATASMITH_HARBOR_JOB_TIMEOUT_S)
    except TimeoutError:
        # Not logger.exception: the TimeoutError carries no useful traceback
        # (it's raised by wait_for itself, not the underlying task), and the
        # message + job_name + cap are the actionable signal for operators.
        logger.error(  # noqa: TRY400
            "Harbor job '%s' exceeded wall-clock cap %.0fs; cancelling and harvesting partial results",
            job_name,
            DATASMITH_HARBOR_JOB_TIMEOUT_S,
        )
        job_task.cancel()
        with contextlib.suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(job_task, timeout=300.0)
        timed_out = True

    # Harbor's Job.run() returns a JobResult whose `trial_results` field is
    # never populated (job.py:380-435 — only the stats summary is bubbled up;
    # per-trial TrialResult objects are written to per-trial result.json files
    # on disk and otherwise dropped). Walk those files ourselves — completed
    # trials still produce rows even when the job hit the wall-clock cap.
    from harbor.models.trial.result import TrialResult

    rows: list[dict[str, Any]] = []
    job_dir = config.jobs_dir / job_name
    trial_result_paths = sorted(job_dir.glob("*/result.json"))
    logger.info("Walking %d per-trial result.json files under %s", len(trial_result_paths), job_dir)
    artifacts_client = get_client()  # service-role; cheap singleton inside this fn
    for path in trial_result_paths:
        try:
            trial = TrialResult.model_validate_json(path.read_text())
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", path, exc)
            continue
        row = _row_from_trial(trial, task_id_map, environment=environment)
        if row is not None:
            artifacts_key = _upload_run_artifacts_for_row(artifacts_client, row, job_dir=job_dir)
            if artifacts_key:
                row["artifacts_storage_url"] = artifacts_key
            rows.append(row)

    n_success = sum(1 for r in rows if r["status"] == "success")
    n_fast = sum(
        1
        for r in rows
        if r["status"] == "success" and r["max_speedup"] is not None and r["max_speedup"] >= MIN_SPEEDUP_GATE
    )
    logger.info(
        "Harbor job '%s' done: %d/%d trials succeeded, %d >= %.2fx gate%s",
        job_name,
        n_success,
        len(rows),
        n_fast,
        MIN_SPEEDUP_GATE,
        " (job timed out)" if timed_out else "",
    )

    _insert_harbor_runs(rows)
    if timed_out:
        # Half-cancelled jobs can produce technically-successful trials that
        # we don't trust enough to promote as the per-PR baseline pointer.
        # Skip both the snapshot upload and the pull_requests pointer update
        # — the harbor_runs rows are still recorded for triage.
        logger.warning(
            "Harbor job '%s' timed out; skipping baseline pointer + snapshot upload",
            job_name,
        )
    else:
        _update_baseline_pointers(rows, environment=environment, job_dir=job_dir)
    return rows
