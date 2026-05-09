from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2, rmtree
from typing import Any

from datasmith.harbor_adapter.utils import (
    render_dockerfile,
    render_instruction_md,
    render_run_setup_sh,
    render_solution_sh,
    render_task_toml,
    render_test_sh,
)


def _resolve_image_digest(container_name: str | None) -> str:
    """Best-effort resolve a stable identifier for ``container_name``.

    Returns a ``manifest:<hash>`` string on success, the literal
    ``container_name`` if ``docker manifest inspect`` is unavailable or the
    image isn't pushed yet, or ``""`` if no name was supplied. The result
    is part of the ``lsv_baseline_cache`` PK; failure to resolve just makes
    the key coarser, never wrong, so we never raise. Was previously
    ``runners.harbor_healthcheck._resolve_image_digest`` — moved here so a
    direct ``adapter.generate_task()`` caller (SkyRL, harbor CLI) gets the
    same behavior the fc-data runner produces, without duplicating the
    docker shell-out.
    """
    if not container_name:
        return ""
    try:
        out = subprocess.run(
            ["docker", "manifest", "inspect", container_name],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return container_name
    if out.returncode != 0 or not out.stdout.strip():
        return container_name
    digest = hashlib.sha256(out.stdout.strip().encode()).hexdigest()[:16]
    return f"manifest:{digest}"


@dataclass
class FormulaCodeRecord:
    container_name: str  # repo_name-{base_sha}:final
    patch: str  # diff(base_sha, merge_sha)
    task_id: str  # repo_name_{ID}
    gt_hash: str  # merge_sha
    base_commit: str  # base_sha
    instructions: str
    date: str | None = None  # merge_date
    classification: str | None = None  # classification
    difficulty: str = "hard"  # difficulty level
    repo_name: str | None = None
    # Harbor agent identity baked into setup.sh and exported in test.sh.
    # ``"oracle"`` runs CAPTURE the LSV deps DB + baselines + snapshot
    # tarball; any other value is an agent-evaluation run that CONSUMES
    # the cached oracle artifacts (pre-staged by the caller into
    # ``environment/cache/`` before image build).
    harbor_agent_name: str = "oracle"


@dataclass
class OracleArtifacts:
    """Pre-fetched oracle cache contents for an agent-evaluation run.

    Both fields are *resource-independent*: the deps DB is the asv
    test-to-source-file map (same on every machine), and the snapshots
    tarball holds the pickled benchmark/lexer state at the patched commit.
    The caller downloads these from datasmith Supabase and hands them to
    ``generate_task`` / ``_stage_oracle_cache``, which writes them into the
    task's ``environment/cache/`` directory so the Dockerfile's
    ``COPY cache`` bakes them into the image.

    Baselines, by contrast, are *resource-keyed* (per cpu_count + mem_bytes
    + machine_class + docker_host_id) and can't be pre-staged at image-build
    time. ``lsv_init.py`` fetches them at runtime via PostgREST against
    ``lsv_baseline_cache`` using the cache identity the runner pinned and
    inlined into setup.sh's render_env (LSV_CPU_COUNT / LSV_MEM_BYTES are
    what we *asked Harbor for*, since /proc inside the sandbox shows host
    specs not cgroup limits — see harbor_healthcheck:_resource_limits).
    """

    deps_db_bytes: bytes
    snapshots_tarball_bytes: bytes


class HarborTaskPaths:
    """Convenience paths for writing a Harbor task."""

    def __init__(self, task_dir: Path) -> None:
        self.task_dir = Path(task_dir)
        self.environment_dir = self.task_dir / "environment"
        self.tests_dir = self.task_dir / "tests"
        self.solution_dir = self.task_dir / "solution"

        self.instruction_path = self.task_dir / "instruction.md"
        self.config_path = self.task_dir / "task.toml"

        # Create subdirectories
        self.environment_dir.mkdir(parents=True, exist_ok=True)
        self.tests_dir.mkdir(parents=True, exist_ok=True)
        self.solution_dir.mkdir(parents=True, exist_ok=True)


class FormulaCodeAdapter:
    def __init__(self, harbor_tasks_root: Path, force: bool = False) -> None:
        self.out_root = Path(harbor_tasks_root)
        self.out_root.mkdir(parents=True, exist_ok=True)
        self.template_dir = Path(__file__).parent / "template"

        # Remove any existing tasks if force=True
        if force:
            for sub in self.out_root.iterdir():
                if sub.is_dir():
                    rmtree(sub)

    def _copy_template_files(self, paths: HarborTaskPaths) -> None:
        """Copy static template files to appropriate directories.

        Harbor's trial runner uploads only ``tests/setup.sh`` alone to
        ``/tmp/setup.sh`` and runs it BEFORE uploading the rest of
        ``tests/`` via the verifier (see harbor/trial/trial.py:225 and
        verifier/verifier.py:86). That means any helper script setup.sh
        calls must already exist inside the container when setup runs.

        We work around that by copying the LSV helper scripts into
        ``environment/`` instead, so the Dockerfile ``COPY`` directive
        bakes them into the image at build time. setup.sh and test.sh then
        invoke them from ``/opt/lsv/`` inside the container.
        """
        for name in [
            "entrypoint.sh",
            "lsv_init.py",
            "lsv_measure.py",
            "parser.py",
            "pytest_runner.py",
            "jinja_patch_plugin_pandas.py",
            "lsv_cache_writeback.py",
        ]:
            copy2(self.template_dir / name, paths.environment_dir / name)

        # Pre-create the cache directory with a sentinel file. The Dockerfile's
        # `COPY cache /opt/lsv/cache` directive needs *something* in the dir
        # for Daytona's build-context uploader (which tars + ships the dir
        # tree) to include it — empty dirs get dropped, then the COPY fails
        # with "file not found in build context". The runner overwrites this
        # dir with deps_db / baselines on cache hit before Job.run(); on miss
        # only `.gitkeep` is present and lsv_init.py falls through to a full
        # initialize_diffcheck.
        cache_dir = paths.environment_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / ".gitkeep").write_text("")

    def _write_instruction_md(self, rec: FormulaCodeRecord, paths: HarborTaskPaths) -> None:
        """Generate instruction.md file."""
        instruction_content = render_instruction_md(rec.instructions)
        paths.instruction_path.write_text(instruction_content)

    def _write_task_toml(
        self,
        rec: FormulaCodeRecord,
        paths: HarborTaskPaths,
        timeout_sec: float = 21600.0,
        cpus: int = 2,
        memory: str = "4G",
        storage: str = "10G",
        verifier_env: dict[str, str] | None = None,
    ) -> None:
        """Generate task.toml configuration."""
        toml_content = render_task_toml(
            difficulty=rec.difficulty,
            category=rec.classification or "optimization",
            tags=["optimization", "formulacode", "asv", "benchmarking"],
            timeout_sec=timeout_sec,
            cpus=cpus,
            memory=memory,
            storage=storage,
            verifier_env=verifier_env,
        )
        paths.config_path.write_text(toml_content)

    def _write_environment_files(self, rec: FormulaCodeRecord, paths: HarborTaskPaths) -> None:
        """Generate environment files (Dockerfile, scripts)."""
        # Dockerfile
        base_img = rec.container_name or "python:3.11-slim"
        dockerfile_content = render_dockerfile(base_img)
        (paths.environment_dir / "Dockerfile").write_text(dockerfile_content)

    def _write_test_files(
        self,
        rec: FormulaCodeRecord,
        paths: HarborTaskPaths,
        run_pytest: bool = True,
        rounds: int = 1,
        test_render_env: dict[str, str] | None = None,
    ) -> None:
        """Generate test files (test.sh, config.json)."""
        # Inject HARBOR_AGENT_NAME so test.sh's render_env loop exports it for
        # in-container scripts (lsv_cache_writeback.py reads it to gate the
        # baseline writeback). Caller may override by passing it explicitly.
        merged_env = dict(test_render_env or {})
        merged_env.setdefault("HARBOR_AGENT_NAME", rec.harbor_agent_name)

        # test.sh
        test_sh_content = render_test_sh(
            base_commit=rec.base_commit,
            task_id=rec.task_id,
            run_pytest=run_pytest,
            rounds=rounds,
            render_env=merged_env,
        )
        test_sh_path = paths.tests_dir / "test.sh"
        test_sh_path.write_text(test_sh_content)
        test_sh_path.chmod(0o755)

        # tests/config.json — Harbor verifier mounts this at /tests/config.json
        # at trial time (post-setup). lsv_init.py reads it from /tests/config.json
        # FIRST during setup.sh, before the verifier mount exists, so we ALSO
        # bake the same content into environment/config.json — the Dockerfile
        # COPYs it to /tests/config.json at image build time. The two paths end
        # up with identical content; Harbor's later upload_dir() either overwrites
        # with the same bytes or merges harmlessly.
        cfg = rec.__dict__.copy()
        cfg_json = json.dumps(cfg, indent=2)
        (paths.tests_dir / "config.json").write_text(cfg_json)
        (paths.environment_dir / "config.json").write_text(cfg_json)

    def _write_solution_files(
        self,
        rec: FormulaCodeRecord,
        paths: HarborTaskPaths,
        rounds: int = 1,
        setup_render_env: dict[str, str] | None = None,
    ) -> None:
        """Generate solution files (solve.sh, setup.sh)."""
        # solve.sh
        solve_sh_content = render_solution_sh(rec.patch)
        solve_sh_path = paths.solution_dir / "solve.sh"
        solve_sh_path.write_text(solve_sh_content)
        solve_sh_path.chmod(0o755)

        # setup.sh
        extra_setup_commands = ""
        setup_sh_content = render_run_setup_sh(
            task_id=rec.task_id,
            rounds=rounds,
            extra_setup_commands=extra_setup_commands,
            harbor_agent_name=rec.harbor_agent_name,
            render_env=setup_render_env,
        )
        setup_sh_path = paths.tests_dir / "setup.sh"
        setup_sh_path.write_text(setup_sh_content)
        setup_sh_path.chmod(0o755)

    def _stage_oracle_cache(self, paths: HarborTaskPaths, artifacts: OracleArtifacts) -> None:
        """Drop oracle's deps DB and snapshot files into ``cache/``.

        The Dockerfile's ``COPY cache /opt/lsv/cache`` directive bakes the
        directory into the image at build time. ``lsv_init.py`` then loads
        the deps DB into the LightspeedSession and copies the snapshot
        files into ``SNAPSHOT_DIR`` so ``snapshot-tool verify`` can diff the
        agent's behavior against the oracle baseline. Baselines are NOT
        pre-staged here — they're resource-keyed and lsv_init.py fetches
        them at runtime via PostgREST against ``lsv_baseline_cache`` using
        the cache identity (cpu_count, mem_bytes, etc.) the runner pinned
        and rendered into setup.sh's env.
        """
        cache_dir = paths.environment_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        (cache_dir / "lightspeed_deps.db").write_bytes(artifacts.deps_db_bytes)

        snapshots_dir = cache_dir / "snapshots"
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(artifacts.snapshots_tarball_bytes), mode="r:gz") as tar:
            # filter="data" rejects path-traversal entries, devices, and
            # other dangerous member types — Python 3.12+ default behavior.
            tar.extractall(snapshots_dir, filter="data")

    def generate_task(
        self,
        rec: FormulaCodeRecord,
        run_pytest: bool = True,
        timeout_sec: float = 21600.0,
        cpus: int = 2,
        memory: str = "4G",
        storage: str = "10G",
        rounds: int = 1,
        verifier_env: dict[str, str] | None = None,
        test_render_env: dict[str, str] | None = None,
        setup_render_env: dict[str, str] | None = None,
        oracle_artifacts: OracleArtifacts | None = None,
    ) -> Path:
        """Generate a complete Harbor task directory for the given FormulaCodeRecord.

        ``oracle_artifacts``, when provided, pre-stages the oracle's deps DB,
        baselines JSON, and snapshot tarball into ``environment/cache/`` so
        the resulting image short-circuits the LSV survey + snapshot-capture
        passes. Required for non-oracle agent runs; ignored (but harmless)
        for oracle runs.

        The adapter auto-populates four keys into the render_env dicts that
        the in-sandbox parser/lsv_init/lsv_cache_writeback rely on:
          * ``LSV_IMAGE_DIGEST`` — resolved via ``docker manifest inspect``
          * ``LSV_SHA``         — from ``rec.gt_hash`` (the merge_commit_sha)
          * ``HARBOR_AGENT_NAME`` — from ``rec.harbor_agent_name``
          * ``LSV_AGENT_NAME``  — same as HARBOR_AGENT_NAME (parser writes
            this into ``harbor_runs.agent_name``)
        Caller-supplied keys win on conflict — the runner can still override
        e.g. LSV_IMAGE_DIGEST when it has a stable docker_host_id-derived
        identifier handy. SkyRL / direct ``harbor trials start`` callers
        get the auto-populated values for free.
        """
        out_dir = self.out_root / rec.task_id
        out_dir.mkdir(parents=True, exist_ok=True)

        paths = HarborTaskPaths(out_dir)

        self._copy_template_files(paths)

        if oracle_artifacts is not None:
            self._stage_oracle_cache(paths, oracle_artifacts)

        adapter_defaults = {
            "LSV_IMAGE_DIGEST": _resolve_image_digest(rec.container_name),
            "LSV_SHA": rec.gt_hash,
            "HARBOR_AGENT_NAME": rec.harbor_agent_name,
            "LSV_AGENT_NAME": rec.harbor_agent_name,
        }

        def _merge_defaults(supplied: dict[str, str] | None) -> dict[str, str]:
            merged = {k: v for k, v in adapter_defaults.items() if v}
            if supplied:
                merged.update(supplied)  # caller wins
            return merged

        merged_verifier_env = _merge_defaults(verifier_env)
        merged_test_render_env = _merge_defaults(test_render_env)
        merged_setup_render_env = _merge_defaults(setup_render_env)

        self._write_instruction_md(rec, paths)
        self._write_task_toml(
            rec,
            paths,
            timeout_sec,
            cpus,
            memory,
            storage,
            verifier_env=merged_verifier_env,
        )
        self._write_environment_files(rec, paths)
        self._write_test_files(
            rec,
            paths,
            run_pytest,
            rounds,
            test_render_env=merged_test_render_env,
        )
        self._write_solution_files(
            rec,
            paths,
            rounds,
            setup_render_env=merged_setup_render_env,
        )

        return out_dir


# ============================================================================
# Caller-side helpers for fetching oracle artifacts from datasmith Supabase.
# Stdlib-only (urllib + tarfile) so the adapter package is portable to
# Harbor's tree without dragging supabase-py along.
# ============================================================================


def _datasmith_headers(
    service_key: str,
    cf_access_id: str | None = None,
    cf_access_secret: str | None = None,
) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {service_key}",
        "apikey": service_key,
        "Accept": "application/json",
        "User-Agent": "datasmith-harbor-adapter/1.0",
    }
    if cf_access_id and cf_access_secret:
        headers["CF-Access-Client-Id"] = cf_access_id
        headers["CF-Access-Client-Secret"] = cf_access_secret
    return headers


def _check_http_url(url: str) -> None:
    if not url.startswith(("https://", "http://")):
        raise ValueError(f"refusing to open non-http(s) URL: {url!r}")


def _http_get_json(url: str, headers: dict[str, str], timeout: float = 30.0) -> Any:
    _check_http_url(url)
    req = urllib.request.Request(url, headers=headers, method="GET")  # noqa: S310
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        return json.loads(resp.read().decode())


def _http_get_bytes(url: str, headers: dict[str, str], timeout: float = 60.0) -> bytes:
    _check_http_url(url)
    req = urllib.request.Request(url, headers=headers, method="GET")  # noqa: S310
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        data: bytes = resp.read()
        return data


def fetch_oracle_artifacts(
    *,
    owner: str,
    repo: str,
    issue_number: int,
    datasmith_supabase_url: str,
    datasmith_supabase_service_key: str,
    cf_access_id: str | None = None,
    cf_access_secret: str | None = None,
    deps_bucket: str = "lsv-deps",
    snapshots_bucket: str = "snapshots",
) -> OracleArtifacts | None:
    """Pull oracle's resource-independent cached artifacts (deps DB + snapshot tarball).

    Returns ``None`` if either piece is missing — the caller decides whether
    to fall back to a full oracle run or fail-fast. Baselines are fetched
    by ``lsv_init.py`` from inside the sandbox using cache-key attrs the
    runner injects via render_env (``LSV_CPU_COUNT``/``LSV_MEM_BYTES``
    derived from what we ask Harbor to pin via task.toml), so the cache key
    matches what the runner pinned rather than what /proc reports.
    """
    headers = _datasmith_headers(datasmith_supabase_service_key, cf_access_id, cf_access_secret)

    pr_url = (
        f"{datasmith_supabase_url}/rest/v1/pull_requests"
        f"?select=lsv_deps_db_url,snapshot_storage_url"
        f"&owner=eq.{urllib.parse.quote(owner)}"
        f"&repo=eq.{urllib.parse.quote(repo)}"
        f"&issue_number=eq.{issue_number}"
    )
    try:
        pr_rows = _http_get_json(pr_url, headers)
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
        return None
    if not pr_rows:
        return None
    deps_db_key = pr_rows[0].get("lsv_deps_db_url")
    snapshot_key = pr_rows[0].get("snapshot_storage_url")
    if not deps_db_key or not snapshot_key:
        return None

    deps_url = f"{datasmith_supabase_url}/storage/v1/object/{deps_bucket}/{deps_db_key}"
    snap_url = f"{datasmith_supabase_url}/storage/v1/object/{snapshots_bucket}/{snapshot_key}"
    try:
        deps_bytes = _http_get_bytes(deps_url, headers)
        snap_bytes = _http_get_bytes(snap_url, headers)
    except (urllib.error.URLError, urllib.error.HTTPError):
        return None

    return OracleArtifacts(
        deps_db_bytes=deps_bytes,
        snapshots_tarball_bytes=snap_bytes,
    )
