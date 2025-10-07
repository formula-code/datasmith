from __future__ import annotations

import argparse
import contextlib
import logging
import shlex
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import docker
from docker.errors import APIError, ImageNotFound, NotFound
from docker.models.containers import Container

from datasmith.agents.utils import _TEST_SUITE_IMPORT_OVERRIDES
from datasmith.docker.context import BuildResult, ContextRegistry, DockerContext, Task

logger = logging.getLogger(__name__)

_err_lock = threading.Lock()


# ============================================================================
# Configuration and Result Classes
# ============================================================================


@dataclass
class ValidationConfig:
    """Configuration for Docker container validation."""

    output_dir: Path
    build_timeout: int
    run_timeout: int
    tail_chars: int
    profile_timeout: int = 600
    test_timeout: int = 900
    quick_timeout: int = 30


@dataclass
class ProfileValidationResult:
    """Result from profile validation."""

    ok: bool
    output: str
    duration_s: float
    error: str | None = None


@dataclass
class TestValidationResult:
    """Result from test validation."""

    ok: bool
    output: str
    duration_s: float
    suite_name: str
    error: str | None = None


@dataclass
class AcceptanceResult:
    """Overall acceptance validation result."""

    profile: ProfileValidationResult
    tests: TestValidationResult | None
    accepted: bool
    reason: str


# ============================================================================
# DockerValidator Class
# ============================================================================


class DockerValidator:
    """Object-oriented Docker container validator.

    This class encapsulates container validation logic with separate methods for:
    - Profile validation (ASV profiling checks)
    - Test validation (pytest suite checks)
    - Acceptance criteria (combined validation)
    """

    def __init__(
        self,
        client: docker.DockerClient,
        context_registry: ContextRegistry,
        machine_defaults: dict,
        config: ValidationConfig,
    ):
        """Initialize the validator.

        Args:
            client: Docker client instance
            context_registry: Registry of Docker contexts
            machine_defaults: Default machine configuration
            config: Validation configuration
        """
        self.client = client
        self.context_registry = context_registry
        self.machine_defaults = machine_defaults
        self.config = config
        self.error_lock = threading.Lock()

    def validate_profile(
        self, image_name: str, run_labels: dict[str, str] | None = None, timeout: int | None = None
    ) -> ProfileValidationResult:
        """Validate that profiling works in the container.

        Args:
            image_name: Name of the Docker image
            run_labels: Optional labels for the container run
            timeout: Optional timeout override

        Returns:
            ProfileValidationResult with validation outcome
        """
        if run_labels is None:
            run_labels = {}
        if timeout is None:
            timeout = self.config.profile_timeout

        container = None
        quick_s = self.config.quick_timeout
        start_time = time.time()

        try:
            cmd = [f'timeout -k 5 {quick_s}s /profile.sh /output/profile ""']
            logger.debug("profile:spawn cmd=%s", " ".join(cmd))

            container = self.client.containers.run(
                image=image_name,
                command=cmd,
                entrypoint=["/bin/bash", "-lc"],
                detach=True,
                labels=run_labels,
            )

            rc = container.wait(timeout=quick_s + 10).get("StatusCode", 1)
            logs = (container.logs() or b"").decode("utf-8", errors="replace").replace("\\n", "\n")
            duration = time.time() - start_time

            if rc == 124:
                logger.debug("profile:timeout rc=124 treated as success")
                return ProfileValidationResult(
                    ok=True,
                    output=_preview(logs, 4000),
                    duration_s=duration,
                )

            if rc != 0:
                logger.debug("profile:failed rc=%s stderr_tail: %s", rc, _preview(logs, 240))
                return ProfileValidationResult(
                    ok=False,
                    output=_preview(logs, 4000),
                    duration_s=duration,
                    error=f"Profile validation failed with rc={rc}",
                )

            return ProfileValidationResult(
                ok=True,
                output=_preview(logs, 4000),
                duration_s=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ProfileValidationResult(
                ok=False,
                output="",
                duration_s=duration,
                error=f"exception: {e}",
            )
        finally:
            with contextlib.suppress(Exception):
                if container is not None:
                    container.remove(force=True)

    def validate_tests(
        self,
        image_name: str,
        repo_name: str,
        run_labels: dict[str, str],
        timeout: int | None = None,
    ) -> TestValidationResult:
        """Validate that testing works in the container.

        Args:
            image_name: Name of the Docker image
            repo_name: Repository name
            run_labels: Labels for the container run
            timeout: Optional timeout override

        Returns:
            TestValidationResult with validation outcome
        """
        if timeout is None:
            timeout = self.config.test_timeout

        suite = self._resolve_test_suite_name(repo_name)
        container = None
        quick_s = self.config.quick_timeout
        start_time = time.time()

        try:
            cmdline = f"timeout -k 5 {quick_s}s /run_tests.sh"
            logger.debug("tests:spawn cmd=%s", cmdline)

            container = self.client.containers.run(
                image=image_name,
                command=[cmdline],
                entrypoint=["/bin/bash", "-lc"],
                detach=True,
                labels=run_labels,
            )

            rc = container.wait(timeout=quick_s + 10).get("StatusCode", 1)
            logs = (container.logs() or b"").decode("utf-8", errors="replace").replace("\\n", "\n")
            duration = time.time() - start_time

            if rc == 124:
                logger.debug("tests:timeout rc=124 treated as success")
                return TestValidationResult(
                    ok=True,
                    output=_preview(logs, 4000),
                    duration_s=duration,
                    suite_name=suite,
                )

            if rc != 0:
                logger.debug("tests:failed rc=%s stderr_tail: %s", rc, _preview(logs, 240))
                return TestValidationResult(
                    ok=False,
                    output=_preview(logs, 4000),
                    duration_s=duration,
                    suite_name=suite,
                    error=f"Test validation failed with rc={rc}",
                )

            return TestValidationResult(
                ok=True,
                output=_preview(logs, 4000),
                duration_s=duration,
                suite_name=suite,
            )

        except Exception as e:
            duration = time.time() - start_time
            return TestValidationResult(
                ok=False,
                output="",
                duration_s=duration,
                suite_name=suite,
                error=f"exception: {e}",
            )
        finally:
            with contextlib.suppress(Exception):
                if container is not None:
                    container.remove(force=True)

    def validate_acceptance(
        self,
        image_name: str,
        repo_name: str,
        run_labels: dict[str, str],
    ) -> AcceptanceResult:
        """Run full acceptance validation (profile + tests).

        Args:
            image_name: Name of the Docker image
            repo_name: Repository name
            run_labels: Labels for the container run

        Returns:
            AcceptanceResult with combined validation outcome
        """
        logger.info("validate_acceptance: validating image '%s'", image_name)

        # First, validate profile
        profile_result = self.validate_profile(image_name, run_labels)
        logger.debug("validate_acceptance: profile_ok=%s", profile_result.ok)

        # Only run tests if profile succeeded
        if not profile_result.ok:
            tests_result = None
            accepted = False
            reason = "Profile validation failed"
        else:
            tests_result = self.validate_tests(image_name, repo_name, run_labels)
            logger.debug("validate_acceptance: tests_ok=%s", tests_result.ok)
            accepted = tests_result.ok
            reason = "All validations passed" if accepted else "Test validation failed"

        return AcceptanceResult(
            profile=profile_result,
            tests=tests_result,
            accepted=accepted,
            reason=reason,
        )

    def validate_task(self, task: Task, run_labels: dict[str, str]) -> AcceptanceResult:
        """Validate a task (main entry point for scripts).

        Args:
            task: Task to validate

        Returns:
            Dictionary with validation results
        """
        # Create a fake args namespace from our config
        # args = argparse.Namespace(
        #     output_dir=self.config.output_dir,
        #     build_timeout=self.config.build_timeout,
        #     run_timeout=self.config.run_timeout,
        #     tail_chars=self.config.tail_chars,
        # )

        return self.validate_acceptance(
            image_name=task.get_image_name(),
            repo_name=f"{task.owner}/{task.repo}",
            run_labels=run_labels,
        )

        # Delegate to the existing validate_one function
        # This maintains backward compatibility while using the new class
        # return validate_one(
        #     task=task,
        #     args=args,
        #     client=self.client,
        #     context_registry=self.context_registry,
        #     machine_defaults=self.machine_defaults,
        # )

    def build_and_validate(
        self,
        task: Task,
        context: DockerContext,
        repo_url: str,
        sha: str,
        run_labels: dict[str, str],
        build_once_fn: Callable[..., BuildResult],
    ) -> BuildResult:
        """Build and validate a container.

        Args:
            task: Task to build and validate
            context: DockerContext with build configuration
            repo_url: Repository URL
            sha: Commit SHA
            run_labels: Labels for the container run
            build_once_fn: Function to call for building

        Returns:
            BuildResult with build and validation outcome
        """
        # Create args namespace from config
        args = argparse.Namespace(
            build_timeout=self.config.build_timeout,
            tail_chars=self.config.tail_chars,
        )

        # Delegate to the module-level build_and_validate function
        # This maintains backward compatibility
        return build_and_validate(
            client=self.client,
            task=task,
            context=context,
            repo_url=repo_url,
            sha=sha,
            run_labels=run_labels,
            args=args,
            build_once_fn=build_once_fn,
        )

    @staticmethod
    def _resolve_test_suite_name(repo_name: str) -> str:
        """Resolve the test suite name from repository name.

        Args:
            repo_name: Repository name

        Returns:
            Test suite import name
        """
        if repo_name in _TEST_SUITE_IMPORT_OVERRIDES:
            return _TEST_SUITE_IMPORT_OVERRIDES[repo_name]
        return repo_name.replace("-", "_")


# ============================================================================
# Helper Functions (Module-level)
# ============================================================================


def _preview(s: str, n: int = 160) -> str:
    """Return the last n characters of a string, replacing newlines with \\n."""
    bottom_s = n
    s = s or ""
    # s = s.replace("\n", "\\n")
    return s[-bottom_s:]


def _resolve_test_suite_name(repo_name: str) -> str:
    """
    Decide the TEST_SUITE argument for run_tests.sh, based on the repo key.
    Falls back to a conservative transform: hyphens → underscores.
    """
    if repo_name in _TEST_SUITE_IMPORT_OVERRIDES:
        return _TEST_SUITE_IMPORT_OVERRIDES[repo_name]
    # default heuristic: convert repo name to a plausible import name
    return repo_name.replace("-", "_")


def _run_quick_profile(
    client: docker.DockerClient,
    image_name: str,
    run_labels: dict[str, str] | None = None,
    timeout: int = 600,
) -> tuple[bool, str]:
    """Run a quick profiling sanity check using /profile.sh in the image."""
    if run_labels is None:
        run_labels = {}

    container = None
    quick_s = 45
    try:
        cmd = [
            f'timeout -k 5 {quick_s}s /profile.sh /output/profile ""',
        ]
        logger.debug("profile:spawn cmd=%s", " ".join(cmd))
        container = client.containers.run(
            image=image_name,
            command=cmd,
            entrypoint=["/bin/bash", "-lc"],
            detach=True,
            labels=run_labels,
        )
        rc = container.wait(timeout=quick_s + 10).get("StatusCode", 1)
        logs = (container.logs() or b"").decode("utf-8", errors="replace").replace("\\n", "\n")
        if rc == 124:
            logger.debug("profile:timeout rc=124 treated as success")
            return (True, _preview(logs, 4000))
        if rc != 0:
            logger.debug("profile:failed rc=%s stderr_tail: %s", rc, _preview(logs, 240))
        return (rc == 0, _preview(logs, 4000))
    except Exception as e:
        return (False, f"exception: {e}")
    finally:
        with contextlib.suppress(Exception):
            if container is not None:
                container.remove(force=True)


def _run_quick_tests(
    client: docker.DockerClient,
    image_name: str,
    repo_name: str,
    run_labels: dict[str, str],
    timeout: int = 900,
) -> tuple[bool, str]:
    """Run a quick test sanity check using /run_tests.sh in the image.
    Semantics preserved: feedparser install, 45s timeout, -q -k filter, rc==124 => success.
    """
    suite = _resolve_test_suite_name(repo_name)
    container = None
    quick_s = 45
    try:
        # Build the base command with default options
        base_options = "-q -k 'not slow and not network' --disable-warnings"
        if suite == "astropy":
            base_options += " --override-ini='addopts=--color=yes --maxfail=0'"

        # Add synthesized pytest options if provided
        cmdline = f"timeout -k 5 {quick_s}s /run_tests.sh /output/tests {shlex.quote(suite)} {base_options}"
        logger.debug("tests:spawn cmd=%s", cmdline)
        container = client.containers.run(
            image=image_name,
            command=[cmdline],
            entrypoint=["/bin/bash", "-lc"],
            detach=True,
            labels=run_labels,
        )
        rc = container.wait(timeout=quick_s + 10).get("StatusCode", 1)
        logs = (container.logs() or b"").decode("utf-8", errors="replace").replace("\\n", "\n")
        if rc == 124:
            logger.debug("tests:timeout rc=124 treated as success")
            return (True, _preview(logs, 4000))
        if rc != 0:
            logger.debug("tests:failed rc=%s stderr_tail: %s", rc, _preview(logs, 240))
        return (rc == 0, _preview(logs, 4000))
    except Exception as e:
        return (False, f"exception: {e}")
    finally:
        with contextlib.suppress(Exception):
            if container is not None:
                container.remove(force=True)


def build_and_validate(
    client: docker.DockerClient,
    task: Task,
    context: DockerContext,
    repo_url: str,
    sha: str,
    run_labels: dict[str, str],
    args: argparse.Namespace,
    *,
    build_once_fn: Callable[..., BuildResult],
) -> BuildResult:
    """
    Unified function that builds a container and validates it with profiling and testing.

    Returns a BuildResult that indicates success/failure of the entire process.
    If the build succeeds but profiling fails, the BuildResult will be marked as failed
    with error information from the profiling/testing phase.

    Args:
        client: Docker client instance
        task: Task to build and validate
        context: DockerContext with build configuration
        repo_url: Repository URL
        sha: Commit SHA
        run_labels: Labels for the container run
        args: Command-line arguments with build_timeout and tail_chars
        build_once_fn: Function to call for building (injected to avoid circular imports)
    """
    import time

    logger.info("build_and_validate: building image '%s'", task.get_image_name())
    logger.debug("build:start image=%s", task.get_image_name())
    _t_build_start = time.time()

    build_res = build_once_fn(
        client=client,
        task=task.with_tag("pkg"),
        context=context,
        repo_url=repo_url,
        sha=sha,
        timeout_s=args.build_timeout,
        tail_chars=args.tail_chars * 2,
        force=False,  # always rebuild to pick up new script
        run_labels=run_labels,
    )
    logger.debug(
        "build:done ok=%s rc=%s duration=%.1fs",
        build_res.ok,
        build_res.rc,
        time.time() - _t_build_start,
    )

    # If build failed, return the build result as-is
    if not build_res.ok:
        return build_res

    # Build succeeded, now validate with profiling and testing
    logger.info("build_and_validate: build ok; verifying profile+tests before recording attempt")
    logger.debug("profile:start")
    _t_profile_start = time.time()
    profile_ok, profile_preview = _run_quick_profile(
        client=client,
        image_name=task.with_tag("pkg").get_image_name(),
        run_labels=run_labels,
        timeout=min(args.build_timeout, 600),
    )
    logger.debug("profile:done ok=%s duration=%.1fs", profile_ok, time.time() - _t_profile_start)

    if not profile_ok:
        tests_ok = False
        tests_preview = "SKIPPED"
    else:
        logger.debug("tests:start")
        _t_tests_start = time.time()
        tests_ok, tests_preview = _run_quick_tests(
            client=client,
            image_name=task.with_tag("pkg").get_image_name(),
            repo_name=task.repo,
            run_labels=run_labels,
            timeout=min(args.build_timeout, 900),
        )
        logger.debug("tests:done ok=%s duration=%.1fs", tests_ok, time.time() - _t_tests_start)

    logger.warning(
        "build_and_validate: verification failed (profile_ok=%s, tests_ok=%s)",
        profile_ok,
        tests_ok,
    )
    if not profile_ok:
        combined_preview = []
        combined_preview.append(f"[profile_ok={profile_ok}] {profile_preview}")
        if tests_preview != "SKIPPED":
            combined_preview.append(f"[tests_ok={tests_ok}] {tests_preview}")
        preview_text = " | ".join(combined_preview)
        return BuildResult(
            ok=False,
            image_id=build_res.image_id,
            image_name=build_res.image_name,
            rc=1,
            duration_s=build_res.duration_s,
            stderr_tail=preview_text,
            stdout_tail="",
        )
    else:
        logger.info("build_and_validate: verification passed (profile and tests)")
        return build_res


def format_cmds(image_name: str, owner: str, repo: str, sha: str, out_dir: Path) -> tuple[str, str]:
    build_cmd = (
        f"docker build -t {shlex.quote(image_name)} src/datasmith/docker/ "
        f"--build-arg REPO_URL=https://www.github.com/{owner}/{repo} "
        f"--build-arg COMMIT_SHA={sha}"
    )
    run_cmd = (
        f"docker run --rm -v {shlex.quote(str((out_dir / 'results').absolute()))}:/output "
        f"{shlex.quote(image_name)} /profile.sh /output/profile "
    )
    return build_cmd, run_cmd


def append_error_line(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _err_lock, open(path, "a") as f:
        f.write(text.rstrip() + "\n")


def tail_chars(text: str | bytes, n: int) -> str:
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8", errors="replace")
        except Exception:
            if isinstance(text, bytes):
                text = text.decode("latin-1", errors="replace")
    return str((text or "")[-n:])


def wait_container_with_timeout(container: Container, timeout_s: int) -> tuple[int | None, bool]:
    """
    Wait for container to exit; on timeout, stop it. Returns (exit_code or None, timed_out).
    """
    code_box: dict[str, int | None] = {"code": None}
    done = threading.Event()

    def _wait() -> None:
        try:
            res = container.wait()  # blocking
            code_box["code"] = res.get("StatusCode")
        except Exception:
            code_box["code"] = None
        finally:
            done.set()

    t = threading.Thread(target=_wait, daemon=True)
    t.start()
    finished = done.wait(timeout=timeout_s)
    if finished:
        return code_box["code"], False

    # Timeout: stop the container
    with contextlib.suppress(Exception):
        container.stop(timeout=10)
    # Make a best-effort to fetch a code after stop
    try:
        res = container.wait(timeout=10)  # docker-py may ignore timeout; best effort
        return res.get("StatusCode"), True
    except Exception:
        return None, True


def _handle_build_error(
    task: Task,
    build_cmd: str,
    run_cmd: str,
    build_res: BuildResult,
    args: argparse.Namespace,
    image_name: str,
    build_stage: str,
) -> dict:
    msg = f"$ {build_cmd}\n$[build FAILED rc={build_res.rc} in {build_res.duration_s:.1f}s]"
    if build_res.stderr_tail:
        msg += f"\n---- build stderr tail ----\n{build_res.stderr_tail}"
    append_error_line(args.output_dir / "errors.txt", msg)
    logger.error(msg)
    return {
        "owner": task.owner,
        "repo": task.repo,
        "sha": task.sha,
        "image_name": image_name,
        "stage": build_stage,
        "ok": False,
        "rc": build_res.rc,
        "duration_s": build_res.duration_s,
        "cmd_build": build_cmd,
        "cmd_run": run_cmd,
        "stderr_tail": build_res.stderr_tail,
        "stdout_tail": build_res.stdout_tail,
        "files": [],
    }


def _handle_run_error(
    task: Task,
    build_cmd: str,
    run_cmd: str,
    rc: int,
    logs_tail: str,
    args: argparse.Namespace,
    image_name: str,
    run_stage: str,
    build_stage: str,
    files: dict[str, str],
) -> dict:
    msg = f"$ {build_cmd}\n$ {run_cmd}\n[run FAILED rc={rc} in (<= {args.run_timeout}s)]"
    if logs_tail:
        msg += f"\n---- run logs tail ----\n{logs_tail}"
    append_error_line(args.output_dir / "errors.txt", msg)
    logger.error(msg)
    return {
        "owner": task.owner,
        "repo": task.repo,
        "sha": task.sha,
        "image_name": image_name,
        "stage": f"{run_stage}+{build_stage}",
        "ok": False,
        "rc": rc,
        "duration_s": None,
        "cmd_build": build_cmd,
        "cmd_run": run_cmd,
        "stderr_tail": logs_tail,
        "stdout_tail": "",
        "files": files,
    }


def _handle_run_exception(
    task: Task, build_cmd: str, run_cmd: str, args: argparse.Namespace, image_name: str, build_stage: str
) -> dict:
    logger.exception("%s failed to run.", image_name)
    msg = f"$ {build_cmd}\n$ {run_cmd}\n[run FAILED: exception during start]"
    append_error_line(args.output_dir / "errors.txt", msg)
    return {
        "owner": task.owner,
        "repo": task.repo,
        "sha": task.sha,
        "image_name": image_name,
        "stage": f"run-exception+{build_stage}",
        "ok": False,
        "rc": 1,
        "duration_s": None,
        "cmd_build": build_cmd,
        "cmd_run": run_cmd,
        "stderr_tail": "",
        "stdout_tail": "",
        "files": [],
    }


def validate_one(  # noqa: C901
    task: Task,
    args: argparse.Namespace,
    client: docker.DockerClient,
    context_registry: ContextRegistry,
    machine_defaults: dict,
) -> dict:
    """
    Build via Docker SDK streaming (with timeout), then run container (with timeout).
    Emits errors immediately on failure (build or run).
    Returns a structured dict for JSONL summarization.
    """
    assert task.sha is not None, "Task.sha must be set"  # noqa: S101
    with contextlib.suppress(ImageNotFound, NotFound):
        if client.images.get(task.get_image_name()):
            # remove the image
            logger.debug("validate_one: image %s already exists, reusing image...", task.get_image_name())
            # client.images.remove(image=task.get_image_name(), force=True)
            # logger.debug("validate_one: removed image %s", task.get_image_name())
            return {
                "owner": task.owner,
                "repo": task.repo,
                "sha": task.sha,
                "image_name": task.get_image_name(),
                "stage": "build-skipped",
                "ok": True,
                "rc": 0,
                "duration_s": 0.0,
                "cmd_build": f"docker image {task.get_image_name()} (skipped)",
                "cmd_run": "",
                "stderr_tail": "",
                "stdout_tail": "",
                "files": {},
            }
    docker_ctx = context_registry.get(task)
    if docker_ctx == context_registry.get_default():
        _, docker_ctx = context_registry.get_similar(task)[0]

    build_cmd, run_cmd = format_cmds(task.get_image_name(), task.owner, task.repo, task.sha, args.output_dir)

    build_res: BuildResult = docker_ctx.build_container_streaming(
        client=client,
        image_name=task.get_image_name(),
        build_args={
            "REPO_URL": f"https://www.github.com/{task.owner}/{task.repo}",
            "COMMIT_SHA": task.sha,
            "ENV_PAYLOAD": task.env_payload or "",
            "PY_VERSION": task.python_version or "",
        },
        force=False,
        timeout_s=args.build_timeout,
        tail_chars=args.tail_chars,
        pull=True,
    )
    if build_res.rc == 124:
        build_stage = "build-timeout"
    elif build_res.rc != 0:
        build_stage = "build-failed"
    else:
        build_stage = "build-ok"

    if not build_res.ok:
        return _handle_build_error(task, build_cmd, run_cmd, build_res, args, task.get_image_name(), build_stage)

    machine_args = dict(machine_defaults)
    machine_args["machine"] = task.sha

    container = None
    files: dict[str, str] = {}
    try:
        profile_ok, profile_preview = _run_quick_profile(client=client, image_name=task.get_image_name(), timeout=600)
        if not profile_ok:
            logger.warning(
                "validate_one: failed container %s exited with code. Removing image.", task.get_container_name()
            )
            # remove the image if the container failed
            try:
                client.images.remove(image=task.get_image_name(), force=True)
            except ImageNotFound:
                logger.warning("validate_one: image %s not found when trying to remove it.", task.get_image_name())
            except APIError:
                logger.exception("validate_one: error removing image %s", task.get_image_name())

        run_stage = f"profile-{'ok' if profile_ok else 'failed'}"
        logs_tail = profile_preview
        ok = profile_ok and build_res.ok
        rc = 0 if profile_ok else 1

        return {
            "owner": task.owner,
            "repo": task.repo,
            "sha": task.sha,
            "image_name": task.get_image_name(),
            "stage": f"{run_stage}+{build_stage}",
            "ok": ok,
            "rc": rc,
            "duration_s": None,
            "cmd_build": build_cmd,
            "cmd_run": run_cmd,
            "stderr_tail": logs_tail,
            "stdout_tail": "",
            "files": files,
        }
    except Exception:
        return _handle_run_exception(task, build_cmd, run_cmd, args, task.get_image_name(), build_stage)
    finally:
        # best-effort cleanup
        try:
            if container:
                container.remove(force=True)
        except Exception:
            logger.exception("Failed to remove container for %s", task.get_image_name())
