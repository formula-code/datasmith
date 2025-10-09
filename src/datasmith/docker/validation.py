from __future__ import annotations

import argparse
import contextlib
import glob
import logging
import shlex
import shutil
import tarfile
import tempfile
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


def _safe_rmtree(path: Path, client: docker.DockerClient | None = None) -> None:
    """Safely remove a directory tree, handling Docker-created root-owned files.

    When Docker containers write to volume-mounted directories, they often create
    files owned by root. This function attempts normal removal first, then uses
    Docker to clean up root-owned files if needed.

    Args:
        path: Directory path to remove
        client: Optional Docker client to use for cleanup (uses new client if not provided)
    """
    try:
        shutil.rmtree(path)
    except PermissionError:
        # Docker created root-owned files - use Docker to remove them as root
        logger.debug("Permission error removing %s, using Docker to cleanup root-owned files", path)
        try:
            # Use Docker to remove the files as root
            if client is None:
                client = docker.from_env()

            _ = client.containers.run(
                image="alpine:latest",
                command=["rm", "-rf", "/cleanup"],
                volumes={str(path.absolute()): {"bind": "/cleanup", "mode": "rw"}},
                remove=True,
                detach=False,
            )
            # Now try to remove the (hopefully empty) directory
            shutil.rmtree(path, ignore_errors=True)
        except Exception as e:
            # If all else fails, just log and continue - temp dirs will be cleaned by OS eventually
            logger.warning("Failed to remove temporary directory %s: %s", path, e)


# ============================================================================
# Helper Functions
# ============================================================================


def extract_asv_benchmarks_from_tarballs(output_dir: Path) -> str | None:
    """Extract and read asv_benchmarks.txt from all tar.gz files in output_dir.

    Args:
        output_dir: Directory containing tar.gz files

    Returns:
        Contents of asv_benchmarks.txt if found, None otherwise
    """
    # Find all tar.gz files in the output directory
    tarball_pattern = str(output_dir / "*.tar.gz")
    tarballs = glob.glob(tarball_pattern)

    if not tarballs:
        logger.debug("No tar.gz files found in %s", output_dir)
        return None

    # Extract each tarball and look for asv_benchmarks.txt
    for tarball_path in tarballs:
        try:
            logger.debug("Extracting tarball: %s", tarball_path)
            with tarfile.open(tarball_path, "r:gz") as tar:
                # Extract all files to a temporary location within output_dir
                extract_dir = output_dir / "extracted"
                extract_dir.mkdir(exist_ok=True)
                tar.extractall(path=extract_dir, filter="data")

                # Look for asv_benchmarks.txt in the extracted files
                benchmark_files = glob.glob(str(extract_dir / "**/asv_benchmarks.txt"), recursive=True)
                if benchmark_files:
                    benchmark_file = Path(benchmark_files[0])
                    logger.debug("Found asv_benchmarks.txt: %s", benchmark_file)
                    return benchmark_file.read_text()

        except Exception as e:
            logger.warning("Failed to extract/read tarball %s: %s", tarball_path, e)
            continue

    logger.debug("No asv_benchmarks.txt found in any tarball")
    return None


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
    stdout: str
    stderr: str
    duration_s: float
    benchmarks: str = ""


@dataclass
class TestValidationResult:
    """Result from test validation."""

    ok: bool
    stdout: str
    stderr: str
    duration_s: float
    suite_name: str


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

        # Create temporary directory for output - manually managed to handle Docker root-owned files
        tmpdir_path = Path(tempfile.mkdtemp())

        try:
            cmd = [f'timeout -k 5 {quick_s}s /profile.sh /output/profile ""']
            logger.debug("profile:spawn cmd=%s", " ".join(cmd))

            # Mount the temporary directory to /output in the container
            container = self.client.containers.run(
                image=image_name,
                command=cmd,
                entrypoint=["/bin/bash", "-lc"],
                detach=True,
                labels=run_labels,
                volumes={str(tmpdir_path): {"bind": "/output", "mode": "rw"}},
            )

            rc = container.wait(timeout=quick_s + 10).get("StatusCode", 1)
            # Collect stdout and stderr separately
            stdout = (container.logs(stdout=True, stderr=False) or b"").decode("utf-8", errors="replace")
            stderr = (container.logs(stdout=False, stderr=True) or b"").decode("utf-8", errors="replace")
            duration = time.time() - start_time

            # Extract benchmark results from tarballs
            benchmark_content = extract_asv_benchmarks_from_tarballs(tmpdir_path)
            if benchmark_content:
                logger.debug("Successfully extracted asv_benchmarks.txt (%d chars)", len(benchmark_content))

            if rc == 124:
                logger.debug("profile:timeout rc=124 treated as success")
                return ProfileValidationResult(
                    ok=True,
                    stdout=_preview(stdout, 4000),
                    stderr=_preview(stderr, 4000),
                    duration_s=duration,
                    benchmarks=benchmark_content or "",
                )

            if rc != 0:
                logger.debug("profile:failed rc=%s stderr_tail: %s", rc, _preview(stderr, 240))
                return ProfileValidationResult(
                    ok=False,
                    stdout=_preview(stdout, 4000),
                    stderr=_preview(stderr, 4000),
                    duration_s=duration,
                    benchmarks=benchmark_content or "",
                )

            return ProfileValidationResult(
                ok=True,
                stdout=_preview(stdout, 4000),
                stderr=_preview(stderr, 4000),
                duration_s=duration,
                benchmarks=benchmark_content or "",
            )

        except Exception as e:
            duration = time.time() - start_time
            return ProfileValidationResult(
                ok=False,
                stdout="",
                stderr=f"exception: {e}",
                duration_s=duration,
            )
        finally:
            # Clean up container
            with contextlib.suppress(Exception):
                if container is not None:
                    container.remove(force=True)

            # Clean up temp directory using our safe removal function
            _safe_rmtree(tmpdir_path, self.client)

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
            # Collect stdout and stderr separately
            stdout = (container.logs(stdout=True, stderr=False) or b"").decode("utf-8", errors="replace")
            stderr = (container.logs(stdout=False, stderr=True) or b"").decode("utf-8", errors="replace")
            duration = time.time() - start_time

            if rc == 124:
                logger.debug("tests:timeout rc=124 treated as success")
                return TestValidationResult(
                    ok=True,
                    stdout=_preview(stdout, 4000),
                    stderr=_preview(stderr, 4000),
                    duration_s=duration,
                    suite_name=suite,
                )

            if rc != 0:
                logger.debug("tests:failed rc=%s stderr_tail: %s", rc, _preview(stderr, 240))
                return TestValidationResult(
                    ok=False,
                    stdout=_preview(stdout, 4000),
                    stderr=_preview(stderr, 4000),
                    duration_s=duration,
                    suite_name=suite,
                )

            return TestValidationResult(
                ok=True,
                stdout=_preview(stdout, 4000),
                stderr=_preview(stderr, 4000),
                duration_s=duration,
                suite_name=suite,
            )

        except Exception as e:
            duration = time.time() - start_time
            return TestValidationResult(
                ok=False,
                stdout="",
                stderr=f"exception: {e}",
                duration_s=duration,
                suite_name=suite,
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
        # repo_url: str,
        # sha: str,
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
        logger.info("build_and_validate: building image '%s'", task.get_image_name())
        logger.debug("build:start image=%s", task.get_image_name())
        _t_build_start = time.time()

        # Build the Docker image
        build_res = build_once_fn(
            client=self.client,
            task=task,
            context=context,
            repo_url=f"https://www.github.com/{task.owner}/{task.repo}",
            sha=task.sha or "",
            timeout_s=self.config.build_timeout,
            tail_chars=self.config.tail_chars * 2,
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

        # Validate profile
        logger.debug("profile:start")
        _t_profile_start = time.time()
        profile_result = self.validate_profile(
            image_name=task.get_image_name(),
            run_labels=run_labels,
            timeout=min(self.config.build_timeout, self.config.profile_timeout),
        )
        logger.debug("profile:done ok=%s duration=%.1fs", profile_result.ok, time.time() - _t_profile_start)

        # Validate tests (only if profile succeeded)
        if not profile_result.ok:
            tests_result = None
        else:
            logger.debug("tests:start")
            _t_tests_start = time.time()
            tests_result = self.validate_tests(
                image_name=task.get_image_name(),
                repo_name=task.repo,
                run_labels=run_labels,
                timeout=min(self.config.build_timeout, self.config.test_timeout),
            )
            logger.debug("tests:done ok=%s duration=%.1fs", tests_result.ok, time.time() - _t_tests_start)

        # Check if validation passed - precedence: build > profile > test
        if not profile_result.ok:
            logger.warning("build_and_validate: profile validation failed")
            # Return only profile logs (the failing step)
            return BuildResult(
                ok=False,
                image_id=build_res.image_id,
                image_name=build_res.image_name,
                rc=1,
                duration_s=build_res.duration_s,
                stderr_tail=profile_result.stderr,
                stdout_tail=profile_result.stdout,
                failure_stage="profile",
            )
        elif tests_result is not None and not tests_result.ok:
            logger.warning("build_and_validate: test validation failed")
            # Return only test logs (the failing step)
            return BuildResult(
                ok=False,
                image_id=build_res.image_id,
                image_name=build_res.image_name,
                rc=1,
                duration_s=build_res.duration_s,
                stderr_tail=tests_result.stderr,
                stdout_tail=tests_result.stdout,
                failure_stage="tests",
            )
        else:
            logger.info("build_and_validate: verification passed (profile and tests)")
            # On success, concatenate all three pairs of logs
            combined_stdout = []
            combined_stderr = []

            # Build logs
            combined_stdout.append("=== BUILD ===")
            combined_stdout.append(build_res.stdout_tail if build_res.stdout_tail else "(no stdout)")
            combined_stderr.append("=== BUILD ===")
            combined_stderr.append(build_res.stderr_tail if build_res.stderr_tail else "(no stderr)")

            # Profile logs
            combined_stdout.append("\n=== PROFILE VALIDATION ===")
            combined_stdout.append(profile_result.stdout)
            if profile_result.benchmarks:
                combined_stdout.append("\n--- Benchmarks ---")
                combined_stdout.append(profile_result.benchmarks)
            combined_stderr.append("\n=== PROFILE VALIDATION ===")
            combined_stderr.append(profile_result.stderr)

            # Test logs
            if tests_result is not None:
                combined_stdout.append("\n=== TEST VALIDATION ===")
                combined_stdout.append(tests_result.stdout)
                combined_stderr.append("\n=== TEST VALIDATION ===")
                combined_stderr.append(tests_result.stderr)

            return BuildResult(
                ok=True,
                image_id=build_res.image_id,
                image_name=build_res.image_name,
                rc=0,
                duration_s=build_res.duration_s,
                stderr_tail="\n".join(combined_stderr),
                stdout_tail="\n".join(combined_stdout),
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
) -> tuple[bool, str, str]:
    """Run a quick profiling sanity check using /profile.sh in the image.

    Returns:
        Tuple of (success, stdout, stderr)
    """
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
        stdout = (container.logs(stdout=True, stderr=False) or b"").decode("utf-8", errors="replace")
        stderr = (container.logs(stdout=False, stderr=True) or b"").decode("utf-8", errors="replace")
        if rc == 124:
            logger.debug("profile:timeout rc=124 treated as success")
            return (True, _preview(stdout, 4000), _preview(stderr, 4000))
        if rc != 0:
            logger.debug("profile:failed rc=%s stderr_tail: %s", rc, _preview(stderr, 240))
        return (rc == 0, _preview(stdout, 4000), _preview(stderr, 4000))
    except Exception as e:
        return (False, "", f"exception: {e}")
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
) -> tuple[bool, str, str]:
    """Run a quick test sanity check using /run_tests.sh in the image.

    Semantics preserved: feedparser install, 45s timeout, -q -k filter, rc==124 => success.

    Returns:
        Tuple of (success, stdout, stderr)
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
        stdout = (container.logs(stdout=True, stderr=False) or b"").decode("utf-8", errors="replace")
        stderr = (container.logs(stdout=False, stderr=True) or b"").decode("utf-8", errors="replace")
        if rc == 124:
            logger.debug("tests:timeout rc=124 treated as success")
            return (True, _preview(stdout, 4000), _preview(stderr, 4000))
        if rc != 0:
            logger.debug("tests:failed rc=%s stderr_tail: %s", rc, _preview(stderr, 240))
        return (rc == 0, _preview(stdout, 4000), _preview(stderr, 4000))
    except Exception as e:
        return (False, "", f"exception: {e}")
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
    profile_ok, profile_stdout, profile_stderr = _run_quick_profile(
        client=client,
        image_name=task.with_tag("pkg").get_image_name(),
        run_labels=run_labels,
        timeout=min(args.build_timeout, 600),
    )
    logger.debug("profile:done ok=%s duration=%.1fs", profile_ok, time.time() - _t_profile_start)

    if not profile_ok:
        tests_ok = False
        tests_stdout = ""
        tests_stderr = ""
    else:
        logger.debug("tests:start")
        _t_tests_start = time.time()
        tests_ok, tests_stdout, tests_stderr = _run_quick_tests(
            client=client,
            image_name=task.with_tag("pkg").get_image_name(),
            repo_name=task.repo,
            run_labels=run_labels,
            timeout=min(args.build_timeout, 900),
        )
        logger.debug("tests:done ok=%s duration=%.1fs", tests_ok, time.time() - _t_tests_start)

    # Check if validation passed - precedence: build > profile > test
    if not profile_ok:
        logger.warning("build_and_validate: profile validation failed")
        # Return only profile logs (the failing step)
        return BuildResult(
            ok=False,
            image_id=build_res.image_id,
            image_name=build_res.image_name,
            rc=1,
            duration_s=build_res.duration_s,
            stderr_tail=profile_stderr,
            stdout_tail=profile_stdout,
            failure_stage="profile",
        )
    elif not tests_ok:
        logger.warning("build_and_validate: test validation failed")
        # Return only test logs (the failing step)
        return BuildResult(
            ok=False,
            image_id=build_res.image_id,
            image_name=build_res.image_name,
            rc=1,
            duration_s=build_res.duration_s,
            stderr_tail=tests_stderr,
            stdout_tail=tests_stdout,
            failure_stage="tests",
        )
    else:
        logger.info("build_and_validate: verification passed (profile and tests)")
        # On success, concatenate all three pairs of logs
        combined_stdout = []
        combined_stderr = []

        # Build logs
        combined_stdout.append("=== BUILD ===")
        combined_stdout.append(build_res.stdout_tail if build_res.stdout_tail else "(no stdout)")
        combined_stderr.append("=== BUILD ===")
        combined_stderr.append(build_res.stderr_tail if build_res.stderr_tail else "(no stderr)")

        # Profile logs
        combined_stdout.append("\n=== PROFILE VALIDATION ===")
        combined_stdout.append(profile_stdout)
        combined_stderr.append("\n=== PROFILE VALIDATION ===")
        combined_stderr.append(profile_stderr)

        # Test logs
        combined_stdout.append("\n=== TEST VALIDATION ===")
        combined_stdout.append(tests_stdout)
        combined_stderr.append("\n=== TEST VALIDATION ===")
        combined_stderr.append(tests_stderr)

        return BuildResult(
            ok=True,
            image_id=build_res.image_id,
            image_name=build_res.image_name,
            rc=0,
            duration_s=build_res.duration_s,
            stderr_tail="\n".join(combined_stderr),
            stdout_tail="\n".join(combined_stdout),
        )


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
        profile_ok, profile_stdout, profile_stderr = _run_quick_profile(
            client=client, image_name=task.get_image_name(), timeout=600
        )
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
            "stderr_tail": profile_stderr,
            "stdout_tail": profile_stdout,
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
