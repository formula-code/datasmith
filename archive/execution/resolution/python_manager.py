"""Python version management and uv interaction."""

from __future__ import annotations

import datetime as dt
import os
import subprocess
from pathlib import Path

from datasmith.logging_config import get_logger

logger = get_logger(__name__)


def run_uv(
    args: list[str],
    *,
    input_text: str | None = None,
    cwd: Path | None = None,
    extra_env: dict[str, str] | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess:
    """
    Run a uv command with specified arguments.

    Args:
        args: Command arguments to pass to uv
        input_text: Optional text to pass as stdin
        cwd: Optional working directory
        extra_env: Optional environment variables to add
        check: Whether to raise an exception on non-zero exit code

    Returns:
        CompletedProcess instance with command results

    Raises:
        RuntimeError: If check=True and command fails
    """
    env = os.environ.copy()
    env.setdefault("UV_COLOR", "never")  # avoid ANSI in output
    env.setdefault("NO_COLOR", "1")
    if extra_env:
        env.update(extra_env)
    cp = subprocess.run(
        ["uv", *args],
        input=input_text.encode("utf-8") if input_text is not None else None,
        capture_output=True,
        cwd=str(cwd) if cwd else None,
        env=env,
    )
    if check and cp.returncode != 0:
        raise RuntimeError(
            f"uv {' '.join(args)} failed with code {cp.returncode}\nSTDOUT:\n{cp.stdout.decode()}\nSTDERR:\n{cp.stderr.decode()}"
        )
    return cp


def ensure_python_version_available(version: str) -> bool:
    """
    Ensure uv has the requested Python version available, downloading if needed.

    Args:
        version: Python version string (e.g., "3.8", "3.9.7")

    Returns:
        True if version is available (or successfully installed), False otherwise.
    """
    # First check if the version is already available
    list_cp = run_uv(["python", "list"])
    if list_cp.returncode == 0:
        output = list_cp.stdout.decode()
        # uv python list shows versions like "cpython-3.8.18" or "3.8.18"
        # Check if our version appears in the output
        if version in output or f"cpython-{version}" in output or version.replace(".", "") in output:
            return True

    # Try to install the version
    install_cp = run_uv(["python", "install", version])
    if install_cp.returncode == 0:
        logger.debug(f"Successfully installed Python {version}")
        return True

    # If installation failed, log why and return False
    logger.debug(f"Failed to install Python {version}: {install_cp.stderr.decode()}")
    return False


def filter_python_versions_by_commit_date(  # noqa: C901
    available_versions: set[tuple[int, ...]], commit_date: dt.datetime
) -> list[tuple[int, ...]]:
    """
    Filter Python versions to avoid anachronistic choices.
    Don't select Python versions that didn't exist at commit time.

    Note: Python 3.7 is excluded since it's EOL and not available in uv.

    Args:
        available_versions: Set of Python version tuples available in ASV config
        commit_date: Datetime when the commit was made

    Returns:
        Sorted list of version tuples (newest to oldest).
        Always returns at least one version if any valid versions exist.
    """
    # Filter out very old versions (< 3.8) - Python 3.7 is EOL and not available in uv
    valid_versions = [v for v in available_versions if v >= (3, 8)]
    if not valid_versions:
        return []

    # Python release dates (approximately, using stable release dates)
    py_releases = {
        (3, 7): dt.datetime(2018, 6, 27, tzinfo=dt.timezone.utc),
        (3, 8): dt.datetime(2019, 10, 14, tzinfo=dt.timezone.utc),
        (3, 9): dt.datetime(2020, 10, 5, tzinfo=dt.timezone.utc),
        (3, 10): dt.datetime(2021, 10, 4, tzinfo=dt.timezone.utc),
        (3, 11): dt.datetime(2022, 10, 24, tzinfo=dt.timezone.utc),
        (3, 12): dt.datetime(2023, 10, 2, tzinfo=dt.timezone.utc),
        (3, 13): dt.datetime(2024, 10, 7, tzinfo=dt.timezone.utc),
    }

    # Filter out Python versions released after the commit
    # Add a small grace period (3 months) to account for early adoption
    grace_period = dt.timedelta(days=90)
    filtered = []
    for v in valid_versions:
        # Only major.minor matters for release date
        version_key = (v[0], v[1])
        release_date = py_releases.get(version_key)

        if release_date is None:
            # Unknown version, assume it's very new and exclude if commit is old
            if commit_date < dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc):
                continue
            filtered.append(v)
        elif commit_date >= release_date - grace_period:
            # Commit is after (or shortly before) Python version release
            filtered.append(v)

    # If filtering removed everything, infer sensible Python versions for the commit date
    # This ensures we don't skip commits entirely, and we use appropriate versions
    if not filtered:
        # Infer appropriate Python versions based on commit date
        # We'll use the 2-3 newest versions that existed at commit time (excluding 3.7)
        inferred = []
        for version_key, release_date in sorted(py_releases.items(), reverse=True):
            # Skip Python 3.7 - it's EOL and not available in uv
            if version_key < (3, 8):
                continue

            if release_date <= commit_date + grace_period:
                # This version existed at commit time, add it
                # Look for this version in valid_versions (matching major.minor)
                matching = [v for v in valid_versions if (v[0], v[1]) == version_key]
                if matching:
                    inferred.extend(matching)
                elif len(inferred) < 3:
                    # Version not in ASV config, but we can try it anyway
                    inferred.append(version_key)

                if len(inferred) >= 3:
                    break

        filtered = inferred if inferred else [(3, 8)]

    # Sort newest to oldest
    return sorted(filtered, reverse=True)
