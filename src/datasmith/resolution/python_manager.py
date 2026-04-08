"""Python version management and uv interaction."""

from __future__ import annotations

import datetime as dt
import os
import subprocess
from pathlib import Path

from datasmith.utils import get_logger

logger = get_logger("resolution.python_manager")


def run_uv(
    args: list[str],
    *,
    input_text: str | None = None,
    cwd: Path | None = None,
    extra_env: dict[str, str] | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess:
    """Run a uv command with specified arguments."""
    env = os.environ.copy()
    env.setdefault("UV_COLOR", "never")
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
            f"uv {' '.join(args)} failed with code {cp.returncode}\n"
            f"STDOUT:\n{cp.stdout.decode()}\nSTDERR:\n{cp.stderr.decode()}"
        )
    return cp


def ensure_python_version_available(version: str) -> bool:
    """Ensure uv has the requested Python version available, downloading if needed."""
    list_cp = run_uv(["python", "list"])
    if list_cp.returncode == 0:
        output = list_cp.stdout.decode()
        if version in output or f"cpython-{version}" in output or version.replace(".", "") in output:
            return True

    install_cp = run_uv(["python", "install", version])
    if install_cp.returncode == 0:
        logger.debug("Successfully installed Python %s", version)
        return True

    logger.debug("Failed to install Python %s: %s", version, install_cp.stderr.decode())
    return False


def filter_python_versions_by_commit_date(  # noqa: C901
    available_versions: set[tuple[int, ...]], commit_date: dt.datetime
) -> list[tuple[int, ...]]:
    """Filter Python versions to avoid anachronistic choices.

    Note: Python 3.7 is excluded since it's EOL and not available in uv.
    """
    valid_versions = [v for v in available_versions if v >= (3, 8)]
    if not valid_versions:
        return []

    py_releases = {
        (3, 7): dt.datetime(2018, 6, 27, tzinfo=dt.timezone.utc),
        (3, 8): dt.datetime(2019, 10, 14, tzinfo=dt.timezone.utc),
        (3, 9): dt.datetime(2020, 10, 5, tzinfo=dt.timezone.utc),
        (3, 10): dt.datetime(2021, 10, 4, tzinfo=dt.timezone.utc),
        (3, 11): dt.datetime(2022, 10, 24, tzinfo=dt.timezone.utc),
        (3, 12): dt.datetime(2023, 10, 2, tzinfo=dt.timezone.utc),
        (3, 13): dt.datetime(2024, 10, 7, tzinfo=dt.timezone.utc),
    }

    grace_period = dt.timedelta(days=90)
    filtered = []
    for v in valid_versions:
        version_key = (v[0], v[1])
        release_date = py_releases.get(version_key)

        if release_date is None:
            if commit_date < dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc):
                continue
            filtered.append(v)
        elif commit_date >= release_date - grace_period:
            filtered.append(v)

    if not filtered:
        inferred = []
        for version_key, release_date in sorted(py_releases.items(), reverse=True):
            if version_key < (3, 8):
                continue
            if release_date <= commit_date + grace_period:
                matching = [v for v in valid_versions if (v[0], v[1]) == version_key]
                if matching:
                    inferred.extend(matching)
                elif len(inferred) < 3:
                    inferred.append(version_key)
                if len(inferred) >= 3:
                    break
        filtered = inferred if inferred else [(3, 8)]

    return sorted(filtered, reverse=True)
