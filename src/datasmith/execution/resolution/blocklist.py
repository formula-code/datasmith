"""Dynamic blocklist for packages that don't exist on PyPI or can't be resolved.

This module implements a self-healing mechanism that learns from resolution failures.
When uv reports that a package is not found, we automatically add it to a blocklist
and retry resolution without that package.
"""

from __future__ import annotations

import json
import re
import threading

from datasmith.logging_config import get_logger

from .constants import GIT_CACHE_DIR

logger = get_logger(__name__)

# Path to persistent blocklist file
BLOCKLIST_PATH = GIT_CACHE_DIR / "package_blocklist.json"

# Thread-safe access to blocklist
_blocklist_lock = threading.Lock()
_blocklist_cache: set[str] | None = None


def _load_blocklist() -> set[str]:
    """Load the blocklist from disk, creating an empty one if it doesn't exist."""
    if not BLOCKLIST_PATH.exists():
        return set()

    try:
        with BLOCKLIST_PATH.open("r") as f:
            data = json.load(f)
            return set(data.get("blocked_packages", []))
    except Exception as e:
        logger.warning(f"Failed to load blocklist from {BLOCKLIST_PATH}: {e}")
        return set()


def _save_blocklist(blocklist: set[str]) -> None:
    """Save the blocklist to disk."""
    try:
        BLOCKLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        with BLOCKLIST_PATH.open("w") as f:
            json.dump(
                {
                    "blocked_packages": sorted(blocklist),
                    "description": "Packages that don't exist on PyPI or can't be resolved",
                },
                f,
                indent=2,
            )
    except Exception as e:
        logger.warning(f"Failed to save blocklist to {BLOCKLIST_PATH}: {e}")


def get_blocklist() -> set[str]:
    """
    Get the current blocklist of packages to filter out.

    Returns a cached version for performance.
    """
    global _blocklist_cache

    with _blocklist_lock:
        if _blocklist_cache is None:
            _blocklist_cache = _load_blocklist()
        return _blocklist_cache.copy()


def add_to_blocklist(package_name: str) -> bool:
    """
    Add a package to the blocklist.

    Args:
        package_name: Name of the package to block

    Returns:
        True if the package was newly added, False if it was already blocked
    """
    global _blocklist_cache

    if not package_name or not package_name.strip():
        return False

    package_name = package_name.strip().lower()

    with _blocklist_lock:
        blocklist = _load_blocklist()

        if package_name in blocklist:
            return False

        blocklist.add(package_name)
        _save_blocklist(blocklist)

        # Update cache
        _blocklist_cache = blocklist.copy()

        logger.info(f"Added '{package_name}' to package blocklist")
        return True


def extract_failing_package(error_log: str) -> str | None:
    """
    Extract the package name that caused a resolution failure from uv error logs.

    Handles patterns like:
    - "Because <package> was not found in the package registry"
    - "Because there are no versions of <package>"
    - "Because you require <package> and <package>, we can conclude..."

    Args:
        error_log: The error log from uv

    Returns:
        The package name that failed, or None if no clear failure detected
    """
    if not error_log:
        return None

    # Pattern 1: "Because <package> was not found in the package registry"
    match = re.search(r"Because ([\w\-]+) was not found in the package registry", error_log)
    if match:
        return match.group(1)

    # Pattern 2: "Because there are no versions of <package>"
    match = re.search(r"Because there are no versions of ([\w\-]+)", error_log)
    if match:
        return match.group(1)

    # Pattern 3: Version conflicts that suggest a non-existent package
    # "Because you require <package>==X.Y.Z and <package>>=A.B.C, we can conclude..."
    # This often indicates the package doesn't exist with those versions
    match = re.search(
        r"Because you require ([\w\-]+)==[\d\.]+ and \1[><=!]+[\d\.]+, we can conclude",
        error_log,
    )
    if match:
        pkg = match.group(1)
        # Only treat as non-existent if it's an unusual name (version-like or unusual chars)
        if re.match(r"^\d+[\-\d]+$", pkg) or pkg in {"uninstall", "install"}:
            return pkg

    return None


def should_retry_without_package(error_log: str) -> bool:
    """
    Determine if a resolution failure should trigger a retry without the failing package.

    Args:
        error_log: The error log from uv

    Returns:
        True if we should retry, False otherwise
    """
    if not error_log:
        return False

    # Retry for "not found" errors
    if "was not found in the package registry" in error_log:
        return True

    # Retry for "no versions" errors
    if "Because there are no versions of" in error_log:
        return True

    # Don't retry for build failures, network errors, etc.
    if "Failed to build" in error_log:
        return False

    if "Failed to download" in error_log:
        return False

    return False


def remove_package_from_requirements(requirements: list[str], package_name: str) -> tuple[list[str], bool]:
    """
    Remove all requirements for a given package from a list.

    Args:
        requirements: List of requirement strings
        package_name: Name of package to remove (case-insensitive)

    Returns:
        Tuple of (filtered_requirements, was_removed)
    """
    if not package_name:
        return requirements, False

    package_name_lower = package_name.lower()
    filtered: list[str] = []
    was_removed = False

    for req in requirements:
        # Extract package name from requirement string
        # Handle formats: "package>=1.0", "package[extra]", "package ; marker"
        pkg_match = re.match(r"^([a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?)", req)
        if pkg_match:
            req_pkg_name = pkg_match.group(1).lower()
            if req_pkg_name == package_name_lower:
                was_removed = True
                continue

        filtered.append(req)

    return filtered, was_removed
