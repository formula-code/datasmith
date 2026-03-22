"""Dynamic blocklist for packages that don't exist on PyPI or can't be resolved."""

from __future__ import annotations

import json
import re
import threading

from datasmith.utils import get_logger

from .constants import GIT_CACHE_DIR

logger = get_logger("resolution.blocklist")


def normalize_package_name(name: str) -> str:
    """Normalize a package name according to PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


BLOCKLIST_PATH = GIT_CACHE_DIR / "package_blocklist.json"

_blocklist_lock = threading.Lock()
_blocklist_cache: set[str] | None = None


def _load_blocklist() -> set[str]:
    """Load the blocklist from disk."""
    if not BLOCKLIST_PATH.exists():
        return set()
    try:
        with BLOCKLIST_PATH.open("r") as f:
            data = json.load(f)
            return set(data.get("blocked_packages", []))
    except Exception as e:
        logger.warning("Failed to load blocklist from %s: %s", BLOCKLIST_PATH, e)
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
        logger.warning("Failed to save blocklist to %s: %s", BLOCKLIST_PATH, e)


def get_blocklist() -> set[str]:
    """Get the current blocklist of packages to filter out."""
    global _blocklist_cache

    with _blocklist_lock:
        if _blocklist_cache is None:
            _blocklist_cache = _load_blocklist()
        return _blocklist_cache.copy()


def add_to_blocklist(package_name: str) -> bool:
    """Add a package to the blocklist. Returns True if newly added."""
    global _blocklist_cache

    if not package_name or not package_name.strip():
        return False

    package_name = normalize_package_name(package_name.strip())

    with _blocklist_lock:
        blocklist = _load_blocklist()
        if package_name in blocklist:
            return False
        blocklist.add(package_name)
        _save_blocklist(blocklist)
        _blocklist_cache = blocklist.copy()
        logger.info("Added '%s' to package blocklist", package_name)
        return True


def extract_failing_package(error_log: str) -> str | None:
    """Extract the package name that caused a resolution failure from uv error logs."""
    if not error_log:
        return None

    match = re.search(r"Because ([\w\-]+) was not found in the package registry", error_log)
    if match:
        return match.group(1)

    match = re.search(r"Because there are no versions of ([\w\-]+)", error_log)
    if match:
        return match.group(1)

    match = re.search(
        r"Because you require ([\w\-]+)==[\d\.]+ and \1[><=!]+[\d\.]+, we can conclude",
        error_log,
    )
    if match:
        pkg = match.group(1)
        if re.match(r"^\d+[\-\d]+$", pkg) or pkg in {"uninstall", "install"}:
            return pkg

    return None


def should_retry_without_package(error_log: str) -> bool:
    """Determine if a resolution failure should trigger a retry without the failing package."""
    if not error_log:
        return False
    if "was not found in the package registry" in error_log:
        return True
    if "Because there are no versions of" in error_log:
        return True
    if "Failed to build" in error_log:
        return False
    if "Failed to download" in error_log:
        return False
    return False


def remove_package_from_requirements(requirements: list[str], package_name: str) -> tuple[list[str], bool]:
    """Remove all requirements for a given package from a list."""
    if not package_name:
        return requirements, False

    package_name_normalized = normalize_package_name(package_name)
    filtered: list[str] = []
    was_removed = False

    for req in requirements:
        pkg_match = re.match(r"^([a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?)", req)
        if pkg_match:
            req_pkg_name = pkg_match.group(1)
            if normalize_package_name(req_pkg_name) == package_name_normalized:
                was_removed = True
                continue
        filtered.append(req)

    return filtered, was_removed
