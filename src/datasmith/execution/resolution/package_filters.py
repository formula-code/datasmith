"""Filtering and normalizing package requirements."""

from __future__ import annotations

import re
import shlex
from collections.abc import Iterable, Mapping
from pathlib import Path

from git import Commit

from .constants import (
    ALLOWLIST_COMMON_PYPI,
    CONDA_SYSTEM_PACKAGES,
    EXTRA_MARKER_RE,
    GENERIC_LOCAL_NAMES,
    NOT_REQUIREMENTS,
    STDLIB,
)
from .git_utils import read_blob_text


def parse_extras_segment(token: str) -> list[str]:
    """
    Extract extras from a token like 'package[extra1,extra2]'.

    Args:
        token: String that may contain extras in brackets

    Returns:
        List of extra names, or empty list if no extras
    """
    if "[" not in token or not token.endswith("]"):
        return []
    segment = token[token.rfind("[") + 1 : -1]
    if not segment:
        return []
    return [part.strip() for part in segment.split(",") if part.strip()]


def extras_from_install_commands(install_cmds: Iterable[str], extras_available: set[str]) -> set[str]:
    """
    Extract extras requested in install commands.

    Args:
        install_cmds: Install command strings from ASV config
        extras_available: Set of available extra names

    Returns:
        Set of requested extra names
    """
    requested: set[str] = set()
    for cmd in install_cmds:
        if not cmd:
            continue
        for token in shlex.split(cmd):
            for extra in parse_extras_segment(token):
                if extra in extras_available:
                    requested.add(extra)
    return requested


def extras_from_matrix(matrix: Mapping[str, set[str]] | None, extras_available: set[str]) -> set[str]:
    """
    Extract extras from ASV matrix configuration.

    Args:
        matrix: ASV matrix configuration
        extras_available: Set of available extra names

    Returns:
        Set of requested extra names
    """
    if not matrix:
        return set()
    requested: set[str] = set()
    for values in matrix.values():
        for value in values:
            if value in extras_available:
                requested.add(value)
    return requested


def extract_requested_extras(
    install_cmds: Iterable[str],
    matrix: Mapping[str, set[str]] | None,
    available: Iterable[str],
) -> set[str]:
    """
    Extract all requested extras from install commands and matrix.

    Args:
        install_cmds: Install command strings
        matrix: ASV matrix configuration
        available: Available extra names

    Returns:
        Set of all requested extra names
    """
    extras_available = set(available)
    requested = extras_from_install_commands(install_cmds, extras_available)
    requested.update(extras_from_matrix(matrix, extras_available))
    return requested


def resolve_requirements_file(commit: Commit, rel_path: str, seen: set[str]) -> set[str]:
    """
    Recursively resolve a requirements file from a commit, handling nested -r references.

    Args:
        commit: Git commit object
        rel_path: Relative path to requirements file
        seen: Set of already-seen paths (to avoid infinite loops)

    Returns:
        Set of requirement strings
    """
    # Avoid infinite loops
    if rel_path in seen:
        return set()
    seen.add(rel_path)

    requirements: set[str] = set()
    content = read_blob_text(commit, rel_path)
    if not content:
        return requirements

    for line in content.splitlines():
        line = line.strip()
        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        # Handle nested -r references
        tokens = line.split()
        if len(tokens) >= 2 and tokens[0] in {"-r", "--requirement"}:
            nested_path = tokens[1]
            # Resolve relative to current file's directory
            if "/" in rel_path:
                base_dir = "/".join(rel_path.split("/")[:-1])
                nested_path = f"{base_dir}/{nested_path}"
            requirements.update(resolve_requirements_file(commit, nested_path, seen))
            continue

        # Add non-nested requirements
        requirements.add(line)

    return requirements


def split_shell_command(cmd: str) -> list[str]:
    """
    Split a shell command on operators like &&, ||, ; into separate commands.

    Args:
        cmd: Shell command string

    Returns:
        List of individual commands
    """
    # Split on shell command separators
    parts = re.split(r"\s*(?:&&|\|\||;)\s*", cmd)
    return [p.strip() for p in parts if p.strip()]


def is_valid_direct_url(req: str) -> bool:
    """
    Check if a requirement string is a valid direct URL for uv.

    Args:
        req: Requirement string

    Returns:
        True if it's a valid direct URL with supported archive extension
    """
    if not req or not req.strip():
        return False
    req = req.strip()
    if not (
        req.startswith("http://")
        or req.startswith("https://")
        or req.startswith("git+")
        or req.startswith("hg+")
        or req.startswith("svn+")
        or req.startswith("bzr+")
        or req.startswith("file://")
    ):
        return False
    # uv requires a supported archive extension for direct URLs
    ok_exts = (
        ".whl",
        ".tar.gz",
        ".zip",
        ".tar.bz2",
        ".tar.lz",
        ".tar.lzma",
        ".tar.xz",
        ".tar.zst",
        ".tar",
        ".tbz",
        ".tgz",
        ".tlz",
        ".txz",
    )
    return any(req.lower().endswith(ext) for ext in ok_exts)


def filter_pypi_packages(requirements: Iterable[str]) -> list[str]:
    """
    Filter out conda-only/system packages that don't exist on PyPI.
    Also removes python version specifiers.

    Args:
        requirements: Iterable of requirement strings

    Returns:
        Filtered list of PyPI-installable requirements
    """
    filtered: list[str] = []
    for req in requirements:
        if not req or not req.strip():
            continue
        req = req.strip()
        # Extract package name (before any version specifier or extras)
        pkg_name = re.split(r"[<>=!;\s\[]", req, maxsplit=1)[0].strip().lower()

        # Skip conda-only/system packages
        if pkg_name in CONDA_SYSTEM_PACKAGES:
            continue

        filtered.append(req)

    return filtered


def is_valid_pypi_requirement(req: str) -> bool:
    """
    Validate if a string looks like a valid PyPI requirement per PEP 508,
    or a URL-based requirement.

    Args:
        req: Requirement string

    Returns:
        True if it appears to be a valid requirement
    """
    if not req or not req.strip():
        return False
    req = req.strip()

    # Reject template variables
    if "{" in req or "}" in req or "$" in req:
        return False

    # Reject shell operators
    if any(op in req for op in ["&&", "||", ";;", "|", "&"]):
        return False

    # Reject pip options (start with --)
    if req.startswith("--"):
        return False

    # Allow URLs (git+, http://, https://, file://, etc.)
    if any(req.startswith(prefix) for prefix in ["http://", "https://", "git+", "hg+", "svn+", "bzr+", "file://"]):
        return True

    # Allow local paths with extras like ".[dev]" but reject bare "."
    if req.startswith("."):
        return False

    # Extract package name (before version specifiers, extras, etc.)
    pkg_match = re.match(r"^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)", req)
    if not pkg_match:
        return False

    pkg_name = pkg_match.group(1)

    # Reject names starting with underscore (private/internal modules)
    if pkg_name.startswith("_"):
        return False

    # Reject single-character names (too generic)
    return len(pkg_name) != 1


def fix_marker_spacing(req: str) -> str:
    """
    Fix missing spaces around 'and' and 'or' operators in PEP 508 markers.
    Also strips inline comments that don't have proper spacing.

    Converts malformed markers like:
    - "pydap;python_version<"3.10"andextra=="docs""
    to "pydap;python_version<"3.10" and extra=="docs""
    - "numpy>=1.21#comment" to "numpy>=1.21"

    Args:
        req: Requirement string that may have malformed markers

    Returns:
        Requirement string with properly spaced markers
    """
    # Strip inline comments that don't have proper spacing
    # PEP 508 allows comments only after whitespace
    # Match # that's not preceded by whitespace and remove everything after it
    if "#" in req:
        # Split on first # that's not preceded by whitespace
        match = re.search(r"(?<!\s)#", req)
        if match:
            req = req[: match.start()]

    if ";" not in req:
        return req

    # Split into package spec and marker
    parts = req.split(";", 1)
    if len(parts) != 2:
        return req

    pkg_spec, marker = parts

    # Add spaces around 'and' and 'or' if missing
    # Match 'and' or 'or' that don't have spaces around them
    marker = re.sub(r"(?<=[^\s])and(?=[^\s])", " and ", marker)
    marker = re.sub(r"(?<=[^\s])or(?=[^\s])", " or ", marker)

    return f"{pkg_spec};{marker}"


def normalize_requirement(req: str) -> list[str]:
    """
    Normalize a token into one or more requirement strings we should keep for resolution.
    - Filters out flags and shell operators
    - Keeps direct URLs with supported extensions
    - Keeps "name[extra]" or "name>=..."

    Args:
        req: Raw requirement string

    Returns:
        List of normalized requirement strings (may be empty if invalid)
    """
    if not req or not req.strip():
        return []
    req = req.strip()

    # Fix marker spacing issues (e.g., "andextra" -> " and extra")
    req = fix_marker_spacing(req)

    # Reject template variables
    if "{" in req or "}" in req or "$" in req:
        return []

    # Reject shell operators and pip options
    if any(op in req for op in ["&&", "||", ";;", "|", "&"]) or req.startswith("--"):
        return []

    # -c/-r/-e etc handled elsewhere
    if req in {"-r", "--requirement", "-c", "--constraint", "-e", "--editable"}:
        return []

    # Allow URLs if they look installable
    if req.startswith(("http://", "https://", "git+", "hg+", "svn+", "bzr+", "file://")):
        return [req] if is_valid_direct_url(req) else []

    # Bare dot paths are not acceptable (".", "./foo")
    if req.startswith("."):
        return []

    # Otherwise a simple token; leave as-is (we'll filter later)
    return [req]


def project_local_names(project_dir: Path) -> set[str]:
    """
    Collect names that look like local modules/packages to avoid misclassifying them as PyPI packages.

    Recursively scans the entire project tree to find all local module and package names.

    Args:
        project_dir: Path to project directory

    Returns:
        Set of local module/package names
    """
    names: set[str] = set()

    # Skip these directories when scanning for local modules
    skip_dirs = {"__pycache__", ".git", ".eggs", ".tox", "build", "dist", "node_modules"}

    # Top-level .py files
    for py in project_dir.glob("*.py"):
        if not py.name.startswith("_"):
            names.add(py.stem)

    # Recursively find all packages and modules in the project
    for item in project_dir.rglob("*"):
        # Skip unwanted directories
        if any(skip in item.parts for skip in skip_dirs):
            continue

        if item.is_dir():
            # Skip hidden/private directories
            if item.name.startswith(".") or item.name.startswith("_"):
                continue
            # If it's a package (has __init__.py), add the name
            if (item / "__init__.py").exists():
                names.add(item.name)
        elif item.suffix == ".py":
            # Skip private modules
            if not item.name.startswith("_"):
                names.add(item.stem)

    return names


def clean_pinned(reqs: list[str]) -> list[str]:
    """
    Removes lower-bound version specifiers from requirements that have both >= and <=.
    E.g., "torch>=1.8, <=1.9" becomes "torch<=1.9".
    """
    new_reqs = []
    for r in reqs:
        # remove all whitespace
        r = re.sub(r"\s+", "", r)
        if ">=" in r and "<=" in r:
            # "torch>=1.8, <= 1.9"
            # only keep torch<=1.9
            pkg_name = extract_pkg_name(r)
            parts = re.split(r",\s*", r)
            le_parts = [p for p in parts if "<=" in p]
            if le_parts:
                le_parts = [p if pkg_name in p else f"{pkg_name}{p}" for p in le_parts]
                new_reqs.extend(le_parts)
            else:
                new_reqs.append(r)
        else:
            new_reqs.append(r)
    return new_reqs


def extract_pkg_name(req: str) -> str:
    """
    Extract package name from a requirement string.
    Handles "pkg[extra]>=1.0; python_version<'3.11'"

    Args:
        req: Requirement string

    Returns:
        Package name
    """
    # Extract package name before version specifiers/extras/markers
    name = re.split(r"[<>=!;\s]", req, maxsplit=1)[0]
    # strip extras
    if "[" in name:
        name = name.split("[", 1)[0]
    return name.strip()


def filter_requirements_for_pypi(  # noqa: C901
    requirements: Iterable[str], *, project_dir: Path, own_import_name: str | None
) -> list[str]:
    """
    Remove things that are clearly not PyPI-installable:
      - stdlib, conda/system tools, known non-requirements
      - packages in the dynamic blocklist (learned from previous failures)
      - the project's own import name
      - names that are obviously local modules/packages in the repo
    Keep direct URLs that look installable (whl/sdist).

    Args:
        requirements: Iterable of requirement strings
        project_dir: Path to project directory
        own_import_name: The project's own import name (to exclude)

    Returns:
        Filtered list of PyPI-installable requirements
    """
    # Import here to avoid circular dependency
    from .blocklist import get_blocklist

    local_names = project_local_names(project_dir)
    own_names = set()
    if own_import_name:
        own_names |= {own_import_name, own_import_name.replace("-", "_"), own_import_name.replace("_", "-")}

    # Get dynamic blocklist of packages that failed in previous runs
    dynamic_blocklist = get_blocklist()

    out: list[str] = []
    for raw in requirements:
        if not raw or not raw.strip():
            continue
        raw = raw.strip()

        # Fix marker spacing issues (e.g., "andextra" -> " and extra")
        raw = fix_marker_spacing(raw)

        # Direct URL handled first
        if raw.startswith(("http://", "https://", "git+", "hg+", "svn+", "bzr+", "file://")):
            if is_valid_direct_url(raw):
                out.append(raw)
            continue

        name = extract_pkg_name(raw)
        if not name:
            continue
        low = name.lower()

        # Drop interpreter references such as "python", "python3", "python3.10"
        if low.startswith("python"):
            suffix = low[6:]
            if not suffix or suffix[0].isdigit() or suffix.startswith("."):
                continue

        # Reject invalid package names (starting with _, single char, etc.)
        if name.startswith("_") or len(name) == 1:
            continue

        # stdlib / known not-requirements
        if low in STDLIB or name in NOT_REQUIREMENTS:
            continue

        # Dynamic blocklist (learned from failures)
        if low in dynamic_blocklist or name in dynamic_blocklist:
            continue

        # conda/system/tooling
        if low in CONDA_SYSTEM_PACKAGES:
            continue

        # Generic local module names (unless allowlisted as known PyPI packages)
        if low in GENERIC_LOCAL_NAMES and name not in ALLOWLIST_COMMON_PYPI:
            continue

        # self / internal modules
        if name in own_names:
            continue
        if name in local_names and name not in ALLOWLIST_COMMON_PYPI:
            # likely a local submodule (e.g., "geometry", "utils", "core", etc.)
            continue

        out.append(raw)

    # strip the EXTRA_MARKER_RE if it exists.
    stripped: list[str] = []
    for r in out:
        # Remove the `; extra == "..."` marker (and any surrounding spaces)
        r2 = EXTRA_MARKER_RE.sub("", r).strip()
        # If we end up with a dangling semicolon (e.g., only marker was present), drop it
        r2 = re.sub(r"\s*;\s*$", "", r2)
        stripped.append(r2)

    # remove duplicate while preserving order.
    deduped: list[str] = []
    seen: set[str] = set()
    for r in stripped:
        # use exact string match after previous normalization
        if r not in seen:
            seen.add(r)
            deduped.append(r)

    return deduped
