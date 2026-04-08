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
    """Extract extras from a token like 'package[extra1,extra2]'."""
    if "[" not in token or not token.endswith("]"):
        return []
    segment = token[token.rfind("[") + 1 : -1]
    if not segment:
        return []
    return [part.strip() for part in segment.split(",") if part.strip()]


def extras_from_install_commands(install_cmds: Iterable[str], extras_available: set[str]) -> set[str]:
    """Extract extras requested in install commands."""
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
    """Extract extras from ASV matrix configuration."""
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
    """Extract all requested extras from install commands and matrix."""
    extras_available = set(available)
    requested = extras_from_install_commands(install_cmds, extras_available)
    requested.update(extras_from_matrix(matrix, extras_available))
    return requested


def resolve_requirements_file(commit: Commit, rel_path: str, seen: set[str]) -> set[str]:
    """Recursively resolve a requirements file from a commit."""
    if rel_path in seen:
        return set()
    seen.add(rel_path)

    requirements: set[str] = set()
    content = read_blob_text(commit, rel_path)
    if not content:
        return requirements

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        tokens = line.split()
        if len(tokens) >= 2 and tokens[0] in {"-r", "--requirement"}:
            nested_path = tokens[1]
            if "/" in rel_path:
                base_dir = "/".join(rel_path.split("/")[:-1])
                nested_path = f"{base_dir}/{nested_path}"
            requirements.update(resolve_requirements_file(commit, nested_path, seen))
            continue

        requirements.add(line)

    return requirements


def split_shell_command(cmd: str) -> list[str]:
    """Split a shell command on operators like &&, ||, ; into separate commands."""
    parts = re.split(r"\s*(?:&&|\|\||;)\s*", cmd)
    return [p.strip() for p in parts if p.strip()]


def is_valid_direct_url(req: str) -> bool:
    """Check if a requirement string is a valid direct URL for uv."""
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


def is_valid_pypi_requirement(req: str) -> bool:
    """Validate if a string looks like a valid PyPI requirement per PEP 508."""
    if not req or not req.strip():
        return False
    req = req.strip()
    if "{" in req or "}" in req or "$" in req:
        return False
    if any(op in req for op in ["&&", "||", ";;", "|", "&"]):
        return False
    if req.startswith("--"):
        return False
    if any(req.startswith(prefix) for prefix in ["http://", "https://", "git+", "hg+", "svn+", "bzr+", "file://"]):
        return True
    if req.startswith("."):
        return False
    pkg_match = re.match(r"^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)", req)
    if not pkg_match:
        return False
    pkg_name = pkg_match.group(1)
    return not (pkg_name.startswith("_") or len(pkg_name) == 1)


def fix_marker_spacing(req: str) -> str:
    """Fix missing spaces around 'and' and 'or' operators in PEP 508 markers."""
    if "#" in req:
        match = re.search(r"(?<!\s)#", req)
        if match:
            req = req[: match.start()]
    if ";" not in req:
        return req
    parts = req.split(";", 1)
    if len(parts) != 2:
        return req
    pkg_spec, marker = parts
    marker = re.sub(r"(?<=[^\s])and(?=[^\s])", " and ", marker)
    marker = re.sub(r"(?<=[^\s])or(?=[^\s])", " or ", marker)
    return f"{pkg_spec};{marker}"


def normalize_requirement(req: str) -> list[str]:
    """Normalize a token into one or more requirement strings."""
    if not req or not req.strip():
        return []
    req = req.strip()
    req = fix_marker_spacing(req)
    if "{" in req or "}" in req or "$" in req:
        return []
    if any(op in req for op in ["&&", "||", ";;", "|", "&"]) or req.startswith("--"):
        return []
    if req in {"-r", "--requirement", "-c", "--constraint", "-e", "--editable"}:
        return []
    if req.startswith(("http://", "https://", "git+", "hg+", "svn+", "bzr+", "file://")):
        return [req] if is_valid_direct_url(req) else []
    if req.startswith("."):
        return []
    return [req]


def project_local_names(project_dir: Path) -> set[str]:
    """Collect names that look like local modules/packages."""
    names: set[str] = set()
    skip_dirs = {"__pycache__", ".git", ".eggs", ".tox", "build", "dist", "node_modules"}
    for py in project_dir.glob("*.py"):
        if not py.name.startswith("_"):
            names.add(py.stem)
    for item in project_dir.rglob("*"):
        if any(skip in item.parts for skip in skip_dirs):
            continue
        if item.is_dir():
            if item.name.startswith(".") or item.name.startswith("_"):
                continue
            if (item / "__init__.py").exists():
                names.add(item.name)
        elif item.suffix == ".py":
            if not item.name.startswith("_"):
                names.add(item.stem)
    return names


def clean_pinned(reqs: list[str]) -> list[str]:
    """Removes lower-bound version specifiers from requirements that have both >= and <=."""
    new_reqs = []
    for r in reqs:
        r = re.sub(r"\s+", "", r)
        if ">=" in r and "<=" in r:
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
    """Extract package name from a requirement string."""
    name = re.split(r"[<>=!;\s]", req, maxsplit=1)[0]
    if "[" in name:
        name = name.split("[", 1)[0]
    return name.strip()


def filter_requirements_for_pypi(  # noqa: C901
    requirements: Iterable[str], *, project_dir: Path, own_import_name: str | None
) -> list[str]:
    """Remove things that are clearly not PyPI-installable."""
    from .blocklist import get_blocklist, normalize_package_name

    local_names = project_local_names(project_dir)
    own_names = set()
    if own_import_name:
        own_names |= {own_import_name, own_import_name.replace("-", "_"), own_import_name.replace("_", "-")}

    dynamic_blocklist = get_blocklist()

    out: list[str] = []
    for raw in requirements:
        if not raw or not raw.strip():
            continue
        raw = raw.strip()
        raw = fix_marker_spacing(raw)

        if raw.startswith(("http://", "https://", "git+", "hg+", "svn+", "bzr+", "file://")):
            if is_valid_direct_url(raw):
                out.append(raw)
            continue

        name = extract_pkg_name(raw)
        if not name:
            continue
        low = name.lower()

        if low.startswith("python"):
            suffix = low[6:]
            if not suffix or suffix[0].isdigit() or suffix.startswith("."):
                continue

        if name.startswith("_") or len(name) == 1:
            continue
        if low in STDLIB or name in NOT_REQUIREMENTS:
            continue

        normalized_name = normalize_package_name(name)
        if normalized_name in dynamic_blocklist:
            continue
        if low in CONDA_SYSTEM_PACKAGES:
            continue
        if low in GENERIC_LOCAL_NAMES and name not in ALLOWLIST_COMMON_PYPI:
            continue
        if name in own_names:
            continue
        if name in local_names and name not in ALLOWLIST_COMMON_PYPI:
            continue

        out.append(raw)

    stripped: list[str] = []
    for r in out:
        r2 = EXTRA_MARKER_RE.sub("", r).strip()
        r2 = re.sub(r"\s*;\s*$", "", r2)
        stripped.append(r2)

    deduped: list[str] = []
    seen: set[str] = set()
    for r in stripped:
        if r not in seen:
            seen.add(r)
            deduped.append(r)

    return deduped
