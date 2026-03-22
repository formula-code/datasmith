"""Analyzing Python imports to infer runtime dependencies."""

from __future__ import annotations

import ast
from pathlib import Path

from datasmith.utils import get_logger

from .constants import NOT_REQUIREMENTS, SPECIAL_IMPORT_TO_PYPI, STDLIB

logger = get_logger("resolution.import_analyzer")


def top_level_imports_under(root: Path) -> set[str]:  # noqa: C901
    """Parse all .py files under root and return top-level imported module names."""
    skip_dirs = {"tests", "test", "testing", "benchmarks", "doc", "docs", ".eggs", ".tox", "build", "dist"}
    names: set[str] = set()
    for path in root.rglob("*.py"):
        rel_parts = set(path.parts)
        if skip_dirs & rel_parts:
            continue
        try:
            src = path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            logger.debug("Failed to read %s: %s", path, e)
            continue
        try:
            tree = ast.parse(src, filename=str(path))
        except Exception as e:
            logger.debug("Failed to parse %s: %s", path, e)
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    mod = (alias.name or "").split(".", 1)[0]
                    if mod:
                        names.add(mod)
            elif isinstance(node, ast.ImportFrom):
                if getattr(node, "level", 0) and node.module is None:
                    continue
                mod = (node.module or "").split(".", 1)[0]
                if mod:
                    names.add(mod)
    return names


def infer_runtime_from_imports(project_dir: Path, own_import_name: str | None) -> set[str]:
    """Convert top-level imports to likely PyPI packages, filtering stdlib and self-import."""
    imports = top_level_imports_under(project_dir)
    out: set[str] = set()
    own = set()
    if own_import_name:
        own.add(own_import_name)
        own.add(own_import_name.replace("-", "_"))
        own.add(own_import_name.replace("_", "-"))
    for mod in imports:
        if mod.lower() in STDLIB:
            continue
        if mod in own:
            continue
        if mod in NOT_REQUIREMENTS:
            continue
        pkg = SPECIAL_IMPORT_TO_PYPI.get(mod, mod)
        out.add(pkg)
    return out
