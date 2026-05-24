"""Extract ASV benchmark source code from a checked-out repository.

Used by pipeline stage 9 (``scrape_benchmark_source``) to populate the
``benchmark_codes`` table, which the FormulaCode website joins against
``benchmark_information`` on (owner, repo, benchmark_without_params).

The naming convention mirrors ASV: classes named ``Time*`` / ``Mem*`` /
``Peakmem*`` whose methods are ``time_*`` / ``mem_*`` / ``peakmem_*`` /
``track_*``, and module-level functions matching the same method prefixes.
The fully-qualified benchmark name is ``<module>.<Class>.<method>`` or
``<module>.<function>`` where ``<module>`` is the dotted file path
relative to the ASV ``benchmark_dir``.
"""

from __future__ import annotations

import ast
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from datasmith.utils import get_logger

logger = get_logger("scrape.benchmark_source")

# Tunable knobs (see CLAUDE.md tunable-constants rule).
DATASMITH_BENCH_SCRAPE_MAX_FILE_BYTES: int = int(os.environ.get("DATASMITH_BENCH_SCRAPE_MAX_FILE_BYTES", "1000000"))
DATASMITH_BENCH_SCRAPE_DIRS: tuple[str, ...] = tuple(
    d.strip()
    for d in os.environ.get("DATASMITH_BENCH_SCRAPE_DIRS", "benchmarks,asv_bench/benchmarks").split(",")
    if d.strip()
)

_ASV_METHOD_PREFIXES = ("time_", "mem_", "peakmem_", "track_")
_ASV_CLASS_PREFIXES = ("Time", "Mem", "Peakmem")
_SETUP_NAMES = frozenset({"setup", "setup_cache", "teardown"})


@dataclass(frozen=True)
class BenchmarkSource:
    """One benchmark function's source (and its setup, if any)."""

    benchmark_without_params: str
    source: str
    setup_source: str | None


def _resolve_benchmark_dir(repo_root: Path) -> Path | None:  # noqa: C901
    """Resolve the ASV ``benchmark_dir`` from ``asv.conf.json`` if present.

    Falls back to ``DATASMITH_BENCH_SCRAPE_DIRS`` candidates if no config
    exists or the configured dir is missing.
    """
    conf_path = repo_root / "asv.conf.json"
    if conf_path.is_file():
        try:
            raw = conf_path.read_text(encoding="utf-8", errors="replace")
            # asv.conf.json sometimes carries // comments; strip them defensively.
            raw_no_comments = re.sub(r"^\s*//.*$", "", raw, flags=re.MULTILINE)
            conf = json.loads(raw_no_comments)
        except (OSError, json.JSONDecodeError) as exc:
            logger.debug("asv.conf.json unreadable at %s: %s", conf_path, exc)
        else:
            bench_dir = conf.get("benchmark_dir")
            if isinstance(bench_dir, str) and bench_dir:
                candidate = (repo_root / bench_dir).resolve()
                try:
                    candidate.relative_to(repo_root.resolve())
                except ValueError:
                    logger.debug("benchmark_dir %s escapes repo root", bench_dir)
                else:
                    if candidate.is_dir():
                        return candidate

    for fallback in DATASMITH_BENCH_SCRAPE_DIRS:
        candidate = (repo_root / fallback).resolve()
        try:
            candidate.relative_to(repo_root.resolve())
        except ValueError:
            continue
        if candidate.is_dir():
            return candidate
    return None


def _module_path(bench_dir: Path, py_file: Path) -> str:
    """Dotted module path of ``py_file`` relative to ``bench_dir``.

    ``benchmarks/arithmetic.py`` → ``benchmarks.arithmetic``
    ``benchmarks/sub/foo.py``   → ``benchmarks.sub.foo``
    """
    bench_root_name = bench_dir.name  # the leading segment users see in benchmark_name
    rel = py_file.resolve().relative_to(bench_dir.resolve())
    parts = list(rel.with_suffix("").parts)
    if rel.name == "__init__.py":
        parts = parts[:-1]
    return ".".join([bench_root_name, *parts])


def _is_asv_method(name: str) -> bool:
    return any(name.startswith(p) for p in _ASV_METHOD_PREFIXES)


def _is_asv_class_name(name: str) -> bool:
    return any(name.startswith(p) for p in _ASV_CLASS_PREFIXES)


def _source_of(node: ast.AST, text: str) -> str | None:
    src = ast.get_source_segment(text, node)
    return src.rstrip() + "\n" if src else None


def _extract_from_file(py_file: Path, module_dotted: str) -> list[BenchmarkSource]:  # noqa: C901
    try:
        size = py_file.stat().st_size
    except OSError:
        return []
    if size > DATASMITH_BENCH_SCRAPE_MAX_FILE_BYTES:
        logger.debug("skip oversize bench file %s (%d bytes)", py_file, size)
        return []

    try:
        text = py_file.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        logger.debug("unreadable bench file %s: %s", py_file, exc)
        return []

    try:
        tree = ast.parse(text, filename=str(py_file))
    except SyntaxError as exc:
        logger.debug("syntax error parsing %s: %s", py_file, exc)
        return []

    module_setup: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name in _SETUP_NAMES:
            src = _source_of(node, text)
            if src:
                module_setup.append(src)

    results: list[BenchmarkSource] = []

    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            if not _is_asv_method(node.name):
                continue
            body = _source_of(node, text)
            if not body:
                continue
            setup_src = "\n".join(module_setup) if module_setup else None
            results.append(
                BenchmarkSource(
                    benchmark_without_params=f"{module_dotted}.{node.name}",
                    source=body,
                    setup_source=setup_src,
                )
            )

        elif isinstance(node, ast.ClassDef):
            if not _is_asv_class_name(node.name):
                continue
            class_setup: list[str] = []
            for child in node.body:
                if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef) and child.name in _SETUP_NAMES:
                    src = _source_of(child, text)
                    if src:
                        class_setup.append(src)

            setup_parts = [*module_setup, *class_setup]
            setup_src = "\n".join(setup_parts) if setup_parts else None

            class_src = _source_of(node, text)
            for child in node.body:
                if not isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                if not _is_asv_method(child.name):
                    continue
                method_src = _source_of(child, text)
                if not method_src:
                    continue
                # The website shows the method body, but the class header (and
                # any class-level attrs like params/param_names) is needed for
                # context. Concatenate them when both are available.
                if class_src:
                    combined = class_src
                else:
                    combined = method_src
                results.append(
                    BenchmarkSource(
                        benchmark_without_params=f"{module_dotted}.{node.name}.{child.name}",
                        source=combined,
                        setup_source=setup_src,
                    )
                )

    return results


def extract_benchmarks(repo_root: Path) -> list[BenchmarkSource]:
    """Walk a checked-out repo and return every ASV benchmark's source.

    Returns an empty list if no benchmark directory can be located. Safe to
    call on any repo; idempotent.
    """
    bench_dir = _resolve_benchmark_dir(repo_root)
    if bench_dir is None:
        logger.debug("no benchmark dir found under %s", repo_root)
        return []

    out: list[BenchmarkSource] = []
    for py_file in sorted(bench_dir.rglob("*.py")):
        module_dotted = _module_path(bench_dir, py_file)
        out.extend(_extract_from_file(py_file, module_dotted))
    return out
