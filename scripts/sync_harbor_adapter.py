#!/usr/bin/env python
"""Generate harbor's ``adapters/formulacode/`` from datasmith's ``harbor_adapter``.

``src/datasmith/harbor_adapter/`` is the single canonical FormulaCode task
template. harbor's ``adapters/formulacode/`` is a generated, verifiable mirror
of it: the renderer modules and the whole ``template/`` payload are copied over
with a DO-NOT-EDIT header, and package imports are rewritten to harbor's
script-style layout (``from datasmith.harbor_adapter.utils import`` becomes
``from utils import``, which is how ``run_adapter.py`` imports them).

    python scripts/sync_harbor_adapter.py --write            # regenerate the mirror
    python scripts/sync_harbor_adapter.py --check            # exit 1 on any byte drift

Everything else in ``adapters/formulacode/`` (README.md, run_adapter.py,
dataset.py, dockerhub.py, adapter_metadata.json, parity_experiment.json,
formulacode.yaml, commands.md, deploy/, the documentation-only
template/task.toml) is harbor's own and is never touched.

The header is deliberately NOT added to ``template/instruction.md``: it is
rendered verbatim into the agent's prompt. It IS added to the helper scripts,
which are copied into every rendered task's tests/ -- a comment line there is
harmless and still true about where the file came from.
"""

from __future__ import annotations

import argparse
import os
import re
import stat
import sys
from pathlib import Path

_DATASMITH_ROOT = Path(__file__).resolve().parents[1]
SRC = _DATASMITH_ROOT / "src" / "datasmith" / "harbor_adapter"
DEFAULT_HARBOR = Path(os.environ.get("HARBOR_ROOT", str(Path.home() / "harbor")))
DST_REL = Path("adapters") / "formulacode"

# Renderer modules the mirror needs. stamp.py digests records.py, so it ships too.
RENDER_MODULES = ("adapter.py", "utils.py", "records.py", "stamp.py")

# Files under the mirror's template/ that are harbor's own (kept, never generated).
PRESERVED_TEMPLATE_FILES = frozenset({"task.toml"})

# Generated files that get no header.
NO_HEADER = frozenset({"template/instruction.md"})

HEADER_TEXT = (
    "DO NOT EDIT — generated from datasmith/src/datasmith/harbor_adapter; "
    "run scripts/sync_harbor_adapter.py --write"
)

_IMPORT_RE = re.compile(r"^(\s*)from datasmith\.harbor_adapter\.(\w+) import", re.MULTILINE)
_IMPORT_PKG_RE = re.compile(r"^(\s*)from datasmith\.harbor_adapter import", re.MULTILINE)


def _rewrite_imports(text: str) -> str:
    text = _IMPORT_RE.sub(r"\1from \2 import", text)
    if _IMPORT_PKG_RE.search(text):
        raise SystemExit("cannot mirror a `from datasmith.harbor_adapter import ...`; import the submodule")
    return text


def _with_header(rel: str, text: str) -> str:
    if rel in NO_HEADER:
        return text
    name = Path(rel).name
    if name.endswith((".py", ".sh")) or name == "Dockerfile":
        comment = f"# {HEADER_TEXT}\n"
    else:
        raise SystemExit(f"no header rule for generated file {rel}")
    lines = text.split("\n")
    # Keep a shebang and a PEP 263 coding declaration in the first two lines.
    keep = 0
    if lines and lines[0].startswith("#!"):
        keep = 1
        if len(lines) > 1 and "coding" in lines[1] and lines[1].startswith("#"):
            keep = 2
    return "\n".join(lines[:keep]) + ("\n" if keep else "") + comment + "\n".join(lines[keep:])


def generated_files() -> dict[str, tuple[Path, bytes]]:
    """{relative path in the mirror: (source path, expected bytes)}."""
    out: dict[str, tuple[Path, bytes]] = {}
    for name in RENDER_MODULES:
        src = SRC / name
        text = _rewrite_imports(src.read_text())
        out[name] = (src, _with_header(name, text).encode())
    for src in sorted((SRC / "template").rglob("*")):
        if not src.is_file() or "__pycache__" in src.parts or src.suffix in (".pyc", ".pyo"):
            continue
        rel = src.relative_to(SRC).as_posix()
        out[rel] = (src, _with_header(rel, src.read_text()).encode())
    return out


def _mirror_template_files(dst: Path) -> set[str]:
    found: set[str] = set()
    tpl = dst / "template"
    if not tpl.is_dir():
        return found
    for p in tpl.rglob("*"):
        if not p.is_file() or "__pycache__" in p.parts or p.suffix in (".pyc", ".pyo"):
            continue
        rel = p.relative_to(dst).as_posix()
        if Path(rel).name in PRESERVED_TEMPLATE_FILES and Path(rel).parent.as_posix() == "template":
            continue
        found.add(rel)
    return found


def check(dst: Path) -> list[str]:
    """Paths whose bytes differ from the generated output, plus stale generated files."""
    expected = generated_files()
    problems: list[str] = []
    for rel, (_, data) in expected.items():
        target = dst / rel
        if not target.is_file():
            problems.append(f"MISSING  {rel}")
        elif target.read_bytes() != data:
            problems.append(f"DIFFERS  {rel}")
    for rel in sorted(_mirror_template_files(dst) - set(expected)):
        problems.append(f"STALE    {rel} (not generated from the template; remove it or add it upstream)")
    return problems


def write(dst: Path) -> tuple[int, int, list[str]]:
    expected = generated_files()
    written = unchanged = 0
    for rel, (src, data) in expected.items():
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_file() and target.read_bytes() == data:
            unchanged += 1
        else:
            target.write_bytes(data)
            written += 1
        # Executable bit follows the source (setup.sh / test.sh / solve.sh).
        mode = src.stat().st_mode
        if mode & stat.S_IXUSR:
            target.chmod(mode & 0o777)
    removed: list[str] = []
    for rel in sorted(_mirror_template_files(dst) - set(expected)):
        (dst / rel).unlink()
        removed.append(rel)
    return written, unchanged, removed


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="regenerate the mirror in place")
    mode.add_argument("--check", action="store_true", help="verify the mirror byte-for-byte; exit 1 on drift")
    ap.add_argument(
        "--harbor",
        type=Path,
        default=DEFAULT_HARBOR,
        help=f"harbor checkout (default $HARBOR_ROOT or {DEFAULT_HARBOR})",
    )
    args = ap.parse_args(argv)

    dst = args.harbor / DST_REL
    if not (args.harbor / "adapters").is_dir():
        print(f"[sync] {args.harbor} does not look like a harbor checkout (no adapters/)", file=sys.stderr)
        return 2

    if args.check:
        problems = check(dst)
        for line in problems:
            print(line)
        n = len(generated_files())
        print(f"[sync] --check {dst}: {n} generated file(s), {len(problems)} problem(s)")
        return 1 if problems else 0

    written, unchanged, removed = write(dst)
    for rel in removed:
        print(f"REMOVED  {rel}")
    print(f"[sync] --write {dst}: {written} written, {unchanged} unchanged, {len(removed)} removed")
    problems = check(dst)
    if problems:  # should be impossible; make it loud if it ever is
        for line in problems:
            print(line, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
