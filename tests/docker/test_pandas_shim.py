"""Our pandas shim must be valid Python, and must not be planted in every repo.

`run-tests.sh` wrote `jinja_patch_plugin_pandas.py` into EVERY repository root
and passed `-p jinja_patch_plugin_pandas` to every pytest run.

Two defects. The body was indented at module level, so the file raised
IndentationError and was never importable -- it never worked for pandas either.
And planting it everywhere means any project that collects its rootdir tries to
import it. CalebBell/fluids#38 died on `ERROR jinja_patch_plugin_pandas.py`:
our own file, in our own build, breaking a repository that has nothing to do
with pandas.
"""

from __future__ import annotations

import ast
from pathlib import Path

_TEMPLATE = Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "run-tests.sh"


def _shim_source() -> str:
    lines = _TEMPLATE.read_text(encoding="utf-8").splitlines()
    start = next(i for i, ln in enumerate(lines) if "cat > jinja_patch_plugin_pandas.py" in ln)
    end = next(i for i in range(start + 1, len(lines)) if lines[i].rstrip() == "PY")
    return "\n".join(lines[start + 1 : end])


def test_the_shim_is_valid_python() -> None:
    """It was an IndentationError for the life of the project."""
    ast.parse(_shim_source())


def test_the_shim_is_not_indented_at_module_level() -> None:
    body = _shim_source()
    first = next(ln for ln in body.splitlines() if ln.strip())
    assert not first.startswith((" ", "\t")), f"module-level indent: {first!r}"


def test_the_shim_is_written_only_for_pandas() -> None:
    text = _TEMPLATE.read_text(encoding="utf-8")
    write_at = text.index("cat > jinja_patch_plugin_pandas.py")
    guard_at = text.index('if [ "${IMPORT_NAME:-}" = "pandas" ]')
    assert guard_at < write_at, "the shim must be written inside the pandas guard"


def test_the_plugin_flag_is_not_passed_unconditionally() -> None:
    """Every other repo must invoke pytest without our plugin."""
    text = _TEMPLATE.read_text(encoding="utf-8")
    assert '--extra-args "-p jinja_patch_plugin_pandas' not in text
    assert '--extra-args "$_PYTEST_PLUGIN_ARGS' in text
    assert '_PYTEST_PLUGIN_ARGS=""' in text, "the flag must default to empty"
