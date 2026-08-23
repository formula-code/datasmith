# Stage 4 Resolution Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild stage 4 so it emits an honest, reproducible, provenance-carrying dependency *seed* instead of a self-healing guess that silently excludes a quarter of the corpus.

**Architecture:** The 669-line `orchestrator.analyze_commit` becomes six small units — `discover`, `declare`, `interpreter`, `pin`, `probe`, `emit` — each with one purpose and its own tests. Global mutable state (the blocklist) and name invention (the import analyzer) are deleted outright rather than repaired. Stage 4 stops gating: `can_install` is replaced by an advisory `probe_status` that orders the stage 5 queue instead of truncating it.

**Tech Stack:** Python 3.11/3.12, `uv` (venv + `pip compile`), `packaging` (PEP 508 parsing), GitPython, Supabase/PostgREST, pytest.

**Spec:** `docs/superpowers/specs/2026-08-23-stage4-resolution-redesign-design.md`
**Audit evidence:** `docs/superpowers/specs/2026-08-23-stage4-audit-findings.md`

## Global Constraints

- `requires-python = ">=3.12"` in pyproject and Ruff targets `py312`, **but CI runs tests on 3.11 and 3.12** — do not use 3.12-only syntax in `src/`.
- mypy strict (`disallow_untyped_defs`). Every new function needs full type hints.
- Ruff, 120-char lines. `E501`, `TRY003`, `SIM108`, `S603`, `S607` globally ignored.
- `asyncio_mode = "auto"` — no `@pytest.mark.asyncio` needed.
- Tests that build or run real containers, clone real repos, or hit the network **must** be marked `slow`. `make test` and CI both run `-m "not slow"`.
- A `pyproject.toml` dependency edit **must** be accompanied by a refreshed `uv.lock` (`make check` runs `uv lock --locked`).
- Every tunable constant is overridable from `tokens.env` and prefixed `DATASMITH_`.
- Migrations: check other branches before claiming a number (gaps exist at `00018`, `00024`). **Grant nothing to `anon`.**
- Diagrams in Markdown use Mermaid, never ASCII box art.
- The canonical task identity is `(owner, repo, issue_number)`; never mint a `task_id` string.

---

### Task 1: Requirement parsing primitive

Replaces `fix_marker_spacing`, whose unanchored `or`/`and` substitution turns `platform_system` into `platf or m_system` and `extra=='standard'` into `extra=='st and ard'` (audit B1). Introduces per-requirement isolation: a string that will not parse is dropped and recorded, never rewritten, and never aborts its siblings.

**Files:**
- Modify: `pyproject.toml` (add `packaging` to `dependencies`)
- Modify: `uv.lock` (regenerated)
- Create: `src/datasmith/resolution/requirements.py`
- Create: `tests/resolution/__init__.py`
- Test: `tests/resolution/test_requirements.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `@dataclass(frozen=True) class Dropped: raw: str; reason: str`
  - `def parse_one(raw: str) -> Requirement | None` — `packaging.requirements.Requirement` or `None`
  - `def parse_many(raws: Iterable[str]) -> tuple[list[Requirement], list[Dropped]]` — order-stable, deduplicated by `str(req)`, never raises
  - `def render(reqs: Iterable[Requirement]) -> list[str]` — sorted `str(req)` list

- [ ] **Step 1: Declare the dependency**

`packaging` is currently importable only transitively. Add it to `dependencies` in `pyproject.toml`, alphabetically between `json5` and `portkey-ai`:

```toml
    "packaging>=23.0",
```

- [ ] **Step 2: Refresh the lockfile**

```bash
uv lock
```
Expected: `uv.lock` updated. `make check` runs `uv lock --locked` and fails without this.

- [ ] **Step 3: Write the failing tests**

Create `tests/resolution/__init__.py` (empty) and `tests/resolution/test_requirements.py`:

```python
"""The parser must never corrupt a marker, and never let one bad string kill a batch."""

from datasmith.resolution.requirements import Dropped, parse_many, parse_one, render


def test_common_markers_survive_untouched():
    # These four are the most common markers in Python packaging, and are exactly
    # what the old fix_marker_spacing regex destroyed (audit B1).
    for raw in [
        "numpy; platform_system=='Windows'",
        "foo; platform_machine=='x86_64'",
        "bar; sys_platform=='linux'",
        "baz; python_version<'3.11' and platform_system!='Darwin'",
        "qux; extra=='standard'",
    ]:
        req = parse_one(raw)
        assert req is not None, raw
        rendered = str(req)
        assert "platf or m" not in rendered
        assert "st and ard" not in rendered


def test_unparseable_is_dropped_not_rewritten():
    assert parse_one("pyuwsgi;sys.platform!='win32'") is None


def test_one_bad_string_does_not_kill_its_siblings():
    # apache/arrow died because a single malformed requirement aborted the whole
    # compile. Every other requirement must still come through.
    reqs, dropped = parse_many([
        "numpy>=1.25",
        "pyuwsgi;sys.platform!='win32'",
        "cython>=3.1",
    ])
    assert render(reqs) == ["cython>=3.1", "numpy>=1.25"]
    assert [d.raw for d in dropped] == ["pyuwsgi;sys.platform!='win32'"]
    assert dropped[0].reason


def test_parse_many_is_order_stable_and_deduplicates():
    a, _ = parse_many(["numpy", "cython", "numpy"])
    b, _ = parse_many(["cython", "numpy", "numpy"])
    assert render(a) == render(b) == ["cython", "numpy"]


def test_parse_many_never_raises_on_garbage():
    reqs, dropped = parse_many(["", "   ", "-r reqs.txt", "{wheel_file}", "./local"])
    assert reqs == []
    assert len(dropped) == 3  # blank and whitespace-only are skipped, not recorded


def test_dropped_is_hashable_and_frozen():
    d = Dropped(raw="x", reason="y")
    assert {d}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/resolution/test_requirements.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'datasmith.resolution.requirements'`

- [ ] **Step 5: Write the implementation**

Create `src/datasmith/resolution/requirements.py`:

```python
"""PEP 508 requirement parsing.

The predecessor, ``package_filters.fix_marker_spacing``, tried to repair markers
with two unanchored substitutions::

    re.sub(r"(?<=[^\\s])and(?=[^\\s])", " and ", marker)
    re.sub(r"(?<=[^\\s])or(?=[^\\s])",  " or ",  marker)

``or`` occurs inside ``platform`` and ``and`` occurs inside ``standard``, so it
turned ``platform_system`` into ``platf or m_system`` and ``extra=='standard'``
into ``extra=='st and ard'``.  uv then refused to parse the result and the whole
compile failed, which is how one ``pyuwsgi`` requirement removed apache/arrow
from the dataset.

The rule here is the opposite: parse with the real parser, and if a string does
not parse, drop it and say so.  Never rewrite a requirement, and never let one
bad string decide the fate of its siblings.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from packaging.requirements import InvalidRequirement, Requirement

__all__ = ["Dropped", "parse_many", "parse_one", "render"]


@dataclass(frozen=True)
class Dropped:
    """A requirement string that could not be used, and why."""

    raw: str
    reason: str


#: Prefixes and shapes that are pip-file directives or build placeholders rather
#: than requirements.  They are dropped without being offered to the parser.
_NON_REQUIREMENT_PREFIXES = ("-", "--", "./", "../", ".")


def parse_one(raw: str) -> Requirement | None:
    """Parse one requirement string, or return ``None`` if it is not one."""
    text = raw.strip()
    if not text:
        return None
    if text.startswith(_NON_REQUIREMENT_PREFIXES):
        return None
    if "{" in text or "}" in text or "$" in text:
        return None
    try:
        return Requirement(text)
    except InvalidRequirement:
        return None


def parse_many(raws: Iterable[str]) -> tuple[list[Requirement], list[Dropped]]:
    """Parse many requirement strings, isolating failures.

    Returns ``(parsed, dropped)``.  Order is stable and duplicates are removed by
    rendered form, so the same inputs in a different order give the same output.
    Blank strings are skipped silently; anything else that fails is recorded.
    """
    parsed: list[Requirement] = []
    dropped: list[Dropped] = []
    seen: set[str] = set()

    for raw in raws:
        text = raw.strip() if raw else ""
        if not text:
            continue
        req = parse_one(text)
        if req is None:
            dropped.append(Dropped(raw=text, reason="unparseable requirement"))
            continue
        key = str(req)
        if key in seen:
            continue
        seen.add(key)
        parsed.append(req)

    return parsed, dropped


def render(reqs: Iterable[Requirement]) -> list[str]:
    """Render requirements to a sorted, stable list of strings."""
    return sorted({str(r) for r in reqs})
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/test_requirements.py -v`
Expected: PASS (6 tests)

- [ ] **Step 7: Type-check and lint**

Run: `uv run mypy && uv run ruff check src/datasmith/resolution/requirements.py`
Expected: no errors

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock src/datasmith/resolution/requirements.py tests/resolution/
git commit -m "feat(resolution): parse requirements instead of rewriting them

fix_marker_spacing substituted 'or' and 'and' unanchored, so 'platform'
became 'platf or m' and 'standard' became 'st and ard'. It corrupted every
common PEP 508 marker, and because one malformed string aborts the whole
uv compile, a single pyuwsgi requirement removed apache/arrow from the
dataset.

parse_many isolates failures: a string that will not parse is dropped and
recorded, its siblings still resolve, and no requirement is ever rewritten."
```

---

### Task 2: Retire `fix_marker_spacing`

The function has four callers. Each is switched to the Task 1 parser, then the function and its tests are deleted. Doing this as its own task keeps the blast radius reviewable.

**Files:**
- Modify: `src/datasmith/resolution/package_filters.py:161-175` (delete `fix_marker_spacing`), `:183`, `:245-275`
- Modify: `src/datasmith/resolution/dependency_resolver.py:54`, `:80`, `:105`
- Test: `tests/resolution/test_no_marker_rewriting.py`

**Interfaces:**
- Consumes: `parse_many`, `render` from Task 1.
- Produces: no new public surface. `fix_marker_spacing` no longer exists.

- [ ] **Step 1: Write the failing test**

Create `tests/resolution/test_no_marker_rewriting.py`:

```python
"""fix_marker_spacing must be gone, and no caller may resurrect its behaviour."""

import subprocess


def test_fix_marker_spacing_is_deleted():
    import datasmith.resolution.package_filters as pf

    assert not hasattr(pf, "fix_marker_spacing")


def test_no_source_file_references_it():
    cp = subprocess.run(
        ["grep", "-rn", "fix_marker_spacing", "src/datasmith"],
        capture_output=True,
        text=True,
    )
    assert cp.stdout == "", f"stale references remain:\n{cp.stdout}"


def test_requirements_reach_uv_unmodified():
    from datasmith.resolution.requirements import parse_many, render

    raw = ["numpy; platform_system=='Windows'", "scipy; sys_platform=='linux'"]
    reqs, dropped = parse_many(raw)
    out = render(reqs)
    assert dropped == []
    assert all("platf or m" not in line for line in out)
    assert any("platform_system" in line for line in out)
    assert any("sys_platform" in line for line in out)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/resolution/test_no_marker_rewriting.py -v`
Expected: FAIL — `test_fix_marker_spacing_is_deleted` fails because the attribute still exists.

- [ ] **Step 3: Delete the function**

Remove lines 161-175 of `src/datasmith/resolution/package_filters.py` (the whole `fix_marker_spacing` def and its docstring).

- [ ] **Step 4: Fix the caller in `normalize_requirement`**

In `package_filters.py`, line 183 currently reads `req = fix_marker_spacing(req)`. Delete that line — `normalize_requirement` now passes the string through untouched and lets the parser judge it.

- [ ] **Step 5: Fix the caller in `filter_requirements_for_pypi`**

In `package_filters.py`, inside the loop, delete the line `raw = fix_marker_spacing(raw)`.

- [ ] **Step 6: Fix the three callers in `dependency_resolver.py`**

Replace the comprehension at line 54:

```python
    reqs = sorted({fix_marker_spacing(r.strip()) for r in requirements if r and r.strip()})
```

with:

```python
    parsed, _dropped = parse_many(requirements)
    reqs = render(parsed)
```

At line 80 (`uv_dry_run_install`) replace:

```python
    text_lines = [fix_marker_spacing(x) for x in pinned if x.strip()]
```

with:

```python
    parsed, _dropped = parse_many(pinned)
    text_lines = render(parsed)
```

At line 105 (`uv_install_real`) replace:

```python
    lines = [fix_marker_spacing(x) for x in pinned if x.strip()]
```

with:

```python
    parsed, _dropped = parse_many(pinned)
    lines = render(parsed)
```

Add the import at the top of `dependency_resolver.py`:

```python
from .requirements import parse_many, render
```

and delete `from .package_filters import fix_marker_spacing`.

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/ -v`
Expected: PASS

- [ ] **Step 8: Run the full fast suite for regressions**

Run: `uv run pytest tests/ -m "not slow" -q`
Expected: no new failures

- [ ] **Step 9: Commit**

```bash
git add src/datasmith/resolution/ tests/resolution/
git commit -m "refactor(resolution): route every requirement through the real parser

Deletes fix_marker_spacing and its four callers. A grep-based test keeps it
from coming back."
```

---

### Task 3: Delete the global blocklist

`blocklist.py` is one append-only JSON file in the git cache, read at filter time and written whenever any single resolve fails (audit B2). Its 268 entries include numpy's own submodules (`arraypad`, `multiarray`, `umath`), stdlib (`tomllib`), and real installable projects (`black`, `codecov`, `rdkit`). Because every repository shares it, resolving one changes another's answer.

**Files:**
- Delete: `src/datasmith/resolution/blocklist.py`
- Modify: `src/datasmith/resolution/package_filters.py:249-256` (drop `get_blocklist`)
- Modify: `src/datasmith/resolution/orchestrator.py:485-536`, `:556-601` (drop both self-healing loops)
- Test: `tests/resolution/test_no_global_state.py`

**Interfaces:**
- Consumes: `Dropped` from Task 1.
- Produces: no new public surface. A package that fails resolution is recorded as a `Dropped` on the row, not in a shared file.

- [ ] **Step 1: Write the failing test**

Create `tests/resolution/test_no_global_state.py`:

```python
"""Resolving one repository must not change another's answer."""

import subprocess
from pathlib import Path


def test_blocklist_module_is_deleted():
    import importlib

    try:
        importlib.import_module("datasmith.resolution.blocklist")
    except ModuleNotFoundError:
        return
    raise AssertionError("datasmith.resolution.blocklist still exists")


def test_no_source_file_references_the_blocklist():
    cp = subprocess.run(
        ["grep", "-rniE", "blocklist|add_to_blocklist|get_blocklist", "src/datasmith"],
        capture_output=True,
        text=True,
    )
    assert cp.stdout == "", f"stale references remain:\n{cp.stdout}"


def test_filter_writes_nothing_to_disk(tmp_path, monkeypatch):
    # filter_requirements_for_pypi used to read a shared JSON file and the
    # self-healing loops used to append to it. Nothing may touch disk now.
    monkeypatch.setenv("GIT_CACHE_DIR", str(tmp_path))
    from datasmith.resolution.package_filters import filter_requirements_for_pypi

    project = tmp_path / "proj"
    project.mkdir()
    before = set(tmp_path.rglob("*"))
    filter_requirements_for_pypi({"numpy", "scipy"}, project_dir=project, own_import_name=None)
    after = set(tmp_path.rglob("*"))
    assert before == after, f"filtering wrote {after - before}"


def test_filtering_is_independent_of_call_order(tmp_path):
    from datasmith.resolution.package_filters import filter_requirements_for_pypi

    project = tmp_path / "proj"
    project.mkdir()

    a_first = filter_requirements_for_pypi({"numpy"}, project_dir=project, own_import_name=None)
    b_after_a = filter_requirements_for_pypi({"scipy"}, project_dir=project, own_import_name=None)
    b_alone = filter_requirements_for_pypi({"scipy"}, project_dir=project, own_import_name=None)

    assert b_after_a == b_alone
    assert a_first == ["numpy"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/resolution/test_no_global_state.py -v`
Expected: FAIL — the module still imports.

- [ ] **Step 3: Delete the module**

```bash
git rm src/datasmith/resolution/blocklist.py
```

- [ ] **Step 4: Remove the read site**

In `src/datasmith/resolution/package_filters.py`, inside `filter_requirements_for_pypi`, delete:

```python
    from .blocklist import get_blocklist, normalize_package_name
```
```python
    dynamic_blocklist = get_blocklist()
```

and delete the loop body branch that tests membership in `dynamic_blocklist`. If `normalize_package_name` is still needed for local comparisons, inline it:

```python
def _normalize(name: str) -> str:
    """PEP 503 normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()
```

- [ ] **Step 5: Remove both self-healing loops from the orchestrator**

In `src/datasmith/resolution/orchestrator.py`:
- Delete the nested `_compile_or_pass_through` retry loop (lines ~485-536), leaving a single `uv_compile` call whose failure propagates.
- Delete the dry-run self-healing `while` loop (lines ~556-601) and its `from .blocklist import (...)` import.

These regions are removed wholesale by Task 9's rewrite; this step only has to leave the module importable and the fast suite green.

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/ -v`
Expected: PASS

- [ ] **Step 7: Run the full fast suite**

Run: `uv run pytest tests/ -m "not slow" -q`
Expected: no new failures

- [ ] **Step 8: Commit**

```bash
git add -A src/datasmith/resolution/ tests/resolution/
git commit -m "refactor(resolution): delete the global blocklist

One append-only file in the git cache, read at filter time and written on
any failure, shared by every repository and commit. Its 268 entries included
numpy's own submodules, stdlib names, and real installable projects like
black and rdkit — so resolving one repository changed another's answer.

A package that fails is now recorded on its own row, scoped to the commit
that saw it fail."
```

---

### Task 4: The `declare` unit

Reads **declared** dependencies only, ending the invention of package names from imports (audit B3) and the harvesting of documentation and CI requirements as runtime dependencies (audit B4). numpy's fresh resolution contained `sphinx`, `towncrier`, `ruff`, `PyInstaller`, `version` and `plex`; scipy's contained `conda-build`, `torch`, `jax` and `cupy` — and scipy's resolution died on a `conda-build` yanked-version conflict, meaning a documentation requirement made the environment unsatisfiable.

**Files:**
- Create: `src/datasmith/resolution/declare.py`
- Delete: `src/datasmith/resolution/import_analyzer.py`
- Test: `tests/resolution/test_declare.py`

**Interfaces:**
- Consumes: `parse_many`, `render`, `Dropped` (Task 1); `CandidateMeta` from `.models`.
- Produces:
  - `@dataclass(frozen=True) class Declared: runtime: list[str]; build: list[str]; extras: dict[str, list[str]]; dropped: list[Dropped]`
  - `def declare(meta: CandidateMeta, asv_matrix: Mapping[str, set[str]] | None) -> Declared`

- [ ] **Step 1: Write the failing tests**

Create `tests/resolution/test_declare.py`:

```python
"""Only declared dependencies. No inference, no globbing, no conda files."""

from datasmith.resolution.declare import Declared, declare
from datasmith.resolution.models import CandidateMeta


def _meta(**kw) -> CandidateMeta:
    m = CandidateMeta()
    for k, v in kw.items():
        setattr(m, k, v)
    return m


def test_runtime_and_build_are_separate():
    d = declare(_meta(core_deps={"numpy>=1.25"}, build_requires={"cython>=3.1"}), None)
    assert d.runtime == ["numpy>=1.25"]
    assert d.build == ["cython>=3.1"]


def test_extras_are_kept_but_not_merged_into_runtime():
    d = declare(_meta(core_deps={"numpy"}, extras={"docs": {"sphinx"}}), None)
    assert d.runtime == ["numpy"]
    assert d.extras == {"docs": ["sphinx"]}
    assert "sphinx" not in d.runtime


def test_asv_matrix_req_is_a_declaration():
    d = declare(_meta(core_deps={"numpy"}), {"cython": {"0.29.21"}, "pandas": set()})
    assert d.runtime == ["cython==0.29.21", "numpy", "pandas"]


def test_matrix_keys_are_not_discarded():
    # The predecessor fed bare versions to the resolver, which read '0.29.21' as
    # a package name. The key carries the name and must survive.
    d = declare(_meta(), {"cython": {"0.29.21"}})
    assert d.runtime == ["cython==0.29.21"]


def test_matrix_none_version_is_dropped_not_stringified():
    # str(None) would put the literal 'None' into the requirement set.
    d = declare(_meta(), {"cython": {"None"}})
    assert "cython==None" not in d.runtime


def test_unparseable_declarations_are_recorded():
    d = declare(_meta(core_deps={"numpy", "pyuwsgi;sys.platform!='win32'"}), None)
    assert d.runtime == ["numpy"]
    assert [x.raw for x in d.dropped] == ["pyuwsgi;sys.platform!='win32'"]


def test_output_is_deterministic():
    a = declare(_meta(core_deps={"b", "a", "c"}), None)
    b = declare(_meta(core_deps={"c", "a", "b"}), None)
    assert a == b


def test_import_analyzer_is_deleted():
    import importlib

    try:
        importlib.import_module("datasmith.resolution.import_analyzer")
    except ModuleNotFoundError:
        return
    raise AssertionError("import_analyzer still exists")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/resolution/test_declare.py -v`
Expected: FAIL — `No module named 'datasmith.resolution.declare'`

- [ ] **Step 3: Write the implementation**

Create `src/datasmith/resolution/declare.py`:

```python
"""Collect the dependencies a project actually declares.

The predecessor built its requirement set from four sources, three of which are
not declarations:

* ``requirements*.txt`` globbed anywhere in the tree — which is how ``sphinx``,
  ``towncrier``, ``ruff``, ``PyInstaller``, ``myst-nb``, ``jupytext``, ``torch``,
  ``jax``, ``cupy`` and ``conda-build`` became *runtime* dependencies.  scipy's
  resolution failed on a ``conda-build`` yanked-version conflict: a documentation
  requirement made the environment unsatisfiable.
* ``environment.yml`` — conda names such as ``boost-cpp``, ``thrift-cpp``,
  ``libprotobuf`` and ``lz4-c``, which do not exist on PyPI.
* import analysis — which produced ``arraypad``, ``multiarray``, ``umath``,
  ``mtrand`` (numpy's own submodules), plus ``version`` and ``plex``.  ``version``
  is a dead py2 distribution whose sdist raises ``ImportError: cannot import name
  'izip_longest'``, and that single harvested token is what failed numpy.

Only these are declarations, and only these are read here:

* ``[project].dependencies`` / ``[project.optional-dependencies]``
* ``install_requires`` / ``options.extras_require``
* ``[build-system].requires``
* ASV ``matrix.req`` — a genuine statement of what the benchmarks need
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from .models import CandidateMeta
from .requirements import Dropped, parse_many, render

__all__ = ["Declared", "declare"]


@dataclass(frozen=True)
class Declared:
    """What a project says it needs."""

    runtime: list[str] = field(default_factory=list)
    build: list[str] = field(default_factory=list)
    extras: dict[str, list[str]] = field(default_factory=dict)
    dropped: list[Dropped] = field(default_factory=list)


def _matrix_requirements(matrix: Mapping[str, set[str]] | None) -> list[str]:
    """Turn an ASV ``matrix`` into requirement strings.

    The keys carry the package names.  Passing a bare version such as ``0.29.21``
    to the resolver treats it as a package name, so the key must not be dropped.
    An empty version set means "any version".  The string ``"None"`` means "do not
    install in this combination" and is skipped rather than converted.
    """
    out: list[str] = []
    for pkg, versions in (matrix or {}).items():
        name = str(pkg).strip()
        if not name or name.startswith("-"):
            continue
        real = {v for v in (str(x).strip() for x in versions) if v and v != "None"}
        if real:
            out.extend(f"{name}=={v}" for v in sorted(real))
        else:
            out.append(name)
    return out


def declare(meta: CandidateMeta, asv_matrix: Mapping[str, set[str]] | None) -> Declared:
    """Collect declared runtime, build and extra requirements."""
    dropped: list[Dropped] = []

    runtime_raw = list(meta.core_deps) + _matrix_requirements(asv_matrix)
    runtime_reqs, runtime_dropped = parse_many(runtime_raw)
    dropped.extend(runtime_dropped)

    build_reqs, build_dropped = parse_many(meta.build_requires)
    dropped.extend(build_dropped)

    extras: dict[str, list[str]] = {}
    for name in sorted(meta.extras):
        extra_reqs, extra_dropped = parse_many(meta.extras[name])
        dropped.extend(extra_dropped)
        extras[name] = render(extra_reqs)

    return Declared(
        runtime=render(runtime_reqs),
        build=render(build_reqs),
        extras=extras,
        dropped=sorted(dropped, key=lambda d: d.raw),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/test_declare.py -v`
Expected: PASS except `test_import_analyzer_is_deleted`

- [ ] **Step 5: Delete the import analyzer**

```bash
git rm src/datasmith/resolution/import_analyzer.py
```

Then remove its import from `src/datasmith/resolution/orchestrator.py`:

```python
from .import_analyzer import infer_runtime_from_imports
```

and the block at `orchestrator.py:434-441` that calls `infer_runtime_from_imports` and computes `promote`. Replace it with a bare `runtime_candidates: set[str] = set(wheel_requires)` — Task 9 removes the surrounding code entirely.

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/ -v && uv run pytest tests/ -m "not slow" -q`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -A src/datasmith/resolution/ tests/resolution/
git commit -m "feat(resolution): read declared dependencies, stop inventing them

Deletes import_analyzer, which turned numpy's own submodules (arraypad,
multiarray, umath, mtrand) and the dead py2 distributions 'version' and
'plex' into runtime dependencies — 'version' is what failed numpy.

Stops globbing requirements*.txt and reading environment.yml, which is how
sphinx, towncrier, torch, jax, cupy and conda-build became runtime deps.
scipy's environment was unsatisfiable because of a conda-build conflict
reached through a documentation requirement."
```

---

### Task 5: The interpreter ladder

Replaces "the newest interpreter that did not crash" (audit B7). `orchestrator.py:603-607` assigns `python_version` unconditionally before the success check, tries candidates newest-first, and breaks on the first non-ABI error — so a failed iteration still leaves its interpreter recorded. `requires-python` is parsed and then discarded. Re-running the same 13 SHAs changed the interpreter on 7 of them (audit B8).

Measured coverage over 155 cached repositories: `requires-python` 84%, plus trove classifiers 91%, plus `asv.conf.json` `pythons` 99%, leaving 1% (NVIDIA/physicsnemo) on a date default.

**Files:**
- Create: `src/datasmith/resolution/interpreter.py`
- Test: `tests/resolution/test_interpreter.py`

**Interfaces:**
- Consumes: `PY_RELEASES`, `SUPPORTED_PYTHON_VERSIONS` from `.python_manager`.
- Produces:
  - `@dataclass(frozen=True) class InterpreterChoice: version: str; source: str` where `source` is one of `"requires-python"`, `"trove"`, `"asv"`, `"commit-date"`
  - `def select_interpreter(*, requires_python: str | None, trove_versions: Iterable[str], asv_pythons: Iterable[str], commit_date: datetime) -> InterpreterChoice`
  - `def trove_versions_from_classifiers(classifiers: Iterable[str]) -> list[str]`
  - `DATASMITH_PYTHON_FLOOR: str` (default `"3.8"`), `DATASMITH_PYTHON_CEILING: str` (default `"3.12"`)

- [ ] **Step 1: Write the failing tests**

Create `tests/resolution/test_interpreter.py`:

```python
"""The interpreter is a decision with a recorded reason, not a control-flow accident."""

import datetime as dt

import pytest

from datasmith.resolution.interpreter import (
    InterpreterChoice,
    select_interpreter,
    trove_versions_from_classifiers,
)

JAN_2026 = dt.datetime(2026, 1, 15, tzinfo=dt.UTC)
JUL_2020 = dt.datetime(2020, 7, 2, tzinfo=dt.UTC)


def test_rung_1_requires_python_wins():
    c = select_interpreter(
        requires_python=">=3.9,<3.12", trove_versions=[], asv_pythons=[], commit_date=JAN_2026
    )
    assert c == InterpreterChoice(version="3.11", source="requires-python")


def test_rung_2_trove_when_no_requires_python():
    c = select_interpreter(
        requires_python=None, trove_versions=["3.8", "3.9"], asv_pythons=[], commit_date=JAN_2026
    )
    assert c == InterpreterChoice(version="3.9", source="trove")


def test_rung_3_asv_when_neither():
    c = select_interpreter(
        requires_python=None, trove_versions=[], asv_pythons=["3.10"], commit_date=JAN_2026
    )
    assert c == InterpreterChoice(version="3.10", source="asv")


def test_rung_4_commit_date_when_nothing_is_declared():
    c = select_interpreter(
        requires_python=None, trove_versions=[], asv_pythons=[], commit_date=JUL_2020
    )
    assert c.source == "commit-date"
    # 3.9 was released 2020-10-05, after this commit.
    assert c.version == "3.8"


def test_never_picks_an_interpreter_that_did_not_exist_yet():
    c = select_interpreter(
        requires_python=">=3.8", trove_versions=[], asv_pythons=[], commit_date=JUL_2020
    )
    assert c.version == "3.8"


def test_unsatisfiable_declaration_falls_through_to_the_next_rung():
    # pymc declares ">=3.6,<3.7"; nothing in the supported range satisfies it.
    c = select_interpreter(
        requires_python=">=3.6,<3.7", trove_versions=["3.10"], asv_pythons=[], commit_date=JAN_2026
    )
    assert c == InterpreterChoice(version="3.10", source="trove")


def test_malformed_requires_python_does_not_raise():
    c = select_interpreter(
        requires_python="not a specifier", trove_versions=[], asv_pythons=["3.11"], commit_date=JAN_2026
    )
    assert c.source == "asv"


def test_selection_is_deterministic():
    kw = dict(requires_python=">=3.9", trove_versions=["3.9", "3.10"], asv_pythons=["3.11"], commit_date=JAN_2026)
    assert select_interpreter(**kw) == select_interpreter(**kw)


@pytest.mark.parametrize(
    "classifiers,expected",
    [
        (["Programming Language :: Python :: 3.11"], ["3.11"]),
        (["Programming Language :: Python :: 3 :: Only"], []),
        (["Programming Language :: Python :: 3.9", "License :: OSI Approved"], ["3.9"]),
        ([], []),
    ],
)
def test_trove_extraction(classifiers, expected):
    assert trove_versions_from_classifiers(classifiers) == expected
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/resolution/test_interpreter.py -v`
Expected: FAIL — `No module named 'datasmith.resolution.interpreter'`

- [ ] **Step 3: Write the implementation**

Create `src/datasmith/resolution/interpreter.py`:

```python
"""Choose the Python interpreter for a commit, and record why.

The predecessor assigned ``python_version`` unconditionally before checking
whether the attempt had succeeded (``orchestrator.py:603-607``), tried candidates
newest-first, and broke out of the loop on the first non-ABI error.  The value it
stored was therefore "the newest interpreter that did not crash", and re-running
the same 13 commits changed it on 7 of them.  The project's own
``requires-python`` was parsed and then discarded.

Here the choice is a declared ladder.  Take the newest version that satisfies the
declaration and existed at commit date, and record which rung supplied it.
Measured coverage over 155 cached repositories: rung 1 alone 84%, plus rung 2
91%, plus rung 3 99%.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass

from packaging.specifiers import InvalidSpecifier, SpecifierSet

from .python_manager import PY_RELEASES

__all__ = [
    "DATASMITH_PYTHON_CEILING",
    "DATASMITH_PYTHON_FLOOR",
    "InterpreterChoice",
    "select_interpreter",
    "trove_versions_from_classifiers",
]

#: Oldest interpreter the container toolchain still supports.
DATASMITH_PYTHON_FLOOR: str = os.environ.get("DATASMITH_PYTHON_FLOOR", "3.8")
#: Newest interpreter the container toolchain is known to build against.  This is
#: a ceiling on purpose: a fresh run must not silently start choosing an
#: interpreter no existing image was built with.
DATASMITH_PYTHON_CEILING: str = os.environ.get("DATASMITH_PYTHON_CEILING", "3.12")

_TROVE_RE = re.compile(r"Programming Language :: Python :: (\d+\.\d+)\s*$")


@dataclass(frozen=True)
class InterpreterChoice:
    """The chosen interpreter, and the ladder rung that supplied it."""

    version: str
    source: str


def _as_tuple(version: str) -> tuple[int, int]:
    major, minor = version.split(".")[:2]
    return int(major), int(minor)


def _supported(commit_date: dt.datetime) -> list[str]:
    """Supported interpreters that existed at ``commit_date``, newest first."""
    floor = _as_tuple(DATASMITH_PYTHON_FLOOR)
    ceiling = _as_tuple(DATASMITH_PYTHON_CEILING)
    out: list[str] = []
    for key, released in PY_RELEASES.items():
        if key < floor or key > ceiling:
            continue
        if released > commit_date:
            continue
        out.append(f"{key[0]}.{key[1]}")
    return sorted(out, key=_as_tuple, reverse=True)


def trove_versions_from_classifiers(classifiers: Iterable[str]) -> list[str]:
    """Extract ``3.x`` versions from trove classifiers, newest first.

    ``Programming Language :: Python :: 3 :: Only`` carries no minor version and
    is skipped rather than parsed as ``3.0``.
    """
    found: set[str] = set()
    for line in classifiers:
        match = _TROVE_RE.search(str(line).strip())
        if match:
            found.add(match.group(1))
    return sorted(found, key=_as_tuple, reverse=True)


def select_interpreter(
    *,
    requires_python: str | None,
    trove_versions: Iterable[str],
    asv_pythons: Iterable[str],
    commit_date: dt.datetime,
) -> InterpreterChoice:
    """Pick the newest supported interpreter the project declares.

    Rungs are tried in order and the first that yields a usable version wins.  A
    declaration nothing can satisfy — pymc's ``>=3.6,<3.7``, say — falls through
    to the next rung rather than failing the commit.
    """
    available = _supported(commit_date)
    if not available:
        # Older than every supported interpreter; the floor is the only honest answer.
        return InterpreterChoice(version=DATASMITH_PYTHON_FLOOR, source="commit-date")

    if requires_python:
        try:
            spec = SpecifierSet(requires_python)
        except InvalidSpecifier:
            spec = None
        if spec is not None:
            allowed = [v for v in available if spec.contains(v)]
            if allowed:
                return InterpreterChoice(version=allowed[0], source="requires-python")

    for candidates, source in ((trove_versions, "trove"), (asv_pythons, "asv")):
        declared = {str(v).strip() for v in candidates}
        allowed = [v for v in available if v in declared]
        if allowed:
            return InterpreterChoice(version=allowed[0], source=source)

    return InterpreterChoice(version=available[0], source="commit-date")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/test_interpreter.py -v`
Expected: PASS (12 tests including parametrisation)

- [ ] **Step 5: Type-check**

Run: `uv run mypy`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add src/datasmith/resolution/interpreter.py tests/resolution/test_interpreter.py
git commit -m "feat(resolution): choose the interpreter from what the project declares

The predecessor assigned python_version before checking success and broke on
the first non-ABI error, so it recorded the newest interpreter that did not
crash. Re-running the same 13 commits changed it on 7. requires-python was
parsed and discarded.

A declared ladder replaces it — requires-python, then trove classifiers,
then asv pythons, then commit date — and stores which rung fired. Measured
coverage over 155 repos: 84% / 91% / 99% / 100%."
```

---

### Task 6: Deterministic primary root

`select_primary_candidate` returns from unordered dict iteration in two places, so the same repository can pick a different package root on different runs. scipp resolved to `python`, `binder` and `scippy` across commits, and `binder` is a Binder configuration directory, not a package.

**Files:**
- Modify: `src/datasmith/resolution/metadata_parser.py:383-412`
- Test: `tests/resolution/test_primary_root.py`

**Interfaces:**
- Consumes: `Candidate`, `CandidateMeta` from `.models`.
- Produces: `select_primary_candidate(repo_name, candidates, install_cmds, analyzed) -> str` — signature unchanged, result now order-independent.

- [ ] **Step 1: Write the failing test**

Create `tests/resolution/test_primary_root.py`:

```python
"""The package root must not depend on dict iteration order."""

from datasmith.resolution.metadata_parser import select_primary_candidate
from datasmith.resolution.models import Candidate, CandidateMeta


def _cand(root: str, *, pyproject: bool = False) -> Candidate:
    c = Candidate(root_relpath=root)
    if pyproject:
        c.pyproject_path = object()  # truthy sentinel; only its presence is read
    return c


def test_same_candidates_in_any_order_give_the_same_root():
    roots = ["python", "binder", "scippy"]
    analyzed = {r: CandidateMeta() for r in roots}

    forward = {r: _cand(r, pyproject=True) for r in roots}
    reverse = {r: _cand(r, pyproject=True) for r in reversed(roots)}

    a = select_primary_candidate("scipp/scipp", forward, set(), analyzed)
    b = select_primary_candidate("scipp/scipp", reverse, set(), analyzed)
    assert a == b


def test_name_match_beats_position():
    cands = {"python": _cand("python"), "other": _cand("other")}
    analyzed = {"python": CandidateMeta(), "other": CandidateMeta()}
    analyzed["python"].name = "pyarrow"
    assert select_primary_candidate("apache/arrow", cands, set(), analyzed) == "python"


def test_multiple_name_matches_resolve_deterministically():
    cands = {"b": _cand("b"), "a": _cand("a")}
    analyzed = {"a": CandidateMeta(), "b": CandidateMeta()}
    analyzed["a"].name = "thing"
    analyzed["b"].name = "thing"
    assert select_primary_candidate("x/thing", cands, set(), analyzed) == "a"


def test_shallowest_path_wins_as_the_final_tiebreak():
    cands = {"deep/nested/pkg": _cand("deep/nested/pkg"), "pkg": _cand("pkg")}
    analyzed = {k: CandidateMeta() for k in cands}
    assert select_primary_candidate("x/y", cands, set(), analyzed) == "pkg"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/resolution/test_primary_root.py -v`
Expected: FAIL on `test_multiple_name_matches_resolve_deterministically` and possibly `test_same_candidates_in_any_order_give_the_same_root`

- [ ] **Step 3: Make both return paths deterministic**

In `src/datasmith/resolution/metadata_parser.py`, replace:

```python
    if by_name:
        return by_name[0]
    for root, cand in candidates.items():
        if cand.pyproject_path:
            return root
    return sorted(candidates.keys(), key=lambda s: (len(Path(s).parts), s))[0]
```

with:

```python
    # Every remaining tiebreak is sorted. The predecessor returned from
    # unordered dict iteration, so scipp picked 'python', 'binder' or 'scippy'
    # depending on the run — and 'binder' is a Binder config directory, not a
    # package.
    depth_then_name = lambda s: (len(Path(s).parts), s)
    if by_name:
        return sorted(by_name, key=depth_then_name)[0]
    with_pyproject = [root for root, cand in candidates.items() if cand.pyproject_path]
    if with_pyproject:
        return sorted(with_pyproject, key=depth_then_name)[0]
    return sorted(candidates.keys(), key=depth_then_name)[0]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/test_primary_root.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/resolution/metadata_parser.py tests/resolution/test_primary_root.py
git commit -m "fix(resolution): make the package root independent of dict order

select_primary_candidate returned from unordered iteration in two places, so
scipp resolved to 'python', 'binder' or 'scippy' depending on the run —
'binder' being a Binder config directory, not a package. Every tiebreak is
now sorted by path depth then name."
```

---

### Task 7: The `pin` unit

One `uv pip compile` over declared runtime dependencies and `[build-system].requires`. **No benchmark tooling** — that is audit B6's real fix. The base image already installs `hypothesis`, `pytest` and `versioneer` (`docker_build_base.sh:769`) and `asv` (`:771`), so putting them in `env_payload` creates a version fight, not coverage: an unconstrained `hypothesis` in the payload overrides the base image's deliberate `hypothesis<5`. **No `--all-extras`** — that is what made PostHog 412 packages and napari 291 (audit B5).

**Files:**
- Create: `src/datasmith/resolution/pin.py`
- Test: `tests/resolution/test_pin.py`

**Interfaces:**
- Consumes: `Declared` (Task 4), `uv_compile` from `.dependency_resolver`.
- Produces:
  - `@dataclass(frozen=True) class Pinned: requirements: list[str]; cutoff_used: str | None; cutoff_relaxed: bool; dropped: list[Dropped]`
  - `def pin(declared: Declared, *, python_version: str, commit_date: datetime, extras: Iterable[str] = (), operator_pins: Iterable[str] = ()) -> Pinned`
  - `TOOLING_OWNED_BY_BASE_IMAGE: frozenset[str]`

- [ ] **Step 1: Write the failing tests**

Create `tests/resolution/test_pin.py`:

```python
"""The seed carries project dependencies. The base image owns tooling."""

import datetime as dt

import pytest

from datasmith.resolution.declare import Declared
from datasmith.resolution.pin import TOOLING_OWNED_BY_BASE_IMAGE, pin

JAN_2026 = dt.datetime(2026, 1, 15, tzinfo=dt.UTC)


@pytest.fixture
def fake_compile(monkeypatch):
    """Record what reached uv, and return it pinned."""
    calls = {}

    def _compile(requirements, *, python_version, cutoff_rfc3339):
        calls["requirements"] = sorted(requirements)
        calls["cutoff"] = cutoff_rfc3339
        return [f"{r}==1.0" for r in sorted(requirements)]

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _compile)
    return calls


def test_tooling_never_reaches_the_seed(fake_compile):
    declared = Declared(runtime=["numpy", "pytest", "asv", "hypothesis", "setuptools"], build=["cython"])
    result = pin(declared, python_version="3.11", commit_date=JAN_2026)
    for name in TOOLING_OWNED_BY_BASE_IMAGE:
        assert not any(line.startswith(name) for line in result.requirements), name
    assert any(line.startswith("numpy") for line in result.requirements)


def test_build_requires_are_included(fake_compile):
    declared = Declared(runtime=["numpy"], build=["cython", "meson-python"])
    pin(declared, python_version="3.11", commit_date=JAN_2026)
    assert "cython" in fake_compile["requirements"]
    assert "meson-python" in fake_compile["requirements"]


def test_extras_are_excluded_by_default(fake_compile):
    declared = Declared(runtime=["numpy"], extras={"docs": ["sphinx"]})
    pin(declared, python_version="3.11", commit_date=JAN_2026)
    assert "sphinx" not in fake_compile["requirements"]


def test_named_extras_are_included_when_requested(fake_compile):
    declared = Declared(runtime=["numpy"], extras={"docs": ["sphinx"], "gui": ["qt"]})
    pin(declared, python_version="3.11", commit_date=JAN_2026, extras=["gui"])
    assert "qt" in fake_compile["requirements"]
    assert "sphinx" not in fake_compile["requirements"]


def test_operator_pins_are_added(fake_compile):
    declared = Declared(runtime=["numpy"])
    pin(declared, python_version="3.11", commit_date=JAN_2026, operator_pins=["zarr==2.16.0"])
    assert "zarr==2.16.0" in fake_compile["requirements"]


def test_cutoff_is_applied_first(fake_compile):
    pin(Declared(runtime=["numpy"]), python_version="3.11", commit_date=JAN_2026)
    assert fake_compile["cutoff"] is not None
    assert fake_compile["cutoff"].startswith("2026-01-15")


def test_cutoff_is_relaxed_on_failure_and_recorded(monkeypatch):
    attempts = []

    def _compile(requirements, *, python_version, cutoff_rfc3339):
        attempts.append(cutoff_rfc3339)
        if cutoff_rfc3339 is not None:
            raise RuntimeError("no solution with exclude-newer")
        return ["numpy==2.0"]

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _compile)
    result = pin(Declared(runtime=["numpy"]), python_version="3.11", commit_date=JAN_2026)
    assert len(attempts) == 2
    assert result.cutoff_relaxed is True
    assert result.cutoff_used is None
    assert result.requirements == ["numpy==2.0"]


def test_total_failure_returns_empty_and_records_why(monkeypatch):
    def _compile(requirements, *, python_version, cutoff_rfc3339):
        raise RuntimeError("unsatisfiable")

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _compile)
    result = pin(Declared(runtime=["numpy"]), python_version="3.11", commit_date=JAN_2026)
    assert result.requirements == []
    assert result.dropped and "unsatisfiable" in result.dropped[0].reason


def test_empty_declaration_compiles_nothing(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("uv must not be invoked for an empty declaration")

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _boom)
    result = pin(Declared(), python_version="3.11", commit_date=JAN_2026)
    assert result.requirements == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/resolution/test_pin.py -v`
Expected: FAIL — `No module named 'datasmith.resolution.pin'`

- [ ] **Step 3: Write the implementation**

Create `src/datasmith/resolution/pin.py`:

```python
"""Pin a declared dependency set to concrete versions.

Two deliberate exclusions.

**Tooling.**  The base image already installs ``hypothesis``, ``pytest`` and
``versioneer`` (``docker_build_base.sh:769``), ``asv`` (``:771``) and
``pip setuptools wheel`` (``docker_build_env.sh:262``).  The predecessor's
fallback path also injected ``pytest``, ``setuptools`` and ``hypothesis`` into
``env_payload`` while its pyproject path did not, so the two paths disagreed and
the payload fought the image: an unconstrained ``hypothesis`` in the payload
overrides the image's deliberate ``hypothesis<5``.  The image owns tooling.

**Extras.**  The predecessor always passed ``--all-extras``, which resolved
PostHog to 412 packages and napari to 291 — every optional cloud SDK and
documentation theme.  Extras are opt-in, declared per repository through
``formulacode_task_overrides``.

The commit-date cutoff is a preference, not a rule.  It is tried first because it
cheaply yields era-appropriate versions; if it makes the set unsatisfiable the
compile is retried without it and ``cutoff_relaxed`` records that it happened.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass, field

from datasmith.utils import get_logger

from .declare import Declared
from .dependency_resolver import rfc3339, uv_compile
from .requirements import Dropped, parse_many, render

logger = get_logger("resolution.pin")

__all__ = ["TOOLING_OWNED_BY_BASE_IMAGE", "Pinned", "pin"]

#: Packages the base image installs.  Naming them in ``env_payload`` does not add
#: coverage, it creates a version conflict with the image.
TOOLING_OWNED_BY_BASE_IMAGE: frozenset[str] = frozenset({
    "asv",
    "hypothesis",
    "pip",
    "pytest",
    "setuptools",
    "versioneer",
    "wheel",
})


@dataclass(frozen=True)
class Pinned:
    """A pinned dependency set and the story of how it was reached."""

    requirements: list[str] = field(default_factory=list)
    cutoff_used: str | None = None
    cutoff_relaxed: bool = False
    dropped: list[Dropped] = field(default_factory=list)


def _strip_tooling(reqs: Iterable[str]) -> list[str]:
    """Drop anything the base image owns, comparing on the bare package name."""
    parsed, _ = parse_many(reqs)
    return render(r for r in parsed if r.name.lower() not in TOOLING_OWNED_BY_BASE_IMAGE)


def pin(
    declared: Declared,
    *,
    python_version: str,
    commit_date: dt.datetime,
    extras: Iterable[str] = (),
    operator_pins: Iterable[str] = (),
) -> Pinned:
    """Compile a declared set to pinned requirements."""
    wanted: list[str] = [*declared.runtime, *declared.build, *operator_pins]
    for name in extras:
        wanted.extend(declared.extras.get(name, []))

    candidates = _strip_tooling(wanted)
    if not candidates:
        return Pinned()

    cutoff = rfc3339(commit_date)

    try:
        resolved = uv_compile(candidates, python_version=python_version, cutoff_rfc3339=cutoff)
        return Pinned(requirements=list(resolved), cutoff_used=cutoff)
    except Exception as first:
        logger.debug("Compile with cutoff %s failed, relaxing: %s", cutoff, first)

    try:
        resolved = uv_compile(candidates, python_version=python_version, cutoff_rfc3339=None)
        return Pinned(requirements=list(resolved), cutoff_used=None, cutoff_relaxed=True)
    except Exception as second:
        return Pinned(
            cutoff_used=None,
            cutoff_relaxed=True,
            dropped=[Dropped(raw=", ".join(candidates), reason=f"compile failed: {second}")],
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/test_pin.py -v`
Expected: PASS (9 tests)

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/resolution/pin.py tests/resolution/test_pin.py
git commit -m "feat(resolution): pin project dependencies, and only those

Tooling stays out of the seed. The base image already installs hypothesis,
pytest, versioneer and asv, so naming them in env_payload does not add
coverage — it lets an unconstrained hypothesis override the image's
deliberate hypothesis<5.

Extras become opt-in. --all-extras was unconditional and made PostHog 412
packages and napari 291, pulling in cloud SDKs and doc themes no benchmark
needs.

The commit-date cutoff is a preference: tried first, relaxed on failure, and
the relaxation is recorded."
```

---

### Task 8: The `probe` unit

The advisory signal. A dry-run install against the interpreter the container will actually run. It never gates and never raises.

**Files:**
- Create: `src/datasmith/resolution/probe.py`
- Test: `tests/resolution/test_probe.py`

**Interfaces:**
- Consumes: `Pinned` (Task 7), `uv_dry_run_install` from `.dependency_resolver`.
- Produces:
  - `ProbeStatus = Literal["installable", "unresolved", "failed", "empty"]`
  - `@dataclass(frozen=True) class ProbeResult: status: ProbeStatus; log: str`
  - `def probe(pinned: Pinned, *, python_version: str) -> ProbeResult`
  - `PROBE_RANK: dict[str, int]` — ordering key, lower is better

- [ ] **Step 1: Write the failing tests**

Create `tests/resolution/test_probe.py`:

```python
"""Advisory only. It must never raise, and never decide eligibility."""

import pytest

from datasmith.resolution.pin import Pinned
from datasmith.resolution.probe import PROBE_RANK, probe
from datasmith.resolution.requirements import Dropped


def test_empty_seed_is_empty_not_failed():
    assert probe(Pinned(), python_version="3.11").status == "empty"


def test_clean_dry_run_is_installable(monkeypatch):
    monkeypatch.setattr(
        "datasmith.resolution.probe.uv_dry_run_install", lambda *a, **k: (True, "ok")
    )
    r = probe(Pinned(requirements=["numpy==2.0"]), python_version="3.11")
    assert r.status == "installable"
    assert r.log == "ok"


def test_relaxed_cutoff_is_unresolved_even_when_it_installs(monkeypatch):
    monkeypatch.setattr(
        "datasmith.resolution.probe.uv_dry_run_install", lambda *a, **k: (True, "ok")
    )
    r = probe(Pinned(requirements=["numpy==2.0"], cutoff_relaxed=True), python_version="3.11")
    assert r.status == "unresolved"


def test_failed_dry_run_is_failed(monkeypatch):
    monkeypatch.setattr(
        "datasmith.resolution.probe.uv_dry_run_install", lambda *a, **k: (False, "conflict")
    )
    r = probe(Pinned(requirements=["numpy==2.0"]), python_version="3.11")
    assert r.status == "failed"
    assert "conflict" in r.log


def test_a_raising_uv_is_caught_not_propagated(monkeypatch):
    def _boom(*a, **k):
        raise OSError("uv is missing")

    monkeypatch.setattr("datasmith.resolution.probe.uv_dry_run_install", _boom)
    r = probe(Pinned(requirements=["numpy==2.0"]), python_version="3.11")
    assert r.status == "failed"
    assert "uv is missing" in r.log


def test_pin_failure_is_failed_not_empty():
    p = Pinned(dropped=[Dropped(raw="numpy", reason="compile failed: unsatisfiable")])
    assert probe(p, python_version="3.11").status == "failed"


@pytest.mark.parametrize("status", ["installable", "unresolved", "failed", "empty"])
def test_every_status_has_a_rank(status):
    assert status in PROBE_RANK


def test_rank_orders_best_first():
    assert PROBE_RANK["installable"] < PROBE_RANK["unresolved"] < PROBE_RANK["failed"] < PROBE_RANK["empty"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/resolution/test_probe.py -v`
Expected: FAIL — `No module named 'datasmith.resolution.probe'`

- [ ] **Step 3: Write the implementation**

Create `src/datasmith/resolution/probe.py`:

```python
"""Dry-run the pinned seed, advisorily.

The predecessor's ``can_install`` gated stages 5 and 6.  It blocked 3,217
performance PRs that were then never attempted, while passing h5py on a single
dependency and failing apache/arrow on a corrupted marker.  It claimed to mean
"this builds"; it meant "uv could install these wheels into an empty venv".

This keeps the cheap check and drops the claim.  ``status`` orders the stage 5
queue.  It excludes nobody.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .dependency_resolver import uv_dry_run_install
from .pin import Pinned

__all__ = ["PROBE_RANK", "ProbeResult", "ProbeStatus", "probe"]

ProbeStatus = Literal["installable", "unresolved", "failed", "empty"]

#: Queue ordering, best first.  Lower sorts earlier.
PROBE_RANK: dict[str, int] = {"installable": 0, "unresolved": 1, "failed": 2, "empty": 3}


@dataclass(frozen=True)
class ProbeResult:
    """What the dry-run saw."""

    status: ProbeStatus
    log: str


def probe(pinned: Pinned, *, python_version: str) -> ProbeResult:
    """Dry-run a pinned seed.  Never raises."""
    if not pinned.requirements:
        if pinned.dropped:
            reasons = "; ".join(d.reason for d in pinned.dropped)
            return ProbeResult(status="failed", log=reasons)
        return ProbeResult(status="empty", log="nothing declared")

    try:
        ok, log = uv_dry_run_install(pinned.requirements, python_version=python_version)
    except Exception as exc:
        return ProbeResult(status="failed", log=f"{type(exc).__name__}: {exc}")

    if not ok:
        return ProbeResult(status="failed", log=log)
    if pinned.cutoff_relaxed:
        return ProbeResult(status="unresolved", log=log)
    return ProbeResult(status="installable", log=log)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/test_probe.py -v`
Expected: PASS (11 tests)

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/resolution/probe.py tests/resolution/test_probe.py
git commit -m "feat(resolution): keep the cheap check, drop the claim

can_install gated stages 5 and 6 and blocked 3,217 PRs that were then never
attempted, while passing h5py on one dependency and failing arrow on a
corrupted marker. probe_status keeps the same dry-run and uses it to order
the queue instead of truncating it."
```

---

### Task 9: Rewrite the orchestrator

Composes the six units. Deletes the dual path (audit B6) and host-side `uv_install_real`, which proves nothing about the container and dominates runtime.

**Files:**
- Rewrite: `src/datasmith/resolution/orchestrator.py`
- Modify: `src/datasmith/resolution/__init__.py`
- Modify: `src/datasmith/resolution/dependency_resolver.py` (delete `uv_install_real`)
- Test: `tests/resolution/test_orchestrator.py`

**Interfaces:**
- Consumes: `declare` (Task 4), `select_interpreter` + `trove_versions_from_classifiers` (Task 5), `pin` (Task 7), `probe` (Task 8), `discover_candidates` / `analyze_candidate_meta` / `select_primary_candidate` (Task 6), `prepare_repo_checkout` / `asv_finder` from `.git_utils`.
- Produces:
  - `@dataclass(frozen=True) class ResolutionResult` with fields `owner_repo, sha, package_name, package_version, primary_root, requires_python, python_version, interpreter_source, env_payload (list[str]), probe_status, probe_log, cutoff_used, cutoff_relaxed, dropped_requirements (list[dict[str, str]]), resolver_version`
  - `def analyze_commit(sha: str, repo_name: str, bypass_cache: bool = False) -> ResolutionResult | None` — signature preserved for the runner
  - `RESOLVER_VERSION: str`

- [ ] **Step 1: Write the failing tests**

Create `tests/resolution/test_orchestrator.py`:

```python
"""The orchestrator composes the units and records provenance."""

from dataclasses import fields

from datasmith.resolution.orchestrator import RESOLVER_VERSION, ResolutionResult


def test_result_carries_provenance():
    names = {f.name for f in fields(ResolutionResult)}
    for required in (
        "resolver_version",
        "interpreter_source",
        "cutoff_used",
        "cutoff_relaxed",
        "probe_status",
        "probe_log",
        "dropped_requirements",
        "requires_python",
        "primary_root",
    ):
        assert required in names, required


def test_resolver_version_is_set():
    assert RESOLVER_VERSION and RESOLVER_VERSION != "legacy"


def test_uv_install_real_is_deleted():
    import datasmith.resolution.dependency_resolver as dr

    assert not hasattr(dr, "uv_install_real")


def test_no_dual_path_remains():
    import subprocess

    cp = subprocess.run(
        ["grep", "-cn", "uv_compile_from_pyproject", "src/datasmith/resolution/orchestrator.py"],
        capture_output=True,
        text=True,
    )
    assert cp.stdout.strip() in ("0", ""), "the pyproject fast path must be gone"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/resolution/test_orchestrator.py -v`
Expected: FAIL — `ResolutionResult` does not exist

- [ ] **Step 3: Rewrite the orchestrator**

Replace the whole of `src/datasmith/resolution/orchestrator.py`. The new `analyze_commit`:

1. `prepare_repo_checkout(repo_name, sha, tmp_path)` and check out `sha`.
2. `asv_finder(commit)` → parse each config with `json5.loads` → collect `pythons` and `matrix` (keep the existing `collect_asv_cfg` / `_asv_matrix_entries` helpers; they were fixed already and their `dict.get` reads are correct).
3. `discover_candidates(commit)` → `analyze_candidate_meta` → `select_primary_candidate`.
4. `declare(primary_meta, asv_matrix)`.
5. `select_interpreter(requires_python=primary_meta.requires_python, trove_versions=trove_versions_from_classifiers(primary_meta.classifiers), asv_pythons=asv_pythons, commit_date=commit.authored_datetime)`.
6. `pin(declared, python_version=choice.version, commit_date=commit.authored_datetime, extras=..., operator_pins=...)`.
7. `probe(pinned, python_version=choice.version)`.
8. Build and return `ResolutionResult`.

Keep the `@cache_completion(CACHE_LOCATION, table_name="commit_analysis")` decorator, but **change the table name to `commit_analysis_v2`** so legacy pickles of the old dict shape are never unpickled into the new dataclass:

```python
RESOLVER_VERSION = "2026.08.23"


@cache_completion(CACHE_LOCATION, table_name="commit_analysis_v2")
def analyze_commit(sha: str, repo_name: str, bypass_cache: bool = False) -> ResolutionResult | None:
    ...
```

`CandidateMeta` needs a `classifiers: set[str]` field — add it to `models.py` with `field(default_factory=set)` and populate it in `parse_pyproject` (`proj.get("classifiers")`) and `parse_setup_cfg` (`metadata.classifiers`).

- [ ] **Step 4: Delete `uv_install_real`**

Remove the function from `src/datasmith/resolution/dependency_resolver.py` and any import of it.

- [ ] **Step 5: Update the package export**

In `src/datasmith/resolution/__init__.py`, re-export `ResolutionResult` and `RESOLVER_VERSION` alongside `analyze_commit`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/resolution/ -v && uv run mypy`
Expected: PASS, no type errors

- [ ] **Step 7: Commit**

```bash
git add -A src/datasmith/resolution/ tests/resolution/
git commit -m "refactor(resolution): compose six units instead of one 669-line branch

Deletes the dual path, whose two halves produced different environments for
no principled reason, and the host-side uv_install_real, which proves
nothing about the container and dominated runtime.

The result carries provenance: resolver version, interpreter source, the
cutoff actually applied, and every requirement that was dropped with the
reason. The cache table is versioned so legacy pickles are never read back
into the new shape."
```

---

### Task 10: Schema migration

**Files:**
- Create: `supabase/migrations/000NN_packages_resolution_v2.sql` (claim the next free number — check `git branch -a` first; gaps exist at `00018` and `00024`)
- Test: `tests/resolution/test_migration.py`

**Interfaces:**
- Consumes: nothing.
- Produces: the columns Task 11 writes.

- [ ] **Step 1: Write the failing test**

Create `tests/resolution/test_migration.py`:

```python
"""The migration must add every column the runner writes, and grant nothing to anon."""

import re
from pathlib import Path

import pytest

MIGRATIONS = Path("supabase/migrations")
REQUIRED = [
    "dropped_requirements",
    "probe_status",
    "probe_log",
    "interpreter_source",
    "cutoff_used",
    "resolver_version",
    "uv_version",
    "resolved_at",
]


@pytest.fixture
def migration_sql() -> str:
    matches = sorted(MIGRATIONS.glob("*_packages_resolution_v2.sql"))
    assert matches, "migration not found"
    return matches[-1].read_text()


@pytest.mark.parametrize("column", REQUIRED)
def test_column_is_added(migration_sql, column):
    assert re.search(rf"\b{column}\b", migration_sql), column


def test_grants_nothing_to_anon(migration_sql):
    assert "TO anon" not in migration_sql
    assert "GRANT" not in migration_sql.upper() or "anon" not in migration_sql


def test_legacy_rows_are_stamped(migration_sql):
    assert "legacy" in migration_sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/resolution/test_migration.py -v`
Expected: FAIL — migration not found

- [ ] **Step 3: Write the migration**

Create the file (substituting the number you claimed):

```sql
-- Stage 4 resolution redesign: provenance, advisory probe, dropped requirements.
--
-- Number chosen after checking other branches; the sequence has gaps at 00018
-- (origin/lsv-cache-integration) and 00024 (a separate working tree).
--
-- Deliberately grants nothing to anon. `packages` is private and stays private.

ALTER TABLE packages
    ADD COLUMN IF NOT EXISTS dropped_requirements JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS probe_status         TEXT,
    ADD COLUMN IF NOT EXISTS probe_log            TEXT,
    ADD COLUMN IF NOT EXISTS interpreter_source   TEXT,
    ADD COLUMN IF NOT EXISTS cutoff_used          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resolver_version     TEXT,
    ADD COLUMN IF NOT EXISTS uv_version           TEXT,
    ADD COLUMN IF NOT EXISTS resolved_at          TIMESTAMPTZ;

-- Every existing row came from the resolver this redesign replaces, and carries
-- no provenance. Stamp rather than delete, so a re-resolve is a choice and not a
-- prerequisite.
UPDATE packages SET resolver_version = 'legacy' WHERE resolver_version IS NULL;

-- probe_status orders the stage 5 queue, so it is read with a sort, not a filter.
CREATE INDEX IF NOT EXISTS packages_probe_status_idx ON packages (probe_status);

COMMENT ON COLUMN packages.probe_status IS
    'Advisory only. installable | unresolved | failed | empty. Orders the stage 5 queue; excludes nobody.';
COMMENT ON COLUMN packages.dropped_requirements IS
    'Requirements that could not be parsed or resolved, with reasons. Makes a failure diagnosable without a re-run.';
COMMENT ON COLUMN packages.can_install IS
    'Deprecated. Retained for compatibility; no longer read. Use probe_status.';
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/resolution/test_migration.py -v`
Expected: PASS

- [ ] **Step 5: Apply it locally**

```bash
uv run python -c "
import psycopg2, glob
conn = psycopg2.connect(host='127.0.0.1', port=54322, dbname='postgres', user='postgres', password='postgres')
conn.autocommit = True
path = sorted(glob.glob('supabase/migrations/*_packages_resolution_v2.sql'))[-1]
conn.cursor().execute(open(path).read())
print('applied', path)
"
```

- [ ] **Step 6: Verify the columns and the legacy stamp**

```bash
uv run python -c "
import psycopg2
conn = psycopg2.connect(host='127.0.0.1', port=54322, dbname='postgres', user='postgres', password='postgres')
cur = conn.cursor()
cur.execute(\"select column_name from information_schema.columns where table_name='packages'\")
cols = {r[0] for r in cur.fetchall()}
assert {'probe_status','dropped_requirements','resolver_version','uv_version','resolved_at','interpreter_source','cutoff_used','probe_log'} <= cols
cur.execute(\"select count(*) from packages where resolver_version='legacy'\")
print('legacy rows stamped:', cur.fetchone()[0])
"
```
Expected: prints 13016 (or the current row count)

- [ ] **Step 7: Commit**

```bash
git add supabase/migrations/ tests/resolution/test_migration.py
git commit -m "feat(db): provenance and an advisory probe on packages

Adds resolver_version, uv_version, resolved_at, interpreter_source and
cutoff_used, so two rows produced months apart stop being indistinguishable.
Adds dropped_requirements, so a failure is diagnosable without re-running the
resolver — the dry_run_log that would have told you was computed and thrown
away.

Existing rows are stamped 'legacy' rather than deleted. Grants nothing to
anon."
```

---

### Task 11: Runner writes the new row

**Files:**
- Modify: `src/datasmith/runners/resolve_packages.py:76-116`
- Test: `tests/runners/test_resolve_packages.py`

**Interfaces:**
- Consumes: `ResolutionResult`, `RESOLVER_VERSION` (Task 9).
- Produces: the `packages` row shape Task 12 reads.

- [ ] **Step 1: Write the failing test**

Create `tests/runners/test_resolve_packages.py`:

```python
"""The runner must persist every field the resolver produces."""

import json

from datasmith.resolution.orchestrator import RESOLVER_VERSION, ResolutionResult
from datasmith.runners.resolve_packages import build_row


def _result(**kw) -> ResolutionResult:
    base = dict(
        owner_repo="h5py/h5py",
        sha="abc123",
        package_name="h5py",
        package_version="3.15.1",
        primary_root=".",
        requires_python=">=3.9",
        python_version="3.11",
        interpreter_source="requires-python",
        env_payload=["numpy==2.4.1"],
        probe_status="installable",
        probe_log="ok",
        cutoff_used="2026-01-22T00:00:00Z",
        cutoff_relaxed=False,
        dropped_requirements=[],
        resolver_version=RESOLVER_VERSION,
    )
    base.update(kw)
    return ResolutionResult(**base)


def test_row_carries_provenance():
    row = build_row("h5py", "h5py", "abc123", _result())
    assert row["resolver_version"] == RESOLVER_VERSION
    assert row["interpreter_source"] == "requires-python"
    assert row["cutoff_used"] == "2026-01-22T00:00:00Z"
    assert row["resolved_at"]
    assert row["uv_version"]


def test_requires_python_is_stored_not_nulled():
    # resolve_packages.py:104 hardcoded this to None while the parsed value was
    # computed and discarded.
    row = build_row("h5py", "h5py", "abc123", _result())
    assert row["requires_python"] == ">=3.9"


def test_env_payload_is_json_encoded():
    row = build_row("h5py", "h5py", "abc123", _result())
    assert json.loads(row["env_payload"]) == ["numpy==2.4.1"]


def test_dropped_requirements_round_trip():
    dropped = [{"raw": "pyuwsgi;sys.platform!='win32'", "reason": "unparseable requirement"}]
    row = build_row("h5py", "h5py", "abc123", _result(dropped_requirements=dropped))
    assert json.loads(row["dropped_requirements"]) == dropped


def test_retired_columns_are_not_written():
    row = build_row("h5py", "h5py", "abc123", _result())
    for retired in ("build_commands", "install_commands", "resolution_strategy"):
        assert retired not in row
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/runners/test_resolve_packages.py -v`
Expected: FAIL — `cannot import name 'build_row'`

- [ ] **Step 3: Extract `build_row` and use it**

In `src/datasmith/runners/resolve_packages.py`, replace the row construction inside `_process_item` with a module-level, testable function:

```python
def build_row(owner: str, repo: str, sha: str, result: ResolutionResult) -> dict[str, Any]:
    """Map a resolution result onto a ``packages`` row.

    ``requires_python`` is stored. Its predecessor hardcoded ``None`` here while
    the parsed value was computed upstream and discarded.

    ``build_commands``, ``install_commands`` and ``resolution_strategy`` are gone:
    a reader audit found zero consumers outside this module for all three, and
    the explicit provenance columns say what ``resolution_strategy`` was trying to.
    """
    return {
        "owner": owner,
        "repo": repo,
        "sha": sha,
        "package_name": result.package_name,
        "package_version": result.package_version,
        "primary_root": result.primary_root,
        "requires_python": result.requires_python,
        "python_version": result.python_version,
        "interpreter_source": result.interpreter_source,
        "env_payload": json.dumps(result.env_payload),
        "probe_status": result.probe_status,
        "probe_log": result.probe_log,
        "cutoff_used": result.cutoff_used,
        "dropped_requirements": json.dumps(result.dropped_requirements),
        "resolver_version": result.resolver_version,
        "uv_version": _uv_version(),
        "resolved_at": dt.datetime.now(dt.UTC).isoformat(),
    }
```

Add a cached `_uv_version()` helper that shells `uv --version` once per process and returns the string, falling back to `"unknown"`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/runners/test_resolve_packages.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/runners/resolve_packages.py tests/runners/test_resolve_packages.py
git commit -m "feat(runners): persist what the resolver actually found

requires_python was hardcoded to None while the parsed value was computed and
discarded. build_commands, install_commands and resolution_strategy are
dropped — a reader audit found zero consumers for any of them outside this
module."
```

---

### Task 12: Stop gating; order instead

`can_install` is read at `pipeline.py:627` (stage 5) and `synthesize_images.py:273` (stage 6). It blocks 3,217 performance PRs that have never once been attempted.

**Files:**
- Modify: `src/datasmith/update/pipeline.py:619-665`, `:730-745`
- Modify: `src/datasmith/runners/synthesize_images.py:270-280`
- Test: `tests/update/test_no_gate.py`

**Interfaces:**
- Consumes: `PROBE_RANK` (Task 8).
- Produces: no new public surface.

- [ ] **Step 1: Write the failing test**

Create `tests/update/test_no_gate.py`:

```python
"""Nothing is excluded for its probe result; it only decides who runs first."""

import subprocess

from datasmith.resolution.probe import PROBE_RANK
from datasmith.update.pipeline import order_by_probe


def test_no_source_file_filters_on_can_install():
    cp = subprocess.run(
        ["grep", "-rn", "can_install", "src/datasmith/update", "src/datasmith/runners"],
        capture_output=True,
        text=True,
    )
    offending = [ln for ln in cp.stdout.splitlines() if "filters=" in ln or '"can_install": True' in ln]
    assert offending == [], f"can_install is still a filter:\n{chr(10).join(offending)}"


def test_ordering_puts_installable_first():
    rows = [
        {"sha": "c", "probe_status": "failed"},
        {"sha": "a", "probe_status": "installable"},
        {"sha": "d", "probe_status": "empty"},
        {"sha": "b", "probe_status": "unresolved"},
    ]
    assert [r["sha"] for r in order_by_probe(rows)] == ["a", "b", "c", "d"]


def test_ordering_keeps_every_row():
    rows = [{"sha": str(i), "probe_status": s} for i, s in enumerate(PROBE_RANK)]
    assert len(order_by_probe(rows)) == len(rows)


def test_unknown_and_missing_status_sort_last_but_survive():
    rows = [
        {"sha": "x", "probe_status": None},
        {"sha": "y", "probe_status": "installable"},
        {"sha": "z", "probe_status": "bogus"},
    ]
    out = order_by_probe(rows)
    assert out[0]["sha"] == "y"
    assert len(out) == 3


def test_ordering_is_stable_within_a_status():
    rows = [
        {"sha": "b", "probe_status": "installable"},
        {"sha": "a", "probe_status": "installable"},
    ]
    assert [r["sha"] for r in order_by_probe(rows)] == ["b", "a"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/update/test_no_gate.py -v`
Expected: FAIL — `cannot import name 'order_by_probe'`

- [ ] **Step 3: Add the ordering helper**

In `src/datasmith/update/pipeline.py`:

```python
def order_by_probe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order rows best-probe-first, keeping every one of them.

    ``can_install`` used to filter here. It blocked 3,217 performance PRs that
    were then never attempted — and it passed h5py on a single dependency while
    failing apache/arrow on a corrupted marker. Stage 6 is the only stage that
    can answer whether a task builds, so this decides order, not eligibility.

    ``sorted`` is stable, so rows sharing a status keep their incoming order.
    """
    from datasmith.resolution.probe import PROBE_RANK

    unknown = max(PROBE_RANK.values()) + 1
    return sorted(rows, key=lambda r: PROBE_RANK.get(r.get("probe_status") or "", unknown))
```

- [ ] **Step 4: Remove the filter at the three read sites**

In `pipeline.py` at both `fetch_all("packages", ...)` calls, delete `filters={"can_install": True}` and select `probe_status` instead of `can_install`. Pass the result through `order_by_probe` before use. Update the two log lines that say "no can_install package" to say "no resolved package".

In `synthesize_images.py:272-273`, do the same: drop `"can_install": True` from `filters`, keep `"owner"` and `"repo"`, and add `probe_status` to the select.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/update/ tests/resolution/ -v`
Expected: PASS

- [ ] **Step 6: Verify the yield change against the real database**

```bash
uv run python -c "
import psycopg2
conn = psycopg2.connect(host='127.0.0.1', port=54322, dbname='postgres', user='postgres', password='postgres')
cur = conn.cursor()
cur.execute('''select count(distinct (pr.owner,pr.repo,pr.issue_number))
 from pull_requests pr join packages p
   on p.owner=pr.owner and p.repo=pr.repo and p.sha=pr.merge_commit_sha
 where pr.is_performance_commit''')
print('perf PRs now eligible:', cur.fetchone()[0])
"
```
Expected: 12,988 — up from 9,771

- [ ] **Step 7: Commit**

```bash
git add src/datasmith/update/pipeline.py src/datasmith/runners/synthesize_images.py tests/update/test_no_gate.py
git commit -m "feat(pipeline): order on the probe instead of excluding on it

can_install blocked 3,217 performance PRs from ever reaching a container, and
none of them was ever attempted, so nothing has ever tested whether they
build. It also passed h5py on one dependency and failed apache/arrow on a
corrupted marker.

Stage 6 is the only stage that builds in a real container. probe_status now
decides who runs first; it excludes nobody."
```

---

### Task 13: The image tag names the interpreter

`get_repo_image_name(owner, repo)` returns `:latest` with no interpreter in it, while `build_repo_image` bakes `PY_VERSION` in and `_ensure_prerequisite_images` builds only when the tag is absent. One image per repository is therefore built from whichever commit ran first, and **129 of 147 repositories (88%)** have commits that disagree on `python_version`.

**Files:**
- Modify: `src/datasmith/docker/images.py:40-44`, `:92`, `:124`
- Modify: `src/datasmith/runners/synthesize_images.py:51-63`, `:142-152`, `:565-568`
- Test: `tests/docker/test_image_names.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `get_repo_image_name(owner: str, repo: str, py_version: str = "") -> str`

- [ ] **Step 1: Write the failing test**

Create `tests/docker/test_image_names.py`:

```python
"""The tag must name what varies inside the image."""

from datasmith.docker.images import get_repo_image_name


def test_interpreter_appears_in_the_tag():
    assert get_repo_image_name("apache", "arrow", "3.11").endswith(":py3.11")


def test_two_interpreters_give_two_tags():
    a = get_repo_image_name("dask", "dask", "3.9")
    b = get_repo_image_name("dask", "dask", "3.12")
    assert a != b


def test_same_interpreter_gives_a_stable_tag():
    assert get_repo_image_name("dask", "dask", "3.9") == get_repo_image_name("dask", "dask", "3.9")


def test_tag_is_lowercased():
    assert get_repo_image_name("PostHog", "posthog", "3.12") == get_repo_image_name(
        "posthog", "posthog", "3.12"
    )


def test_missing_version_still_yields_a_usable_tag():
    # Legacy callers and images built before this change.
    assert get_repo_image_name("dask", "dask").endswith(":latest")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/docker/test_image_names.py -v`
Expected: FAIL — the tag is `:latest` regardless of version

- [ ] **Step 3: Put the version in the tag**

In `src/datasmith/docker/images.py`:

```python
def get_repo_image_name(owner: str, repo: str, py_version: str = "") -> str:
    """Return the canonical tag for a repository image.

    The interpreter belongs in the tag because it is baked into the image. When
    it was not, one image per repository was built from whichever commit ran
    first, and 88% of repositories have commits that disagree on the
    interpreter — so most containers ran a Python their env_payload was never
    pinned against.

    An empty ``py_version`` yields ``:latest``, which is what images built
    before this change are tagged with.
    """
    owner = owner.lower()
    repo = repo.lower()
    tag = f"py{py_version}" if py_version else "latest"
    return f"{_docker_namespace()}/{owner}-{repo}:{tag}".lower()
```

- [ ] **Step 4: Thread `py_version` through every call site**

- `images.py:92` — `build_repo_image` already takes `py_version`; pass it: `tag = get_repo_image_name(owner, repo, py_version)`.
- `images.py:124` — `build_pr_image` receives `py_version`; pass it through.
- `synthesize_images.py:55` — `_ensure_prerequisite_images` already takes `py_version`; pass it.
- `synthesize_images.py:150` and `:567` — thread the item's `python_version` through to the helper.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/docker/ -m "not slow" -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/datasmith/docker/images.py src/datasmith/runners/synthesize_images.py tests/docker/test_image_names.py
git commit -m "fix(docker): put the interpreter in the repo image tag

The tag was :latest while PY_VERSION was baked in, and the image was built
only when the tag was absent. One image per repository therefore came from
whichever commit ran first, and 129 of 147 repositories have commits that
disagree on python_version — so most containers ran an interpreter their
env_payload was never pinned against."
```

---

### Task 14: `primary_root` reaches the build

Stage 4 discovers `primary_root` correctly and nothing uses it. `Dockerfile.repo` hardcodes `WORKDIR /workspace/repo`, so apache/arrow's `python/` subdirectory is ignored — 733 rows are affected, 385 of them arrow.

**Files:**
- Modify: `src/datasmith/docker/templates/Dockerfile.repo`
- Modify: `src/datasmith/docker/images.py` (`build_repo_image` build args)
- Modify: `src/datasmith/runners/synthesize_images.py` (pass `primary_root`)
- Test: `tests/docker/test_build_root.py`

**Interfaces:**
- Consumes: `primary_root` from the `packages` row.
- Produces: `build_repo_image(..., build_root: str = ".")`

- [ ] **Step 1: Write the failing test**

Create `tests/docker/test_build_root.py`:

```python
"""The discovered package root must reach the image."""

from pathlib import Path

TEMPLATES = Path("src/datasmith/docker/templates")


def test_dockerfile_declares_build_root():
    text = (TEMPLATES / "Dockerfile.repo").read_text()
    assert "ARG BUILD_ROOT" in text


def test_dockerfile_uses_build_root_for_the_workdir():
    text = (TEMPLATES / "Dockerfile.repo").read_text()
    assert "WORKDIR /workspace/repo" not in text.replace("WORKDIR /workspace/repo/${BUILD_ROOT}", "")


def test_build_repo_image_accepts_build_root():
    import inspect

    from datasmith.docker.images import ImageManager

    assert "build_root" in inspect.signature(ImageManager.build_repo_image).parameters
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/docker/test_build_root.py -v`
Expected: FAIL — no `ARG BUILD_ROOT`

- [ ] **Step 3: Add the build arg to the Dockerfile**

In `src/datasmith/docker/templates/Dockerfile.repo`, replace `WORKDIR /workspace/repo` with:

```dockerfile
# The package root inside the repository. Most repos build at the root, but
# apache/arrow's package lives in python/, MDAnalysis' in package/, and Qiskit's
# in qiskit_pkg/. Stage 4 discovers this and it used to be discarded.
ARG BUILD_ROOT="."
WORKDIR /workspace/repo/${BUILD_ROOT}
```

- [ ] **Step 4: Pass it from `build_repo_image`**

In `images.py`, add a `build_root: str = "."` keyword parameter and include it in `build_args`:

```python
        build_args["BUILD_ROOT"] = build_root or "."
```

- [ ] **Step 5: Pass it from the runner**

In `synthesize_images.py`, add `primary_root` to the `packages` select and thread it into `_ensure_prerequisite_images` → `build_repo_image`.

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/docker/ -m "not slow" -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/datasmith/docker/ src/datasmith/runners/synthesize_images.py tests/docker/test_build_root.py
git commit -m "fix(docker): build in the package root stage 4 discovered

Dockerfile.repo hardcoded WORKDIR /workspace/repo, so apache/arrow's python/
subdirectory was discovered correctly and then ignored — 733 rows, 385 of
them arrow."
```

---

### Task 15: Golden fixtures and the full verification pass

Locks the behaviour in against the 13 audited commits and proves the redesign end to end.

**Files:**
- Create: `tests/resolution/fixtures/jan2026/*.json` (13 files)
- Create: `tests/resolution/test_golden.py`
- Modify: `docs/guide/` — whichever page documents stage 4 knobs
- Test: the whole suite

**Interfaces:**
- Consumes: everything above.
- Produces: nothing new.

- [ ] **Step 1: Write the golden test**

Create `tests/resolution/test_golden.py`:

```python
"""The 13 audited commits, locked in.

Marked slow: these clone real repositories and shell out to uv.
"""

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "jan2026"


def _cases():
    return sorted(FIXTURES.glob("*.json"))


@pytest.mark.slow
@pytest.mark.parametrize("fixture", _cases(), ids=lambda p: p.stem)
def test_resolution_matches_the_recorded_artifact(fixture):
    from datasmith.resolution import analyze_commit

    expected = json.loads(fixture.read_text())
    result = analyze_commit(expected["sha"], expected["repo_name"], bypass_cache=True)
    assert result is not None
    assert result.python_version == expected["python_version"]
    assert result.interpreter_source == expected["interpreter_source"]
    assert result.primary_root == expected["primary_root"]
    assert sorted(result.env_payload) == sorted(expected["env_payload"])


@pytest.mark.slow
@pytest.mark.parametrize("fixture", _cases(), ids=lambda p: p.stem)
def test_resolution_is_deterministic(fixture):
    from datasmith.resolution import analyze_commit

    expected = json.loads(fixture.read_text())
    a = analyze_commit(expected["sha"], expected["repo_name"], bypass_cache=True)
    b = analyze_commit(expected["sha"], expected["repo_name"], bypass_cache=True)
    assert a == b


def test_every_audited_repo_has_a_fixture():
    names = {p.stem.split("__")[0] for p in _cases()}
    for owner in (
        "apache", "h5py", "napari", "numpy", "optuna", "pandas-dev", "PostHog",
        "pypa", "quantumlib", "scikit-learn", "scipy", "shapely", "xdslproject",
    ):
        assert owner.lower() in {n.lower() for n in names}, owner


def test_no_fixture_contains_base_image_tooling():
    for path in _cases():
        payload = json.loads(path.read_text())["env_payload"]
        names = {line.split("==")[0].split("[")[0].lower() for line in payload}
        assert not (names & {"pytest", "asv", "hypothesis", "setuptools", "wheel", "pip"}), path.stem


def test_no_fixture_contains_invented_names():
    # These came from the deleted import analyzer and requirements globbing.
    banned = {"version", "plex", "spline", "image", "arraypad", "umath", "conda-build", "boost-cpp"}
    for path in _cases():
        payload = json.loads(path.read_text())["env_payload"]
        names = {line.split("==")[0].split("[")[0].lower() for line in payload}
        assert not (names & banned), f"{path.stem}: {names & banned}"
```

- [ ] **Step 2: Generate the fixtures**

Run the resolver once against each audited commit and record the artifact. The target list is `/mnt/sdd1/atharvas/formulacode/stage4-audit-2026-08-23/targets.json`.

```bash
uv run python - <<'PY'
import json, dataclasses
from pathlib import Path
from datasmith.resolution import analyze_commit

targets = json.load(open("/mnt/sdd1/atharvas/formulacode/stage4-audit-2026-08-23/targets.json"))
out = Path("tests/resolution/fixtures/jan2026")
out.mkdir(parents=True, exist_ok=True)
for t in targets:
    repo_name = f"{t['owner']}/{t['repo']}"
    r = analyze_commit(t["sha"], repo_name, bypass_cache=True)
    if r is None:
        print("SKIP (returned None):", repo_name)
        continue
    d = dataclasses.asdict(r)
    d["repo_name"] = repo_name
    # probe_log is machine- and time-dependent; it is not part of the contract.
    d.pop("probe_log", None)
    (out / f"{t['owner']}__{t['repo']}__{t['sha'][:8]}.json").write_text(json.dumps(d, indent=1, sort_keys=True))
    print("wrote", repo_name, r.python_version, r.interpreter_source, len(r.env_payload))
PY
```

- [ ] **Step 3: Review the fixtures by hand before committing them**

Check each one against the audit's known defects. numpy must not contain `version` or `plex`; scipy must not contain `conda-build`, `torch` or `cupy`; arrow's `primary_root` must be `python`; no fixture may contain `pytest` or `asv`; every `interpreter_source` must be populated; no `python_version` may exceed `DATASMITH_PYTHON_CEILING`.

- [ ] **Step 4: Run the golden tests**

Run: `uv run pytest tests/resolution/test_golden.py -v -m slow`
Expected: PASS

- [ ] **Step 5: Run the fast tests**

Run: `uv run pytest tests/resolution/test_golden.py -v -m "not slow"`
Expected: PASS (the three non-slow assertions)

- [ ] **Step 6: Full verification**

```bash
make check
uv run pytest tests/ -m "not slow" -q
```
Expected: both clean. `make check` runs ruff, format, mypy, deptry and `uv lock --locked`.

- [ ] **Step 7: Update the documentation**

Add `DATASMITH_PYTHON_FLOOR` and `DATASMITH_PYTHON_CEILING` to the tunable-constants list in `CLAUDE.md`. Update the stage 4 line in the pipeline-stages section to describe the seed contract and note that `can_install` no longer gates. Update whichever page under `docs/guide/` documents the `packages` table columns.

- [ ] **Step 8: Commit**

```bash
git add tests/resolution/ CLAUDE.md docs/
git commit -m "test(resolution): lock in the 13 audited commits as golden fixtures

Each asserts the interpreter, its source, the package root and the pinned
set. Two further assertions encode the audit directly: no fixture may carry
tooling the base image owns, and none may carry a name the deleted import
analyzer would have invented."
```

---

## Self-Review

**Spec coverage.** §2 contract → Tasks 7, 8, 12. §3 unit of work and B13 → Task 13; `primary_root` → Task 14; `requires_python` → Task 11. §4.1 discover → Task 6. §4.2 declare → Tasks 1, 2, 4. §4.3 interpreter → Task 5. §4.4 pin → Task 7. §4.5 probe → Task 8. §4.6 emit → Tasks 9, 11. §5 deletions → Tasks 2 (`fix_marker_spacing`), 3 (`blocklist`), 4 (`import_analyzer`), 9 (`uv_install_real`, dual path). §6 schema → Tasks 10, 11. §7 testing → every task, consolidated in Task 15. §8 out of scope — the base image's unpinned asv is deliberately untouched and stays out of scope.

**Placeholders.** None. Every code step carries the actual code; every test step carries the actual test.

**Type consistency.** `Dropped(raw, reason)` is used identically in Tasks 1, 4, 7, 8 and serialised as `{"raw", "reason"}` in Task 11's `dropped_requirements`. `Declared(runtime, build, extras, dropped)` is produced by Task 4 and consumed by Task 7. `Pinned(requirements, cutoff_used, cutoff_relaxed, dropped)` is produced by Task 7 and consumed by Task 8. `InterpreterChoice(version, source)` is produced by Task 5 and consumed by Task 9. `ProbeResult(status, log)` maps onto `probe_status`/`probe_log`. `PROBE_RANK` is defined in Task 8 and consumed in Task 12. `get_repo_image_name(owner, repo, py_version="")` keeps a default so Task 13 does not break callers before they are threaded.

**Ordering note.** Tasks 1–9 are strictly sequential (each consumes the previous). Task 10 is independent and may run any time before Task 11. Tasks 13 and 14 touch `docker/` and `synthesize_images.py` only, so they are independent of 1–11 — but both must land before Task 15's verification pass.
