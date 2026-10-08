# Reproducible Container Pre-Pass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the no-agent build path read each repository's own `asv.conf.json`, then measure how many of 20 repositories build with no agent at all.

**Architecture:** The pipeline already has a no-agent path, `TRY_DEFAULT` in `synthesizer.py`, which builds from a generic committed template. Four defects stop it using what the repository declares about itself. This plan fixes those four defects, adds one measurement, and then runs a trial that produces the number the rest of the design depends on.

**Tech Stack:** Python 3.12, pytest, ruff, uv, Docker with BuildKit, local Supabase, json5.

**Spec:** `docs/superpowers/specs/2026-08-23-reproducible-container-build-design.md`

## Global Constraints

- Python floor is `>=3.12` in `pyproject.toml`, but CI runs 3.11 and 3.12. Do not use 3.12-only syntax in `src/`.
- Type hints are required. `mypy` runs in strict mode with `disallow_untyped_defs`.
- Ruff line length is 120. `E501`, `TRY003`, `SIM108`, `S603`, `S607` are globally ignored.
- Tests that build or run real containers must carry `@pytest.mark.slow`. `make test` runs `-m "not slow"`.
- Any tunable constant must be named `DATASMITH_*` and read from the environment at module scope, with a literal default.
- Never write to `db.formulacode.org`. Use `SUPABASE_URL=http://127.0.0.1:54321`.
- Never run `docker volume prune`. The local Supabase database lives in a volume.
- Three template directories are excluded from ruff and mypy by `pyproject.toml:102`: `src/datasmith/docker/templates/`, `src/datasmith/agents/templates/`, `src/datasmith/harbor_adapter/template/`. Files there use the standard library only and must not import `datasmith`.

---

### Task 1: Guard templates against undefined names

The three template directories are excluded from ruff with `force-exclude = true`. That exclusion hid a `NameError` that reached every image and that 130 of 134 repositories worked around by writing into `builtins`.

**Files:**
- Create: `tests/docker/test_template_lint.py`
- Modify: `src/datasmith/docker/templates/pytest_runner.py` (add one import)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: nothing later tasks import. This task is a standalone guard.

- [ ] **Step 1: Write the failing test**

Create `tests/docker/test_template_lint.py`:

```python
"""Template directories are excluded from ruff, so undefined names reach production.

`pyproject.toml:102` lists all three template directories in `extend-exclude`
with `force-exclude = true`. That is deliberate: the files there are stdlib-only
scripts baked into container images, and most ruff rules do not apply to them.

The cost was a real defect. `pytest_runner.py` called `sys.exit()` without
importing `sys`, so every test run in every image ended in a `NameError`. The
agent could not edit the file, so 130 of 134 repositories injected
`builtins.sys = sys` instead, which then ran inside the measured benchmark
process.

This test re-enables only the rules that catch that class: undefined name,
undefined export, redefinition. It uses `--isolated` so the exclusion in
`pyproject.toml` does not apply.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]

_TEMPLATE_DIRS = [
    "src/datasmith/docker/templates",
    "src/datasmith/agents/templates",
    "src/datasmith/harbor_adapter/template",
]

# F821 undefined name, F822 undefined name in __all__, F811 redefinition.
_RULES = "F821,F822,F811"


@pytest.mark.parametrize("rel", _TEMPLATE_DIRS)
def test_no_undefined_names_in_templates(rel: str) -> None:
    directory = _ROOT / rel
    assert directory.is_dir(), f"template directory missing: {rel}"

    proc = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--isolated",
            "--select",
            _RULES,
            "--output-format",
            "concise",
            str(directory),
        ],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, (
        f"undefined or redefined names in {rel}.\n"
        f"These files are excluded from ruff, so nothing else will catch this.\n"
        f"{proc.stdout}\n{proc.stderr}"
    )
```

- [ ] **Step 2: Run the test and confirm it fails**

Run:

```bash
uv run pytest tests/docker/test_template_lint.py -v
```

Expected: the `docker/templates` case FAILS with `F821 Undefined name 'sys'` at `pytest_runner.py:708`. The other two cases PASS.

- [ ] **Step 3: Add the missing import**

In `src/datasmith/docker/templates/pytest_runner.py`, the import block is lines 3 to 9 and reads:

```python
import argparse
import json
import os
import shlex
import subprocess
import time
from glob import glob
```

Change it to:

```python
import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from glob import glob
```

- [ ] **Step 4: Run the test and confirm it passes**

Run:

```bash
uv run pytest tests/docker/test_template_lint.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Confirm nothing else broke**

Run:

```bash
uv run pytest tests/ -q -m "not slow"
```

Expected: 850 passed or more, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add tests/docker/test_template_lint.py src/datasmith/docker/templates/pytest_runner.py
git commit -m "fix(templates): import sys in pytest_runner, and guard the exclusion

pytest_runner.py:708 called sys.exit() without importing sys, so every test
run in every image raised NameError. The three template directories are
excluded from ruff with force-exclude, so nothing caught it. 130 of 134 repos
worked around it by assigning builtins.sys, inside the measured process.

The new test runs ruff F821/F822/F811 over the excluded directories with
--isolated, which catches the whole class rather than this one instance."
```

---

### Task 2: Read the ASV config instead of silently ignoring it

`orchestrator.py:81` parses each config with `json5.loads`, which returns a plain dict. Lines 86 to 99 read that dict with `getattr`, which is attribute access. Every read returns its default, so `pythons`, `build_command`, `install_command`, and `matrix` are always empty.

**Files:**
- Modify: `src/datasmith/resolution/orchestrator.py:84-105`
- Create: `tests/resolution/__init__.py`
- Create: `tests/resolution/test_asv_cfg.py`

**Interfaces:**
- Consumes: `ASVCfgAggregate` from `datasmith.resolution.models`, with fields `pythons: set[tuple[int, ...]]`, `build_commands: set[str]`, `install_commands: set[str]`, `matrix: dict[str, set[str]]`.
- Produces: a module-level function
  `collect_asv_cfg(cfgs: list[dict]) -> ASVCfgAggregate`
  in `datasmith.resolution.orchestrator`. Task 3 modifies the consumer of `ASVCfgAggregate.matrix`, and relies on the shape produced here: `None` entries are dropped, and every other value is a stripped string.

- [ ] **Step 1: Write the failing test**

Create `tests/resolution/__init__.py` as an empty file. Then create `tests/resolution/test_asv_cfg.py`:

```python
"""The ASV config read was a no-op for the life of the project.

`json5.loads` returns a plain dict. The reader used `getattr(cfg, "pythons", [])`,
which is attribute access, so every field came back as its default. The repo's
declared Python version, dependency matrix and build commands were all discarded.
"""

from __future__ import annotations

from datasmith.resolution.orchestrator import collect_asv_cfg

# Shape taken from pandas-dev/pandas asv_bench/asv.conf.json.
PANDAS_CFG = {
    "version": 1,
    "project": "pandas",
    "pythons": ["3.8"],
    "matrix": {
        "numpy": [],
        "Cython": ["0.29.21"],
        "matplotlib": [],
        "pytables": [None],
    },
    "build_command": [
        "python setup.py build -j4",
        "PIP_NO_BUILD_ISOLATION=false python -mpip wheel --no-deps -w {build_cache_dir} {build_dir}",
    ],
    "install_command": ["in-dir={env_dir} python -mpip install {wheel_file}"],
}

# Shape taken from apache/arrow python/asv.conf.json.
ARROW_CFG = {
    "version": 1,
    "pythons": ["3.9"],
    "matrix": {"boost-cpp": ["1.68.0"], "cmake": [], "cython": []},
    "build_command": ["/bin/bash {build_dir}/asv-build.sh"],
}

# ASV 0.5 and later also accept a grouped matrix.
NESTED_CFG = {
    "version": 1,
    "pythons": ["3.11"],
    "matrix": {
        "req": {"numpy": ["1.26.0"], "scipy": []},
        "env": {"OMP_NUM_THREADS": ["1"]},
    },
}


class TestPythons:
    def test_declared_python_is_read(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.pythons == {(3, 8)}

    def test_multiple_configs_union(self):
        agg = collect_asv_cfg([PANDAS_CFG, ARROW_CFG])
        assert agg.pythons == {(3, 8), (3, 9)}

    def test_absent_pythons_yields_empty_not_error(self):
        agg = collect_asv_cfg([{"version": 1}])
        assert agg.pythons == set()


class TestCommands:
    def test_build_command_is_read_and_joined(self):
        agg = collect_asv_cfg([ARROW_CFG])
        assert agg.build_commands == {"/bin/bash {build_dir}/asv-build.sh"}

    def test_mpip_is_normalised(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        joined = next(iter(agg.build_commands))
        assert "-m pip" in joined
        assert "-mpip" not in joined

    def test_install_command_is_read(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.install_commands == {
            "in-dir={env_dir} python -mpip install {wheel_file}"
        }


class TestMatrix:
    def test_keys_are_preserved(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert set(agg.matrix) >= {"numpy", "Cython", "matplotlib"}

    def test_pinned_version_is_preserved(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.matrix["Cython"] == {"0.29.21"}

    def test_unpinned_package_has_no_versions(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.matrix["numpy"] == set()

    def test_null_version_is_dropped_not_stringified(self):
        """ASV uses null to mean "do not install". `str(None)` would leak "None"."""
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.matrix["pytables"] == set()

    def test_grouped_matrix_reads_the_req_group(self):
        agg = collect_asv_cfg([NESTED_CFG])
        assert agg.matrix["numpy"] == {"1.26.0"}
        assert agg.matrix["scipy"] == set()
        assert "OMP_NUM_THREADS" not in agg.matrix

    def test_non_dict_config_is_skipped(self):
        agg = collect_asv_cfg([None, "not a config", PANDAS_CFG])
        assert agg.pythons == {(3, 8)}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run:

```bash
uv run pytest tests/resolution/test_asv_cfg.py -v
```

Expected: collection FAILS with `ImportError: cannot import name 'collect_asv_cfg'`.

- [ ] **Step 3: Write the implementation**

In `src/datasmith/resolution/orchestrator.py`, add this function at module scope, above the function that contains the current loop:

```python
def _asv_matrix_entries(matrix: dict) -> dict[str, set[str]]:
    """Normalise an ASV `matrix` block into ``{package: {versions}}``.

    ASV accepts two shapes. The legacy shape maps a package name straight to a
    list of versions. The shape used since ASV 0.5 groups entries under ``req``,
    ``env`` and ``env_nobuild``. Only ``req`` names packages, so the grouped
    shape is read through that key and the rest is ignored.

    A ``None`` version means "do not install this package in this combination".
    It is dropped rather than converted, because ``str(None)`` would put the
    literal string ``"None"`` into the requirement set.
    """
    if not isinstance(matrix, dict):
        return {}

    # Grouped shape: any value that is itself a dict means ASV 0.5 or later.
    if any(isinstance(v, dict) for v in matrix.values()):
        matrix = matrix.get("req") or {}
        if not isinstance(matrix, dict):
            return {}

    out: dict[str, set[str]] = {}
    for pkg, raw in matrix.items():
        if not isinstance(pkg, str) or not pkg.strip():
            continue
        values = raw if isinstance(raw, list | tuple | set) else [raw]
        out[pkg.strip()] = {
            str(v).strip() for v in values if v is not None and str(v).strip()
        }
    return out


def collect_asv_cfg(cfgs: list) -> ASVCfgAggregate:
    """Aggregate every ASV config found for one commit.

    `json5.loads` returns a plain dict, so every field must be read with
    ``dict.get``. The previous code used ``getattr``, which is attribute access
    and always returned the default, making the whole read a no-op.
    """
    agg = ASVCfgAggregate()
    for cfg in cfgs:
        if not isinstance(cfg, dict):
            continue

        for py in cfg.get("pythons") or []:
            with contextlib.suppress(Exception):
                agg.pythons.add(tuple(int(part) for part in str(py).split(".")))

        bc = cfg.get("build_command")
        if bc:
            if isinstance(bc, list | tuple):
                bc = " && ".join(str(x) for x in bc)
            agg.build_commands.add(str(bc).replace("-mpip", "-m pip"))

        ic = cfg.get("install_command")
        if ic:
            if isinstance(ic, list | tuple):
                ic = " && ".join(str(x) for x in ic)
            agg.install_commands.add(str(ic))

        for pkg, versions in _asv_matrix_entries(cfg.get("matrix") or {}).items():
            agg.matrix.setdefault(pkg, set()).update(versions)

    return agg
```

Then replace the existing loop. Delete these lines from `orchestrator.py` (currently 84 to 105):

```python
            cfg_items = ASVCfgAggregate()
            for cfg in asv_cfgs:
                pythons: set[tuple[int, ...]] = set()
                for py in getattr(cfg, "pythons", []) or []:
                    with contextlib.suppress(Exception):
                        pythons.add(tuple(map(int, str(py).split("."))))
                cfg_items.pythons.update(pythons)
                bc = getattr(cfg, "build_command", None)
                ic = getattr(cfg, "install_command", None)
                if bc:
                    if isinstance(bc, list | tuple):
                        bc = " && ".join(bc).replace("-mpip", "-m pip")
                    cfg_items.build_commands.add(str(bc))
                if ic:
                    if isinstance(ic, list | tuple):
                        ic = " && ".join(ic)
                    cfg_items.install_commands.add(str(ic))
                mx = getattr(cfg, "matrix", None) or {}
                for k, v in mx.items():
                    values = cfg_items.matrix.setdefault(k, set())
                    if isinstance(v, list | tuple | set):
                        values.update(map(str, v))
                    else:
                        values.add(str(v))
```

Replace them with:

```python
            cfg_items = collect_asv_cfg(asv_cfgs)
```

The `install_command` normalisation moved: the old code applied `-mpip` to `build_command` only, and the new code does the same. Keep `install_commands` unchanged, because `extract_requested_extras` matches on the raw text.

- [ ] **Step 4: Run the test and confirm it passes**

Run:

```bash
uv run pytest tests/resolution/test_asv_cfg.py -v
```

Expected: 12 passed.

- [ ] **Step 5: Confirm the whole suite and the type check still pass**

Run:

```bash
uv run pytest tests/ -q -m "not slow"
uv run mypy
uv run ruff check src/datasmith/resolution/orchestrator.py
```

Expected: tests pass, mypy reports no error, ruff reports no error.

- [ ] **Step 6: Commit**

```bash
git add src/datasmith/resolution/orchestrator.py tests/resolution/
git commit -m "fix(resolution): read the asv config with dict access, not getattr

json5.loads returns a dict. The reader used getattr, so pythons,
build_command, install_command and matrix always came back empty and the
repo's own declaration was never used. Handles both the legacy flat matrix and
the grouped req/env shape, and drops null versions instead of stringifying
them."
```

---

### Task 3: Apply ASV matrix semantics to the requirement set

Task 2 makes `cfg_items.matrix` non-empty for the first time. That exposes a second defect below it. `orchestrator.py:269` iterates the values and discards the keys, so `{"Cython": ["0.29.21"]}` yields the string `'0.29.21'`, which is then handed to the resolver as a package name.

**Files:**
- Modify: `src/datasmith/resolution/orchestrator.py:269-274`
- Create: `tests/resolution/test_matrix_requirements.py`

**Interfaces:**
- Consumes: `collect_asv_cfg` from Task 2, and `normalize_requirement(req: str) -> list[str]` from `datasmith.resolution.package_filters`.
- Produces: a module-level function
  `matrix_requirements(matrix: dict[str, set[str]]) -> set[str]`
  in `datasmith.resolution.orchestrator`.

- [ ] **Step 1: Write the failing test**

Create `tests/resolution/test_matrix_requirements.py`:

```python
"""ASV's matrix maps package name to required versions.

The previous code iterated the values and discarded the keys, so a pinned entry
became a bare version string that the resolver would receive as a package name.
"""

from __future__ import annotations

from datasmith.resolution.orchestrator import matrix_requirements


def test_pinned_version_becomes_an_equality_requirement():
    assert matrix_requirements({"Cython": {"0.29.21"}}) == {"cython==0.29.21"}


def test_unpinned_package_becomes_a_bare_requirement():
    assert matrix_requirements({"numpy": set()}) == {"numpy"}


def test_bare_version_string_is_never_emitted_alone():
    """The old behaviour. `0.29.21` must never appear as a package name."""
    out = matrix_requirements({"Cython": {"0.29.21"}})
    assert "0.29.21" not in out


def test_several_versions_yield_several_requirements():
    out = matrix_requirements({"numpy": {"1.25.0", "1.26.0"}})
    assert out == {"numpy==1.25.0", "numpy==1.26.0"}


def test_conda_only_package_is_still_emitted():
    """boost-cpp is a conda package. Filtering happens downstream, not here."""
    assert matrix_requirements({"boost-cpp": {"1.68.0"}}) == {"boost-cpp==1.68.0"}


def test_empty_matrix_yields_nothing():
    assert matrix_requirements({}) == set()


def test_flag_like_keys_are_skipped():
    assert matrix_requirements({"-e": {"1.0"}, "numpy": set()}) == {"numpy"}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run:

```bash
uv run pytest tests/resolution/test_matrix_requirements.py -v
```

Expected: collection FAILS with `ImportError: cannot import name 'matrix_requirements'`.

- [ ] **Step 3: Write the implementation**

Add this function to `src/datasmith/resolution/orchestrator.py`, next to `collect_asv_cfg`:

```python
def matrix_requirements(matrix: dict[str, set[str]]) -> set[str]:
    """Turn an ASV matrix into requirement strings.

    ASV maps a package name to the versions its benchmarks require. An empty
    version set means "require this package at any version". A non-empty set
    means "pin it".

    The keys carry the package names, so they must not be discarded. Passing a
    bare version such as ``0.29.21`` to the resolver treats it as a package
    name, which either fails resolution or is silently dropped.
    """
    out: set[str] = set()
    for pkg, versions in (matrix or {}).items():
        name = str(pkg).strip()
        if not name or name.startswith("-"):
            continue
        if versions:
            for version in versions:
                value = str(version).strip()
                if value and not value.startswith("-"):
                    out.update(normalize_requirement(f"{name}=={value}"))
        else:
            out.update(normalize_requirement(name))
    return out
```

Then replace these lines in `orchestrator.py` (currently 269 to 274):

```python
            for vals in cfg_items.matrix.values():
                for v in vals:
                    s = str(v).strip()
                    if s and not s.startswith("-"):
                        normalized = normalize_requirement(s)
                        base_requirements.update(normalized)
```

with:

```python
            base_requirements.update(matrix_requirements(cfg_items.matrix))
```

- [ ] **Step 4: Run the test and confirm it passes**

Run:

```bash
uv run pytest tests/resolution/test_matrix_requirements.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Confirm the whole suite and the type check still pass**

Run:

```bash
uv run pytest tests/ -q -m "not slow"
uv run mypy
```

Expected: tests pass, mypy reports no error.

- [ ] **Step 6: Commit**

```bash
git add src/datasmith/resolution/orchestrator.py tests/resolution/test_matrix_requirements.py
git commit -m "fix(resolution): read the asv matrix as name-to-versions

The loop iterated matrix.values() and discarded the keys, so pandas'
Cython 0.29.21 pin became the package name '0.29.21'. Unreachable until the
config read was fixed, so it lands with that change."
```

---

### Task 4: Record whether the no-agent path succeeded

`TRY_DEFAULT` succeeds or fails silently. Its outcome only reaches `error_logs` indirectly, through the agent attempts that follow a failure. The trial in Task 5 cannot measure a rate that is not recorded.

**Files:**
- Modify: `src/datasmith/agents/synthesizer.py:179-231`
- Create: `tests/agents/test_default_template_logging.py`

**Interfaces:**
- Consumes: `Synthesizer._log_attempt`, which writes an `error_logs` row and already accepts `agent_name` through `SandboxResult`.
- Produces: a method
  `Synthesizer._log_default_attempt(owner: str, repo: str, sha: str, issue_number: int, success: bool, duration_s: float, error_message: str | None) -> None`
  which writes one `error_logs` row with `agent_name="default_template"`.

- [ ] **Step 1: Write the failing test**

Create `tests/agents/test_default_template_logging.py`:

```python
"""The no-agent build path must record its own outcome.

TRY_DEFAULT is the only path that can build without an agent. Its success rate
is the number that decides how much agent work the pipeline needs, and nothing
recorded it. Rows use agent_name="default_template" so the rate is one query.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.agents.synthesizer import Synthesizer


def _rows_written(mock_client: MagicMock) -> list[dict]:
    table = mock_client.return_value.table
    return [call.args[0] for call in table.return_value.insert.call_args_list]


class TestDefaultTemplateLogging:
    def test_success_writes_a_row_marked_default_template(self):
        synth = Synthesizer(agent="codex")
        with patch("datasmith.agents.synthesizer.get_client") as client:
            synth._log_default_attempt(
                owner="pandas-dev",
                repo="pandas",
                sha="a" * 40,
                issue_number=43524,
                success=True,
                duration_s=446.0,
                error_message=None,
            )
        rows = _rows_written(client)
        assert len(rows) == 1
        assert rows[0]["agent_name"] == "default_template"
        assert rows[0]["success"] is True
        assert rows[0]["issue_number"] == 43524

    def test_failure_records_the_message(self):
        synth = Synthesizer(agent="codex")
        with patch("datasmith.agents.synthesizer.get_client") as client:
            synth._log_default_attempt(
                owner="apache",
                repo="arrow",
                sha="b" * 40,
                issue_number=44236,
                success=False,
                duration_s=1606.0,
                error_message="pkg stage failed",
            )
        rows = _rows_written(client)
        assert rows[0]["success"] is False
        assert rows[0]["failure_stage"] == "default_template"
        assert "pkg stage failed" in rows[0]["error_message"]

    def test_a_supabase_outage_does_not_raise(self):
        """Logging is never allowed to fail a build."""
        synth = Synthesizer(agent="codex")
        with patch("datasmith.agents.synthesizer.get_client", side_effect=RuntimeError("down")):
            synth._log_default_attempt(
                owner="networkx",
                repo="networkx",
                sha="c" * 40,
                issue_number=8148,
                success=True,
                duration_s=1.0,
                error_message=None,
            )
```

- [ ] **Step 2: Run the test and confirm it fails**

Run:

```bash
uv run pytest tests/agents/test_default_template_logging.py -v
```

Expected: FAIL with `AttributeError: 'Synthesizer' object has no attribute '_log_default_attempt'`.

- [ ] **Step 3: Write the implementation**

Add this method to `Synthesizer` in `src/datasmith/agents/synthesizer.py`, directly after `_log_attempt`:

```python
    def _log_default_attempt(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        success: bool,
        duration_s: float,
        error_message: str | None,
    ) -> None:
        """Record one TRY_DEFAULT outcome in ``error_logs``.

        The no-agent path is the only one that can build without spending agent
        time, so its success rate decides how much agent work the pipeline
        needs. Rows carry ``agent_name="default_template"`` so the rate is a
        single query, and they never carry an agent transcript.

        A logging failure must never fail a build, so every error is swallowed.
        """
        row = {
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "issue_number": issue_number,
            "attempt_index": 0,
            "agent_name": "default_template",
            "success": success,
            "duration_s": duration_s,
            "failure_stage": None if success else "default_template",
            "error_message": (error_message or "")[-10_000:] or None,
            "created_at": datetime.datetime.now(tz=datetime.UTC).isoformat(),
        }
        try:
            get_client().table("error_logs").insert(row).execute()
        except Exception:
            logger.debug("Failed to log default-template attempt", exc_info=True)
```

Then call it from the `TRY_DEFAULT` block. In `synthesizer.py`, wrap the `verify_context` call so the duration is measured, and log both outcomes.

Before the `verify_context(...)` call inside `if (not already_succeeded) and (not too_many_failures):`, add:

```python
            default_started = time.monotonic()
```

After `result = verify_context(...)` returns, and before `if result.success:`, add:

```python
            default_failure = result.failure_json or {}
            self._log_default_attempt(
                owner=owner,
                repo=repo,
                sha=sha,
                issue_number=issue_number,
                success=bool(result.success),
                duration_s=round(time.monotonic() - default_started, 2),
                error_message=(
                    None
                    if result.success
                    else (
                        f"{default_failure.get('stage') or 'unknown'}: "
                        f"{default_failure.get('error_message') or ''}"
                    )[-10_000:]
                ),
            )
```

`synthesizer.py:3` already imports `datetime`. It does **not** import `time`.
Add `import time` to the import block.

- [ ] **Step 4: Run the test and confirm it passes**

Run:

```bash
uv run pytest tests/agents/test_default_template_logging.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Confirm the whole suite and the type check still pass**

Run:

```bash
uv run pytest tests/ -q -m "not slow"
uv run mypy
```

Expected: tests pass, mypy reports no error.

- [ ] **Step 6: Commit**

```bash
git add src/datasmith/agents/synthesizer.py tests/agents/test_default_template_logging.py
git commit -m "feat(synthesis): record the no-agent build outcome

TRY_DEFAULT is the only path that builds without an agent, and its rate was
never recorded. Rows use agent_name='default_template' so the rate is one
query. Logging never fails a build."
```

---

### Task 5: Run the 20-repository trial

The design depends on one unmeasured number: how many repositories build with no agent when the pipeline uses what they declare. This task measures it.

**Files:**
- Create: `scripts/prepass_trial.py`
- Create: `docs/superpowers/plans/2026-08-23-prepass-trial-results.md` (written by the run)

**Interfaces:**
- Consumes: `collect_asv_cfg` and `matrix_requirements` from Tasks 2 and 3, and the `error_logs` rows written by Task 4.
- Produces: a results document. No later task imports from this script.

- [ ] **Step 1: Write the trial script**

Create `scripts/prepass_trial.py`:

```python
#!/usr/bin/env python3
"""Measure how many repositories build with no agent.

Runs stage 6 against a fixed sample of repositories with the agent disabled, so
only the TRY_DEFAULT path can succeed. Task 4 records each outcome in
`error_logs` with `agent_name="default_template"`, and this script reads them
back.

Selection is seeded and printed, so the run is repeatable and the sample can be
audited. One task per repository, because the number under measurement is a
per-repository property.

    python scripts/prepass_trial.py --sample 20 --seed 20260823

Environment this needs, and why:
    SUPABASE_URL=http://127.0.0.1:54321   local only, never the tunnel
    DATASMITH_DISABLE_DOCKER_PRUNE=1      the prune watcher deletes BuildKit
                                          cache every 7200s during the run
    TMPDIR=/mnt/sdd2/tmp-prepass          / is at 98%; Docker is on /mnt/sdd2
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import random
import sys
import time


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sample", type=int, default=20, help="repositories to try")
    p.add_argument("--seed", type=int, default=20260823)
    p.add_argument("--n-concurrent", type=int, default=4)
    p.add_argument("--start-date", default="2017-01-01")
    p.add_argument("--end-date", default="2026-12-31")
    p.add_argument("--out", default="docs/superpowers/plans/2026-08-23-prepass-trial-results.md")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    for var in ("SUPABASE_URL", "DATASMITH_DISABLE_DOCKER_PRUNE", "TMPDIR"):
        print(f"[trial] {var}={os.environ.get(var)}", flush=True)
    if os.environ.get("SUPABASE_URL") != "http://127.0.0.1:54321":
        print("[trial] refusing to run against a non-local SUPABASE_URL", file=sys.stderr)
        return 2

    import datasmith  # noqa: F401  loads tokens.env

    from datasmith.runners import synthesize_images as si
    from datasmith.update.pipeline import Pipeline
    from datasmith.utils.db import fetch_all

    chosen: list[tuple[str, str, int]] = []
    real_run = si.SynthesizeImagesRunner.run

    async def sampling_run(self, items):  # type: ignore[no-untyped-def]
        by_repo: dict[tuple[str, str], list] = {}
        for item in items:
            by_repo.setdefault((item["owner"], item["repo"]), []).append(item)
        repos = sorted(by_repo)
        rng = random.Random(args.seed)
        picked = rng.sample(repos, min(args.sample, len(repos)))
        # One task per repository: the number under measurement is per-repo.
        sampled = [sorted(by_repo[r], key=lambda i: i["issue_number"])[0] for r in picked]
        for item in sampled:
            key = (item["owner"], item["repo"], item["issue_number"])
            chosen.append(key)
            print(f"[trial]   {key[0]}/{key[1]}#{key[2]}", flush=True)
        pathlib.Path("prepass_chosen.json").write_text(
            json.dumps({"seed": args.seed, "chosen": [list(c) for c in chosen]}, indent=2)
        )
        return await real_run(self, sampled)

    si.SynthesizeImagesRunner.run = sampling_run  # type: ignore[method-assign]

    started = time.time()
    # agent="none" skips LLM generation. It does NOT skip TRY_SIMILAR, which can
    # reuse an agent-written context from an earlier run, and `candidate_containers`
    # already holds 1856 such rows. That would inflate the result badly.
    #
    # The metric is safe regardless, because Task 4 logs only the TRY_DEFAULT
    # outcome, under agent_name="default_template". TRY_SIMILAR writes no such
    # row. So the rate below counts TRY_DEFAULT alone even when the pipeline
    # goes on to succeed by another route.
    pipeline = Pipeline(agent="none", force=True, n_concurrent=args.n_concurrent)
    asyncio.run(pipeline.run(start_date=args.start_date, end_date=args.end_date, stage=[6]))
    elapsed = time.time() - started

    rows = [
        r
        for r in fetch_all(
            "error_logs",
            select="owner,repo,issue_number,success,duration_s,error_message,created_at",
            filters={"agent_name": "default_template"},
        )
        if (r["owner"], r["repo"], r["issue_number"]) in set(chosen)
    ]
    ok = [r for r in rows if r.get("success")]
    durations = sorted(r["duration_s"] for r in rows if r.get("duration_s"))

    def pct(vals, q):
        return vals[min(len(vals) - 1, int(q * len(vals)))] if vals else float("nan")

    logged = {(r["owner"], r["repo"], r["issue_number"]) for r in rows}
    missing = [c for c in chosen if c not in logged]

    lines = [
        "# Pre-pass trial results",
        "",
        f"Seed {args.seed}. {len(chosen)} repositories, one task each.",
        f"Wall clock {elapsed / 3600:.2f} hours.",
        "",
        f"- repositories selected: {len(chosen)}",
        f"- repositories that reached TRY_DEFAULT: {len(rows)}",
        f"- repositories with no TRY_DEFAULT row: {len(missing)} {missing if missing else ''}",
        f"- built with no agent: {len(ok)}",
        f"- rate: {100 * len(ok) / len(rows):.1f}%" if rows else "- rate: no rows",
        f"- build duration p50: {pct(durations, 0.5):.0f}s, p90: {pct(durations, 0.9):.0f}s",
        "",
        "| repository | issue | built | duration s | error |",
        "|---|---|---|---|---|",
    ]
    for r in sorted(rows, key=lambda x: (x["owner"], x["repo"])):
        msg = (r.get("error_message") or "").splitlines()[0][:80] if r.get("error_message") else ""
        lines.append(
            f"| {r['owner']}/{r['repo']} | {r['issue_number']} | "
            f"{'yes' if r.get('success') else 'no'} | {r.get('duration_s') or 0:.0f} | {msg} |"
        )
    pathlib.Path(args.out).write_text("\n".join(lines) + "\n")
    print(f"[trial] wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Confirm the script selects without building**

Run:

```bash
SUPABASE_URL=http://127.0.0.1:54321 uv run python -c "
import ast, pathlib
ast.parse(pathlib.Path('scripts/prepass_trial.py').read_text())
print('parses')
"
uv run ruff check --isolated scripts/prepass_trial.py
```

Expected: `parses`, and ruff reports no error.

- [ ] **Step 3: Check disk headroom before building**

Run:

```bash
df -h /mnt/sdd2 && docker system df
```

Expected: `/mnt/sdd2` has at least 500 GB free. Images are 9.7 GB at p50, so 20 builds need roughly 200 GB. Stop and report if the headroom is smaller.

- [ ] **Step 4: Run the trial**

Run:

```bash
mkdir -p /mnt/sdd2/tmp-prepass
SUPABASE_URL=http://127.0.0.1:54321 \
DATASMITH_DISABLE_DOCKER_PRUNE=1 \
TMPDIR=/mnt/sdd2/tmp-prepass \
nohup uv run python scripts/prepass_trial.py --sample 20 --seed 20260823 \
  > prepass_trial.log 2>&1 &
```

Expected: roughly 2 to 9 hours, from a build p50 of 446 seconds and p90 of 1606 seconds across 20 builds at 4 concurrent.

- [ ] **Step 5: Read the result and record what it means**

Run:

```bash
cat docs/superpowers/plans/2026-08-23-prepass-trial-results.md
```

Then append one section to that file, answering three questions in plain sentences.

1. What rate did the pre-pass reach, and does it carry most repositories or only a few?
2. Which failures share a cause? Group the error messages, and name any cause that appears three or more times.
3. Does the rate justify the agent work in section 4 of the spec, or does the tail need a different approach?

- [ ] **Step 6: Commit**

```bash
git add scripts/prepass_trial.py docs/superpowers/plans/2026-08-23-prepass-trial-results.md
git commit -m "feat(scripts): measure the no-agent build rate over 20 repos

The design's cost model rests on this number and nothing measured it. Runs
stage 6 with agent=none so only TRY_DEFAULT can succeed, then reads back the
rows Task 4 records."
```

---

## What this plan does not cover

These parts of the spec are deliberately left to later plans. Each needs the trial's number before it can be sized.

- Section 3, the five lock files and the build contract.
- Section 4 steps 2 to 4, the agent in a live container, the typed spec, and replay.
- Section 5, the layer reordering and the three cache mounts.
- Section 6, the three verification tests.
- Moving the run-time harness out of the image.

The prune watcher is disabled for the trial through an environment variable only. Changing its default belongs to the layer plan.
