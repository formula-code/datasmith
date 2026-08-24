"""PRODUCE_VERIFY builds without the legacy `rc != 0` pytest gate.

In that path pytest runs only in the verifier's battery, and severity.py
grades the verdict. The legacy gate still applies to TRY_SIMILAR and
TRY_DEFAULT, whose behaviour must not change.
"""

from __future__ import annotations

import importlib.util
import inspect
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

from datasmith.agents.sandbox import SandboxResult, verify_context


def test_sandbox_result_carries_the_image_tag() -> None:
    """The loop must never guess the tag. verify_context serves TRY_SIMILAR
    and does not necessarily tag what another caller would assume."""
    assert "image_tag" in SandboxResult.__dataclass_fields__
    assert SandboxResult(success=False).image_tag == ""


def test_verify_context_accepts_run_tests_gate() -> None:
    params = inspect.signature(verify_context).parameters
    assert "run_tests_gate" in params


def test_the_gate_defaults_to_on_so_legacy_callers_are_unchanged() -> None:
    """TRY_SIMILAR and TRY_DEFAULT must behave exactly as before."""
    assert inspect.signature(verify_context).parameters["run_tests_gate"].default is True


def test_local_ci_is_told_to_skip_the_gate_when_asked() -> None:
    source = inspect.getsource(verify_context)
    assert "run_tests_gate" in source
    assert "--skip-test-gate" in source, "the flag must reach local_ci.py"


# ── Behaviour, not just shape ────────────────────────────────────────────
# The four tests above pin the wire between verify_context and local_ci.py.
# These pin what the flag actually DOES, because a flag that is threaded
# correctly and ignored inside verify() would leave them all green.

_LOCAL_CI = Path(__file__).parents[3] / "src" / "datasmith" / "agents" / "templates" / "local_ci.py"


def _load_local_ci(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """local_ci.py ships as a template, not an importable module."""
    spec = importlib.util.spec_from_file_location("local_ci", _LOCAL_CI)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "local_ci", mod)
    spec.loader.exec_module(mod)
    return mod


def _task_dir(tmp_path: Path) -> Path:
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    (task_dir / "task.txt").write_text(
        "Task(owner='o', repo='r', sha='" + "a" * 40 + "', env_payload='[]', python_version='3.11', repo_image='img')"
    )
    (task_dir / "solution.patch").write_text("diff --git a/x b/x\n")
    return task_dir


def _stub_build(m: ModuleType, monkeypatch: pytest.MonkeyPatch, manifest: dict) -> None:
    monkeypatch.setattr(m, "DockerClient", lambda *a, **k: object())
    monkeypatch.setattr(m, "build_image", lambda *a, **k: "img:tag")
    monkeypatch.setattr(m, "read_manifest_from_image", lambda tag: manifest)


def _clean_manifest() -> dict:
    return {
        "schema_version": 1,
        "build": {
            "discovered_n": 3,
            "declared_commit": "a" * 40,
            "head_at_seal": "a" * 40,
            "secrets_scan_clean": True,
        },
        "verify": {},
    }


def test_a_failing_pytest_still_reaches_measure_when_the_gate_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole point: the verifier cannot weigh a container it never gets.

    With the gate on, a fluids-shaped repo dies at `tests` and the loop sees
    build_failed. With it off, the run continues and the image exists.
    """
    m = _load_local_ci(monkeypatch)
    task_dir = _task_dir(tmp_path)
    _stub_build(m, monkeypatch, _clean_manifest())

    def _tests(tag, timeout=None, metrics=None):
        if metrics is not None:
            metrics["test_timed_out"] = False
        return False, "", "pytest exploded", 1

    def _measure(tag, patch_path, timeout=None, metrics=None):
        if metrics is not None:
            metrics["measure_timed_out"] = False
        block = {"benchmarks_measured_n": 4, "patch_present": True, "patch_applied": True}
        return True, f"FORMULACODE_MEASURE_START\n{json.dumps(block)}\nFORMULACODE_MEASURE_END\n", "", 0

    monkeypatch.setattr(m, "run_tests", _tests)
    monkeypatch.setattr(m, "run_measure", _measure)
    monkeypatch.setattr(m, "_SKIP_TEST_GATE", True)

    assert m.verify(task_dir) is True
    info = json.loads((task_dir / "verification_success.json").read_text())
    assert info["local_image"] == "img:tag", "the tag verify_context reads back must be recorded"


def test_the_facts_are_still_collected_when_the_gate_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skipping the GATE must not skip collecting the facts, or PRODUCE_VERIFY
    would accept a container whose manifest is empty."""
    m = _load_local_ci(monkeypatch)
    task_dir = _task_dir(tmp_path)
    _stub_build(m, monkeypatch, _clean_manifest())

    def _tests(tag, timeout=None, metrics=None):
        if metrics is not None:
            metrics["test_duration_s"] = 12.0
            metrics["test_timed_out"] = False
        return False, "", "pytest exploded", 1

    def _measure(tag, patch_path, timeout=None, metrics=None):
        if metrics is not None:
            metrics["measure_timed_out"] = False
        block = {"benchmarks_measured_n": 4, "patch_present": True, "patch_applied": True}
        return True, f"FORMULACODE_MEASURE_START\n{json.dumps(block)}\nFORMULACODE_MEASURE_END\n", "", 0

    monkeypatch.setattr(m, "run_tests", _tests)
    monkeypatch.setattr(m, "run_measure", _measure)
    monkeypatch.setattr(m, "_SKIP_TEST_GATE", True)

    assert m.verify(task_dir) is True
    manifest = json.loads((task_dir / "verification_success.json").read_text())["resource_metrics"]["build_manifest"]
    assert manifest["verify"]["test_duration_s"] == 12.0
    assert manifest["verify"]["benchmarks_measured_n"] == 4


def test_a_timed_out_test_run_is_still_rejected_with_the_gate_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An error is a rejection, never an acceptance.

    Skipping the gate must not launder a host-side timeout into success --
    the `test_timed_out` FATAL invariant is the backstop, and skipping the
    gate is precisely what makes that branch reachable.
    """
    m = _load_local_ci(monkeypatch)
    task_dir = _task_dir(tmp_path)
    _stub_build(m, monkeypatch, _clean_manifest())

    def _tests(tag, timeout=None, metrics=None):
        if metrics is not None:
            metrics["test_timed_out"] = True
        return False, "", "killed at the limit", 124

    def _measure(tag, patch_path, timeout=None, metrics=None):
        if metrics is not None:
            metrics["measure_timed_out"] = False
        block = {"benchmarks_measured_n": 4, "patch_present": True, "patch_applied": True}
        return True, f"FORMULACODE_MEASURE_START\n{json.dumps(block)}\nFORMULACODE_MEASURE_END\n", "", 0

    monkeypatch.setattr(m, "run_tests", _tests)
    monkeypatch.setattr(m, "run_measure", _measure)
    monkeypatch.setattr(m, "_SKIP_TEST_GATE", True)

    assert m.verify(task_dir) is False
    failure = json.loads((task_dir / "failure.json").read_text())
    assert failure["stage"] == "invariants"
    assert "test_timed_out" in failure["stderr"]


def test_the_gate_still_fires_when_it_is_not_skipped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The default is unchanged: TRY_SIMILAR and TRY_DEFAULT still die at
    `tests`, and never pay for a measure run on a doomed build."""
    m = _load_local_ci(monkeypatch)
    task_dir = _task_dir(tmp_path)
    _stub_build(m, monkeypatch, _clean_manifest())
    called: list[int] = []

    monkeypatch.setattr(m, "run_tests", lambda *a, **k: (False, "", "pytest exploded", 1))
    monkeypatch.setattr(m, "run_measure", lambda *a, **k: called.append(1) or (True, "", "", 0))

    assert m._SKIP_TEST_GATE is False, "the module default must be the legacy behaviour"
    assert m.verify(task_dir) is False
    assert called == []
    assert json.loads((task_dir / "failure.json").read_text())["stage"] == "tests"
