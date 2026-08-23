"""The dry-run must target an interpreter uv is allowed to write to.

``uv pip install --dry-run --python 3.12 --system`` does not check a seed on this
host, or on any host whose Pythons are uv-managed.  uv answers::

    error: The interpreter at .../cpython-3.12-linux-x86_64-gnu is externally
    managed, and indicates the following:
      This Python installation is managed by uv and should not be modified.
    hint: Virtual environments were not considered due to the `--system` flag

Every commit therefore came back ``failed``, ``can_install`` came back false for
every row, and stages 5 and 6 -- which still filter on it -- were starved by the
very stage meant to unblock them.  The recorded status was the host refusing,
not a fact about the requirements.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import datasmith.resolution.dependency_resolver as dr
from datasmith.resolution.python_manager import SUPPORTED_PYTHON_VERSIONS

# Anchor the grep at the repository, not at the current directory: run from
# anywhere else, a relative path makes grep fail, stdout comes back empty, and
# the guard passes while the flag is back. `resolution` is the whole search
# space -- it holds the only two modules in datasmith that invoke uv.
SRC = Path(__file__).resolve().parents[2] / "src" / "datasmith" / "resolution"

SEED = ["numpy==2.0", "scipy==1.14.0"]


@pytest.fixture
def uv_calls(monkeypatch):
    """Record every uv invocation and run none of them."""
    calls: list[list[str]] = []

    def fake_run_uv(args, *, input_text=None, cwd=None, extra_env=None, check=False):
        calls.append(list(args))
        return subprocess.CompletedProcess(args=args, returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(dr, "run_uv", fake_run_uv)
    return calls


def _dry_run_call(calls: list[list[str]]) -> list[str]:
    return next(args for args in calls if args[:2] == ["pip", "install"])


def test_the_system_flag_is_never_passed(uv_calls):
    dr.uv_dry_run_install(SEED, python_version="3.12")
    for args in uv_calls:
        assert "--system" not in args, args


def test_no_source_file_reaches_for_the_system_flag():
    assert SRC.is_dir(), f"source tree not found at {SRC}"
    cp = subprocess.run(["grep", "-rn", '"--system"', str(SRC)], capture_output=True, text=True)
    # grep exits 0 on a match and 1 on no match; anything else is grep itself
    # failing, which would make an empty stdout meaningless.
    assert cp.returncode in (0, 1), f"grep failed ({cp.returncode}): {cp.stderr}"
    assert cp.stdout == "", f"--system is back:\n{cp.stdout}"


def test_the_system_flag_guard_sees_a_planted_reference(tmp_path):
    # The guard above is only worth having if it can fail. Prove the grep it runs
    # finds the flag, using the same invocation against a decoy file.
    (tmp_path / "decoy.py").write_text('args.extend(["--python", v, "--system"])\n')
    cp = subprocess.run(["grep", "-rn", '"--system"', str(tmp_path)], capture_output=True, text=True)
    assert cp.returncode == 0
    assert "--system" in cp.stdout


def test_a_requested_version_gets_a_throwaway_environment(uv_calls):
    dr.uv_dry_run_install(SEED, python_version="3.12")
    venv_calls = [args for args in uv_calls if args[0] == "venv"]
    assert len(venv_calls) == 1, uv_calls
    assert venv_calls[0][2:] == ["--python", "3.12"]

    created = Path(venv_calls[0][1])
    dry_run = _dry_run_call(uv_calls)
    assert dry_run[-2] == "--python"
    assert Path(dry_run[-1]).parent.parent == created


def test_the_throwaway_environment_does_not_outlive_the_call(uv_calls):
    dr.uv_dry_run_install(SEED, python_version="3.12")
    created = Path(next(args for args in uv_calls if args[0] == "venv")[1])
    assert not created.parent.exists()


def test_no_version_asks_for_no_environment(uv_calls):
    # The caller has said nothing about the interpreter, so uv's own default
    # stands and there is nothing to build.
    dr.uv_dry_run_install(SEED, python_version=None)
    assert [args for args in uv_calls if args[0] == "venv"] == []
    assert "--python" not in _dry_run_call(uv_calls)


def test_a_supplied_environment_is_used_as_it_is(uv_calls, tmp_path):
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "python").touch()

    dr.uv_dry_run_install(SEED, python_version="3.12", venv_path=venv)
    assert [args for args in uv_calls if args[0] == "venv"] == []
    assert _dry_run_call(uv_calls)[-1] == str(venv / "bin" / "python")


def test_an_unbuildable_environment_is_not_reported_as_a_bad_seed(monkeypatch):
    # A host that cannot supply the interpreter says nothing about the
    # requirements, so the log has to be told apart from a resolution failure.
    def fake_run_uv(args, *, input_text=None, cwd=None, extra_env=None, check=False):
        assert args[0] == "venv", f"the dry-run must not run without an environment: {args}"
        return subprocess.CompletedProcess(args=args, returncode=1, stdout=b"", stderr=b"no such python")

    monkeypatch.setattr(dr, "run_uv", fake_run_uv)
    ok, log = dr.uv_dry_run_install(SEED, python_version="3.99")
    assert ok is False
    assert log.startswith(dr.VENV_SETUP_FAILED)
    assert "no such python" in log


def test_an_empty_seed_never_touches_uv(uv_calls):
    ok, log = dr.uv_dry_run_install([], python_version="3.12")
    assert ok is True
    assert uv_calls == []
    assert "No runtime dependencies" in log


@pytest.mark.slow
@pytest.mark.parametrize("version", [f"{major}.{minor}" for major, minor in sorted(SUPPORTED_PYTHON_VERSIONS)])
def test_a_real_dry_run_against_a_real_interpreter_succeeds(version):
    # The unit tests above pin the call shape; this one proves the shape works
    # against the uv on this machine, which is what the shape was wrong about.
    #
    # Every version the interpreter ladder can return is covered, not just the
    # one this host happens to have unpacked. A version whose environment cannot
    # be built reports VENV_SETUP_FAILED, which reads as `failed`, which reads as
    # can_install=False -- the original blocker, narrowed to a version band.
    ok, log = dr.uv_dry_run_install(["packaging==24.2"], python_version=version)
    assert ok, log
    assert "externally managed" not in log
    assert dr.VENV_SETUP_FAILED not in log
