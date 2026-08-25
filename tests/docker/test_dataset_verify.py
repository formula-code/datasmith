"""Tests for dataset/verify.py's timeout handling.

Two distinct defects lived here, and they are guarded together so neither can
be reintroduced by copying the other:

1. ``run_profile()`` scored a timeout as SUCCESS (``if "124" in err: return
   True``). It was never called, but leaving a working example of the
   inversion in the tree invites its reuse.
2. ``run_tests()`` passed no timeout to ``docker.run`` at all, so it could not
   time out and would hang indefinitely on a container that never exits.

dataset/ is not part of the installed package, so the module is loaded by
path -- the same pattern tests/docker/test_emit_manifest.py uses for the
in-image templates.
"""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_VERIFY = Path(__file__).parents[2] / "dataset" / "verify.py"


# ``dataset/`` is gitignored (.gitignore:216) and was removed from git in
# e37cf3a, so the module this file tests is present only in a checkout that
# happens to carry the untracked directory. Without this guard the whole class
# errors on collection in CI, in a fresh clone, and in any git worktree -- which
# reads as a broken test suite rather than as an absent optional input.
pytestmark = pytest.mark.skipif(
    not (Path(__file__).parents[2] / "dataset" / "verify.py").exists(),
    reason="dataset/ is untracked (gitignored); verify.py is not in this checkout",
)


def _load(monkeypatch=None):
    spec = importlib.util.spec_from_file_location("dataset_verify", _VERIFY)
    mod = importlib.util.module_from_spec(spec)
    if monkeypatch is not None:
        monkeypatch.setitem(sys.modules, "dataset_verify", mod)
    else:
        sys.modules["dataset_verify"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    sys.modules.pop("dataset_verify", None)


class TestTimeoutIsAFailure:
    def test_timeout_returns_failure(self, monkeypatch):
        """The whole point. A container killed at the limit must not verify."""
        m = _load(monkeypatch)

        def _boom(*a, **k):
            raise subprocess.TimeoutExpired(cmd="docker run", timeout=5)

        monkeypatch.setattr(m.subprocess, "run", _boom)
        ok, out = m.run_tests(object(), "img:tag", timeout=5)
        assert ok is False
        assert "exceeded the 5s limit" in out

    def test_timeout_names_the_knob_that_raises_it(self, monkeypatch):
        m = _load(monkeypatch)

        def _boom(*a, **k):
            raise subprocess.TimeoutExpired(cmd="docker run", timeout=5)

        monkeypatch.setattr(m.subprocess, "run", _boom)
        _ok, out = m.run_tests(object(), "img:tag", timeout=5)
        assert "DATASMITH_VERIFY_TEST_TIMEOUT_S" in out

    def test_a_timeout_is_actually_passed_to_the_subprocess(self, monkeypatch):
        """Guards the original defect directly: no timeout reached docker.run,
        so the call could never time out however long the container ran."""
        m = _load(monkeypatch)
        seen = {}

        class _R:
            returncode = 0
            stdout = "ok"
            stderr = ""

        def _capture(cmd, **kw):
            seen.update(kw)
            return _R()

        monkeypatch.setattr(m.subprocess, "run", _capture)
        m.run_tests(object(), "img:tag", timeout=1234)
        assert seen.get("timeout") == 1234, "run_tests did not pass a timeout to the subprocess"

    def test_default_timeout_comes_from_the_env_knob(self, monkeypatch):
        m = _load(monkeypatch)
        seen = {}

        class _R:
            returncode = 0
            stdout = "ok"
            stderr = ""

        def _capture(cmd, **kw):
            seen.update(kw)
            return _R()

        monkeypatch.setattr(m.subprocess, "run", _capture)
        m.run_tests(object(), "img:tag")
        assert seen.get("timeout") == m.DATASMITH_VERIFY_TEST_TIMEOUT_S
        assert m.DATASMITH_VERIFY_TEST_TIMEOUT_S == 3600


class TestNonZeroExitIsAFailure:
    def test_failing_container_does_not_verify(self, monkeypatch):
        m = _load(monkeypatch)

        class _R:
            returncode = 1
            stdout = "pytest exploded"
            stderr = ""

        monkeypatch.setattr(m.subprocess, "run", lambda *a, **k: _R())
        ok, out = m.run_tests(object(), "img:tag")
        assert ok is False
        assert "exploded" in out

    def test_no_benchmarks_sentinel_is_a_failure(self, monkeypatch):
        m = _load(monkeypatch)

        class _R:
            returncode = 0
            stdout = "FORMULACODE_NO_BENCHMARKS: 0 ASV benchmarks discovered."
            stderr = ""

        monkeypatch.setattr(m.subprocess, "run", lambda *a, **k: _R())
        ok, out = m.run_tests(object(), "img:tag")
        assert ok is False
        assert "No ASV benchmarks" in out

    def test_clean_run_verifies(self, monkeypatch):
        m = _load(monkeypatch)

        class _R:
            returncode = 0
            stdout = "all good"
            stderr = ""

        monkeypatch.setattr(m.subprocess, "run", lambda *a, **k: _R())
        ok, _out = m.run_tests(object(), "img:tag")
        assert ok is True


class TestTimeoutAsSuccessIsGone:
    def test_run_profile_no_longer_exists(self):
        """It carried `if "124" in err: return True` -- an explicit
        timeout-as-success. Dead code, but the exact inversion this effort
        exists to remove, so it is deleted rather than left as a template."""
        m = _load()
        assert not hasattr(m, "run_profile")

    def test_source_contains_no_timeout_as_success_inversion(self):
        src = _VERIFY.read_text()
        assert "treated as success" not in src

    def test_docstring_no_longer_claims_profile_runs_inside_run_tests(self):
        """The module docstring said run-tests.sh 'includes profile.sh'. It
        does not, and never did."""
        src = _VERIFY.read_text()
        assert "which includes profile.sh" not in src
