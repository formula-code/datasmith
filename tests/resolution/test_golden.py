"""The 13 audited commits, locked in.

Marked slow: these clone real repositories and shell out to uv.

Point ``GIT_CACHE_DIR`` at a populated git cache before running them. Without
it ``CACHE_LOCATION`` falls back to ``cache.db`` in the working directory,
``GIT_CACHE_DIR`` becomes ``./git``, and the run clones all 13 repositories
from scratch instead of adding a worktree to a clone that already exists.

Regenerating a fixture: resolve the commit again, write
``json.dumps(dataclasses.asdict(result) | {"repo_name": ...}, indent=2,
sort_keys=True)`` with ``probe_log`` removed, and hand-review the result
against the audit before committing it. Note that every fixture records
``DATASMITH_PYTHON_CEILING`` as its ``python_version``, so raising the ceiling
means regenerating all 13.
"""

import json
from dataclasses import replace
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
    assert a is not None and b is not None
    # ``probe_log`` is uv's own console output. It carries the elapsed time of
    # the resolve ("Resolved 1 package in 33ms" against "in 2ms") and the path
    # of the throwaway probe venv, and both differ between two runs of one
    # commit on one host. It is not part of the contract -- the fixture
    # generator drops it for the same reason -- so it is normalised away here
    # and every other field is still compared exactly.
    assert replace(a, probe_log="") == replace(b, probe_log="")


def test_every_audited_repo_has_a_fixture():
    names = {p.stem.split("__")[0] for p in _cases()}
    for owner in (
        "apache",
        "h5py",
        "napari",
        "numpy",
        "optuna",
        "pandas-dev",
        "PostHog",
        "pypa",
        "quantumlib",
        "scikit-learn",
        "scipy",
        "shapely",
        "xdslproject",
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
