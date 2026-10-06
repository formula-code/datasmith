"""Image LSV baseline reuse: same sha, and the same timing fingerprint unless timing is paired."""

import importlib.util
from pathlib import Path

import pytest

_TEMPLATE = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template"
_LSV_INIT = _TEMPLATE / "tests" / "lsv_init.py"

FP = {
    "cpu_model": "AMD EPYC 7B13",
    "usable_cpus": 2,
    "os_cpu_count": 64,
    "affinity_cpus": 2,
    "cpu_limit": "pinned",
    "numba_num_threads": "2",
    "lsv_rounds": 5,
    "lsv_commit": "fc16ba47239f68b93c335be22b1d2c37d259667c",
}


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("fc_lsv_init_test", _LSV_INIT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _image(**kw):
    return {"baseline_sha": "abc", "env_fingerprint": dict(FP), **kw}


def test_match_reuses(m):
    assert m.image_reuse_reason(_image(), "abc", dict(FP)) is None


@pytest.mark.parametrize("field", sorted(FP))
def test_any_field_differs_remeasures(m, field):
    trial = dict(FP, **{field: "other"})
    reason = m.image_reuse_reason(_image(), "abc", trial)
    assert reason and field in reason


@pytest.mark.parametrize(
    "image,head",
    [
        (None, "abc"),
        (_image(), "def"),
        (_image(baseline_sha=None), None),
        ({"baseline_sha": "abc"}, "abc"),
    ],
)
def test_missing_or_stale_remeasures(m, image, head):
    assert m.image_reuse_reason(image, head, dict(FP))


def test_fingerprint_has_every_field(m):
    assert set(m.env_fingerprint(5)) == set(FP)
    assert m.env_fingerprint(5)["usable_cpus"] >= 1


def test_dockerfile_bounds_build_to_trial_cpus():
    from datasmith.harbor_adapter.utils import render_dockerfile

    df = render_dockerfile("img", numba_threads=3, lsv_rounds=5)
    assert "FORMULACODE_IMAGE_CPUS=3" in df
    assert "NUMBA_NUM_THREADS=3" in df


def test_pinned_build_and_quota_trial_differ(m, monkeypatch):
    monkeypatch.setattr(m.os, "cpu_count", lambda: 64)
    monkeypatch.setattr(m, "_affinity_cpus", lambda: 2)
    monkeypatch.setattr(m, "_cfs_quota_cpus", lambda: None)
    assert m._cpu_limit() == "pinned"
    monkeypatch.setattr(m, "_affinity_cpus", lambda: 64)
    monkeypatch.setattr(m, "_cfs_quota_cpus", lambda: 2)
    assert m._cpu_limit() == "quota"


def test_paired_reuse_ignores_cpu_and_rounds(m):
    trial = dict(FP, os_cpu_count=128, affinity_cpus=128, cpu_limit="quota", lsv_rounds=3)
    assert m.image_reuse_reason(_image(), "abc", trial) is not None
    assert m.image_reuse_reason(_image(), "abc", trial, paired=True) is None
    old_build = {k: v for k, v in FP.items() if k not in ("os_cpu_count", "affinity_cpus", "cpu_limit")}
    assert m.image_reuse_reason(_image(env_fingerprint=old_build), "abc", trial, paired=True) is None


@pytest.mark.parametrize(
    "image,head,trial", [(_image(), "def", dict(FP)), (_image(), "abc", dict(FP, lsv_commit="other"))]
)
def test_paired_reuse_still_needs_sha_and_lsv(m, image, head, trial):
    assert m.image_reuse_reason(image, head, trial, paired=True)


def test_baseline_commit_reuses_the_image_measured_at_its_parent(m, tmp_path, monkeypatch):
    import subprocess

    def git(*a):
        return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], cwd=tmp_path, check=True,
                              capture_output=True, text=True).stdout.strip()

    git("init", "-q")
    git("commit", "-q", "--allow-empty", "-m", "upstream")
    upstream = git("rev-parse", "HEAD")
    git("commit", "-q", "--allow-empty", "-m", "fc-baseline")
    head = git("rev-parse", "HEAD")
    sha_file = tmp_path / "fc_baseline_sha"
    monkeypatch.setattr(m, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(m, "BASELINE_SHA_FILE", str(sha_file))
    assert m.image_commit(head) == head
    sha_file.write_text(head + "\n")
    assert m.image_commit(head) == upstream
    assert m.image_reuse_reason(_image(baseline_sha=upstream), m.image_commit(head), dict(FP)) is None
