"""Baked LSV baseline reuse: only at the same sha and the same timing fingerprint."""

import importlib.util
from pathlib import Path

import pytest

_TEMPLATE = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template"
_LSV_INIT = _TEMPLATE / "tests" / "lsv_init.py"

FP = {
    "cpu_model": "AMD EPYC 7B13",
    "usable_cpus": 2,
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


def _baked(**kw):
    return {"baseline_sha": "abc", "env_fingerprint": dict(FP), **kw}


def test_match_reuses(m):
    assert m.baked_reuse_reason(_baked(), "abc", dict(FP)) is None


@pytest.mark.parametrize("field", sorted(FP))
def test_any_field_differs_remeasures(m, field):
    trial = dict(FP, **{field: "other"})
    reason = m.baked_reuse_reason(_baked(), "abc", trial)
    assert reason and field in reason


@pytest.mark.parametrize(
    "baked,head",
    [
        (None, "abc"),
        (_baked(), "def"),
        (_baked(baseline_sha=None), None),
        ({"baseline_sha": "abc"}, "abc"),
    ],
)
def test_missing_or_stale_remeasures(m, baked, head):
    assert m.baked_reuse_reason(baked, head, dict(FP))


def test_fingerprint_has_every_field(m):
    assert set(m.env_fingerprint(5)) == set(FP)
    assert m.env_fingerprint(5)["usable_cpus"] >= 1


def test_dockerfile_bounds_bake_to_trial_cpus():
    from datasmith.harbor_adapter.utils import render_dockerfile

    df = render_dockerfile("img", numba_threads=3, lsv_rounds=5)
    assert "FORMULACODE_BAKE_CPUS=3" in df
    assert "NUMBA_NUM_THREADS=3" in df
