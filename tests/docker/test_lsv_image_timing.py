"""The image build keeps LSV's coverage pass and skips its full-suite timing when trials are paired."""

import importlib.util
import types
from pathlib import Path

_LSV_INIT = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "lsv_init.py"


def _load():
    spec = importlib.util.spec_from_file_location("fc_lsv_init_timing", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_timing_skipped_only_at_paired_image_build(monkeypatch):
    m = _load()
    monkeypatch.delenv("FC_LSV_PAIRED", raising=False)
    monkeypatch.delenv("FORMULACODE_IMAGE_TIMING", raising=False)
    assert m.image_timing_skipped(True) and not m.image_timing_skipped(False)
    monkeypatch.setenv("FORMULACODE_IMAGE_TIMING", "1")
    assert not m.image_timing_skipped(True)
    monkeypatch.delenv("FORMULACODE_IMAGE_TIMING")
    monkeypatch.setenv("FC_LSV_PAIRED", "0")
    assert not m.image_timing_skipped(True)


def test_skip_suite_timing_replaces_the_timing_calls():
    m = _load()
    fake = types.SimpleNamespace(run_benchmarks=lambda *a, **k: 1 / 0, _store_baseline=lambda *a, **k: 1 / 0)
    m.skip_suite_timing(fake)
    assert fake.run_benchmarks(["b"], None) == {} and fake._store_baseline({}, [], None) is None
