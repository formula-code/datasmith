"""Paired LSV measure: A-B-B-A order, the patched tree is always back in place, paired log-ratio math."""

import importlib.util
import math
from pathlib import Path

import pytest

_TEMPLATE = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests"


def _load(name):
    spec = importlib.util.spec_from_file_location(f"fc_{name}_test", _TEMPLATE / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def m(tmp_path, monkeypatch):
    mod = _load("lsv_measure")
    for attr, name, side in (("REPO_ROOT", "repo", "patched"), ("BASE_COPY", ".fc_base", "base")):
        d = tmp_path / name
        d.mkdir()
        (d / "side").write_text(side)
        monkeypatch.setattr(mod, attr, d)
    monkeypatch.setattr(mod, "PARKED", tmp_path / ".fc_patched")
    (tmp_path / "tmp").mkdir()
    monkeypatch.setattr(mod, "TEMP_ROOTS", (tmp_path / "tmp",))
    monkeypatch.setattr(mod.tempfile, "tempdir", str(tmp_path / "tmp"))
    return mod


def _side(m):
    return (m.REPO_ROOT / "side").read_text()


def test_rounds_alternate_abba(m):
    seen = []
    m.run_paired(lambda: seen.append(_side(m)) or {}, 4)
    assert seen == ["base", "patched", "patched", "base"] * 2
    assert _side(m) == "patched" and (m.BASE_COPY / "side").read_text() == "base"


def test_rounds_remove_the_temp_dirs_benchmarks_leave(m):
    keep = Path(m.tempfile.gettempdir()) / "before"
    keep.mkdir()
    m.run_paired(lambda: Path(m.tempfile.mkdtemp()).joinpath("array").write_text("x") and {}, 2)
    assert list(Path(m.tempfile.gettempdir()).iterdir()) == [keep]


def test_error_on_base_side_leaves_repo_patched(m):
    def run():
        if _side(m) == "base":
            raise RuntimeError("benchmark crashed")
        return {}

    with pytest.raises(RuntimeError):
        m.run_paired(run, 3)
    assert _side(m) == "patched" and (m.BASE_COPY / "side").read_text() == "base"
    assert not m.PARKED.exists()


def test_paired_stats_median_log_ratio():
    m = _load("lsv_measure")
    rounds = [
        {"base": {"a": 2.0, "b": 1.0}, "patched": {"a": 1.0, "b": 1.0}},
        {"base": {"a": 4.0}, "patched": {"a": 1.0, "b": 1.0}},
        {"base": {"a": 1.0, "b": 1.0}, "patched": {"a": 1.0}},
    ]
    stats = m.paired_stats(rounds, 2)
    assert set(stats) == {"a"}
    assert stats["a"]["log_ratios"] == pytest.approx([math.log(2), math.log(4), 0.0])
    assert stats["a"]["speedup"] == pytest.approx(2.0)
    assert stats["a"]["mad_log_ratio"] == pytest.approx(math.log(2))
    assert stats["a"]["base_times"] == [2.0, 4.0, 1.0]


def test_parser_prefers_paired_speedup():
    p = _load("parser")
    bench = {"a": {"baseline": 3.0, "current": 1.0, "paired": {"speedup": 1.5}}, "b": {"baseline": 2.0, "current": 1.0}}
    assert p.compute_per_benchmark_speedups(bench) == {"a": 1.5, "b": 2.0}
