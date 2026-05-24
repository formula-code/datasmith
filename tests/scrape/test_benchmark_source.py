"""Unit tests for the ASV benchmark source extractor."""

from __future__ import annotations

import json
from pathlib import Path

from datasmith.scrape.benchmark_source import extract_benchmarks

ARITH_PY = '''
def setup(*args, **kwargs):
    """Module-level setup; should attach to every benchmark in this file."""
    return None


class TimeArithmetic:
    params = [(100, 1000), (0, 1)]
    param_names = ["shape", "axis"]

    def setup(self, shape, axis):
        self.df = make_df(shape)

    def time_abs(self, shape, axis):
        execute(self.df.abs())

    def time_neg(self, shape, axis):
        execute(-self.df)


class MemFootprint:
    def mem_total(self):
        return self.df.memory_usage().sum()


def track_loose_metric():
    return 1.0


def some_helper_that_is_not_a_benchmark():
    return None
'''


def _write_repo(tmp_path: Path, files: dict[str, str], conf: dict | None = None) -> Path:
    if conf is not None:
        (tmp_path / "asv.conf.json").write_text(json.dumps(conf))
    for rel, content in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    return tmp_path


def test_extract_class_and_module_benchmarks(tmp_path: Path) -> None:
    repo = _write_repo(tmp_path, {"benchmarks/arithmetic.py": ARITH_PY})
    out = extract_benchmarks(repo)

    names = {b.benchmark_without_params for b in out}
    assert "benchmarks.arithmetic.TimeArithmetic.time_abs" in names
    assert "benchmarks.arithmetic.TimeArithmetic.time_neg" in names
    assert "benchmarks.arithmetic.MemFootprint.mem_total" in names
    assert "benchmarks.arithmetic.track_loose_metric" in names
    assert "benchmarks.arithmetic.some_helper_that_is_not_a_benchmark" not in names


def test_setup_source_includes_module_and_class_setup(tmp_path: Path) -> None:
    repo = _write_repo(tmp_path, {"benchmarks/arithmetic.py": ARITH_PY})
    out = {b.benchmark_without_params: b for b in extract_benchmarks(repo)}

    time_abs = out["benchmarks.arithmetic.TimeArithmetic.time_abs"]
    assert time_abs.setup_source is not None
    assert "def setup(*args, **kwargs)" in time_abs.setup_source  # module-level
    assert "self.df = make_df(shape)" in time_abs.setup_source  # class-level

    track = out["benchmarks.arithmetic.track_loose_metric"]
    assert track.setup_source is not None
    assert "def setup(*args, **kwargs)" in track.setup_source
    # No class-level setup for module-level function:
    assert "self.df = make_df" not in track.setup_source


def test_honours_asv_conf_benchmark_dir(tmp_path: Path) -> None:
    repo = _write_repo(
        tmp_path,
        {"asv_bench/benchmarks/arithmetic.py": ARITH_PY},
        conf={"benchmark_dir": "asv_bench/benchmarks"},
    )
    out = extract_benchmarks(repo)
    names = {b.benchmark_without_params for b in out}
    # Module prefix should be the leaf dir name from benchmark_dir, not the full path.
    assert "benchmarks.arithmetic.TimeArithmetic.time_abs" in names


def test_no_benchmark_dir_returns_empty(tmp_path: Path) -> None:
    repo = _write_repo(tmp_path, {"src/lib.py": "x = 1"})
    assert extract_benchmarks(repo) == []


def test_class_methods_include_class_header_in_source(tmp_path: Path) -> None:
    """Website needs class-level attrs like `params` to render meaningful code blocks."""
    repo = _write_repo(tmp_path, {"benchmarks/arithmetic.py": ARITH_PY})
    out = {b.benchmark_without_params: b for b in extract_benchmarks(repo)}
    body = out["benchmarks.arithmetic.TimeArithmetic.time_abs"].source
    assert "class TimeArithmetic" in body
    assert "params = [(100, 1000), (0, 1)]" in body
    assert "def time_abs" in body
