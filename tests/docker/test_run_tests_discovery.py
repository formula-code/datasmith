"""`no benchmarks` and `our discovery broke` must not print the same thing.

run-tests.sh exits 78 when asv_benchmarks.txt is empty, with the message "This
task has no benchmarks and cannot be used in the FormulaCode dataset." Six
repositories in the 2026-08-23 trial hit that line -- including pydata/xarray
and joblib/joblib, which plainly do have benchmark suites. joblib had already
run 1526 passing tests by that point.

The source count decides which message is right. It is extracted from the
template and executed here so the embedded program is actually tested rather
than merely present.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

_TEMPLATE = Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "run-tests.sh"


@pytest.fixture(scope="module")
def counter_source() -> str:
    text = _TEMPLATE.read_text(encoding="utf-8")
    match = re.search(r"<<'SRC_COUNT_EOF'.*?\n(.*?)\nSRC_COUNT_EOF", text, re.DOTALL)
    assert match, "the embedded source counter is missing from run-tests.sh"
    return match.group(1)


def _run(counter: str, conf: Path) -> str:
    proc = subprocess.run([sys.executable, "-c", counter, str(conf)], capture_output=True, text=True, timeout=60)
    return proc.stdout.strip()


def _write_suite(tmp_path: Path, body: str) -> Path:
    bench = tmp_path / "benchmarks"
    bench.mkdir()
    (bench / "bench.py").write_text(body, encoding="utf-8")
    conf = tmp_path / "asv.conf.json"
    conf.write_text('{\n  // comment\n  "benchmark_dir": "benchmarks"\n}\n', encoding="utf-8")
    return conf


def test_counts_a_real_suite(counter_source: str, tmp_path: Path) -> None:
    conf = _write_suite(
        tmp_path,
        "class S:\n    def time_a(self):\n        pass\n    def peakmem_b(self):\n        pass\n"
        "    def setup(self):\n        pass\n",
    )
    assert _run(counter_source, conf) == "2"


def test_a_module_with_a_missing_dependency_still_counts(counter_source: str, tmp_path: Path) -> None:
    """The joblib/xarray shape: the suite exists, the import fails."""
    conf = _write_suite(tmp_path, "import a_module_that_does_not_exist\n\ndef time_a():\n    pass\n")
    assert _run(counter_source, conf) == "1"


def test_a_repo_with_no_benchmarks_counts_zero(counter_source: str, tmp_path: Path) -> None:
    conf = _write_suite(tmp_path, "def helper():\n    pass\n")
    assert _run(counter_source, conf) == "0"


def test_an_unreadable_config_reports_unknown_not_zero(counter_source: str, tmp_path: Path) -> None:
    """`?` must not be mistaken for `0`. Only a real 0 writes a task off."""
    conf = tmp_path / "asv.conf.json"
    conf.write_text("{not json at all\n", encoding="utf-8")
    assert _run(counter_source, conf) == "?"


def test_the_two_outcomes_print_different_markers() -> None:
    text = _TEMPLATE.read_text(encoding="utf-8")
    assert "FORMULACODE_NO_BENCHMARKS" in text
    assert "FORMULACODE_DISCOVERY_FAILED" in text
    assert text.index("FORMULACODE_DISCOVERY_FAILED") > text.index('_SRC_N" = "0"'), (
        "the discovery-failed branch must be guarded by the source count"
    )
