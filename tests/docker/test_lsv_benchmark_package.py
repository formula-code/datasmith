"""At image build, a benchmark folder without __init__.py gets one that git does not list as untracked."""

import importlib.util
import subprocess
from pathlib import Path

_LSV_INIT = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "lsv_init.py"


def test_benchmark_folder_becomes_a_package_git_ignores(tmp_path):
    spec = importlib.util.spec_from_file_location("fc_lsv_init_pkg", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    (tmp_path / "benchmarks").mkdir()
    (tmp_path / "benchmarks" / "bench_a.py").write_text("def time_a():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "benchmarks/bench_a.py"], check=True)
    assert m.make_benchmark_package(tmp_path, tmp_path / "benchmarks")
    assert (tmp_path / "benchmarks" / "__init__.py").is_file()
    others = subprocess.check_output(["git", "-C", str(tmp_path), "ls-files", "--others", "--exclude-standard"], text=True)
    assert "__init__.py" not in others
    assert not m.make_benchmark_package(tmp_path, tmp_path / "benchmarks")
