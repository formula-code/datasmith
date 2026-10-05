"""At image build, benchmark-only dependencies the env lacks are found from asv.conf.json and the benchmark imports."""

import importlib.util
from pathlib import Path

_LSV_INIT = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "lsv_init.py"


def test_missing_benchmark_deps(tmp_path):
    spec = importlib.util.spec_from_file_location("fc_lsv_init_deps", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    bench = tmp_path / "benchmarks"
    bench.mkdir()
    (bench / "common.py").write_text("X = 1\n")
    (bench / "bench_a.py").write_text(
        "import os\nimport json\nfrom . import common\nimport common\nimport pytest\nimport fc_not_a_module_x\nfrom sklearn import svm\nimport mypkg.sub\n"
    )
    (tmp_path / "mypkg").mkdir()
    config = {"matrix": {"req": {"pip+fc_declared_dist_y": "", "pytest": "", "fc_skipped_z": None}, "@env": {"A": ["1"]}}}
    missing = m.missing_benchmark_deps(config, bench, tmp_path)
    assert "fc_not_a_module_x" in missing and "fc_declared_dist_y" in missing
    assert not {"os", "json", "common", "pytest", "mypkg", "fc_skipped_z", "@env"} & set(missing)
    assert ("scikit-learn" in missing) == (importlib.util.find_spec("sklearn") is None)


def test_constraints_pin_every_installed_distribution():
    import importlib.metadata as md

    spec = importlib.util.spec_from_file_location("fc_lsv_init_deps2", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    pins = m.env_constraints()
    assert f"pytest=={md.version('pytest')}" in pins
    assert len({p.split("==")[0].lower() for p in pins}) >= len({d.metadata["Name"].lower() for d in md.distributions()}) - 1
