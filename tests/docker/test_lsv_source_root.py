"""detect_source_root at image build (no /tests/config.json) finds a src-layout package whose name differs from the repo."""

import importlib.util
import subprocess
from pathlib import Path

_LSV_INIT = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "lsv_init.py"


def test_src_layout_package_with_another_name(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("fc_lsv_init_root", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    for d in ("src/skimage", "benchmarks", "tools", "doc"):
        (tmp_path / d).mkdir(parents=True)
    (tmp_path / "src/skimage/__init__.py").write_text("")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "remote", "add", "origin", "https://github.com/scikit-image/scikit-image.git"], check=True)
    monkeypatch.setattr(m, "REPO_ROOT", tmp_path)
    assert m.detect_source_root() == tmp_path / "src" / "skimage"
