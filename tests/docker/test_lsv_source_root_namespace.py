"""detect_source_root finds a namespace package named by the repo (geocat-comp: geocat/comp/, no geocat/__init__.py)."""

import importlib.util
import subprocess
from pathlib import Path

_LSV_INIT = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "lsv_init.py"


def test_namespace_package_from_the_repo_name(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("fc_lsv_init_namespace", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    repo = tmp_path / "repo"
    (repo / "geocat" / "comp").mkdir(parents=True)
    (repo / "geocat" / "comp" / "__init__.py").write_text("")
    (repo / "test").mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", "https://github.com/NCAR/geocat-comp.git"], check=True)
    monkeypatch.setattr(m, "REPO_ROOT", repo)
    monkeypatch.setattr(m, "source_root_from_install", lambda root: None)
    assert m.detect_source_root() == repo / "geocat" / "comp"
