"""At image build, an untracked asv_runner/ in the repo is moved out (it hid the installed asv_runner); a tracked one stays."""

import importlib.util
import subprocess
from pathlib import Path

_LSV_INIT = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "lsv_init.py"


def _repo(tmp_path: Path, track: bool) -> Path:
    repo = tmp_path / ("tracked" if track else "untracked")
    (repo / "asv_runner").mkdir(parents=True)
    (repo / "asv_runner" / "__init__.py").write_text("from .statistics import get_err\n")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    if track:
        subprocess.run(["git", "-C", str(repo), "add", "asv_runner"], check=True)
    return repo


def test_moves_only_an_untracked_asv_runner(tmp_path):
    spec = importlib.util.spec_from_file_location("fc_lsv_init_shim", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    untracked, tracked = _repo(tmp_path, False), _repo(tmp_path, True)
    assert m.move_untracked_asv_runner(untracked, tmp_path / "shim")
    assert not (untracked / "asv_runner").exists() and (tmp_path / "shim" / "__init__.py").is_file()
    assert not m.move_untracked_asv_runner(tracked, tmp_path / "shim2")
    assert (tracked / "asv_runner" / "__init__.py").is_file()
