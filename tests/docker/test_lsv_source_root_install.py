"""detect_source_root uses the editable install's package folder when the repo name finds nothing (nanoarrow)."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

_LSV_INIT = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "lsv_init.py"


def test_editable_install_in_a_nested_folder(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("fc_lsv_init_install", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    repo, site = tmp_path / "repo", tmp_path / "site"
    (repo / "python" / "src" / "nanoarrow").mkdir(parents=True)
    (repo / "python" / "src" / "nanoarrow" / "__init__.py").write_text("")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", "https://github.com/apache/arrow-nanoarrow.git"], check=True)
    info = site / "nanoarrow-0.1.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text("Metadata-Version: 2.1\nName: nanoarrow\nVersion: 0.1\n")
    (info / "top_level.txt").write_text("nanoarrow\n")
    (info / "direct_url.json").write_text(json.dumps({"url": f"file://{repo}", "dir_info": {"editable": True}}))
    monkeypatch.syspath_prepend(str(repo / "python" / "src"))
    monkeypatch.syspath_prepend(str(site))
    monkeypatch.setattr(m, "REPO_ROOT", repo)
    sys.modules.pop("nanoarrow", None)
    assert m.detect_source_root() == repo / "python" / "src" / "nanoarrow"
