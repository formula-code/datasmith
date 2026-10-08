import subprocess
from pathlib import Path

SETUP = Path(__file__).resolve().parents[2] / "src/datasmith/harbor_adapter/template/tests/prepare.sh"


def block(root: Path) -> str:
    text = SETUP.read_text()
    start, end = text.index('SETUP_PHASE="base_copy"'), text.index('SETUP_PHASE="lsv_init"')
    return text[start:end].replace("/workspace", str(root))


def test_base_copy_makes_two_equal_trees_without_moving_the_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, text=True).stdout
    git("init", "-q")
    (repo / "a.py").write_text("x = 1\n")
    git("add", "-A")
    git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base")
    head = git("rev-parse", "HEAD")
    subprocess.run(["bash", "-c", "set -euo pipefail\n" + block(tmp_path)], check=True, capture_output=True)
    for tree in (repo, tmp_path / ".fc_base"):
        assert (tree / "a.py").read_text() == "x = 1\n"
        assert subprocess.run(["git", "-C", str(tree), "rev-parse", "HEAD"], capture_output=True, text=True).stdout == head
    assert not (tmp_path / ".fc_new").exists()
    assert " mv /workspace/repo " not in SETUP.read_text()
