import subprocess
import time
from pathlib import Path

SETUP = Path(__file__).resolve().parents[2] / "src/datasmith/harbor_adapter/template/tests/setup.sh"


def baseline_block(sha_file: Path) -> str:
    text = SETUP.read_text()
    start, end = text.index('SETUP_PHASE="baseline_commit"'), text.index('SETUP_PHASE="base_copy"')
    return text[start:end].replace("/opt/fc_baseline_sha", str(sha_file))


def test_baseline_commit_starts_no_background_gc(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, text=True).stdout
    git("init", "-q")
    for i in range(2000):
        (repo / f"f{i}.txt").write_text(f"{i}\n")
    git("add", "-A")
    git("-c", "user.name=t", "-c", "user.email=t@t", "-c", "gc.auto=0", "commit", "-q", "-m", "base")
    git("config", "gc.auto", "1")
    (repo / "edit.txt").write_text("image edit\n")
    subprocess.run(["bash", "-c", "set -euo pipefail\n" + baseline_block(tmp_path / "sha")], cwd=repo, check=True,
                   capture_output=True)
    time.sleep(2)
    assert git("log", "-1", "--format=%s").strip() == "fc-baseline"
    assert not list((repo / ".git/objects/pack").glob("*.pack"))
    assert int(git("count-objects").split()[0]) > 2000
