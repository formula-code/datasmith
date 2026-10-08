import subprocess
import time
from pathlib import Path

from datasmith.harbor_adapter.utils import render_template


def git(repo: Path, *a: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, text=True).stdout


def bash(script: str, cwd: Path) -> None:
    subprocess.run(["bash", "-c", "set -euo pipefail\n" + script], cwd=cwd, check=True, capture_output=True)


def prepare_block(start: str, end: str, sha_file: Path, base: str) -> str:
    text = render_template(
        "tests/prepare.sh", base_commit=base, rounds=None, task_id="t", owner="o", repo="r", issue_number=1
    )
    block = text[text.index(f'SETUP_PHASE="{start}"') : text.index(f'SETUP_PHASE="{end}"')]
    return block.replace("/opt/fc_baseline_sha", str(sha_file)).replace("/logs/artifacts", str(sha_file.parent))


def collect_script(base: str, out: Path) -> str:
    # kill -9 -1 would stop every process of the test user.
    text = render_template("shared/collect.sh", base_commit=base).replace("kill -9 -1", "true")
    return text.replace("/workspace/repo", ".").replace("/fc_submission", str(out))


def files(repo: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(repo)): p.read_bytes()
        for p in sorted(repo.rglob("*"))
        if p.is_file() and ".git" not in p.relative_to(repo).parts
    }


def image_repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "image"
    repo.mkdir()
    git(repo, "init", "-q")
    (repo / "pkg.py").write_text("x = 1\n")
    (repo / ".gitignore").write_text("*.so\n")
    git(repo, "add", "-A")
    git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base")
    (repo / "pkg.py").write_text("x = 1  # image edit\n")
    (repo / "benchmarks").mkdir()
    (repo / "benchmarks/bench.py").write_text("def time_x(): pass\n")
    (repo / "ext.so").write_bytes(b"\x7fELF build")
    nested = repo / "suite-benchmarks"
    nested.mkdir()
    git(nested, "init", "-q")
    (nested / "b.py").write_text("def time_y(): pass\n")
    return repo, git(repo, "rev-parse", "HEAD").strip()


def test_baseline_commit_starts_no_background_gc(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    for i in range(2000):
        (repo / f"f{i}.txt").write_text(f"{i}\n")
    git(repo, "add", "-A")
    git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "-c", "gc.auto=0", "commit", "-q", "-m", "base")
    git(repo, "config", "gc.auto", "1")
    (repo / "edit.txt").write_text("image edit\n")
    base = git(repo, "rev-parse", "HEAD").strip()
    bash(prepare_block("baseline_commit", "base_copy", tmp_path / "sha", base), repo)
    time.sleep(2)
    assert git(repo, "log", "-1", "--format=%s").strip() == "fc-baseline"
    assert not list((repo / ".git/objects/pack").glob("*.pack"))
    assert int(git(repo, "count-objects").split()[0]) > 2000


def test_verifier_recreates_agent_tree_and_patch_excludes_image_edits(tmp_path):
    image, base = image_repo(tmp_path)
    agent, verifier = tmp_path / "agent", tmp_path / "verifier"
    subprocess.run(["cp", "-a", str(image), str(agent)], check=True)
    subprocess.run(["cp", "-a", str(image), str(verifier)], check=True)

    (agent / "pkg.py").write_text("x = 2  # agent\n")
    (agent / "new.py").write_text("y = 3\n")
    (agent / "suite-benchmarks/b.py").write_text("def time_y(): return 1\n")
    git(agent, "-c", "user.name=a", "-c", "user.email=a@a", "commit", "-qam", "agent commit hides nothing")
    sub = tmp_path / "submission"
    bash(collect_script(base, sub), agent)

    sha = verifier / "sha"
    bash(prepare_block("baseline_commit", "base_copy", sha, base), verifier)
    tree_block = prepare_block("agent_tree", "complete", sha, base).replace("/fc_submission", str(sub))
    bash(tree_block, verifier)

    assert {k: v for k, v in files(verifier).items() if k != "sha"} == files(agent)
    changed = set(git(verifier, "diff", "--name-only", sha.read_text().strip()).split())
    assert changed == {"pkg.py", "suite-benchmarks/b.py"}
    assert "new.py" in git(verifier, "status", "--porcelain")


def test_broken_tree_diff_leaves_starting_tree(tmp_path):
    image, base = image_repo(tmp_path)
    sub = tmp_path / "submission"
    sub.mkdir()
    (sub / "tree.diff").write_text("diff --git a/nope b/nope\n--- a/nope\n+++ b/nope\n@@ -1 +1 @@\n-a\n+b\n")
    sha = image / "sha"
    bash(prepare_block("baseline_commit", "base_copy", sha, base), image)
    before = {k: v for k, v in files(image).items() if k != "sha"}
    bash(prepare_block("agent_tree", "complete", sha, base).replace("/fc_submission", str(sub)), image)
    assert {k: v for k, v in files(image).items() if k not in ("sha", "agent_tree.txt")} == before
    assert (image / "agent_tree.txt").read_text().strip() == "tree_diff_failed"


def _collect_and_recreate(tmp_path: Path, edit) -> tuple[Path, Path]:
    image, base = image_repo(tmp_path)
    agent, verifier = tmp_path / "agent", tmp_path / "verifier"
    subprocess.run(["cp", "-a", str(image), str(agent)], check=True)
    subprocess.run(["cp", "-a", str(image), str(verifier)], check=True)
    edit(agent)
    sub = tmp_path / "submission"
    bash(collect_script(base, sub), agent)
    sha = verifier / "sha"
    bash(prepare_block("baseline_commit", "base_copy", sha, base), verifier)
    bash(prepare_block("agent_tree", "complete", sha, base).replace("/fc_submission", str(sub)), verifier)
    return verifier, sha


def test_gitattributes_cannot_hide_a_benchmark_edit_from_the_verifier_diff(tmp_path):
    def edit(agent: Path) -> None:
        (agent / ".gitattributes").write_text("benchmarks/** -diff\n")
        (agent / "benchmarks/bench.py").write_text("def time_x(): return 0\n")

    verifier, sha = _collect_and_recreate(tmp_path, edit)
    test_sh = render_template(
        "tests/test.sh",
        base_commit="x",
        run_pytest=False,
        rounds=None,
        task_id="t",
        owner="o",
        repo="r",
        issue_number=1,
    )
    fc_diff = next(line for line in test_sh.splitlines() if line.startswith("fc_diff()"))
    setup = "\n".join(
        line for line in test_sh.splitlines() if line.startswith(("FC_INDEX=", "cp .git/index", "GIT_INDEX_FILE="))
    )
    script = f"FC_BASE={sha.read_text().strip()}\n{setup}\n{fc_diff}\nfc_diff"
    out = subprocess.run(["bash", "-c", script], cwd=verifier, check=True, capture_output=True, text=True).stdout
    assert "+def time_x(): return 0" in out


def test_diff_with_pth_file_or_symlink_is_refused(tmp_path):
    def edit(agent: Path) -> None:
        (agent / "pkg.py").write_text("x = 2\n")
        (agent / "evil.pth").write_text("import os\n")
        (agent / "link").symlink_to("/etc/passwd")

    verifier, _ = _collect_and_recreate(tmp_path, edit)
    assert (verifier / "agent_tree.txt").read_text().strip() == "tree_diff_refused"
    assert not (verifier / "evil.pth").exists() and not (verifier / "link").is_symlink()
    assert (verifier / "pkg.py").read_text() == "x = 1  # image edit\n"
