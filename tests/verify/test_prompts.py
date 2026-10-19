import json

from datasmith.verify import prompts

INSTR = (
    "# Task\n\nUse the following information if needed to understand the problem:\n\n**Initial Observations**\n\nold text\n"
    "\n**Relevant Issues**\n\nold issue\n\n## Task Requirements\n- x\n"
)


def test_rewrite_keeps_or_replaces_issues():
    kept = prompts.rewrite(INSTR, "new text", None)
    assert (
        "new text" in kept
        and "old text" not in kept
        and "old issue" in kept
        and kept.endswith("## Task Requirements\n- x\n")
    )
    replaced = prompts.rewrite(INSTR, "new text", "new issue")
    assert "new issue" in replaced and "old issue" not in replaced


def test_apply_writes_version_and_backup(tmp_path):
    root, pdir = tmp_path / "tasks", tmp_path / "prompts"
    (root / "o__r__1").mkdir(parents=True)
    (root / "o__r__1" / "instruction.md").write_text(INSTR)
    pdir.mkdir()
    (pdir / "versions.json").write_text(
        json.dumps({
            "o__r__1": {"version": 2, "approved": "2026-10-08", "relevant_issues": None},
            "o__r__9": {"version": 1, "approved": "x", "relevant_issues": None},
        })
    )
    (pdir / "o__r__1.md").write_text("new text")
    assert prompts.apply(root, pdir) == ["o__r__1"]
    assert "new text" in (root / "o__r__1" / "instruction.md").read_text()
    assert json.loads((root / "o__r__1" / "PROMPT_VERSION.json").read_text())["version"] == 2
    assert list((tmp_path / "prompt-backups" / "o__r__1").iterdir())
