import json

import pytest

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
            "o__r__2": {"version": 1, "approved": "None", "relevant_issues": None},
            "o__r__3": {"version": 1, "approved": "2026-10-08", "relevant_issues": None},
        })
    )
    for t in ("o__r__2", "o__r__3"):
        (root / t).mkdir()
        (root / t / "instruction.md").write_text(INSTR)
    (pdir / "o__r__2.md").write_text("draft")
    (pdir / "o__r__1.md").write_text("new text")
    assert prompts.apply(root, pdir) == ["o__r__1"]
    assert "new text" in (root / "o__r__1" / "instruction.md").read_text()
    assert json.loads((root / "o__r__1" / "PROMPT_VERSION.json").read_text())["version"] == 2
    assert list((tmp_path / "prompt-backups" / "o__r__1").iterdir())


def test_rewrite_fills_an_empty_problem_text():
    text = "To understand the problem:\n\n## Task Requirements\n- x\n"
    assert (
        prompts.rewrite(text, "slow", "")
        == "To understand the problem:\n\n**Initial Observations**\n\nslow\n\n## Task Requirements\n- x\n"
    )


def test_relevant_issues_must_name_a_file(tmp_path):
    root, pdir = tmp_path / "tasks", tmp_path / "prompts"
    (root / "o__r__1").mkdir(parents=True)
    (root / "o__r__1" / "instruction.md").write_text(INSTR)
    pdir.mkdir()
    (pdir / "versions.json").write_text(
        json.dumps({"o__r__1": {"approved": "2026-10-08", "relevant_issues": "# Issue\ntext"}})
    )
    (pdir / "o__r__1.md").write_text("new text")
    with pytest.raises(ValueError, match="relevant_issues"):
        prompts.apply(root, pdir)
