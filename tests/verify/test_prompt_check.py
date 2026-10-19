import json

from datasmith.verify import prompt_check, prompts, short_prompt

HEAD = "# Task\n\nUse the following information if needed to understand the problem:\n\n"
TAIL = "\n## Task Requirements\n- x\n"
PATCH = "--- a/pkg/m.py\n+++ b/pkg/m.py\n@@ -1,3 +1,3 @@ def _fast_path(x):\n-    return slow(x)\n+    return fast(x)\n+def public_api(y):\n"


def prompt(text):
    return HEAD + text + TAIL


def test_symptom_only_passes():
    assert prompt_check.check(prompt("Calling public_api on large arrays takes several seconds."), PATCH) == []


def test_each_kind_of_giveaway_is_found():
    cases = {
        "names benchmarks": "The ASV benchmark time_mean is slow.",
        "link": "See https://github.com/o/r/pull/12 for details.",
        "code or diff": "Try this:\n```python\nfast(x)\n```",
        "changed internal code": "The loop in _fast_path is slow.",
        "relevant issues": "Slow.\n\n**Relevant Issues**\n\nissue text",
    }
    for kind, text in cases.items():
        found = prompt_check.check(prompt(text), PATCH)
        assert any(f.startswith(kind) for f in found), (kind, found)


def test_drafts_are_scored_and_kept_unapproved(tmp_path, monkeypatch):
    d = tmp_path / "tasks" / "o__r__1"
    (d / "solution").mkdir(parents=True)
    (d / "instruction.md").write_text(prompt("The loop in _fast_path is slow; see #123."))
    (d / "solution" / "solve.sh").write_text(f"cat > p << '__SOLUTION__'\n{PATCH}\n__SOLUTION__\n")
    texts = iter(["The loop in _fast_path is slow.", "Calling public_api on large inputs is slow."])
    monkeypatch.setattr(short_prompt, "draft", lambda problem, patch, model: next(texts))
    monkeypatch.setattr(
        short_prompt, "score", lambda text, patch, model: {"score": 2 if "_fast" in text else 1, "phrase": ""}
    )
    out = tmp_path / "drafts"
    v = short_prompt.draft_tasks(tmp_path / "tasks", out, model=None, tasks=["o__r__1"])
    assert v["o__r__1"]["approved"] is None and v["o__r__1"]["score"] == 1
    assert (out / "o__r__1.md").read_text().startswith("Calling public_api")
    assert prompts.apply(tmp_path / "tasks", out) == []
    assert prompts.apply(tmp_path / "tasks", out, drafts=True) == ["o__r__1"]
    text = (d / "instruction.md").read_text()
    assert (
        "public_api" in text
        and "#123" not in text
        and json.loads((d / "PROMPT_VERSION.json").read_text())["version"] == 1
    )


def test_ipython_display_hooks_are_public():
    patch = "--- a/m.py\n+++ b/m.py\n@@ -1 +1 @@ def _repr_html_(self):\n-a\n+b\n"
    assert not [f for f in prompt_check.check("`arr._repr_html_()` is slow.", patch) if "internal" in f]
