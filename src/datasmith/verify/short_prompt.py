"""Short (symptom-only) task prompts: draft them with a model, score how much a prompt gives away, and write drafts
for review. Drafts are applied with `fc-verify prompt` only after a reviewer sets "approved" in versions.json.

Giveaway score: 0 = only the symptom (what is slow, rough data size, rough time); 1 = names the slow public function or
entry point; 2 = names the exact slow code path, internal function or input case; 3 = names the fix or technique,
quotes code of the fix, or includes issue or PR text that states the fix. A short prompt is written as the developer
would describe the problem before any fix existed, names no ASV benchmark and gives no timings of named benchmarks.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from datasmith.verify.prompt_check import check, task_text
from datasmith.verify.prompts import START

TARGET_SCORE = 1
RUBRIC = __doc__.split("Giveaway score: ", 1)[1].split("Drafts", 1)[0]


def lm(model: str, api_base: str | None, api_key: str | None) -> Any:
    import dspy

    return dspy.LM(model=model, api_base=api_base, api_key=api_key or "unused", temperature=0.0, max_tokens=4000)


def _signatures() -> tuple[Any, Any]:
    import dspy

    class Draft(dspy.Signature):
        __doc__ = (
            "Rewrite the problem description of a performance task so it states only the symptom, as the developer "
            "would describe it before any fix existed. Keep what is slow, the rough data size and rough time. "
            "Do not name ASV benchmarks, timings of named benchmarks, internal functions, the slow code path or the "
            "fix. Do not include links, code or issue text. Plain English, a few sentences. " + RUBRIC
        )

        problem: str = dspy.InputField(desc="The current problem description")
        patch: str = dspy.InputField(desc="The human fix, so the text can avoid naming it")
        short_problem: str = dspy.OutputField(desc="Symptom-only problem description, giveaway score 0 or 1")

    class Score(dspy.Signature):
        __doc__ = "Score how much this task prompt gives away the fix in the patch. " + RUBRIC

        problem: str = dspy.InputField()
        patch: str = dspy.InputField()
        score: int = dspy.OutputField(desc="0, 1, 2 or 3")
        phrase: str = dspy.OutputField(desc="The words that give the most away, or empty")

    return Draft, Score


def score(problem: str, patch: str, model: Any) -> dict:
    import dspy

    _, sig = _signatures()
    with dspy.context(lm=model):
        r = dspy.Predict(sig)(problem=problem, patch=patch[:20000])
    return {"score": int(r.score), "phrase": r.phrase}


def draft(problem: str, patch: str, model: Any) -> str:
    import dspy

    sig, _ = _signatures()
    with dspy.context(lm=model):
        return dspy.Predict(sig)(problem=problem, patch=patch[:20000]).short_problem.strip()


def _patch(task_dir: Path) -> str:
    solve = task_dir / "solution" / "solve.sh"
    m = re.search(r"<< '__SOLUTION__'\n(.*?)\n__SOLUTION__", solve.read_text(), re.S) if solve.exists() else None
    return m.group(1) if m else ""


def draft_tasks(tasks_root: Path, out: Path, model: Any, tasks: list[str], tries: int = 2) -> dict[str, dict]:
    """Draft a short prompt per task into OUT (versions.json + <task>.md, not approved); keep the best of `tries` drafts."""
    out.mkdir(parents=True, exist_ok=True)
    versions_path = out / "versions.json"
    versions = json.loads(versions_path.read_text()) if versions_path.exists() else {}
    for task in tasks:
        d = tasks_root / task
        instruction = (d / "instruction.md").read_text()
        problem, patch = task_text(instruction), _patch(d)
        best = None
        for _ in range(tries):
            text = draft(problem, patch, model)
            new = instruction.replace(problem, text + "\n") if START in instruction else text
            found = check(new, patch)
            s = score(text, patch, model)
            cand = {"text": text, "findings": found, **s}
            if best is None or (s["score"], len(found)) < (best["score"], len(best["findings"])):
                best = cand
            if s["score"] <= TARGET_SCORE and not found:
                break
        (out / f"{task}.md").write_text(best["text"] + "\n")
        versions[task] = {
            "version": versions.get(task, {}).get("version", 0) + 1,
            "approved": None,
            "relevant_issues": "",
            "score": best["score"],
            "phrase": best["phrase"],
            "findings": best["findings"],
        }
        versions_path.write_text(json.dumps(versions, indent=1, sort_keys=True))
    return versions
