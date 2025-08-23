import json
import re

import dspy

SYSTEM_PROMPT = """
You are a classifier that decides whether a Git commit message describes a **performance enhancement to the product/runtime** (not merely tests/benchmarks/CI).

### Count as performance (label **YES**)
- Runtime speedups (e.g., faster loops, vectorization, inlining, caching, avoiding imports, lazy init, non-blocking/async for throughput, lower latency).
- Startup/import time reductions, memory reductions, fewer allocations, less I/O, fewer syscalls.
- Fixing a **speed regression** or a change whose *intent* is “speed up”.
- Behavior changes **explicitly** framed as speeding things up (e.g., “non-blocking requests (speed-up …)”).

### Do **NOT** count (label **NO**) unless the message clearly states product runtime gets faster:
- Test/bench/ASV/perf-test changes; thresholds; CI; coverage; Makefile/tox/pre-commit; refactors “for tests”.
- Merges, version bumps, housekeeping (“tidy”), or ambiguous “attempt to fix perf tests”.
- Pure UX frequency changes with “no measurable reduction in speed”.

### Tie-breaker (recall-first)
If ambiguous but plausibly about product/runtime performance, prefer **YES**. Only choose **NO** when it clearly applies solely to tests/infra or non-runtime concerns.

### Output
Return strict JSON with:
{
  "label": "YES" | "NO",
  "reason": "one short sentence",
  "confidence": 0-100,
  "flags": ["tests-only" | "mentions-speed" | "startup" | "memory" | "non-blocking" | "regression" | "ambiguous" | "merge-or-version" | "infra"]
}
Do not include analysis steps—only the JSON object.
"""

POSITIVE_RE = re.compile(
    r"(perf\b|performance|speed(?:\s*up|(?:\s*-\s*)?up)?|faster|fasterer|"
    r"reduce\s+(latency|allocations?|memory|overhead)|latency|throughput|"
    r"optimi[sz]e(?:d|s|r)?|optimization|micro[-\s]?opt|vectori[sz]e|"
    r"inlin(?:e|ing)|cache|cached|caching|memoiz|non[-\s]?blocking|async|"
    r"import\s*time|startup|boot\s*time|hot\s*path|tighten\s*loop|"
    r"regression.*(speed|perf)|\[perf\]|^perf:)",
    re.I,
)

NEGATIVE_STRONG_RE = re.compile(
    r"(^merge\b|^bump\b|version\b|release\b|chore\b|tidy\b|housekeep|"
    r"^revert\b(?!.*regression)|"
    r"^tests?:|tests?\b|bench(mark)?\b|asv\b|capsys\b|thresholds?\b|"
    r"coverage\b|tox\b|pre-commit\b|makefile\b|ci\b|\bflake8\b|\blint)",
    re.I,
)

TESTS_ONLY_HINT = re.compile(
    r"(tests?:|perf[-\s]?tests?|asv|benchmark|capsys|threshold|coverage|tox|pre-commit|pytest|unittest)", re.I
)


def heuristic_prior(msg: str) -> tuple[bool | None, int, list[str]]:
    m = msg.strip()
    pos = bool(POSITIVE_RE.search(m))
    # neg = bool(NEGATIVE_STRONG_RE.search(m))
    tests_only = bool(TESTS_ONLY_HINT.search(m)) and not re.search(r"(runtime|import|startup|prod(uction)?)", m, re.I)

    flags = []
    flag_checks = [
        (r"non[-\s]?blocking|async", "non-blocking"),
        (r"import\s*time|startup|boot\s*time", "startup"),
        (r"memory|alloc", "memory"),
        (r"regression", "regression"),
        (r"speed|faster|perf|optimi[sz]", "mentions-speed"),
        (r"merge|bump|version|release", "merge-or-version"),
    ]
    for pattern, flag in flag_checks:
        if re.search(pattern, m, re.I):
            flags.append(flag)
    if tests_only:
        flags.append("tests-only")

    # Recall-first decision:
    if not pos and tests_only:
        return False, 65, flags
    if pos:
        if tests_only:
            return True, 55, [*flags, "ambiguous"]
        return True, 80, flags
    return None, 50, flags


class JudgeSignature(dspy.Signature):
    """You are a classifier that decides whether a Git commit message describes a **performance enhancement to the product/runtime** (not merely tests/benchmarks/CI).
    ### Count as performance (label **YES**)
    - Runtime speedups (e.g., faster loops, vectorization, inlining, caching, avoiding imports, lazy init, non-blocking/async for throughput, lower latency).
    - Startup/import time reductions, memory reductions, fewer allocations, less I/O, fewer syscalls.
    - Fixing a **speed regression** or a change whose *intent* is “speed up”.
    - Behavior changes **explicitly** framed as speeding things up (e.g., “non-blocking requests (speed-up …)”).

    ### Do **NOT** count (label **NO**) unless the message clearly states product runtime gets faster:
    - Test/bench/ASV/perf-test changes; thresholds; CI; coverage; Makefile/tox/pre-commit; refactors “for tests”.
    - Merges, version bumps, housekeeping (“tidy”), or ambiguous “attempt to fix perf tests”.
    - Pure UX frequency changes with “no measurable reduction in speed”.

    ### Tie-breaker (recall-first)
    If ambiguous but plausibly about product/runtime performance, prefer **YES**. Only choose **NO** when it clearly applies solely to tests/infra or non-runtime concerns.

    ### Output
    Return strict JSON with:
    {
        "label": "YES" | "NO",
        "reason": "one short sentence",
        "confidence": 0-100,
        "flags": ["tests-only" | "mentions-speed" | "startup" | "memory" | "non-blocking" | "regression" | "ambiguous" | "merge-or-version" | "infra"]
    }
    Do not include analysis steps—only the JSON object.
    """

    message = dspy.InputField(desc="A single commit message string.")
    debug_json = dspy.OutputField(
        desc="JSON dump of the model's internal state, useful for debugging.",
        default=None,
    )


class LLMJudge(dspy.Module):
    def __init__(self) -> None:
        super().__init__()
        self.predict = dspy.Predict(JudgeSignature)

    def forward(self, message: str) -> dspy.Prediction:
        prediction = self.predict(message=message)
        out: str = prediction.get("debug_json", None)  # pyright: ignore[reportAttributeAccessIssue]
        try:
            data = json.loads(out)
        except Exception:
            data = {"label": "YES", "reason": "Permissive fallback", "confidence": 40, "flags": ["ambiguous"]}

        data["label"] = "YES" if str(data.get("label", "YES")).upper().startswith("Y") else "NO"
        try:
            data["confidence"] = max(0, min(100, int(data.get("confidence", 50))))
        except Exception:
            data["confidence"] = 50
        data["flags"] = list(dict.fromkeys(map(str, data.get("flags", []))))  # dedupe
        return dspy.Prediction(json=json.dumps(data))


class PerfClassifier(dspy.Module):
    """
    Pipeline:
      1) Heuristic prior (recall-first)
      2) LLM judge (JSON)
      3) Combine: favor YES unless strong evidence of tests-only/infra-only
    """

    def __init__(self) -> None:
        super().__init__()
        self.judge = LLMJudge()

    def forward(self, message: str) -> dspy.Prediction:
        prior_label, prior_conf, prior_flags = heuristic_prior(message)
        if prior_label is True and prior_conf >= 55:
            result = {
                "label": "YES",
                "reason": "Positive performance cues in message.",
                "confidence": prior_conf,
                "flags": prior_flags,
            }
            return dspy.Prediction(json=json.dumps(result))

        # Ask LLM judge
        judged = json.loads(self.judge(message=message).json)  # pyright: ignore[reportAttributeAccessIssue]

        tests_only = "tests-only" in prior_flags or "tests-only" in judged.get("flags", [])
        if judged["label"] == "YES":
            return dspy.Prediction(json=json.dumps(judged))
        if prior_label is True and not tests_only:
            judged["label"] = "YES"
            judged["reason"] = "Recall-first override: positive perf hints."
            judged["confidence"] = max(judged["confidence"], 60)
            judged["flags"] = list(dict.fromkeys(judged.get("flags", []) + prior_flags + ["ambiguous"]))
            return dspy.Prediction(json=json.dumps(judged))

        # Otherwise respect NO (or explicit tests-only)
        if tests_only:
            judged["label"] = "NO"
            judged["reason"] = "Tests/bench/infra-only message."
            judged["confidence"] = max(judged["confidence"], prior_conf, 70)
            judged["flags"] = list(dict.fromkeys(judged.get("flags", []) + prior_flags + ["infra"]))
        return dspy.Prediction(json=json.dumps(judged))

    def get_response(self, message: str) -> tuple[bool, str]:
        """
        Get the label for a commit message.
        """
        json_str = self(message=message).json  # pyright: ignore[reportAttributeAccessIssue]
        response = json.loads(json_str)
        return (response["label"] == "YES", json_str)


# if __name__ == "__main__":
#     classifier = PerfClassifier()
#     examples = [
#         "Speed up tqdm.auto import when not in an IPython notebook",
#         ">5% speed increase on empty loops",
#         "fix speed regression by inlining",
#         "non-blocking requests (speed-up factor ~0.02s/it)",
#         "tests: fix asv",
#         "tests:perf:capsys upgrades",
#         "revert to N_BAR=10 as default, a slightly faster update interval looks better without measurable reduction in iteration speed",
#         "performance/optimisation and slight tidy",
#         "better ETA for wildly varying iteration speeds",
#     ]
#     for m in examples:
#         print(m)
#         print(json.loads(classifier(message=m)))
#         print()

# from dspy.teleprompt import BootstrapFewShot
# train = [
# dspy.Example(message="Speed up tqdm.auto import when not in an IPython notebook", json='{"label":"YES","reason":"Import-time speedup","confidence":90,"flags":["startup","mentions-speed"]}').with_inputs("message"),
# dspy.Example(message=">5% speed increase on empty loops", json='{"label":"YES","reason":"Explicit runtime speedup","confidence":95,"flags":["mentions-speed"]}').with_inputs("message"),
# dspy.Example(message="fix speed regression by inlining", json='{"label":"YES","reason":"Fixes speed regression","confidence":95,"flags":["regression","mentions-speed","inlining"]}').with_inputs("message"),
# dspy.Example(message="non-blocking requests (speed-up factor ~0.02s/it)", json='{"label":"YES","reason":"Throughput via non-blocking","confidence":85,"flags":["non-blocking","mentions-speed"]}').with_inputs("message"),
# dspy.Example(message="tests: fix asv", json='{"label":"NO","reason":"ASV/tests only","confidence":85,"flags":["tests-only","infra"]}').with_inputs("message"),
# dspy.Example(message="tests:perf:capsys upgrades", json='{"label":"NO","reason":"Perf tests infra","confidence":85,"flags":["tests-only","infra"]}').with_inputs("message"),
# dspy.Example(message="revert to N_BAR=10 as default, ... looks better without measurable reduction in iteration speed", json='{"label":"NO","reason":"UI freq, not faster runtime","confidence":80,"flags":["ambiguous"]}').with_inputs("message"),
# ]
# def recall_weighted_metric(golds, preds):
#     # penalize FN 3x more than FP
#     tp=fp=tn=fn=0
#     for g,p in zip(golds,preds):
#         g_y = json.loads(g.json)["label"] == "YES"
#         p_y = json.loads(p.json)["label"] == "YES"
#         if g_y and p_y: tp+=1
#         elif (not g_y) and p_y: fp+=1
#         elif (not g_y) and (not p_y): tn+=1
#         else: fn+=1
#     # higher is better
#     return (tp - 3*fn) - 0.5*fp
# tele = BootstrapFewShot(metric=recall_weighted_metric, max_bootstrapped_demos=6, max_labeled_demos=6)
# optimized = tele.compile(PerfClassifier(), trainset=train)  # returns an optimized program
