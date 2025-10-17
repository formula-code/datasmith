from typing import Any

import dspy

# TODO: move this to new file [DONE]
# TODO: classification and make it structured (multiple outputs) --> add reasoning in the steps (take the solution as context) [DONE]
# TODO: generate summary for pd dataframe  --> with results
# TODO: collection.py for dataframe
# TODO: change build_report (so that you can on and off features)--> summarization (build tags)

MAX_TOKEN = 30000


# Summarization for git issues
class StructureSignature(dspy.Signature):
    """
    You are a senior open-source maintainer. Given raw GitHub issue text,
    your task is to *structure* it into a clear and coherent problem statement
    without summarizing, omitting, or rephrasing the core details.

    Your goal:
    - Preserve all technical information and specific phrasing from the input.
    - Reorder and lightly edit sentences only for clarity and logical flow.
    - Add section headers where useful (e.g., “Problem”,
      “Related Issues”, "Proposed Fix for Issues", “Acceptance Criteria”, “Open Questions”).
    - You will be provided with the list of issues that are related to the problem.
    - Do NOT paraphrase, shorten, or add interpretations.
    - Maintain the original tone and wording as much as possible.

    The result should read like a structured engineering problem statement,
    not a summary or rewrite.
    """

    github_text: str = dspy.InputField(desc="A single git issue message string.")
    related_issues: str = dspy.InputField(desc="Github descriptions of related issues and their references.")
    structured_issue: str = dspy.OutputField(desc="Plain-text summary of the message.")


class LLMStructurer(dspy.Module):
    def __init__(self) -> None:
        super().__init__()
        self.predict = dspy.Predict(StructureSignature)

    def forward(self, message: str, related_issues: str) -> object | Any:
        out = self.predict(github_text=message, related_issues=related_issues)
        return out


# Summarization for git comments
class CommentSummarizeSignature(dspy.Signature):
    """
    You are a senior engineer. Read the following GitHub comment thread and produce a plain-text that omits all identifying details (no names, usernames, dates, times, PR/issue numbers, orgs, or links).

    Goal: distill the discussion into hints for solving the technical problem.

    Be concise and just produce a plain text bosy, no headings. Do not write any code.
    Do not include any references or identifying information about the commenters in the description or comment.
    """

    github_text: str = dspy.InputField(desc="A single commit message string.")
    summary: str = dspy.OutputField(desc="Plain-text summary of the message.")


class LLMCommentSummarizer(dspy.Module):
    def __init__(self) -> None:
        super().__init__()
        self.predict = dspy.Predict(CommentSummarizeSignature)

    def forward(self, message: str) -> object | Any:
        out = self.predict(github_text=message)
        return out


# Summarization for git comments
class ClassifySignature(dspy.Signature):
    """
    You must classify every commit into **exactly one** optimization category from the list below, and use **Uncategorized only if absolutely no category fits**., and rating difficulty (**easy | medium | hard**). Prefer **high recall** for performance-related categories; avoid test/bench/CI-only changes.
    If a commit has any plausible optimization intent (even if partially), choose the **closest fitting category** rather than Uncategorized.


    ### Categories (choose exactly one)
    1. Use a better algorithm
    2. Use a better data structure (and layout)
    3. Use a lower-level system
    4. Accept a less-precise solution (approximation/heuristics)
    5. Use parallelization
    6. Remove or reduce work (requirements & UX)
    7. Cache & reuse
    8. Do it earlier / batch it / throttle it
    9. Scale the platform
    10. Database & storage tuning
    11. Micro-optimizations (hot path tweaks)
    12. I/O and latency hiding (async, overlap I/O/compute)
    13. Use a higher-level system that optimizes for you
    14. Uncategorized

    ### Hints
    - Import/startup speedups -> 11 (micro), unless it uses async/IO overlap -> 12.
    - Memoization/LRU/materialized views -> 7.
    - Complexity drop (O(n^2)->O(n log n)) -> 1.
    - Sets/maps/tries/indices/locality -> 2.
    - Threads/processes/async concurrency -> 5 (or 12 if primarily I/O overlap).
    - C/Rust/NumPy/SIMD/flags -> 3.
    - Heuristics/approx/probabilistic -> 4.
    - Batching/precompute/throttle -> 8.
    - Bigger/faster hardware/GPU/edge -> 9.
    - SQL indexes/queries/partitioning/N+1 -> 10.
    - Vectorization/inlining/loop tweaks/fewer allocations -> 11.
    - Swap to system that auto-optimizes (columnar db, solver) -> 13.
    - If test/ASV/CI-only or housekeeping or other, return Uncategorized.

    ### Difficulty guidance
    - easy: small localized changes (inlining, simple cache, import guard).
    - medium: moderate refactors (data structure changes, query/index tuning, batching, cache invalidation).
    - hard: algorithmic rewrites, parallel architecture, language/runtime migrations, correctness-risky approximations.

    ### Output (strict JSON)
    {
    "category": "<exact name above">,
    "reason": "one concise sentence citing the strongest evidence",
    "difficulty": "easy | medium | hard",
    "confidence": 0-100
    }
    Do not include any other text.
    """

    problem_description: str = dspy.InputField(desc="problem description")
    github_patch: str = dspy.InputField(desc="Associated git patch.")
    category: str = dspy.OutputField(desc="")
    reason: str = dspy.OutputField(desc="one concise sentence citing the strongest evidence")
    difficulty: str = dspy.OutputField(desc="easy | medium | hard")
    confidence: int = dspy.OutputField(desc="0-100")


"""
Classification Judge which classifies a given task into broad optimization categories and also gives a classification for the difficulty level.
Inputs:
 - problem_description: str (problem description of the task)
 - github_patch: str (associated github patch with the problem)
"""


class ClassifyJudge(dspy.Module):
    def __init__(self) -> None:
        super().__init__()
        self.predict = dspy.Predict(ClassifySignature)

    def forward(self, message: str, patch: str) -> object | Any:
        patch = patch[:MAX_TOKEN]
        out = self.predict(problem_description=message, github_patch=patch)
        return out
