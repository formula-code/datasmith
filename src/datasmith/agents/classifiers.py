from __future__ import annotations

import enum
import os
from dataclasses import dataclass
from typing import Any

from datasmith.utils import get_logger

logger = get_logger("agents.classifiers")


class OptimizationType(str, enum.Enum):
    USE_BETTER_ALGORITHM = "use_better_algorithm"
    USE_BETTER_DATA_STRUCTURE_AND_LAYOUT = "use_better_data_structure_and_layout"
    USE_LOWER_LEVEL_SYSTEM = "use_lower_level_system"
    ACCEPT_LESS_PRECISE_SOLUTION = "accept_less_precise_solution"
    USE_PARALLELIZATION = "use_parallelization"
    REMOVE_OR_REDUCE_WORK = "remove_or_reduce_work"
    CACHE_AND_REUSE = "cache_and_reuse"
    DO_IT_EARLIER_BATCH_THROTTLE = "do_it_earlier_batch_throttle"
    SCALE_PLATFORM = "scale_platform"
    DATABASE_AND_STORAGE_TUNING = "database_and_storage_tuning"
    MICRO_OPTIMIZATIONS = "micro_optimizations"
    IO_AND_LATENCY_HIDING = "io_and_latency_hiding"
    USE_HIGHER_LEVEL_SYSTEM = "use_higher_level_system"
    UNCATEGORIZED = "uncategorized"

    @property
    def description(self) -> str:
        return _OPTIMIZATION_DESCRIPTIONS.get(self.value, "")


_OPTIMIZATION_DESCRIPTIONS: dict[str, str] = {
    "use_better_algorithm": (
        "Complexity reduction or switching to a faster algorithm "
        "(e.g. O(n^2) -> O(n log n), better sorting, smarter search)."
    ),
    "use_better_data_structure_and_layout": (
        "Switching to a more efficient data structure or improving memory layout "
        "(e.g. list -> set/dict for lookups, struct-of-arrays, contiguous buffers)."
    ),
    "use_lower_level_system": (
        "Offloading work to C/Cython/Rust/Fortran extensions, NumPy vectorized ops, "
        "or native SIMD intrinsics instead of pure Python."
    ),
    "accept_less_precise_solution": (
        "Trading accuracy for speed via approximations, heuristics, sampling, "
        "or reduced precision (e.g. float32 instead of float64)."
    ),
    "use_parallelization": (
        "Using threads, multiprocessing, GPU kernels, or parallel algorithms "
        "to split work across cores (not just async I/O)."
    ),
    "remove_or_reduce_work": (
        "Eliminating unnecessary computation, short-circuiting, early exits, "
        "skipping redundant steps, or simplifying requirements."
    ),
    "cache_and_reuse": (
        "Memoization, LRU caches, materialized views, precomputed lookup tables, "
        "or reusing expensive results across calls."
    ),
    "do_it_earlier_batch_throttle": (
        "Batching small operations, lazy evaluation, deferred computation, "
        "throttling, or moving work to an earlier/better time."
    ),
    "scale_platform": (
        "Horizontal/vertical scaling, load balancing, sharding, or infrastructure-level capacity changes."
    ),
    "database_and_storage_tuning": (
        "Adding indices, optimizing queries, denormalization, partitioning, "
        "connection pooling, or storage engine configuration."
    ),
    "micro_optimizations": (
        "Hot-path tweaks: inlining, branch reordering, avoiding temporary objects, "
        "strength reduction, guard clauses, or tight-loop tuning."
    ),
    "io_and_latency_hiding": (
        "Async/non-blocking I/O, overlapping I/O with compute, prefetching, pipelining, or reducing round-trip latency."
    ),
    "use_higher_level_system": (
        "Replacing hand-rolled logic with an optimized library or framework "
        "(e.g. pandas, polars, scipy, BLAS) that handles performance internally."
    ),
    "uncategorized": ("Performance-related change that does not clearly fit any of the above categories."),
}


class DifficultyLevel(str, enum.Enum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"


@dataclass
class ClassificationDecision:
    reason: str = ""
    category: str = ""
    difficulty: str = ""
    confidence: int = 0

    def __post_init__(self) -> None:
        self.confidence = max(0, min(100, self.confidence))

    @classmethod
    def from_prediction(cls, prediction: Any) -> ClassificationDecision:
        """Create a decision object from a DSPy prediction response."""
        reasoning = getattr(prediction, "reasoning", "") or ""
        category = getattr(prediction, "category", "") or ""
        difficulty = getattr(prediction, "difficulty", "") or ""
        raw_confidence = getattr(prediction, "confidence", None)

        confidence: int
        if isinstance(raw_confidence, int):
            confidence = raw_confidence
        else:
            try:
                confidence = int(str(raw_confidence).strip()) if raw_confidence is not None else 0
            except (TypeError, ValueError):
                confidence = 0

        return cls(
            reason=str(reasoning).strip(),
            category=str(category).strip(),
            difficulty=str(difficulty).strip(),
            confidence=confidence,
        )


class PerfClassifier:
    """Binary classifier: is this PR a performance improvement?"""

    def __init__(self) -> None:
        self._predictor: Any | None = None

    def _get_predictor(self) -> Any:
        if self._predictor is None:
            from datasmith.agents.config import ensure_configured

            ensure_configured()
            import dspy

            class JudgeSignature(dspy.Signature):
                """Decide if this commit's PRIMARY intent is to improve product/runtime performance.

                Label YES only when there is CLEAR, EXPLICIT evidence in the description and/or patch that the
                runtime gets faster (e.g., algorithm change, fewer allocations, caching, vectorization, reduced I/O,
                async/non-blocking for throughput, latency reduction, memory footprint reduction, fix a speed regression).

                Strong positive signals (weigh these collectively):
                - PR title/body contains performance intent (e.g., "PERF:", "speed up", "faster", "performance").
                - Linked issues/comments include benchmark links or timings demonstrating impact.
                - Low-level/hot-path tweaks (e.g., reuse global context, avoid per-call init/teardown, vectorize C/NumPy).

                Hard NO (non-performance) examples: tests/ASV/harness-only changes; CI/workflows/build/packaging; coverage;
                pre-commit/format/lints (clippy/ruff/black); docs; version bumps; terminology/renames; pure refactors without
                performance claims; changes aimed at making perf tests pass but not improving runtime.

                If ambiguous, weigh the concrete code changes and problem description together. When there are
                specific performance cues (title keywords, measured timings, fewer allocations, vectorization,
                caching/reuse) lean YES; otherwise NO.
                """

                problem_description: str = dspy.InputField(desc="Problem statement and technical context from PR/issue")
                github_patch: str = dspy.InputField(desc="Git diff showing actual code changes")
                file_change_summary: str = dspy.InputField(
                    desc="A markdown table summarizing all the files changed in the commit along with lines added/removed.",
                    default="",
                )
                reasoning: str = dspy.OutputField(desc="Deductive reasoning steps leading to the classification.")
                label: str = dspy.OutputField(desc='Final label: "YES" for performance-related, "NO" otherwise.')

            self._predictor = dspy.Predict(JudgeSignature)
        return self._predictor

    def classify(
        self, problem_description: str, github_patch: str = "", file_change_summary: str = ""
    ) -> tuple[bool, str]:
        """Return ``(is_performance, reasoning)``, or raise if the model could not answer.

        Raising is the point.  This used to catch every exception and return
        ``(False, "Classification failed")``, which stage 3 then wrote as
        ``is_performance_commit = False``.  The resume predicate is
        ``is_performance_commit IS NULL``, so a transient model timeout became
        a permanent exclusion that no re-run would revisit, and it was
        indistinguishable from a PR the model had genuinely judged.  Across
        the roughly 9 400 calls a July-plus-August run makes, a one percent
        error rate silently discards about ninety commits.

        Letting it propagate costs one ``runner_failures`` row and leaves the
        column NULL, so the next run picks the PR up again.
        """
        predictor = self._get_predictor()
        result = predictor(
            problem_description=problem_description,
            github_patch=github_patch,
            file_change_summary=file_change_summary,
        )
        label = str(getattr(result, "label", "")).strip().upper()
        reasoning = str(getattr(result, "reasoning", ""))
        if not label.startswith(("YES", "NO")):
            # An unparseable label is a failure to classify, not a negative.
            raise ValueError(f"PerfClassifier returned an unusable label: {label[:80]!r}")
        return label.startswith("YES"), reasoning


class ClassifyJudge:
    """Classify optimization type and difficulty."""

    def __init__(self, max_tokens: int | None = None) -> None:
        self._max_tokens = max_tokens or int(os.getenv("DSPY_MAX_TOKENS", "16000"))
        self._predictor: Any | None = None

    def _get_predictor(self) -> Any:
        if self._predictor is None:
            from datasmith.agents.config import ensure_configured

            ensure_configured()
            import dspy

            cat_lines = "\n".join(f"- {t.value}: {t.description}" for t in OptimizationType)
            cat_values = ", ".join(t.value for t in OptimizationType)

            class ClassifySignature(dspy.Signature):
                """Decide the PRIMARY performance optimization technique and difficulty level."""

                problem_description: str = dspy.InputField(desc="Problem statement and technical context from PR/issue")
                github_patch: str = dspy.InputField(desc="Git patch showing code changes")
                category: str = dspy.OutputField(desc=f"One of: {cat_values}")
                difficulty: str = dspy.OutputField(desc="One of: easy, medium, hard")
                reasoning: str = dspy.OutputField(desc="Brief explanation of the classification")

            ClassifySignature.__doc__ = (
                "Decide the PRIMARY performance optimization technique and difficulty level.\n\n"
                f"Category mapping (pick the single best match):\n{cat_lines}\n\n"
                "Difficulty levels:\n"
                "- easy: localized change (<50 lines), minimal risk\n"
                "- medium: module-level refactor, data structure changes\n"
                "- hard: algorithm rewrite or architectural change"
            )

            self._predictor = dspy.Predict(ClassifySignature)
        return self._predictor

    def truncate_patch(self, patch: str) -> str:
        try:
            import tiktoken

            enc = tiktoken.get_encoding("cl100k_base")
            tokens = enc.encode(patch)
            if len(tokens) > self._max_tokens:
                tokens = tokens[: self._max_tokens - 10]
                truncated = enc.decode(tokens)
                return truncated + "\n\n// [TRUNCATED DUE TO LENGTH]"
        except Exception:  # noqa: S110
            pass  # tiktoken not available, return untruncated
        return patch

    def classify(self, problem_description: str, github_patch: str = "") -> ClassificationDecision:
        github_patch = self.truncate_patch(github_patch)
        try:
            predictor = self._get_predictor()
            result = predictor(problem_description=problem_description, github_patch=github_patch)

            cat = str(getattr(result, "category", "")).strip().lower()
            valid_cats = {t.value for t in OptimizationType}
            if cat not in valid_cats:
                cat = "uncategorized"

            diff = str(getattr(result, "difficulty", "")).strip().lower()
            if diff not in ("easy", "medium", "hard"):
                diff = "medium"

            return ClassificationDecision(
                reason=str(getattr(result, "reasoning", "")),
                category=cat,
                difficulty=diff,
            )
        except Exception:
            logger.exception("ClassifyJudge failed")
            return ClassificationDecision(
                reason="Classification failed", category="uncategorized", difficulty="medium", confidence=0
            )
