from __future__ import annotations

import enum
from dataclasses import dataclass

from datasmith.utils import get_logger

logger = get_logger("agents.classifiers")


class OptimizationType(str, enum.Enum):
    ALGORITHMIC = "algorithmic"
    CACHING = "caching"
    CONCURRENCY = "concurrency"
    DATA_STRUCTURE = "data_structure"
    IO_OPTIMIZATION = "io_optimization"
    LAZY_EVALUATION = "lazy_evaluation"
    LOOP_OPTIMIZATION = "loop_optimization"
    MEMORY = "memory"
    NATIVE_EXTENSION = "native_extension"
    PRECOMPUTATION = "precomputation"
    QUERY_OPTIMIZATION = "query_optimization"
    SERIALIZATION = "serialization"
    VECTORIZATION = "vectorization"
    OTHER = "other"

    @property
    def description(self) -> str:
        descriptions = {
            "algorithmic": "Using a better algorithm with lower time complexity",
            "caching": "Storing computed results to avoid redundant work",
            "concurrency": "Parallelism, threading, or async improvements",
            "data_structure": "Switching to a more efficient data structure",
            "io_optimization": "Reducing I/O overhead or batching I/O operations",
            "lazy_evaluation": "Deferring computation until needed",
            "loop_optimization": "Reducing iterations, loop unrolling, early exit",
            "memory": "Reducing memory allocations or improving memory access patterns",
            "native_extension": "Offloading to C/Cython/Rust extensions",
            "precomputation": "Computing values ahead of time",
            "query_optimization": "Optimizing database or API queries",
            "serialization": "Faster serialization/deserialization",
            "vectorization": "Using SIMD, NumPy vectorization, or batch operations",
            "other": "Performance improvement not fitting other categories",
        }
        return descriptions.get(self.value, "")


@dataclass
class ClassificationDecision:
    reason: str = ""
    category: str = ""
    difficulty: str = ""
    confidence: int = 0

    def __post_init__(self) -> None:
        self.confidence = max(0, min(100, self.confidence))


class PerfClassifier:
    """Binary classifier: is this PR a performance improvement?"""

    def classify(self, problem_description: str, github_patch: str = "") -> tuple[bool, str]:
        try:
            from datasmith.agents.config import ensure_configured

            ensure_configured()
            import dspy

            class JudgeSignature(dspy.Signature):
                """Determine if a pull request is a performance improvement."""

                problem_description: str = dspy.InputField()
                github_patch: str = dspy.InputField()
                is_performance: str = dspy.OutputField(desc="YES or NO")
                reasoning: str = dspy.OutputField()

            predictor = dspy.Predict(JudgeSignature)
            result = predictor(problem_description=problem_description, github_patch=github_patch)
            is_perf = result.is_performance.strip().upper().startswith("YES")
        except Exception:
            logger.exception("PerfClassifier failed")
            return False, "Classification failed"
        else:
            return is_perf, result.reasoning


class ClassifyJudge:
    """Classify optimization type and difficulty."""

    def __init__(self, max_tokens: int = 16000) -> None:
        self._max_tokens = max_tokens

    def truncate_patch(self, patch: str) -> str:
        try:
            import tiktoken

            enc = tiktoken.get_encoding("cl100k_base")
            tokens = enc.encode(patch)
            if len(tokens) > self._max_tokens:
                tokens = tokens[: self._max_tokens]
                return enc.decode(tokens)
        except Exception:  # noqa: S110
            pass  # tiktoken not available, return untruncated
        return patch

    def classify(self, problem_description: str, github_patch: str = "") -> ClassificationDecision:
        github_patch = self.truncate_patch(github_patch)
        try:
            from datasmith.agents.config import ensure_configured

            ensure_configured()
            import dspy

            valid_types = ", ".join(t.value for t in OptimizationType)

            class ClassifySignature(dspy.Signature):
                """Classify the optimization type and difficulty of a performance PR."""

                problem_description: str = dspy.InputField()
                github_patch: str = dspy.InputField()
                category: str = dspy.OutputField(desc=f"One of: {valid_types}")
                difficulty: str = dspy.OutputField(desc="One of: easy, medium, hard")
                confidence: str = dspy.OutputField(desc="0-100")
                reasoning: str = dspy.OutputField()

            predictor = dspy.Predict(ClassifySignature)
            result = predictor(problem_description=problem_description, github_patch=github_patch)

            cat = result.category.strip().lower()
            if cat not in [t.value for t in OptimizationType]:
                cat = "other"

            diff = result.difficulty.strip().lower()
            if diff not in ("easy", "medium", "hard"):
                diff = "medium"

            try:
                conf = int(result.confidence.strip())
            except (ValueError, AttributeError):
                conf = 50

            return ClassificationDecision(
                reason=result.reasoning,
                category=cat,
                difficulty=diff,
                confidence=conf,
            )
        except Exception:
            logger.exception("ClassifyJudge failed")
            return ClassificationDecision(
                reason="Classification failed", category="other", difficulty="medium", confidence=0
            )
