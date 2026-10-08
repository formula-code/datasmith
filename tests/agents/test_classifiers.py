"""Tests for datasmith.agents.classifiers."""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from datasmith.agents.classifiers import (
    PERF_CLASSIFIER_INSTRUCTIONS,
    ClassificationDecision,
    ClassifyJudge,
    OptimizationType,
    PerfClassifier,
    _truncate_to_tokens,
)


class TestOptimizationType:
    def test_optimization_type_has_descriptions(self) -> None:
        """All 14 enum members have non-empty descriptions."""
        assert len(OptimizationType) == 14
        for opt_type in OptimizationType:
            assert opt_type.description, f"{opt_type.value} has no description"
            assert isinstance(opt_type.description, str)

    def test_optimization_type_values(self) -> None:
        assert OptimizationType.USE_BETTER_ALGORITHM.value == "use_better_algorithm"
        assert OptimizationType.MICRO_OPTIMIZATIONS.value == "micro_optimizations"
        assert OptimizationType.UNCATEGORIZED.value == "uncategorized"

    def test_optimization_type_is_str_enum(self) -> None:
        assert isinstance(OptimizationType.CACHE_AND_REUSE, str)
        assert OptimizationType.CACHE_AND_REUSE == "cache_and_reuse"


class TestClassificationDecision:
    def test_classification_decision_confidence_clamped(self) -> None:
        """>100 clamped to 100, <0 clamped to 0."""
        high = ClassificationDecision(confidence=150)
        assert high.confidence == 100

        low = ClassificationDecision(confidence=-10)
        assert low.confidence == 0

    def test_classification_decision_defaults(self) -> None:
        decision = ClassificationDecision()
        assert decision.reason == ""
        assert decision.category == ""
        assert decision.difficulty == ""
        assert decision.confidence == 0

    def test_classification_decision_normal_confidence(self) -> None:
        decision = ClassificationDecision(confidence=75)
        assert decision.confidence == 75

    def test_from_prediction(self) -> None:
        pred = MagicMock()
        pred.reasoning = "Uses caching"
        pred.category = "cache_and_reuse"
        pred.difficulty = "easy"
        pred.confidence = "85"
        decision = ClassificationDecision.from_prediction(pred)
        assert decision.reason == "Uses caching"
        assert decision.category == "cache_and_reuse"
        assert decision.difficulty == "easy"
        assert decision.confidence == 85

    def test_from_prediction_bad_confidence(self) -> None:
        pred = MagicMock()
        pred.reasoning = "reason"
        pred.category = "uncategorized"
        pred.difficulty = "medium"
        pred.confidence = "not_a_number"
        decision = ClassificationDecision.from_prediction(pred)
        assert decision.confidence == 0


class TestPerfClassifier:
    def test_perf_classifier_yes(self) -> None:
        """Mock dspy YES returns (True, reasoning)."""
        mock_dspy = MagicMock()
        mock_result = MagicMock()
        mock_result.label = "YES"
        mock_result.reasoning = "Uses better algorithm"
        mock_dspy.Predict.return_value.return_value = mock_result

        classifier = PerfClassifier()
        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            is_perf, reason = classifier.classify("speed up loop", "diff content")
        assert is_perf is True
        assert reason == "Uses better algorithm"

    def test_perf_classifier_no(self) -> None:
        """Mock dspy NO returns (False, reasoning)."""
        mock_dspy = MagicMock()
        mock_result = MagicMock()
        mock_result.label = "NO"
        mock_result.reasoning = "This is a refactoring change"
        mock_dspy.Predict.return_value.return_value = mock_result

        classifier = PerfClassifier()
        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            is_perf, reason = classifier.classify("refactor code", "diff content")
        assert is_perf is False
        assert reason == "This is a refactoring change"

    def test_perf_classifier_with_file_change_summary(self) -> None:
        """file_change_summary is passed through."""
        mock_dspy = MagicMock()
        mock_result = MagicMock()
        mock_result.label = "YES"
        mock_result.reasoning = "perf change"
        mock_dspy.Predict.return_value.return_value = mock_result

        classifier = PerfClassifier()
        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            is_perf, _ = classifier.classify("speed up", "diff", "| file | +10 | -5 |")
        assert is_perf is True

    def test_perf_classifier_propagates_failure(self) -> None:
        """A model that cannot answer must raise, never report "not performance".

        This test previously asserted the opposite: that a failure returned
        ``(False, "Classification failed")``.  Stage 3 wrote that as
        ``is_performance_commit = False`` and the resume predicate is
        ``is_performance_commit IS NULL``, so a transient error permanently
        excluded the PR and looked exactly like a real negative.
        """
        classifier = PerfClassifier()
        with patch.dict("sys.modules", {"dspy": None}), pytest.raises(Exception, match=r".*"):
            classifier.classify("test", "patch")

    def test_unparseable_label_is_a_failure_not_a_negative(self) -> None:
        """A label that is neither YES nor NO must raise rather than read as NO."""
        classifier = PerfClassifier()
        classifier._predictor = MagicMock(return_value=SimpleNamespace(label="MAYBE?", reasoning="hedged"))
        with pytest.raises(ValueError, match="unusable label"):
            classifier.classify("test", "patch")

    def test_no_label_is_a_real_negative(self) -> None:
        """A genuine NO still returns cleanly — the raise must not swallow it."""
        classifier = PerfClassifier()
        classifier._predictor = MagicMock(return_value=SimpleNamespace(label="NO", reasoning="docs only"))
        is_perf, reason = classifier.classify("test", "patch")
        assert is_perf is False
        assert reason == "docs only"


class TestClassifyJudge:
    def test_classify_judge_truncate_patch(self) -> None:
        """Long patch gets truncated."""
        judge = ClassifyJudge(max_tokens=100)
        long_patch = "x " * 10000
        truncated = judge.truncate_patch(long_patch)
        assert isinstance(truncated, str)
        assert len(truncated) > 0

    def test_classify_judge_truncate_short_patch(self) -> None:
        """Short patch is returned unchanged."""
        judge = ClassifyJudge(max_tokens=16000)
        short_patch = "def foo(): pass"
        result = judge.truncate_patch(short_patch)
        assert result == short_patch

    def test_classify_judge_truncation_indicator(self) -> None:
        """Truncated patch ends with indicator."""
        judge = ClassifyJudge(max_tokens=100)
        long_patch = "x " * 10000
        truncated = judge.truncate_patch(long_patch)
        if len(truncated) < len(long_patch):
            assert "TRUNCATED" in truncated

    def test_classify_judge_valid_category(self) -> None:
        """Mock valid output returns correct ClassificationDecision."""
        mock_dspy = MagicMock()
        mock_result = MagicMock()
        mock_result.category = "use_better_algorithm"
        mock_result.difficulty = "hard"
        mock_result.reasoning = "Replaced O(n^2) with O(n log n)"
        mock_dspy.Predict.return_value.return_value = mock_result

        judge = ClassifyJudge()
        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            decision = judge.classify("optimize sort", "diff")
        assert decision.category == "use_better_algorithm"
        assert decision.difficulty == "hard"
        assert "O(n" in decision.reason

    def test_classify_judge_invalid_category_falls_to_uncategorized(self) -> None:
        """Unknown category defaults to 'uncategorized'."""
        mock_dspy = MagicMock()
        mock_result = MagicMock()
        mock_result.category = "unknown_category"
        mock_result.difficulty = "easy"
        mock_result.reasoning = "reason"
        mock_dspy.Predict.return_value.return_value = mock_result

        judge = ClassifyJudge()
        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            decision = judge.classify("test", "diff")
        assert decision.category == "uncategorized"

    def test_classify_judge_invalid_difficulty_falls_to_medium(self) -> None:
        """Unknown difficulty defaults to 'medium'."""
        mock_dspy = MagicMock()
        mock_result = MagicMock()
        mock_result.category = "cache_and_reuse"
        mock_result.difficulty = "extreme"
        mock_result.reasoning = "reason"
        mock_dspy.Predict.return_value.return_value = mock_result

        judge = ClassifyJudge()
        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            decision = judge.classify("test", "diff")
        assert decision.difficulty == "medium"

    def test_classify_judge_exception(self) -> None:
        """On exception, returns fallback ClassificationDecision."""
        judge = ClassifyJudge()
        with patch.dict("sys.modules", {"dspy": None}):
            decision = judge.classify("test", "patch")
        assert decision.category == "uncategorized"
        assert decision.difficulty == "medium"
        assert decision.confidence == 0


class TestPerfClassifierInstructions:
    """Pin the properties of the prompt that the evaluation actually paid for.

    These are not style assertions.  Each one corresponds to a measured failure:
    dropping the hard-NO list admitted ``CI: Bump dask`` and ``DOC: whats new
    v0.20``, and leaning on title keywords cost ten points of recall on PRs whose
    titles say nothing about performance -- which is most of them.
    """

    def test_hard_no_list_precedes_the_yes_guidance(self) -> None:
        """Order matters: the exclusions must win, so they must be stated first."""
        text = PERF_CLASSIFIER_INSTRUCTIONS
        no_at = text.index("Answer NO")
        yes_at = text.index("Otherwise answer YES")
        assert no_at < yes_at, "the YES guidance must not precede the exclusions it is scoped by"
        assert "wins outright" in text

    def test_benchmark_harness_changes_are_excluded(self) -> None:
        """Adding or speeding up a benchmark is not an optimisable task for this pipeline."""
        assert "benchmark-harness" in PERF_CLASSIFIER_INSTRUCTIONS
        assert "the code under measurement must change, not the measurement" in PERF_CLASSIFIER_INSTRUCTIONS

    def test_it_tells_the_model_not_to_rely_on_title_keywords(self) -> None:
        """43% of confirmed perf PRs carry a perf keyword; the rest must still be caught."""
        text = PERF_CLASSIFIER_INSTRUCTIONS
        assert "Judge the CODE, not the wording" in text
        assert "is not evidence" in text

    def test_the_unscoped_answer_yes_instruction_is_not_reintroduced(self) -> None:
        """An unconditional "if unsure, YES" measured a 5% false-positive rate."""
        text = PERF_CLASSIFIER_INSTRUCTIONS
        # The concession is allowed, but only after the NO list has excluded the obvious.
        assert "If the diff is not in the NO list above" in text

    def test_the_signature_actually_uses_it(self) -> None:
        """The constant is only worth pinning if the predictor is built from it."""
        mock_dspy = MagicMock()
        captured: dict[str, object] = {}

        class _Sig:
            pass

        mock_dspy.Signature = _Sig
        mock_dspy.Predict.side_effect = lambda sig: captured.setdefault("doc", sig.__doc__)
        classifier = PerfClassifier()
        with patch.dict("sys.modules", {"dspy": mock_dspy}), contextlib.suppress(Exception):
            classifier._get_predictor()
        assert captured.get("doc") == PERF_CLASSIFIER_INSTRUCTIONS


class TestPerfClassifierTruncatesItsPatch:
    """An oversized patch must be cut, not sent and retried forever.

    ClassifyJudge always truncated; PerfClassifier never did.  A patch larger
    than the model's context came back as ContextWindowExceededError, which no
    retry can fix because the patch is the same size next time -- and because a
    failed classify correctly leaves is_performance_commit NULL, the stage 3
    resume predicate re-selected that row on every future run.
    """

    def test_oversized_patch_is_truncated_before_the_call(self) -> None:
        seen: dict[str, str] = {}

        def _capture(**kw: Any) -> Any:
            seen.update(kw)
            return SimpleNamespace(label="NO", reasoning="r")

        classifier = PerfClassifier()
        classifier._predictor = _capture
        huge = "x " * 400_000
        classifier.classify("t", huge)
        assert len(seen["github_patch"]) < len(huge)
        assert "TRUNCATED" in seen["github_patch"]

    def test_small_patch_is_passed_through_untouched(self) -> None:
        seen: dict[str, str] = {}

        def _capture(**kw: Any) -> Any:
            seen.update(kw)
            return SimpleNamespace(label="NO", reasoning="r")

        classifier = PerfClassifier()
        classifier._predictor = _capture
        patch = "--- a/x.py\n+++ b/x.py\n@@\n-slow\n+fast\n"
        classifier.classify("t", patch)
        assert seen["github_patch"] == patch

    def test_truncation_survives_a_missing_tiktoken(self) -> None:
        """Returning the text untouched is what let the overflow through."""
        huge = "x " * 400_000
        with patch.dict("sys.modules", {"tiktoken": None}):
            out = _truncate_to_tokens(huge, 1000)
        assert len(out) < len(huge)
        assert "TRUNCATED" in out
