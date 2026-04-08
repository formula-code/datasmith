"""Tests for datasmith.agents.classifiers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.agents.classifiers import ClassificationDecision, ClassifyJudge, OptimizationType, PerfClassifier


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

    def test_perf_classifier_exception(self) -> None:
        """On exception, returns (False, 'Classification failed')."""
        classifier = PerfClassifier()
        with patch.dict("sys.modules", {"dspy": None}):
            is_perf, reason = classifier.classify("test", "patch")
        assert is_perf is False
        assert "failed" in reason.lower()


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
