"""
Integration tests for ReportBuilder with ProblemExtractor.

These tests verify that the new extractive approach works correctly
when integrated with ReportBuilder.
"""

from unittest.mock import Mock, patch

import pytest

from datasmith.agents.problem_extractor import ProblemExtraction
from datasmith.scrape.report_builder import ReportBuilder
from datasmith.scrape.verbatim_checker import is_verbatim_subset

# Sample PR data for testing
SAMPLE_PR_DICT = {
    "sha": "abc123def456",
    "pr_url": "https://api.github.com/repos/test-org/test-repo/pulls/123",
    "pr_body": """
Fixes #456

Points in napari are sometimes invisible (too small) or take up the entire screen (too large)
depending on the coordinate system used.

## Reproduction

```python
viewer = napari.Viewer()
viewer.add_image(np.zeros((1000,1000)))
viewer.add_points([(20, 20)])
```

Points are hardly visible.

## Proposed Solution

Use `experimental_canvas_size_limits` to clamp point sizes on the canvas.
Default values: (2, 100) or (2, 10000).
""",
    "patch": """
diff --git a/napari/layers/points.py b/napari/layers/points.py
index abc123..def456 100644
--- a/napari/layers/points.py
+++ b/napari/layers/points.py
@@ -10,6 +10,7 @@ class PointsLayer(Layer):
     def __init__(self, data, **kwargs):
         super().__init__(data, **kwargs)
         self.size = kwargs.get('size', 10)
+        self.experimental_canvas_size_limits = (2, 10000)
""",
    "message": "Add experimental_canvas_size_limits to Points layer",
    "date": "2023-05-15T10:30:00Z",
    "pr_number": 123,
    "pr_merged_at": "2023-05-16T10:30:00Z",
}


class TestReportBuilderWithProblemExtractor:
    """Test ReportBuilder integration with ProblemExtractor."""

    @pytest.fixture
    def builder_no_llm(self):
        """Create ReportBuilder without LLM backends."""
        return ReportBuilder(
            enable_llm_backends=False,
            summarize_llm=False,
        )

    def test_builder_initialization_without_llm(self, builder_no_llm):
        """Test that builder initializes correctly without LLM."""
        assert builder_no_llm.problem_extractor is None
        assert builder_no_llm.summarize_llm is False

    def test_builder_initialization_with_llm(self):
        """Test that builder initializes ProblemExtractor when LLM enabled."""
        with (
            patch("datasmith.scrape.report_builder.configure_agent_backends"),
            patch("datasmith.scrape.report_builder.ProblemExtractor") as mock_extractor,
            patch("datasmith.scrape.report_builder.ClassifyJudge"),
            patch("datasmith.scrape.report_builder.PerfClassifier"),
        ):
            ReportBuilder(
                enable_llm_backends=True,
                summarize_llm=True,
            )

            # Verify ProblemExtractor was initialized with correct params
            mock_extractor.assert_called_once_with(validate_lcs=True)

    @patch("datasmith.scrape.report_builder.issue_timeline")
    @patch("datasmith.scrape.report_builder.extract_issues_from_description")
    def test_build_without_llm(self, mock_extract, mock_timeline, builder_no_llm):
        """Test building report without LLM extraction."""
        # Mock the data fetching
        mock_timeline.return_value = []
        mock_extract.return_value = []

        result = builder_no_llm.build(SAMPLE_PR_DICT)

        # Should succeed without LLM
        assert result.final_md != "NOT_A_VALID_PR"
        # Raw problem statement should contain the original text
        assert "Points in napari are sometimes invisible" in result.raw_problem_statement


class TestExtractProblemStatementMethod:
    """Test _extract_problem_statement method."""

    @pytest.fixture
    def builder_with_mock_extractor(self):
        """Create builder with mocked ProblemExtractor."""
        with patch("datasmith.scrape.report_builder.configure_agent_backends"):
            builder = ReportBuilder(enable_llm_backends=False)
            builder.problem_extractor = Mock()
            builder.summarize_llm = True
            return builder

    def test_extract_problem_statement_with_extractor(self, builder_with_mock_extractor):
        """Test that _extract_problem_statement uses ProblemExtractor."""
        mock_extracted = ProblemExtraction(initial_observations="This is the extracted problem statement")
        mock_validation = {
            "validation_enabled": True,
            "overall_pass": True,
            "avg_lcs": 0.90,
            "avg_ngram": 0.85,
        }
        builder_with_mock_extractor.problem_extractor.extract_problem.return_value = (
            mock_extracted,
            mock_validation,
        )

        result = builder_with_mock_extractor._extract_problem_statement("issue text", "related issues")

        assert isinstance(result, ProblemExtraction)
        assert result.initial_observations == "This is the extracted problem statement"
        builder_with_mock_extractor.problem_extractor.extract_problem.assert_called_once_with(
            pr_body="issue text", pr_comments="related issues"
        )

    def test_extract_problem_statement_logs_warning_on_low_quality(self, builder_with_mock_extractor, caplog):
        """Test that low extraction quality triggers warning."""
        mock_extracted = ProblemExtraction(initial_observations="Extracted text")
        mock_validation = {
            "validation_enabled": True,
            "overall_pass": False,  # Failed validation
            "avg_lcs": 0.60,
            "avg_ngram": 0.50,
            "details": "Extraction quality too low",
        }
        builder_with_mock_extractor.problem_extractor.extract_problem.return_value = (
            mock_extracted,
            mock_validation,
        )

        with caplog.at_level("INFO"):
            result = builder_with_mock_extractor._extract_problem_statement("issue text", "related issues")

        # Should still return the extracted data
        assert isinstance(result, ProblemExtraction)
        # Should log validation metrics even for low extractiveness
        assert "problem statement extraction validation" in caplog.text.lower()


class TestExtractiveQualityMeasurement:
    """Test that extraction maintains high LCS ratio."""

    def test_mock_extraction_lcs_ratio(self):
        """Test LCS ratio calculation on mock extraction."""
        source = "Points are hardly visible when using large coordinate values."

        # Good extraction (verbatim) - uses sliding window to find best match
        good_extraction = "Points are hardly visible"
        score_good = is_verbatim_subset(good_extraction, source, min_lcs=0.85)
        assert score_good.is_verbatim, f"Good extraction should be verbatim: {score_good.details}"
        assert score_good.lcs_ratio >= 0.85, f"Good extraction LCS: {score_good.lcs_ratio:.2%}"

        # Bad extraction (abstractive)
        bad_extraction = "There has been discussion about visibility issues"
        score_bad = is_verbatim_subset(bad_extraction, source, min_lcs=0.85)
        assert not score_bad.is_verbatim, f"Bad extraction should not be verbatim: {score_bad.details}"
        assert score_bad.lcs_ratio < 0.85, f"Bad extraction LCS: {score_bad.lcs_ratio:.2%}"

    def test_code_preservation_in_extraction(self):
        """Test that code blocks would be preserved."""
        source = """
        The fix is simple:
        ```python
        viewer.add_points([(20, 20)])
        ```
        """

        # Good: code preserved exactly
        good_extraction = "viewer.add_points([(20, 20)])"
        assert good_extraction in source

        # Bad: code paraphrased
        bad_extraction = "viewer.add_points([(20,20)])"
        assert bad_extraction not in source


class TestEndToEndWithMockedLLM:
    """End-to-end tests with mocked LLM responses."""

    @patch("datasmith.scrape.report_builder.issue_timeline")
    @patch("datasmith.scrape.report_builder.extract_issues_from_description")
    @patch("datasmith.scrape.report_builder.configure_agent_backends")
    def test_full_build_with_mocked_extraction(self, mock_config, mock_extract, mock_timeline):
        """Test complete build workflow with mocked LLM extraction."""
        # Setup mocks
        mock_timeline.return_value = []
        mock_extract.return_value = []

        # Create builder with mocked extractor
        builder = ReportBuilder(enable_llm_backends=False)

        # Mock the extractor
        mock_extractor = Mock()
        mock_extractor.extract_problem.return_value = (
            ProblemExtraction(initial_observations="Points are hardly visible. Points are super big."),
            {"validation_enabled": True, "overall_pass": True, "avg_lcs": 0.95},
        )
        mock_extractor.extract_comments.return_value = (
            "No comments",
            {"validation_enabled": True, "overall_pass": True, "avg_lcs": 1.0},
        )
        builder.problem_extractor = mock_extractor
        builder.summarize_llm = True

        # Build report
        result = builder.build(SAMPLE_PR_DICT)

        # Verify extraction was called
        assert mock_extractor.extract_problem.called

        # Verify result structure
        assert result.final_md != "NOT_A_VALID_PR"
        assert "Points are hardly visible" in result.problem_statement
        assert result.problem_sections is not None
        assert result.problem_sections.initial_observations
        assert result.is_performance_commit is False  # No perf detection


class TestBackwardCompatibility:
    """Test backward compatibility with old approach."""

    def test_flag_names_unchanged(self):
        """Test that configuration flag names are unchanged."""
        # Should work with old flag names
        builder = ReportBuilder(
            enable_llm_backends=False,
            summarize_llm=False,  # Old flag name still works
            add_classification=False,
            filter_performance_only=False,
        )

        assert builder.summarize_llm is False
        assert builder.add_classification is False

    def test_output_structure_unchanged(self):
        """Test that output structure remains the same."""
        with (
            patch("datasmith.scrape.report_builder.issue_timeline", return_value=[]),
            patch("datasmith.scrape.report_builder.extract_issues_from_description", return_value=[]),
        ):
            builder = ReportBuilder(enable_llm_backends=False)
            result = builder.build(SAMPLE_PR_DICT)

            # Check that result has expected attributes
            assert hasattr(result, "final_md")
            assert hasattr(result, "problem_statement")
            assert hasattr(result, "hints")
            assert hasattr(result, "classification")
            assert hasattr(result, "difficulty")
            assert hasattr(result, "is_performance_commit")
            assert hasattr(result, "classification_reason")
            assert hasattr(result, "classification_confidence")
            assert hasattr(result, "problem_sections")
