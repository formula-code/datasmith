"""
Unit tests for ProblemExtractor agent.

These tests verify that the extractive approach maintains high fidelity
to source material (85%+ LCS ratio) and preserves code blocks exactly.
"""

from unittest.mock import Mock, patch

import pytest

from datasmith.agents.problem_extractor import (
    ProblemExtraction,
    ProblemExtractor,
    ProblemExtractorSignature,
)
from datasmith.scrape.verbatim_checker import (
    is_verbatim_subset,
    validate_code_block_verbatim,
)

# Sample source material for testing
SAMPLE_ISSUE_SOURCE = """
Points in napari are sometimes invisible (too small) or take up the entire screen (too large)
depending on the coordinate system used.

For example, when using large coordinates:
```python
viewer = napari.Viewer()
viewer.add_image(np.zeros((1000,1000)))
viewer.add_points([(20, 20)])
```
Points are hardly visible.

When using small coordinates:
```python
viewer = napari.Viewer()
viewer.add_image(np.zeros((10,10)))
viewer.add_points([(5, 5)])
```
Points are super big.

Proposed solution: Use `experimental_canvas_size_limits` to clamp point sizes.
Default values under discussion: (2, 100) or (2, 10000).
"""

SAMPLE_COMMENT_THREAD = """
**brisvag** — 09:58 11/07/2022

This PR doesn't solve the original #4705 issue (auto-sizing based on data scale),
only prevents extreme invisibility.

**jni** — 22:54 01/08/2022

I like the minimum size, but dislike the maximum size. 100 is too aggressive,
molecular vis demos would break.

**haesleinhuepf** — 17:09 12/09/2022

Consider having a computed default-point size:
```python
points_layer.size = np.asarray(viewer.dims.range).max() * 0.01
```
"""


class TestProblemExtractorSignatures:
    """Test the DSPy signatures."""

    def test_problem_extractor_signature_fields(self):
        """Test that ProblemExtractorSignature has required fields."""
        # DSPy signatures have fields as class attributes via Field()
        # Check that we can instantiate the signature
        sig = ProblemExtractorSignature
        # Verify signature has the expected field names in __annotations__
        assert "pr_body" in sig.__annotations__
        assert "pr_comments" in sig.__annotations__
        assert "reasoning" in sig.__annotations__
        assert "extracted_problem_description" in sig.__annotations__
        assert "extracted_solution_overview" in sig.__annotations__


class TestProblemExtractorInitialization:
    """Test ProblemExtractor initialization."""

    def test_default_initialization(self):
        """Test default initialization."""
        extractor = ProblemExtractor()
        assert extractor.validate_lcs is True
        assert extractor.min_lcs == 0.75
        assert extractor.log_validation is True

    def test_custom_initialization(self):
        """Test custom initialization parameters."""
        extractor = ProblemExtractor(
            validate_lcs=False,
            min_lcs=0.75,
            log_validation=False,
        )
        assert extractor.validate_lcs is False
        assert extractor.min_lcs == 0.75
        assert extractor.log_validation is False


class TestProblemExtraction:
    """Test problem statement extraction."""

    @pytest.fixture
    def extractor(self):
        """Create ProblemExtractor instance."""
        return ProblemExtractor(validate_lcs=True, min_lcs=0.85, log_validation=False)

    @patch("datasmith.agents.problem_extractor.dspy.Predict")
    def test_extract_problem_returns_tuple(self, mock_predict, extractor):
        """Test that extract_problem returns (extracted, validation) tuple."""
        # Mock the predictor response
        mock_result = Mock()
        mock_result.extracted_problem_description = "This is a test extracted problem statement with enough length."
        mock_result.extracted_solution_overview = None
        mock_predict.return_value = Mock(return_value=mock_result)

        # Create extractor with mocked predictor
        extractor.problem_predictor = mock_predict.return_value

        result = extractor.extract_problem(
            pr_body="test message body with sufficient length",
            pr_comments="test comments with sufficient length",
        )

        assert isinstance(result, tuple)
        assert len(result) == 2
        extracted, validation = result
        assert isinstance(extracted, ProblemExtraction)
        assert extracted.problem_statement == "This is a test extracted problem statement with enough length."
        assert isinstance(validation, dict)

    @patch("datasmith.agents.problem_extractor.dspy.Predict")
    def test_extract_problem_validation_disabled(self, mock_predict, extractor):
        """Test extraction with validation disabled."""
        extractor.validate_lcs = False

        mock_result = Mock()
        mock_result.extracted_problem_description = "This is a test problem description that is long enough."
        mock_result.extracted_solution_overview = None
        mock_predict.return_value = Mock(return_value=mock_result)
        extractor.problem_predictor = mock_predict.return_value

        extracted, validation = extractor.extract_problem(
            pr_body="issue body text that is long enough",
            pr_comments="related comments text that is long enough",
        )

        assert validation["validation_enabled"] is False
        assert validation["overall_pass"] is True
        assert extracted.problem_statement == "This is a test problem description that is long enough."

    @patch("datasmith.agents.problem_extractor.dspy.Predict")
    def test_extract_problem_handles_exceptions(self, mock_predict, extractor):
        """Test that extraction handles exceptions gracefully."""
        # Make predictor raise exception
        mock_predict.return_value = Mock(side_effect=Exception("Test error"))
        extractor.problem_predictor = mock_predict.return_value

        extracted, validation = extractor.extract_problem(pr_body="message body", pr_comments="issues text")

        assert isinstance(extracted, ProblemExtraction)
        assert extracted.problem_statement is not None
        assert "[extraction failed:" in extracted.problem_statement
        assert validation["overall_pass"] is False
        assert "error" in validation


class TestValidation:
    """Test validation functionality."""

    @pytest.fixture
    def extractor(self):
        """Create ProblemExtractor instance."""
        return ProblemExtractor(validate_lcs=True, min_lcs=0.85, log_validation=False)

    def test_validate_extraction_verbatim_text(self, extractor):
        """Test validation with verbatim extracted text."""
        source = "Points are hardly visible when using large coordinate values."
        extracted = "Points are hardly visible when using large coordinate values."

        validation = extractor._validate_extraction(extracted=extracted, source=source, extraction_type="test")

        assert validation["overall_pass"] is True
        assert validation["avg_lcs"] >= 0.95  # Should be very high for verbatim

    def test_validate_extraction_abstractive_text(self, extractor):
        """Test validation with too-abstractive text."""
        source = "Points are hardly visible when using large coordinate values."
        extracted = "There has been discussion about point visibility issues."

        validation = extractor._validate_extraction(extracted=extracted, source=source, extraction_type="test")

        # This should fail because it's too abstractive
        assert validation["overall_pass"] is False
        assert validation["avg_lcs"] < 0.85

    def test_validate_extraction_disabled(self, extractor):
        """Test that validation can be disabled."""
        extractor.validate_lcs = False

        validation = extractor._validate_extraction(extracted="anything", source="source", extraction_type="test")

        assert validation["validation_enabled"] is False
        assert validation["overall_pass"] is True

    def test_validate_extraction_with_code_blocks(self, extractor):
        """Test validation with code blocks."""
        source = """
        Example code:
        ```python
        viewer.add_points([(20, 20)])
        ```
        """
        # Exact code preservation
        extracted = """
        Example code:
        ```python
        viewer.add_points([(20, 20)])
        ```
        """

        validation = extractor._validate_extraction(extracted=extracted, source=source, extraction_type="test")

        # Should pass with verbatim code
        assert validation["overall_pass"] is True


class TestCodeBlockPreservation:
    """Test that code blocks are preserved verbatim."""

    def test_code_block_exact_match(self):
        """Test that code blocks must be character-exact."""
        # Source with code block
        source = """Points are invisible:
```python
viewer = napari.Viewer()
viewer.add_points([(20, 20)])
```
This is the problem."""

        # Exact match (should pass)
        exact_code = "viewer = napari.Viewer()\nviewer.add_points([(20, 20)])"
        assert validate_code_block_verbatim(exact_code, source)

        # Modified code (should fail)
        modified_code = "viewer = napari.Viewer()\nviewer.add_points([(20,20)])"  # No space
        assert not validate_code_block_verbatim(modified_code, source)


class TestExtractivenessMeasurement:
    """Test LCS ratio measurement."""

    def test_lcs_ratio_verbatim(self):
        """Test LCS ratio for verbatim text."""
        text = "Points are hardly visible"
        source = "When using large images, points are hardly visible."

        score = is_verbatim_subset(text, source, min_lcs=0.85)

        # Should pass because text appears verbatim in source
        assert score.is_verbatim is True
        assert score.lcs_ratio >= 0.85

    def test_lcs_ratio_abstractive(self):
        """Test LCS ratio for abstractive text."""
        text = "There has been discussion about visibility"
        source = "Points are hardly visible when using large coordinates."

        score = is_verbatim_subset(text, source, min_lcs=0.85)

        # Should fail because text is too different from source
        assert score.is_verbatim is False
        assert score.lcs_ratio < 0.85


class TestEndToEnd:
    """End-to-end integration tests."""

    @pytest.fixture
    def extractor_no_validation(self):
        """Create extractor without validation for faster testing."""
        return ProblemExtractor(validate_lcs=False, log_validation=False)

    def test_full_extraction_workflow(self, extractor_no_validation):
        """Test complete extraction workflow."""
        # Mock good extractive output
        mock_problem_result = Mock()
        mock_problem_result.extracted_problem_description = SAMPLE_ISSUE_SOURCE.strip()
        mock_problem_result.extracted_solution_overview = None

        # Set up predictor
        problem_mock = Mock(return_value=mock_problem_result)

        with patch.object(extractor_no_validation, "problem_predictor", problem_mock):
            # Extract problem using both issue and comment text
            problem, p_validation = extractor_no_validation.extract_problem(
                pr_body=SAMPLE_ISSUE_SOURCE,
                pr_comments=SAMPLE_COMMENT_THREAD,
            )

            # Check results
            assert isinstance(problem, ProblemExtraction)
            assert problem.problem_statement is not None
            assert problem.to_markdown()
            assert p_validation["overall_pass"] is True


class TestValidationReporting:
    """Test validation reporting functionality."""

    @pytest.fixture
    def extractor(self):
        """Create extractor with validation enabled."""
        return ProblemExtractor(validate_lcs=True, min_lcs=0.85, log_validation=False)

    def test_validation_report_structure(self, extractor):
        """Test that validation report has expected structure."""
        validation = extractor._validate_extraction(
            extracted="Points are hardly visible",
            source="Points are hardly visible when using large images",
            extraction_type="test",
        )

        # Check report structure
        assert "overall_pass" in validation
        assert "avg_lcs" in validation
        assert "avg_ngram" in validation
        assert "details" in validation
        assert "validation_enabled" in validation

    def test_validation_report_passing(self, extractor):
        """Test validation report for passing extraction."""
        source = "Points are hardly visible"
        extracted = "Points are hardly visible"

        validation = extractor._validate_extraction(extracted=extracted, source=source, extraction_type="test")

        assert validation["overall_pass"] is True
        assert validation["avg_lcs"] >= 0.95

    def test_validation_report_failing(self, extractor):
        """Test validation report for failing extraction."""
        source = "Points are hardly visible when using large coordinate values"
        extracted = "There has been some discussion about point size issues"

        validation = extractor._validate_extraction(extracted=extracted, source=source, extraction_type="test")

        assert validation["overall_pass"] is False
        assert validation["avg_lcs"] < 0.85
        assert "details" in validation
        assert "FAIL" in validation["details"]
