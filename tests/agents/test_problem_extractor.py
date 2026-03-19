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
        assert "initial_observations" in sig.__annotations__
        assert "triage_attempts" in sig.__annotations__
        assert "solution_overview" in sig.__annotations__
        assert "solution_observations" in sig.__annotations__


class TestProblemExtractorInitialization:
    """Test ProblemExtractor initialization."""

    def test_default_initialization(self):
        """Test default initialization."""
        extractor = ProblemExtractor()
        assert extractor.problem_predictor is not None

    def test_custom_initialization(self):
        """Test that extractor initializes correctly."""
        extractor = ProblemExtractor()
        assert extractor.problem_predictor is not None


class TestProblemExtraction:
    """Test problem statement extraction."""

    @pytest.fixture
    def extractor(self):
        """Create ProblemExtractor instance."""
        return ProblemExtractor()

    @patch("datasmith.agents.problem_extractor.dspy.Predict")
    def test_extract_problem_returns_extraction(self, mock_predict, extractor):
        """Test that extract_problem returns ProblemExtraction."""
        # Mock the predictor response
        mock_result = Mock()
        mock_result.initial_observations = "This is a test extracted problem statement with enough length."
        mock_result.triage_attempts = None
        mock_result.solution_overview = None
        mock_result.solution_observations = None
        mock_predict.return_value = Mock(return_value=mock_result)

        # Create extractor with mocked predictor
        extractor.problem_predictor = mock_predict.return_value

        result = extractor.extract_problem(
            pr_body="test message body with sufficient length",
            pr_comments="test comments with sufficient length",
        )

        assert isinstance(result, ProblemExtraction)
        assert result.initial_observations == "This is a test extracted problem statement with enough length."

    @patch("datasmith.agents.problem_extractor.dspy.Predict")
    def test_extract_problem_with_all_fields(self, mock_predict, extractor):
        """Test extraction with all four fields populated."""
        mock_result = Mock()
        mock_result.initial_observations = "This is a test problem description that is long enough."
        mock_result.triage_attempts = "We checked the performance metrics."
        mock_result.solution_overview = "Changed the algorithm to use caching."
        mock_result.solution_observations = "Performance improved by 50%."
        mock_predict.return_value = Mock(return_value=mock_result)
        extractor.problem_predictor = mock_predict.return_value

        extracted = extractor.extract_problem(
            pr_body="issue body text that is long enough",
            pr_comments="related comments text that is long enough",
        )

        assert extracted.initial_observations == "This is a test problem description that is long enough."
        assert extracted.triage_attempts == "We checked the performance metrics."
        assert extracted.solution_overview == "Changed the algorithm to use caching."
        assert extracted.solution_observations == "Performance improved by 50%."

    @patch("datasmith.agents.problem_extractor.dspy.Predict")
    def test_extract_problem_handles_exceptions(self, mock_predict, extractor):
        """Test that extraction handles exceptions gracefully."""
        # Make predictor raise exception
        mock_predict.return_value = Mock(side_effect=Exception("Test error"))
        extractor.problem_predictor = mock_predict.return_value

        extracted = extractor.extract_problem(pr_body="message body", pr_comments="issues text")

        assert isinstance(extracted, ProblemExtraction)
        assert extracted.initial_observations is not None
        assert "[extraction failed:" in extracted.initial_observations


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
        """Create extractor for testing."""
        return ProblemExtractor()

    def test_full_extraction_workflow(self, extractor_no_validation):
        """Test complete extraction workflow."""
        # Mock good extractive output
        mock_problem_result = Mock()
        mock_problem_result.initial_observations = SAMPLE_ISSUE_SOURCE.strip()
        mock_problem_result.triage_attempts = None
        mock_problem_result.solution_overview = None
        mock_problem_result.solution_observations = None

        # Set up predictor
        problem_mock = Mock(return_value=mock_problem_result)

        with patch.object(extractor_no_validation, "problem_predictor", problem_mock):
            # Extract problem using both issue and comment text
            problem = extractor_no_validation.extract_problem(
                pr_body=SAMPLE_ISSUE_SOURCE,
                pr_comments=SAMPLE_COMMENT_THREAD,
            )

            # Check results
            assert isinstance(problem, ProblemExtraction)
            assert problem.initial_observations is not None
            assert problem.to_markdown()
