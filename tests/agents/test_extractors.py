"""Tests for datasmith.agents.extractors."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.agents.extractors import ProblemExtraction, ProblemExtractor


class TestProblemExtraction:
    def test_problem_extraction_fields(self) -> None:
        """All 4 fields are settable."""
        ext = ProblemExtraction(
            initial_observations="obs",
            triage_attempts="triage",
            solution_overview="overview",
            solution_observations="sol_obs",
        )
        assert ext.initial_observations == "obs"
        assert ext.triage_attempts == "triage"
        assert ext.solution_overview == "overview"
        assert ext.solution_observations == "sol_obs"

    def test_to_problem_markdown_format(self) -> None:
        """Returns only initial_observations without headers."""
        ext = ProblemExtraction(
            initial_observations="The system is slow",
            triage_attempts="Profiled the hotloop",
        )
        md = ext.to_problem_markdown()
        assert "The system is slow" in md
        # No headers or triage in problem markdown
        assert "## Initial Observations" not in md
        assert "## Triage Attempts" not in md
        assert "Profiled the hotloop" not in md

    def test_to_problem_markdown_strips_json_fences(self) -> None:
        ext = ProblemExtraction(initial_observations="```json\nsome text")
        md = ext.to_problem_markdown()
        assert "```json" not in md
        assert "some text" in md

    def test_to_full_markdown_includes_all_sections(self) -> None:
        ext = ProblemExtraction(
            initial_observations="obs text here",
            triage_attempts="triage text here",
            solution_overview="overview text here",
            solution_observations="sol obs text here",
        )
        md = ext.to_full_markdown()
        assert "obs text here" in md
        assert "## Triage Attempts" in md
        assert "## Solution Overview" in md
        assert "## Solution Observations" in md

    def test_to_full_markdown_normalises_redundant_headers(self) -> None:
        ext = ProblemExtraction(
            initial_observations="obs",
            triage_attempts="## Triage Attempts\nactual content",
        )
        md = ext.to_full_markdown()
        # Should have exactly one Triage Attempts header, not two
        assert md.count("## Triage Attempts") == 1

    def test_to_dict(self) -> None:
        ext = ProblemExtraction(initial_observations="a", triage_attempts="b")
        d = ext.to_dict()
        assert d["initial_observations"] == "a"
        assert d["triage_attempts"] == "b"
        assert d["solution_overview"] == ""
        assert d["solution_observations"] == ""

    def test_empty_fields_produce_empty_markdown(self) -> None:
        ext = ProblemExtraction()
        assert ext.to_problem_markdown() == ""
        assert ext.to_full_markdown() == ""


class TestProblemExtractor:
    def test_extractor_handles_empty_input(self) -> None:
        """Empty body returns gracefully (mock dspy to raise)."""
        extractor = ProblemExtractor()
        with (
            patch.dict("sys.modules", {"dspy": MagicMock(side_effect=ImportError)}),
            patch("builtins.__import__", side_effect=ImportError("no dspy")),
        ):
            result = extractor.extract_problem(pr_title="", pr_body="", pr_comments="")
        assert isinstance(result, ProblemExtraction)

    def test_extractor_with_mock_dspy(self) -> None:
        """Full dspy mock returning valid output."""
        mock_dspy = MagicMock()
        mock_result = MagicMock()
        mock_result.initial_observations = "This is a meaningful initial observation text"
        mock_result.triage_attempts = "We tried profiling the application"
        mock_result.solution_overview = "Replaced the O(n^2) loop with a hash map"
        mock_result.solution_observations = "The fix reduced runtime by 50 percent overall"
        mock_dspy.Predict.return_value.return_value = mock_result

        extractor = ProblemExtractor()
        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            result = extractor.extract_problem(
                pr_title="Speed up lookup",
                pr_body="The lookup function is too slow.",
                pr_comments="",
            )
        assert isinstance(result, ProblemExtraction)

    def test_extractor_fallback_on_exception(self) -> None:
        """On exception, returns ProblemExtraction with body[:500]."""
        extractor = ProblemExtractor()
        body = "A" * 600
        # Patch dspy import to raise
        with patch.dict("sys.modules", {"dspy": None}):
            result = extractor.extract_problem(pr_title="test", pr_body=body, pr_comments="")
        assert isinstance(result, ProblemExtraction)
        assert len(result.initial_observations) <= 500

    def test_clean_text_handles_lists(self) -> None:
        extractor = ProblemExtractor()
        assert extractor._clean_text(["line one", "line two"]) == "line one\nline two"

    def test_clean_text_handles_null_strings(self) -> None:
        extractor = ProblemExtractor()
        assert extractor._clean_text("null") is None
        assert extractor._clean_text("None") is None
        assert extractor._clean_text("undefined") is None
        assert extractor._clean_text("N/A") is None

    def test_clean_text_handles_none(self) -> None:
        extractor = ProblemExtractor()
        assert extractor._clean_text(None) is None

    def test_build_extraction_rejects_short_fields(self) -> None:
        extractor = ProblemExtractor()
        prediction = MagicMock()
        prediction.initial_observations = "short"  # <20 chars
        prediction.triage_attempts = "tiny"  # <10 chars
        prediction.solution_overview = "x"  # <10 chars
        prediction.solution_observations = "y"  # <10 chars
        result = extractor._build_extraction(prediction)
        assert result.initial_observations == ""
        assert result.triage_attempts == ""
