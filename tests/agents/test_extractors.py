"""Tests for datasmith.agents.extractors."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.agents.extractors import ProblemExtraction, ProblemExtractor, _clean_section, _is_null


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
        """Verify markdown headers present for problem sections only."""
        ext = ProblemExtraction(
            initial_observations="The system is slow",
            triage_attempts="Profiled the hotloop",
        )
        md = ext.to_problem_markdown()
        assert "## Initial Observations" in md
        assert "## Triage Attempts" in md
        assert "The system is slow" in md
        assert "Profiled the hotloop" in md
        # Solution sections should NOT be present
        assert "## Solution Overview" not in md
        assert "## Solution Observations" not in md

    def test_to_full_markdown_includes_solution(self) -> None:
        """Verify all 4 sections appear in full markdown."""
        ext = ProblemExtraction(
            initial_observations="obs text here",
            triage_attempts="triage text here",
            solution_overview="overview text here",
            solution_observations="sol obs text here",
        )
        md = ext.to_full_markdown()
        assert "## Initial Observations" in md
        assert "## Triage Attempts" in md
        assert "## Solution Overview" in md
        assert "## Solution Observations" in md

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


class TestCleanSection:
    def test_clean_section_removes_headers(self) -> None:
        """Verify header cleanup removes known section headers."""
        text = "## Initial Observations\nThis is a long enough observation string to pass."
        cleaned = _clean_section(text)
        assert not cleaned.startswith("## Initial Observations")
        assert "long enough" in cleaned

    def test_clean_section_rejects_short(self) -> None:
        """Text <20 chars returns empty string."""
        assert _clean_section("short") == ""
        assert _clean_section("12345678901234567") == ""

    def test_clean_section_empty(self) -> None:
        assert _clean_section("") == ""

    def test_clean_section_no_alpha(self) -> None:
        """Strings with no alphabetic chars return empty."""
        assert _clean_section("1234567890123456789012345") == ""


class TestIsNull:
    def test_is_null_values(self) -> None:
        """'null', 'none', 'n/a' and '' are detected as null."""
        assert _is_null("null") is True
        assert _is_null("None") is True
        assert _is_null("N/A") is True
        assert _is_null("") is True
        assert _is_null("  null  ") is True
        assert _is_null("actual content") is False


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
