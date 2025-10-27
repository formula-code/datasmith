"""Tests for the refactored report builder components."""

from __future__ import annotations

import pytest

from datasmith.scrape.issue_extractor import _extract_issue_refs
from datasmith.scrape.models import ReportResult


class TestIssueExtractor:
    """Tests for issue reference extraction."""

    def test_extract_same_repo_issue(self):
        """Test extraction of same-repo issue reference (#123)."""
        text = "This fixes #123 and resolves #456"
        refs = _extract_issue_refs(text, "owner", "repo")

        assert len(refs) == 2
        assert ("owner", "repo", "123") in refs
        assert ("owner", "repo", "456") in refs

    def test_extract_cross_repo_issue(self):
        """Test extraction of cross-repo issue reference (owner/repo#123)."""
        text = "Related to dedupeio/dedupe#997"
        refs = _extract_issue_refs(text, "default_owner", "default_repo")

        assert len(refs) == 1
        assert ("dedupeio", "dedupe", "997") in refs

    def test_extract_full_url_issue(self):
        """Test extraction of full GitHub issue URL."""
        text = "See https://github.com/pandas-dev/pandas/issues/51207"
        refs = _extract_issue_refs(text, "default_owner", "default_repo")

        assert len(refs) == 1
        assert ("pandas-dev", "pandas", "51207") in refs

    def test_extract_full_url_pull(self):
        """Test extraction of full GitHub PR URL."""
        text = "See https://github.com/pandas-dev/pandas/pull/51207"
        refs = _extract_issue_refs(text, "default_owner", "default_repo")

        # PR URLs are included in extraction (filtering happens later based on merged status)
        assert len(refs) == 1
        assert ("pandas-dev", "pandas", "51207") in refs

    def test_extract_mixed_formats(self):
        """Test extraction of mixed reference formats."""
        text = """
        Fixes #123
        Related to owner/repo#456
        See also https://github.com/org/project/issues/789
        """
        refs = _extract_issue_refs(text, "default_owner", "default_repo")

        assert len(refs) == 3
        assert ("default_owner", "default_repo", "123") in refs
        assert ("owner", "repo", "456") in refs
        assert ("org", "project", "789") in refs

    def test_extract_with_closing_keywords(self):
        """Test that closing keywords are properly handled."""
        text = "Closes #123, fixes owner/repo#456, resolves https://github.com/org/proj/issues/789"
        refs = _extract_issue_refs(text, "default", "default")

        assert len(refs) == 3

    def test_ignore_markdown_headings(self):
        """Test that markdown headings (##123) are not extracted."""
        text = """
        ## Issue #123

        This fixes #123
        """
        refs = _extract_issue_refs(text, "owner", "repo")

        # Should only find one #123, not the heading
        assert len(refs) == 1
        assert ("owner", "repo", "123") in refs

    def test_deduplication(self):
        """Test that duplicate references are deduplicated."""
        text = "Fixes #123, closes #123, resolves #123"
        refs = _extract_issue_refs(text, "owner", "repo")

        assert len(refs) == 1
        assert ("owner", "repo", "123") in refs

    def test_empty_text(self):
        """Test extraction from empty text."""
        refs = _extract_issue_refs("", "owner", "repo")
        assert len(refs) == 0

    def test_no_references(self):
        """Test extraction when no references exist."""
        text = "This is just a description with no issue references"
        refs = _extract_issue_refs(text, "owner", "repo")
        assert len(refs) == 0

    def test_none_description(self):
        """Test extraction when description is None."""
        from datasmith.scrape.issue_extractor import extract_issues_from_description

        issue_data = extract_issues_from_description(None, "owner", "repo")
        assert len(issue_data) == 0


class TestReportResult:
    """Tests for ReportResult dataclass."""

    def test_report_result_creation(self):
        """Test creating a ReportResult instance."""
        result = ReportResult(
            final_md="# Test Report",
            problem_statement="Test problem",
            hints="Test hints",
            classification="Better algorithm",
            difficulty="medium",
            is_performance_commit=True,
        )

        assert result.final_md == "# Test Report"
        assert result.problem_statement == "Test problem"
        assert result.hints == "Test hints"
        assert result.classification == "Better algorithm"
        assert result.difficulty == "medium"
        assert result.is_performance_commit is True
        assert result.all_data == {}
        assert result.classification_reason == ""
        assert result.classification_confidence is None
        assert result.problem_sections is None

    def test_report_result_empty_values(self):
        """Test ReportResult with empty values."""
        result = ReportResult(
            final_md="NOT_A_VALID_PR",
            problem_statement="",
            hints="",
            classification="",
            difficulty="",
            is_performance_commit=False,
        )

        assert result.final_md == "NOT_A_VALID_PR"
        assert result.problem_statement == ""
        assert result.is_performance_commit is False
        assert result.classification_reason == ""
        assert result.classification_confidence is None


class TestReportBuilder:
    """Tests for ReportBuilder class."""

    def test_report_builder_initialization(self):
        """Test ReportBuilder initialization with default parameters."""
        from datasmith.scrape.report_builder import ReportBuilder

        rb = ReportBuilder()

        assert rb.enable_llm_backends is False
        assert rb.summarize_llm is False
        assert rb.add_classification is False
        assert rb.max_links_to_follow == 60

    def test_report_builder_custom_config(self):
        """Test ReportBuilder initialization with custom parameters."""
        from datasmith.scrape.report_builder import ReportBuilder

        rb = ReportBuilder(
            enable_llm_backends=False,
            filter_performance_only=False,
            include_bot_comments=True,
            anonymize_output=False,
            max_links_to_follow=100,
            model_name="custom-model",
        )

        assert rb.filter_performance_only is False
        assert rb.include_bot_comments is True
        assert rb.anonymize_output is False
        assert rb.max_links_to_follow == 100
        assert rb.model_name == "custom-model"

    def test_config_validation_summarize_llm(self):
        """Test that summarize_llm requires enable_llm_backends."""
        from datasmith.scrape.report_builder import ReportBuilder

        with pytest.raises(ValueError, match="summarize_llm=True requires enable_llm_backends=True"):
            ReportBuilder(enable_llm_backends=False, summarize_llm=True)

    def test_config_validation_add_classification(self):
        """Test that add_classification requires enable_llm_backends."""
        from datasmith.scrape.report_builder import ReportBuilder

        with pytest.raises(ValueError, match="add_classification=True requires enable_llm_backends=True"):
            ReportBuilder(enable_llm_backends=False, add_classification=True)

    def test_config_validation_filter_performance(self):
        """Test that filter_performance_only requires enable_llm_backends."""
        from datasmith.scrape.report_builder import ReportBuilder

        with pytest.raises(ValueError, match="filter_performance_only=True requires enable_llm_backends=True"):
            ReportBuilder(enable_llm_backends=False, filter_performance_only=True)

    def test_iso_format(self):
        """Test timestamp formatting."""
        from datasmith.scrape.report_builder import ReportBuilder

        rb = ReportBuilder()

        ts = "2022-05-24T13:48:06Z"
        formatted = rb._iso_format(ts)

        # Should be in format HH:MM DD/MM/YYYY
        assert "13:48" in formatted
        assert "24/05/2022" in formatted

    def test_extract_links(self):
        """Test link extraction from text."""
        from datasmith.scrape.report_builder import ReportBuilder

        rb = ReportBuilder()

        text = "See https://example.com and http://test.org for more info"
        links = rb._extract_links(text)

        assert len(links) == 2
        assert "https://example.com" in links
        assert "http://test.org" in links

    def test_build_missing_pr_url(self):
        """Test that build() raises ValueError when pr_url is missing."""
        from datasmith.scrape.report_builder import ReportBuilder

        rb = ReportBuilder()

        with pytest.raises(ValueError, match="pr_dict must contain 'pr_url'"):
            rb.build(pr_dict={})
