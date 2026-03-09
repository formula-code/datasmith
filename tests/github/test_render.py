"""Tests for datasmith.github.render — problem statement rendering and anonymization."""

from __future__ import annotations

from datasmith.github.models import PR, IssueExpanded
from datasmith.github.render import Anonymizer, render_problem_statement


class TestAnonymizer:
    def test_anonymize_usernames(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("Thanks @alice and @bob!")
        assert "@alice" not in result
        assert "@bob" not in result
        assert "@user_1" in result
        assert "@user_2" in result

    def test_anonymize_consistent(self) -> None:
        """Same username always maps to same placeholder."""
        anon = Anonymizer()
        result = anon.anonymize("@alice said hi, then @alice said bye")
        # Both occurrences of @alice should become the same placeholder
        assert result.count("@user_1") == 2

    def test_strips_emails(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("Contact me at alice@example.com or bob@test.org")
        assert "alice@example.com" not in result
        assert "bob@test.org" not in result
        assert "[email]" in result

    def test_no_change_without_pii(self) -> None:
        anon = Anonymizer()
        text = "This has no PII at all"
        assert anon.anonymize(text) == text


class TestRenderProblemStatement:
    def test_render_basic(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="Speed up sorting",
            body="This PR speeds up sorting by 2x using a better algorithm.",
        )
        rendered = render_problem_statement(pr)
        assert "This PR speeds up sorting by 2x" in rendered
        assert "performance optimization expert" in rendered
        assert "ASV" in rendered

    def test_render_with_repo_description(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Improved parsing",
        )
        rendered = render_problem_statement(pr, repo_description="A fast data library")
        assert "A fast data library" in rendered
        assert "Repository Description" in rendered

    def test_render_without_repo_description(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Some changes",
        )
        rendered = render_problem_statement(pr, repo_description="")
        assert "Repository Description" not in rendered

    def test_render_anonymize_usernames(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Thanks @contributor for the review, cc @maintainer",
        )
        rendered = render_problem_statement(pr, anonymize=True)
        assert "@contributor" not in rendered
        assert "@maintainer" not in rendered
        assert "@user_" in rendered

    def test_render_anonymize_consistent(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="@alice wrote this. @alice also reviewed it.",
        )
        rendered = render_problem_statement(pr, anonymize=True)
        # Count occurrences of the anonymized username
        assert rendered.count("@user_1") == 2

    def test_render_strips_emails(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Contact dev@example.com for details",
        )
        rendered = render_problem_statement(pr, anonymize=True)
        assert "dev@example.com" not in rendered
        assert "[email]" in rendered

    def test_render_includes_linked_issues(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Main optimization PR",
        )
        issues = [
            IssueExpanded(
                number=10,
                title="Slow groupby",
                description="Groupby is too slow on large datasets",
                comments=["I see the same issue", "Confirmed"],
                cross_references=["#11"],
            ),
            IssueExpanded(
                number=11,
                title="Related perf regression",
                description="Performance regressed in v2.0",
            ),
        ]
        rendered = render_problem_statement(pr, issues=issues)
        assert "Relevant Issues" in rendered
        assert "Slow groupby" in rendered
        assert "Related perf regression" in rendered
        assert "Groupby is too slow" in rendered
        assert "I see the same issue" in rendered
        assert "#11" in rendered

    def test_render_no_issues_section_when_empty(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Just a fix",
        )
        rendered = render_problem_statement(pr, issues=[])
        assert "Relevant Issues" not in rendered

    def test_render_no_issues_section_when_none(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Just a fix",
        )
        rendered = render_problem_statement(pr, issues=None)
        assert "Relevant Issues" not in rendered
