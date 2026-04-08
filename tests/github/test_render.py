"""Tests for datasmith.github.render — problem statement rendering and anonymization."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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

    def test_github_profile_url(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("See https://github.com/alice for details")
        assert "alice" not in result
        assert "https://github.com/user_1" in result

    def test_github_repo_url(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("Check https://github.com/alice/myrepo/issues/42")
        assert "/alice/" not in result
        assert "https://github.com/user_1/myrepo/issues/42" in result

    def test_github_url_consistent_with_mention(self) -> None:
        """@alice and github.com/alice should map to the same placeholder."""
        anon = Anonymizer()
        result = anon.anonymize("@alice opened https://github.com/alice/repo")
        assert result.count("user_1") == 2

    def test_signoff(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("Signed-off-by: John Smith <john@example.com>")
        assert "John Smith" not in result
        assert "john@example.com" not in result
        assert "[name]" in result
        assert "[email]" in result

    def test_coauthored_by(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("Co-authored-by: Alice Jones <alice@test.org>")
        assert "Alice Jones" not in result
        assert "alice@test.org" not in result
        assert "Co-authored-by: [name] <[email]>" in result

    def test_user_images_url(self) -> None:
        anon = Anonymizer()
        url = "https://user-images.githubusercontent.com/12345/abcdef-image.png"
        result = anon.anonymize(f"Screenshot: ![img]({url})")
        assert "12345" not in result
        assert "[image]" in result

    def test_home_path_linux(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("File at /home/alice/project/foo.py")
        assert "/home/alice/" not in result
        assert "/home/user_1/" in result

    def test_home_path_macos(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("File at /Users/bob/code/bar.py")
        assert "/Users/bob/" not in result
        assert "/Users/user_1/" in result

    def test_home_path_windows(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize(r"Path is C:\Users\carol\Documents\test.py")
        assert "carol" not in result
        assert r"C:\Users\user_1" in result

    def test_ip_address(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("Server at 192.168.1.42 crashed")
        assert "192.168.1.42" not in result
        assert "[ip]" in result

    def test_ip_address_preserves_non_ips(self) -> None:
        anon = Anonymizer()
        result = anon.anonymize("Version 3.9.1 is out")
        assert result == "Version 3.9.1 is out"

    def test_known_bare_usernames(self) -> None:
        anon = Anonymizer(known_usernames={"alice"})
        result = anon.anonymize("alice reported the issue")
        assert "alice" not in result
        assert "user_1" in result

    def test_known_usernames_case_insensitive(self) -> None:
        anon = Anonymizer(known_usernames={"Alice"})
        result = anon.anonymize("ALICE and alice both matched")
        assert "ALICE" not in result
        assert "alice" not in result

    def test_known_usernames_consistent_with_mentions(self) -> None:
        """@alice and bare 'alice' (via known_usernames) get the same placeholder."""
        anon = Anonymizer(known_usernames={"alice"})
        result = anon.anonymize("@alice said something. Later alice said more.")
        # Both should map to user_1
        assert result == "@user_1 said something. Later user_1 said more."

    def test_combined_pii(self) -> None:
        """Regression test: multiple PII types in one block of text."""
        anon = Anonymizer(known_usernames={"charlie"})
        text = (
            "@alice reported a bug from /home/alice/project\n"
            "charlie confirmed at 10.0.0.1\n"
            "See https://github.com/bob/repo/issues/5\n"
            "Contact support@example.com\n"
            "Signed-off-by: Dave Lee <dave@corp.io>\n"
            "![screenshot](https://user-images.githubusercontent.com/999/img.png)"
        )
        result = anon.anonymize(text)
        for leak in [
            "alice",
            "bob",
            "charlie",
            "Dave Lee",
            "dave@corp.io",
            "support@example.com",
            "10.0.0.1",
            "999/img.png",
        ]:
            assert leak not in result, f"'{leak}' leaked through anonymization"
        assert "@user_" in result
        assert "[email]" in result
        assert "[name]" in result
        assert "[ip]" in result
        assert "[image]" in result


class TestRenderProblemStatement:
    def test_render_basic(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="Speed up sorting",
            body="This PR speeds up sorting by 2x using a better algorithm.",
        )
        rendered = render_problem_statement(pr, extract=False)
        assert "This PR speeds up sorting by 2x" in rendered
        assert "performance optimization expert" in rendered
        assert "ASV" in rendered

    def test_render_with_repo_description(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Improved parsing",
        )
        rendered = render_problem_statement(pr, repo_description="A fast data library", extract=False)
        assert "A fast data library" in rendered
        assert "Repository Description" in rendered

    def test_render_without_repo_description(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Some changes",
        )
        rendered = render_problem_statement(pr, repo_description="", extract=False)
        assert "Repository Description" not in rendered

    def test_render_anonymize_usernames(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Thanks @contributor for the review, cc @maintainer",
        )
        rendered = render_problem_statement(pr, anonymize=True, extract=False)
        assert "@contributor" not in rendered
        assert "@maintainer" not in rendered
        assert "@user_" in rendered

    def test_render_anonymize_consistent(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="@alice wrote this. @alice also reviewed it.",
        )
        rendered = render_problem_statement(pr, anonymize=True, extract=False)
        # Count occurrences of the anonymized username
        assert rendered.count("@user_1") == 2

    def test_render_strips_emails(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Contact dev@example.com for details",
        )
        rendered = render_problem_statement(pr, anonymize=True, extract=False)
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
        rendered = render_problem_statement(pr, issues=issues, extract=False)
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
        rendered = render_problem_statement(pr, issues=[], extract=False)
        assert "Relevant Issues" not in rendered

    def test_render_anonymize_known_usernames(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="alice optimized the loop, see https://github.com/alice/repo",
        )
        rendered = render_problem_statement(
            pr,
            anonymize=True,
            known_usernames={"alice"},
            extract=False,
        )
        assert "alice" not in rendered
        assert "user_1" in rendered

    def test_render_no_issues_section_when_none(self) -> None:
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            body="Just a fix",
        )
        rendered = render_problem_statement(pr, issues=None, extract=False)
        assert "Relevant Issues" not in rendered

    def test_render_extract_false_uses_raw_body(self) -> None:
        """With extract=False, ProblemExtractor is NOT called and raw body is used."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="Speed up sorting",
            body="Raw body content here.",
        )
        with patch("datasmith.agents.extractors.ProblemExtractor") as mock_cls:
            rendered = render_problem_statement(pr, extract=False)
            mock_cls.assert_not_called()
        assert "Raw body content here." in rendered

    def test_render_extract_true_calls_extractor(self) -> None:
        """With extract=True (default), ProblemExtractor is called and its output used."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="Speed up sorting",
            body="Raw body — should NOT appear.",
        )
        mock_extraction = MagicMock()
        mock_extraction.to_problem_markdown.return_value = "Extracted problem observations only."

        mock_extractor = MagicMock()
        mock_extractor.extract_problem.return_value = mock_extraction

        with patch("datasmith.agents.extractors.ProblemExtractor", return_value=mock_extractor):
            rendered = render_problem_statement(pr, extract=True)

        assert "Extracted problem observations only." in rendered
        assert "Raw body — should NOT appear." not in rendered

    def test_render_extract_fallback_on_failure(self) -> None:
        """If ProblemExtractor raises, falls back to raw PR body."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="Speed up sorting",
            body="Fallback body content.",
        )
        with patch("datasmith.agents.extractors.ProblemExtractor", side_effect=RuntimeError("LLM unavailable")):
            rendered = render_problem_statement(pr, extract=True)

        assert "Fallback body content." in rendered
