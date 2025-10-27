"""Data models for PR report generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from datasmith.agents.problem_extractor import ProblemExtraction


@dataclass
class ReportResult:
    """Result of building a PR report.

    Attributes:
        report_md: Full markdown report text (problem-only)
        report_with_sol: Full markdown report text including a Solution section
        all_data: Raw data used to generate the report
        problem_statement: Extracted/summarized problem statement
        problem_statement_with_sol: Problem statement plus appended solution (when available)
        raw_problem_statement: Raw, non-LLM, non-anonymized problem statement extracted from PR body
        hints: Hints from comments and reviews
        classification: Performance optimization classification (if enabled)
        difficulty: Difficulty rating (if classification enabled)
        is_performance_commit: Whether this is identified as a performance-related commit
        classification_reason: Natural-language rationale from the classifier
        classification_confidence: Confidence score (0-100) if available
        problem_sections: Structured ``ProblemExtraction`` output when LLM summaries are enabled
        final_results: Individually rendered sections and the final assembled report
    """

    final_md: str
    final_md_no_hints: str = ""
    # PR metadata times (ISO strings as provided by GitHub)
    pr_created_at: str = ""
    pr_merged_at: str | None = None
    final_with_sol: str = ""
    all_data: dict[str, Any] = field(default_factory=dict)
    problem_statement: str = ""
    problem_statement_with_sol: str = ""
    raw_problem_statement: str = ""
    hints: str = ""
    classification: str = ""
    difficulty: str = ""
    is_performance_commit: bool = False
    classification_reason: str = ""
    classification_confidence: int | None = None
    problem_sections: ProblemExtraction | None = None
    # New: expose individually rendered sections for downstream consumers
    final_results: dict[str, str] = field(default_factory=dict)


@dataclass
class ReportContext:
    """Context rendered by the monolithic Jinja template.

    This provides a structured, typed object that the template can read
    directly via attribute access (e.g. ctx.problem_statement).
    """

    # Core sections
    problem_section_title: str = "Problem Statement"
    problem_extraction: ProblemExtraction | None = None
    git_problem_str: str = ""
    problem_statement: str = ""
    git_issue_str: str = ""

    # Optional solution
    include_solution: bool = False
    solution_overview: str = ""

    # Discussions / hints
    hints: str = ""

    # Performance and classification metadata
    performance_section: str = ""
    classification: str = ""
    difficulty: str = ""
    classification_reason: str = ""
    classification_confidence: int | None = None


@dataclass
class HintComment:
    """Single discussion item for the Hints section."""

    user_login: str
    timestamp: str  # pre-formatted via report_utils.iso
    body: str
    links: list[IssueExpanded] = field(default_factory=list)
    kind: str = "issue"  # one of: issue, review_comment, review
    created_at: str = ""  # raw GitHub ISO timestamp


@dataclass
class HintsContext:
    """Structured context for the Relevant Discussions (Hints) section."""

    items: list[HintComment] = field(default_factory=list)
    summary: str = ""
    prefer_summary: bool = False


# ----- GitHub link and issue/rendered content models -----


LinkType = Literal["pr", "issue", "commit"]


@dataclass
class LinkSummary:
    """Structured representation of a referenced GitHub resource.

    Derived from URLs seen in comments or bodies and enriched via GitHub API.
    """

    typ: LinkType
    owner: str
    repo: str
    ident: str  # number for PR/issue; SHA for commit
    url: str
    title: str | None = None  # PR/Issue title
    message: str | None = None  # Commit subject line
    short_sha: str | None = None  # 7-char short SHA for commits
    created_at: str | None = None
    merged_at: str | None = None
    closed_at: str | None = None


@dataclass(eq=True, frozen=True)
class IssueExpanded:
    """Expanded single-issue details used in the Referenced Issues section."""

    number: int
    title: str
    url: str
    description: str
    comments: tuple[str, ...] = field(default_factory=tuple)
    cross_references: tuple[str, ...] = field(default_factory=tuple)
    # Issue timestamps
    created_at: str | None = None
    closed_at: str | None = None

    def with_comments(self, comments: tuple[str]) -> IssueExpanded:
        """Return a new IssueExpanded with updated comments."""
        return IssueExpanded(
            number=self.number,
            title=self.title,
            url=self.url,
            description=self.description,
            comments=comments,
            cross_references=self.cross_references,
            created_at=self.created_at,
            closed_at=self.closed_at,
        )


@dataclass
class PRFileChange:
    filename: str
    status: str
    additions: int
    deletions: int
    changes: int
    previous_filename: str | None = None


@dataclass
class PRChangeSummary:
    files: list[PRFileChange] = field(default_factory=list)
    total_additions: int = 0
    total_deletions: int = 0
    total_changes: int = 0
