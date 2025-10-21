"""Data models for PR report generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

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
        hints: Hints from comments and reviews
        classification: Performance optimization classification (if enabled)
        difficulty: Difficulty rating (if classification enabled)
        is_performance_commit: Whether this is identified as a performance-related commit
        classification_reason: Natural-language rationale from the classifier
        classification_confidence: Confidence score (0-100) if available
        problem_sections: Structured ``ProblemExtraction`` output when LLM summaries are enabled
    """

    report_md: str
    report_with_sol: str = ""
    all_data: dict[str, Any] = field(default_factory=dict)
    problem_statement: str = ""
    problem_statement_with_sol: str = ""
    hints: str = ""
    classification: str = ""
    difficulty: str = ""
    is_performance_commit: bool = False
    classification_reason: str = ""
    classification_confidence: int | None = None
    problem_sections: ProblemExtraction | None = None
