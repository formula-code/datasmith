"""Data models for PR report generation."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReportResult:
    """Result of building a PR report.

    Attributes:
        report_md: Full markdown report text
        problem_statement: Extracted/summarized problem statement
        hints: Hints from comments and reviews
        classification: Performance optimization classification (if enabled)
        difficulty: Difficulty rating (if classification enabled)
        is_performance_commit: Whether this is identified as a performance-related commit
    """

    report_md: str
    report_data: dict
    problem_statement: str
    hints: str
    classification: str
    difficulty: str
    is_performance_commit: bool
