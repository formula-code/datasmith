from __future__ import annotations

import re
from dataclasses import dataclass

from datasmith.utils import get_logger

logger = get_logger("agents.extractors")


@dataclass
class ProblemExtraction:
    """Structured extraction from a PR description."""

    initial_observations: str = ""
    triage_attempts: str = ""
    solution_overview: str = ""
    solution_observations: str = ""

    def to_problem_markdown(self) -> str:
        parts: list[str] = []
        if self.initial_observations:
            parts.append(f"## Initial Observations\n\n{self.initial_observations}")
        if self.triage_attempts:
            parts.append(f"## Triage Attempts\n\n{self.triage_attempts}")
        return "\n\n".join(parts) if parts else ""

    def to_full_markdown(self) -> str:
        parts: list[str] = []
        if self.initial_observations:
            parts.append(f"## Initial Observations\n\n{self.initial_observations}")
        if self.triage_attempts:
            parts.append(f"## Triage Attempts\n\n{self.triage_attempts}")
        if self.solution_overview:
            parts.append(f"## Solution Overview\n\n{self.solution_overview}")
        if self.solution_observations:
            parts.append(f"## Solution Observations\n\n{self.solution_observations}")
        return "\n\n".join(parts) if parts else ""

    def to_dict(self) -> dict[str, str]:
        return {
            "initial_observations": self.initial_observations,
            "triage_attempts": self.triage_attempts,
            "solution_overview": self.solution_overview,
            "solution_observations": self.solution_observations,
        }


def _clean_section(text: str) -> str:
    """Remove redundant headers and clean a section string."""
    if not text:
        return ""
    # Remove section headers that match known patterns
    text = re.sub(
        r"^##\s*(Initial Observations|Triage Attempts|Solution Overview|Solution Observations)\s*\n",
        "",
        text,
        flags=re.MULTILINE,
    )
    text = text.strip()
    return text if len(text) >= 20 and any(c.isalpha() for c in text) else ""


def _is_null(value: str) -> bool:
    return value.lower().strip() in ("null", "none", "n/a", "")


class ProblemExtractor:
    """Extract structured problem/solution from PR text using DSPy."""

    def extract_problem(self, pr_title: str, pr_body: str, pr_comments: str = "") -> ProblemExtraction:
        try:
            import dspy

            class ProblemExtractorSignature(dspy.Signature):
                """Extract a structured problem description from a pull request."""

                pr_title: str = dspy.InputField(desc="Title of the pull request")
                pr_body: str = dspy.InputField(desc="Body/description of the pull request")
                pr_comments: str = dspy.InputField(desc="Comments on the pull request")
                initial_observations: str = dspy.OutputField(desc="What the problem is and initial observations")
                triage_attempts: str = dspy.OutputField(desc="Attempts to diagnose or triage the issue")
                solution_overview: str = dspy.OutputField(desc="High-level overview of the solution")
                solution_observations: str = dspy.OutputField(desc="Detailed observations about the solution")

            predictor = dspy.Predict(ProblemExtractorSignature)
            result = predictor(pr_title=pr_title, pr_body=pr_body, pr_comments=pr_comments)

            return ProblemExtraction(
                initial_observations=_clean_section(result.initial_observations)
                if not _is_null(result.initial_observations)
                else "",
                triage_attempts=_clean_section(result.triage_attempts) if not _is_null(result.triage_attempts) else "",
                solution_overview=_clean_section(result.solution_overview)
                if not _is_null(result.solution_overview)
                else "",
                solution_observations=_clean_section(result.solution_observations)
                if not _is_null(result.solution_observations)
                else "",
            )
        except Exception:
            logger.exception("Problem extraction failed, returning empty")
            return ProblemExtraction(initial_observations=pr_body[:500] if pr_body else "")
