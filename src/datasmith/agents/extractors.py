"""
ProblemExtractor: Extractive-first approach for problem statement generation.

Key principles:
- 90% extractive, 10% abstractive
- Preserve code snippets verbatim (character-exact)
- Keep technical terms exactly as written
- Natural structure over imposed templates
- Preserve disagreements and different viewpoints
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from datasmith.utils import get_logger

logger = get_logger("agents.extractors")


@dataclass
class ProblemExtraction:
    """Structured extraction from a PR description.

    Captures four phases of a performance optimization or bug fix:
    1. initial_observations: Objective symptoms of the problematic behavior
    2. triage_attempts: Investigative steps and reasoning used to narrow down the issue
    3. solution_overview: Description of the change(s) made
    4. solution_observations: Observations after applying the change
    """

    initial_observations: str = ""
    triage_attempts: str = ""
    solution_overview: str = ""
    solution_observations: str = ""

    def to_problem_markdown(self) -> str:
        """Render only the problem portion (initial observations)."""
        text = (self.initial_observations or "").strip()
        text = re.sub(r"^```json\s*$", "", text, flags=re.MULTILINE)
        return text

    def _normalise_section(self, content: str | None, header_variants: list[str]) -> str | None:
        """Remove redundant headers from section content."""
        if not content:
            return None
        content = content.strip()
        low = content.lstrip().lower()
        for variant in header_variants:
            if low.startswith(variant.lower()):
                lines = content.splitlines()
                if lines:
                    lines = lines[1:]
                content = "\n".join(lines).lstrip()
                break
        return content

    def to_full_markdown(self) -> str:
        """Render all sections with headers."""
        sections: list[str] = []

        initial_obs = (self.initial_observations or "").strip()
        if initial_obs:
            sections.append(initial_obs)

        triage = self._normalise_section(
            self.triage_attempts,
            ["## triage attempts", "**triage attempts**"],
        )
        if triage:
            sections.append(f"## Triage Attempts\n\n{triage}")

        solution = self._normalise_section(
            self.solution_overview,
            ["## solution overview", "**solution overview**"],
        )
        if solution:
            sections.append(f"## Solution Overview\n\n{solution}")

        solution_obs = self._normalise_section(
            self.solution_observations,
            ["## solution observations", "**solution observations**"],
        )
        if solution_obs:
            sections.append(f"## Solution Observations\n\n{solution_obs}")

        text = "\n\n".join(sections).strip()
        text = re.sub(r"^```json\s*$", "", text, flags=re.MULTILINE)
        return text

    def to_dict(self) -> dict[str, str]:
        return {
            "initial_observations": self.initial_observations,
            "triage_attempts": self.triage_attempts,
            "solution_overview": self.solution_overview,
            "solution_observations": self.solution_observations,
        }


class ProblemExtractor:
    """Extractive problem/solution bucketizer using DSPy."""

    def __init__(self) -> None:
        self._predictor: Any | None = None

    def _get_predictor(self) -> Any:
        if self._predictor is None:
            from datasmith.agents.config import ensure_configured

            ensure_configured()
            import dspy

            class ProblemExtractorSignature(dspy.Signature):
                """What problem is this Github PR trying to solve? Extract near-verbatim relevant text following the given JSON output. If no relevant context exists for a field, return an empty string for it."""

                pr_title: str = dspy.InputField(desc="The GitHub PR title")
                pr_body: str = dspy.InputField(desc="The GitHub PR description")
                pr_comments: str = dspy.InputField(desc="Comments on the PR thread.")
                initial_observations: str = dspy.OutputField(
                    desc="Objective symptoms of the problematic behavior, described in the present tense. Focus strictly on what is happening (metrics, user impact, frequency). Do not include causes, hypotheses, or explanations."
                )
                triage_attempts: str = dspy.OutputField(
                    desc="The investigative steps and reasoning used to narrow down contributing factors—what you checked, what you ruled out, and what evidence you gathered to understand where the issue originates."
                )
                solution_overview: str = dspy.OutputField(
                    desc="A concise description of the change(s) made and how they address the identified bottleneck or constraint."
                )
                solution_observations: str = dspy.OutputField(
                    desc="What you observe after applying the change—new measurements, behavior differences, and any regressions or trade-offs that appeared."
                )

            self._predictor = dspy.Predict(ProblemExtractorSignature)
        return self._predictor

    def extract_problem(self, pr_title: str, pr_body: str, pr_comments: str = "") -> ProblemExtraction:
        try:
            predictor = self._get_predictor()
            result = predictor(pr_title=pr_title, pr_body=pr_body, pr_comments=pr_comments)
            return self._build_extraction(result)
        except Exception:
            logger.exception("Problem extraction failed, returning empty")
            return ProblemExtraction(initial_observations=pr_body[:500] if pr_body else "")

    def _clean_text(self, value: Any | None) -> str | None:
        """Clean and normalize text values from predictions."""
        if value is None:
            return None
        if isinstance(value, list):
            try:
                flat: list[str] = []
                for v in value:
                    if isinstance(v, list):
                        flat.extend(str(x) for x in v)
                    else:
                        flat.append(str(v))
                value = "\n".join(flat)
            except Exception:
                value = "\n".join(str(v) for v in value)
        if not isinstance(value, str):
            value = str(value)
        stripped = value.strip()
        if stripped.lower() in {"null", "none", "undefined", "n/a", ""}:
            return None
        return stripped or None

    def _build_extraction(self, prediction: Any) -> ProblemExtraction:
        """Normalize the raw DSPy prediction into a ProblemExtraction."""
        initial_obs = self._clean_text(getattr(prediction, "initial_observations", None))
        triage = self._clean_text(getattr(prediction, "triage_attempts", None))
        solution = self._clean_text(getattr(prediction, "solution_overview", None))
        solution_obs = self._clean_text(getattr(prediction, "solution_observations", None))

        def plausible(s: str | None, *, min_len: int = 20) -> bool:
            if s is None:
                return False
            stripped = s.strip()
            if len(stripped) < min_len:
                return False
            return bool(re.search(r"[A-Za-z]", stripped))

        if not plausible(initial_obs, min_len=20):
            initial_obs = None
        if not plausible(triage, min_len=10):
            triage = None
        if not plausible(solution, min_len=10):
            solution = None
        if not plausible(solution_obs, min_len=10):
            solution_obs = None

        return ProblemExtraction(
            initial_observations=initial_obs or "",
            triage_attempts=triage or "",
            solution_overview=solution or "",
            solution_observations=solution_obs or "",
        )
