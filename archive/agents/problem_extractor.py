"""
ProblemExtractor: Extractive-first approach for problem statement generation.

This module replaces the abstractive LLMStructurer and LLMCommentSummarizer
with an extractive approach that maintains 75%+ LCS (Longest Common Subsequence)
ratio with source material.

Key principles:
- 90% extractive, 10% abstractive
- Preserve code snippets verbatim (character-exact)
- Keep technical terms exactly as written
- Natural structure over imposed templates
- Preserve disagreements and different viewpoints
"""

import re
from dataclasses import dataclass
from typing import Any

import dspy

from datasmith.logging_config import configure_logging

logger = configure_logging()


@dataclass
class ProblemExtraction:
    """
    Structured representation of an extracted problem and solution narrative.

    This dataclass captures the four phases of a performance optimization or bug fix:
    1. initial_observations: Objective symptoms of the problematic behavior
    2. triage_attempts: Investigative steps and reasoning used to narrow down the issue
    3. solution_overview: Description of the change(s) made
    4. solution_observations: Observations after applying the change

    Notes on rendering:
    - Use ``to_problem_markdown`` for the problem-only variant (initial_observations).
    - Use ``to_problem_with_solution_markdown`` to include all sections with headers.
    - ``to_markdown`` remains for backward-compatibility and includes all sections.
    """

    initial_observations: str | None = None
    triage_attempts: str | None = None
    solution_overview: str | None = None
    solution_observations: str | None = None

    def _strip(self, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None

    def to_problem_markdown(self) -> str:
        """Render only the problem portion in markdown (initial observations only)."""
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

    def to_problem_with_solution_markdown(self) -> str:
        """Render problem + all solution sections with headers."""
        sections: list[str] = []

        # Initial observations (no header, rendered as-is)
        initial_obs = (self.initial_observations or "").strip()
        if initial_obs:
            sections.append(initial_obs)

        # Triage attempts
        triage = self._normalise_section(self.triage_attempts, ["## triage attempts", "**triage attempts**"])
        if triage:
            sections.append(f"## Triage Attempts\n\n{triage}")

        # Solution overview
        solution = self._normalise_section(self.solution_overview, ["## solution overview", "**solution overview**"])
        if solution:
            sections.append(f"## Solution Overview\n\n{solution}")

        # Solution observations
        solution_obs = self._normalise_section(
            self.solution_observations, ["## solution observations", "**solution observations**"]
        )
        if solution_obs:
            sections.append(f"## Solution Observations\n\n{solution_obs}")

        text = "\n\n".join(sections).strip()
        text = re.sub(r"^```json\s*$", "", text, flags=re.MULTILINE)
        return text

    def to_markdown(self) -> str:
        """Backward-compatible rendering that includes solution when present."""
        return self.to_problem_with_solution_markdown()

    def to_dict(self) -> dict[str, Any]:
        """Return a serialisable representation of the extraction."""

        return {
            "initial_observations": self._strip(self.initial_observations),
            "triage_attempts": self._strip(self.triage_attempts),
            "solution_overview": self._strip(self.solution_overview),
            "solution_observations": self._strip(self.solution_observations),
        }


class ProblemExtractorSignature(dspy.Signature):
    """
    What problem is this Github PR trying to solve? Extract near-verbatim relevant text following the given JSON output. If no relevant context exists for a field, return an empty string for it.
    """

    # Inputs
    pr_title: str = dspy.InputField(desc="The GitHub PR title")
    pr_body: str = dspy.InputField(desc="The GitHub PR description")
    pr_comments: str = dspy.InputField(desc="Comments on the PR thread.")
    # pr_patch: str | None = dspy.InputField(desc="The GitHub PR code patch/diff.")

    initial_observations: str | list[Any] | None = dspy.OutputField(
        desc="Objective symptoms of the problematic behavior, described in the present tense. Focus strictly on what is happening (metrics, user impact, frequency). Do not include causes, hypotheses, or explanations."
    )
    triage_attempts: str | list[Any] | None = dspy.OutputField(
        desc="The investigative steps and reasoning used to narrow down contributing factors—what you checked, what you ruled out, and what evidence you gathered to understand where the issue originates."
    )
    solution_overview: str | list[Any] | None = dspy.OutputField(
        desc="A concise description of the change(s) made and how they address the identified bottleneck or constraint."
    )
    solution_observations: str | list[Any] | None = dspy.OutputField(
        desc="What you observe after applying the change—new measurements, behavior differences, and any regressions or trade-offs that appeared."
    )


class ProblemExtractor(dspy.Module):
    """
    Extractive problem/solution bucketizer and discussion extractor with LCS validation.

    This module uses the updated signatures:
      - ProblemExtractorSignature(pr_body, pr_comments)

    Usage:
        extractor = ProblemExtractor(validate_lcs=True, min_lcs=0.75)

        # Extract Problem/Solution buckets from a PR
        extraction = extractor.extract_problem(
            pr_title=pr_title_text,
            pr_body=pr_body_text,
            pr_comments=all_pr_comments_text,
            # pr_patch=pr_patch_text,
        )
    """

    def __init__(self) -> None:
        super().__init__()
        self.problem_predictor = dspy.Predict(ProblemExtractorSignature)

    # ---------- PUBLIC API ----------

    def extract_problem(
        self,
        *,
        pr_title: str,
        pr_body: str,
        pr_comments: str,
        # pr_patch: str | None = None,
    ) -> ProblemExtraction:
        """
        Extract Problem/Solution buckets from a GitHub PR + comments.

        Args:
            pr_title: The GitHub PR title
            pr_body: The GitHub PR/issue description
            pr_comments: Comments on the PR thread
            # pr_patch: The GitHub PR code patch/diff

        Returns:
            ProblemExtraction
        """
        # Run extraction with UPDATED input arg names
        try:
            result = self.problem_predictor(
                pr_title=pr_title,
                pr_body=pr_body,
                pr_comments=pr_comments,
                # pr_patch=pr_patch,
            )
            extraction = self._build_extraction(result)
        except Exception as e:
            logger.error(f"Problem extraction failed: {e}", exc_info=True)
            fallback = ProblemExtraction(
                initial_observations=f"[extraction failed: {e}]",
                triage_attempts=None,
                solution_overview=None,
                solution_observations=None,
            )
            return fallback

        return extraction

    def _clean_text(self, value: Any | None) -> str | None:
        """Clean and normalize text values from predictions."""
        if value is None:
            return None
        if isinstance(value, list):
            try:
                # Flatten nested lists and join with newlines
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
        if stripped.lower() in {"null", "none", "undefined"}:
            return None
        return stripped or None

    def _build_extraction(self, prediction: Any) -> ProblemExtraction:  # noqa: C901
        """
        Normalize the raw DSPy prediction into a ProblemExtraction object.

        Extracts the four output fields from ProblemExtractorSignature:
        - initial_observations
        - triage_attempts
        - solution_overview
        - solution_observations
        """
        # Extract all four fields from the prediction
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

        # Validate each field
        if not plausible(initial_obs, min_len=20):
            initial_obs = None
        if not plausible(triage, min_len=10):
            triage = None
        if not plausible(solution, min_len=10):
            solution = None
        if not plausible(solution_obs, min_len=10):
            solution_obs = None

        extraction = ProblemExtraction(
            initial_observations=initial_obs or "",
            triage_attempts=triage or "",
            solution_overview=solution or "",
            solution_observations=solution_obs or "",
        )

        # Ensure there's at least some content string (empty strings are allowed)
        if extraction.initial_observations is None:
            extraction.initial_observations = ""
        if extraction.triage_attempts is None:
            extraction.triage_attempts = ""
        if extraction.solution_overview is None:
            extraction.solution_overview = ""
        if extraction.solution_observations is None:
            extraction.solution_observations = ""

        return extraction
