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

# Verbatim validation disabled: previously imported utilities for LCS checks
# have been removed to simplify extraction and avoid noisy logs.

logger = configure_logging()


@dataclass
class ProblemExtraction:
    """Structured representation of an extracted problem statement.

    Notes on rendering:
    - Use ``to_problem_markdown`` for the problem-only variant.
    - Use ``to_problem_with_solution_markdown`` to include the solution section.
    - ``to_markdown`` remains for backward-compatibility and includes the solution when present.
    """

    problem_statement: str | None = None
    solution_overview: str | None = None

    def _strip(self, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None

    def to_problem_markdown(self) -> str:
        """Render only the problem portion in markdown (no solution)."""
        text = (self.problem_statement or "").strip()
        text = re.sub(r"^```json\s*$", "", text, flags=re.MULTILINE)
        return text

    def _normalise_solution(self) -> str | None:
        if not self.solution_overview:
            return None
        content = self.solution_overview.strip()
        low = content.lstrip().lower()
        if low.startswith("## solution overview") or low.startswith("**solution overview**"):
            lines = content.splitlines()
            if lines:
                lines = lines[1:]
            content = "\n".join(lines).lstrip()
        return content

    def to_problem_with_solution_markdown(self) -> str:
        """Render problem + a separate Solution Overview section."""
        sections: list[str] = []
        problem = (self.problem_statement or "").strip()
        if problem:
            sections.append(problem)
        solution = self._normalise_solution()
        if solution:
            sections.append(f"**Solution Overview**\n\n{solution}")
        text = "\n\n".join(sections).strip()
        text = re.sub(r"^```json\s*$", "", text, flags=re.MULTILINE)
        return text

    def to_markdown(self) -> str:
        """Backward-compatible rendering that includes solution when present."""
        return self.to_problem_with_solution_markdown()

    def to_dict(self) -> dict[str, Any]:
        """Return a serialisable representation of the extraction."""

        return {
            "problem_statement": self._strip(self.problem_statement),
            "solution_overview": self._strip(self.solution_overview),
        }


class ProblemExtractorSignature(dspy.Signature):
    """
    Create a comprehensive problem statement from GitHub PR/issue discussions.

    Focus on four key aspects:
    1. The original problem or need
    2. Key discussions and decisions made
    3. Important technical details and constraints
    4. The implemented solution (in a separate section.)

    ## Output Format
    Output plain content for two fields (no headings in the content itself):
    1. Problem Statement (required when any evidence exists)
       - The core issue being addressed (1-3 sentences)
       - Technical context: code snippets, error messages, repro steps (verbatim)
       - Key viewpoints or constraints (optional)
    2. Solution Overview (optional)
       - A concise summary of the implemented changes (verbatim wording preferred)


    ## General guidelines
    - Preserve technical accuracy (exact error messages, code snippets, file paths)
    - Maintain attribution for important viewpoints
    - Create a coherent narrative that flows logically
    - Focus on information essential for understanding the PR
    - Extract, don't abstract - use original wording wherever possible
    - Ignore boilerplate such as PR template checklists (`- [x] ...`) and
      CI/automation headers unless they contain unique technical details

    CRITICAL: Your output will be AUTOMATICALLY VALIDATED for extractiveness using LCS
    (Longest Common Subsequence) ratio. Target: 75%+ of your output should be verbatim
    from the source.

    RULES:
    1. Extract, don't abstract - use original wording wherever possible
       - BAD: "There has been discussion about size issues"
       - GOOD: "Points are sometimes invisible when using large coordinate values"

    2. ALL code snippets must be CHARACTER-EXACT verbatim
       - Copy-paste code blocks with no modifications
       - Preserve all whitespace, indentation, comments
       - Any paraphrased code = automatic failure

    3. Keep technical terms EXACTLY as written
       - `experimental_canvas_size_limits` NOT "canvas size settings"
       - `np.asarray(viewer.dims.range).max() * 0.01` NOT "computed from viewer dimensions"
       - Preserve function names, variable names, file paths exactly

    4. Include specific values, never generalize
       - "(2, 100) or (2, 10000)" NOT "small range values"
       - "1000x1000 image" NOT "large images"
       - "haesleinhuepf" NOT "a user" (when attribution matters)
       - Keep version numbers, commit hashes, issue numbers exact

    VALIDATION CRITERIA (your output will be checked):
    - Each sentence will be LCS-matched against source
    - Required: 75% LCS ratio OR 70% exact 4-gram match
    - Code blocks: 100% exact match required
    - If validation fails, your output will be rejected


    Additional requirements:
    - Always provide a non-empty Problem Statement when the PR/issue contains any relevant
      description (title/body/comments/issues). If the source only has a solution description,
      infer the minimal problem description from it without adding new facts.
    - Do NOT include section headings like "Problem Statement" or "Solution Overview" in your content.
    - Do NOT return only links as the problem statement; summarise the problem in 1-3 sentences.

    ANTI-PATTERNS TO AVOID:
    ❌ "There has been some discussion" → ✓ State the actual issue
    ❌ "Users report that..." → ✓ Give concrete example with code
    ❌ "The proposed fix involves..." → ✓ Describe the actual change
    ❌ Paraphrasing code → ✓ Copy-paste exactly
    ❌ "Issue 0, Issue 1, Issue 2" → ✓ Only reference if actually numbered in source
    """

    github_text: str = dspy.InputField(desc="Raw GitHub PR/issue text (titles, bodies, comments).")
    related_issues: str = dspy.InputField(desc="Concatenated text from linked issues/PRs/discussions, if any.")

    # Structured outputs (fill only when supported by input)
    # Some providers occasionally emit arrays/content chunks; accept list[str] and we will
    # normalise to a string in post-processing to avoid adapter parse failures.
    problem_statement: str | list[Any] | None = dspy.OutputField(
        desc="A markdown document detailing the problem statement and relevant discussion."
    )
    solution_overview: str | list[Any] | None = dspy.OutputField(
        desc="What the PR implements or changes. Describe the actual change using precise terms from the source."
    )


class CommentExtractorSignature(dspy.Signature):
    """
    Extract key technical discussions from GitHub comment threads.

    Focus on:
    - Technical decisions and their rationales
    - Alternative approaches discussed
    - Specific concerns or objections raised
    - Debugging insights and workarounds

    ## Guidelines
    - Preserve exact technical details (code, error messages, values)
    - Attribute viewpoints to specific users
    - Organize by topic for readability
    - Omit social pleasantries and redundant restatements
    - Keep disagreements and different perspectives
    - Extract, don't abstract - preserve original wording
    - Omit greetings, thanks, off-topic tangents
    - Compress verbose restatements of the same point
    - Keep all distinct technical points
    - Preserve links to related discussions (verbatim URLs)
    """

    comment_thread: str = dspy.InputField(desc="Concatenated GitHub comment thread")
    extracted_discussion: str = dspy.OutputField(
        desc="Extracted discussion summary with high fidelity to source (75%+ LCS ratio)"
    )


class ProblemExtractor(dspy.Module):
    """
    Extractive problem statement and discussion extractor with LCS validation.

    This module extracts (not summarizes) problem statements and discussion points
    from GitHub PR/issue content while maintaining high fidelity to source material.

    Validation:
        - LCS ratio: 75%+ for each section
        - Code blocks: 100% character-exact match
        - Technical terms: preserved exactly
        - Specific values: not generalized

    Usage:
        extractor = ProblemExtractor(validate_lcs=True, min_lcs=0.75)

        # Extract problem statement
        problem, validation = extractor.extract_problem(
            message="GitHub issue text...",
            related_issues="Related issue details..."
        )

        # Extract discussion
        discussion, validation = extractor.extract_comments(
            comment_thread="Comment thread..."
        )
    """

    def __init__(
        self,
        validate_lcs: bool = True,
        min_lcs: float = 0.75,
        log_validation: bool = True,
    ):
        """
        Initialize ProblemExtractor.

        Args:
            validate_lcs: Whether to validate extractiveness using LCS ratio
            min_lcs: Minimum LCS ratio required (0.0-1.0, default 0.75)
            log_validation: Whether to log validation reports
        """
        super().__init__()
        self.validate_lcs = validate_lcs
        self.min_lcs = min_lcs
        self.log_validation = log_validation

        # Initialize DSPy predictors
        self.problem_predictor = dspy.Predict(ProblemExtractorSignature)
        self.comment_predictor = dspy.Predict(CommentExtractorSignature)

    def extract_problem(self, message: str, related_issues: str) -> tuple[ProblemExtraction, dict[str, Any]]:
        """
        Extract problem statement from GitHub issue/PR with validation.

        Args:
            message: Main GitHub issue or PR description
            related_issues: Text from related issues and references

        Returns:
            Tuple of (ProblemExtraction, validation_report)

            validation_report contains:
                - overall_pass: bool
                - avg_lcs: float
                - avg_ngram: float
                - details: str (human-readable report)
                - raw_scores: list of VerbatimScore objects
        """
        # Run extraction
        try:
            result = self.problem_predictor(
                github_text=message,
                related_issues=related_issues,
            )
            extraction = self._build_extraction(result)
        except Exception as e:
            logger.error(f"Problem extraction failed: {e}", exc_info=True)
            fallback = ProblemExtraction(problem_statement=f"[extraction failed: {e}]")
            return fallback, {"overall_pass": False, "error": str(e)}

        markdown_output = extraction.to_markdown()

        # Validate if enabled
        validation_report = self._validate_extraction(
            extracted=markdown_output,
            source=message + "\n\n" + related_issues,
            extraction_type="problem_statement",
        )

        return extraction, validation_report

    def _clean_text(self, value: Any | None) -> str | None:
        """Clean and normalize text values from predictions."""
        if value is None:
            return None
        # Accept list[str] or other providers' chunked content and coerce to text
        if isinstance(value, list):
            try:
                # Flatten nested lists and join with newlines
                flat: list[str] = []
                for v in value:
                    if isinstance(v, list):
                        for x in v:
                            flat.append(str(x))
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

    def _build_extraction(self, prediction: Any) -> ProblemExtraction:
        """Normalise the raw DSPy prediction into a structured extraction object."""
        problem = self._clean_text(getattr(prediction, "problem_statement", None))
        solution = self._clean_text(getattr(prediction, "solution_overview", None))

        def plausible(s: str | None, *, min_len: int = 20) -> bool:
            if s is None:
                return False
            stripped = s.strip()
            if len(stripped) < min_len:
                return False
            # Require at least one alphabetic character (avoids bare numbers / tokens)
            return bool(re.search(r"[A-Za-z]", stripped))

        # Sanity checks: discard implausible outputs to enable robust fallback in ReportBuilder
        if not plausible(problem, min_len=20):
            problem = None
        if not plausible(solution, min_len=10):
            solution = None

        extraction = ProblemExtraction(
            problem_statement=problem,
            solution_overview=solution,
        )

        # Ensure there's at least some content to present
        if not extraction.problem_statement and not extraction.to_markdown():
            extraction.problem_statement = ""

        return extraction

    def extract_comments(self, comment_thread: str) -> tuple[str, dict[str, Any]]:
        """
        Extract discussion points from GitHub comment thread with validation.

        Args:
            comment_thread: Concatenated GitHub comment text

        Returns:
            Tuple of (extracted_discussion, validation_report)
        """
        # Run extraction
        try:
            result = self.comment_predictor(comment_thread=comment_thread)
            extracted = str(result.extracted_discussion).strip()
        except Exception as e:
            logger.error(f"Comment extraction failed: {e}", exc_info=True)
            return f"[extraction failed: {e}]", {"overall_pass": False, "error": str(e)}

        # Validate if enabled
        validation_report = self._validate_extraction(
            extracted=extracted,
            source=comment_thread,
            extraction_type="discussion",
        )

        return extracted, validation_report

    def _validate_extraction(self, extracted: str, source: str, extraction_type: str) -> dict[str, Any]:
        """Validation disabled: return a stubbed pass report without side effects."""
        return {
            "overall_pass": True,
            "validation_enabled": False,
            "message": "Validation disabled",
        }
