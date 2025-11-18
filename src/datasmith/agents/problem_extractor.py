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
from datasmith.scrape.verbatim_checker import (
    validate_section_extractiveness,
)

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
    PURPOSE
    --------
    Given a GitHub PR and its comments, decide what verbatim content belongs in:
      (A) Problem Description — original problem/need/motivation/context
      (B) Solution Overview — proposed or implemented changes
    Be STRICTLY EXTRACTIVE: select contiguous spans that already exist in the source.
    Do NOT rewrite or add words. You may trim leading/trailing whitespace only.
    Code blocks and error logs MUST be character-exact (including fences/whitespace).
    ASV benchmarking efforts should ALWAYS be included in the Problem Description if present.

    INPUT
    -----
      - `pr_body`: concatenated raw text from the PR title/body
      - `pr_comments`: concatenated raw text from all PR comments

    WHAT TO RETURN
    --------------
    Two plain-text fields (no headings in the content):
      1) Problem Description (may be empty)
         - Verbatim spans describing problem/need, symptoms, error messages, repro steps,
           constraints, affected components, etc.
         - Ignore pleasantries, greetings, sign-offs, and non-technical commentary.
      2) Solution Overview (may be empty)
         - Verbatim spans describing implemented/proposed changes: diffs, function/file names,
           config/flags, commands, metrics, validations, “Fixes #…”, related links.

    SPAN RULES
    ----------
    - Copy whole sentences OR contiguous code/log blocks as they appear in the source.
    - Do NOT splice non-contiguous text; do NOT insert ellipses unless present in source.
    - If a sentence alone is incomplete, include the immediately adjacent sentence that
      completes the thought—still verbatim.
    - Lists are OK, but output ONE span per line with no bullets/numbers/labels/commentary.

    ORDER & DEDUP
    -------------
    - Preserve original chronological order (top -> bottom).
    - When de-duplicating near-identical spans, keep the FIRST occurrence and preserve order.

    FORMAT & STYLE
    --------------
    - Preserve exact tokens: function/variable names, file paths, versions, hashes, issue/PR IDs.
    - Code/logs must remain character-exact, including markdown fences if present.
    - Ignore boilerplate (checklists, CI banners) unless they contain unique technical details.
    - Maintain attribution only when the text itself names people/handles.

    VALIDATION NOTES
    ----------------
    - Either field may be an empty string if no evidence exists.
    - High extractiveness is expected (e.g., LCS/4-gram checks); code/log blocks must be exact.
    - Do NOT add headings like “Problem Statement” or “Solution Overview” into the content.

    NOTES
    -----
    - Favor specificity. When only solution text exists, leave Problem empty (and vice-versa).
    """

    # Inputs
    pr_body: str = dspy.InputField(desc="The GitHub PR/issue description")

    pr_comments: str = dspy.InputField(desc="Comments on the PR thread.")

    # Outputs (lists allowed; caller can normalize to string later)
    reasoning: str | list[Any] | None = dspy.OutputField(
        desc="(Optional) Empty OR a JSON list of source offsets only (no prose), e.g. "
        '[{"field":"problem","start":123,"end":256},{"field":"solution","start":...}].'
    )
    extracted_problem_description: str | list[Any] | None = dspy.OutputField(
        desc="Verbatim spans evidencing the problem/symptoms/constraints, one span per line (may be empty)."
    )
    extracted_solution_overview: str | list[Any] | None = dspy.OutputField(
        desc="Verbatim spans describing the implemented/proposed solution, one span per line (may be empty)."
    )


class ProblemExtractor(dspy.Module):
    """
    Extractive problem/solution bucketizer and discussion extractor with LCS validation.

    This module uses the updated signatures:
      - ProblemExtractorSignature(pr_body, pr_comments)
      - CommentExtractorSignature(comment_thread)

    Validation:
        - LCS ratio: 75%+ for each section
        - Code blocks: 100% character-exact match
        - Technical terms: preserved exactly
        - Specific values: not generalized

    Usage:
        extractor = ProblemExtractor(validate_lcs=True, min_lcs=0.75)

        # Extract Problem/Solution buckets from a PR
        extraction, validation = extractor.extract_problem(
            pr_body=pr_body_text,
            pr_comments=all_pr_comments_text,
        )
    """

    def __init__(
        self,
        validate_lcs: bool = True,
        min_lcs: float = 0.75,
        log_validation: bool = True,
    ):
        super().__init__()
        self.validate_lcs = validate_lcs
        self.min_lcs = min_lcs
        self.log_validation = log_validation

        # Updated predictors (signatures themselves already updated by you)
        self.problem_predictor = dspy.Predict(ProblemExtractorSignature)
        # self.comment_predictor = dspy.Predict(CommentExtractorSignature)

    # ---------- PUBLIC API ----------

    def extract_problem(
        self,
        *,
        pr_body: str,
        pr_comments: str,
    ) -> tuple[ProblemExtraction, dict[str, Any]]:
        """
        Extract Problem/Solution buckets from a GitHub PR + comments.

        Args:
            pr_body: The GitHub PR/issue description
            pr_comments: Comments on the PR thread

        Returns:
            (ProblemExtraction, validation_report)
        """
        # Run extraction with UPDATED input arg names
        try:
            result = self.problem_predictor(
                pr_body=pr_body,
                pr_comments=pr_comments,
            )
            extraction = self._build_extraction(result)
        except Exception as e:
            logger.error(f"Problem extraction failed: {e}", exc_info=True)
            fallback = ProblemExtraction(problem_statement=f"[extraction failed: {e}]", solution_overview=None)
            return fallback, {"overall_pass": False, "error": str(e)}

        markdown_output = extraction.to_markdown()

        # Validate against the FULL source (body + both comment sets)
        full_source = "\n\n".join(s for s in (pr_body, pr_comments) if s)
        validation_report = self._validate_extraction(
            extracted=markdown_output,
            source=full_source,
            extraction_type="problem_description",
        )

        return extraction, validation_report

    # def extract_comments(self, *, comment_thread: str) -> tuple[str, dict[str, Any]]:
    #     """
    #     Extract discussion points from a GitHub comment thread with validation.

    #     Args:
    #         comment_thread: Concatenated GitHub comment text

    #     Returns:
    #         (extracted_discussion, validation_report)
    #     """
    #     try:
    #         result = self.comment_predictor(comment_thread=comment_thread)
    #         extracted = str(getattr(result, "extracted_discussion", "")).strip()
    #     except Exception as e:
    #         logger.error(f"Comment extraction failed: {e}", exc_info=True)
    #         return f"[extraction failed: {e}]", {"overall_pass": False, "error": str(e)}

    #     validation_report = self._validate_extraction(
    #         extracted=extracted,
    #         source=comment_thread,
    #         extraction_type="discussion",
    #     )
    #     return extracted, validation_report

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

    def _build_extraction(self, prediction: Any) -> ProblemExtraction:
        """
        Normalize the raw DSPy prediction (using UPDATED output names)
        into your ProblemExtraction object (which still expects
        problem_statement / solution_overview).
        """
        # Primary: new field names
        problem = self._clean_text(getattr(prediction, "extracted_problem_description", None))
        solution = self._clean_text(getattr(prediction, "extracted_solution_overview", None))

        def plausible(s: str | None, *, min_len: int = 20) -> bool:
            if s is None:
                return False
            stripped = s.strip()
            if len(stripped) < min_len:
                return False
            return bool(re.search(r"[A-Za-z]", stripped))

        if not plausible(problem, min_len=20):
            problem = None
        if not plausible(solution, min_len=10):
            solution = None

        extraction = ProblemExtraction(
            problem_statement=problem or "",
            solution_overview=solution or "",
        )

        # Ensure there's at least some content string (empty strings are allowed)
        if extraction.problem_statement is None:
            extraction.problem_statement = ""
        if extraction.solution_overview is None:
            extraction.solution_overview = ""

        return extraction

    def _validate_extraction(self, extracted: str, source: str, extraction_type: str) -> dict[str, Any]:
        """Validate extractiveness using LCS and exact n-gram metrics.

        - When ``validate_lcs`` is False, returns a passing stub with validation disabled.
        - When enabled, enforces code-block exactness and computes per-sentence scores.
        """
        if not self.validate_lcs:
            return {
                "overall_pass": True,
                "validation_enabled": False,
            }

        try:
            overall_pass, scores = validate_section_extractiveness(
                extracted,
                source,
                min_lcs=self.min_lcs,
                code_blocks_must_be_exact=True,
            )
        except Exception as e:  # never fail the caller due to validator issues
            return {
                "overall_pass": False,
                "validation_enabled": True,
                "details": f"FAIL: validator error: {e}",
            }

        avg_lcs = 0.0
        avg_ngram = 0.0
        if scores:
            avg_lcs = sum(s.lcs_ratio for s in scores) / len(scores)
            avg_ngram = sum(s.exact_match_ratio for s in scores) / len(scores)

        total = len(scores)
        passed = sum(1 for s in scores if s.is_verbatim)
        status = "PASS" if overall_pass else "FAIL"
        details = f"{status}: {passed}/{total} sentences ≥ LCS {self.min_lcs:.2f}; avg LCS={avg_lcs:.3f}, avg 4-gram={avg_ngram:.3f}"

        return {
            "overall_pass": overall_pass,
            "validation_enabled": True,
            "avg_lcs": avg_lcs,
            "avg_ngram": avg_ngram,
            "details": details,
            "type": extraction_type,
        }
