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

from typing import Any, Dict, Tuple

import dspy

from datasmith.logging_config import configure_logging
from datasmith.scrape.verbatim_checker import (
    generate_verbatim_report,
    validate_section_extractiveness,
)

logger = configure_logging()


class ProblemExtractorSignature(dspy.Signature):
    """
    Extract problem statement from GitHub PR/issue discussions.

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

    5. Preserve disagreements - don't smooth over conflicts
       - If Person A says X and Person B says Y, show BOTH viewpoints
       - Use quotes when capturing specific arguments
       - Attribute opinions: "jni said: 'X', but brisvag noted: 'Y'"

    6. Only create section headers if they match natural divisions
       - Don't force "Problem/Solution/Acceptance Criteria" template
       - Use: "Issue Description", "Reproduction", "Discussion", etc.
       - Skip sections that have no content

    7. Start with the concrete problem, not meta-discussion
       - NOT: "This PR addresses concerns raised about..."
       - YES: "Points in napari are invisible when..."
       - Lead with the actual issue, not history

    VALIDATION CRITERIA (your output will be checked):
    - Each sentence will be LCS-matched against source
    - Required: 75% LCS ratio OR 70% exact 4-gram match
    - Code blocks: 100% exact match required
    - If validation fails, your output will be rejected

    OUTPUT FORMAT:
    Use these sections only if they have content. Skip sections that don't apply.

    ## Issue Description
    [Extractive summary of core problem, 2-4 sentences]
    [Use original author's words - quote if needed]

    ## Reproduction
    [Code examples VERBATIM from source]
    [Include expected vs actual behavior]

    ## Proposed Solution
    [What this PR/discussion suggests]
    [Include code if architectural changes described]

    ## Alternative Solutions
    [Other approaches mentioned]
    [Preserve who suggested what if there's debate]

    ## Discussion Summary
    [Key points, preserving different viewpoints]
    [Attribute opinions when they conflict]

    ## Key Technical Details
    [Versions, environments, edge cases]
    [Specific numbers, file paths, function names]

    ANTI-PATTERNS TO AVOID:
    ❌ "There has been some discussion" → ✓ State the actual issue
    ❌ "Users report that..." → ✓ Give concrete example with code
    ❌ "The proposed fix involves..." → ✓ Describe the actual change
    ❌ Paraphrasing code → ✓ Copy-paste exactly
    ❌ "Issue 0, Issue 1, Issue 2" → ✓ Only reference if actually numbered in source
    ❌ Adding "Acceptance Criteria" → ✓ Only if explicitly stated

    COMPLETE EXAMPLE - THIS IS WHAT WE WANT:

    Input (source text from PR discussion):
    ```
    Points are sometimes invisible when using large coordinates (1000x1000 image) and too large
    when using small coordinates (10x10 image).

    Code to reproduce:
    ```python
    viewer = napari.Viewer()
    viewer.add_image(np.zeros((1000,1000)))
    viewer.add_points([(20, 20)])
    # Points barely visible
    ```

    This PR implements experimental_canvas_size_limits to clamp point sizes.
    Default values under discussion: (2, 100) or (2, 10000).

    @haesleinhuepf suggested auto-sizing:
    points_layer.size = np.asarray(viewer.dims.range).max() * 0.01
    But @brisvag noted this requires layer to know viewer state.

    @jni said: "100 is too aggressive, molecular vis demos would break"
    ```

    BAD Output (abstractive - AVOID THIS):
    ```
    ## Problem
    There has been some discussion about setting new defaults for canvas size limits
    to prevent weird behaviors at zoom extremes.

    ## Related Issues
    - Issue 0: Point visibility problems
    - Issue 1: Size configuration
    ```

    GOOD Output (extractive - DO THIS):
    ```
    ## Issue Description

    Points are sometimes invisible when using large coordinates (1000x1000 image) and too large
    when using small coordinates (10x10 image).

    ## Reproduction

    ```python
    viewer = napari.Viewer()
    viewer.add_image(np.zeros((1000,1000)))
    viewer.add_points([(20, 20)])
    # Points barely visible
    ```

    ## Proposed Solution

    This PR implements experimental_canvas_size_limits to clamp point sizes.
    Default values under discussion: (2, 100) or (2, 10000).

    ## Alternative Solutions Discussed

    @haesleinhuepf suggested auto-sizing:
    ```python
    points_layer.size = np.asarray(viewer.dims.range).max() * 0.01
    ```
    But @brisvag noted this requires layer to know viewer state.

    ## Key Discussion Points

    @jni said: "100 is too aggressive, molecular vis demos would break"
    ```

    Notice in the GOOD version:
    - Sentences are VERBATIM from source (same wording)
    - Code is CHARACTER-EXACT (including comments)
    - Specific values preserved ("1000x1000", "(2, 100)")
    - Quotes preserved with attribution ("@jni said")
    - No generic phrases ("there has been discussion")
    - No fabricated sections ("Issue 0, 1, 2")

    Remember: Your goal is EXTRACTION, not summarization. Copy-paste from source with minimal reorganization.
    """

    github_text: str = dspy.InputField(desc="Raw GitHub issue/PR description text")
    related_issues: str = dspy.InputField(desc="Text from related issues and their references")
    extracted_problem: str = dspy.OutputField(
        desc="Extracted problem statement with high fidelity to source (75%+ LCS ratio)"
    )


class CommentExtractorSignature(dspy.Signature):
    """
    Extract key technical discussion points from GitHub comment threads.

    Same LCS validation requirements as ProblemExtractor. Your output will be validated
    for extractiveness with 75%+ LCS ratio target.

    RULES (same as ProblemExtractor):
    1. Extract, don't abstract - preserve original wording
    2. ALL code snippets CHARACTER-EXACT
    3. Keep technical terms EXACTLY as written
    4. Include specific values (numbers, versions, etc.)
    5. Preserve disagreements with attribution
    6. Natural structure only
    7. Focus on concrete technical substance

    ADDITIONAL RULES FOR COMMENTS:
    - Omit greetings, thanks, off-topic tangents
    - Compress verbose restatements of the same point
    - Keep all distinct technical points
    - Preserve debugging insights, workarounds, alternatives
    - Include exact error messages if mentioned
    - Preserve links to related discussions (verbatim URLs)

    OUTPUT FORMAT:
    Plain text discussion summary organized by topic. Use minimal headers if needed.
    Focus on technical substance. Preserve exact quotes for key insights.

    Example structure:

    [Main technical point from discussion, verbatim]

    [Supporting details, code examples verbatim]

    Alternative approach mentioned: [exact description]

    [Username] noted: "[exact quote of key insight]"

    ANTI-PATTERNS TO AVOID:
    ❌ "Several users mentioned performance issues" → ✓ Give specific details
    ❌ "A workaround was suggested" → ✓ Include the actual workaround
    ❌ Summarizing code snippets → ✓ Include them verbatim
    ❌ "Some discussion about X" → ✓ Extract the actual technical points

    COMPLETE EXAMPLE - THIS IS WHAT WE WANT:

    Input (comment thread):
    ```
    @brisvag: This PR doesn't solve the original #4705 issue (auto-sizing based on data scale),
    only prevents extreme invisibility.

    @jni: I like the minimum size, but dislike the maximum size. 100 is too aggressive,
    molecular vis demos would break.

    @haesleinhuepf: Consider having a computed default-point size:
    ```python
    points_layer.size = np.asarray(viewer.dims.range).max() * 0.01
    ```

    @brisvag: That approach relies too much on unrelated viewer state. I would rather add
    a special 'auto' value that explicitly sets the size automatically.
    ```

    BAD Output (abstractive - AVOID THIS):
    ```
    There was discussion about whether this PR solves the original issue. Several users
    raised concerns about the default values. Alternative approaches were suggested.
    ```

    GOOD Output (extractive - DO THIS):
    ```
    @brisvag: This PR doesn't solve the original #4705 issue (auto-sizing based on data scale),
    only prevents extreme invisibility.

    @jni: I like the minimum size, but dislike the maximum size. 100 is too aggressive,
    molecular vis demos would break.

    Alternative approach suggested by @haesleinhuepf:
    ```python
    points_layer.size = np.asarray(viewer.dims.range).max() * 0.01
    ```

    @brisvag noted this approach relies too much on unrelated viewer state. Suggested adding
    a special 'auto' value that explicitly sets the size automatically.
    ```

    Notice in the GOOD version:
    - Exact quotes with attribution preserved
    - Code is CHARACTER-EXACT
    - Specific technical details maintained ("100", "#4705")
    - Original phrasing used ("relies too much on unrelated viewer state")
    - No generic summaries ("there was discussion", "users raised concerns")

    Remember: Extract technical substance while filtering noise. Maintain 75%+ LCS ratio.
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

    def extract_problem(self, message: str, related_issues: str) -> Tuple[str, Dict[str, Any]]:
        """
        Extract problem statement from GitHub issue/PR with validation.

        Args:
            message: Main GitHub issue or PR description
            related_issues: Text from related issues and references

        Returns:
            Tuple of (extracted_problem_statement, validation_report)

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
            extracted = str(result.extracted_problem).strip()
        except Exception as e:
            logger.error(f"Problem extraction failed: {e}", exc_info=True)
            return f"[extraction failed: {e}]", {"overall_pass": False, "error": str(e)}

        # Validate if enabled
        validation_report = self._validate_extraction(
            extracted=extracted,
            source=message + "\n\n" + related_issues,
            extraction_type="problem_statement",
        )

        return extracted, validation_report

    def extract_comments(self, comment_thread: str) -> Tuple[str, Dict[str, Any]]:
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

    def _validate_extraction(self, extracted: str, source: str, extraction_type: str) -> Dict[str, Any]:
        """
        Validate extraction quality using LCS ratio.

        Args:
            extracted: The extracted text
            source: The source material
            extraction_type: Type of extraction for logging ("problem_statement" or "discussion")

        Returns:
            Validation report dict with metrics and details
        """
        if not self.validate_lcs:
            return {
                "overall_pass": True,
                "validation_enabled": False,
                "message": "Validation disabled",
            }

        try:
            # Run validation
            passes, scores = validate_section_extractiveness(
                section_text=extracted,
                source=source,
                min_lcs=self.min_lcs,
                code_blocks_must_be_exact=True,
            )

            # Calculate averages
            avg_lcs = sum(s.lcs_ratio for s in scores) / len(scores) if scores else 1.0
            avg_ngram = sum(s.exact_match_ratio for s in scores) / len(scores) if scores else 1.0

            # Generate detailed report
            detailed_report = generate_verbatim_report(
                extracted_problem_statement=extracted,
                source=source,
                min_lcs=self.min_lcs,
            )

            # Log if enabled
            if self.log_validation:
                if passes:
                    logger.info(
                        f"{extraction_type} extraction passed validation: LCS={avg_lcs:.2%}, n-gram={avg_ngram:.2%}"
                    )
                else:
                    logger.warning(
                        f"{extraction_type} extraction FAILED validation: "
                        f"LCS={avg_lcs:.2%}, n-gram={avg_ngram:.2%}\n"
                        f"{detailed_report}"
                    )

            return {
                "overall_pass": passes,
                "avg_lcs": avg_lcs,
                "avg_ngram": avg_ngram,
                "details": detailed_report,
                "raw_scores": scores,
                "validation_enabled": True,
            }

        except Exception as e:
            logger.error(f"Validation failed: {e}", exc_info=True)
            return {
                "overall_pass": False,
                "error": str(e),
                "message": f"Validation error: {e}",
            }
