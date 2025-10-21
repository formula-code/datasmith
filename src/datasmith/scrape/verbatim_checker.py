# """
# Utilities for validating extractive/verbatim content in problem statements.

# This module provides tools to ensure that generated problem statements maintain
# high fidelity to source material by measuring LCS (Longest Common Subsequence)
# ratios and other similarity metrics.
# """

# import difflib
# import re
# from dataclasses import dataclass
# from typing import List, Optional, Tuple


# @dataclass
# class VerbatimScore:
#     """Scores for verbatim extraction quality."""

#     lcs_ratio: float  # Longest Common Subsequence ratio
#     exact_match_ratio: float  # Percentage of exact n-gram matches
#     best_window: Optional[str]  # Best matching window in source
#     is_verbatim: bool  # Whether it passes thresholds
#     details: str  # Human-readable explanation


# def compute_lcs_ratio(text: str, source: str) -> float:
#     """
#     Compute LCS ratio between text and source.

#     Args:
#         text: The extracted/generated text
#         source: The source material

#     Returns:
#         Ratio of matching characters (0.0 to 1.0)
#     """
#     matcher = difflib.SequenceMatcher(None, text.lower(), source.lower())
#     return matcher.ratio()


# def find_best_window_match(sentence: str, source: str, window_expansion: float = 1.5) -> Tuple[float, str]:
#     """
#     Find the best matching window in source for a given sentence.

#     Uses a sliding window approach to find the section of source material
#     that best matches the sentence.

#     Args:
#         sentence: The sentence to match
#         source: The source material
#         window_expansion: How much larger the window should be than sentence (default 1.5x)

#     Returns:
#         Tuple of (best_ratio, best_matching_window)
#     """
#     sentence_len = len(sentence)
#     window_size = int(sentence_len * window_expansion)

#     best_ratio = 0.0
#     best_window = ""

#     # Normalize whitespace for comparison
#     clean_sentence = re.sub(r"\s+", " ", sentence.strip().lower())
#     clean_source = re.sub(r"\s+", " ", source.strip().lower())

#     # Try exact substring match first
#     if clean_sentence in clean_source:
#         start_idx = clean_source.index(clean_sentence)
#         # Return a bit of context
#         context_start = max(0, start_idx - 50)
#         context_end = min(len(source), start_idx + len(sentence) + 50)
#         return 1.0, source[context_start:context_end]

#     # Sliding window search
#     for i in range(0, len(clean_source) - window_size + 1, max(1, window_size // 4)):
#         window = clean_source[i : i + window_size]
#         ratio = difflib.SequenceMatcher(None, clean_sentence, window).ratio()

#         if ratio > best_ratio:
#             best_ratio = ratio
#             # Get the original (not lowercased) version
#             best_window = source[i : i + window_size]

#     # If window search didn't work well, try the whole source
#     if best_ratio < 0.5:
#         ratio = difflib.SequenceMatcher(None, clean_sentence, clean_source).ratio()
#         if ratio > best_ratio:
#             best_ratio = ratio
#             best_window = source[: min(500, len(source))]  # First 500 chars

#     return best_ratio, best_window


# def is_verbatim_subset(
#     sentence: str, source: str, min_lcs: float = 0.85, min_exact_ngrams: float = 0.70, ngram_size: int = 4
# ) -> VerbatimScore:
#     """
#     Check if a sentence is a verbatim (or near-verbatim) extraction from source.

#     Args:
#         sentence: The sentence to check
#         source: The source material
#         min_lcs: Minimum LCS ratio to consider verbatim (default 0.85)
#         min_exact_ngrams: Minimum ratio of exact n-gram matches (default 0.70)
#         ngram_size: Size of n-grams to check (default 4)

#     Returns:
#         VerbatimScore with detailed metrics
#     """
#     # Normalize for comparison
#     clean_sentence = re.sub(r"\s+", " ", sentence.strip())

#     # Skip very short sentences (likely headers or simple statements)
#     if len(clean_sentence.split()) < 3:
#         return VerbatimScore(
#             lcs_ratio=1.0,
#             exact_match_ratio=1.0,
#             best_window=clean_sentence,
#             is_verbatim=True,
#             details="Sentence too short to validate (< 3 words), accepting",
#         )

#     # Find best matching window
#     lcs_ratio, best_window = find_best_window_match(clean_sentence, source)

#     # Compute exact n-gram matches
#     exact_match_ratio = compute_exact_ngram_ratio(clean_sentence, source, ngram_size)

#     # Determine if verbatim
#     is_verbatim = lcs_ratio >= min_lcs or exact_match_ratio >= min_exact_ngrams

#     # Generate human-readable details
#     if is_verbatim:
#         if lcs_ratio >= 0.95:
#             details = f"Excellent extraction (LCS: {lcs_ratio:.2%})"
#         elif lcs_ratio >= min_lcs:
#             details = f"Good extraction (LCS: {lcs_ratio:.2%})"
#         else:
#             details = f"Acceptable via n-grams (n-gram: {exact_match_ratio:.2%})"
#     else:
#         details = f"Too abstractive (LCS: {lcs_ratio:.2%}, n-gram: {exact_match_ratio:.2%})"

#     return VerbatimScore(
#         lcs_ratio=lcs_ratio,
#         exact_match_ratio=exact_match_ratio,
#         best_window=best_window[:200] if best_window else "",
#         is_verbatim=is_verbatim,
#         details=details,
#     )


# def compute_exact_ngram_ratio(text: str, source: str, ngram_size: int = 4) -> float:
#     """
#     Compute the ratio of n-grams in text that appear exactly in source.

#     This is useful for detecting paraphrasing vs extraction.

#     Args:
#         text: The text to analyze
#         source: The source material
#         ngram_size: Size of n-grams (default 4)

#     Returns:
#         Ratio of matching n-grams (0.0 to 1.0)
#     """
#     # Normalize
#     text_words = re.sub(r"\s+", " ", text.strip().lower()).split()
#     source_lower = re.sub(r"\s+", " ", source.strip().lower())

#     if len(text_words) < ngram_size:
#         # For short text, check if it appears as-is
#         return 1.0 if " ".join(text_words) in source_lower else 0.0

#     # Generate n-grams
#     ngrams = []
#     for i in range(len(text_words) - ngram_size + 1):
#         ngram = " ".join(text_words[i : i + ngram_size])
#         ngrams.append(ngram)

#     if not ngrams:
#         return 0.0

#     # Count exact matches
#     matches = sum(1 for ngram in ngrams if ngram in source_lower)

#     return matches / len(ngrams)


# def validate_code_block_verbatim(code_block: str, source: str) -> bool:
#     """
#     Validate that a code block is character-for-character identical to source.

#     Code blocks should NEVER be paraphrased.

#     Args:
#         code_block: The extracted code block
#         source: The source material

#     Returns:
#         True if code block appears exactly in source
#     """
#     # Remove leading/trailing whitespace but preserve internal formatting
#     clean_code = code_block.strip()

#     # Also try with normalized line endings
#     clean_code_normalized = clean_code.replace("\r\n", "\n")
#     source_normalized = source.replace("\r\n", "\n")

#     return clean_code in source or clean_code_normalized in source_normalized or clean_code in source_normalized


# def validate_section_extractiveness(
#     section_text: str, source: str, min_lcs: float = 0.85, code_blocks_must_be_exact: bool = True
# ) -> Tuple[bool, List[VerbatimScore]]:
#     """
#     Validate that a section maintains extractive quality.

#     Splits section into sentences and validates each one.

#     Args:
#         section_text: The section to validate
#         source: The source material
#         min_lcs: Minimum LCS ratio for each sentence
#         code_blocks_must_be_exact: Whether code blocks must be character-exact

#     Returns:
#         Tuple of (overall_pass, list of scores per sentence)
#     """
#     # Extract code blocks separately
#     code_pattern = r"```[\w]*\n(.*?)```"
#     code_blocks = re.findall(code_pattern, section_text, re.DOTALL)

#     # Validate code blocks if present
#     if code_blocks_must_be_exact:
#         for code_block in code_blocks:
#             if not validate_code_block_verbatim(code_block, source):
#                 return False, [
#                     VerbatimScore(
#                         lcs_ratio=0.0,
#                         exact_match_ratio=0.0,
#                         best_window="",
#                         is_verbatim=False,
#                         details=f"Code block not found verbatim in source: {code_block[:100]}...",
#                     )
#                 ]

#     # Remove code blocks for sentence analysis
#     text_without_code = re.sub(code_pattern, "", section_text, flags=re.DOTALL)

#     # Split into sentences (simple approach)
#     sentences = [s.strip() for s in re.split(r"[.!?]\s+", text_without_code) if s.strip()]

#     # Skip if no meaningful sentences
#     if not sentences:
#         return True, []

#     # Validate each sentence
#     scores = []
#     for sentence in sentences:
#         # Skip very short or structural sentences
#         if len(sentence.split()) < 3:
#             continue

#         score = is_verbatim_subset(sentence, source, min_lcs=min_lcs)
#         scores.append(score)

#     # Overall pass if 75% of sentences are verbatim
#     if not scores:
#         return True, []

#     pass_count = sum(1 for s in scores if s.is_verbatim)
#     overall_pass = (pass_count / len(scores)) >= 0.75

#     return overall_pass, scores


# def generate_verbatim_report(extracted_problem_statement: str, source: str, min_lcs: float = 0.85) -> str:
#     """
#     Generate a detailed report on extraction quality.

#     Args:
#         extracted_problem_statement: The generated problem statement
#         source: The original source material
#         min_lcs: Minimum LCS threshold

#     Returns:
#         Human-readable report string
#     """
#     sections = extracted_problem_statement.split("\n## ")

#     report_lines = ["=== VERBATIM EXTRACTION QUALITY REPORT ===\n"]

#     overall_pass = True

#     for i, section in enumerate(sections):
#         if not section.strip():
#             continue

#         section_name = section.split("\n")[0] if i > 0 else "Header"
#         section_text = "\n".join(section.split("\n")[1:]) if i > 0 else section

#         passes, scores = validate_section_extractiveness(section_text, source, min_lcs)

#         if not passes:
#             overall_pass = False

#         report_lines.append(f"\n## {section_name}")
#         report_lines.append(f"Status: {'✓ PASS' if passes else '✗ FAIL'}")

#         if scores:
#             avg_lcs = sum(s.lcs_ratio for s in scores) / len(scores)
#             avg_ngram = sum(s.exact_match_ratio for s in scores) / len(scores)
#             report_lines.append(f"Avg LCS: {avg_lcs:.2%}")
#             report_lines.append(f"Avg n-gram: {avg_ngram:.2%}")

#             # Show problematic sentences
#             fails = [s for s in scores if not s.is_verbatim]
#             if fails:
#                 report_lines.append(f"\nProblematic sentences ({len(fails)}):")
#                 for fail in fails[:3]:  # Show first 3
#                     report_lines.append(f"  - {fail.details}")

#     report_lines.append(f"\n{'=' * 50}")
#     report_lines.append(f"OVERALL: {'✓ PASS' if overall_pass else '✗ FAIL'}")
#     report_lines.append(f"{'=' * 50}")

#     return "\n".join(report_lines)

# THRESHOLDS = {
#     "technical_description": {"min_lcs": 0.85, "min_ngrams": 0.70},
#     "code_snippet": {"min_lcs": 1.0, "min_ngrams": 1.0},  # Must be exact
#     "discussion_summary": {"min_lcs": 0.75, "min_ngrams": 0.60},  # Slightly more flexible
#     "user_quote": {"min_lcs": 0.95, "min_ngrams": 0.90},  # Nearly exact
# }
