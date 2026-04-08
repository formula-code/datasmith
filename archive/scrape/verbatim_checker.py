"""
Utilities for validating extractive/verbatim content in problem statements.

This module provides tools to ensure that generated problem statements maintain
high fidelity to source material by measuring LCS (Longest Common Subsequence)
ratios and other similarity metrics.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass


@dataclass
class VerbatimScore:
    """Scores for verbatim extraction quality for a single sentence."""

    lcs_ratio: float  # Longest Common Subsequence ratio (via difflib ratio)
    exact_match_ratio: float  # Percentage of exact n-gram matches
    best_window: str | None  # Best matching window in source
    is_verbatim: bool  # Whether it passes thresholds
    details: str  # Human-readable explanation


def compute_lcs_ratio(text: str, source: str) -> float:
    """Compute an approximate LCS ratio between text and source.

    Uses difflib.SequenceMatcher ratio which correlates well for our purposes.
    """

    matcher = difflib.SequenceMatcher(None, text.lower(), source.lower())
    return matcher.ratio()


def find_best_window_match(sentence: str, source: str, window_expansion: float = 1.5) -> tuple[float, str]:
    """Find the best matching window in source for a given sentence.

    - Try exact substring match (normalized whitespace, case-insensitive) first.
    - Otherwise, slide a window of ~1.5x the sentence length and pick the best ratio.
    - Fall back to comparing with the whole source if window search is weak.
    """

    sentence_len = len(sentence)
    window_size = max(int(sentence_len * window_expansion), sentence_len)

    best_ratio = 0.0
    best_window = ""

    # Normalize whitespace for comparison
    clean_sentence = re.sub(r"\s+", " ", sentence.strip().lower())
    clean_source = re.sub(r"\s+", " ", source.strip().lower())

    # Try exact substring match first
    if clean_sentence and clean_sentence in clean_source:
        start_idx = clean_source.index(clean_sentence)
        context_start = max(0, start_idx - 50)
        context_end = min(len(source), start_idx + len(sentence) + 50)
        return 1.0, source[context_start:context_end]

    # Sliding window search
    step = max(1, window_size // 4)
    for i in range(0, max(0, len(clean_source) - window_size + 1), step):
        window = clean_source[i : i + window_size]
        ratio = difflib.SequenceMatcher(None, clean_sentence, window).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_window = source[i : i + window_size]

    # If window search didn't work well, try the whole source
    if best_ratio < 0.5 and clean_source:
        ratio = difflib.SequenceMatcher(None, clean_sentence, clean_source).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_window = source[: min(500, len(source))]

    return best_ratio, best_window


def compute_exact_ngram_ratio(text: str, source: str, ngram_size: int = 4) -> float:
    """Compute the ratio of n-grams in text that appear exactly in source (case-insensitive)."""

    text_words = re.sub(r"\s+", " ", text.strip().lower()).split()
    source_lower = re.sub(r"\s+", " ", source.strip().lower())

    if not text_words:
        return 0.0

    if len(text_words) < ngram_size:
        return 1.0 if " ".join(text_words) in source_lower else 0.0

    ngrams = [" ".join(text_words[i : i + ngram_size]) for i in range(len(text_words) - ngram_size + 1)]
    if not ngrams:
        return 0.0
    matches = sum(1 for n in ngrams if n in source_lower)
    return matches / len(ngrams)


def is_verbatim_subset(
    sentence: str, source: str, min_lcs: float = 0.85, min_exact_ngrams: float = 0.70, ngram_size: int = 4
) -> VerbatimScore:
    """Check if a sentence is a verbatim (or near-verbatim) extraction from source.

    Returns a VerbatimScore with LCS ratio, n-gram ratio, and pass/fail.
    """

    clean_sentence = re.sub(r"\s+", " ", sentence.strip())

    # Skip very short sentences (likely headers or simple statements)
    if len(clean_sentence.split()) < 3:
        return VerbatimScore(
            lcs_ratio=1.0,
            exact_match_ratio=1.0,
            best_window=clean_sentence,
            is_verbatim=True,
            details="ACCEPT: short sentence (<3 words)",
        )

    lcs_ratio, best_window = find_best_window_match(clean_sentence, source)
    exact_match_ratio = compute_exact_ngram_ratio(clean_sentence, source, ngram_size)

    is_verbatim = (lcs_ratio >= min_lcs) or (exact_match_ratio >= min_exact_ngrams)

    if is_verbatim:
        if lcs_ratio >= 0.95:
            details = f"PASS: Excellent extraction (LCS: {lcs_ratio:.2%})"
        elif lcs_ratio >= min_lcs:
            details = f"PASS: Good extraction (LCS: {lcs_ratio:.2%})"
        else:
            details = f"PASS: Acceptable via n-grams (n-gram: {exact_match_ratio:.2%})"
    else:
        details = f"FAIL: Too abstractive (LCS: {lcs_ratio:.2%}, n-gram: {exact_match_ratio:.2%})"

    return VerbatimScore(
        lcs_ratio=lcs_ratio,
        exact_match_ratio=exact_match_ratio,
        best_window=best_window[:200] if best_window else "",
        is_verbatim=is_verbatim,
        details=details,
    )


def validate_code_block_verbatim(code_block: str, source: str) -> bool:
    """Validate that a code block is character-for-character identical to source.

    Line-ending normalization is applied as a fallback, but internal spaces/newlines
    must still match for a pass.
    """

    clean_code = code_block.strip()
    # Try direct and normalized EOL comparisons
    if not clean_code:
        return True
    if clean_code in source:
        return True
    clean_code_normalized = clean_code.replace("\r\n", "\n")
    source_normalized = source.replace("\r\n", "\n")
    return clean_code_normalized in source_normalized or clean_code in source_normalized


def validate_section_extractiveness(
    section_text: str, source: str, min_lcs: float = 0.85, code_blocks_must_be_exact: bool = True
) -> tuple[bool, list[VerbatimScore]]:
    """Validate that a section maintains extractive quality by checking sentences.

    - Optionally enforces exact match for code blocks in triple backticks.
    - Returns (overall_pass, per-sentence scores).
    """

    code_pattern = r"```[\w]*\n(.*?)```"
    code_blocks = re.findall(code_pattern, section_text, re.DOTALL)

    if code_blocks_must_be_exact:
        for code_block in code_blocks:
            if not validate_code_block_verbatim(code_block, source):
                return False, [
                    VerbatimScore(
                        lcs_ratio=0.0,
                        exact_match_ratio=0.0,
                        best_window="",
                        is_verbatim=False,
                        details=f"Code block not found verbatim in source: {code_block[:100]}...",
                    )
                ]

    # Remove code blocks for sentence analysis
    text_without_code = re.sub(code_pattern, "", section_text, flags=re.DOTALL)

    # Split into sentences (simple approach)
    sentences = [s.strip() for s in re.split(r"[.!?]\s+", text_without_code) if s.strip()]
    if not sentences:
        return True, []

    scores: list[VerbatimScore] = []
    for sentence in sentences:
        if len(sentence.split()) < 3:
            continue
        score = is_verbatim_subset(sentence, source, min_lcs=min_lcs)
        scores.append(score)

    if not scores:
        return True, []

    pass_count = sum(1 for s in scores if s.is_verbatim)
    overall_pass = (pass_count / len(scores)) >= 0.75
    return overall_pass, scores


THRESHOLDS = {
    "technical_description": {"min_lcs": 0.85, "min_ngrams": 0.70},
    "code_snippet": {"min_lcs": 1.0, "min_ngrams": 1.0},  # Must be exact
    "discussion_summary": {"min_lcs": 0.75, "min_ngrams": 0.60},  # Slightly more flexible
    "user_quote": {"min_lcs": 0.95, "min_ngrams": 0.90},  # Nearly exact
}
