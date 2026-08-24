"""Tests for datasmith.filters — attribute compliance pre-screening."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from datasmith.filters import (
    MAX_FILES_CHANGED,
    MAX_PATCH_TOKENS,
    MAX_TOTAL_CHANGES,
    MIN_PATCH_TOKENS,
    check_file_compliance,
    check_patch_size,
    estimate_tokens,
    has_core_file,
    message_filter,
    symbolic_compliance,
)

# ── message_filter ──────────────────────────────────────────────────


class TestMessageFilterPositive:
    """Titles with performance keywords should pass."""

    @pytest.mark.parametrize(
        "title",
        [
            "PERF: speed up groupby aggregation",
            "Optimize DataFrame.merge for sorted keys",
            "Fix performance regression in read_csv",
            "Speed up tree traversal with caching",
            "Faster convolution using vectorized ops",
            "Improve benchmark throughput",
            "Reduce latency of HTTP client",
            "Profile-guided optimization of sort",
            "Cache compiled regex patterns",
            "Parallelize image processing pipeline",
            "Fix slow test discovery (actually a perf issue)",
            "Accelerate model inference",
            "Improve efficiency of memory allocator",
            "Vectorize rolling window calculation",
            "Address bottleneck in IO path",
        ],
    )
    def test_passes(self, title: str) -> None:
        assert message_filter(title) is True


class TestMessageFilterNegative:
    """Titles without performance keywords or with negative keywords should fail."""

    @pytest.mark.parametrize(
        "title",
        [
            "Update documentation for v2.0",
            "Fix typo in README",
            "Bump version to 1.2.3",
            "Add type hints to core module",
            "CI: fix GitHub Actions workflow",
            "Revert broken commit",
            "Backport fix from main",
            "Format code with black",
            "Add changelog entry",
            "Deprecate old API",
            "Lint fixes",
            "Release 3.0.0",
        ],
    )
    def test_rejects_non_perf(self, title: str) -> None:
        assert message_filter(title) is False

    @pytest.mark.parametrize(
        "title",
        [
            "Add new feature for data loading",
            "Fix bug in parser",
            "Refactor internal modules",
            "Update dependencies",
            "Support Python 3.12",
        ],
    )
    def test_passes_ambiguous_no_keyword(self, title: str) -> None:
        """Titles with no positive AND no negative keyword pass (ambiguous → let LLM decide)."""
        assert message_filter(title) is True


class TestMessageFilterEdgeCases:
    def test_positive_overrides_negative(self) -> None:
        """If title has both positive and negative keywords, pass (positive is sufficient)."""
        assert message_filter("Revert performance optimization") is True
        assert message_filter("Documentation for benchmark suite") is True

    def test_case_insensitive(self) -> None:
        assert message_filter("OPTIMIZE memory usage") is True
        assert message_filter("Performance Improvement") is True

    def test_empty_title(self) -> None:
        """Empty title has no negative keyword, so it passes (ambiguous)."""
        assert message_filter("") is True


# ── has_core_file ───────────────────────────────────────────────────


class TestHasCoreFile:
    def test_core_files_pass(self) -> None:
        assert has_core_file(["src/datasmith/core.py"]) is True
        assert has_core_file(["pandas/core/frame.py", "tests/test_frame.py"]) is True

    def test_only_tests_fail(self) -> None:
        assert has_core_file(["tests/test_foo.py", "test/test_bar.py"]) is False

    def test_only_docs_fail(self) -> None:
        assert has_core_file(["docs/index.rst", "doc/guide.md"]) is False

    def test_only_benchmarks_fail(self) -> None:
        assert has_core_file(["benchmarks/bench_sort.py"]) is False

    def test_only_ci_fail(self) -> None:
        assert has_core_file([".github/workflows/ci.yml"]) is False

    def test_only_prose_fail(self) -> None:
        assert has_core_file(["CHANGELOG.md", "CONTRIBUTING.rst"]) is False

    def test_mixed_with_one_core(self) -> None:
        assert (
            has_core_file([
                "tests/test_foo.py",
                "docs/guide.md",
                "src/module.py",  # core
            ])
            is True
        )

    def test_empty_list(self) -> None:
        assert has_core_file([]) is False


# ── estimate_tokens / check_patch_size ──────────────────────────────


class TestEstimateTokens:
    def test_returns_positive_for_non_empty(self) -> None:
        assert estimate_tokens("hello world") > 0

    def test_empty_string(self) -> None:
        assert estimate_tokens("") == 0

    def test_scales_with_length(self) -> None:
        short = estimate_tokens("a" * 100)
        long = estimate_tokens("a" * 10000)
        assert long > short


class TestCheckPatchSize:
    def test_too_short(self) -> None:
        assert check_patch_size("") is False
        assert check_patch_size("x") is False

    def test_reasonable_patch(self) -> None:
        # ~50 tokens — well within range
        patch = "--- a/file.py\n+++ b/file.py\n" + "\n".join(f"-old_line_{i}\n+new_line_{i}" for i in range(20))
        assert check_patch_size(patch) is True

    def test_too_large(self) -> None:
        # Generate a patch with > 16000 tokens (~80000 chars)
        huge = "x " * 80000
        assert check_patch_size(huge) is False


# ── threshold constants ─────────────────────────────────────────────


class TestThresholds:
    def test_max_files(self) -> None:
        assert MAX_FILES_CHANGED == 500

    def test_max_changes(self) -> None:
        assert MAX_TOTAL_CHANGES == 40_000


# ── check_file_compliance ───────────────────────────────────────────


class TestCheckFileCompliance:
    def test_passes_normal(self) -> None:
        files = [{"filename": "src/core.py", "additions": 10, "deletions": 5}]
        assert check_file_compliance(files) is True

    def test_fails_too_many_files(self) -> None:
        files = [{"filename": f"f{i}.py", "additions": 1, "deletions": 0} for i in range(500)]
        assert check_file_compliance(files) is False

    def test_fails_too_many_changes(self) -> None:
        files = [{"filename": "big.py", "additions": 30000, "deletions": 11000}]
        assert check_file_compliance(files) is False

    def test_fails_no_core_file(self) -> None:
        files = [{"filename": "tests/test_x.py", "additions": 5, "deletions": 3}]
        assert check_file_compliance(files) is False


# ── symbolic_compliance ─────────────────────────────────────────────


class TestSymbolicCompliance:
    def test_title_only_pass(self) -> None:
        assert symbolic_compliance("PERF: speed up groupby") is True

    def test_title_only_fail(self) -> None:
        assert symbolic_compliance("Fix typo in README") is False

    def test_with_good_patch(self) -> None:
        patch = "--- a/f.py\n+++ b/f.py\n" + "\n".join(f"-a{i}\n+b{i}" for i in range(20))
        assert symbolic_compliance("Optimize sort", patch=patch) is True

    def test_with_empty_patch(self) -> None:
        assert symbolic_compliance("Optimize sort", patch="") is False

    def test_with_huge_patch(self) -> None:
        assert symbolic_compliance("Optimize sort", patch="x " * 80000) is False

    def test_with_good_files(self) -> None:
        files = [{"filename": "src/core.py", "additions": 10, "deletions": 5}]
        assert symbolic_compliance("Speed up IO", file_changes=files) is True

    def test_with_bad_files(self) -> None:
        files = [{"filename": "tests/test_x.py", "additions": 5, "deletions": 3}]
        assert symbolic_compliance("Speed up IO", file_changes=files) is False

    def test_none_patch_skipped(self) -> None:
        """When patch is None, patch check is skipped (not failed)."""
        assert symbolic_compliance("PERF: faster path", patch=None) is True

    def test_none_files_skipped(self) -> None:
        """When file_changes is None, file check is skipped."""
        assert symbolic_compliance("PERF: faster path", file_changes=None) is True

    def test_all_checks(self) -> None:
        patch = "--- a/f.py\n+++ b/f.py\n" + "\n".join(f"-a{i}\n+b{i}" for i in range(20))
        files = [{"filename": "src/core.py", "additions": 10, "deletions": 5}]
        assert symbolic_compliance("Optimize sort", patch=patch, file_changes=files) is True

    def test_title_fail_short_circuits(self) -> None:
        """If title fails, patch and files don't matter."""
        patch = "--- a/f.py\n+++ b/f.py\n-old\n+new"
        files = [{"filename": "src/core.py", "additions": 1, "deletions": 1}]
        assert symbolic_compliance("Update docs", patch=patch, file_changes=files) is False


class TestModuleDocstringMatchesTheStageSplit:
    """The docstring is the first thing a reader trusts, so it has to be true.

    Stage 2 no longer drops anything: it stores every merged PR and records
    the symbolic verdict alongside.  ``check_patch_size`` runs from stage 3,
    where it gates the diff fetch and the LLM call rather than storage.
    """

    def test_docstring_does_not_claim_stage_2_drops_prs(self) -> None:
        import datasmith.filters as filters_mod

        doc = filters_mod.__doc__ or ""
        assert "avoid storing irrelevant PRs" not in doc
        assert "stage 3" in doc.lower()
        assert "stores **every** merged PR" in doc or "stores every merged PR" in doc


class TestCheckPatchSizeAvoidsTokenising:
    """Length decides wherever length is conclusive.

    ``tiktoken`` is CPU-bound BPE over the whole diff, and PostHog patches
    reach 150 KB. Stage 3 called this straight from its coroutine, so one large
    patch pinned the event loop for minutes: every other item stopped, the
    pacer stopped, and the stall logger meant to report it could not run
    either. The stage went silent and read as a deadlock.
    """

    def test_short_patch_needs_no_encoder(self) -> None:
        with patch("datasmith.filters.estimate_tokens", side_effect=AssertionError("tokenised")):
            assert check_patch_size("x" * (MIN_PATCH_TOKENS - 1)) is False

    def test_patch_shorter_than_the_ceiling_needs_no_encoder(self) -> None:
        """A token is at least one character, so this cannot exceed the ceiling."""
        with patch("datasmith.filters.estimate_tokens", side_effect=AssertionError("tokenised")):
            assert check_patch_size("x" * MAX_PATCH_TOKENS) is True

    def test_only_the_ambiguous_middle_is_encoded(self) -> None:
        with patch("datasmith.filters.estimate_tokens", return_value=MAX_PATCH_TOKENS + 1) as est:
            assert check_patch_size("x" * (MAX_PATCH_TOKENS * 4)) is False
            est.assert_called_once()

    def test_a_long_but_token_cheap_patch_still_passes(self) -> None:
        with patch("datasmith.filters.estimate_tokens", return_value=100):
            assert check_patch_size("x " * MAX_PATCH_TOKENS) is True
