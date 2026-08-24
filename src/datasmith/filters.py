"""Cheap attribute-compliance filters for PR pre-screening.

Split across two stages, because only one of the three components needs a
diff.  Stage 2 stores **every** merged PR and evaluates the two components
GraphQL can answer -- the title keyword filter and file compliance --
recording the verdict in ``is_performance_commit_symbolic``.  Nothing is
dropped there.  ``check_patch_size`` runs from stage 3, where it gates the
diff fetch and the LLM call rather than storage.

Implements the attribute compliance checks from the design docs:

- ``message_filter``: positive perf keywords AND NOT negative keywords
- ``has_core_file``: at least one changed file is not test/doc/benchmark/CI
- ``check_patch_size``: patch token count within bounds (stage 3)
- Size limits: max total changes, max files changed
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Positive patterns: title keywords suggesting performance work
# ---------------------------------------------------------------------------
_POSITIVE_RE = re.compile(
    r"\b("
    r"perf(?:orm(?:ance)?)?|"
    r"optimi[sz]e[ds]?|optimi[sz]ation|"
    r"speed(?:up|-up| up)?|"
    r"fast(?:er)?|"
    r"slow(?:er|down|-down| down|ness)?|"
    r"cach(?:e[ds]?|ing)|"
    r"parall(?:el(?:i[sz])?(?:e[ds]?)?|ism)|"
    r"benchmark|"
    r"throughput|"
    r"latenc(?:y|ies)?|"
    r"bottleneck|"
    r"profil(?:e[ds]?|ing)|"
    r"vectori[sz](?:e[ds]?|ation)|"
    r"accelerat(?:e[ds]?|ion)|"
    r"efficien(?:t|cy)|"
    r"regress(?:ion|ed)?|"
    r"memory|mem(?:ory)?|"
    r"reduc(?:e[ds]?|ing|tion)|"
    r"batch(?:ed|ing)?|"
    r"inline[ds]?|inlining|"
    r"async(?:io)?|"
    r"lazy|"
    r"gpu|cpu|"
    r"chunk(?:ed|ing|s)?|"
    r"compil(?:e[ds]?|ation)|jit|"
    r"simd|sse|avx|neon|"
    r"overhead|"
    r"allocat(?:e[ds]?|ion[s]?)|preallocat(?:e[ds]?|ion)|"
    r"memoi[sz](?:e[ds]?|ation)|precomput(?:e[ds]?|ation)|"
    r"concurren(?:t|cy)|"
    r"buffer(?:ed|ing|s)?|"
    r"compress(?:ed|ion)?|"
    r"unroll(?:ed|ing)?|"
    r"dedup(?:licat(?:e[ds]?|ion))?|"
    r"pool(?:ing|ed)?|"
    r"streaming"
    r")\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Negative patterns: title keywords suggesting non-performance changes
# ---------------------------------------------------------------------------
_NEGATIVE_RE = re.compile(
    r"\b("
    r"doc(?:s|umentation)|"
    r"readme|"
    r"changelog|"
    r"ci(?:[/-]cd)?|"
    r"github.actions?|"
    r"packag(?:e|ing)|"
    r"version(?:ing)?|"
    r"bump|"
    r"releas(?:e[ds]?|ing)|"
    r"format(?:ting)?|"
    r"lint(?:ing|er)?|"
    r"typ(?:o|os)|"
    r"type[- ]?hints?|"
    r"annotations?|"
    r"deprecat(?:e[ds]?|ion)|"
    r"revert(?:ed|ing)?|"
    r"backport|"
    r"docstring[s]?|"
    r"autoupdate|"
    r"pre-commit"
    r")\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Non-core file patterns (tests, docs, benchmarks, CI, prose)
# ---------------------------------------------------------------------------
_NON_CORE_RE = re.compile(
    r"(^|/)(?:tests?|doc[s]?|benchmarks?|\.github)(/|$)"
    r"|\.(?:rst|md)$",
)

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
MAX_TOTAL_CHANGES = 40_000
MAX_FILES_CHANGED = 500
MIN_PATCH_TOKENS = 5
MAX_PATCH_TOKENS = 16_000


def message_filter(title: str) -> bool:
    """Return True if the PR title suggests a performance improvement.

    Passes if the title matches at least one positive keyword OR
    does NOT match any negative keyword.  This is intentionally loose:
    a positive keyword is strong signal, and the absence of negative
    keywords indicates ambiguity worth sending to the LLM classifier.
    """
    return bool(_POSITIVE_RE.search(title)) or not bool(_NEGATIVE_RE.search(title))


def has_core_file(filenames: list[str]) -> bool:
    """Return True if at least one changed file is a core source file.

    A "core" file is any file that is NOT exclusively a test, doc,
    benchmark, CI config, or prose (.rst/.md) file.
    """
    return any(not _NON_CORE_RE.search(f) for f in filenames)


def estimate_tokens(text: str) -> int:
    """Estimate token count.  Uses tiktoken if available, else len/4."""
    try:
        import tiktoken

        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text))
    except Exception:
        return len(text) // 4


def check_patch_size(patch: str) -> bool:
    """Return True if patch token count is within acceptable bounds.

    Decides from length alone whenever length is conclusive, because
    tokenising is not cheap and this runs on every candidate PR.  A token is
    at least one character, so:

    * fewer than ``MIN_PATCH_TOKENS`` characters cannot reach the floor;
    * at most ``MAX_PATCH_TOKENS`` characters cannot exceed the ceiling.

    Only a patch between those two bounds is genuinely ambiguous and worth
    encoding.  This matters more than it looks: ``tiktoken`` is CPU-bound BPE,
    and PostHog diffs reach 150 KB.  Stage 3 called this straight from its
    coroutine, so a single large patch stalled the whole event loop -- every
    other item, the pacer, and the logging that was supposed to report the
    stall.  The caller now also runs it off the loop; this keeps most calls
    from needing a thread at all.
    """
    length = len(patch)
    if length < MIN_PATCH_TOKENS:
        return False
    if length <= MAX_PATCH_TOKENS:
        return True
    n = estimate_tokens(patch)
    return MIN_PATCH_TOKENS <= n <= MAX_PATCH_TOKENS


def check_file_compliance(file_changes: list[dict[str, Any]]) -> bool:
    """Run file-level compliance checks on a list of file-change dicts.

    Each dict should have ``filename``, ``additions``, ``deletions``.
    Returns True if the PR passes all file-level checks.
    """
    if len(file_changes) >= MAX_FILES_CHANGED:
        return False
    total = sum(f.get("additions", 0) + f.get("deletions", 0) for f in file_changes)
    if total >= MAX_TOTAL_CHANGES:
        return False
    filenames = [f.get("filename", "") for f in file_changes]
    return not (filenames and not has_core_file(filenames))


def symbolic_compliance(
    title: str,
    patch: str | None = None,
    file_changes: list[dict[str, Any]] | None = None,
) -> bool:
    """Evaluate all available attribute-compliance filters.

    Returns True only if every evaluable filter passes:

    * **message_filter** on *title* (always evaluated).
    * **check_patch_size** on *patch* (skipped if *patch* is ``None``).
    * **check_file_compliance** on *file_changes* (skipped if ``None``).
    """
    if not message_filter(title):
        return False
    if patch is not None and not check_patch_size(patch):
        return False
    return not (file_changes is not None and not check_file_compliance(file_changes))
