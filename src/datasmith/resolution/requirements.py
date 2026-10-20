"""PEP 508 requirement parsing.

Strings are parsed with the real parser; one that does not parse is dropped with
a reason, never rewritten, so one bad string cannot break its siblings.  Inline
``# comments`` are removed and bare archive or VCS URLs are kept as uv reads them.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from packaging.requirements import InvalidRequirement, Requirement

__all__ = [
    "Dropped",
    "is_direct_url_line",
    "parse_many",
    "parse_one",
    "render",
    "strip_inline_comment",
    "to_requirement_lines",
]


@dataclass(frozen=True)
class Dropped:
    """A requirement string that could not be used, and why."""

    raw: str
    reason: str


#: Why a string was not usable; stored next to the string, so each names the actual cause.
REASON_DIRECTIVE = "requirements-file directive, not a requirement"
REASON_LOCAL_PATH = "local path, not a PyPI requirement"
REASON_PLACEHOLDER = "unexpanded template placeholder"
REASON_UNPARSEABLE = "unparseable requirement"

#: Schemes uv accepts as a bare requirements-file line.
_DIRECT_URL_PREFIXES = ("http://", "https://", "git+", "hg+", "svn+", "bzr+", "file://")

#: ``#`` opens a comment at the start of a line or after whitespace -- pip's own
#: rule.  A ``#`` inside a URL fragment has no whitespace before it.
_INLINE_COMMENT_RE = re.compile(r"(?:^|\s+)#.*$")


def strip_inline_comment(text: str) -> str:
    """Remove a requirements-file comment from a line, and surrounding whitespace.

    ``numpy>=1.25  # pinned for the ABI`` states the requirement ``numpy>=1.25``;
    the comment is file syntax.  ``foo @ https://h/foo.whl#sha256=...`` has no
    whitespace before its ``#``, so the URL fragment survives untouched.
    """
    return _INLINE_COMMENT_RE.sub("", text).strip()


def is_direct_url_line(text: str) -> bool:
    """True when the line is a bare URL requirement.

    ``https://host/pkg-1.0-py3-none-any.whl`` is a valid requirements-file line
    that uv installs, but PEP 508 has no syntax for it, so ``Requirement``
    rejects it.  Such a line is handed to uv verbatim rather than dropped.
    """
    head = text.split(";", 1)[0].strip()
    if not head.startswith(_DIRECT_URL_PREFIXES):
        return False
    return not any(ch.isspace() for ch in head)


def _rejection_reason(text: str) -> str | None:
    """Name the reason a string is not a requirement at all, or ``None``."""
    if text.startswith("-"):
        # Also refuses ``-e <url>``: an editable install belongs to the build, not the dependency seed.
        return REASON_DIRECTIVE
    if text.startswith("."):
        return REASON_LOCAL_PATH
    if "{" in text or "}" in text or "$" in text:
        return REASON_PLACEHOLDER
    return None


def _parse_one(raw: str) -> tuple[Requirement | None, str | None]:
    """Parse one line, returning ``(requirement, reason)``; exactly one is set.

    Both are ``None`` for a blank or comment-only line: nothing was stated, so
    nothing was lost.
    """
    text = strip_inline_comment(raw)
    if not text:
        return None, None
    reason = _rejection_reason(text)
    if reason is not None:
        return None, reason
    try:
        return Requirement(text), None
    except InvalidRequirement:
        return None, REASON_UNPARSEABLE


def parse_one(raw: str) -> Requirement | None:
    """Parse one requirement string, or return ``None`` if it is not one."""
    req, _reason = _parse_one(raw)
    return req


def parse_many(raws: Iterable[str]) -> tuple[list[Requirement], list[Dropped]]:
    """Parse many requirement strings, isolating failures.

    Returns ``(parsed, dropped)``.  Order is stable and duplicates are removed by
    rendered form, so the same inputs in a different order give the same output.
    Blank strings are skipped silently; anything else that fails is recorded.

    A bare URL line has no PEP 508 form, so it cannot appear in ``parsed`` and is
    reported as dropped.  Use :func:`to_requirement_lines` whenever the answer
    feeds a resolver or a stored record -- it keeps such a line.
    """
    parsed: list[Requirement] = []
    dropped: list[Dropped] = []
    seen: set[str] = set()

    for raw in raws:
        text = strip_inline_comment(raw) if raw else ""
        if not text:
            continue
        req, reason = _parse_one(text)
        if req is None:
            dropped.append(Dropped(raw=text, reason=reason or REASON_UNPARSEABLE))
            continue
        key = str(req)
        if key in seen:
            continue
        seen.add(key)
        parsed.append(req)

    return parsed, dropped


def to_requirement_lines(raws: Iterable[str]) -> tuple[list[str], list[Dropped]]:
    """Turn requirements-file lines into the exact text to hand to uv.

    Every input line is either returned or listed in ``dropped`` with its reason.
    Prefer it over :func:`parse_many` for anything that feeds a resolver, because
    it keeps bare URL lines.  Output is sorted and deduplicated.
    """
    lines: list[str] = []
    dropped: list[Dropped] = []
    seen: set[str] = set()

    for raw in raws:
        text = strip_inline_comment(raw) if raw else ""
        if not text:
            continue
        if is_direct_url_line(text):
            candidate = text
        else:
            req, reason = _parse_one(text)
            if req is None:
                dropped.append(Dropped(raw=text, reason=reason or REASON_UNPARSEABLE))
                continue
            candidate = str(req)
        if candidate in seen:
            continue
        seen.add(candidate)
        lines.append(candidate)

    return sorted(lines), dropped


def render(reqs: Iterable[Requirement]) -> list[str]:
    """Render requirements to a sorted, stable list of strings."""
    return sorted({str(r) for r in reqs})
