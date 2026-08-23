r"""PEP 508 requirement parsing.

The predecessor -- the marker-spacing repair in ``package_filters``, audit B1 --
tried to fix markers with two unanchored substitutions::

    re.sub(r"(?<=[^\s])and(?=[^\s])", " and ", marker)
    re.sub(r"(?<=[^\s])or(?=[^\s])",  " or ",  marker)

``or`` occurs inside ``platform`` and ``and`` occurs inside ``standard``, so it
turned ``platform_system`` into ``platf or m_system`` and ``extra=='standard'``
into ``extra=='st and ard'``.  uv then refused to parse the result and the whole
compile failed, which is how one ``pyuwsgi`` requirement removed apache/arrow
from the dataset.

The rule here is the opposite: parse with the real parser, and if a string does
not parse, drop it and say so.  Never rewrite a requirement, and never let one
bad string decide the fate of its siblings.

Two shapes are requirements-*file* syntax rather than requirement syntax, and
both are handled here so that routing a file line through the parser does not
lose it:

* an inline ``# comment``, which is not part of the requirement the line states;
* a bare archive or VCS URL, which uv installs from a requirements file but for
  which PEP 508 has no syntax at all.

Removing a comment is not a rewrite of a requirement -- it is reading the line
the way the file format defines it, exactly as uv reads the same line.
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


#: Why a string was not usable.  The reason is stored next to the string, so it
#: has to name the actual cause -- a pip directive is not "unparseable", it was
#: never offered to the parser.
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

    This is the single entry point for "what does this commit ask uv to
    install".  Every line it returns is in the seed and every line it refuses is
    in ``dropped`` with its reason, so the two lists together account for the
    input.  Prefer it over :func:`parse_many` for anything that feeds a resolver
    or a stored record: a bare archive URL belongs in the seed but has no PEP 508
    form, so ``parse_many`` alone would report a shipped requirement as dropped.

    Output is sorted and deduplicated, so the same input in a different order
    gives the same answer.
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
