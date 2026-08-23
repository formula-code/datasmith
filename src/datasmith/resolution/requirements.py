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
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from packaging.requirements import InvalidRequirement, Requirement

__all__ = ["Dropped", "parse_many", "parse_one", "render"]


@dataclass(frozen=True)
class Dropped:
    """A requirement string that could not be used, and why."""

    raw: str
    reason: str


#: Prefixes and shapes that are pip-file directives or build placeholders rather
#: than requirements.  They are dropped without being offered to the parser.
_NON_REQUIREMENT_PREFIXES = ("-", "--", "./", "../", ".")


def parse_one(raw: str) -> Requirement | None:
    """Parse one requirement string, or return ``None`` if it is not one."""
    text = raw.strip()
    if not text:
        return None
    if text.startswith(_NON_REQUIREMENT_PREFIXES):
        return None
    if "{" in text or "}" in text or "$" in text:
        return None
    try:
        return Requirement(text)
    except InvalidRequirement:
        return None


def parse_many(raws: Iterable[str]) -> tuple[list[Requirement], list[Dropped]]:
    """Parse many requirement strings, isolating failures.

    Returns ``(parsed, dropped)``.  Order is stable and duplicates are removed by
    rendered form, so the same inputs in a different order give the same output.
    Blank strings are skipped silently; anything else that fails is recorded.
    """
    parsed: list[Requirement] = []
    dropped: list[Dropped] = []
    seen: set[str] = set()

    for raw in raws:
        text = raw.strip() if raw else ""
        if not text:
            continue
        req = parse_one(text)
        if req is None:
            dropped.append(Dropped(raw=text, reason="unparseable requirement"))
            continue
        key = str(req)
        if key in seen:
            continue
        seen.add(key)
        parsed.append(req)

    return parsed, dropped


def render(reqs: Iterable[Requirement]) -> list[str]:
    """Render requirements to a sorted, stable list of strings."""
    return sorted({str(r) for r in reqs})
