"""Regex helpers shared across modules."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from datasmith.logging_config import get_logger

logger = get_logger("core.text_utils")

_FLAG_MAP = {
    "i": re.IGNORECASE,
    "m": re.MULTILINE,
    "s": re.DOTALL,
    "x": re.VERBOSE,
}


def parse_flag_string(flag_str: str) -> int:
    """Convert inline regex flag strings (e.g. ``"im"``) to a bitmask."""
    flags = 0
    for ch in flag_str:
        flags |= _FLAG_MAP.get(ch, 0)
    return flags


def compile_patterns(raw_patterns: Iterable[str], base_flags: int = 0) -> list[re.Pattern[str]]:
    """Compile ``raw_patterns`` applying ``base_flags`` and trailing delimiters."""
    compiled: list[re.Pattern[str]] = []
    for raw in raw_patterns:
        pattern = raw
        flags = base_flags

        match = re.fullmatch(r"/(.*?)/([imsx]*)", raw)
        if match:
            pattern = match.group(1)
            flags |= parse_flag_string(match.group(2))

        try:
            compiled.append(re.compile(pattern, flags))
        except re.error as exc:
            logger.warning("Ignoring invalid regex %r: %s", raw, exc)
    return compiled


def any_match(patterns: Iterable[re.Pattern[str]] | str, text: str) -> bool:
    """Return True if ``text`` matches any regex in ``patterns``."""
    if isinstance(patterns, str):
        return False
    return any(pattern.search(text) for pattern in patterns)


def get_grep_params(qs: dict[str, list[str]]) -> dict[str, Any]:
    """Parse CLI-style grep params into compiled regex buckets."""
    base_flags = parse_flag_string(qs.get("grep_flags", [""])[0])
    pos_any = compile_patterns(qs.get("grep", []), base_flags)
    pos_title = compile_patterns(qs.get("grep_title", []), base_flags)
    pos_msg = compile_patterns(qs.get("grep_msg", []), base_flags)

    neg_any = compile_patterns(qs.get("grep_not", []), base_flags)
    neg_title = compile_patterns(qs.get("grep_title_not", []), base_flags)
    neg_msg = compile_patterns(qs.get("grep_msg_not", []), base_flags)

    grep_mode = (qs.get("grep_mode", ["any"])[0] or "any").lower()
    if grep_mode not in {"any", "all"}:
        grep_mode = "any"

    return {
        "pos_any": pos_any,
        "pos_title": pos_title,
        "pos_msg": pos_msg,
        "neg_any": neg_any,
        "neg_title": neg_title,
        "neg_msg": neg_msg,
        "grep_mode": grep_mode,
    }


def neg_matches(grep_params: dict[str, Any], title: str, message: str) -> bool:
    """Return True if any negative pattern matches title/message."""
    return (
        any_match(grep_params["neg_any"], title)
        or any_match(grep_params["neg_any"], message)
        or any_match(grep_params["neg_title"], title)
        or any_match(grep_params["neg_msg"], message)
    )


def pos_matches(grep_params: dict[str, Any], title: str, message: str) -> bool:
    """Return True if positive patterns match according to configured mode."""
    if grep_params["pos_any"] or grep_params["pos_title"] or grep_params["pos_msg"]:
        checks: list[bool] = []
        if grep_params["pos_any"]:
            checks.append(any_match(grep_params["pos_any"], title) or any_match(grep_params["pos_any"], message))
        if grep_params["pos_title"]:
            checks.append(any_match(grep_params["pos_title"], title))
        if grep_params["pos_msg"]:
            checks.append(any_match(grep_params["pos_msg"], message))

        if grep_params["grep_mode"] == "any":
            if not any(checks):
                return False
        else:
            if not all(checks):
                return False
    return True


__all__ = [
    "any_match",
    "compile_patterns",
    "get_grep_params",
    "neg_matches",
    "parse_flag_string",
    "pos_matches",
]
