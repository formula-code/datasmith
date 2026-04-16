"""Detect CLI-agent rate-limit / budget-exhaustion errors in raw output.

Stage 6 (synthesize_images) drives `codex` and `claude` CLI agents against
a shared weekly budget. When the budget runs out, each subsequent attempt
fails in ~2s with an unhelpful `failure_stage='aborted'` unless we
recognise the pattern. This module parses the raw JSONL stream emitted
by either CLI and, if a rate-limit signal is present, returns the
budget-reset timestamp so the runner can pause until the limit clears.

Detection schemas (observed in error_logs):

* **Codex** — free-text error event:
      {"type":"error","message":"You've hit your usage limit. ... try again
       at Apr 11th, 2026 2:32 PM."}
  The reset time is a tz-naive, human-readable string. We parse it with
  dateutil and assume UTC since the Codex CLI does not include a zone.

* **Claude** — structured `rate_limit_event` with `resetsAt` (unix epoch):
      {"type":"rate_limit_event","rate_limit_info":{
         "status":"allowed|allowed_warning|exceeded|blocked|...",
         "resetsAt":1775559600,
         "rateLimitType":"five_hour|weekly|...",
         "overageStatus":"...", "overageResetsAt":...}}
  Status values `allowed` and `allowed_warning` are non-blocking. Any
  other status is treated as rate-limited.
"""

from __future__ import annotations

import datetime
import json
import re

# Claude statuses that are NOT rate-limited. Anything else we see means the
# CLI is signalling that the next turn will be blocked or has been blocked.
_CLAUDE_OK_STATUSES = frozenset({"allowed", "allowed_warning"})

# Matches the human-readable reset time embedded in Codex usage-limit errors.
# Example: "try again at Apr 11th, 2026 2:32 PM."
_CODEX_RESET_RE = re.compile(
    r"try again at\s+"
    r"(?P<month>[A-Za-z]{3,9})\s+"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?,\s+"
    r"(?P<year>\d{4})\s+"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
    r"(?P<ampm>AM|PM)",
    re.IGNORECASE,
)

_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


class RateLimitError(RuntimeError):
    """Raised by the synthesizer when an agent attempt hit a usage limit.

    ``reset_at`` is always timezone-aware UTC when known, or ``None`` if
    the CLI signalled a rate limit without giving a reset timestamp (in
    which case the caller should pick a default pause duration).
    """

    def __init__(
        self,
        agent_name: str,
        reset_at: datetime.datetime | None,
        message: str = "",
    ) -> None:
        self.agent_name = agent_name
        self.reset_at = reset_at
        self.message = message or f"{agent_name} hit usage limit"
        super().__init__(self.message)


def detect(agent_name: str, raw_output: str) -> datetime.datetime | None | object:
    """Return the reset timestamp if *raw_output* shows a rate-limit hit.

    Returns:
        * ``None`` — not rate-limited.
        * ``False`` sentinel (via ``_NO_RESET``) — rate-limited but no
          reset time could be parsed.
        * ``datetime`` (UTC, tz-aware) — rate-limited and we know when the
          budget will clear.
    """
    if not raw_output:
        return None

    if agent_name == "codex" or "codex" in agent_name.lower():
        return _detect_codex(raw_output)
    if agent_name == "claude" or "claude" in agent_name.lower():
        return _detect_claude(raw_output)
    return None


# Sentinel returned when we know a rate limit happened but couldn't parse a
# reset time. ``None`` already means "no rate limit", so we need a distinct
# "yes but unknown reset" value.
_NO_RESET: object = object()


def _detect_codex(raw_output: str) -> datetime.datetime | None | object:  # noqa: C901
    """Parse the codex JSONL stream for a usage-limit error event."""
    if "usage limit" not in raw_output.lower():
        return None

    # Walk the last few lines — the error is always near the tail.
    for line in reversed(raw_output.splitlines()[-20:]):
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            continue

        msg = ""
        if evt.get("type") == "error":
            msg = evt.get("message") or ""
        elif evt.get("type") == "turn.failed":
            err = evt.get("error") or {}
            msg = err.get("message") or ""
        if not msg or "usage limit" not in msg.lower():
            continue

        m = _CODEX_RESET_RE.search(msg)
        if not m:
            return _NO_RESET
        try:
            return _parse_codex_reset(m)
        except ValueError:
            return _NO_RESET

    # Fell through without matching a structured event but the substring
    # is present — still treat it as rate limited.
    m = _CODEX_RESET_RE.search(raw_output)
    if m:
        try:
            return _parse_codex_reset(m)
        except ValueError:
            return _NO_RESET
    return _NO_RESET


def _parse_codex_reset(match: re.Match[str]) -> datetime.datetime:
    month = _MONTHS[match.group("month")[:3].lower()]
    day = int(match.group("day"))
    year = int(match.group("year"))
    hour = int(match.group("hour")) % 12
    if match.group("ampm").upper() == "PM":
        hour += 12
    minute = int(match.group("minute"))
    # Codex does not disclose a timezone for the reset time. Assume UTC —
    # this is slightly conservative (may resume up to a few hours early or
    # late) but correct to within a budget cycle.
    return datetime.datetime(year, month, day, hour, minute, tzinfo=datetime.UTC)


def _detect_claude(raw_output: str) -> datetime.datetime | None | object:  # noqa: C901
    """Parse the claude JSONL stream for a rate_limit_event with a blocking status."""
    if "rate_limit_event" not in raw_output and "rate_limit" not in raw_output.lower():
        return None

    # Scan all lines; Claude emits a rate_limit_event after every turn, so
    # the *last* one is authoritative about current budget state.
    last_reset: int | None = None
    last_overage_reset: int | None = None
    blocked = False
    for line in raw_output.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            continue
        if evt.get("type") != "rate_limit_event":
            continue
        info = evt.get("rate_limit_info") or {}
        status = info.get("status")
        overage_status = info.get("overageStatus")
        # Track the most recent values.
        if isinstance(info.get("resetsAt"), int | float):
            last_reset = int(info["resetsAt"])
        if isinstance(info.get("overageResetsAt"), int | float):
            last_overage_reset = int(info["overageResetsAt"])
        if status and status not in _CLAUDE_OK_STATUSES:
            blocked = True
        if overage_status and overage_status not in _CLAUDE_OK_STATUSES:
            blocked = True

    if not blocked:
        return None

    # Prefer the standard reset over the overage reset if both are present.
    reset_epoch = last_reset or last_overage_reset
    if reset_epoch is None:
        return _NO_RESET
    return datetime.datetime.fromtimestamp(reset_epoch, tz=datetime.UTC)


def check(agent_name: str, raw_output: str) -> tuple[bool, datetime.datetime | None]:
    """Convenience wrapper returning ``(is_rate_limited, reset_at_or_None)``."""
    result = detect(agent_name, raw_output)
    if result is None:
        return False, None
    if result is _NO_RESET:
        return True, None
    return True, result  # type: ignore[return-value]
