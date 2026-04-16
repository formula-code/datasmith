"""Tests for the CLI-agent rate-limit detector."""

from __future__ import annotations

import datetime
import json

from datasmith.agents.rate_limit import check, detect

CODEX_HEADER = '{"type":"thread.started","thread_id":"abc"}\n{"type":"turn.started"}\n'

CODEX_ERROR_EVENT = (
    '{"type":"error","message":"You\'ve hit your usage limit. Upgrade to Plus to '
    "continue using Codex (https://chatgpt.com/explore/plus), or try again at "
    'Apr 11th, 2026 2:32 PM."}\n'
)

CODEX_TURN_FAILED = (
    '{"type":"turn.failed","error":{"message":"You\'ve hit your usage limit. '
    "Upgrade to Plus to continue using Codex (https://chatgpt.com/explore/plus), "
    'or try again at Apr 16th, 2026 4:54 PM."}}\n'
)


def test_codex_usage_limit_parses_reset_time() -> None:
    raw = CODEX_HEADER + CODEX_ERROR_EVENT + CODEX_TURN_FAILED
    blocked, reset = check("codex", raw)
    assert blocked
    assert reset == datetime.datetime(2026, 4, 16, 16, 54, tzinfo=datetime.UTC)


def test_codex_usage_limit_without_reset_still_flags() -> None:
    # Error present but garbled reset text.
    raw = CODEX_HEADER + '{"type":"error","message":"You\'ve hit your usage limit, try again later."}\n'
    blocked, reset = check("codex", raw)
    assert blocked is True
    assert reset is None


def test_codex_clean_run_not_flagged() -> None:
    raw = CODEX_HEADER + '{"type":"turn.completed","usage":{"input_tokens":4290}}\n'
    blocked, reset = check("codex", raw)
    assert blocked is False
    assert reset is None


def test_claude_allowed_status_not_flagged() -> None:
    evt = {
        "type": "rate_limit_event",
        "rate_limit_info": {
            "status": "allowed",
            "resetsAt": 1775559600,
            "rateLimitType": "five_hour",
            "overageStatus": "allowed",
            "overageResetsAt": 1777593600,
            "isUsingOverage": False,
        },
    }
    raw = json.dumps(evt) + "\n"
    blocked, _ = check("claude", raw)
    assert blocked is False


def test_claude_allowed_warning_not_flagged() -> None:
    evt = {
        "type": "rate_limit_event",
        "rate_limit_info": {
            "status": "allowed_warning",
            "resetsAt": 1775559600,
            "rateLimitType": "five_hour",
            "utilization": 0.93,
            "surpassedThreshold": 0.9,
        },
    }
    raw = json.dumps(evt) + "\n"
    blocked, _ = check("claude", raw)
    assert blocked is False


def test_claude_blocked_status_returns_reset_time() -> None:
    evt = {
        "type": "rate_limit_event",
        "rate_limit_info": {
            "status": "exceeded",
            "resetsAt": 1775559600,
            "rateLimitType": "weekly",
            "overageStatus": "exceeded",
            "overageResetsAt": 1777593600,
            "isUsingOverage": True,
        },
    }
    raw = json.dumps(evt) + "\n"
    blocked, reset = check("claude", raw)
    assert blocked is True
    assert reset == datetime.datetime.fromtimestamp(1775559600, tz=datetime.UTC)


def test_claude_uses_latest_event() -> None:
    # Two events: the older is allowed, the newer is exceeded.
    ok = {
        "type": "rate_limit_event",
        "rate_limit_info": {"status": "allowed", "resetsAt": 1775000000},
    }
    bad = {
        "type": "rate_limit_event",
        "rate_limit_info": {"status": "blocked", "resetsAt": 1775559600},
    }
    raw = json.dumps(ok) + "\n" + json.dumps(bad) + "\n"
    blocked, reset = check("claude", raw)
    assert blocked is True
    assert reset == datetime.datetime.fromtimestamp(1775559600, tz=datetime.UTC)


def test_empty_output_not_flagged() -> None:
    assert check("codex", "") == (False, None)
    assert check("claude", "") == (False, None)


def test_unknown_agent_not_flagged() -> None:
    assert check("gemini", CODEX_ERROR_EVENT) == (False, None)


def test_detect_returns_none_when_not_limited() -> None:
    assert detect("codex", "ok done\n") is None
