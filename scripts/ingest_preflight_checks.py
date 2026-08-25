"""Live checks for an fc-data stage 2-5 run: GitHub budget, model, exclusions.

Split out of ingest_preflight.sh because a Python heredoc nested inside a shell
heredoc silently produced no output -- a check that cannot fail is worse than no
check, since it reads as a pass.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

import datasmith  # noqa: F401  (loads tokens.env)

OK, BAD, WARN = "  \033[32mOK\033[0m    ", "  \033[31mFAIL\033[0m  ", "  \033[33mWARN\033[0m  "
failed = False


def _fail(msg: str) -> None:
    global failed
    failed = True
    print(BAD + msg)


async def check_github() -> None:
    from datasmith.github.client import GitHubClient
    from datasmith.utils.tokens import TokenPool

    print("== github ==")
    try:
        pool = TokenPool()
    except Exception as exc:
        _fail(f"no GitHub token: {exc}")
        return
    gh = GitHubClient(pool)
    try:
        resp = await gh._request("GET", "/rate_limit")
        res = resp.json()["resources"]
    except Exception as exc:
        _fail(f"GitHub unreachable: {exc}")
        return
    finally:
        await gh.close()
    core, gql = res["core"], res["graphql"]
    mins = int((core["reset"] - time.time()) / 60)
    print(
        f"{OK}tokens={pool.size}  REST core {core['remaining']}/{core['limit']} "
        f"(resets {mins}m)  graphql {gql['remaining']}/{gql['limit']}"
    )
    # Stage 3 spends one REST call per PR that has no stored patch.
    per_hour = pool.size * 5000
    print(f"{OK}stage 3 diff budget: ~{per_hour:,} fetches/hour across {pool.size} token(s)")
    if pool.size == 1:
        print(f"{WARN}one token: a month of ingestion crosses a rate-limit reset by design")


def check_model() -> None:
    """Confirm the LLM endpoint is up and its context fits what we will send.

    vLLM ports are ephemeral: a restart moves them, and a stale DSPY_API_BASE is
    a silent stall rather than an error. On failure this prints the live ports so
    the fix is visible instead of deduced.
    """
    import httpx

    print("== model (stage 3 + 5) ==")
    base = (os.environ.get("DSPY_API_BASE") or "").rstrip("/")
    if not base:
        _fail("DSPY_API_BASE is unset")
        return
    try:
        resp = httpx.get(
            f"{base}/models",
            headers={"Authorization": f"Bearer {os.environ.get('DSPY_API_KEY', 'EMPTY')}"},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()["data"][0]
    except Exception as exc:
        _fail(f"model endpoint {base} is DOWN ({type(exc).__name__})")
        print(f"        live vLLM ports right now: {', '.join(_live_vllm_ports()) or 'none'}")
        return

    print(f"{OK}{data['id']} at {base}")
    ctx = int(data.get("max_model_len") or 0)
    patch = int(os.environ.get("DATASMITH_CLASSIFY_PATCH_TOKENS", "12000"))
    out = int(os.environ.get("DSPY_MAX_TOKENS", "6144"))
    need = patch + out + 2000  # + the prompt and the PR body
    if ctx and ctx < need:
        _fail(
            f"context {ctx:,} < patch+output+prompt ({need:,}); lower "
            "DATASMITH_CLASSIFY_PATCH_TOKENS or DSPY_MAX_TOKENS, or raise the server's context"
        )
    else:
        print(f"{OK}context {ctx:,} >= {need:,} needed")
    if out > 8192:
        print(f"{WARN}DSPY_MAX_TOKENS={out} is an OUTPUT cap; every request reserves it from the context")


def _live_vllm_ports() -> list[str]:
    """Ports of the vLLM servers running right now, for a stale-endpoint message."""
    import re
    import subprocess

    try:
        ps = subprocess.run(["ps", "-eo", "cmd"], capture_output=True, text=True, check=False).stdout
    except Exception:
        return []
    return sorted({
        m
        for line in ps.splitlines()
        if "vllm serve" in line and "grep" not in line
        for m in re.findall(r"--port (\d+)", line)
    })


def check_exclusions() -> None:
    from datasmith.utils.db import excluded_repos

    print("== exclusions ==")
    try:
        skip = excluded_repos()
    except Exception as exc:
        print(f"{WARN}could not read exclusions: {exc}")
        return
    if not skip:
        print(f"{WARN}no repositories excluded (is migration 00030 applied?)")
        return
    print(f"{OK}{len(skip)} repository(ies) excluded from stages 2 and 3")
    for owner, repo in sorted(skip):
        print(f"          {owner}/{repo}")


def main() -> int:
    asyncio.run(check_github())
    check_model()
    check_exclusions()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
