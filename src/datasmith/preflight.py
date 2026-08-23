"""Startup environment checks, plus a report of what this machine actually has.

Two kinds of line come out of here and they are deliberately different.  A
*check* can fail and block the run.  A *report* never can: spec section 6.4
requires an operator on a new machine to see their real limits — token pool
size, database round-trip latency, resolved concurrency caps — and none of
those has a threshold that is wrong everywhere.  A 32.7 ms round trip to
``db.formulacode.org`` is slower than 5.1 ms to local Postgres and is still a
perfectly legitimate way to run the pipeline, so measuring it must not be able
to refuse the run.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from datasmith.utils import get_logger

logger = get_logger("preflight")

# Round trips used to measure database latency.  The first is discarded: it
# pays for connection setup and TLS, which is a one-off cost and not the
# per-query cost the number is meant to describe.
DATASMITH_PREFLIGHT_DB_PINGS: int = int(os.environ.get("DATASMITH_PREFLIGHT_DB_PINGS", "5"))


def _check(name: str, condition: bool, detail: str = "") -> bool:
    status = "OK" if condition else "FAIL"
    msg = f"  [{status}] {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    return condition


def _report(name: str, detail: str) -> None:
    """Print an observation that must never gate the run."""
    print(f"  [INFO] {name} — {detail}")


def resolved_concurrency_caps() -> list[tuple[str, int | float]]:
    """Return every concurrency and pacing dial with the value this run will use.

    Pure, and importing-from-modules only, so it can be tested offline.  The
    values are read from the modules rather than from ``os.environ`` directly:
    that is where the fallback defaults live, so an operator who has set
    nothing still sees the real numbers instead of an empty list.
    """
    from datasmith.github import client as gh_client
    from datasmith.runners import classify_prs, render_problems, resolve_packages, scrape_commits

    return [
        ("stage 2 repositories in flight", scrape_commits.DATASMITH_SCRAPE_COMMITS_CONCURRENCY),
        ("stage 2 search POSTs (bisection fan-out)", gh_client.DATASMITH_GH_SEARCH_CONCURRENCY),
        ("stage 2 files-fallback requests", gh_client.DATASMITH_GH_FILES_FALLBACK_CONCURRENCY),
        ("stage 3 diff fetches", classify_prs.DATASMITH_CLASSIFY_DIFF_CONCURRENCY),
        ("stage 3 diff min interval (s)", classify_prs.DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S),
        ("stage 3 LLM worker threads", classify_prs.DATASMITH_CLASSIFY_LLM_WORKERS),
        ("stage 4 resolver worker threads", resolve_packages.DATASMITH_RESOLVE_PACKAGES_WORKERS),
        ("stage 5 render worker threads", render_problems.DATASMITH_RENDER_PROBLEMS_WORKERS),
        ("GitHub retries per request", gh_client.DATASMITH_GH_RETRIES),
    ]


def run_preflight() -> bool:  # noqa: C901
    """Verify all prerequisites for the DataSmith pipeline."""
    print("DataSmith Preflight Check")
    print("=" * 40)

    all_ok = True

    # 1. Environment variables
    print("\n== Environment ==")
    supabase_url = os.environ.get("SUPABASE_URL", "")
    all_ok &= _check(
        "SUPABASE_URL", bool(supabase_url), supabase_url[:30] + "..." if len(supabase_url) > 30 else supabase_url
    )

    supabase_key = os.environ.get("SUPABASE_KEY", "")
    all_ok &= _check("SUPABASE_KEY", bool(supabase_key), "***" if supabase_key else "")

    gh_tokens = os.environ.get("GH_TOKENS", os.environ.get("GH_TOKEN", ""))
    token_count = len([t for t in gh_tokens.split(",") if t.strip()]) if gh_tokens else 0
    all_ok &= _check("GH_TOKENS", token_count > 0, f"{token_count} token(s)")
    if token_count:
        # The pool size is the ceiling on stages 2 and 3, and it is the number
        # an operator most often assumes is larger than it is.  Say it, and say
        # what it buys, rather than leaving it to be inferred from a count.
        _report(
            "Token pool size",
            f"{token_count} token(s) — roughly {token_count * 5000} REST request(s) "
            f"and {token_count * 5000} GraphQL point(s) per hour for stages 2 and 3",
        )

    hf_token_path = os.environ.get("HF_TOKEN_PATH", "/mnt/sdd3/llama_atharvas/huggingface/token")
    hf_exists = Path(hf_token_path).exists()
    all_ok &= _check("HF_TOKEN", hf_exists, hf_token_path)

    # 2. LLM Backend
    print("\n== LLM Backend ==")
    dspy_model = os.environ.get("DSPY_MODEL", "openai/gpt-oss-120b")
    dspy_api_base = os.environ.get("DSPY_API_BASE", "http://localhost:30001/v1")
    all_ok &= _check("DSPY_MODEL", bool(dspy_model), dspy_model)
    all_ok &= _check("DSPY_API_BASE", bool(dspy_api_base), dspy_api_base)

    if dspy_model and dspy_api_base:
        try:
            import httpx

            # Strip "openai/" prefix for model matching
            model_id = dspy_model.removeprefix("openai/")
            models_url = f"{dspy_api_base}/models"
            resp = httpx.get(models_url, timeout=10)
            resp.raise_for_status()
            models_data = resp.json().get("data", [])
            model_ids = [m.get("id", "") for m in models_data]
            found = model_id in model_ids
            all_ok &= _check("Model available", found, f"{model_id} in {len(model_ids)} model(s)")

            if found:
                # Test completion
                chat_url = f"{dspy_api_base}/chat/completions"
                payload = {
                    "model": model_id,
                    "messages": [{"role": "user", "content": "Say OK"}],
                    "max_tokens": 64,
                }
                comp_resp = httpx.post(chat_url, json=payload, timeout=60)
                comp_resp.raise_for_status()
                choices = comp_resp.json().get("choices", [])
                content = ""
                if choices:
                    msg = choices[0].get("message") or {}
                    if isinstance(msg, dict):
                        content = msg.get("content") or msg.get("reasoning") or ""
                    finish = choices[0].get("finish_reason", "")
                else:
                    finish = ""
                detail = repr(content[:40]) if content else f"finish_reason={finish}"
                all_ok &= _check("Test completion", True, detail)
        except Exception as e:
            all_ok &= _check("LLM server", False, str(e)[:80])

    # 3. Supabase connection
    print("\n== Supabase ==")
    try:
        import datasmith.utils.db as db_mod
        from datasmith.utils.db import get_client

        db_mod._client = None  # Force fresh connection
        client = get_client()
        # Try a simple query
        client.table("repositories").select("owner").limit(1).execute()
        all_ok &= _check("Connection", True)

        # Measured, not assumed.  Local Postgres and db.formulacode.org
        # through CF Access are both first-class, and they differ by roughly
        # 6x; an operator needs to know which one they are on before they
        # wonder why a skip set is slow.  Reported, never gated.
        pings = max(2, DATASMITH_PREFLIGHT_DB_PINGS)
        samples: list[float] = []
        for index in range(pings):
            started = time.perf_counter()
            client.table("repositories").select("owner").limit(1).execute()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            if index:  # discard the first: it pays for connection setup
                samples.append(elapsed_ms)
        samples.sort()
        median = samples[len(samples) // 2]
        _report(
            "Round-trip latency",
            f"median {median:.1f} ms over {len(samples)} query(s) "
            f"(min {samples[0]:.1f} ms, max {samples[-1]:.1f} ms) against {supabase_url or 'SUPABASE_URL'}",
        )
        db_mod._client = None
    except Exception as e:
        all_ok &= _check("Connection", False, str(e)[:80])

    # 4. Docker
    print("\n== Docker ==")
    try:
        from python_on_whales import DockerClient

        docker = DockerClient()
        docker.version()
        all_ok &= _check("Docker daemon", True)
    except Exception as e:
        all_ok &= _check("Docker daemon", False, str(e)[:80])

    # 5. GitHub tokens
    print("\n== GitHub ==")
    if token_count > 0:
        try:
            import httpx

            total_remaining = 0
            failed_tokens = 0
            tokens = [t.strip() for t in gh_tokens.split(",") if t.strip()]
            for token in tokens:
                resp = httpx.get(
                    "https://api.github.com/rate_limit",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    total_remaining += resp.json()["rate"]["remaining"]
                else:
                    failed_tokens += 1

            if failed_tokens == len(tokens):
                all_ok &= _check("API access", False, "all tokens failed")
            else:
                detail = f"remaining={total_remaining} across {len(tokens) - failed_tokens}/{len(tokens)} token(s)"
                all_ok &= _check("API access", True, detail)
        except Exception as e:
            all_ok &= _check("API access", False, str(e)[:80])
    else:
        all_ok &= _check("API access", False, "no tokens")

    # 6. Resolved concurrency
    print("\n== Concurrency ==")
    try:
        for label, value in resolved_concurrency_caps():
            _report(label, f"{value:g}" if isinstance(value, float) else str(value))
    except Exception as e:  # pragma: no cover - a report must not break preflight
        _report("Concurrency caps", f"unavailable: {str(e)[:80]}")

    # Summary
    print("\n" + "=" * 40)
    if all_ok:
        print("All checks passed!")
    else:
        print("Some checks failed. Fix issues above before running the pipeline.")

    return all_ok


if __name__ == "__main__":
    from datasmith import setup_environment

    setup_environment()
    ok = run_preflight()
    sys.exit(0 if ok else 1)
