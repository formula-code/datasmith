from __future__ import annotations

import os
import sys
from pathlib import Path

from datasmith.utils import get_logger

logger = get_logger("preflight")


def _check(name: str, condition: bool, detail: str = "") -> bool:
    status = "OK" if condition else "FAIL"
    msg = f"  [{status}] {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    return condition


def run_preflight() -> bool:
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

    hf_token_path = os.environ.get("HF_TOKEN_PATH", "/mnt/sdd3/llama_atharvas/huggingface/token")
    hf_exists = Path(hf_token_path).exists()
    all_ok &= _check("HF_TOKEN", hf_exists, hf_token_path)

    # 2. Supabase connection
    print("\n== Supabase ==")
    try:
        import datasmith.utils.db as db_mod
        from datasmith.utils.db import get_client

        db_mod._client = None  # Force fresh connection
        client = get_client()
        # Try a simple query
        client.table("repositories").select("owner").limit(1).execute()
        all_ok &= _check("Connection", True)
        db_mod._client = None
    except Exception as e:
        all_ok &= _check("Connection", False, str(e)[:80])

    # 3. Docker
    print("\n== Docker ==")
    try:
        from python_on_whales import DockerClient

        docker = DockerClient()
        docker.version()
        all_ok &= _check("Docker daemon", True)
    except Exception as e:
        all_ok &= _check("Docker daemon", False, str(e)[:80])

    # 4. GitHub tokens
    print("\n== GitHub ==")
    if token_count > 0:
        try:
            import httpx

            token = gh_tokens.split(",")[0].strip()
            resp = httpx.get(
                "https://api.github.com/rate_limit",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            if resp.status_code == 200:
                remaining = resp.json()["rate"]["remaining"]
                all_ok &= _check("API access", True, f"remaining={remaining}")
            else:
                all_ok &= _check("API access", False, f"status={resp.status_code}")
        except Exception as e:
            all_ok &= _check("API access", False, str(e)[:80])
    else:
        all_ok &= _check("API access", False, "no tokens")

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
