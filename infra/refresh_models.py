#!/usr/bin/env python3
"""Reconcile the LiteLLM proxy's model registry with the live vLLM servers.

Discovery is dynamic: vLLM servers come and go on whatever ports they were
launched with, so a static ``model_list`` in ``infra/litellm.config.yaml``
drifts the moment a server restarts. This script makes the proxy match reality:

1. Find every live inference server (parse ``ps``): vLLM (``vllm serve``),
   llama.cpp (``llama-server``), and Ollama (``ollama serve``) — anything
   fronting an OpenAI-compatible ``/v1``.
2. Probe each one's ``/v1/models`` endpoint — this both confirms the server is
   actually up (a crashed-but-unreaped process still shows in ``ps``) and yields
   the authoritative *served id* clients must pass in the ``model`` field.
3. Diff against the proxy's current registry (``GET /model/info``) and reconcile
   via the admin API: ``POST /model/new`` for live-but-unregistered models,
   ``POST /model/delete`` for DB-registered models whose upstream is gone.

LiteLLM runs DB-backed (``STORE_MODEL_IN_DB=True``), so these admin calls persist
to Postgres and the proxy re-reads them on its background interval — no restart,
no dropped in-flight requests. Only models added by this script (``db_model``)
are ever deleted; anything pinned in the YAML is left untouched.

Run once (default) for a single reconcile, or ``--watch`` to loop. ``--dry-run``
prints the plan without mutating the proxy.

Every server discovered is exposed; there is no per-user allowlist. A server
launched with ``--api-key`` (e.g. ``--api-key EMPTY``) is registered with that
key so the proxy can reach it; otherwise a ``dummy`` key is used (vLLM ignores
it). When two live servers serve the same id, both deployments are registered
under one ``model_name`` and LiteLLM load-balances across them.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

# --- Tunable knobs (overridable from tokens.env, per the DATASMITH_ convention) ---
DATASMITH_MODEL_REFRESH_INTERVAL_S: int = int(os.environ.get("DATASMITH_MODEL_REFRESH_INTERVAL_S", "90"))
DATASMITH_MODEL_PROBE_TIMEOUT_S: float = float(os.environ.get("DATASMITH_MODEL_PROBE_TIMEOUT_S", "3"))
DATASMITH_LITELLM_BASE: str = os.environ.get("DATASMITH_LITELLM_BASE", "http://127.0.0.1:4100")
# vLLM binds 0.0.0.0 but we always reach it over loopback.
DATASMITH_VLLM_HOST: str = os.environ.get("DATASMITH_VLLM_HOST", "127.0.0.1")


def _log(msg: str) -> None:
    print(f"[refresh-models] {msg}", flush=True)


def _parse_arg(args: list[str], *flags: str) -> str | None:
    """Return the value of ``--flag value`` or ``--flag=value`` from a token list."""
    for i, tok in enumerate(args):
        for flag in flags:
            if tok == flag and i + 1 < len(args):
                return args[i + 1]
            if tok.startswith(flag + "="):
                return tok.split("=", 1)[1]
    return None


def _server_engine(cmd: str) -> str | None:
    """Classify a process cmdline as a servable inference engine, or ``None``.

    Any engine that exposes an OpenAI-compatible ``/v1`` qualifies; the served
    ids are confirmed later by probing ``/v1/models``.
    """
    low = cmd.lower()
    # vLLM — match the top-level server only, not its worker/engine subprocesses.
    is_vllm_server = ("vllm serve" in cmd or "vllm.entrypoints" in cmd) and not re.search(
        r"VLLM::|resource_tracker|multiprocessing", cmd
    )
    if is_vllm_server:
        return "vllm"
    # llama.cpp's OpenAI-compatible server.
    if "llama-server" in low:
        return "llama.cpp"
    # Ollama hosts an OpenAI-compatible API at /v1 (default port 11434).
    if re.search(r"\bollama\b.*\bserve\b", low):
        return "ollama"
    return None


def discover_servers() -> list[dict]:
    """Return ``[{port, user, api_key, engine}]`` for each live inference server.

    Recognises vLLM (``vllm serve``), llama.cpp (``llama-server``), and Ollama
    (``ollama serve``) — anything fronting an OpenAI-compatible ``/v1``.
    """
    out = subprocess.check_output(["ps", "-eo", "user,pid,args"], text=True)
    by_port: dict[str, dict] = {}
    for line in out.splitlines()[1:]:
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        user, _pid, cmd = parts
        engine = _server_engine(cmd)
        if engine is None:
            continue
        args = cmd.split()
        port = _parse_arg(args, "--port")
        if not port and engine == "ollama":
            port = "11434"  # ollama's default; override via its own OLLAMA_HOST
        if not port:
            continue
        # Last writer wins, but a port is owned by one server at a time anyway.
        by_port[port] = {
            "port": port,
            "user": user,
            "api_key": _parse_arg(args, "--api-key"),
            "engine": engine,
        }
    return list(by_port.values())


def probe_served_ids(port: str, api_key: str | None) -> list[str]:
    """Hit ``/v1/models`` on a vLLM server; return its served ids, or ``[]`` if down."""
    url = f"http://{DATASMITH_VLLM_HOST}:{port}/v1/models"
    req = urllib.request.Request(url)  # noqa: S310 — fixed http scheme, loopback only
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")
    try:
        with urllib.request.urlopen(req, timeout=DATASMITH_MODEL_PROBE_TIMEOUT_S) as r:  # noqa: S310
            data = json.load(r)
        return [m["id"] for m in data.get("data", []) if m.get("id")]
    except (urllib.error.URLError, OSError, json.JSONDecodeError, KeyError):
        return []


def _api_base(port: str) -> str:
    return f"http://{DATASMITH_VLLM_HOST}:{port}/v1"


def desired_registry() -> dict[tuple[str, str], dict]:
    """Build ``{(model_name, api_base): deployment_spec}`` from live servers."""
    desired: dict[tuple[str, str], dict] = {}
    for srv in discover_servers():
        port, api_key = srv["port"], srv["api_key"]
        for served_id in probe_served_ids(port, api_key):
            base = _api_base(port)
            desired[(served_id, base)] = {
                "model_name": served_id,
                "litellm_params": {
                    # `openai/` prefix → LiteLLM speaks the OpenAI-compatible
                    # protocol; the suffix is forwarded to the engine verbatim.
                    "model": f"openai/{served_id}",
                    "api_base": base,
                    "api_key": api_key or "dummy",
                },
            }
    return desired


def _litellm_request(method: str, path: str, master_key: str, body: dict | None = None) -> dict:
    url = f"{DATASMITH_LITELLM_BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)  # noqa: S310 — fixed http scheme, loopback only
    req.add_header("Authorization", f"Bearer {master_key}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=DATASMITH_MODEL_PROBE_TIMEOUT_S * 5) as r:  # noqa: S310
        raw = r.read()
    return json.loads(raw) if raw else {}


def current_registry(master_key: str) -> dict[tuple[str, str], dict]:
    """Return ``{(model_name, api_base): {id, db_model}}`` from the proxy."""
    info = _litellm_request("GET", "/model/info", master_key)
    out: dict[tuple[str, str], dict] = {}
    for m in info.get("data", []):
        params = m.get("litellm_params") or {}
        meta = m.get("model_info") or {}
        key = (m.get("model_name"), params.get("api_base"))
        out[key] = {"id": meta.get("id"), "db_model": meta.get("db_model")}
    return out


def reconcile(master_key: str, dry_run: bool = False) -> tuple[int, int]:
    """Add live-but-missing models, remove dead DB models. Return (added, removed)."""
    desired = desired_registry()
    current = current_registry(master_key)

    added = 0
    for key, spec in desired.items():
        if key not in current:
            served_id, base = key
            if dry_run:
                _log(f"+ would add  {served_id}  ({base})")
            else:
                _litellm_request("POST", "/model/new", master_key, spec)
                _log(f"+ added      {served_id}  ({base})")
            added += 1

    removed = 0
    for key, meta in current.items():
        # Only ever remove models we (or the admin API) put in the DB, and only
        # when their upstream is no longer live.
        if key in desired or not meta.get("db_model") or not meta.get("id"):
            continue
        name, base = key
        if dry_run:
            _log(f"- would remove {name}  ({base}, upstream gone)")
        else:
            _litellm_request("POST", "/model/delete", master_key, {"id": meta["id"]})
            _log(f"- removed     {name}  ({base}, upstream gone)")
        removed += 1

    if not added and not removed:
        _log(f"in sync — {len(desired)} live model(s), nothing to change")
    else:
        verb = "would change" if dry_run else "changed"
        _log(f"{verb}: +{added} -{removed} ({len(desired)} live model(s))")
    return added, removed


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--watch", action="store_true", help="loop forever instead of a single pass")
    ap.add_argument(
        "--interval",
        type=int,
        default=DATASMITH_MODEL_REFRESH_INTERVAL_S,
        help="seconds between reconciles in --watch mode",
    )
    ap.add_argument("--dry-run", action="store_true", help="print the plan, change nothing")
    args = ap.parse_args()

    master_key = os.environ.get("LITELLM_MASTER_KEY")
    if not master_key:
        _log("LITELLM_MASTER_KEY not set — source tokens.env first")
        return 1

    if not args.watch:
        reconcile(master_key, dry_run=args.dry_run)
        return 0

    _log(f"watching: reconcile every {args.interval}s (dry_run={args.dry_run})")
    while True:
        try:
            reconcile(master_key, dry_run=args.dry_run)
        except Exception as exc:  # keep the daemon alive across transient errors
            _log(f"reconcile error (continuing): {exc!r}")
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
