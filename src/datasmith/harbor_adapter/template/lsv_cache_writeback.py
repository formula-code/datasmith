"""LSV baseline cache writeback — runs from test.sh after lsv_measure.

Inspects ``/logs/artifacts/lsv/lsv_cache_state.json`` (written by lsv_init.py
on this trial) and, for any cache layer that was a *miss*, uploads the new
state to Supabase so the next run on the same (PR, resource_signature) hits.

Two layers, written independently:
  * deps DB (per-PR, structural)         — Supabase Storage bucket ``lsv-deps``
                                            + ``lsv_dep_cache`` row
  * baseline timings (per-PR-per-sig)    — ``lsv_baseline_cache`` row (JSONB)

Stdlib only — uses ``urllib.request`` and ``sqlite3`` to avoid pulling
extra dependencies into the verifier image.

Usage (invoked from test.sh; all inputs read from env):
    python /opt/lsv/lsv_cache_writeback.py
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LSV_DIR = Path(os.environ.get("LSV_OUTPUT_DIR", "/logs/artifacts/lsv"))
CACHE_STATE_PATH = LSV_DIR / "lsv_cache_state.json"


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _service_headers() -> dict[str, str]:
    """Auth headers for datasmith's own Supabase (NOT Harbor's project).

    The two are separate Postgres projects: Harbor's holds run uploads and
    snapshots, datasmith's holds the cache tables (`lsv_dep_cache`,
    `lsv_baseline_cache`). Using the wrong key writes to the wrong DB, where
    the tables don't exist, and the inserts 4xx silently — which is exactly
    the bug we hit on the first end-to-end run.

    Also injects Cloudflare Access service-token headers when present, since
    `db.formulacode.org` is gated by CF Access (see CLAUDE.md remote-access).
    The User-Agent is overridden because Cloudflare's bot-fight WAF rule
    blocks the default ``Python-urllib/3.x`` UA with error code 1010 *before*
    Access even sees the request.
    """
    key = os.environ.get("DATASMITH_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise RuntimeError("DATASMITH_SUPABASE_SERVICE_KEY not set")
    headers = {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "User-Agent": "datasmith-lsv-cache/1.0",
    }
    cf_id = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_ID", "")
    cf_secret = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_SECRET", "")
    if cf_id and cf_secret:
        headers["CF-Access-Client-Id"] = cf_id
        headers["CF-Access-Client-Secret"] = cf_secret
    return headers


def _request(
    url: str,
    method: str,
    headers: dict[str, str],
    data: bytes | None = None,
    content_type: str | None = None,
) -> bytes:
    if content_type:
        headers = {**headers, "Content-Type": content_type}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"[{_ts()}] [cache_writeback] ERROR: {method} {url} -> {e.code}: {body}")
        raise


def _parse_task_id(task_id: str) -> tuple[str, str, int]:
    """``owner_repo_<issue>`` (with ``_`` already separating the components,
    matching ``records.py`` task_id minting). Repo names with underscores are
    rare but possible — split from the right so the trailing integer wins."""
    *rest, issue_str = task_id.rsplit("_", 1)
    issue = int(issue_str)
    head = "_".join(rest)
    owner, _, repo = head.partition("_")
    return owner, repo, issue


def _upload_deps_db(
    base_url: str,
    bucket: str,
    deps_db_path: Path,
) -> str:
    """Upload a baseline-stripped copy of the deps DB to Storage. Returns the
    object key (which is what we store in ``pull_requests.lsv_deps_db_url``)."""
    if not deps_db_path.exists():
        raise FileNotFoundError(f"deps DB missing at {deps_db_path}")

    stripped = deps_db_path.parent / "lightspeed_deps_stripped.db"
    shutil.copy(deps_db_path, stripped)
    # The deps DB cache is structural-only; baselines live in
    # `lsv_baseline_cache` keyed by raw resource columns. Strip the
    # `baseline` table so a future hit doesn't replay stale per-resource
    # timings.
    with sqlite3.connect(stripped) as con:
        con.execute("DELETE FROM baseline")
        con.commit()
        # LSV runs the deps DB in WAL mode (PRAGMA journal_mode=WAL), so the
        # DELETE above lands in a `.db-wal` sidecar — `commit()` marks it
        # durable but doesn't fold it back into the main file, and SQLite's
        # default heuristics don't auto-checkpoint on close for a WAL this
        # small. We then upload `stripped.read_bytes()` (the main file only;
        # Storage has no notion of sidecars), so a downstream trial that
        # downloads the blob sees the *pre-DELETE* 140 baseline rows. That
        # silently re-introduces foreign baselines and makes
        # `initialize_diffcheck(force=False)` short-circuit the per-env
        # timing pass — exactly the cross-host pollution this strip is
        # supposed to prevent. TRUNCATE checkpoint folds the WAL into the
        # main file *and* zeroes the WAL, so the bytes we read are the
        # actually-stripped state.
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    owner, repo, issue = _parse_task_id(os.environ["LSV_TASK_ID"])
    object_key = f"{owner}__{repo}__{issue}.sqlite"

    blob = stripped.read_bytes()
    headers = {**_service_headers(), "x-upsert": "true"}
    _request(
        f"{base_url}/storage/v1/object/{bucket}/{object_key}",
        "POST",
        headers,
        data=blob,
        content_type="application/x-sqlite3",
    )
    print(f"[{_ts()}] [cache_writeback] uploaded deps DB ({len(blob)} bytes) → {bucket}/{object_key}")
    return object_key


def _patch_pr_deps_url(
    base_url: str,
    *,
    owner: str,
    repo: str,
    issue_number: int,
    deps_db_url: str,
) -> None:
    """Set ``pull_requests.lsv_deps_db_url`` for this PR. The PR row is
    guaranteed to exist by the time stage 7 reaches us (stages 1-6 created
    it). PostgREST PATCH against zero rows is a silent no-op, so we use
    Prefer: count=exact and check the Content-Range header to log a warning
    if our PR row is somehow missing."""
    payload = {
        "lsv_deps_db_url": deps_db_url,
    }
    headers = {
        **_service_headers(),
        "Prefer": "return=representation,count=exact",
    }
    url = (
        f"{base_url}/rest/v1/pull_requests?owner=eq.{owner}"
        f"&repo=eq.{repo}&issue_number=eq.{issue_number}"
    )
    body = _request(
        url, "PATCH", headers,
        data=json.dumps(payload).encode(),
        content_type="application/json",
    )
    n = len(json.loads(body or b"[]"))
    if n == 0:
        print(
            f"[{_ts()}] [cache_writeback] WARN: PATCH pull_requests for "
            f"{owner}/{repo}#{issue_number} updated 0 rows (PR row missing?)"
        )
    else:
        print(
            f"[{_ts()}] [cache_writeback] set pull_requests.lsv_deps_db_url "
            f"for {owner}/{repo}#{issue_number}"
        )


def _read_baselines(deps_db_path: Path) -> dict[str, dict[str, float | int]]:
    """Pull every baseline row out of the SQLite as a JSON-serializable dict."""
    if not deps_db_path.exists():
        return {}
    out: dict[str, dict[str, float | int]] = {}
    with sqlite3.connect(deps_db_path) as con:
        rows = con.execute(
            "SELECT benchmark_id, median, ci_99_a, ci_99_b, q_25, q_75, repeat, number FROM baseline"
        ).fetchall()
    for bid, median, ci_a, ci_b, q25, q75, rep, num in rows:
        out[bid] = {
            "median":  median,
            "ci_99_a": ci_a,
            "ci_99_b": ci_b,
            "q_25":    q25,
            "q_75":    q75,
            "repeat":  rep,
            "number":  num,
        }
    return out


_BASELINE_PK_COLS = (
    "owner", "repo", "issue_number",
    "env", "container_name", "image_digest",
    "machine_class", "docker_host_id",
    "cpu_count", "mem_bytes",
    "detected_cpu_model",
)


def _upsert_baseline_cache_row(
    base_url: str,
    *,
    owner: str,
    repo: str,
    issue_number: int,
    attrs: dict,
    baselines: dict,
) -> None:
    """Upsert one row into ``lsv_baseline_cache`` keyed on the raw resource
    columns. ``attrs`` must contain all of: env, container_name, image_digest,
    machine_class, docker_host_id, cpu_count, mem_bytes, detected_cpu_model.
    The PK spans every PK column so on_conflict has to enumerate them all."""
    row = {
        "owner": owner,
        "repo": repo,
        "issue_number": issue_number,
        "baselines": baselines,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        **attrs,
    }
    headers = {
        **_service_headers(),
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    on_conflict = ",".join(_BASELINE_PK_COLS)
    _request(
        f"{base_url}/rest/v1/lsv_baseline_cache?on_conflict={on_conflict}",
        "POST",
        headers,
        data=json.dumps(row).encode(),
        content_type="application/json",
    )
    print(
        f"[{_ts()}] [cache_writeback] upserted lsv_baseline_cache row "
        f"for {owner}/{repo}#{issue_number} env={attrs['env']} "
        f"digest={str(attrs['image_digest'])[:24]} ({len(baselines)} baselines)"
    )


def _env_summary() -> str:
    """One-line summary of the env vars this script depends on, for diagnostics.

    Each var is printed as ``NAME=set`` or ``NAME=unset`` — never the actual
    value, since the service key is sensitive. We log this unconditionally at
    the top of every run so silent-skip failures stop being mysterious.
    """
    keys = [
        "DATASMITH_SUPABASE_URL",
        "DATASMITH_SUPABASE_SERVICE_KEY",
        "DATASMITH_CF_ACCESS_CLIENT_ID",
        "DATASMITH_CF_ACCESS_CLIENT_SECRET",
        "LSV_TASK_ID",
        "LSV_DEPS_BUCKET",
        "FORMULACODE_NO_UPLOAD",
    ]
    parts = []
    for k in keys:
        v = os.environ.get(k, "")
        parts.append(f"{k}={'set' if v else 'unset'}")
    return ", ".join(parts)


_RESOURCE_ATTRS_PATH = LSV_DIR / "lsv_resource_attrs.json"


def _read_resource_attrs() -> dict[str, Any]:
    """Load the cache key attrs from the JSON file lsv_init.py wrote.

    Single source of truth — both lookup (in lsv_init) and upsert (here)
    use identical values. Removes the env-var → shell-quote → env-var
    round-trip that previously produced ``''``-vs-``""`` cache-row
    duplication for empty fields like ``machine_class`` on docker.
    """
    if not _RESOURCE_ATTRS_PATH.exists():
        print(
            f"[{_ts()}] [cache_writeback] WARNING: {_RESOURCE_ATTRS_PATH} missing — "
            "lsv_init.py likely didn't complete; falling back to LSV_* env vars"
        )
        return _read_resource_attrs_from_env()
    try:
        attrs: dict[str, Any] = json.loads(_RESOURCE_ATTRS_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[{_ts()}] [cache_writeback] WARNING: {_RESOURCE_ATTRS_PATH} unreadable ({exc})")
        return _read_resource_attrs_from_env()
    # Coerce numeric fields defensively in case the JSON encoded them as strings.
    for k in ("cpu_count", "mem_bytes"):
        try:
            attrs[k] = int(attrs.get(k, 0))
        except (TypeError, ValueError):
            attrs[k] = 0
    return attrs


def _read_resource_attrs_from_env() -> dict[str, Any]:
    """Fallback when ``lsv_resource_attrs.json`` is missing — read the
    runner-supplied cache-key fields from env vars directly. Same logic
    as ``lsv_init.py:_read_resource_attrs`` for the env-sourced attrs.

    We do NOT fall back to /proc for cpu_count/mem_bytes: inside the
    sandbox they reflect the *host* (Daytona's physical machine, not the
    cgroup-pinned 2/8), which would write the wrong key.

    ``detected_cpu_model`` has no env-var equivalent — it is only
    populated via ``lsv_resource_attrs.json`` written by lsv_init.py
    after reading /proc/cpuinfo inside the sandbox. When this fallback
    path runs, lsv_init didn't complete, so we leave the field empty
    and accept that the row goes to the empty-string slot.
    """
    def _int(v: str) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    return {
        "env":                os.environ.get("LSV_ENV", ""),
        "container_name":     os.environ.get("LSV_CONTAINER_NAME", ""),
        "image_digest":       os.environ.get("LSV_IMAGE_DIGEST", ""),
        "machine_class":      os.environ.get("LSV_MACHINE_CLASS", ""),
        "docker_host_id":     os.environ.get("LSV_DOCKER_HOST_ID", ""),
        "cpu_count":          _int(os.environ.get("LSV_CPU_COUNT", "")),
        "mem_bytes":          _int(os.environ.get("LSV_MEM_BYTES", "")),
        "detected_cpu_model": "",
    }


def main() -> int:
    print(f"[{_ts()}] [cache_writeback] starting; env: {_env_summary()}")

    # Only oracle runs *produce* the canonical baselines + deps DB. Agent
    # evaluation runs read those artifacts and must NOT overwrite them — an
    # agent's measurements (post-patch) would corrupt the cache that future
    # agent runs use as the comparison baseline. Bail out early before any
    # network I/O.
    agent_name = os.environ.get("HARBOR_AGENT_NAME", "oracle").lower()
    if agent_name != "oracle":
        print(
            f"[{_ts()}] [cache_writeback] SKIP: HARBOR_AGENT_NAME={agent_name!r} "
            f"(agent runs must not overwrite oracle baselines)"
        )
        return 0

    base_url = os.environ.get("DATASMITH_SUPABASE_URL")
    if not base_url:
        print(
            f"[{_ts()}] [cache_writeback] SKIP: DATASMITH_SUPABASE_URL not set "
            f"(verifier_env in task.toml must include it for cache writeback to fire)"
        )
        return 0
    if os.environ.get("FORMULACODE_NO_UPLOAD"):
        print(f"[{_ts()}] [cache_writeback] SKIP: FORMULACODE_NO_UPLOAD is set")
        return 0
    if not os.environ.get("DATASMITH_SUPABASE_SERVICE_KEY"):
        print(f"[{_ts()}] [cache_writeback] SKIP: DATASMITH_SUPABASE_SERVICE_KEY not set")
        return 0

    if not CACHE_STATE_PATH.exists():
        print(f"[{_ts()}] [cache_writeback] SKIP: no {CACHE_STATE_PATH} (lsv_init likely failed before writing it)")
        return 0

    state = json.loads(CACHE_STATE_PATH.read_text())
    deps_was_cached = bool(state.get("deps_was_cached"))
    baselines_was_cached = bool(state.get("baselines_was_cached"))
    deps_db_path = Path(state.get("deps_db_path") or "")
    print(
        f"[{_ts()}] [cache_writeback] state: deps_was_cached={deps_was_cached} "
        f"baselines_was_cached={baselines_was_cached} deps_db_path={deps_db_path}"
    )

    if deps_was_cached and baselines_was_cached:
        print(f"[{_ts()}] [cache_writeback] both layers were cache hits, nothing to write back")
        return 0

    task_id = os.environ.get("LSV_TASK_ID")
    if not task_id:
        print(f"[{_ts()}] [cache_writeback] SKIP: missing LSV_TASK_ID")
        return 0

    attrs = _read_resource_attrs()
    if not attrs["env"]:
        print(f"[{_ts()}] [cache_writeback] SKIP: missing LSV_ENV (resource columns not provided)")
        return 0

    bucket = os.environ.get("LSV_DEPS_BUCKET", "lsv-deps")
    owner, repo, issue_number = _parse_task_id(task_id)
    print(
        f"[{_ts()}] [cache_writeback] target: {owner}/{repo}#{issue_number} "
        f"env={attrs['env']} digest={attrs['image_digest'][:24]} bucket={bucket} url={base_url}"
    )

    rc = 0

    # ── deps DB layer ────────────────────────────────────────────────────
    if not deps_was_cached:
        try:
            object_key = _upload_deps_db(base_url, bucket, deps_db_path)
            _patch_pr_deps_url(
                base_url, owner=owner, repo=repo, issue_number=issue_number, deps_db_url=object_key,
            )
        except Exception as exc:
            print(f"[{_ts()}] [cache_writeback] deps DB writeback FAILED: {exc}")
            rc = 1
    else:
        print(f"[{_ts()}] [cache_writeback] deps DB layer was a hit; skipping upload")

    # ── baselines layer ──────────────────────────────────────────────────
    if not baselines_was_cached:
        try:
            baselines = _read_baselines(deps_db_path)
            if not baselines:
                print(f"[{_ts()}] [cache_writeback] no baselines in {deps_db_path}, skipping baseline writeback")
            else:
                _upsert_baseline_cache_row(
                    base_url,
                    owner=owner, repo=repo, issue_number=issue_number,
                    attrs=attrs, baselines=baselines,
                )
        except Exception as exc:
            print(f"[{_ts()}] [cache_writeback] baselines writeback FAILED: {exc}")
            rc = 1
    else:
        print(f"[{_ts()}] [cache_writeback] baselines layer was a hit; skipping upload")

    print(f"[{_ts()}] [cache_writeback] done (rc={rc})")
    return rc


if __name__ == "__main__":
    sys.exit(main())
