"""LSV cache writeback — runs from test.sh after lsv_measure.

Reads ``lsv_cache_state.json`` (written by lsv_init.py this trial) and, on a
MISS, persists the freshly measured facts to datasmith's own Supabase so the
next trial on the same task/hardware hits and skips a pass:

* baselines (resource-keyed) -> ``lsv_baseline_cache`` (00031), on a baseline
  miss. Skips the base-commit timing pass.
* the coverage survey (resource-independent) -> ``lsv_deps_cache`` (00032), on a
  deps miss. Skips the survey pass. Uploaded baselines-stripped so no per-host
  timing rows leak across hardware.

Oracle-only: only the oracle trial produces canonical facts. An agent run
measures post-patch code, so letting it write would corrupt the very baseline
future agent runs compare against. This self-gates on ``HARBOR_AGENT_NAME``, and
test.sh only invokes it on the oracle branch.

Stdlib only (``urllib`` + ``sqlite3``) so nothing extra is baked into the image.

Usage (invoked from test.sh; all inputs from env):
    python /opt/lsv/lsv_cache_writeback.py
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LSV_DIR = Path(os.environ.get("LSV_OUTPUT_DIR", "/logs/artifacts/lsv"))
CACHE_STATE_PATH = LSV_DIR / "lsv_cache_state.json"
_RESOURCE_ATTRS_PATH = LSV_DIR / "lsv_resource_attrs.json"

# Order matters: the upsert enumerates these as the on_conflict target, so it
# must match the primary key declared in migration 00031 exactly.
_BASELINE_PK_COLS = (
    "owner", "repo", "issue_number",
    "env", "container_name", "image_digest",
    "machine_class", "docker_host_id",
    "cpu_count", "mem_bytes",
    "detected_cpu_model",
)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _service_headers() -> dict[str, str]:
    """Auth headers for datasmith's own Supabase (NOT Harbor's project).

    The two are separate Postgres projects: Harbor's holds run uploads and
    snapshots, datasmith's holds ``lsv_baseline_cache``. The User-Agent override
    matters -- Cloudflare's bot-fight rule blocks the default ``Python-urllib``
    UA before Access sees the request. CF-Access headers are injected when
    ``db.formulacode.org`` is the target (see CLAUDE.md remote-access).
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
        with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310
            return resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"[{_ts()}] [cache_writeback] ERROR: {method} {url} -> {e.code}: {body}")
        raise


def _parse_task_id(task_id: str) -> tuple[str, str, int] | None:
    """Parse ``owner__repo__<issue>`` (double-underscore, matching
    ``adapter.FormulaCodeRecord.task_dir_name``). Returns None if malformed."""
    try:
        owner, repo, issue_str = task_id.rsplit("__", 2)
        if not owner or not repo:
            return None
        return owner, repo, int(issue_str)
    except (ValueError, IndexError):
        return None


def _read_baselines(deps_db_path: Path) -> dict[str, dict[str, float | int]]:
    """Pull every baseline row out of the deps DB as a JSON-serializable dict.
    Shape matches ``session.export_baselines`` so a future ``load_baselines``
    accepts it verbatim."""
    if not deps_db_path.exists():
        return {}
    with sqlite3.connect(deps_db_path) as con:
        rows = con.execute(
            "SELECT benchmark_id, median, ci_99_a, ci_99_b, q_25, q_75, repeat, number "
            "FROM baseline"
        ).fetchall()
    out: dict[str, dict[str, float | int]] = {}
    for bid, median, ci_a, ci_b, q25, q75, rep, num in rows:
        out[bid] = {
            "median": median,
            "ci_99_a": ci_a,
            "ci_99_b": ci_b,
            "q_25": q25,
            "q_75": q75,
            "repeat": rep,
            "number": num,
        }
    return out


def _survey_bytes(deps_db_path: Path) -> bytes | None:
    """Return the survey-only deps DB as bytes: a copy with every baseline row
    deleted and the WAL checkpointed into the main file.

    The DELETE strips the resource-DEPENDENT timings (they live in
    lsv_baseline_cache under a hardware key); the survey that remains is
    resource-independent and safe to reuse on any host. ``PRAGMA
    wal_checkpoint(TRUNCATE)`` is load-bearing: without it the deleted rows
    survive in the WAL and get uploaded anyway, reintroducing exactly the
    cross-host timing pollution the DELETE removed. Operates on a copy so the
    live DB the trial still uses is untouched. Returns None if absent/unreadable.
    """
    if not deps_db_path.exists() or deps_db_path.stat().st_size == 0:
        return None
    tmp_dir = tempfile.mkdtemp(prefix="lsv_deps_")
    try:
        tmp_db = Path(tmp_dir) / "survey.db"
        shutil.copy(deps_db_path, tmp_db)
        with sqlite3.connect(tmp_db) as con:
            con.execute("DELETE FROM baseline")
            con.commit()
            con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return tmp_db.read_bytes()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _upsert_deps_cache_row(
    base_url: str,
    *,
    owner: str,
    repo: str,
    issue_number: int,
    survey: bytes,
) -> None:
    """Upsert the survey blob into ``lsv_deps_cache`` keyed by the task alone.
    BYTEA moves through PostgREST as PostgreSQL's hex form: send ``\\x<hex>`` and
    Postgres parses it back to the raw bytes (the runner reverses this on read)."""
    row = {
        "owner": owner,
        "repo": repo,
        "issue_number": issue_number,
        "deps_db": "\\x" + survey.hex(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    headers = {
        **_service_headers(),
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    _request(
        f"{base_url}/rest/v1/lsv_deps_cache?on_conflict=owner,repo,issue_number",
        "POST",
        headers,
        data=json.dumps(row).encode(),
        content_type="application/json",
    )
    print(
        f"[{_ts()}] [cache_writeback] upserted lsv_deps_cache for "
        f"{owner}/{repo}#{issue_number} ({len(survey)} bytes)"
    )


def _upsert_baseline_cache_row(
    base_url: str,
    *,
    owner: str,
    repo: str,
    issue_number: int,
    attrs: dict[str, Any],
    baselines: dict[str, Any],
) -> None:
    """Upsert one ``lsv_baseline_cache`` row keyed on the full resource tuple.
    ``attrs`` supplies every non-task PK column; on_conflict enumerates the
    whole PK so a re-measure on identical hardware replaces the row."""
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
        f"[{_ts()}] [cache_writeback] upserted lsv_baseline_cache for "
        f"{owner}/{repo}#{issue_number} env={attrs['env']} "
        f"digest={str(attrs['image_digest'])[:24]} ({len(baselines)} baselines)"
    )


def _read_resource_attrs() -> dict[str, Any]:
    """Load the cache key from the JSON lsv_init.py wrote — the single source of
    truth, so lookup and upsert never disagree on '' vs "". Falls back to the
    LSV_* env vars if the file is missing (lsv_init did not complete), which
    loses only ``detected_cpu_model`` (no env equivalent — it is /proc-derived)."""
    if not _RESOURCE_ATTRS_PATH.exists():
        print(
            f"[{_ts()}] [cache_writeback] WARNING: {_RESOURCE_ATTRS_PATH} missing — "
            "falling back to LSV_* env vars"
        )
        return _read_resource_attrs_from_env()
    try:
        attrs: dict[str, Any] = json.loads(_RESOURCE_ATTRS_PATH.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[{_ts()}] [cache_writeback] WARNING: {_RESOURCE_ATTRS_PATH} unreadable ({exc})")
        return _read_resource_attrs_from_env()
    for k in ("cpu_count", "mem_bytes"):
        try:
            attrs[k] = int(attrs.get(k, 0))
        except (TypeError, ValueError):
            attrs[k] = 0
    return attrs


def _read_resource_attrs_from_env() -> dict[str, Any]:
    """Fallback cache key from env. cpu_count/mem_bytes are never read from /proc
    (inside the sandbox they show the host, not the cgroup pin); detected_cpu_model
    has no env source, so it defaults to '' here."""

    def _int(v: str) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    return {
        "env": os.environ.get("LSV_ENV", ""),
        "container_name": os.environ.get("LSV_CONTAINER_NAME", ""),
        "image_digest": os.environ.get("LSV_IMAGE_DIGEST", ""),
        "machine_class": os.environ.get("LSV_MACHINE_CLASS", ""),
        "docker_host_id": os.environ.get("LSV_DOCKER_HOST_ID", ""),
        "cpu_count": _int(os.environ.get("LSV_CPU_COUNT", "")),
        "mem_bytes": _int(os.environ.get("LSV_MEM_BYTES", "")),
        "detected_cpu_model": "",
    }


def _env_summary() -> str:
    """One-line ``NAME=set|unset`` summary (never values — the key is secret) so
    silent-skip failures are diagnosable."""
    keys = [
        "DATASMITH_SUPABASE_URL",
        "DATASMITH_SUPABASE_SERVICE_KEY",
        "DATASMITH_CF_ACCESS_CLIENT_ID",
        "DATASMITH_CF_ACCESS_CLIENT_SECRET",
        "LSV_TASK_ID",
        "HARBOR_AGENT_NAME",
        "FORMULACODE_NO_UPLOAD",
    ]
    return ", ".join(f"{k}={'set' if os.environ.get(k) else 'unset'}" for k in keys)


def main() -> int:
    print(f"[{_ts()}] [cache_writeback] starting; env: {_env_summary()}")

    # Oracle-only: bail before any I/O so an agent run can never overwrite the
    # oracle baseline it reads.
    agent_name = os.environ.get("HARBOR_AGENT_NAME", "oracle").lower()
    if agent_name != "oracle":
        print(f"[{_ts()}] [cache_writeback] SKIP: HARBOR_AGENT_NAME={agent_name!r} (not oracle)")
        return 0

    base_url = os.environ.get("DATASMITH_SUPABASE_URL")
    if not base_url:
        print(f"[{_ts()}] [cache_writeback] SKIP: DATASMITH_SUPABASE_URL not set")
        return 0
    if os.environ.get("FORMULACODE_NO_UPLOAD"):
        print(f"[{_ts()}] [cache_writeback] SKIP: FORMULACODE_NO_UPLOAD is set")
        return 0
    if not os.environ.get("DATASMITH_SUPABASE_SERVICE_KEY"):
        print(f"[{_ts()}] [cache_writeback] SKIP: DATASMITH_SUPABASE_SERVICE_KEY not set")
        return 0
    if not CACHE_STATE_PATH.exists():
        print(f"[{_ts()}] [cache_writeback] SKIP: no {CACHE_STATE_PATH} (lsv_init likely failed)")
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
        print(f"[{_ts()}] [cache_writeback] full cache hit, nothing to write back")
        return 0

    task_id = os.environ.get("LSV_TASK_ID", "")
    parsed = _parse_task_id(task_id)
    if parsed is None:
        print(f"[{_ts()}] [cache_writeback] SKIP: cannot parse LSV_TASK_ID={task_id!r}")
        return 0
    owner, repo, issue_number = parsed

    rc = 0

    # Deps (survey) layer: task-keyed, resource-independent. Upload only on a
    # miss; a hit means the staged survey is already canonical.
    if not deps_was_cached:
        try:
            survey = _survey_bytes(deps_db_path)
            if survey is None:
                print(f"[{_ts()}] [cache_writeback] no deps DB at {deps_db_path}, nothing to upload")
            else:
                _upsert_deps_cache_row(
                    base_url, owner=owner, repo=repo, issue_number=issue_number, survey=survey
                )
        except Exception as exc:  # noqa: BLE001 -- best-effort, never fails the trial
            print(f"[{_ts()}] [cache_writeback] deps writeback FAILED: {exc}")
            rc = 1

    # Baseline layer: resource-keyed. Needs the full hardware key (env etc.).
    if not baselines_was_cached:
        attrs = _read_resource_attrs()
        if not attrs["env"]:
            print(f"[{_ts()}] [cache_writeback] SKIP baselines: missing LSV_ENV (resource columns not provided)")
        else:
            try:
                baselines = _read_baselines(deps_db_path)
                if not baselines:
                    print(f"[{_ts()}] [cache_writeback] no baselines in {deps_db_path}, nothing to write")
                else:
                    _upsert_baseline_cache_row(
                        base_url,
                        owner=owner, repo=repo, issue_number=issue_number,
                        attrs=attrs, baselines=baselines,
                    )
            except Exception as exc:  # noqa: BLE001 -- best-effort, never fails the trial
                print(f"[{_ts()}] [cache_writeback] baselines writeback FAILED: {exc}")
                rc = 1

    print(f"[{_ts()}] [cache_writeback] done")
    return rc


if __name__ == "__main__":
    sys.exit(main())
