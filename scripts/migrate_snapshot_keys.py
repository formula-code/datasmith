"""One-shot rename of Harbor snapshot tarballs to the (owner, repo, issue_number) layout.

Companion to ``scripts/harbor_tasks_migration.sql``. After the SQL has
backfilled ``tasks.owner / repo / issue_number`` and the new harbor_adapter
code is writing to the triple-keyed path, this script walks the existing
``snapshots/`` bucket in Harbor's Supabase Storage and moves every
``snapshots/{old_task_id}/oracle.tar.gz`` to
``snapshots/{owner}/{repo}/{issue_number}/oracle.tar.gz``.

Safe to run repeatedly: objects already at the new key are skipped.
Defaults to ``--dry-run``; pass ``--apply`` to actually move objects.

Required env (read from tokens.env via ``datasmith.utils``):
- HARBOR_SUPABASE_URL
- HARBOR_SUPABASE_SERVICE_KEY  (move + delete need the service role)

Usage:
    python scripts/migrate_snapshot_keys.py [--apply] [--limit N]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any

# Make sure tokens.env is loaded.
import datasmith  # noqa: F401  -- side-effect import; dotenv.load_dotenv

BUCKET = "snapshots"
PAGE_SIZE = 1000
OBJECT_NAME = "oracle.tar.gz"


def _service_headers() -> dict[str, str]:
    key = os.environ.get("HARBOR_SUPABASE_SERVICE_KEY", "")
    if not key:
        raise SystemExit("HARBOR_SUPABASE_SERVICE_KEY not set")
    return {"Authorization": f"Bearer {key}", "apikey": key}


def _request(url: str, method: str, headers: dict[str, str], data: bytes | None = None) -> bytes:
    # URLs are always constructed from HARBOR_SUPABASE_URL + a literal path, never user input.
    req = urllib.request.Request(url, data=data, headers=headers, method=method)  # noqa: S310
    with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
        return resp.read()


def list_top_level_dirs(base_url: str) -> list[str]:
    """Return the top-level 'directory' names under the snapshots bucket.

    Supabase Storage doesn't have real directories — ``list`` with a prefix
    returns objects whose name begins with that prefix. We list with an empty
    prefix and a ``/`` delimiter to get the unique first-segment names.
    """
    names: list[str] = []
    offset = 0
    while True:
        body = {
            "prefix": "",
            "limit": PAGE_SIZE,
            "offset": offset,
            "sortBy": {"column": "name", "order": "asc"},
        }
        try:
            raw = _request(
                f"{base_url}/storage/v1/object/list/{BUCKET}",
                "POST",
                {**_service_headers(), "Content-Type": "application/json"},
                data=json.dumps(body).encode(),
            )
        except urllib.error.HTTPError as exc:
            sys.stderr.write(f"list failed: {exc.code} {exc.read().decode(errors='replace')}\n")
            raise SystemExit(2) from exc
        page: list[dict[str, Any]] = json.loads(raw)
        if not page:
            break
        names.extend(entry["name"] for entry in page if entry.get("name"))
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return names


def fetch_task_map(base_url: str) -> dict[str, tuple[str, str, int]]:
    """Build legacy_task_id → (owner, repo, issue_number) from the tasks table."""
    raw = _request(
        f"{base_url}/rest/v1/tasks?select=task_id,owner,repo,issue_number",
        "GET",
        {**_service_headers(), "Accept": "application/json"},
    )
    out: dict[str, tuple[str, str, int]] = {}
    for row in json.loads(raw):
        legacy = row.get("task_id")
        owner = row.get("owner")
        repo = row.get("repo")
        issue_number = row.get("issue_number")
        if not legacy or not owner or not repo or issue_number is None:
            continue
        out[str(legacy)] = (owner, repo, int(issue_number))
    return out


def move_object(base_url: str, source_key: str, dest_key: str) -> None:
    body = {"bucketId": BUCKET, "sourceKey": source_key, "destinationKey": dest_key}
    _request(
        f"{base_url}/storage/v1/object/move",
        "POST",
        {**_service_headers(), "Content-Type": "application/json"},
        data=json.dumps(body).encode(),
    )


def object_exists(base_url: str, key: str) -> bool:
    """HEAD-style probe via list — cheaper than fetching the blob."""
    parent, _, name = key.rpartition("/")
    body = {"prefix": parent + "/", "limit": PAGE_SIZE, "search": name}
    try:
        raw = _request(
            f"{base_url}/storage/v1/object/list/{BUCKET}",
            "POST",
            {**_service_headers(), "Content-Type": "application/json"},
            data=json.dumps(body).encode(),
        )
    except urllib.error.HTTPError:
        return False
    return any(entry.get("name") == name for entry in json.loads(raw))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Actually move objects (default: dry run).")
    ap.add_argument("--limit", type=int, default=None, help="Stop after this many candidates.")
    args = ap.parse_args()

    base_url = os.environ.get("HARBOR_SUPABASE_URL", "")
    if not base_url:
        raise SystemExit("HARBOR_SUPABASE_URL not set")

    task_map = fetch_task_map(base_url)
    if not task_map:
        raise SystemExit("tasks table returned no rows with the triple — run harbor_tasks_migration.sql first")
    print(f"loaded {len(task_map)} task rows", flush=True)

    legacy_dirs = list_top_level_dirs(base_url)
    candidates = [d for d in legacy_dirs if "/" not in d and d in task_map]
    print(f"found {len(candidates)} legacy snapshot dirs to migrate", flush=True)

    n_renamed = 0
    n_skipped = 0
    n_missing = 0
    for legacy in candidates:
        if args.limit is not None and n_renamed >= args.limit:
            break
        owner, repo, issue = task_map[legacy]
        source = f"{legacy}/{OBJECT_NAME}"
        dest = f"{owner}/{repo}/{issue}/{OBJECT_NAME}"

        if not object_exists(base_url, source):
            n_missing += 1
            continue
        if object_exists(base_url, dest):
            print(f"skip (dest exists): {source} → {dest}")
            n_skipped += 1
            continue

        print(f"{'MOVE' if args.apply else 'DRY '} {source} → {dest}")
        if args.apply:
            move_object(base_url, source, dest)
        n_renamed += 1

    print(
        f"done — moved={n_renamed} skipped_dest_exists={n_skipped} no_oracle_tarball={n_missing}",
        flush=True,
    )


if __name__ == "__main__":
    main()
