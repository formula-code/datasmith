"""Supabase client, ``@supabase_cached`` decorator, and batch upsert helpers."""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import os
from collections.abc import Callable, Sequence
from typing import Any, TypeVar, cast

from supabase import Client, ClientOptions, create_client

logger = logging.getLogger(__name__)

# Cloudflare Access service-token credentials. When both are set, every
# Supabase HTTP request includes the CF-Access-Client-Id / Secret headers
# so requests can pass through a Cloudflare Access-protected tunnel.
DATASMITH_CF_ACCESS_CLIENT_ID: str = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_ID", "")
DATASMITH_CF_ACCESS_CLIENT_SECRET: str = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_SECRET", "")

# Stable primary-key ordering per table, used by ``fetch_all`` to make
# range-based pagination deterministic. PostgREST/Postgres provide no
# stable ordering without an explicit ORDER BY, so paginating large
# result sets without one silently drops and duplicates rows — which
# is exactly how classify_prs and other stages ended up leaving work
# behind across repeated invocations. Keep entries in sync with the
# primary keys declared in ``supabase/migrations/``.
_DEFAULT_ORDER_BY: dict[str, tuple[str, ...]] = {
    "repositories": ("owner", "repo"),
    "pull_requests": ("owner", "repo", "issue_number"),
    "candidate_prs": ("owner", "repo", "issue_number"),
    "packages": ("owner", "repo", "sha"),
    "candidate_containers": ("owner", "repo", "sha"),
    "harbor_runs": ("run_id",),
    "error_logs": ("id",),
    "runner_progress": ("runner_id",),
    "runner_failures": ("id",),
}

F = TypeVar("F", bound=Callable[..., Any])

_client: Client | None = None

_CF_ACCESS_HEADERS: dict[str, str] = {}
if DATASMITH_CF_ACCESS_CLIENT_ID and DATASMITH_CF_ACCESS_CLIENT_SECRET:
    _CF_ACCESS_HEADERS = {
        "CF-Access-Client-Id": DATASMITH_CF_ACCESS_CLIENT_ID,
        "CF-Access-Client-Secret": DATASMITH_CF_ACCESS_CLIENT_SECRET,
    }


def get_client() -> Client:
    """Return a singleton Supabase client from env vars."""
    global _client
    if _client is None:
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        if _CF_ACCESS_HEADERS:
            options = ClientOptions(headers=_CF_ACCESS_HEADERS)
            _client = create_client(url, key, options=options)
        else:
            _client = create_client(url, key)
    return _client


async def get_async_client() -> Any:
    """Return an async Supabase client.

    Imported lazily to avoid import errors when supabase async extras
    are not installed.
    """
    from supabase import AsyncClientOptions, acreate_client

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
    if _CF_ACCESS_HEADERS:
        options = AsyncClientOptions(headers=_CF_ACCESS_HEADERS)
        return await acreate_client(url, key, options=options)
    return await acreate_client(url, key)


def stable_hash(*args: Any) -> str:
    """Produce a deterministic SHA-256 hex digest for the given arguments.

    Arguments are serialized to canonical JSON (sorted keys, no whitespace).
    """
    payload = json.dumps(args, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def batch_upsert(table: str, rows: list[dict[str, Any]], chunk_size: int = 100) -> int:
    """Insert/update *rows* into *table* in chunks. Returns total row count."""
    if not rows:
        return 0
    client = get_client()
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        client.table(table).upsert(chunk).execute()
        total += len(chunk)
    return total


def fetch_all(  # noqa: C901
    table: str,
    select: str = "*",
    filters: dict[str, Any] | None = None,
    is_null: list[str] | None = None,
    gte_filters: dict[str, Any] | None = None,
    lte_filters: dict[str, Any] | None = None,
    neq_filters: dict[str, Any] | None = None,
    page_size: int = 1000,
    order_by: Sequence[str] | str | None = None,
) -> list[dict[str, Any]]:
    """Paginate through all rows matching the query.

    Supabase/PostgREST caps responses at 1 000 rows by default.
    This helper fetches successive pages using ``range()`` until
    a page returns fewer than *page_size* rows.

    ``order_by`` must specify a stable (unique) key — otherwise
    Postgres is free to return rows in different orders across
    pages, silently dropping and duplicating rows. If the caller
    doesn't pass one, we fall back to ``_DEFAULT_ORDER_BY[table]``
    (the table's primary key). Tables missing from that map will
    emit a warning when pagination actually crosses a page boundary.
    """
    if order_by is None:
        order_cols: tuple[str, ...] = _DEFAULT_ORDER_BY.get(table, ())
    elif isinstance(order_by, str):
        order_cols = (order_by,)
    else:
        order_cols = tuple(order_by)

    client = get_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    warned_unstable = False
    while True:
        query = client.table(table).select(select)
        for col, val in (filters or {}).items():
            query = query.eq(col, val)
        for col in is_null or []:
            query = query.is_(col, "null")
        for col, val in (gte_filters or {}).items():
            query = query.gte(col, val)
        for col, val in (lte_filters or {}).items():
            query = query.lte(col, val)
        for col, val in (neq_filters or {}).items():
            query = query.neq(col, val)
        for col in order_cols:
            query = query.order(col)
        resp = query.range(offset, offset + page_size - 1).execute()
        page = cast(list[dict[str, Any]], resp.data or [])
        rows.extend(page)
        if len(page) < page_size:
            break
        if not order_cols and not warned_unstable:
            logger.warning(
                "fetch_all(%r) crossed page boundary without an order_by; "
                "pagination is non-deterministic and may drop or duplicate rows. "
                "Add the table's primary key to _DEFAULT_ORDER_BY or pass order_by explicitly.",
                table,
            )
            warned_unstable = True
        offset += page_size
    return rows


def supabase_cached(func: F) -> F:
    """Decorator that caches function results in the Supabase ``hook_cache`` table.

    The decorated function's first positional argument must expose a
    ``cache_key`` attribute (e.g. a Pydantic model with that property).

    Special kwarg ``force=True`` bypasses the cache lookup and overwrites
    the stored value.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        force = kwargs.pop("force", False)

        # Build cache key components
        entity = args[0] if args else None
        entity_key = getattr(entity, "cache_key", "unknown")
        hook_name = func.__name__
        args_hash = stable_hash(args[1:], kwargs)

        client = get_client()

        if not force:
            resp = (
                client.table("hook_cache")
                .select("result_json")
                .eq("entity_key", entity_key)
                .eq("hook_name", hook_name)
                .eq("args_hash", args_hash)
                .execute()
            )
            if resp.data:
                first = cast(dict[str, Any], resp.data[0])
                return first["result_json"]

        result = func(*args, **kwargs)

        # Serialize: Pydantic models → dict, everything else → as-is
        stored = result.model_dump(mode="json") if hasattr(result, "model_dump") else result

        client.table("hook_cache").upsert({
            "entity_key": entity_key,
            "hook_name": hook_name,
            "args_hash": args_hash,
            "result_json": stored,
        }).execute()

        return result

    return wrapper  # type: ignore[return-value]
