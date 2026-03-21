"""Supabase client, ``@supabase_cached`` decorator, and batch upsert helpers."""

from __future__ import annotations

import functools
import hashlib
import json
import os
from typing import Any, Callable, TypeVar, cast

from supabase import Client, create_client

F = TypeVar("F", bound=Callable[..., Any])

_client: Client | None = None


def get_client() -> Client:
    """Return a singleton Supabase client from env vars."""
    global _client
    if _client is None:
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        _client = create_client(url, key)
    return _client


async def get_async_client() -> Any:
    """Return an async Supabase client.

    Imported lazily to avoid import errors when supabase async extras
    are not installed.
    """
    from supabase import acreate_client

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
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


def fetch_all(
    table: str,
    select: str = "*",
    filters: dict[str, Any] | None = None,
    is_null: list[str] | None = None,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    """Paginate through all rows matching the query.

    Supabase/PostgREST caps responses at 1 000 rows by default.
    This helper fetches successive pages using ``range()`` until
    a page returns fewer than *page_size* rows.
    """
    client = get_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        query = client.table(table).select(select)
        for col, val in (filters or {}).items():
            query = query.eq(col, val)
        for col in is_null or []:
            query = query.is_(col, "null")
        resp = query.range(offset, offset + page_size - 1).execute()
        page = cast(list[dict[str, Any]], resp.data or [])
        rows.extend(page)
        if len(page) < page_size:
            break
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
