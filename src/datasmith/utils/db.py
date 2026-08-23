"""Supabase client, ``@supabase_cached`` decorator, and batch upsert helpers."""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import os
import weakref
from collections.abc import Callable, Sequence
from typing import Any, TypeVar, cast

from supabase import Client, ClientOptions, create_client

logger = logging.getLogger(__name__)

# Cloudflare Access service-token credentials. When both are set, every
# Supabase HTTP request includes the CF-Access-Client-Id / Secret headers
# so requests can pass through a Cloudflare Access-protected tunnel.
DATASMITH_CF_ACCESS_CLIENT_ID: str = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_ID", "")
DATASMITH_CF_ACCESS_CLIENT_SECRET: str = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_SECRET", "")

# Guardrail against reads that stream a large text column across a whole
# table. Selecting ``patch`` table-wide on ``pull_requests`` (265 181 rows)
# did not merely run slowly — it terminated PostgREST with
# "out of memory - cannot enlarge string buffer containing 1073741822 bytes".
# Such a read must carry a filter or use a server-side aggregate.
#
# Both knobs are comma-separated and env-overridable: an operator who has
# measured a different table, or who genuinely wants the unfiltered read,
# edits ``tokens.env`` rather than the source.
DATASMITH_LARGE_TABLES: frozenset[str] = frozenset(
    part.strip() for part in os.environ.get("DATASMITH_LARGE_TABLES", "pull_requests").split(",") if part.strip()
)
DATASMITH_LARGE_COLUMNS: frozenset[str] = frozenset(
    part.strip() for part in os.environ.get("DATASMITH_LARGE_COLUMNS", "patch").split(",") if part.strip()
)

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

# One async client per running event loop. ``get_async_client`` used to build
# a fresh client — and therefore a fresh connection pool — on every call, so
# a stage running 32 coroutines opened 32 pools. Keyed weakly so a finished
# loop (pytest makes one per test) does not pin its client forever.
_async_clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any] = weakref.WeakKeyDictionary()

_CF_ACCESS_HEADERS: dict[str, str] = {}
if DATASMITH_CF_ACCESS_CLIENT_ID and DATASMITH_CF_ACCESS_CLIENT_SECRET:
    _CF_ACCESS_HEADERS = {
        "CF-Access-Client-Id": DATASMITH_CF_ACCESS_CLIENT_ID,
        "CF-Access-Client-Secret": DATASMITH_CF_ACCESS_CLIENT_SECRET,
    }


class UnfilteredLargeSelectError(RuntimeError):
    """A read would stream a large text column across an entire large table.

    Raised before the query is issued, because the failure mode on the server
    is an out-of-memory abort that takes PostgREST down for every other
    caller — a loud client-side error is strictly cheaper.
    """


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
    """Return the async Supabase client for the running event loop.

    Memoised per event loop, mirroring :func:`get_client`, so concurrent
    callers share one connection pool instead of opening one each.

    Imported lazily to avoid import errors when supabase async extras
    are not installed.
    """
    from supabase import AsyncClientOptions, acreate_client

    loop = asyncio.get_running_loop()
    existing = _async_clients.get(loop)
    if existing is not None:
        return existing

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
    if _CF_ACCESS_HEADERS:
        options = AsyncClientOptions(headers=_CF_ACCESS_HEADERS)
        client = await acreate_client(url, key, options=options)
    else:
        client = await acreate_client(url, key)
    # Two coroutines can both miss the check above and both await a create.
    # ``setdefault`` makes the first one to finish the winner for everybody.
    return _async_clients.setdefault(loop, client)


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


async def abatch_upsert(table: str, rows: list[dict[str, Any]], chunk_size: int = 100) -> int:
    """Async sibling of :func:`batch_upsert`.

    Chunks are written sequentially, exactly as the sync version does.
    Overlapping upserts against one table buy nothing and invite deadlocks.
    """
    if not rows:
        return 0
    client = await get_async_client()
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        await client.table(table).upsert(chunk).execute()
        total += len(chunk)
    return total


def _selected_columns(select: str) -> list[str]:
    """Split a PostgREST ``select`` string into the column names it names.

    ``alias:column`` renames resolve to the real column, so a guardrail is
    not fooled by ``diff:patch``. Substring matching would instead
    false-positive on names like ``patch_url``.
    """
    columns: list[str] = []
    for part in select.split(","):
        name = part.strip()
        if not name:
            continue
        if ":" in name:
            name = name.split(":", 1)[1].strip()
        if name:
            columns.append(name)
    return columns


def _guard_large_select(table: str, select: str, has_filters: bool) -> None:
    """Refuse an unfiltered read of a large text column on a large table."""
    if has_filters or table not in DATASMITH_LARGE_TABLES:
        return
    columns = _selected_columns(select)
    if "*" in columns:
        offending = "*"
    else:
        large = sorted(c for c in columns if c in DATASMITH_LARGE_COLUMNS)
        if not large:
            return
        offending = ", ".join(large)
    raise UnfilteredLargeSelectError(
        f"refusing an unfiltered read of {offending!r} on table {table!r}: it streams a large "
        f"text column across every row and has already taken PostgREST down with an "
        f"out-of-memory abort. Narrow it with a filter "
        f"(filters/gte_filters/lte_filters/lt_filters/neq_filters/in_filters/is_null), ask for only the "
        f"columns you need, or use a server-side aggregate. To override, set "
        f"DATASMITH_LARGE_TABLES or DATASMITH_LARGE_COLUMNS in tokens.env."
    )


def _resolve_order_cols(table: str, order_by: Sequence[str] | str | None) -> tuple[str, ...]:
    """Resolve the ORDER BY columns that make pagination deterministic."""
    if order_by is None:
        return _DEFAULT_ORDER_BY.get(table, ())
    if isinstance(order_by, str):
        return (order_by,)
    return tuple(order_by)


def _apply_query_parts(
    query: Any,
    filters: dict[str, Any] | None,
    is_null: list[str] | None,
    gte_filters: dict[str, Any] | None,
    lte_filters: dict[str, Any] | None,
    lt_filters: dict[str, Any] | None,
    neq_filters: dict[str, Any] | None,
    in_filters: dict[str, Sequence[Any]] | None,
    order_cols: Sequence[str],
) -> Any:
    """Attach every filter and ordering clause to *query*.

    Shared by :func:`fetch_all` and :func:`afetch_all` so the two cannot
    drift apart in which predicates they support.
    """
    for col, val in (filters or {}).items():
        query = query.eq(col, val)
    for col in is_null or []:
        query = query.is_(col, "null")
    for col, val in (gte_filters or {}).items():
        query = query.gte(col, val)
    for col, val in (lte_filters or {}).items():
        query = query.lte(col, val)
    for col, val in (lt_filters or {}).items():
        query = query.lt(col, val)
    for col, val in (neq_filters or {}).items():
        query = query.neq(col, val)
    for col, values in (in_filters or {}).items():
        query = query.in_(col, list(values))
    for col in order_cols:
        query = query.order(col)
    return query


def _warn_unstable_pagination(table: str) -> None:
    logger.warning(
        "fetch_all(%r) crossed page boundary without an order_by; "
        "pagination is non-deterministic and may drop or duplicate rows. "
        "Add the table's primary key to _DEFAULT_ORDER_BY or pass order_by explicitly.",
        table,
    )


# The column the run window is measured on, written down once.  It is a schema
# column rather than a tunable knob, so it stays a literal: CLAUDE.md's rule
# about DATASMITH_* env overrides explicitly carves out protocol fields and
# schema columns, and a window that could be repointed at another column from
# the environment would defeat the point of having one definition.
WINDOW_COLUMN = "merged_at"


def window_filters(start_date: str | None, end_date: str | None) -> dict[str, Any]:
    """Return the ``fetch_all`` kwargs that select one pipeline run's window.

    The window is ``merged_at``, half-open ``[start_date, end_date)``, and this
    is the only place either half of that sentence is written down.  Callers
    spread it into their query::

        rows = fetch_all("pull_requests", select=..., filters=..., **window_filters(start, end))

    so that no stage can name the wrong column or the wrong boundary by hand.
    Every stage is asking the same question -- which tasks does this run own --
    and the answer stops being a matter of remembering to copy it correctly.

    **Why half-open.**  Consecutive monthly runs must *partition* the corpus,
    not overlap at midnight.  Under an inclusive upper bound a PR merged at
    exactly ``2026-02-01T00:00:00Z`` belongs to January's window and to
    February's alike, so both runs claim it and every stage downstream does its
    work for it twice: two classifications, two syntheses, two Harbor trials,
    two published records.  Excluding the upper bound makes ``[Jan, Feb)`` and
    ``[Feb, Mar)`` join up to exactly ``[Jan, Mar)`` -- no row counted twice,
    none skipped -- and it is the same boundary stage 2 asks GitHub for, so the
    database and the search API agree on which side of midnight a PR falls.

    **Why ``merged_at`` and not ``created_at``.**  A run is responsible for the
    work that *landed* while it was watching.  ``created_at`` answers a
    different question -- when somebody opened a pull request -- and its answer
    is biased against exactly the repositories this dataset is made of: a
    project with a deliberate review process merges PRs that were opened weeks
    or months earlier, and a ``created_at`` window drops all of them.  Stages
    3-7 windowed ``created_at`` for months without anyone noticing, because
    stage 2 stored only PRs both created *and* merged inside the window, so no
    stored row could have ``created_at`` before the window and the mismatch
    never showed.

    A ``None`` bound is omitted rather than guessed at, so a query left open on
    one side stays open there while the other side keeps its meaning.
    """
    kwargs: dict[str, Any] = {}
    if start_date:
        kwargs["gte_filters"] = {WINDOW_COLUMN: start_date}
    if end_date:
        # Strict less-than, not ``lte`` minus a second: see ``fetch_all``.
        kwargs["lt_filters"] = {WINDOW_COLUMN: end_date}
    return kwargs


def fetch_all(
    table: str,
    select: str = "*",
    filters: dict[str, Any] | None = None,
    is_null: list[str] | None = None,
    gte_filters: dict[str, Any] | None = None,
    lte_filters: dict[str, Any] | None = None,
    lt_filters: dict[str, Any] | None = None,
    neq_filters: dict[str, Any] | None = None,
    in_filters: dict[str, Sequence[Any]] | None = None,
    page_size: int = 1000,
    order_by: Sequence[str] | str | None = None,
) -> list[dict[str, Any]]:
    """Paginate through all rows matching the query.

    Supabase/PostgREST caps responses at 1 000 rows by default.
    This helper fetches successive pages using ``range()`` until
    a page returns fewer than *page_size* rows.

    ``lt_filters`` is a strict less-than, which the half-open window
    ``[start, end)`` needs: approximating it as ``lte`` minus a second
    makes the boundary mean something different in the database than it
    does in the GitHub query it must agree with.

    ``in_filters`` maps a column to the values it may take. It exists so a
    skip-set read can be scoped to the keys the caller actually asked about
    instead of pulling a whole table: an equality filter on ``(owner, repo)``
    still returns every row that repository ever produced, which for a
    150-repository window is the table again in 150 requests. Chunk the value
    list at the call site — PostgREST puts it in the URL, and a few hundred
    40-character SHAs exceed the usual 8 KB proxy ceiling.

    ``order_by`` must specify a stable (unique) key — otherwise
    Postgres is free to return rows in different orders across
    pages, silently dropping and duplicating rows. If the caller
    doesn't pass one, we fall back to ``_DEFAULT_ORDER_BY[table]``
    (the table's primary key). Tables missing from that map will
    emit a warning when pagination actually crosses a page boundary.

    Raises :class:`UnfilteredLargeSelectError` if the read would stream a
    large text column across a whole large table.
    """
    _guard_large_select(
        table,
        select,
        any((filters, is_null, gte_filters, lte_filters, lt_filters, neq_filters, in_filters)),
    )
    order_cols = _resolve_order_cols(table, order_by)

    client = get_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    warned_unstable = False
    while True:
        query = _apply_query_parts(
            client.table(table).select(select),
            filters,
            is_null,
            gte_filters,
            lte_filters,
            lt_filters,
            neq_filters,
            in_filters,
            order_cols,
        )
        resp = query.range(offset, offset + page_size - 1).execute()
        page = cast(list[dict[str, Any]], resp.data or [])
        rows.extend(page)
        if len(page) < page_size:
            break
        if not order_cols and not warned_unstable:
            _warn_unstable_pagination(table)
            warned_unstable = True
        offset += page_size
    return rows


async def afetch_all(
    table: str,
    select: str = "*",
    filters: dict[str, Any] | None = None,
    is_null: list[str] | None = None,
    gte_filters: dict[str, Any] | None = None,
    lte_filters: dict[str, Any] | None = None,
    lt_filters: dict[str, Any] | None = None,
    neq_filters: dict[str, Any] | None = None,
    in_filters: dict[str, Sequence[Any]] | None = None,
    page_size: int = 1000,
    order_by: Sequence[str] | str | None = None,
) -> list[dict[str, Any]]:
    """Async sibling of :func:`fetch_all`.

    Same pagination, same filter kwargs, same guardrail — but issued on the
    async client, so callers inside ``async def`` stop serialising every
    database round trip on the event loop.
    """
    _guard_large_select(
        table,
        select,
        any((filters, is_null, gte_filters, lte_filters, lt_filters, neq_filters, in_filters)),
    )
    order_cols = _resolve_order_cols(table, order_by)

    client = await get_async_client()
    rows: list[dict[str, Any]] = []
    offset = 0
    warned_unstable = False
    while True:
        query = _apply_query_parts(
            client.table(table).select(select),
            filters,
            is_null,
            gte_filters,
            lte_filters,
            lt_filters,
            neq_filters,
            in_filters,
            order_cols,
        )
        resp = await query.range(offset, offset + page_size - 1).execute()
        page = cast(list[dict[str, Any]], resp.data or [])
        rows.extend(page)
        if len(page) < page_size:
            break
        if not order_cols and not warned_unstable:
            _warn_unstable_pagination(table)
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
