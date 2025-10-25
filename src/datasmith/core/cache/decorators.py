"""SQLite-backed caching helpers."""

from __future__ import annotations

import contextlib
import functools
import os
import pickle
import re
import sqlite3
import threading
from collections.abc import Iterator
from typing import Callable, ParamSpec, TypeVar, cast

_cache_lock = threading.Lock()
_P = ParamSpec("_P")
_T = TypeVar("_T")


@contextlib.contextmanager
def _file_lock(lock_path: str) -> Iterator[None]:
    """Cross-process exclusive lock using a sidecar .lock file."""
    lockfile = lock_path + ".lock"
    fd = os.open(lockfile, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        try:
            import fcntl  # POSIX

            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
            fcntl.flock(fd, fcntl.LOCK_UN)
        except ImportError:  # pragma: no cover Windows-only  # type: ignore[unused-ignore]
            import msvcrt

            msvcrt.locking(fd, msvcrt.LK_LOCK, 1)  # type: ignore[attr-defined]
            yield
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
    finally:
        os.close(fd)


def get_db_connection(db_loc: str) -> sqlite3.Connection:
    """Open a SQLite connection configured for concurrent workloads."""
    conn = sqlite3.connect(db_loc, timeout=30, isolation_level=None)
    jm = conn.execute("PRAGMA journal_mode=WAL;").fetchone()[0].lower()
    if jm != "wal":
        conn.execute("PRAGMA journal_mode=TRUNCATE;")
        conn.execute("PRAGMA synchronous=FULL;")
    else:
        conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def cache_completion(db_loc: str, table_name: str = "cache") -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
    """Cache function results in a SQLite table keyed by args/kwargs.

    Passing ``bypass_cache=True`` to the wrapped function forces a refresh and
    overwrites the cached result.
    """
    if not re.match(r"^\w+$", table_name):
        raise ValueError("table_name must be alphanumeric/underscore only")

    def decorator(func: Callable[_P, _T]) -> Callable[_P, _T]:
        @functools.wraps(func)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            bypass = cast(bool, kwargs.pop("bypass_cache", False))
            key_kwargs = dict(sorted(kwargs.items()))

            conn = get_db_connection(db_loc)
            try:
                function_name = func.__name__

                # Create table once (protected by both locks)
                create_sql = (
                    f"CREATE TABLE IF NOT EXISTS {table_name} ("
                    "  function_name TEXT NOT NULL,"
                    "  argument_blob BLOB NOT NULL,"
                    "  result_blob   BLOB,"
                    "  created_at    TEXT DEFAULT CURRENT_TIMESTAMP,"
                    "  updated_at    TEXT DEFAULT CURRENT_TIMESTAMP,"
                    "  PRIMARY KEY (function_name, argument_blob)"
                    ")"
                )
                with _file_lock(db_loc), _cache_lock:
                    conn.execute(create_sql)

                args_blob = pickle.dumps((function_name, args, key_kwargs))

                if not bypass:
                    # Reads don't need the file lock, but keep the intra-process lock
                    # to avoid creating the table concurrently in-process.
                    with _cache_lock:
                        row = conn.execute(
                            f"SELECT result_blob FROM {table_name} WHERE function_name=? AND argument_blob=?",  # noqa: S608
                            (function_name, args_blob),
                        ).fetchone()
                    if row is not None:
                        return cast(_T, pickle.loads(row[0]))  # noqa: S301

                # Keep bypass_cache behavior for the wrapped function if it supports it
                if "bypass_cache" in func.__code__.co_varnames:
                    kwargs["bypass_cache"] = bypass

                # Compute result outside any locks
                result = func(*args, **kwargs)
                result_blob = pickle.dumps(result)

                # Single-statement UPSERT under cross-process lock
                with _file_lock(db_loc), _cache_lock:
                    conn.execute(
                        f"""
                        INSERT INTO {table_name}(function_name, argument_blob, result_blob)
                        VALUES(?, ?, ?)
                        ON CONFLICT(function_name, argument_blob) DO UPDATE SET
                            result_blob=excluded.result_blob,
                            updated_at=CURRENT_TIMESTAMP
                        """,
                        (function_name, args_blob, result_blob),
                    )
                return result
            finally:
                # Always close, even if func() raised
                conn.close()

        return wrapped

    return decorator


__all__ = ["cache_completion", "get_db_connection"]
