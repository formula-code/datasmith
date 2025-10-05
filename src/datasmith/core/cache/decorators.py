"""SQLite-backed caching helpers."""

from __future__ import annotations

import functools
import pickle
import re
import sqlite3
import threading
from typing import Callable, ParamSpec, TypeVar, cast

_cache_lock = threading.Lock()
_P = ParamSpec("_P")
_T = TypeVar("_T")


def get_db_connection(db_loc: str) -> tuple[sqlite3.Cursor, sqlite3.Connection]:
    """Open a SQLite connection configured for concurrent workloads."""
    conn = sqlite3.connect(db_loc, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn.cursor(), conn


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
            key_kwargs = kwargs.copy()

            cursor, conn = get_db_connection(db_loc)
            function_name = func.__name__

            create_table_sql = (
                f"CREATE TABLE IF NOT EXISTS {table_name} ("
                "function_name TEXT,"
                "argument_blob BLOB,"
                "result_blob   BLOB,"
                "PRIMARY KEY (function_name, argument_blob)"
                ")"
            )
            with _cache_lock:
                cursor.execute(create_table_sql)
                conn.commit()

            args_blob = pickle.dumps((function_name, args, key_kwargs))

            if not bypass:
                with _cache_lock:
                    cursor.execute(
                        f"SELECT result_blob FROM {table_name} WHERE function_name = ? AND argument_blob = ?",  # noqa: S608
                        (function_name, args_blob),
                    )
                    row = cursor.fetchone()
                    if row is not None:
                        conn.close()
                        return cast(_T, pickle.loads(row[0]))  # noqa: S301

            if "bypass_cache" in func.__code__.co_varnames:
                kwargs["bypass_cache"] = bypass

            result = func(*args, **kwargs)
            result_blob = pickle.dumps(result)

            with _cache_lock:
                cursor.execute(
                    f"INSERT OR REPLACE INTO {table_name} (function_name, argument_blob, result_blob) VALUES (?, ?, ?)",  # noqa: S608
                    (function_name, args_blob, result_blob),
                )
                conn.commit()
            conn.close()
            return result

        return wrapped

    return decorator


__all__ = ["cache_completion", "get_db_connection"]
