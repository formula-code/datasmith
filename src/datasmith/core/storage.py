"""SQLite-backed pipeline storage for DataFrames.

Replaces per-step parquet files with tables inside a single SQLite database.
The DB path is configurable via the ``PIPELINE_DB`` environment variable
(default: ``scratch/artifacts/pipeflush.db``).
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Module-level lock for serialising writes (pipeline is sequential anyway)
# ---------------------------------------------------------------------------
_write_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Known complex columns that need JSON serialisation for SQLite storage
# ---------------------------------------------------------------------------
_KNOWN_COMPLEX_COLUMNS: set[str] = {
    "pr_user",
    "pr_base",
    "pr_head",
    "pr__links",
    "pr_labels",
    "pr_assignees",
    "pr_requested_reviewers",
    "pr_requested_teams",
    "pr_milestone",
    "pr_assignee",
    "analysis_build_command",
    "analysis_install_command",
    "analysis_final_dependencies",
    "pr_base_license",
    "pr_base_owner",
    "pr_base_topics",
}

# Columns that are *already* JSON text produced upstream (collect_perf_commits)
_ALREADY_JSON_COLUMNS: set[str] = {
    "all_data",
    "problem_sections",
    "final_results",
}

# ---------------------------------------------------------------------------
# Default DB path
# ---------------------------------------------------------------------------
_DEFAULT_DB = "scratch/artifacts/pipeflush.db"


def get_pipeline_db() -> str:
    """Return the pipeline DB path from ``PIPELINE_DB`` env var or the default."""
    return os.environ.get("PIPELINE_DB", _DEFAULT_DB)


def get_pipeline_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Open a SQLite connection configured for concurrent workloads.

    Mirrors the pattern from ``get_db_connection`` in
    ``src/datasmith/core/cache/decorators.py``.
    """
    db_path = db_path or get_pipeline_db()
    # Ensure parent directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    jm = conn.execute("PRAGMA journal_mode=WAL;").fetchone()[0].lower()
    if jm != "wal":
        conn.execute("PRAGMA journal_mode=TRUNCATE;")
        conn.execute("PRAGMA synchronous=FULL;")
    else:
        conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


# ---------------------------------------------------------------------------
# Table-name helpers
# ---------------------------------------------------------------------------

def path_to_table_name(file_path: str | Path) -> str:
    """Convert a file path to a table name.

    ``foo/bar.parquet``       -> ``bar``
    ``foo/bar.raw.parquet``   -> ``bar_raw``
    ``foo/bar_2.parquet``     -> ``bar_2``
    """
    p = Path(file_path)
    name = p.name
    # Strip the final .parquet suffix
    if name.endswith(".parquet"):
        name = name[: -len(".parquet")]
    # Convert remaining dots to underscores (e.g. ".raw" -> "_raw")
    name = name.replace(".", "_")
    # Sanitise: only keep word chars and underscores
    name = re.sub(r"[^\w]", "_", name)
    return name


def resolve_table_name(arg_value: str) -> str:
    """If *arg_value* looks like a file path, convert it; otherwise return as-is."""
    if "/" in arg_value or arg_value.endswith(".parquet"):
        return path_to_table_name(arg_value)
    return arg_value


# ---------------------------------------------------------------------------
# Complex-column serialisation helpers
# ---------------------------------------------------------------------------

def _numpy_json_default(obj: Any) -> Any:
    """Fallback serialiser for ``json.dumps`` to handle numpy types."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


def _column_needs_serialization(series: pd.Series) -> bool:
    """Heuristic: sample first non-null values and check for complex types."""
    sample = series.dropna().head(5)
    if sample.empty:
        return False
    for val in sample:
        if isinstance(val, (dict, list, np.ndarray)):
            return True
    return False


def _serialize_value(val: Any) -> Any:
    """Serialize a single value to a JSON string, if it's complex."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    # Already a string (possibly already JSON) -- pass through
    if isinstance(val, str):
        return val
    try:
        return json.dumps(val, ensure_ascii=False, default=_numpy_json_default)
    except (TypeError, ValueError):
        return str(val)


def _deserialize_value(val: Any) -> Any:
    """Attempt to deserialize a JSON string back to Python objects."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return val
    if not isinstance(val, str):
        return val
    # Quick check: does it look like JSON?
    stripped = val.strip()
    if not stripped:
        return val
    if stripped[0] in ("{", "["):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            return val
    return val


# ---------------------------------------------------------------------------
# Core read / write
# ---------------------------------------------------------------------------

def write_table(
    df: pd.DataFrame,
    table_name: str,
    db_path: str | None = None,
    if_exists: str = "replace",
) -> None:
    """Write a DataFrame to a SQLite table, serialising complex columns.

    Parameters
    ----------
    df : DataFrame
        The data to write.
    table_name : str
        Target table name inside the DB.
    db_path : str, optional
        Path to the SQLite database.  Falls back to ``get_pipeline_db()``.
    if_exists : str
        Pandas ``to_sql`` *if_exists* argument (default ``"replace"``).
    """
    db_path = db_path or get_pipeline_db()

    # Identify columns requiring serialisation
    complex_cols: set[str] = set()
    for col in df.columns:
        if col in _KNOWN_COMPLEX_COLUMNS:
            complex_cols.add(col)
        elif col not in _ALREADY_JSON_COLUMNS and _column_needs_serialization(df[col]):
            complex_cols.add(col)

    # Make a shallow copy so we don't mutate the caller's DataFrame
    df_out = df.copy()
    for col in complex_cols:
        df_out[col] = df_out[col].apply(_serialize_value)

    with _write_lock:
        conn = get_pipeline_connection(db_path)
        try:
            df_out.to_sql(table_name, conn, if_exists=if_exists, index=False)
            _create_indexes(conn, table_name, list(df_out.columns))
        finally:
            conn.close()


def _create_indexes(conn: sqlite3.Connection, table_name: str, columns: list[str]) -> None:
    """Create useful indexes after writing a table."""
    # Common columns
    for col in ("sha", "repo_name"):
        if col in columns:
            idx_name = f"idx_{table_name}_{col}"
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS [{idx_name}] ON [{table_name}] ([{col}])"
            )

    # perfonly-specific indexes
    if "perfonly" in table_name:
        if "is_performance_commit" in columns:
            idx = f"idx_{table_name}_is_perf"
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS [{idx}] ON [{table_name}] ([is_performance_commit])"
            )
        if "master" in table_name:
            for col in ("pr_merge_commit_sha", "pr_base_sha"):
                if col in columns:
                    idx = f"idx_{table_name}_{col}"
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS [{idx}] ON [{table_name}] ([{col}])"
                    )


def read_table(
    table_name: str,
    db_path: str | None = None,
) -> pd.DataFrame:
    """Read a table from the pipeline SQLite DB, deserialising JSON columns.

    Parameters
    ----------
    table_name : str
        Table name to read.
    db_path : str, optional
        Path to the SQLite database.
    """
    db_path = db_path or get_pipeline_db()
    conn = get_pipeline_connection(db_path)
    try:
        df = pd.read_sql_query(f"SELECT * FROM [{table_name}]", conn)
    finally:
        conn.close()

    # Deserialise known complex columns
    for col in df.columns:
        if col in _KNOWN_COMPLEX_COLUMNS:
            df[col] = df[col].apply(_deserialize_value)

    return df


def table_exists(table_name: str, db_path: str | None = None) -> bool:
    """Check whether *table_name* exists in the database."""
    db_path = db_path or get_pipeline_db()
    if not Path(db_path).exists():
        return False
    conn = get_pipeline_connection(db_path)
    try:
        cur = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def list_tables(db_path: str | None = None) -> list[str]:
    """Return a list of all table names in the database."""
    db_path = db_path or get_pipeline_db()
    if not Path(db_path).exists():
        return []
    conn = get_pipeline_connection(db_path)
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def read_parquet_or_table(
    path_or_table: str | Path,
    db_path: str | None = None,
) -> pd.DataFrame:
    """Compatibility shim: try parquet on disk first, fall back to DB table."""
    p = Path(path_or_table)
    if p.exists() and p.suffix == ".parquet":
        return pd.read_parquet(p)
    table_name = path_to_table_name(str(path_or_table)) if p.suffix else str(path_or_table)
    return read_table(table_name, db_path=db_path)
