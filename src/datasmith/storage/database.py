"""Core database connection and transaction management for DataSmith."""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from datasmith.logging_config import get_logger

logger = get_logger("storage.database")


class DataSmithDB:
    """Thread-safe SQLite database connection manager for DataSmith pipeline.
    
    Features:
    - Thread-local connections to avoid concurrency issues
    - WAL mode for better concurrent read/write performance
    - Foreign key constraints enabled
    - Automatic schema initialization
    - Transaction context managers for atomic operations
    """
    
    def __init__(self, db_path: Path | str, timeout: float = 30.0):
        """Initialize database connection manager.
        
        Args:
            db_path: Path to SQLite database file
            timeout: Connection timeout in seconds
        """
        self.db_path = Path(db_path)
        self.timeout = timeout
        self._local = threading.local()
        self._ensure_database_dir()
        self._ensure_schema()
        logger.info(f"Initialized DataSmith database at {self.db_path}")
    
    def _ensure_database_dir(self) -> None:
        """Create database directory if it doesn't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get or create thread-local database connection."""
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(
                self.db_path,
                timeout=self.timeout,
                check_same_thread=False
            )
            
            # Configure connection
            conn = self._local.conn
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA mmap_size = 268435456")  # 256MB
            
            # Enable row factory for dict-like access
            conn.row_factory = sqlite3.Row
            
            logger.debug(f"Created new database connection for thread {threading.get_ident()}")
        
        return self._local.conn
    
    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database transactions.
        
        Automatically commits on success or rolls back on exception.
        
        Yields:
            sqlite3.Connection: Database connection within transaction
        """
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
    
    def execute(self, query: str, params: tuple | dict | None = None) -> sqlite3.Cursor:
        """Execute a single query with automatic transaction.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            sqlite3.Cursor: Query result cursor
        """
        with self.transaction() as conn:
            if params is None:
                return conn.execute(query)
            else:
                return conn.execute(query, params)
    
    def executemany(self, query: str, params_list: list[tuple | dict]) -> sqlite3.Cursor:
        """Execute query with multiple parameter sets.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples/dicts
            
        Returns:
            sqlite3.Cursor: Query result cursor
        """
        with self.transaction() as conn:
            return conn.executemany(query, params_list)
    
    def fetchone(self, query: str, params: tuple | dict | None = None) -> sqlite3.Row | None:
        """Fetch single row from query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            sqlite3.Row | None: Single row or None if no results
        """
        cursor = self.execute(query, params)
        return cursor.fetchone()
    
    def fetchall(self, query: str, params: tuple | dict | None = None) -> list[sqlite3.Row]:
        """Fetch all rows from query.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            list[sqlite3.Row]: List of result rows
        """
        cursor = self.execute(query, params)
        return cursor.fetchall()
    
    def _ensure_schema(self) -> None:
        """Initialize database schema from schema.sql file."""
        schema_path = Path(__file__).parent / "schema.sql"
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        with self.transaction() as conn:
            schema_sql = schema_path.read_text(encoding="utf-8")
            conn.executescript(schema_sql)
            logger.info("Database schema initialized successfully")
    
    def vacuum(self) -> None:
        """Optimize database by running VACUUM."""
        conn = self._get_connection()
        conn.execute("VACUUM")
        logger.info("Database vacuum completed")
    
    def get_stats(self) -> dict[str, Any]:
        """Get database statistics.
        
        Returns:
            dict: Database statistics including table counts
        """
        stats = {}
        
        # Get table row counts
        tables = [
            'repositories', 'commits', 'build_contexts', 
            'benchmark_collections', 'benchmark_runs', 'benchmark_summaries',
            'breakpoints', 'pipeline_runs', 'pipeline_run_items'
        ]
        
        for table in tables:
            try:
                result = self.fetchone(f"SELECT COUNT(*) as count FROM {table}")
                stats[f"{table}_count"] = result['count'] if result else 0
            except sqlite3.OperationalError:
                stats[f"{table}_count"] = 0
        
        # Get database file size
        if self.db_path.exists():
            stats['db_size_bytes'] = self.db_path.stat().st_size
            stats['db_size_mb'] = round(stats['db_size_bytes'] / (1024 * 1024), 2)
        
        return stats
    
    def close(self) -> None:
        """Close database connections."""
        if hasattr(self._local, 'conn'):
            self._local.conn.close()
            delattr(self._local, 'conn')
            logger.debug("Database connection closed")
    
    def __enter__(self) -> DataSmithDB:
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()