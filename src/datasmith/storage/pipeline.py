"""Pipeline execution tracking for DataSmith."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from datasmith.logging_config import get_logger
from .database import DataSmithDB

logger = get_logger("storage.pipeline")


@dataclass
class PipelineRun:
    """Pipeline run data model."""
    id: Optional[int]
    run_name: str
    script_name: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: str = 'running'  # 'running', 'completed', 'failed', 'cancelled'
    config: Optional[dict[str, Any]] = None
    output_summary: Optional[dict[str, Any]] = None
    error_message: Optional[str] = None
    environment_info: Optional[dict[str, Any]] = None
    
    def __post_init__(self):
        if self.config is None:
            self.config = {}
        if self.output_summary is None:
            self.output_summary = {}
        if self.environment_info is None:
            self.environment_info = {}
    
    @classmethod
    def from_row(cls, row) -> PipelineRun:
        """Create PipelineRun from database row."""
        config = json.loads(row['config']) if row['config'] else {}
        output_summary = json.loads(row['output_summary']) if row['output_summary'] else {}
        environment_info = json.loads(row['environment_info']) if row['environment_info'] else {}
        started_at = datetime.fromisoformat(row['started_at']) if row['started_at'] else datetime.utcnow()
        finished_at = datetime.fromisoformat(row['finished_at']) if row['finished_at'] else None
        
        return cls(
            id=row['id'],
            run_name=row['run_name'],
            script_name=row['script_name'],
            started_at=started_at,
            finished_at=finished_at,
            status=row['status'],
            config=config,
            output_summary=output_summary,
            error_message=row['error_message'],
            environment_info=environment_info
        )


@dataclass
class PipelineRunItem:
    """Pipeline run item data model."""
    id: Optional[int]
    run_id: int
    item_type: str
    item_id: str
    status: str = 'pending'  # 'pending', 'processing', 'completed', 'failed', 'skipped'
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
    output_data: Optional[dict[str, Any]] = None
    retry_count: int = 0
    
    def __post_init__(self):
        if self.output_data is None:
            self.output_data = {}
    
    @classmethod
    def from_row(cls, row) -> PipelineRunItem:
        """Create PipelineRunItem from database row."""
        output_data = json.loads(row['output_data']) if row['output_data'] else {}
        started_at = datetime.fromisoformat(row['started_at']) if row['started_at'] else None
        finished_at = datetime.fromisoformat(row['finished_at']) if row['finished_at'] else None
        
        return cls(
            id=row['id'],
            run_id=row['run_id'],
            item_type=row['item_type'],
            item_id=row['item_id'],
            status=row['status'],
            started_at=started_at,
            finished_at=finished_at,
            error_message=row['error_message'],
            output_data=output_data,
            retry_count=row['retry_count'] or 0
        )


class PipelineTracker:
    """Pipeline execution tracking operations."""
    
    def __init__(self, db: DataSmithDB):
        self.db = db
    
    def start_pipeline_run(self, 
                          run_name: str, 
                          script_name: str,
                          config: Optional[dict[str, Any]] = None,
                          environment_info: Optional[dict[str, Any]] = None) -> int:
        """Start a new pipeline run.
        
        Args:
            run_name: Human-readable run name
            script_name: Script being executed
            config: Configuration/arguments for the run
            environment_info: Environment information
            
        Returns:
            int: Pipeline run ID
        """
        with self.db.transaction() as conn:
            cursor = conn.execute("""
                INSERT INTO pipeline_runs 
                (run_name, script_name, started_at, status, config, environment_info)
                VALUES (?, ?, ?, 'running', ?, ?)
            """, (
                run_name, script_name, datetime.utcnow(),
                json.dumps(config or {}), json.dumps(environment_info or {})
            ))
            
            run_id = cursor.lastrowid
            if run_id:
                logger.info(f"Started pipeline run '{run_name}' with ID {run_id}")
                return run_id
            else:
                raise ValueError(f"Failed to start pipeline run '{run_name}'")
    
    def complete_pipeline_run(self, 
                             run_id: int, 
                             status: str,
                             output_summary: Optional[dict[str, Any]] = None,
                             error_message: Optional[str] = None) -> None:
        """Complete a pipeline run.
        
        Args:
            run_id: Pipeline run ID
            status: Final status ('completed', 'failed', 'cancelled')
            output_summary: Summary of run outputs
            error_message: Error message if failed
        """
        self.db.execute("""
            UPDATE pipeline_runs 
            SET finished_at = ?, status = ?, output_summary = ?, error_message = ?
            WHERE id = ?
        """, (
            datetime.utcnow(), status, 
            json.dumps(output_summary or {}), error_message, run_id
        ))
        
        logger.info(f"Completed pipeline run {run_id} with status: {status}")
    
    def track_item(self, 
                   run_id: int,
                   item_type: str,
                   item_id: str,
                   status: str,
                   error_message: Optional[str] = None,
                   output_data: Optional[dict[str, Any]] = None) -> int:
        """Track a pipeline run item.
        
        Args:
            run_id: Pipeline run ID
            item_type: Type of item ('repository', 'commit', 'context', etc.)
            item_id: Unique identifier for the item
            status: Item status
            error_message: Error message if failed
            output_data: Item-specific output data
            
        Returns:
            int: Pipeline run item ID
        """
        now = datetime.utcnow()
        
        # Check if item already exists
        existing = self.db.fetchone(
            "SELECT id, retry_count FROM pipeline_run_items WHERE run_id = ? AND item_type = ? AND item_id = ?",
            (run_id, item_type, item_id)
        )
        
        if existing:
            # Update existing item
            retry_count = existing['retry_count'] + (1 if status == 'failed' else 0)
            finished_at = now if status in ('completed', 'failed', 'skipped') else None
            
            self.db.execute("""
                UPDATE pipeline_run_items 
                SET status = ?, started_at = COALESCE(started_at, ?), finished_at = ?, 
                    error_message = ?, output_data = ?, retry_count = ?
                WHERE id = ?
            """, (
                status, now, finished_at, error_message, 
                json.dumps(output_data or {}), retry_count, existing['id']
            ))
            
            return existing['id']
        else:
            # Insert new item
            with self.db.transaction() as conn:
                cursor = conn.execute("""
                    INSERT INTO pipeline_run_items 
                    (run_id, item_type, item_id, status, started_at, finished_at, 
                     error_message, output_data, retry_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                """, (
                    run_id, item_type, item_id, status,
                    now if status == 'processing' else None,
                    now if status in ('completed', 'failed', 'skipped') else None,
                    error_message, json.dumps(output_data or {})
                ))
                
                item_id_db = cursor.lastrowid
                if item_id_db:
                    return item_id_db
                else:
                    raise ValueError(f"Failed to track item {item_type}:{item_id}")
    
    def get_pipeline_run(self, run_id: int) -> Optional[PipelineRun]:
        """Get pipeline run by ID.
        
        Args:
            run_id: Pipeline run ID
            
        Returns:
            Optional[PipelineRun]: Pipeline run if found, None otherwise
        """
        row = self.db.fetchone("SELECT * FROM pipeline_runs WHERE id = ?", (run_id,))
        return PipelineRun.from_row(row) if row else None
    
    def get_recent_runs(self, limit: int = 50) -> list[PipelineRun]:
        """Get recent pipeline runs.
        
        Args:
            limit: Maximum number of runs to return
            
        Returns:
            list[PipelineRun]: Recent pipeline runs
        """
        rows = self.db.fetchall(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?",
            (limit,)
        )
        return [PipelineRun.from_row(row) for row in rows]
    
    def get_run_items(self, run_id: int) -> list[PipelineRunItem]:
        """Get items for a pipeline run.
        
        Args:
            run_id: Pipeline run ID
            
        Returns:
            list[PipelineRunItem]: Run items
        """
        rows = self.db.fetchall(
            "SELECT * FROM pipeline_run_items WHERE run_id = ? ORDER BY started_at",
            (run_id,)
        )
        return [PipelineRunItem.from_row(row) for row in rows]
    
    def get_failed_items(self, run_id: int) -> list[PipelineRunItem]:
        """Get failed items for a pipeline run.
        
        Args:
            run_id: Pipeline run ID
            
        Returns:
            list[PipelineRunItem]: Failed items
        """
        rows = self.db.fetchall(
            "SELECT * FROM pipeline_run_items WHERE run_id = ? AND status = 'failed' ORDER BY started_at",
            (run_id,)
        )
        return [PipelineRunItem.from_row(row) for row in rows]
    
    def get_pipeline_stats(self) -> dict[str, Any]:
        """Get pipeline execution statistics.
        
        Returns:
            dict: Pipeline statistics
        """
        stats = {}
        
        # Run status distribution
        status_stats = self.db.fetchall("""
            SELECT status, COUNT(*) as count 
            FROM pipeline_runs 
            GROUP BY status
        """)
        stats['runs_by_status'] = {row['status']: row['count'] for row in status_stats}
        
        # Recent activity
        recent_activity = self.db.fetchall("""
            SELECT DATE(started_at) as date, COUNT(*) as runs
            FROM pipeline_runs 
            WHERE started_at >= datetime('now', '-30 days')
            GROUP BY DATE(started_at)
            ORDER BY date DESC
        """)
        stats['recent_daily_runs'] = {row['date']: row['runs'] for row in recent_activity}
        
        # Script popularity
        script_stats = self.db.fetchall("""
            SELECT script_name, COUNT(*) as count 
            FROM pipeline_runs 
            GROUP BY script_name 
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['popular_scripts'] = {row['script_name']: row['count'] for row in script_stats}
        
        return stats