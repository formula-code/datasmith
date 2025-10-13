"""Benchmark data storage and management for DataSmith pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from datasmith.logging_config import get_logger
from .database import DataSmithDB

logger = get_logger("storage.benchmarks")


@dataclass
class BenchmarkCollection:
    """Benchmark collection data model (replaces dashboard.fc.pkl)."""
    id: Optional[int]
    repository_id: int
    base_url: str
    collected_at: datetime
    modified_at: datetime
    param_keys: list[str]
    index_data: dict[str, Any]
    collection_metadata: Optional[dict[str, Any]] = None
    
    def __post_init__(self):
        if self.collection_metadata is None:
            self.collection_metadata = {}
    
    @classmethod
    def from_row(cls, row) -> BenchmarkCollection:
        """Create BenchmarkCollection from database row."""
        param_keys = json.loads(row['param_keys']) if row['param_keys'] else []
        index_data = json.loads(row['index_data']) if row['index_data'] else {}
        collection_metadata = json.loads(row['collection_metadata']) if row['collection_metadata'] else {}
        collected_at = datetime.fromisoformat(row['collected_at']) if row['collected_at'] else datetime.utcnow()
        modified_at = datetime.fromisoformat(row['modified_at']) if row['modified_at'] else datetime.utcnow()
        
        return cls(
            id=row['id'],
            repository_id=row['repository_id'],
            base_url=row['base_url'],
            collected_at=collected_at,
            modified_at=modified_at,
            param_keys=param_keys,
            index_data=index_data,
            collection_metadata=collection_metadata
        )


@dataclass
class BenchmarkRun:
    """Individual benchmark run data model."""
    id: Optional[int]
    collection_id: int
    commit_sha: str
    benchmark_name: str
    machine_name: str
    value: float
    unit: Optional[str] = None
    params: Optional[dict[str, Any]] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    run_metadata: Optional[dict[str, Any]] = None
    
    def __post_init__(self):
        if self.params is None:
            self.params = {}
        if self.run_metadata is None:
            self.run_metadata = {}
    
    @classmethod
    def from_row(cls, row) -> BenchmarkRun:
        """Create BenchmarkRun from database row."""
        params = json.loads(row['params']) if row['params'] else {}
        run_metadata = json.loads(row['run_metadata']) if row['run_metadata'] else {}
        started_at = datetime.fromisoformat(row['started_at']) if row['started_at'] else None
        finished_at = datetime.fromisoformat(row['finished_at']) if row['finished_at'] else None
        
        return cls(
            id=row['id'],
            collection_id=row['collection_id'],
            commit_sha=row['commit_sha'],
            benchmark_name=row['benchmark_name'],
            machine_name=row['machine_name'],
            value=float(row['value']),
            unit=row['unit'],
            params=params,
            started_at=started_at,
            finished_at=finished_at,
            run_metadata=run_metadata
        )


@dataclass 
class Breakpoint:
    """Performance breakpoint data model."""
    id: Optional[int]
    collection_id: int
    commit_sha: str
    benchmark_name: str
    machine_name: str
    change_type: str  # 'improvement', 'regression', 'neutral'
    confidence_score: float
    detection_method: str
    before_value: float
    after_value: float
    relative_change: float
    absolute_change: float
    before_commit_sha: Optional[str] = None
    after_commit_sha: Optional[str] = None
    detected_at: Optional[datetime] = None
    coverage_data: Optional[dict[str, Any]] = None
    github_data: Optional[dict[str, Any]] = None
    breakpoint_metadata: Optional[dict[str, Any]] = None
    
    def __post_init__(self):
        if self.detected_at is None:
            self.detected_at = datetime.utcnow()
        if self.coverage_data is None:
            self.coverage_data = {}
        if self.github_data is None:
            self.github_data = {}
        if self.breakpoint_metadata is None:
            self.breakpoint_metadata = {}
    
    @classmethod
    def from_row(cls, row) -> Breakpoint:
        """Create Breakpoint from database row."""
        detected_at = datetime.fromisoformat(row['detected_at']) if row['detected_at'] else None
        coverage_data = json.loads(row['coverage_data']) if row['coverage_data'] else {}
        github_data = json.loads(row['github_data']) if row['github_data'] else {}
        breakpoint_metadata = json.loads(row['breakpoint_metadata']) if row['breakpoint_metadata'] else {}
        
        return cls(
            id=row['id'],
            collection_id=row['collection_id'],
            commit_sha=row['commit_sha'],
            benchmark_name=row['benchmark_name'],
            machine_name=row['machine_name'],
            change_type=row['change_type'],
            confidence_score=float(row['confidence_score']),
            detection_method=row['detection_method'],
            before_value=float(row['before_value']),
            after_value=float(row['after_value']),
            relative_change=float(row['relative_change']),
            absolute_change=float(row['absolute_change']),
            before_commit_sha=row['before_commit_sha'],
            after_commit_sha=row['after_commit_sha'],
            detected_at=detected_at,
            coverage_data=coverage_data,
            github_data=github_data,
            breakpoint_metadata=breakpoint_metadata
        )


class BenchmarkStore:
    """Benchmark data storage operations."""
    
    def __init__(self, db: DataSmithDB):
        self.db = db
    
    def insert_collection(self, collection: BenchmarkCollection) -> int:
        """Insert a benchmark collection.
        
        Args:
            collection: Benchmark collection to insert
            
        Returns:
            int: Collection ID
        """
        with self.db.transaction() as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO benchmark_collections 
                (repository_id, base_url, collected_at, modified_at, param_keys, 
                 index_data, collection_metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                collection.repository_id, collection.base_url, collection.collected_at,
                collection.modified_at, json.dumps(collection.param_keys),
                json.dumps(collection.index_data), json.dumps(collection.collection_metadata)
            ))
            
            collection_id = cursor.lastrowid
            if collection_id:
                logger.debug(f"Inserted benchmark collection with ID {collection_id}")
                return collection_id
            else:
                raise ValueError("Failed to insert benchmark collection")
    
    def insert_runs(self, runs: list[BenchmarkRun]) -> list[int]:
        """Insert multiple benchmark runs in batch.
        
        Args:
            runs: List of benchmark runs to insert
            
        Returns:
            list[int]: List of run IDs
        """
        if not runs:
            return []
        
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            run_ids = []
            
            for run in runs:
                cursor.execute("""
                    INSERT OR REPLACE INTO benchmark_runs 
                    (collection_id, commit_sha, benchmark_name, machine_name, value,
                     unit, params, started_at, finished_at, run_metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    run.collection_id, run.commit_sha, run.benchmark_name, run.machine_name,
                    run.value, run.unit, json.dumps(run.params), run.started_at,
                    run.finished_at, json.dumps(run.run_metadata)
                ))
                
                run_ids.append(cursor.lastrowid)
            
            logger.info(f"Inserted {len(runs)} benchmark runs")
            return run_ids
    
    def insert_breakpoint(self, breakpoint: Breakpoint) -> int:
        """Insert a performance breakpoint.
        
        Args:
            breakpoint: Breakpoint to insert
            
        Returns:
            int: Breakpoint ID
        """
        with self.db.transaction() as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO breakpoints 
                (collection_id, commit_sha, benchmark_name, machine_name, change_type,
                 confidence_score, detection_method, before_value, after_value,
                 relative_change, absolute_change, before_commit_sha, after_commit_sha,
                 detected_at, coverage_data, github_data, breakpoint_metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                breakpoint.collection_id, breakpoint.commit_sha, breakpoint.benchmark_name,
                breakpoint.machine_name, breakpoint.change_type, breakpoint.confidence_score,
                breakpoint.detection_method, breakpoint.before_value, breakpoint.after_value,
                breakpoint.relative_change, breakpoint.absolute_change, breakpoint.before_commit_sha,
                breakpoint.after_commit_sha, breakpoint.detected_at, json.dumps(breakpoint.coverage_data),
                json.dumps(breakpoint.github_data), json.dumps(breakpoint.breakpoint_metadata)
            ))
            
            breakpoint_id = cursor.lastrowid
            if breakpoint_id:
                logger.debug(f"Inserted breakpoint with ID {breakpoint_id}")
                return breakpoint_id
            else:
                raise ValueError("Failed to insert breakpoint")
    
    def get_collection(self, repository_id: int, base_url: str) -> Optional[BenchmarkCollection]:
        """Get benchmark collection by repository and base URL.
        
        Args:
            repository_id: Repository ID
            base_url: Dashboard base URL
            
        Returns:
            Optional[BenchmarkCollection]: Collection if found, None otherwise
        """
        row = self.db.fetchone(
            "SELECT * FROM benchmark_collections WHERE repository_id = ? AND base_url = ?",
            (repository_id, base_url)
        )
        return BenchmarkCollection.from_row(row) if row else None
    
    def get_runs_for_collection(self, collection_id: int, commit_sha: Optional[str] = None) -> list[BenchmarkRun]:
        """Get benchmark runs for a collection.
        
        Args:
            collection_id: Collection ID
            commit_sha: Optional commit SHA filter
            
        Returns:
            list[BenchmarkRun]: List of benchmark runs
        """
        if commit_sha:
            query = "SELECT * FROM benchmark_runs WHERE collection_id = ? AND commit_sha = ?"
            params = (collection_id, commit_sha)
        else:
            query = "SELECT * FROM benchmark_runs WHERE collection_id = ?"
            params = (collection_id,)
        
        rows = self.db.fetchall(query, params)
        return [BenchmarkRun.from_row(row) for row in rows]
    
    def get_breakpoints_for_collection(self, 
                                      collection_id: int, 
                                      change_type: Optional[str] = None) -> list[Breakpoint]:
        """Get breakpoints for a collection.
        
        Args:
            collection_id: Collection ID
            change_type: Optional change type filter ('improvement', 'regression', 'neutral')
            
        Returns:
            list[Breakpoint]: List of breakpoints
        """
        if change_type:
            query = "SELECT * FROM breakpoints WHERE collection_id = ? AND change_type = ? ORDER BY detected_at DESC"
            params = (collection_id, change_type)
        else:
            query = "SELECT * FROM breakpoints WHERE collection_id = ? ORDER BY detected_at DESC"
            params = (collection_id,)
        
        rows = self.db.fetchall(query, params)
        return [Breakpoint.from_row(row) for row in rows]