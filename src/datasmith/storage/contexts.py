"""Build context storage and management for DataSmith pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from datasmith.logging_config import get_logger
from .database import DataSmithDB

logger = get_logger("storage.contexts")


@dataclass
class BuildContext:
    """Build context data model."""
    id: Optional[int]
    repository_id: int
    sha: str
    tag: str = 'pkg'
    commit_date: Optional[datetime] = None
    
    # Docker context data
    dockerfile_data: Optional[str] = None
    entrypoint_data: Optional[str] = None
    env_building_data: Optional[str] = None
    base_building_data: Optional[str] = None
    building_data: Optional[str] = None
    run_building_data: Optional[str] = None
    
    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    validated: bool = False
    validation_result: Optional[dict[str, Any]] = None
    build_logs: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()
        if self.validation_result is None:
            self.validation_result = {}
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'id': self.id,
            'repository_id': self.repository_id,
            'sha': self.sha,
            'tag': self.tag,
            'commit_date': self.commit_date.isoformat() if self.commit_date else None,
            'dockerfile_data': self.dockerfile_data,
            'entrypoint_data': self.entrypoint_data,
            'env_building_data': self.env_building_data,
            'base_building_data': self.base_building_data,
            'building_data': self.building_data,
            'run_building_data': self.run_building_data,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'validated': self.validated,
            'validation_result': self.validation_result,
            'build_logs': self.build_logs
        }
    
    @classmethod
    def from_row(cls, row) -> BuildContext:
        """Create BuildContext from database row."""
        validation_result = json.loads(row['validation_result']) if row['validation_result'] else {}
        commit_date = datetime.fromisoformat(row['commit_date']) if row['commit_date'] else None
        created_at = datetime.fromisoformat(row['created_at']) if row['created_at'] else None
        updated_at = datetime.fromisoformat(row['updated_at']) if row['updated_at'] else None
        
        return cls(
            id=row['id'],
            repository_id=row['repository_id'],
            sha=row['sha'],
            tag=row['tag'],
            commit_date=commit_date,
            dockerfile_data=row['dockerfile_data'],
            entrypoint_data=row['entrypoint_data'],
            env_building_data=row['env_building_data'],
            base_building_data=row['base_building_data'],
            building_data=row['building_data'],
            run_building_data=row['run_building_data'],
            created_at=created_at,
            updated_at=updated_at,
            validated=bool(row['validated']),
            validation_result=validation_result,
            build_logs=row['build_logs']
        )


class BuildContextStore:
    """Build context storage operations."""
    
    def __init__(self, db: DataSmithDB):
        self.db = db
    
    def insert_context(self, context: BuildContext) -> int:
        """Insert a build context.
        
        Args:
            context: Build context to insert
            
        Returns:
            int: Context ID
        """
        with self.db.transaction() as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO build_contexts 
                (repository_id, sha, tag, commit_date, dockerfile_data, entrypoint_data,
                 env_building_data, base_building_data, building_data, run_building_data,
                 created_at, updated_at, validated, validation_result, build_logs)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                context.repository_id, context.sha, context.tag, context.commit_date,
                context.dockerfile_data, context.entrypoint_data, context.env_building_data,
                context.base_building_data, context.building_data, context.run_building_data,
                context.created_at, context.updated_at, context.validated,
                json.dumps(context.validation_result), context.build_logs
            ))
            
            context_id = cursor.lastrowid
            if context_id:
                logger.debug(f"Inserted build context {context.sha}:{context.tag} with ID {context_id}")
                return context_id
            else:
                raise ValueError(f"Failed to insert build context {context.sha}:{context.tag}")
    
    def get_context(self, repository_id: int, sha: str, tag: str = 'pkg') -> Optional[BuildContext]:
        """Get build context by repository, SHA, and tag.
        
        Args:
            repository_id: Repository ID
            sha: Commit SHA
            tag: Context tag
            
        Returns:
            Optional[BuildContext]: Build context if found, None otherwise
        """
        row = self.db.fetchone(
            "SELECT * FROM build_contexts WHERE repository_id = ? AND sha = ? AND tag = ?",
            (repository_id, sha, tag)
        )
        return BuildContext.from_row(row) if row else None
    
    def get_contexts_for_repository(self, 
                                   repository_id: int,
                                   validated_only: bool = False,
                                   limit: Optional[int] = None) -> list[BuildContext]:
        """Get build contexts for a repository.
        
        Args:
            repository_id: Repository ID
            validated_only: If True, only return validated contexts
            limit: Maximum number of contexts to return
            
        Returns:
            list[BuildContext]: List of build contexts
        """
        query = "SELECT * FROM build_contexts WHERE repository_id = ?"
        params = [repository_id]
        
        if validated_only:
            query += " AND validated = TRUE"
        
        query += " ORDER BY created_at DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        rows = self.db.fetchall(query, tuple(params))
        return [BuildContext.from_row(row) for row in rows]
    
    def update_validation_status(self, 
                                context_id: int, 
                                validated: bool, 
                                validation_result: dict[str, Any],
                                build_logs: Optional[str] = None) -> None:
        """Update context validation status.
        
        Args:
            context_id: Context ID
            validated: Validation result
            validation_result: Detailed validation results
            build_logs: Build logs (optional)
        """
        self.db.execute("""
            UPDATE build_contexts 
            SET validated = ?, validation_result = ?, build_logs = ?, updated_at = ?
            WHERE id = ?
        """, (
            validated, 
            json.dumps(validation_result), 
            build_logs, 
            datetime.utcnow(),
            context_id
        ))
        logger.debug(f"Updated validation status for context ID {context_id}: {validated}")
    
    def get_context_stats(self) -> dict[str, Any]:
        """Get build context statistics.
        
        Returns:
            dict: Context statistics
        """
        stats = {}
        
        # Total counts
        result = self.db.fetchone("SELECT COUNT(*) as total FROM build_contexts")
        stats['total_contexts'] = result['total'] if result else 0
        
        result = self.db.fetchone("SELECT COUNT(*) as validated FROM build_contexts WHERE validated = TRUE")
        stats['validated_contexts'] = result['validated'] if result else 0
        
        # Tag distribution
        tag_stats = self.db.fetchall("""
            SELECT tag, COUNT(*) as count 
            FROM build_contexts 
            GROUP BY tag 
            ORDER BY count DESC
        """)
        stats['contexts_by_tag'] = {row['tag']: row['count'] for row in tag_stats}
        
        return stats