"""Commit storage and management for DataSmith pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from datasmith.logging_config import get_logger
from .database import DataSmithDB

logger = get_logger("storage.commits")


@dataclass
class Commit:
    """Commit data model."""
    id: Optional[int]
    repository_id: int
    sha: str
    commit_date: datetime
    author_name: Optional[str] = None
    author_email: Optional[str] = None
    message: Optional[str] = None
    pr_number: Optional[int] = None
    is_merge: bool = False
    is_performance_relevant: bool = False
    collected_at: Optional[datetime] = None
    metadata: Optional[dict[str, Any]] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.collected_at is None:
            self.collected_at = datetime.utcnow()
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'id': self.id,
            'repository_id': self.repository_id,
            'sha': self.sha,
            'commit_date': self.commit_date.isoformat(),
            'author_name': self.author_name,
            'author_email': self.author_email,
            'message': self.message,
            'pr_number': self.pr_number,
            'is_merge': self.is_merge,
            'is_performance_relevant': self.is_performance_relevant,
            'collected_at': self.collected_at.isoformat() if self.collected_at else None,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_row(cls, row) -> Commit:
        """Create Commit from database row."""
        metadata = json.loads(row['metadata']) if row['metadata'] else {}
        commit_date = datetime.fromisoformat(row['commit_date']) if row['commit_date'] else datetime.utcnow()
        collected_at = datetime.fromisoformat(row['collected_at']) if row['collected_at'] else None
        
        return cls(
            id=row['id'],
            repository_id=row['repository_id'],
            sha=row['sha'],
            commit_date=commit_date,
            author_name=row['author_name'],
            author_email=row['author_email'],
            message=row['message'],
            pr_number=row['pr_number'],
            is_merge=bool(row['is_merge']),
            is_performance_relevant=bool(row['is_performance_relevant']),
            collected_at=collected_at,
            metadata=metadata
        )


class CommitStore:
    """Commit storage operations."""
    
    def __init__(self, db: DataSmithDB):
        self.db = db
    
    def insert_commit(self, commit: Commit) -> int:
        """Insert a single commit.
        
        Args:
            commit: Commit to insert
            
        Returns:
            int: Commit ID
        """
        with self.db.transaction() as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO commits 
                (repository_id, sha, commit_date, author_name, author_email, message,
                 pr_number, is_merge, is_performance_relevant, collected_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                commit.repository_id, commit.sha, commit.commit_date,
                commit.author_name, commit.author_email, commit.message,
                commit.pr_number, commit.is_merge, commit.is_performance_relevant,
                commit.collected_at, json.dumps(commit.metadata)
            ))
            
            commit_id = cursor.lastrowid
            if commit_id:
                logger.debug(f"Inserted commit {commit.sha} with ID {commit_id}")
                return commit_id
            else:
                raise ValueError(f"Failed to insert commit {commit.sha}")
    
    def insert_commits(self, commits: list[Commit]) -> list[int]:
        """Insert multiple commits in batch.
        
        Args:
            commits: List of commits to insert
            
        Returns:
            list[int]: List of commit IDs
        """
        if not commits:
            return []
        
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            commit_ids = []
            
            for commit in commits:
                cursor.execute("""
                    INSERT OR REPLACE INTO commits 
                    (repository_id, sha, commit_date, author_name, author_email, message,
                     pr_number, is_merge, is_performance_relevant, collected_at, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    commit.repository_id, commit.sha, commit.commit_date,
                    commit.author_name, commit.author_email, commit.message,
                    commit.pr_number, commit.is_merge, commit.is_performance_relevant,
                    commit.collected_at, json.dumps(commit.metadata)
                ))
                
                commit_ids.append(cursor.lastrowid)
            
            logger.info(f"Inserted {len(commits)} commits")
            return commit_ids
    
    def get_commit(self, repository_id: int, sha: str) -> Optional[Commit]:
        """Get commit by repository and SHA.
        
        Args:
            repository_id: Repository ID
            sha: Commit SHA
            
        Returns:
            Optional[Commit]: Commit if found, None otherwise
        """
        row = self.db.fetchone(
            "SELECT * FROM commits WHERE repository_id = ? AND sha = ?",
            (repository_id, sha)
        )
        return Commit.from_row(row) if row else None
    
    def get_commits_for_repository(self, 
                                  repository_id: int,
                                  limit: Optional[int] = None,
                                  performance_relevant_only: bool = False) -> list[Commit]:
        """Get commits for a repository.
        
        Args:
            repository_id: Repository ID
            limit: Maximum number of commits to return
            performance_relevant_only: If True, only return performance-relevant commits
            
        Returns:
            list[Commit]: List of commits
        """
        query = "SELECT * FROM commits WHERE repository_id = ?"
        params = [repository_id]
        
        if performance_relevant_only:
            query += " AND is_performance_relevant = TRUE"
        
        query += " ORDER BY commit_date DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        rows = self.db.fetchall(query, tuple(params))
        return [Commit.from_row(row) for row in rows]
    
    def mark_performance_relevant(self, commit_id: int, is_relevant: bool = True) -> None:
        """Mark commit as performance relevant or not.
        
        Args:
            commit_id: Commit ID
            is_relevant: Whether commit is performance relevant
        """
        self.db.execute(
            "UPDATE commits SET is_performance_relevant = ? WHERE id = ?",
            (is_relevant, commit_id)
        )
        logger.debug(f"Updated performance relevance for commit ID {commit_id}: {is_relevant}")
    
    def get_recent_commits(self, days: int = 30, limit: int = 100) -> list[Commit]:
        """Get recent commits across all repositories.
        
        Args:
            days: Number of days to look back
            limit: Maximum number of commits
            
        Returns:
            list[Commit]: Recent commits
        """
        rows = self.db.fetchall("""
            SELECT * FROM commits 
            WHERE commit_date >= datetime('now', '-{} days')
            ORDER BY commit_date DESC
            LIMIT ?
        """.format(days), (limit,))
        
        return [Commit.from_row(row) for row in rows]
    
    def get_commit_stats(self) -> dict[str, Any]:
        """Get commit statistics.
        
        Returns:
            dict: Commit statistics
        """
        stats = {}
        
        # Total counts
        result = self.db.fetchone("SELECT COUNT(*) as total FROM commits")
        stats['total_commits'] = result['total'] if result else 0
        
        result = self.db.fetchone("SELECT COUNT(*) as perf_relevant FROM commits WHERE is_performance_relevant = TRUE")
        stats['performance_relevant_commits'] = result['perf_relevant'] if result else 0
        
        # Repository distribution
        repo_stats = self.db.fetchall("""
            SELECT r.owner, r.repo, COUNT(c.id) as commit_count
            FROM repositories r
            LEFT JOIN commits c ON r.id = c.repository_id
            GROUP BY r.id
            ORDER BY commit_count DESC
            LIMIT 10
        """)
        stats['commits_by_repository'] = [
            {'repository': f"{row['owner']}/{row['repo']}", 'count': row['commit_count']}
            for row in repo_stats
        ]
        
        return stats