"""Repository storage and management for DataSmith pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from datasmith.logging_config import get_logger
from .database import DataSmithDB

logger = get_logger("storage.repositories")


@dataclass
class Repository:
    """Repository data model."""
    id: Optional[int]
    owner: str
    repo: str
    url: str
    stars: int = 0
    forks: int = 0
    language: Optional[str] = None
    description: Optional[str] = None
    homepage: Optional[str] = None
    is_valid: bool = False
    discovered_at: Optional[datetime] = None
    validated_at: Optional[datetime] = None
    metadata: Optional[dict[str, Any]] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def full_name(self) -> str:
        """Get full repository name (owner/repo)."""
        return f"{self.owner}/{self.repo}"
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'id': self.id,
            'owner': self.owner,
            'repo': self.repo,
            'url': self.url,
            'stars': self.stars,
            'forks': self.forks,
            'language': self.language,
            'description': self.description,
            'homepage': self.homepage,
            'is_valid': self.is_valid,
            'discovered_at': self.discovered_at.isoformat() if self.discovered_at else None,
            'validated_at': self.validated_at.isoformat() if self.validated_at else None,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_row(cls, row) -> Repository:
        """Create Repository from database row."""
        metadata = json.loads(row['metadata']) if row['metadata'] else {}
        discovered_at = datetime.fromisoformat(row['discovered_at']) if row['discovered_at'] else None
        validated_at = datetime.fromisoformat(row['validated_at']) if row['validated_at'] else None
        
        return cls(
            id=row['id'],
            owner=row['owner'],
            repo=row['repo'],
            url=row['url'],
            stars=row['stars'],
            forks=row['forks'],
            language=row['language'],
            description=row['description'],
            homepage=row['homepage'],
            is_valid=bool(row['is_valid']),
            discovered_at=discovered_at,
            validated_at=validated_at,
            metadata=metadata
        )


class RepositoryStore:
    """Repository storage operations."""
    
    def __init__(self, db: DataSmithDB):
        self.db = db
    
    def insert_repository(self, repo: Repository) -> int:
        """Insert a single repository.
        
        Args:
            repo: Repository to insert
            
        Returns:
            int: Repository ID
        """
        with self.db.transaction() as conn:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO repositories 
                (owner, repo, url, stars, forks, language, description, homepage, 
                 is_valid, discovered_at, validated_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                repo.owner, repo.repo, repo.url, repo.stars, repo.forks,
                repo.language, repo.description, repo.homepage, repo.is_valid,
                repo.discovered_at, repo.validated_at, json.dumps(repo.metadata)
            ))
            
            repo_id = cursor.lastrowid
            if repo_id:
                logger.debug(f"Inserted repository {repo.full_name} with ID {repo_id}")
                return repo_id
            else:
                raise ValueError(f"Failed to insert repository {repo.full_name}")
    
    def insert_repositories(self, repos: list[Repository]) -> list[int]:
        """Insert multiple repositories in batch.
        
        Args:
            repos: List of repositories to insert
            
        Returns:
            list[int]: List of repository IDs
        """
        if not repos:
            return []
        
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            repo_ids = []
            
            for repo in repos:
                cursor.execute("""
                    INSERT OR REPLACE INTO repositories 
                    (owner, repo, url, stars, forks, language, description, homepage, 
                     is_valid, discovered_at, validated_at, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    repo.owner, repo.repo, repo.url, repo.stars, repo.forks,
                    repo.language, repo.description, repo.homepage, repo.is_valid,
                    repo.discovered_at, repo.validated_at, json.dumps(repo.metadata)
                ))
                
                repo_ids.append(cursor.lastrowid)
            
            logger.info(f"Inserted {len(repos)} repositories")
            return repo_ids
    
    def get_repository(self, owner: str, repo: str) -> Optional[Repository]:
        """Get repository by owner/repo name.
        
        Args:
            owner: Repository owner
            repo: Repository name
            
        Returns:
            Optional[Repository]: Repository if found, None otherwise
        """
        row = self.db.fetchone(
            "SELECT * FROM repositories WHERE owner = ? AND repo = ?",
            (owner, repo)
        )
        return Repository.from_row(row) if row else None
    
    def get_repository_by_id(self, repo_id: int) -> Optional[Repository]:
        """Get repository by ID.
        
        Args:
            repo_id: Repository ID
            
        Returns:
            Optional[Repository]: Repository if found, None otherwise
        """
        row = self.db.fetchone(
            "SELECT * FROM repositories WHERE id = ?",
            (repo_id,)
        )
        return Repository.from_row(row) if row else None
    
    def get_valid_repositories(self, 
                              limit: Optional[int] = None, 
                              min_stars: int = 0,
                              language: Optional[str] = None) -> list[Repository]:
        """Get valid repositories with optional filtering.
        
        Args:
            limit: Maximum number of repositories to return
            min_stars: Minimum star count filter
            language: Programming language filter
            
        Returns:
            list[Repository]: List of repositories
        """
        query = """
            SELECT * FROM repositories 
            WHERE is_valid = TRUE AND stars >= ?
        """
        params: list[Any] = [min_stars]
        
        if language:
            query += " AND language = ?"
            params.append(language)
        
        query += " ORDER BY stars DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        rows = self.db.fetchall(query, tuple(params))
        return [Repository.from_row(row) for row in rows]
    
    def update_validation_status(self, repo_id: int, is_valid: bool, 
                                validated_at: Optional[datetime] = None) -> None:
        """Update repository validation status.
        
        Args:
            repo_id: Repository ID
            is_valid: Validation result
            validated_at: Validation timestamp (defaults to now)
        """
        if validated_at is None:
            validated_at = datetime.utcnow()
        
        self.db.execute(
            "UPDATE repositories SET is_valid = ?, validated_at = ? WHERE id = ?",
            (is_valid, validated_at, repo_id)
        )
        logger.debug(f"Updated validation status for repository ID {repo_id}: {is_valid}")
    
    def search_repositories(self, query: str, limit: int = 100) -> list[Repository]:
        """Search repositories by name or description.
        
        Args:
            query: Search query
            limit: Maximum results
            
        Returns:
            list[Repository]: Matching repositories
        """
        search_pattern = f"%{query}%"
        rows = self.db.fetchall("""
            SELECT * FROM repositories 
            WHERE (owner LIKE ? OR repo LIKE ? OR description LIKE ?)
            ORDER BY stars DESC
            LIMIT ?
        """, (search_pattern, search_pattern, search_pattern, limit))
        
        return [Repository.from_row(row) for row in rows]
    
    def get_repository_stats(self) -> dict[str, Any]:
        """Get repository statistics.
        
        Returns:
            dict: Repository statistics
        """
        stats = {}
        
        # Total counts
        result = self.db.fetchone("SELECT COUNT(*) as total FROM repositories")
        stats['total_repositories'] = result['total'] if result else 0
        
        result = self.db.fetchone("SELECT COUNT(*) as valid FROM repositories WHERE is_valid = TRUE")
        stats['valid_repositories'] = result['valid'] if result else 0
        
        # Language distribution
        language_rows = self.db.fetchall("""
            SELECT language, COUNT(*) as count 
            FROM repositories 
            WHERE language IS NOT NULL AND is_valid = TRUE
            GROUP BY language 
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['top_languages'] = {row['language']: row['count'] for row in language_rows}
        
        # Star distribution
        star_stats = self.db.fetchone("""
            SELECT 
                AVG(stars) as avg_stars,
                MAX(stars) as max_stars,
                MIN(stars) as min_stars
            FROM repositories 
            WHERE is_valid = TRUE
        """)
        if star_stats:
            stats['star_statistics'] = {
                'average': round(star_stats['avg_stars'], 2) if star_stats['avg_stars'] else 0,
                'maximum': star_stats['max_stars'] or 0,
                'minimum': star_stats['min_stars'] or 0
            }
        
        return stats
    
    def export_to_csv(self, filepath: str, valid_only: bool = True) -> None:
        """Export repositories to CSV file.
        
        Args:
            filepath: Output CSV file path
            valid_only: If True, export only valid repositories
        """
        query = "SELECT * FROM repositories"
        if valid_only:
            query += " WHERE is_valid = TRUE"
        query += " ORDER BY stars DESC"
        
        repositories = [Repository.from_row(row) for row in self.db.fetchall(query)]
        
        # Convert to DataFrame and save
        df = pd.DataFrame([repo.to_dict() for repo in repositories])
        df.to_csv(filepath, index=False)
        logger.info(f"Exported {len(repositories)} repositories to {filepath}")
    
    def import_from_csv(self, filepath: str, mark_as_valid: bool = False) -> int:
        """Import repositories from CSV file.
        
        Args:
            filepath: Input CSV file path
            mark_as_valid: If True, mark imported repos as valid
            
        Returns:
            int: Number of repositories imported
        """
        df = pd.read_csv(filepath)
        repositories = []
        
        for _, row in df.iterrows():
            repo = Repository(
                id=None,
                owner=row.get('owner', ''),
                repo=row.get('repo', ''),
                url=row.get('url', ''),
                stars=int(row.get('stars', 0)),
                forks=int(row.get('forks', 0)),
                language=row.get('language'),
                description=row.get('description'),
                homepage=row.get('homepage'),
                is_valid=mark_as_valid or bool(row.get('is_valid', False)),
                metadata={}
            )
            repositories.append(repo)
        
        self.insert_repositories(repositories)
        logger.info(f"Imported {len(repositories)} repositories from {filepath}")
        return len(repositories)