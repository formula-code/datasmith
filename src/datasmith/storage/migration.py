"""
Migration utilities for converting legacy data formats to SQLite database.

This module provides utilities to migrate existing DataSmith pipeline data
from various file formats (CSV, JSONL, JSON, Pickle) to the new SQLite database.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd

from .database import DataSmithDB
from .repositories import RepositoryStore, Repository
from .commits import CommitStore, Commit
from .contexts import BuildContextStore, BuildContext
from .benchmarks import BenchmarkStore, BenchmarkCollection, BenchmarkRun, Breakpoint
from .pipeline import PipelineTracker

logger = logging.getLogger(__name__)


class PipelineMigrator:
    """Handles migration of legacy DataSmith data to SQLite database."""

    def __init__(self, db: DataSmithDB):
        """Initialize migrator with database connection.
        
        Args:
            db: Database connection instance
        """
        self.db = db
        self.repo_store = RepositoryStore(db)
        self.commit_store = CommitStore(db)
        self.context_store = BuildContextStore(db)
        self.benchmark_store = BenchmarkStore(db)
        self.tracker = PipelineTracker(db)

    def migrate_repositories(self, csv_path: Path, max_repos: Optional[int] = None) -> int:
        """Migrate repositories from CSV file to database.
        
        Args:
            csv_path: Path to repositories CSV file
            max_repos: Optional limit on number of repositories to migrate
            
        Returns:
            int: Number of repositories migrated
        """
        logger.info(f"Migrating repositories from {csv_path}")
        
        df = pd.read_csv(csv_path)
        if max_repos:
            df = df.head(max_repos)
        
        repositories = []
        for _, row in df.iterrows():
            repo = Repository(
                id=None,
                owner=row.get('owner', ''),
                repo=row.get('repo', ''),
                url=row.get('url', ''),
                is_valid=row.get('is_valid', True),
                metadata={
                    'stars': row.get('stars'),
                    'forks': row.get('forks'),
                    'language': row.get('language'),
                    'description': row.get('description')
                }
            )
            repositories.append(repo)
        
        # Batch insert repositories
        self.repo_store.insert_repositories(repositories)
        count = len(repositories)
        logger.info(f"Migrated {count} repositories")
        return count

    def migrate_commits(self, jsonl_path: Path, max_commits: Optional[int] = None) -> int:
        """Migrate commits from JSONL file to database.
        
        Args:
            jsonl_path: Path to commits JSONL file
            max_commits: Optional limit on number of commits to migrate
            
        Returns:
            int: Number of commits migrated
        """
        logger.info(f"Migrating commits from {jsonl_path}")
        
        commits = []
        count = 0
        
        with open(jsonl_path, 'r') as f:
            for line in f:
                if max_commits and count >= max_commits:
                    break
                
                data = json.loads(line.strip())
                
                # Get or create repository
                repo = self.repo_store.get_repository(data.get('owner', ''), data.get('repo', ''))
                if not repo:
                    repo = Repository(
                        id=None,
                        owner=data.get('owner', ''),
                        repo=data.get('repo', ''),
                        url=data.get('html_url', ''),
                        is_valid=True,
                        metadata={}
                    )
                    repo_id = self.repo_store.insert_repository(repo)
                else:
                    repo_id = repo.id
                
                # Ensure repo_id is not None
                if repo_id is None:
                    logger.warning(f"Failed to get repository ID for {data.get('owner')}/{data.get('repo')}")
                    continue
                
                # Create commit
                commit = Commit(
                    id=None,
                    repository_id=repo_id,
                    sha=data.get('sha', ''),
                    commit_date=pd.to_datetime(data.get('commit_date', '')),
                    author_name=data.get('author_name'),
                    author_email=data.get('author_email'),
                    message=data.get('message'),
                    pr_number=data.get('number'),
                    is_merge=data.get('is_merge', False),
                    is_performance_relevant=data.get('is_performance_relevant', False),
                    metadata={
                        'labels': data.get('labels', []),
                        'changed_files': data.get('changed_files', []),
                        'additions': data.get('additions'),
                        'deletions': data.get('deletions')
                    }
                )
                commits.append(commit)
                count += 1
                
                # Batch insert every 1000 commits
                if len(commits) >= 1000:
                    self.commit_store.insert_commits(commits)
                    commits = []
        
        # Insert remaining commits
        if commits:
            self.commit_store.insert_commits(commits)
        
        logger.info(f"Migrated {count} commits")
        return count
    
    def migrate_context_registry(self, registry_path: Path) -> int:
        """Migrate context registry from JSON file to database.
        
        Args:
            registry_path: Path to context_registry.json
            
        Returns:
            int: Number of contexts migrated
        """
        logger.info(f"Migrating context registry from {registry_path}")
        
        # Note: This is a placeholder implementation
        # The actual implementation would need to parse the Task class
        # and handle the specific context registry format
        logger.info("Context registry migration not yet implemented")
        return 0
    
    def migrate_benchmark_collection(self, pkl_path: Path) -> int:
        """Migrate benchmark collection from pickle file to database.
        
        Args:
            pkl_path: Path to dashboard.fc.pkl file
            
        Returns:
            int: Number of benchmark runs migrated
        """
        logger.info(f"Migrating benchmark collection from {pkl_path}")
        
        # Note: This is a placeholder implementation
        # The actual implementation would need to load and parse
        # the LegacyBenchmarkCollection format
        logger.info("Benchmark collection migration not yet implemented")
        return 0
    
    def export_legacy_format(self, output_dir: Path, repository_id: int) -> None:
        """Export database data back to legacy format for backwards compatibility.
        
        Args:
            output_dir: Directory to write legacy format files
            repository_id: Repository to export
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Exporting repository {repository_id} to legacy format in {output_dir}")
        
        # Note: This is a placeholder implementation
        logger.info("Legacy format export not yet implemented")
    
    def validate_migration(self, original_files: Dict[str, Path], repository_id: Optional[int] = None) -> Dict[str, Any]:
        """Validate that migration was successful by comparing counts.
        
        Args:
            original_files: Dictionary mapping file types to paths
            repository_id: Optional repository ID to validate
            
        Returns:
            Dict[str, Any]: Validation results
        """
        results = {
            'validation_passed': True,
            'details': {},
            'errors': []
        }
        
        logger.info("Validating migration results")
        
        # Note: This is a placeholder implementation
        logger.info("Migration validation not yet implemented")
        
        return results