# DataSmith SQLite Migration Plan

## Executive Summary

This document outlines a comprehensive plan to migrate DataSmith's pipeline from intermediate JSON/CSV/Parquet files to SQLite-backed storage for improved robustness, consistency, and performance.

## Current State Analysis

### Existing Data Flows & Formats

1. **Dashboard Data**: JSONL → `dashboard.fc.pkl` (BenchmarkCollection)
2. **Repository Discovery**: CSV (`repos_discovered.csv`, `repos_valid.csv`)
3. **Commit Collections**: JSONL/Parquet (`commits_all.jsonl`, `commits_filtered.parquet`)
4. **Context Registry**: JSON (`context_registry.json` - Task→DockerContext mappings)
5. **Benchmark Results**: Individual ASV JSON files → Collated pickle files
6. **Breakpoints**: Pickle files (`breakpoints.fc.pkl`)

### Current Pain Points

- **Data Consistency**: No schema enforcement across pipeline steps
- **Partial Writes**: Risk of corruption during script interruptions
- **Query Performance**: Linear scans through large JSON/CSV files
- **Concurrency**: No atomic operations for parallel processing
- **Debugging**: Difficult to inspect intermediate states
- **Resumability**: Hard to resume failed pipeline runs

## Migration Strategy

### Phase 1: Database Design & Schema Creation

#### Core Tables

```sql
-- Repositories
CREATE TABLE repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    url TEXT NOT NULL,
    stars INTEGER DEFAULT 0,
    forks INTEGER DEFAULT 0,
    language TEXT,
    description TEXT,
    homepage TEXT,
    is_valid BOOLEAN DEFAULT FALSE,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMP NULL,
    UNIQUE(owner, repo)
);

-- Commits
CREATE TABLE commits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER REFERENCES repositories(id),
    sha TEXT NOT NULL,
    commit_date TIMESTAMP NOT NULL,
    author_name TEXT,
    author_email TEXT,
    message TEXT,
    pr_number INTEGER NULL,
    is_merge BOOLEAN DEFAULT FALSE,
    is_performance_relevant BOOLEAN DEFAULT FALSE,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(repository_id, sha)
);

-- Build Contexts
CREATE TABLE build_contexts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER REFERENCES repositories(id),
    sha TEXT NOT NULL,
    tag TEXT DEFAULT 'pkg',
    commit_date TIMESTAMP,
    
    -- Docker context data (stored as TEXT/JSON)
    dockerfile_data TEXT,
    entrypoint_data TEXT,
    env_building_data TEXT,
    base_building_data TEXT,
    building_data TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    validated BOOLEAN DEFAULT FALSE,
    validation_result TEXT, -- JSON with validation details
    
    UNIQUE(repository_id, sha, tag)
);

-- Benchmark Collections (dashboard data)
CREATE TABLE benchmark_collections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER REFERENCES repositories(id),
    base_url TEXT NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    modified_at TIMESTAMP NOT NULL,
    param_keys TEXT, -- JSON array
    index_data TEXT, -- JSON metadata
    UNIQUE(repository_id, base_url)
);

-- Benchmark Runs (individual measurements)
CREATE TABLE benchmark_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER REFERENCES benchmark_collections(id),
    commit_sha TEXT NOT NULL,
    benchmark_name TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    
    -- Performance data
    value REAL NOT NULL,
    unit TEXT,
    params TEXT, -- JSON parameters
    
    -- Timing
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    
    INDEX(collection_id, commit_sha),
    INDEX(benchmark_name, machine_name)
);

-- Breakpoints (performance changes)
CREATE TABLE breakpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collection_id INTEGER REFERENCES benchmark_collections(id),
    commit_sha TEXT NOT NULL,
    benchmark_name TEXT NOT NULL,
    
    -- Change detection
    change_type TEXT CHECK(change_type IN ('improvement', 'regression', 'neutral')),
    confidence_score REAL,
    detection_method TEXT, -- 'asv' or 'rbf'
    
    -- Performance metrics
    before_value REAL,
    after_value REAL,
    relative_change REAL,
    
    -- Metadata
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    coverage_data TEXT, -- JSON with coverage info
    
    INDEX(collection_id, change_type),
    INDEX(commit_sha)
);

-- Pipeline Runs (execution tracking)
CREATE TABLE pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_name TEXT NOT NULL,
    script_name TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    status TEXT DEFAULT 'running' CHECK(status IN ('running', 'completed', 'failed', 'cancelled')),
    config TEXT, -- JSON with script arguments/config
    output_summary TEXT, -- JSON with counts, errors, etc.
    error_message TEXT NULL
);

-- Pipeline Run Items (detailed tracking)
CREATE TABLE pipeline_run_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER REFERENCES pipeline_runs(id),
    item_type TEXT NOT NULL, -- 'repository', 'commit', 'context', etc.
    item_id TEXT NOT NULL, -- Repository ID, commit SHA, etc.
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    error_message TEXT NULL,
    output_data TEXT, -- JSON with item-specific results
    
    INDEX(run_id, status),
    INDEX(item_type, item_id)
);
```

#### Indexes for Performance

```sql
-- Repository queries
CREATE INDEX idx_repositories_owner_repo ON repositories(owner, repo);
CREATE INDEX idx_repositories_stars ON repositories(stars DESC);
CREATE INDEX idx_repositories_valid ON repositories(is_valid);

-- Commit queries
CREATE INDEX idx_commits_repo_date ON commits(repository_id, commit_date DESC);
CREATE INDEX idx_commits_sha ON commits(sha);
CREATE INDEX idx_commits_perf_relevant ON commits(is_performance_relevant);

-- Context queries
CREATE INDEX idx_build_contexts_repo_sha ON build_contexts(repository_id, sha);
CREATE INDEX idx_build_contexts_validated ON build_contexts(validated);

-- Benchmark queries
CREATE INDEX idx_benchmark_runs_commit_benchmark ON benchmark_runs(commit_sha, benchmark_name);
CREATE INDEX idx_benchmark_runs_collection_commit ON benchmark_runs(collection_id, commit_sha);

-- Pipeline tracking
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status, started_at);
CREATE INDEX idx_pipeline_run_items_status ON pipeline_run_items(run_id, status);
```

### Phase 2: Data Access Layer

#### Database Connection Management

```python
# src/datasmith/storage/database.py
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

class DataSmithDB:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self._local = threading.local()
        self._ensure_schema()
    
    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,
                check_same_thread=False
            )
            self._local.conn.execute("PRAGMA foreign_keys = ON")
            self._local.conn.execute("PRAGMA journal_mode = WAL")
        return self._local.conn
    
    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    
    def _ensure_schema(self) -> None:
        with self.transaction() as conn:
            # Create all tables from schema.sql
            schema_path = Path(__file__).parent / "schema.sql"
            conn.executescript(schema_path.read_text())
```

#### Repository Operations

```python
# src/datasmith/storage/repositories.py
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

@dataclass
class Repository:
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

class RepositoryStore:
    def __init__(self, db: DataSmithDB):
        self.db = db
    
    def insert_repositories(self, repos: List[Repository]) -> List[int]:
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            repo_ids = []
            for repo in repos:
                cursor.execute("""
                    INSERT OR IGNORE INTO repositories 
                    (owner, repo, url, stars, forks, language, description, homepage, is_valid)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (repo.owner, repo.repo, repo.url, repo.stars, repo.forks, 
                     repo.language, repo.description, repo.homepage, repo.is_valid))
                
                cursor.execute("SELECT id FROM repositories WHERE owner = ? AND repo = ?", 
                             (repo.owner, repo.repo))
                repo_ids.append(cursor.fetchone()[0])
            return repo_ids
    
    def get_valid_repositories(self, limit: Optional[int] = None) -> List[Repository]:
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM repositories WHERE is_valid = TRUE ORDER BY stars DESC"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            return [Repository(*row) for row in cursor.fetchall()]
```

### Phase 3: Script Migration

#### Migration Utilities

```python
# src/datasmith/storage/migration.py
import json
import pandas as pd
from pathlib import Path
from typing import Any, Dict, List

from datasmith.storage.database import DataSmithDB
from datasmith.storage.repositories import RepositoryStore, Repository
from datasmith.benchmark.collection import BenchmarkCollection

class PipelineMigrator:
    def __init__(self, db: DataSmithDB):
        self.db = db
        self.repo_store = RepositoryStore(db)
    
    def migrate_repositories_from_csv(self, csv_path: Path) -> None:
        """Migrate repos_valid.csv to repositories table"""
        df = pd.read_csv(csv_path)
        repositories = []
        
        for _, row in df.iterrows():
            repo = Repository(
                id=None,
                owner=row.get('owner', ''),
                repo=row.get('repo', ''),
                url=row.get('url', ''),
                stars=row.get('stars', 0),
                forks=row.get('forks', 0),
                language=row.get('language'),
                description=row.get('description'),
                is_valid=True
            )
            repositories.append(repo)
        
        self.repo_store.insert_repositories(repositories)
    
    def migrate_benchmark_collection(self, pkl_path: Path) -> None:
        """Migrate dashboard.fc.pkl to database tables"""
        collection = BenchmarkCollection.load(pkl_path)
        
        with self.db.transaction() as conn:
            # Insert benchmark collection
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO benchmark_collections 
                (repository_id, base_url, collected_at, modified_at, param_keys, index_data)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                self._get_repository_id(collection.task.owner, collection.task.repo),
                collection.base_url,
                collection.collected_at,
                collection.modified_at,
                json.dumps(collection.param_keys),
                json.dumps(collection.index_data)
            ))
            
            collection_id = cursor.lastrowid
            
            # Insert benchmark runs
            for _, run in collection.benchmarks.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO benchmark_runs 
                    (collection_id, commit_sha, benchmark_name, machine_name, value, unit, params)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    collection_id,
                    run.get('commit_hash', ''),
                    run.get('benchmark', ''),
                    run.get('machine', ''),
                    run.get('value', 0.0),
                    run.get('unit', ''),
                    json.dumps(run.get('params', {}))
                ))
```

#### Modernized Scripts

```python
# scratch/scripts/collect_and_filter_commits_v2.py
from datasmith.storage.database import DataSmithDB
from datasmith.storage.repositories import RepositoryStore
from datasmith.storage.commits import CommitStore

def main():
    db = DataSmithDB("scratch/artifacts/datasmith.db")
    repo_store = RepositoryStore(db)
    commit_store = CommitStore(db)
    
    # Start pipeline run tracking
    run_id = db.start_pipeline_run("collect_and_filter_commits_v2", config=vars(args))
    
    try:
        repositories = repo_store.get_valid_repositories(limit=args.max_repos)
        
        for repo in repositories:
            db.track_item(run_id, "repository", f"{repo.owner}/{repo.repo}", "processing")
            
            try:
                commits = collect_commits_for_repo(repo)
                filtered_commits = filter_performance_commits(commits)
                commit_store.insert_commits(repo.id, filtered_commits)
                
                db.track_item(run_id, "repository", f"{repo.owner}/{repo.repo}", "completed")
                
            except Exception as e:
                db.track_item(run_id, "repository", f"{repo.owner}/{repo.repo}", "failed", str(e))
                logger.error(f"Failed to process {repo.owner}/{repo.repo}: {e}")
        
        db.complete_pipeline_run(run_id, "completed")
        
    except Exception as e:
        db.complete_pipeline_run(run_id, "failed", str(e))
        raise
```

### Phase 4: Backwards Compatibility

#### Legacy File Support

```python
# src/datasmith/storage/legacy.py
class LegacyBridge:
    """Provides backwards compatibility with existing pickle/JSON files"""
    
    def __init__(self, db: DataSmithDB):
        self.db = db
    
    def export_to_pickle(self, collection_id: int, output_path: Path) -> None:
        """Export database data back to BenchmarkCollection pickle format"""
        collection = self._build_collection_from_db(collection_id)
        collection.save(output_path)
    
    def export_context_registry(self, output_path: Path) -> None:
        """Export build contexts to JSON format"""
        contexts = self._get_all_contexts()
        registry = ContextRegistry(contexts)
        registry.save_to_file(output_path)
```

### Phase 5: Performance Optimizations

#### Query Optimization

```python
# Batch operations for large datasets
class BatchProcessor:
    def __init__(self, db: DataSmithDB, batch_size: int = 1000):
        self.db = db
        self.batch_size = batch_size
    
    def bulk_insert_commits(self, commits: List[Commit]) -> None:
        for i in range(0, len(commits), self.batch_size):
            batch = commits[i:i + self.batch_size]
            with self.db.transaction() as conn:
                conn.executemany("""
                    INSERT OR IGNORE INTO commits 
                    (repository_id, sha, commit_date, author_name, message)
                    VALUES (?, ?, ?, ?, ?)
                """, [(c.repo_id, c.sha, c.date, c.author, c.message) for c in batch])
```

### Phase 6: Migration Timeline

#### Week 1-2: Foundation
- [ ] Create database schema and migration utilities
- [ ] Implement core data access layer
- [ ] Create migration scripts for existing data
- [ ] Set up comprehensive testing

#### Week 3-4: Script Migration
- [ ] Migrate repository discovery scripts
- [ ] Migrate commit collection/filtering scripts
- [ ] Migrate context synthesis scripts
- [ ] Add pipeline run tracking

#### Week 5-6: Advanced Features
- [ ] Migrate benchmark collection scripts
- [ ] Migrate breakpoint detection
- [ ] Add query optimization and indexing
- [ ] Performance testing and tuning

#### Week 7-8: Integration & Testing
- [ ] End-to-end pipeline testing
- [ ] Backwards compatibility validation
- [ ] Performance benchmarking
- [ ] Documentation and training

### Benefits After Migration

1. **Robustness**: Atomic transactions prevent data corruption
2. **Performance**: Indexed queries vs. linear file scans
3. **Consistency**: Schema enforcement and foreign key constraints
4. **Debugging**: SQL queries to inspect pipeline state
5. **Resumability**: Track and resume failed pipeline runs
6. **Concurrency**: Multiple scripts can safely read/write
7. **Analytics**: Rich queries across pipeline stages
8. **Monitoring**: Built-in pipeline run tracking and metrics

### Risk Mitigation

1. **Data Loss**: Full migration testing with backups
2. **Performance Regression**: Benchmark before/after migration
3. **Script Compatibility**: Gradual migration with legacy bridge
4. **Team Training**: Documentation and examples for new patterns

## Implementation Priority

1. **High Priority**: Repository and commit storage (foundation)
2. **Medium Priority**: Context registry and benchmark results
3. **Low Priority**: Advanced analytics and monitoring features

This migration will transform DataSmith from a file-based pipeline to a robust, database-backed system suitable for production use at scale.