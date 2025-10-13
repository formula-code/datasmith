"""Migration utilities for converting existing DataSmith data to SQLite."""
from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from datasmith.benchmark.collection import BenchmarkCollection as LegacyBenchmarkCollection
from datasmith.docker.context import ContextRegistry, Task
from datasmith.logging_config import get_logger
from datasmith.storage.database import DataSmithDB
from datasmith.storage.repositories import Repository, RepositoryStore
from datasmith.storage.commits import Commit, CommitStore
from datasmith.storage.contexts import BuildContext, BuildContextStore
from datasmith.storage.benchmarks import (
    BenchmarkCollection, BenchmarkRun, Breakpoint, BenchmarkStore
)

logger = get_logger("storage.migration")


class PipelineMigrator:
    """Migrates existing DataSmith data files to SQLite database."""
    
    def __init__(self, db: DataSmithDB):
        self.db = db
        self.repo_store = RepositoryStore(db)
        self.commit_store = CommitStore(db)
        self.context_store = BuildContextStore(db)
        self.benchmark_store = BenchmarkStore(db)
    
    def migrate_repositories_from_csv(self, csv_path: Path) -> int:
        """Migrate repositories from CSV file to database.
        
        Args:
            csv_path: Path to repos_valid.csv or repos_discovered.csv
            
        Returns:
            int: Number of repositories migrated
        """
        logger.info(f"Migrating repositories from {csv_path}")
        
        df = pd.read_csv(csv_path)
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
                is_valid=bool(row.get('is_valid', csv_path.name.startswith('repos_valid'))),
                metadata={}
            )
            repositories.append(repo)
        
        self.repo_store.insert_repositories(repositories)
        logger.info(f"Migrated {len(repositories)} repositories")
        return len(repositories)
    
    def migrate_commits_from_jsonl(self, jsonl_path: Path, max_commits: Optional[int] = None) -> int:
        """Migrate commits from JSONL file to database.
        
        Args:
            jsonl_path: Path to commits_filtered.jsonl or commits_all.jsonl
            max_commits: Maximum number of commits to migrate (for testing)
            
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
        return count\n    \n    def migrate_context_registry(self, registry_path: Path) -> int:\n        """Migrate context registry from JSON file to database.\n        \n        Args:\n            registry_path: Path to context_registry.json\n            \n        Returns:\n            int: Number of contexts migrated\n        """
        logger.info(f\"Migrating context registry from {registry_path}\")\n        \n        with open(registry_path, 'r') as f:\n            data = json.load(f)\n        \n        contexts = data.get('contexts', {})\n        count = 0\n        \n        for task_key, context_data in contexts.items():\n            # Parse task from key\n            task = Task.from_string(task_key)  # Assumes Task has from_string method\n            \n            # Get repository ID\n            repo = self.repo_store.get_repository(task.owner, task.repo)\n            if not repo:\n                logger.warning(f\"Repository {task.owner}/{task.repo} not found, skipping context\")\n                continue\n            \n            # Create build context\n            build_context = BuildContext(\n                id=None,\n                repository_id=repo.id,\n                sha=task.sha or '',\n                tag=task.tag,\n                commit_date=pd.to_datetime(task.commit_date, unit='s') if task.commit_date else None,\n                dockerfile_data=context_data.get('dockerfile_data'),\n                entrypoint_data=context_data.get('entrypoint_data'),\n                env_building_data=context_data.get('env_building_data'),\n                base_building_data=context_data.get('base_building_data'),\n                building_data=context_data.get('building_data'),\n                run_building_data=context_data.get('run_building_data'),\n                validated=False,  # Will need validation\n                validation_result={}\n            )\n            \n            self.context_store.insert_context(build_context)\n            count += 1\n        \n        logger.info(f\"Migrated {count} build contexts\")\n        return count\n    \n    def migrate_benchmark_collection(self, pkl_path: Path) -> int:\n        \"\"\"Migrate benchmark collection from pickle file to database.\n        \n        Args:\n            pkl_path: Path to dashboard.fc.pkl file\n            \n        Returns:\n            int: Number of benchmark runs migrated\n        \"\"\"\n        logger.info(f\"Migrating benchmark collection from {pkl_path}\")\n        \n        # Load legacy collection\n        legacy_collection = LegacyBenchmarkCollection.load(pkl_path)\n        \n        # Get repository\n        task = legacy_collection.task\n        repo = self.repo_store.get_repository(task.owner, task.repo)\n        if not repo:\n            logger.warning(f\"Repository {task.owner}/{task.repo} not found, creating it\")\n            repo = Repository(\n                id=None,\n                owner=task.owner,\n                repo=task.repo,\n                url=f\"https://github.com/{task.owner}/{task.repo}\",\n                is_valid=True,\n                metadata={}\n            )\n            repo_id = self.repo_store.insert_repository(repo)\n        else:\n            repo_id = repo.id\n        \n        # Create benchmark collection\n        collection = BenchmarkCollection(\n            id=None,\n            repository_id=repo_id,\n            base_url=legacy_collection.base_url,\n            collected_at=legacy_collection.collected_at,\n            modified_at=legacy_collection.modified_at,\n            param_keys=legacy_collection.param_keys,\n            index_data=legacy_collection.index_data,\n            collection_metadata={}\n        )\n        \n        collection_id = self.benchmark_store.insert_collection(collection)\n        \n        # Migrate benchmark runs\n        runs = []\n        if not legacy_collection.benchmarks.empty:\n            for _, run_data in legacy_collection.benchmarks.iterrows():\n                run = BenchmarkRun(\n                    id=None,\n                    collection_id=collection_id,\n                    commit_sha=run_data.get('commit_hash', ''),\n                    benchmark_name=run_data.get('benchmark', ''),\n                    machine_name=run_data.get('machine', ''),\n                    value=float(run_data.get('value', 0.0)),\n                    unit=run_data.get('unit'),\n                    params=run_data.get('params', {})\n                )\n                runs.append(run)\n                \n                # Batch insert\n                if len(runs) >= 1000:\n                    self.benchmark_store.insert_runs(runs)\n                    runs = []\n        \n        # Insert remaining runs\n        if runs:\n            self.benchmark_store.insert_runs(runs)\n        \n        # Migrate breakpoints if available\n        breakpoint_count = 0\n        if hasattr(legacy_collection, 'breakpoints') and not legacy_collection.breakpoints.empty:\n            breakpoints = []\n            for _, bp_data in legacy_collection.breakpoints.iterrows():\n                breakpoint = Breakpoint(\n                    id=None,\n                    collection_id=collection_id,\n                    commit_sha=bp_data.get('commit_hash', ''),\n                    benchmark_name=bp_data.get('benchmark', ''),\n                    machine_name=bp_data.get('machine', ''),\n                    change_type=bp_data.get('change_type', 'neutral'),\n                    confidence_score=float(bp_data.get('confidence', 0.0)),\n                    detection_method=bp_data.get('method', 'unknown'),\n                    before_value=float(bp_data.get('before_value', 0.0)),\n                    after_value=float(bp_data.get('after_value', 0.0)),\n                    relative_change=float(bp_data.get('relative_change', 0.0)),\n                    absolute_change=float(bp_data.get('absolute_change', 0.0)),\n                    coverage_data=bp_data.get('coverage_data', {}),\n                    github_data=bp_data.get('github_data', {}),\n                    breakpoint_metadata={}\n                )\n                breakpoints.append(breakpoint)\n                \n                if len(breakpoints) >= 100:\n                    for bp in breakpoints:\n                        self.benchmark_store.insert_breakpoint(bp)\n                    breakpoint_count += len(breakpoints)\n                    breakpoints = []\n            \n            # Insert remaining breakpoints\n            for bp in breakpoints:\n                self.benchmark_store.insert_breakpoint(bp)\n            breakpoint_count += len(breakpoints)\n        \n        run_count = len(legacy_collection.benchmarks) if not legacy_collection.benchmarks.empty else 0\n        logger.info(f\"Migrated benchmark collection with {run_count} runs and {breakpoint_count} breakpoints\")\n        return run_count\n    \n    def export_legacy_format(self, output_dir: Path, repository_id: int) -> None:\n        \"\"\"Export database data back to legacy format for backwards compatibility.\n        \n        Args:\n            output_dir: Directory to write legacy format files\n            repository_id: Repository to export\n        \"\"\"\n        output_dir.mkdir(parents=True, exist_ok=True)\n        \n        # Export repositories\n        repo = self.repo_store.get_repository_by_id(repository_id)\n        if not repo:\n            raise ValueError(f\"Repository {repository_id} not found\")\n        \n        # Export to CSV\n        repos_df = pd.DataFrame([repo.to_dict()])\n        repos_df.to_csv(output_dir / \"repos_exported.csv\", index=False)\n        \n        # Export commits\n        commits = self.commit_store.get_commits_for_repository(repository_id)\n        if commits:\n            commits_data = []\n            for commit in commits:\n                commit_dict = commit.to_dict()\n                commit_dict['owner'] = repo.owner\n                commit_dict['repo'] = repo.repo\n                commits_data.append(commit_dict)\n            \n            with open(output_dir / \"commits_exported.jsonl\", 'w') as f:\n                for commit_data in commits_data:\n                    f.write(json.dumps(commit_data) + '\\n')\n        \n        # Export contexts to JSON registry format\n        contexts = self.context_store.get_contexts_for_repository(repository_id)\n        if contexts:\n            registry_data = {'contexts': {}}\n            for context in contexts:\n                task_key = f\"Task(owner='{repo.owner}', repo='{repo.repo}', sha='{context.sha}', commit_date={context.commit_date.timestamp() if context.commit_date else 0.0}, tag='{context.tag}')\"\n                registry_data['contexts'][task_key] = {\n                    'dockerfile_data': context.dockerfile_data,\n                    'entrypoint_data': context.entrypoint_data,\n                    'env_building_data': context.env_building_data,\n                    'base_building_data': context.base_building_data,\n                    'building_data': context.building_data,\n                    'run_building_data': context.run_building_data\n                }\n            \n            with open(output_dir / \"context_registry_exported.json\", 'w') as f:\n                json.dump(registry_data, f, indent=2)\n        \n        logger.info(f\"Exported legacy format files to {output_dir}\")\n    \n    def validate_migration(self, original_files: Dict[str, Path], repository_id: Optional[int] = None) -> Dict[str, Any]:\n        \"\"\"Validate that migration was successful by comparing counts.\n        \n        Args:\n            original_files: Dictionary mapping file types to paths\n            repository_id: Optional repository ID to validate\n            \n        Returns:\n            Dict[str, Any]: Validation results\n        \"\"\"\n        results = {\n            'validation_passed': True,\n            'details': {},\n            'errors': []\n        }\n        \n        try:\n            # Validate repositories\n            if 'repositories_csv' in original_files:\n                original_df = pd.read_csv(original_files['repositories_csv'])\n                db_count = self.repo_store.get_repository_stats()['total_repositories']\n                \n                results['details']['repositories'] = {\n                    'original_count': len(original_df),\n                    'migrated_count': db_count,\n                    'match': len(original_df) == db_count\n                }\n                \n                if len(original_df) != db_count:\n                    results['validation_passed'] = False\n                    results['errors'].append(f\"Repository count mismatch: {len(original_df)} vs {db_count}\")\n            \n            # Validate commits\n            if 'commits_jsonl' in original_files:\n                original_count = 0\n                with open(original_files['commits_jsonl'], 'r') as f:\n                    for _ in f:\n                        original_count += 1\n                \n                db_count = self.commit_store.get_commit_stats()['total_commits']\n                \n                results['details']['commits'] = {\n                    'original_count': original_count,\n                    'migrated_count': db_count,\n                    'match': original_count == db_count\n                }\n                \n                if original_count != db_count:\n                    results['validation_passed'] = False\n                    results['errors'].append(f\"Commit count mismatch: {original_count} vs {db_count}\")\n            \n            # Validate database integrity\n            stats = self.db.get_stats()\n            results['details']['database_stats'] = stats\n            \n        except Exception as e:\n            results['validation_passed'] = False\n            results['errors'].append(f\"Validation error: {str(e)}\")\n            logger.error(f\"Migration validation failed: {e}\")\n        \n        return results